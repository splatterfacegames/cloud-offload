"""
Dispatcher - watches queue and spins up cloud workers when needed.

Runs locally, monitors the job queue, and launches cloud instances
when enough jobs are waiting.
"""

import json
import logging
import math
import os
import secrets
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from cloud_offload.config import (
    LIVE_RELOADABLE_CONFIG_FIELDS,
    CloudConfig,
    estimate_runpod_storage_monthly,
)
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.cache_scheduler import (
    PlacementCandidate,
    PlacementDecision,
    choose_placement,
    resolve_prepared_requirements,
    scheduler_runtime,
)
from cloud_offload.credentials import huggingface_token
from cloud_offload.providers import create_connector
from cloud_offload.providers.base import CloudConnector, CloudProvider, Instance
from cloud_offload.providers.base import PlacementConstraints, StorageAttachment
from cloud_offload.queue import JobLease, JobQueue, JobStatus, utc_now
from cloud_offload.profiles import (
    configured_worker_profiles,
    profile_providing,
    worker_profile_gpu_type,
    worker_profile_min_gpu_ram,
)

logger = logging.getLogger(__name__)

# How long a specific offer sits out after its host refuses a launch.
OFFER_COOLDOWN_SECONDS = 600

# A provider can report a pod as running before it has created a container
# runtime. If the entrypoint never runs, no worker can report the failure home.
RUNNER_REGISTRATION_TIMEOUT_SECONDS = 3600

# A rented pod bills from creation even when its host never starts the
# container. When the provider reports container telemetry, a pod with zero
# container uptime this long after rental is stalled, not booting: give it
# back and rent elsewhere instead of waiting out the registration deadline.
RUNNER_CONTAINER_START_TIMEOUT_SECONDS = 600

# A managed worker cannot destroy its provider resource when its process exits.
# Some providers restart the container instead. Give the dispatcher time to
# destroy the paid resource before the worker's local idle fail-safe can fire.
WORKER_IDLE_GRACE_SECONDS = 60


def _load_or_create_worker_token(config: CloudConfig) -> str:
    """Return a stable coordinator credential for workers across restarts.

    A random token held only in dispatcher memory disconnects every warm worker
    whenever the local dispatcher restarts.  Keep the generated credential next
    to the queue database instead.  An explicitly configured token still wins.
    """
    if config.worker_token:
        return config.worker_token

    token_path = Path(config.queue_db_path).with_name("worker-token")
    token_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        token = token_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        token = secrets.token_urlsafe(32)
        try:
            with token_path.open("x", encoding="utf-8") as handle:
                handle.write(token + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            try:
                token_path.chmod(0o600)
            except OSError:
                logger.warning("Could not restrict worker token file permissions")
        except FileExistsError:
            token = token_path.read_text(encoding="utf-8").strip()

    if len(token) < 32:
        raise RuntimeError(
            f"Persistent worker token is missing or invalid: {token_path}. "
            "Remove it only after terminating all cloud workers."
        )
    return token


class Dispatcher:
    """
    Monitors job queue and manages cloud workers.

    Responsibilities:
    - Watch for queued jobs
    - Spin up workers when queue depth >= threshold
    - Track active workers
    - Clean up idle workers
    """

    def __init__(
        self,
        config: CloudConfig,
        queue: JobQueue | None = None,
        provider: CloudProvider | None = None,
        *,
        connector: CloudConnector | None = None,
    ):
        self.config = config
        self.queue = queue or JobQueue(config.queue_db_path)
        self.cache_registry = CacheRegistry(config.queue_db_path)

        if provider is not None and connector is not None:
            raise ValueError("Pass connector or legacy provider, not both")
        supplied = connector or provider
        if supplied:
            self.connectors = {config.provider: supplied}
        else:
            self.connectors = {
                name: create_connector(name, config)
                for name in config.provider_order
                if config.api_key_for(name)
            }
            if not self.connectors:
                self.connectors = {
                    config.provider: create_connector(config.provider, config)
                }
        self.connector = self.connectors.get(config.provider) or next(
            iter(self.connectors.values())
        )
        # Compatibility for integrations that accessed ``dispatcher.provider``.
        self.provider = self.connector

        # Track active workers
        self.active_instances: dict[str, Instance] = {}
        self.instance_providers: dict[str, str] = {}
        self.instance_profiles: dict[str, str] = {}
        self.instance_leases: dict[str, str] = {}
        self.last_activity: dict[str, datetime] = {}
        self.launched_at: dict[str, datetime] = {}
        self.runner_ready_instances: set[str] = set()
        self.runner_feedback_at: dict[str, datetime] = {}
        self.event_producer_id = f"dispatcher:{uuid.uuid4()}"
        self.event_producer_sequence = 0
        self.launch_failures: dict[tuple[str, str], int] = {}
        self.next_launch_at: dict[tuple[str, str], float] = {}
        # (provider, offer_id) -> monotonic expiry. A host that refuses a launch
        # keeps being the cheapest offer, so without this the dispatcher retries
        # the same dead machine forever.
        self.offer_cooldowns: dict[tuple[str, str], float] = {}
        self.worker_token = _load_or_create_worker_token(self.config)
        self.queue.set_worker_token(self.worker_token)
        self._tunnel = None  # opened lazily when ingress == "cloudflared"
        self._replication_thread: threading.Thread | None = None
        self._next_replication_at = 0.0

    def _resolve_coordinator_url(self) -> str | None:
        """The URL a worker uses to reach the coordinator.

        An explicit ``coordinator_url`` always wins. Otherwise, when ingress is
        ``cloudflared``, open (once) an ephemeral tunnel to the local coordinator
        and return its public URL. With ingress ``none`` and no URL, there is no
        way for a worker to call home, so a launch is refused.
        """
        if self.config.coordinator_url:
            return self.config.coordinator_url
        if self.config.ingress != "cloudflared":
            return None

        from cloud_offload.ingress import CloudflaredTunnel, IngressError
        from cloud_offload.service_config import read_service_info

        if self._tunnel is not None and self._tunnel.running:
            return self._tunnel.url

        info = read_service_info()
        if not info or not info.get("port"):
            logger.error("Cannot open ingress: coordinator discovery file missing")
            return None
        try:
            self._tunnel = CloudflaredTunnel()
            return self._tunnel.open(int(info["port"]))
        except IngressError as exc:
            logger.error("Ingress failed: %s", exc)
            self._tunnel = None
            return None

    def run(self, once: bool = False):
        """
        Main dispatcher loop.

        Args:
            once: If True, run one iteration and exit (for testing)
        """
        logger.info(
            f"Dispatcher starting (min_queue_depth={self.config.min_queue_depth})"
        )

        while True:
            try:
                self._tick()
            except Exception as e:
                logger.error(f"Dispatcher error: {e}")

            if once:
                break

            time.sleep(self.config.poll_interval_seconds)

    def _tick(self):
        """Single dispatcher iteration."""
        # Runtime policy and immutable worker-profile pins are user-controlled
        # through the coordinator. Refresh these non-secret fields each tick so
        # a newly submitted job cannot launch with a stale image or GPU policy.
        # Provider credentials/connectors remain process-owned.
        try:
            config_path = getattr(self.config, "_source_path", None)
            persisted = CloudConfig.load(config_path, resolve_secrets=False)
            # A programmatically constructed config (no source path) owns its
            # runtime policy: never overwrite it with the default user config
            # or with built-in defaults when no file exists on disk.
            persisted_source = getattr(persisted, "_source_path", None)
            if persisted_source is None or (
                config_path is not None and persisted_source.exists()
            ):
                for field_name in LIVE_RELOADABLE_CONFIG_FIELDS:
                    # Prepared storage can create paid durable resources. Only
                    # refresh it for file-backed services; a programmatically
                    # constructed config remains authoritative for this opt-in.
                    if field_name == "prepared_storage" and config_path is None:
                        continue
                    setattr(self.config, field_name, getattr(persisted, field_name))
            runpod = self.connectors.get("runpod")
            if runpod is not None and hasattr(runpod, "registry_auth_id"):
                runpod.registry_auth_id = self.config.runpod_registry_auth_id.strip()
        except Exception as exc:
            logger.warning("Could not refresh cloud runtime policy: %s", exc)

        # Restore durable provider ownership before any decision can rent a
        # second resource. This also executes cancellation and hard limits.
        self._reconcile_leases()
        self._tick_regional_replication()

        # Count queued jobs
        queued_count = self.queue.count_by_status(JobStatus.QUEUED)
        logger.debug(
            f"Queue: {queued_count} queued, {len(self.active_instances)} workers active"
        )

        profiles = configured_worker_profiles(self.config)
        for provider_name in self.connectors:
            queued_jobs = self.queue.list_by_status(
                JobStatus.QUEUED, provider=provider_name
            )
            queued_profiles = {
                str(job.params.get("runtime_profile"))
                for job in queued_jobs
                if job.params.get("runtime_profile")
            }
            for requested_profile in queued_profiles:
                # Jobs carry whatever the client stamped, which is usually a
                # capability like comfyui-partition-v1 rather than an operator's
                # profile name. Resolve it the way routing does, or a correctly
                # configured worker never launches and the job waits forever.
                resolved = profiles.get(requested_profile) or profile_providing(
                    profiles, requested_profile
                )
                if resolved is None:
                    logger.error(
                        "Queued jobs reference unknown runtime profile %s "
                        "(configured profiles: %s)",
                        requested_profile,
                        ", ".join(sorted(profiles)) or "none",
                    )
                    continue
                profile_name = resolved["name"]
                matching_jobs = [
                    job
                    for job in queued_jobs
                    if job.params.get("runtime_profile") == requested_profile
                ]
                launch_groups: dict[str, list] = {}
                for job in matching_jobs:
                    confirmed = job.params.get("preflight") or {}
                    group = str(confirmed.get("candidate_id") or "unconfirmed")
                    launch_groups.setdefault(group, []).append(job)
                for candidate_id, launch_jobs in launch_groups.items():
                    launch_key = (provider_name, profile_name)
                    profile_queued = len(launch_jobs)
                    required_depth = (
                        1
                        if candidate_id != "unconfirmed"
                        else self.config.min_queue_depth
                    )
                    profile_running = any(
                        self.instance_providers.get(instance_id) == provider_name
                        and self.instance_profiles.get(instance_id) == profile_name
                        for instance_id in self.active_instances
                    )
                    profile_running = profile_running or any(
                        worker.get("provider") == provider_name
                        and worker.get("runtime_profile") == profile_name
                        for worker in self.queue.list_active_workers()
                    )
                    profile_running = profile_running or any(
                        lease.provider == provider_name
                        and lease.runtime_profile == profile_name
                        for lease in self.queue.list_open_leases()
                    )
                    if profile_queued < required_depth or profile_running:
                        continue
                    if time.monotonic() < self.next_launch_at.get(launch_key, 0):
                        continue
                    logger.info(
                        f"{provider_name}/{profile_name} queue depth {profile_queued} >= "
                        f"{required_depth}, launching worker for "
                        f"candidate {candidate_id}"
                    )
                    self._launch_worker(provider_name, profile_name, launch_jobs)

        # Update worker tracking
        self._update_workers()

        # A rented pod whose entrypoint never runs cannot report its own failure.
        self._check_unregistered_workers()

        # Check for idle workers to terminate
        self._check_idle_workers()

    def _tick_regional_replication(self) -> None:
        """Start one non-blocking automatic controller cycle when it is due."""

        replication = (self.config.prepared_storage or {}).get("replication") or {}
        if replication.get("mode") != "automatic":
            return
        if getattr(self.config, "_source_path", None) is None:
            return
        if self._replication_thread and self._replication_thread.is_alive():
            return
        now = time.monotonic()
        if now < self._next_replication_at:
            return
        self._next_replication_at = now + int(
            replication.get("controller_interval_seconds") or 300
        )
        self._replication_thread = threading.Thread(
            target=self._run_replication_controller_tick,
            name="cloud-offload-replication-controller",
            daemon=True,
        )
        self._replication_thread.start()

    def _run_replication_controller_tick(self) -> None:
        """Ask the authenticated local coordinator to run one durable cycle."""

        import requests

        from cloud_offload.service_config import discover_service_info

        try:
            service = discover_service_info(require_healthy=True)
            headers = (
                {"Authorization": "Bearer " + service["token"]}
                if service.get("token")
                else {}
            )
            replication = (
                (self.config.prepared_storage or {}).get("replication") or {}
            )
            response = requests.post(
                str(service["url"]).rstrip("/")
                + "/api/cache/replication/controller/tick",
                headers=headers,
                timeout=int(replication.get("copy_timeout_seconds") or 21600) + 300,
            )
            response.raise_for_status()
            status = str((response.json() or {}).get("status") or "unknown")
            logger.info("Regional replication controller tick: %s", status)
        except Exception as exc:  # noqa: BLE001 - next interval retries safely
            logger.warning(
                "Regional replication controller tick failed: %s",
                type(exc).__name__,
            )

    def _launch_worker(
        self,
        provider_name: str | None = None,
        profile_name: str | None = None,
        queued_jobs: list | None = None,
    ) -> Instance | None:
        """Launch a new cloud worker."""
        coordinator_url = self._resolve_coordinator_url()
        if not coordinator_url:
            logger.error(
                "Cloud launch needs a reachable coordinator: set coordinator_url "
                "or ingress=cloudflared"
            )
            return None
        provider_name = provider_name or self.config.provider
        connector = self.connectors[provider_name]
        profiles = configured_worker_profiles(self.config)
        if not profile_name or profile_name not in profiles:
            logger.error("Cloud launch requires a configured runtime profile")
            return None
        profile = profiles[profile_name]
        if provider_name not in profile["providers"]:
            logger.error(
                "Runtime profile %s does not support provider %s",
                profile_name,
                provider_name,
            )
            return None
        minimum_vram = max(
            [worker_profile_min_gpu_ram(profile)]
            + [
                int(job.params.get("min_gpu_ram_gb") or 0)
                for job in (queued_jobs or [])
            ]
        )
        requested_gpu_types = [
            str(job.params.get("gpu_type"))
            for job in (queued_jobs or [])
            if str(job.params.get("gpu_type") or "any").lower() != "any"
        ]
        gpu_type = (
            requested_gpu_types[0]
            if requested_gpu_types
            else worker_profile_gpu_type(profile, self.config.gpu_type)
        )
        # Find an offer, optionally treating regional prepared state as a
        # schedulable resource. Disabled mode preserves the original call path.
        cooling = self._offers_on_cooldown(provider_name)
        requirements = resolve_prepared_requirements(
            profile_name, profile, queued_jobs or []
        )
        # A confirmed quote is volatile by contract; freshness is enforced by
        # re-validating the exact offer, price, and prepared volume against the
        # live catalog below, so the quote's age alone is not disqualifying.
        confirmations = self._preflight_entries(queued_jobs)
        confirmed = self._shared_preflight(queued_jobs)
        if confirmations and (
            len(confirmations) != len(queued_jobs or []) or confirmed is None
        ):
            self._refuse_preflight_launch(
                queued_jobs,
                "Queued jobs have conflicting preflight confirmations.",
            )
            return None
        if confirmed and confirmed.get("provider") != provider_name:
            self._refuse_preflight_launch(
                queued_jobs,
                "The confirmed provider no longer matches the queued route.",
            )
            return None
        try:
            region_placement = self._launch_region_constraints(confirmations)
        except ValueError as exc:
            self._refuse_preflight_launch(queued_jobs, str(exc))
            return None
        region_arguments = {"placement": region_placement} if region_placement else {}
        runtime_arguments = (
            {"min_cuda_version": profile["min_cuda_version"]}
            if profile.get("min_cuda_version") else {}
        )
        placement_decision = None
        if confirmed and confirmed.get("prepared_volume_id"):
            placement_decision = self._confirmed_cache_placement(
                connector=connector,
                provider_name=provider_name,
                gpu_type=gpu_type,
                minimum_vram=minimum_vram,
                cooling=cooling,
                requirements=requirements,
                confirmed=confirmed,
            )
            if (
                placement_decision.action != "launch"
                and placement_decision.reason in {
                    "confirmed_prepared_volume_changed",
                    "confirmed_prepared_provider_volume_changed",
                    "confirmed_prepared_storage_changed",
                    "confirmed_provider_volume_unavailable",
                }
                and (
                    placement_decision.reason
                    != "confirmed_prepared_volume_changed"
                    or (confirmed and confirmed.get("prepared_provider_volume_id") is not None)
                )
            ):
                # This is an identity failure, not a capacity failure. Do not
                # publish an event, create a lease, or try a cold replacement:
                # the confirmed physical object needs a fresh decision.
                logger.error(
                    "Refusing confirmed prepared launch for queued jobs: %s",
                    placement_decision.reason,
                )
                return None
            self._publish_launch_event(
                queued_jobs,
                {
                    "type": "cache_placement_considered",
                    **placement_decision.explanation(),
                },
            )
            if (
                placement_decision.action != "launch"
                or not placement_decision.candidate
            ):
                if placement_decision.reason == "confirmed_prepared_binding_changed":
                    self._record_launch_failure(
                        provider_name,
                        profile_name,
                        queued_jobs,
                        "The strict prepared-storage binding no longer names "
                        "the confirmed volume",
                    )
                    return None
                self._refuse_preflight_launch(
                    queued_jobs,
                    "The confirmed prepared placement is no longer available.",
                )
                return None
            offer = placement_decision.candidate.offer
            self._publish_launch_event(
                queued_jobs,
                {
                    "type": "cache_placement_selected",
                    **placement_decision.explanation(),
                },
            )
        elif confirmed:
            try:
                offers = connector.list_available(
                    gpu_type=gpu_type,
                    min_gpu_ram=minimum_vram,
                    max_hourly_rate=float(
                        (confirmed.get("request_policy") or {}).get(
                            "max_hourly_rate", self.config.max_hourly_rate
                        )
                    ),
                    **region_arguments,
                )
            except Exception:
                offers = []
            offer = next(
                (
                    item
                    for item in offers
                    if str(item.get("id")) == str(confirmed.get("offer_id"))
                    and str(item.get("id")) not in cooling
                ),
                None,
            )
            if not offer or not self._confirmed_offer_matches(confirmed, offer):
                self._refuse_preflight_launch(
                    queued_jobs,
                    "The confirmed GPU offer or price changed before provider launch.",
                )
                return None
        elif self.config.prepared_storage.get("enabled"):
            placement_decision = self._choose_cache_placement(
                connector=connector,
                provider_name=provider_name,
                gpu_type=gpu_type,
                minimum_vram=minimum_vram,
                cooling=cooling,
                requirements=requirements,
                region_placement=region_placement,
            )
            self._publish_launch_event(
                queued_jobs,
                {
                    "type": "cache_placement_considered",
                    **placement_decision.explanation(),
                },
            )
            if (
                placement_decision.action != "launch"
                or not placement_decision.candidate
            ):
                detail = placement_decision.reason
                self._record_launch_failure(
                    provider_name, profile_name, queued_jobs, detail
                )
                return None
            offer = placement_decision.candidate.offer
            selected_type = (
                "cache_cold_fallback"
                if placement_decision.fallback
                else "cache_placement_selected"
            )
            self._publish_launch_event(
                queued_jobs,
                {"type": selected_type, **placement_decision.explanation()},
            )
        else:
            offer = connector.find_cheapest(
                gpu_type=gpu_type,
                min_gpu_ram=minimum_vram,
                max_hourly_rate=self.config.max_hourly_rate,
                exclude=cooling,
                **region_arguments,
            )

        if not offer:
            detail = "No available GPU matched the partition constraints"
            if cooling:
                detail += f" ({len(cooling)} offer(s) on launch-failure cooldown)"
            logger.warning(
                f"No available GPUs matching criteria "
                f"(type={self.config.gpu_type}, max_rate=${self.config.max_hourly_rate}/hr"
                + (f", {len(cooling)} on cooldown)" if cooling else ")")
            )
            self._record_launch_failure(
                provider_name, profile_name, queued_jobs, detail
            )
            return None

        if confirmations and not self._confirmed_offers_allowed(confirmations, offer):
            self._refuse_preflight_launch(
                queued_jobs,
                "A queued preflight confirmation does not permit the live offer.",
            )
            return None

        logger.info(
            f"Launching {offer['gpu_type']} @ ${offer['hourly_rate']:.2f}/hr (offer {offer['id']})"
        )
        self._publish_launch_event(
            queued_jobs,
            {
                "type": "provisioning_started",
                "provider": provider_name,
                "runtime_profile": profile_name,
                "gpu_type": offer["gpu_type"],
                "hourly_rate": offer["hourly_rate"],
                "overall_progress": 1,
            },
        )

        # Build startup script
        startup_script = self._build_startup_script(profile)
        wheelhouse_url = profile["wheelhouse_url"] or self.config.worker_wheelhouse_url
        wheelhouse_sha256 = (
            profile["wheelhouse_sha256"] or self.config.worker_wheelhouse_sha256
        )
        # Environment variables for worker.
        # Older immutable worker images do not know the live keep-warm policy.
        # Give them an effectively indefinite timeout as a compatibility path;
        # current workers additionally refresh policy from the coordinator.
        worker_idle_seconds = (
            10 * 365 * 24 * 60 * 60
            if self.config.keep_warm
            else self.config.idle_shutdown_seconds + WORKER_IDLE_GRACE_SECONDS
        )
        env_vars = {
            "CLOUD_OFFLOAD_WORKER_MODE": "true",
            "CLOUD_OFFLOAD_WORKER_TOKEN": self.worker_token,
            "CLOUD_OFFLOAD_COORDINATOR_URL": coordinator_url,
            "CLOUD_OFFLOAD_PROVIDER": provider_name,
            "CLOUD_OFFLOAD_IDLE_SHUTDOWN": str(worker_idle_seconds),
            "CLOUD_OFFLOAD_KEEP_WARM": str(self.config.keep_warm).lower(),
            "CLOUD_OFFLOAD_KEEP_WARM_WARNING": str(
                self.config.keep_warm_warning_seconds
            ),
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL": wheelhouse_url,
            "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256": wheelhouse_sha256,
            "CLOUD_OFFLOAD_WORKER_PROFILE": profile_name,
            "CLOUD_OFFLOAD_WORKER_IMAGE_PROFILE": profile["image_profile"],
            "CLOUD_OFFLOAD_WORKER_MODELS": ",".join(profile["models"]),
        }
        if profile.get("platform"):
            env_vars["CLOUD_OFFLOAD_WORKER_PLATFORM"] = profile["platform"]
        if os.environ.get("CLOUD_OFFLOAD_BENCHMARK_MOUNT_CORRUPTION") == "1":
            env_vars["CLOUD_OFFLOAD_BENCHMARK_MOUNT_CORRUPTION"] = "1"
        if profile.get("python_abi"):
            env_vars["CLOUD_OFFLOAD_WORKER_PYTHON_ABI"] = profile["python_abi"]
        if profile.get("weights"):
            # The worker stages these before its first job. Pass the configured
            # Hub token for public weights too: authenticated downloads avoid
            # the stricter anonymous rate limits and are materially more
            # reliable for large model profiles.
            env_vars["CLOUD_OFFLOAD_WEIGHTS"] = json.dumps(
                profile["weights"], separators=(",", ":")
            )
            hub_token = huggingface_token()
            if hub_token:
                env_vars["HF_TOKEN"] = hub_token
            elif any(entry.get("gated") for entry in profile["weights"]):
                logger.warning(
                    "Profile %s has gated weights but no Hugging Face token "
                    "is configured; the download will run anonymously and "
                    "likely fail",
                    profile_name,
                )
        if profile.get("custom_nodes"):
            # Installed by the worker before its first job, alongside weights.
            # These carry no credentials by construction: a registry release and
            # a public clone URL are both fetched anonymously.
            env_vars["CLOUD_OFFLOAD_CUSTOM_NODES"] = json.dumps(
                profile["custom_nodes"], separators=(",", ":")
            )

        placement = placement_decision.placement() if placement_decision else None
        if placement and region_placement and not set(placement.datacenter_ids).issubset(
            region_placement.datacenter_ids
        ):
            self._refuse_preflight_launch(
                queued_jobs, "The prepared placement is outside the allowed regions."
            )
            return None
        placement = placement or region_placement
        if placement and placement.storage_attachments and placement_decision and placement_decision.candidate:
            volume = placement_decision.candidate.volume
            selected_manifest_id = (
                placement_decision.candidate.manifest_ids[0]
                if placement_decision.candidate.manifest_ids
                else ""
            )
            for queued_job in queued_jobs or []:
                queued_job.params["prepared_requirement"] = requirements
                queued_job.params["cache_volume_id"] = volume.id
                queued_job.params["cache_provider_volume_id"] = (
                    volume.provider_volume_id
                )
                queued_job.params["cache_datacenter_id"] = volume.datacenter_id
                if selected_manifest_id:
                    queued_job.params["cache_manifest_id"] = selected_manifest_id
                else:
                    queued_job.params.pop("cache_manifest_id", None)
                self.queue.update(queued_job)
            env_vars.update(
                {
                    "CLOUD_OFFLOAD_CACHE_ROOT": "/workspace/cloud-offload",
                    "CLOUD_OFFLOAD_CACHE_VOLUME_ID": volume.id,
                    "CLOUD_OFFLOAD_CACHE_EXPECTED_PROVIDER_VOLUME_ID": volume.provider_volume_id,
                    "CLOUD_OFFLOAD_CACHE_MANIFEST": (
                        selected_manifest_id
                        if selected_manifest_id
                        else requirements["profile_fingerprint"]
                    ),
                    "CLOUD_OFFLOAD_CACHE_MODE": "restore-and-populate",
                    "CLOUD_OFFLOAD_CACHE_POLICY": json.dumps(
                        self.config.prepared_storage, separators=(",", ":")
                    ),
                    "CLOUD_OFFLOAD_CACHE_REQUIREMENTS": json.dumps(
                        requirements, separators=(",", ":")
                    ),
                }
            )

        disk_gb = self._planned_disk_gb(profile_name, queued_jobs)
        max_runtime_seconds, max_cost_usd = self._lease_limits(queued_jobs)
        lease = self.queue.create_lease(
            provider=provider_name,
            runtime_profile=profile_name,
            job_ids=[job.id for job in (queued_jobs or [])],
            hourly_rate=float(offer.get("hourly_rate") or 0),
            max_runtime_seconds=max_runtime_seconds,
            max_cost_usd=max_cost_usd,
            ttl_seconds=self._runner_registration_lease_ttl_seconds(),
        )
        env_vars["CLOUD_OFFLOAD_LEASE_ID"] = lease.id

        try:
            launch_arguments = dict(
                offer_id=offer["id"],
                docker_image=profile["image"],
                env_vars=env_vars,
                startup_script=startup_script,
                disk_gb=disk_gb,
                resource_name=lease.resource_name,
                **runtime_arguments,
            )
            if placement is not None:
                launch_arguments["placement"] = placement
            self._publish_launch_event(
                queued_jobs,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": "provider_request_started",
                    "phase": "provider_request",
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    "provider": provider_name,
                    "offer_id": offer["id"],
                    "placement": "cached" if placement and placement.storage_attachments else "cold",
                },
            )
            instance = connector.launch(**launch_arguments)
            self._publish_launch_event(
                queued_jobs,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": "provider_request_completed",
                    "phase": "provider_request",
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    "provider": provider_name,
                    "offer_id": offer["id"],
                    "worker_instance_id": instance.id,
                    "placement": "cached" if placement and placement.storage_attachments else "cold",
                },
            )
            return self._remember_launched_instance(
                instance, provider_name, profile_name, queued_jobs, lease.id
            )

        except Exception as e:
            self._publish_launch_event(
                queued_jobs,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": "provider_request_failed",
                    "phase": "provider_request",
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    "provider": provider_name,
                    "offer_id": offer["id"],
                    "failure": str(e),
                    "placement": "cached" if placement and placement.storage_attachments else "cold",
                },
            )
            logger.error(f"Failed to launch worker: {e}")
            recovered, provider_checked = self._recover_provisioning_lease(lease)
            if recovered is not None:
                self._publish_launch_event(
                    queued_jobs,
                    {
                        "type": "provider_request_recovered",
                        "phase": "provider_request",
                        "provider": provider_name,
                        "worker_instance_id": recovered.id,
                        "lease_id": lease.id,
                    },
                )
                return self._remember_launched_instance(
                    recovered, provider_name, profile_name, queued_jobs, lease.id
                )
            if not provider_checked:
                self._record_launch_failure(
                    provider_name,
                    profile_name,
                    queued_jobs,
                    "Provider launch result is uncertain; reconciliation will continue.",
                )
                return None
            if (
                placement is not None
                and placement_decision is not None
                and confirmed is None
                and self.config.prepared_storage.get("policy") == "smart"
                and self.config.prepared_storage.get("cold_fallback") == "allow"
            ):
                self._publish_launch_event(
                    queued_jobs,
                    {
                        "type": "cache_cold_fallback",
                        "reason": "cached_placement_launch_failed",
                        "failure": str(e),
                        **placement_decision.explanation(),
                    },
                )
                cold_offer = connector.find_cheapest(
                    gpu_type=gpu_type,
                    min_gpu_ram=minimum_vram,
                    max_hourly_rate=self.config.max_hourly_rate,
                    exclude=cooling,
                    **region_arguments,
                )
                if cold_offer:
                    cold_env = {
                        key: value
                        for key, value in env_vars.items()
                        if not key.startswith("CLOUD_OFFLOAD_CACHE_")
                    }
                    cold_lease = self.queue.create_lease(
                        provider=provider_name,
                        runtime_profile=profile_name,
                        job_ids=[job.id for job in (queued_jobs or [])],
                        hourly_rate=float(cold_offer.get("hourly_rate") or 0),
                        max_runtime_seconds=max_runtime_seconds,
                        max_cost_usd=max_cost_usd,
                        ttl_seconds=self._runner_registration_lease_ttl_seconds(),
                    )
                    cold_env["CLOUD_OFFLOAD_LEASE_ID"] = cold_lease.id
                    for queued_job in queued_jobs or []:
                        for key in (
                            "prepared_requirement",
                            "cache_volume_id",
                            "cache_provider_volume_id",
                            "cache_datacenter_id",
                            "cache_manifest_id",
                        ):
                            queued_job.params.pop(key, None)
                        self.queue.update(queued_job)
                    try:
                        self._publish_launch_event(
                            queued_jobs,
                            {
                                "schema": "cloud-offload.phase-event.v1",
                                "type": "provider_request_started",
                                "phase": "provider_request",
                                "monotonic_ms": round(time.monotonic() * 1000, 3),
                                "provider": provider_name,
                                "offer_id": cold_offer["id"],
                                "placement": "cold_fallback",
                            },
                        )
                        instance = connector.launch(
                            offer_id=cold_offer["id"],
                            docker_image=profile["image"],
                            env_vars=cold_env,
                            startup_script=startup_script,
                            disk_gb=disk_gb,
                            resource_name=cold_lease.resource_name,
                            **region_arguments,
                            **runtime_arguments,
                        )
                        self._publish_launch_event(
                            queued_jobs,
                            {
                                "schema": "cloud-offload.phase-event.v1",
                                "type": "provider_request_completed",
                                "phase": "provider_request",
                                "monotonic_ms": round(time.monotonic() * 1000, 3),
                                "provider": provider_name,
                                "offer_id": cold_offer["id"],
                                "worker_instance_id": instance.id,
                                "placement": "cold_fallback",
                            },
                        )
                        return self._remember_launched_instance(
                            instance,
                            provider_name,
                            profile_name,
                            queued_jobs,
                            cold_lease.id,
                        )
                    except Exception as cold_exc:
                        recovered, provider_checked = self._recover_provisioning_lease(
                            cold_lease
                        )
                        if recovered is not None:
                            return self._remember_launched_instance(
                                recovered,
                                provider_name,
                                profile_name,
                                queued_jobs,
                                cold_lease.id,
                            )
                        if not provider_checked:
                            self._record_launch_failure(
                                provider_name,
                                profile_name,
                                queued_jobs,
                                "Cold fallback result is uncertain; reconciliation will continue.",
                            )
                            return None
                        e = RuntimeError(
                            f"cached placement failed ({e}); cold fallback failed ({cold_exc})"
                        )
            self.offer_cooldowns[(provider_name, str(offer["id"]))] = (
                time.monotonic() + OFFER_COOLDOWN_SECONDS
            )
            logger.info(
                "Offer %s on cooldown for %ss after launch failure",
                offer["id"],
                OFFER_COOLDOWN_SECONDS,
            )
            self._record_launch_failure(
                provider_name, profile_name, queued_jobs, str(e)
            )
            return None

    def _remember_launched_instance(
        self,
        instance: Instance,
        provider_name: str,
        profile_name: str,
        queued_jobs: list | None,
        lease_id: str | None = None,
    ) -> Instance:
        # The provider may spend minutes polling for readiness after rental.
        # Anchor startup accounting to the durable pre-mutation lease timestamp
        # so fail-fast includes all billable time, including that wait.
        activity_at = utc_now()
        launched_at = activity_at
        if lease_id:
            bound_lease = self.queue.bind_lease(
                lease_id,
                instance.id,
                ttl_seconds=self._runner_registration_lease_ttl_seconds(),
            )
            self.instance_leases[instance.id] = lease_id
            try:
                launched_at = datetime.fromisoformat(bound_lease.created_at)
            except ValueError:
                pass
        self.active_instances[instance.id] = instance
        self.instance_providers[instance.id] = provider_name
        self.instance_profiles[instance.id] = profile_name
        self.last_activity[instance.id] = activity_at
        self.launched_at[instance.id] = launched_at
        logger.info("Launched worker %s", instance.id)
        self.launch_failures.pop((provider_name, profile_name), None)
        self.next_launch_at.pop((provider_name, profile_name), None)
        self._publish_launch_event(
            queued_jobs,
            {
                "type": "runner_starting",
                "provider": provider_name,
                "runtime_profile": profile_name,
                "worker_instance_id": instance.id,
                "gpu_type": instance.gpu_type,
                "hourly_rate": instance.hourly_rate,
                "lease_id": lease_id,
                "overall_progress": 2,
            },
        )
        return instance

    def _lease_limits(self, jobs: list | None) -> tuple[int, float | None]:
        runtime_limits = [int(self.config.max_job_runtime_seconds)]
        cost_limits: list[float] = []
        if self.config.max_total_job_cost is not None:
            cost_limits.append(float(self.config.max_total_job_cost))
        for job in jobs or []:
            preflight = job.params.get("preflight") or {}
            policy = preflight.get("request_policy") or {}
            try:
                runtime = int(policy.get("max_job_runtime_seconds") or 0)
                if runtime > 0:
                    runtime_limits.append(runtime)
            except (TypeError, ValueError):
                pass
            try:
                cost = float(policy.get("max_total_job_cost") or 0)
                if cost > 0:
                    cost_limits.append(cost)
            except (TypeError, ValueError):
                pass
        return min(runtime_limits), (min(cost_limits) if cost_limits else None)

    def _runner_registration_lease_ttl_seconds(self) -> int:
        """Keep provider ownership valid for the full first-runner start window."""
        return max(
            self.config.lease_ttl_seconds,
            RUNNER_REGISTRATION_TIMEOUT_SECONDS + self.config.poll_interval_seconds,
        )

    @staticmethod
    def _instance_resource_name(instance: Instance) -> str:
        candidate = getattr(instance, "metadata", None)
        metadata = candidate if isinstance(candidate, dict) else {}
        return str(metadata.get("name") or "")

    def _recover_provisioning_lease(
        self, lease: JobLease
    ) -> tuple[Instance | None, bool]:
        """Resolve an uncertain launch by its pre-mutation provider name."""
        connector = self.connectors.get(lease.provider)
        if connector is None:
            return None, False
        try:
            instances = connector.list_instances()
        except Exception as exc:
            logger.warning(
                "Could not reconcile provisioning lease %s: %s", lease.id, exc
            )
            return None, False
        recovered = next(
            (
                item
                for item in instances
                if self._instance_resource_name(item) == lease.resource_name
            ),
            None,
        )
        if recovered is not None:
            return recovered, True
        self.queue.close_unbound_lease(lease.id, "provider_confirmed_no_resource")
        return None, True

    def _forget_instance(self, instance_id: str) -> None:
        self.active_instances.pop(instance_id, None)
        self.instance_providers.pop(instance_id, None)
        self.instance_profiles.pop(instance_id, None)
        self.instance_leases.pop(instance_id, None)
        self.last_activity.pop(instance_id, None)
        self.launched_at.pop(instance_id, None)
        self.runner_ready_instances.discard(instance_id)
        self.runner_feedback_at.pop(instance_id, None)

    def _restore_lease_instance(self, lease: JobLease, instance: Instance) -> None:
        self.active_instances[instance.id] = instance
        self.instance_providers[instance.id] = lease.provider
        self.instance_profiles[instance.id] = lease.runtime_profile
        self.instance_leases[instance.id] = lease.id
        try:
            created = datetime.fromisoformat(lease.created_at)
        except ValueError:
            created = utc_now()
        self.launched_at.setdefault(instance.id, created)
        self.last_activity.setdefault(instance.id, created)

    def _fail_lease_jobs(self, lease: JobLease, reason: str) -> None:
        safe_reason = str(reason).replace("_", " ")
        for job in self.queue.jobs_for_lease(lease.id):
            if job.status not in {
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.DEAD_LETTER,
            }:
                self.queue.append_event(
                    job.id,
                    {
                        "type": "circuit_breaker_triggered",
                        "phase": "resource_closure",
                        "lease_id": lease.id,
                        "provider": lease.provider,
                        "worker_instance_id": lease.instance_id,
                        "reason": reason,
                    },
                    producer_id="dispatcher:lease-control",
                )
                self.queue.update_status(
                    job.id,
                    JobStatus.FAILED,
                    error=f"Cancelled: {safe_reason}",
                )

    def _requeue_lost_lease_jobs(self, lease: JobLease) -> None:
        for job in self.queue.jobs_for_lease(lease.id):
            if job.status not in {
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.DEAD_LETTER,
            }:
                self.queue.append_event(
                    job.id,
                    {
                        "type": "provider_resource_lost",
                        "phase": "resource_closure",
                        "lease_id": lease.id,
                        "provider": lease.provider,
                        "worker_instance_id": lease.instance_id,
                    },
                    producer_id="dispatcher:lease-control",
                )
                self.queue.fail_job(job.id, "Provider resource ended before completion")

    def _terminate_lease(self, lease: JobLease) -> None:
        if not lease.instance_id:
            return
        connector = self.connectors.get(lease.provider)
        if connector is None:
            logger.error("No connector can close lease %s", lease.id)
            return
        attempted = self.queue.record_termination_attempt(lease.id)
        try:
            connector.terminate(str(lease.instance_id))
        except Exception as exc:
            logger.warning(
                "Provider termination attempt %s failed for lease %s: %s",
                attempted.termination_attempts,
                lease.id,
                exc,
            )
        try:
            observed = connector.get_instance(str(lease.instance_id))
        except Exception as exc:
            logger.warning("Could not verify closure for lease %s: %s", lease.id, exc)
            return
        if observed is None:
            self.queue.confirm_lease_termination(
                lease.id, observed_state="absent", provider_absent=True
            )
            self._forget_instance(str(lease.instance_id))
        elif observed.status == "terminated":
            self.queue.confirm_lease_termination(
                lease.id,
                observed_state=observed.status,
                provider_absent=False,
            )
            self._forget_instance(str(lease.instance_id))

    def _reconcile_leases(self) -> None:
        """Rebuild ownership and close every expired or revoked paid resource."""
        now = utc_now()
        for initial in self.queue.list_open_leases():
            lease = self.queue.get_lease(initial.id) or initial
            connector = self.connectors.get(lease.provider)
            if connector is None:
                logger.error("No connector is configured for open lease %s", lease.id)
                continue
            instance = None
            if lease.instance_id:
                try:
                    instance = connector.get_instance(lease.instance_id)
                except Exception as exc:
                    logger.warning("Could not inspect lease %s: %s", lease.id, exc)
                    continue
            else:
                instance, checked = self._recover_provisioning_lease(lease)
                if not checked:
                    continue
                if instance is None:
                    continue
                lease = self.queue.bind_lease(
                    lease.id,
                    instance.id,
                    ttl_seconds=self._runner_registration_lease_ttl_seconds(),
                )

            if instance is None or instance.status == "terminated":
                if lease.status == "active" and not lease.termination_requested_at:
                    self._requeue_lost_lease_jobs(lease)
                self.queue.confirm_lease_termination(
                    lease.id,
                    observed_state="absent" if instance is None else instance.status,
                    provider_absent=instance is None,
                )
                if lease.instance_id:
                    self._forget_instance(lease.instance_id)
                continue

            self._restore_lease_instance(lease, instance)
            reason = None
            if lease.status in {"revocation_requested", "terminating"}:
                reason = lease.reason or "lease_revoked"
            elif instance.status == "stopped":
                reason = "provider_stopped_resource_remains"
            else:
                try:
                    if datetime.fromisoformat(lease.expires_at) <= now:
                        reason = "lease_expired"
                    elif lease.runtime_deadline and datetime.fromisoformat(
                        lease.runtime_deadline
                    ) <= now:
                        reason = "runtime_limit"
                    elif lease.cost_deadline and datetime.fromisoformat(
                        lease.cost_deadline
                    ) <= now:
                        reason = "cost_limit"
                except ValueError:
                    reason = "invalid_lease_deadline"
            if reason:
                if lease.status not in {"revocation_requested", "terminating"}:
                    lease = self.queue.request_lease_revocation(lease.id, reason)
                    self._fail_lease_jobs(lease, reason)
                self._terminate_lease(lease)

    @staticmethod
    def _shared_preflight(jobs: list | None) -> dict | None:
        entries = [
            job.params.get("preflight")
            for job in (jobs or [])
            if isinstance(job.params.get("preflight"), dict)
        ]
        if not entries:
            return None
        candidate_ids = {str(item.get("candidate_id") or "") for item in entries}
        if len(candidate_ids) != 1 or "" in candidate_ids:
            return None
        return entries[0]

    @staticmethod
    def _preflight_entries(jobs: list | None) -> list[dict]:
        return [
            item
            for job in (jobs or [])
            if isinstance(item := job.params.get("preflight"), dict)
        ]

    def _launch_region_constraints(
        self, confirmations: list[dict]
    ) -> PlacementConstraints | None:
        """Carry the intersection of current policy and confirmed locality to launch."""
        allowed = set(self.config.allowed_regions) or None
        for confirmed in confirmations:
            policy_regions = (confirmed.get("request_policy") or {}).get("allowed_regions") or []
            region = confirmed.get("region")
            constraints = [set(policy_regions)] if policy_regions else []
            if region and region not in {"auto", "unknown"}:
                constraints.append({region})
            for regions in constraints:
                allowed = regions if allowed is None else allowed & regions
                if not allowed:
                    raise ValueError("The confirmed region conflicts with the allowed regions.")
        return PlacementConstraints(datacenter_ids=tuple(sorted(allowed))) if allowed else None

    def _confirmed_offers_allowed(
        self, confirmations: list[dict], offer: dict
    ) -> bool:
        """Ensure every queued confirmation permits the exact live offer."""
        try:
            live_rate = float(offer.get("hourly_rate"))
        except (TypeError, ValueError):
            return False
        for confirmed in confirmations:
            if not self._confirmed_offer_matches(confirmed, offer):
                return False
            policy = confirmed.get("request_policy") or {}
            try:
                ceiling = float(
                    policy.get("max_hourly_rate", self.config.max_hourly_rate)
                )
            except (TypeError, ValueError):
                return False
            if live_rate > ceiling:
                return False
        return True

    @staticmethod
    def _confirmed_offer_matches(confirmed: dict, offer: dict) -> bool:
        try:
            return (
                str(offer.get("id")) == str(confirmed.get("offer_id"))
                and str(offer.get("gpu_type")) == str(confirmed.get("gpu_type"))
                and float(offer.get("gpu_ram_gb") or 0)
                == float(confirmed.get("gpu_ram_gb") or 0)
                and math.isclose(
                    float(offer.get("hourly_rate")),
                    float(confirmed.get("hourly_rate")),
                    rel_tol=0,
                    abs_tol=1e-9,
                )
            )
        except (TypeError, ValueError):
            return False

    def _confirmed_cache_placement(
        self,
        *,
        connector: CloudConnector,
        provider_name: str,
        gpu_type: str,
        minimum_vram: int,
        cooling: set[str],
        requirements: dict,
        confirmed: dict,
    ) -> PlacementDecision:
        if not self.config.prepared_storage.get("enabled"):
            return PlacementDecision(
                "unavailable", None, "confirmed_prepared_storage_disabled", ()
            )
        volume = self.cache_registry.get_volume(
            str(confirmed.get("prepared_volume_id") or "")
        )
        if (
            volume is None
            or volume.status != "ready"
            or volume.provider != provider_name
            or volume.datacenter_id != confirmed.get("region")
        ):
            return PlacementDecision(
                "unavailable", None, "confirmed_prepared_volume_changed", ()
            )
        confirmed_provider_volume_id = confirmed.get("prepared_provider_volume_id")
        # New confirmations carry the exact provider identity.  Development
        # jobs created before that field existed are re-proved against the
        # current registry row for compatibility; they never accept a caller
        # supplied replacement.
        if confirmed_provider_volume_id is not None:
            if (
                not isinstance(confirmed_provider_volume_id, str)
                or not confirmed_provider_volume_id.strip()
                or volume.provider_volume_id != confirmed_provider_volume_id
            ):
                return PlacementDecision(
                    "unavailable", None, "confirmed_prepared_provider_volume_changed", ()
                )
        if volume.provider != provider_name:
            return PlacementDecision(
                "unavailable", None, "confirmed_prepared_provider_volume_changed", ()
            )
        confirmed_storage = confirmed.get("storage")
        if confirmed_storage is not None:
            expected_storage = {
                "region": volume.datacenter_id,
                "persistent": True,
                "storage_id": volume.id,
            }
            if confirmed_storage != expected_storage:
                return PlacementDecision(
                    "unavailable", None, "confirmed_prepared_storage_changed", ()
                )
        policy = str(self.config.prepared_storage.get("policy") or "")
        bound = str(self.config.prepared_storage.get("existing_volume_id") or "")
        if (
            policy in {"strict", "pinned"}
            and bound
            and bound != volume.provider_volume_id
        ):
            if (
                confirmed.get("prepared_provider_volume_id") is not None
                and confirmed.get("prepared_provider_volume_id")
                != volume.provider_volume_id
            ):
                return PlacementDecision(
                    "unavailable", None, "confirmed_prepared_provider_volume_changed", ()
                )
            # Strict placement means "the configured volume decides". A quote
            # confirmed against a volume the config no longer binds must not
            # launch — unless the binding names a registered volume in another
            # datacenter, which cannot serve this region at all: the regional
            # confirmed volume then remains the only strict answer.
            bound_volume = self.cache_registry.get_provider_volume(
                provider_name, bound
            )
            try:
                bound_actual = (
                    connector.get_storage(bound) if bound_volume is not None else None
                )
            except Exception:
                bound_actual = None
            if (
                bound_volume is None
                or bound_actual is None
                or bound_actual.datacenter_id != bound_volume.datacenter_id
                or bound_volume.datacenter_id == str(confirmed.get("region") or "")
            ):
                return PlacementDecision(
                    "unavailable", None, "confirmed_prepared_binding_changed", ()
                )
        try:
            actual = connector.get_storage(volume.provider_volume_id)
        except Exception:
            actual = None
        if (
            actual is None
            or not isinstance(volume.provider_volume_id, str)
            or not volume.provider_volume_id.strip()
            or getattr(actual, "id", None) != volume.provider_volume_id
            or getattr(actual, "provider", None) != provider_name
            or getattr(actual, "datacenter_id", None) != volume.datacenter_id
        ):
            return PlacementDecision(
                "unavailable", None, "confirmed_provider_volume_unavailable", ()
            )
        placement = PlacementConstraints(
            datacenter_ids=(volume.datacenter_id,),
            storage_attachments=(
                StorageAttachment(
                    provider_volume_id=volume.provider_volume_id,
                    mount_path="/workspace",
                    datacenter_id=volume.datacenter_id,
                ),
            ),
        )
        try:
            offers = connector.list_available(
                gpu_type=gpu_type,
                min_gpu_ram=minimum_vram,
                max_hourly_rate=float(
                    (confirmed.get("request_policy") or {}).get(
                        "max_hourly_rate", self.config.max_hourly_rate
                    )
                ),
                placement=placement,
            )
        except Exception:
            offers = []
        offer = next(
            (
                item
                for item in offers
                if str(item.get("id")) == str(confirmed.get("offer_id"))
                and str(item.get("id")) not in cooling
            ),
            None,
        )
        if not offer or not self._confirmed_offer_matches(confirmed, offer):
            return PlacementDecision(
                "unavailable", None, "confirmed_offer_changed", ()
            )
        runtime = scheduler_runtime(requirements)
        coverage = next(
            (
                item
                for item in self.cache_registry.volume_coverage(
                    requirements["required"],
                    runtime=runtime,
                    tenant=str(
                        self.config.prepared_storage.get("tenant") or "default"
                    ),
                    profile_fingerprint=str(requirements["profile_fingerprint"]),
                    allow_private=bool(
                        self.config.prepared_storage.get("cache_private_assets")
                    ),
                    logical_required=requirements.get("logical_required") or [],
                )
                if item["volume"].id == volume.id
            ),
            None,
        ) or {
            "cached_bytes": 0,
            "required_bytes": sum(requirements["required"].values()),
            "complete": False,
            "manifest_ids": [],
        }
        candidate = PlacementCandidate(
            offer=offer,
            volume=volume,
            cached_bytes=int(coverage["cached_bytes"]),
            required_bytes=int(coverage["required_bytes"]),
            complete=bool(coverage["complete"]),
            manifest_ids=tuple(coverage["manifest_ids"]),
        )
        return PlacementDecision(
            "launch", candidate, "confirmed_preflight_candidate", ()
        )

    def _refuse_preflight_launch(self, jobs: list | None, reason: str) -> None:
        self._publish_launch_event(
            jobs,
            {
                "type": "preflight_confirmation_required",
                "phase": "preflight",
                "reason": reason,
                "overall_progress": 0,
            },
        )
        for job in jobs or []:
            self.queue.update_status(
                job.id,
                JobStatus.FAILED,
                error=f"Preflight confirmation required: {reason}",
            )

    def _choose_cache_placement(
        self,
        *,
        connector: CloudConnector,
        provider_name: str,
        gpu_type: str,
        minimum_vram: int,
        cooling: set[str],
        requirements: dict,
        region_placement: PlacementConstraints | None = None,
    ):
        policy = self.config.prepared_storage
        existing = policy.get("existing_volume_id")
        region_arguments = {"placement": region_placement} if region_placement else {}

        def storage_failure(reason: str) -> PlacementDecision:
            if (
                policy.get("policy") == "smart"
                and policy.get("cold_fallback") == "allow"
            ):
                cold = [
                    item
                    for item in connector.list_available(
                        gpu_type=gpu_type,
                        min_gpu_ram=minimum_vram,
                        max_hourly_rate=self.config.max_hourly_rate,
                        **region_arguments,
                    )
                    if str(item.get("id")) not in cooling
                ]
                if cold:
                    offer = min(
                        cold,
                        key=lambda item: (
                            float(item.get("hourly_rate", float("inf"))),
                            str(item.get("id") or ""),
                        ),
                    )
                    return PlacementDecision(
                        "launch",
                        PlacementCandidate(offer, None),
                        f"{reason}_running_cold",
                        (),
                        fallback=True,
                    )
            return PlacementDecision("unavailable", None, reason, ())

        try:
            # Provider truth is rechecked before every cached placement. A
            # deleted or moved adopted volume is removed from scheduling before
            # Pod creation, not discovered after billing starts.
            for registered in self.cache_registry.list_volumes():
                if registered.provider != provider_name:
                    continue
                actual = connector.get_storage(registered.provider_volume_id)
                if actual is None or actual.datacenter_id != registered.datacenter_id:
                    self.cache_registry.mark_volume(registered.id, "degraded")
                    if registered.provider_volume_id == existing:
                        return storage_failure(
                            "configured_cache_volume_not_found"
                            if actual is None
                            else "configured_cache_volume_wrong_datacenter"
                        )

            if existing and not self.cache_registry.get_provider_volume(
                provider_name, existing
            ):
                provider_volume = connector.get_storage(existing)
                if provider_volume is None:
                    return storage_failure("configured_cache_volume_not_found")
                if policy.get("region") not in {"auto", provider_volume.datacenter_id}:
                    return storage_failure("configured_cache_volume_wrong_datacenter")
                self.cache_registry.upsert_volume(
                    provider=provider_name,
                    provider_volume_id=provider_volume.id,
                    datacenter_id=provider_volume.datacenter_id,
                    ownership="adopted",
                    capacity_bytes=provider_volume.size_gb * 1024**3,
                    policy=policy,
                    s3_compatible=provider_volume.s3_compatible,
                )

            ready = [
                item
                for item in self.cache_registry.list_volumes(status="ready")
                if item.provider == provider_name
            ]
            if not ready and not existing:
                region = str(policy.get("region") or "auto")
                if region_placement and region != "auto" and region not in region_placement.datacenter_ids:
                    return storage_failure("managed_cache_region_outside_allowed_regions")
                if region == "auto":
                    # RunPod's aggregate GPU-type API cannot prove a specific
                    # datacenter has capacity. Region auto therefore becomes an
                    # actionable one-time decision rather than silently creating
                    # paid, stranded storage or running statelessly.
                    return PlacementDecision(
                        "ask", None, "managed_cache_region_selection_required", ()
                    )
                size_gb = int(policy.get("managed_size_gb") or 250)
                monthly = estimate_runpod_storage_monthly(size_gb)
                budget = policy.get("max_monthly_storage_cost")
                if budget is not None and monthly > float(budget):
                    return PlacementDecision(
                        "unavailable", None, "managed_cache_exceeds_storage_budget", ()
                    )
                provider_volume = connector.create_storage(
                    name=f"cloud-offload-{region.lower()}",
                    size_gb=size_gb,
                    datacenter_id=region,
                )
                self.cache_registry.upsert_volume(
                    provider=provider_name,
                    provider_volume_id=provider_volume.id,
                    datacenter_id=provider_volume.datacenter_id,
                    ownership="managed",
                    capacity_bytes=provider_volume.size_gb * 1024**3,
                    policy=policy,
                    status="ready",
                    s3_compatible=provider_volume.s3_compatible,
                )
        except Exception as exc:
            logger.warning("Prepared storage lifecycle failed: %s", exc)
            return storage_failure(f"prepared_storage_lifecycle_failed: {exc}")

        runtime = scheduler_runtime(requirements)
        coverages = {
            item["volume"].id: item
            for item in self.cache_registry.volume_coverage(
                requirements["required"],
                runtime=runtime,
                tenant=str(policy.get("tenant") or "default"),
                profile_fingerprint=str(requirements["profile_fingerprint"]),
                allow_private=bool(policy.get("cache_private_assets")),
                logical_required=requirements.get("logical_required") or [],
            )
            if item["volume"].provider == provider_name
        }
        cached: list[PlacementCandidate] = []
        for volume in self.cache_registry.list_volumes(status="ready"):
            if volume.provider != provider_name:
                continue
            if region_placement and volume.datacenter_id not in region_placement.datacenter_ids:
                continue
            if policy.get("policy") == "pinned" and volume.datacenter_id != policy.get(
                "region"
            ):
                continue
            constraints = PlacementConstraints(
                datacenter_ids=(volume.datacenter_id,),
                storage_attachments=(
                    StorageAttachment(
                        provider_volume_id=volume.provider_volume_id,
                        mount_path="/workspace",
                        datacenter_id=volume.datacenter_id,
                    ),
                ),
            )
            try:
                offers = connector.list_available(
                    gpu_type=gpu_type,
                    min_gpu_ram=minimum_vram,
                    max_hourly_rate=self.config.max_hourly_rate,
                    placement=constraints,
                )
            except Exception as exc:
                logger.warning(
                    "Prepared placement %s/%s unavailable: %s",
                    volume.datacenter_id,
                    volume.id,
                    exc,
                )
                continue
            coverage = coverages.get(volume.id) or {
                "cached_bytes": 0,
                "required_bytes": sum(requirements["required"].values()),
                "complete": not requirements["required"],
                "manifest_ids": [],
            }
            for offer in offers:
                if str(offer.get("id")) in cooling:
                    continue
                cached.append(
                    PlacementCandidate(
                        offer=offer,
                        volume=volume,
                        cached_bytes=int(coverage["cached_bytes"]),
                        required_bytes=int(coverage["required_bytes"]),
                        complete=bool(coverage["complete"]),
                        manifest_ids=tuple(coverage["manifest_ids"]),
                    )
                )
        cold = [
            offer
            for offer in connector.list_available(
                gpu_type=gpu_type,
                min_gpu_ram=minimum_vram,
                max_hourly_rate=self.config.max_hourly_rate,
                **region_arguments,
            )
            if str(offer.get("id")) not in cooling
        ]
        return choose_placement(
            policy=policy, cached_candidates=cached, cold_offers=cold
        )

    def _planned_disk_gb(self, profile_name: str, queued_jobs: list | None) -> int:
        """The container disk to rent: the configured floor, or a job's plan if larger.

        The coordinator sizes a partition's storage at submission and stamps the
        answer onto the job, so this is the number that keeps a pod from dying
        out of disk after the meter has started. A job queued before storage
        planning existed carries no figure and gets exactly the configured value
        it would have got before.
        """
        configured = int(self.config.runpod_container_disk_gb)
        planned = max(
            [0]
            + [
                int(job.params.get("container_disk_gb") or 0)
                for job in (queued_jobs or [])
            ]
        )
        if planned <= configured:
            return configured
        logger.info(
            "Renting %s GB of container disk for profile %s: the storage plan for "
            "its queued partitions needs more than the configured %s GB",
            planned,
            profile_name,
            configured,
        )
        return planned

    def _offers_on_cooldown(self, provider_name: str) -> set[str]:
        """Offer ids currently sitting out after a failed launch on this provider."""
        now = time.monotonic()
        for key in [k for k, until in self.offer_cooldowns.items() if until <= now]:
            del self.offer_cooldowns[key]
        return {
            offer_id
            for (name, offer_id) in self.offer_cooldowns
            if name == provider_name
        }

    def _publish_launch_event(self, jobs: list | None, event: dict) -> None:
        self.event_producer_sequence += 1
        for job in jobs or []:
            try:
                self.queue.append_event(
                    job.id,
                    event,
                    producer_id=self.event_producer_id,
                    producer_sequence=self.event_producer_sequence,
                )
            except (KeyError, ValueError):
                logger.debug("Could not append provisioning event for %s", job.id)

    def _record_launch_failure(
        self,
        provider_name: str,
        profile_name: str,
        jobs: list | None,
        error: str,
    ) -> None:
        key = (provider_name, profile_name)
        failures = self.launch_failures.get(key, 0) + 1
        self.launch_failures[key] = failures
        retry_seconds = min(300, 10 * (2 ** min(failures - 1, 5)))
        self.next_launch_at[key] = time.monotonic() + retry_seconds
        self._publish_launch_event(
            jobs,
            {
                "type": "provisioning_failed",
                "provider": provider_name,
                "runtime_profile": profile_name,
                "error": error,
                "retry_seconds": retry_seconds,
                "attempt": failures,
                "overall_progress": 0,
            },
        )

    def _build_startup_script(self, profile: dict | None = None) -> str | None:
        """Build the startup script that runs on the cloud instance."""
        wheelhouse_url = (profile or {}).get(
            "wheelhouse_url"
        ) or self.config.worker_wheelhouse_url
        wheelhouse_sha256 = (profile or {}).get(
            "wheelhouse_sha256"
        ) or self.config.worker_wheelhouse_sha256
        if profile and not wheelhouse_url and not wheelhouse_sha256:
            # Immutable runtime-profile images define their own worker ENTRYPOINT.
            # RunPod's dockerArgs field replaces CMD but is appended to ENTRYPOINT,
            # so passing a shell script here would produce a command such as
            # ``cloud-offload worker bash -lc ...`` and make the container exit
            # immediately.
            return None
        if not wheelhouse_url or not wheelhouse_sha256:
            raise RuntimeError(
                "Cloud workers require CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL and "
                "CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256; live registry installs are disabled"
            )
        return """#!/bin/bash
set -e

mkdir -p /opt/cloud-offload-wheelhouse
curl -fsSL "$CLOUD_OFFLOAD_WORKER_WHEELHOUSE_URL" -o /tmp/cloud-offload-wheelhouse.tar.gz
echo "$CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256  /tmp/cloud-offload-wheelhouse.tar.gz" | sha256sum -c -
tar -xzf /tmp/cloud-offload-wheelhouse.tar.gz -C /opt/cloud-offload-wheelhouse

python -m pip install --no-index --find-links /opt/cloud-offload-wheelhouse "cloud-offload[cloud]"

# Run worker
cloud-offload worker --poll 10
"""

    def _update_workers(self):
        """Update status of active workers."""
        for instance_id in list(self.active_instances.keys()):
            provider_name = self.instance_providers[instance_id]
            instance = self.connectors[provider_name].get_instance(instance_id)

            if not instance or instance.status == "terminated":
                logger.info(f"Worker {instance_id} no longer active")
                lease_id = self.instance_leases.get(instance_id)
                if lease_id:
                    lease = self.queue.get_lease(lease_id)
                    if (
                        lease
                        and lease.status == "active"
                        and not lease.termination_requested_at
                    ):
                        self._requeue_lost_lease_jobs(lease)
                    self.queue.confirm_lease_termination(
                        lease_id,
                        observed_state="absent" if not instance else instance.status,
                        provider_absent=not instance,
                    )
                self._forget_instance(instance_id)
            elif instance.status == "stopped" and self.instance_leases.get(instance_id):
                lease = self.queue.request_lease_revocation(
                    self.instance_leases[instance_id],
                    "provider_stopped_resource_remains",
                )
                self._terminate_lease(lease)
            else:
                self.active_instances[instance_id] = instance

    def _check_unregistered_workers(self):
        """Stop a paid instance whose runner never managed to call home."""
        now = utc_now()
        workers = self.queue.list_active_workers()
        profiles = configured_worker_profiles(self.config)
        for instance_id, launched_at in list(self.launched_at.items()):
            provider_name = self.instance_providers[instance_id]
            profile_name = self.instance_profiles[instance_id]

            def registered_after_launch(worker: dict) -> bool:
                if (
                    worker.get("provider") != provider_name
                    or worker.get("runtime_profile") != profile_name
                ):
                    return False
                try:
                    seen = datetime.fromisoformat(
                        str(worker.get("last_seen") or "").replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                except ValueError:
                    return False
                return seen >= launched_at

            registered = any(registered_after_launch(worker) for worker in workers)
            if registered:
                self.runner_feedback_at.pop(instance_id, None)
                continue

            queued_jobs = [
                job
                for job in self.queue.list_by_status(
                    JobStatus.QUEUED, provider=provider_name
                )
                if self._job_profile_name(job, profiles) == profile_name
            ]
            instance = self.active_instances.get(instance_id)
            container_telemetry = (
                self.connectors[provider_name].container_started(instance)
                if instance is not None
                else None
            )
            last_feedback = self.runner_feedback_at.get(instance_id)
            if queued_jobs and (
                last_feedback is None or now - last_feedback >= timedelta(seconds=10)
            ):
                elapsed = round((now - launched_at).total_seconds(), 1)
                raw_metadata = getattr(instance, "metadata", None)
                metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
                provider_state = str(
                    metadata.get("provider_state")
                    or (instance.status if instance is not None else "unknown")
                    or "unknown"
                ).upper()
                if provider_state not in {
                    "PROVISIONING",
                    "STARTING",
                    "RUNNING",
                    "EXITED",
                    "ERROR",
                    "TERMINATED",
                    "UNKNOWN",
                }:
                    provider_state = "UNKNOWN"
                container_state = (
                    "confirmed"
                    if container_telemetry is True
                    else "not_started"
                    if container_telemetry is False
                    else "unknown"
                )
                self._publish_launch_event(
                    queued_jobs,
                    {
                        "schema": "cloud-offload.phase-event.v1",
                        "type": "provider_startup_observation",
                        "phase": "runner_starting",
                        "worker_instance_id": instance_id,
                        "elapsed_seconds": elapsed,
                        "startup_phases": {
                            "allocation": {
                                "state": "confirmed",
                                "provider_state": provider_state,
                            },
                            # RunPod REST v2 does not publish an authoritative
                            # image-pull state. Keep the gap explicit.
                            "image_pull": {"state": "unknown"},
                            "container_start": {"state": container_state},
                            "runner_callback": {"state": "unknown"},
                            "comfyui_readiness": {"state": "unknown"},
                        },
                    },
                )
                self._publish_launch_event(
                    queued_jobs,
                    {
                        "schema": "cloud-offload.phase-event.v1",
                        "type": "runner_starting_progress",
                        "phase": "runner_starting",
                        "worker_instance_id": instance_id,
                        "elapsed_seconds": elapsed,
                        "overall_progress": 2,
                        "message": "Waiting for runner callback and ComfyUI readiness",
                    },
                )
                self.runner_feedback_at[instance_id] = now

            if (
                instance is not None
                and container_telemetry is False
                and now - launched_at
                > timedelta(seconds=RUNNER_CONTAINER_START_TIMEOUT_SECONDS)
            ):
                detail = (
                    "Provider never started the container within "
                    f"{RUNNER_CONTAINER_START_TIMEOUT_SECONDS}s of rental"
                )
                logger.error("Worker %s %s; terminating", instance_id, detail.lower())
                self._record_launch_failure(
                    provider_name, profile_name, queued_jobs, detail
                )
                self._terminate_worker(instance_id, reason="container_start_timeout")
                continue

            if now - launched_at <= timedelta(
                seconds=RUNNER_REGISTRATION_TIMEOUT_SECONDS
            ):
                continue

            detail = (
                f"Runner did not register within {RUNNER_REGISTRATION_TIMEOUT_SECONDS}s"
            )
            logger.error("Worker %s %s; terminating", instance_id, detail.lower())
            self._record_launch_failure(
                provider_name, profile_name, queued_jobs, detail
            )
            self._terminate_worker(instance_id, reason="registration_timeout")

    @staticmethod
    def _job_profile_name(job, profiles: dict) -> str:
        """The worker profile a job resolves to, resolved the way routing does.

        Jobs carry the capability a client stamped — ``comfyui-partition-v1`` —
        not an operator's profile name. Comparing the raw strings matched
        nothing, so a pod could be terminated as idle while the queue held the
        very work it had been rented for.
        """
        requested = str(job.params.get("runtime_profile") or "")
        resolved = profiles.get(requested) or profile_providing(profiles, requested)
        return resolved["name"] if resolved else requested

    def _check_idle_workers(self):
        """Terminate workers that have been idle too long."""
        if self.config.keep_warm:
            return
        # Check for idle workers
        idle_threshold = timedelta(seconds=self.config.idle_shutdown_seconds)
        now = utc_now()

        workers = self.queue.list_active_workers()
        profiles = configured_worker_profiles(self.config)
        for instance_id, last_active in list(self.last_activity.items()):
            provider_name = self.instance_providers[instance_id]
            profile_name = self.instance_profiles[instance_id]
            matching_workers = [
                worker
                for worker in workers
                if worker.get("provider") == provider_name
                and worker.get("runtime_profile") == profile_name
            ]
            if any(
                worker.get("status") not in {"starting", "failed"}
                for worker in matching_workers
            ):
                self.runner_ready_instances.add(instance_id)
            # A runner that has said it is still starting is not idle, it is
            # busy importing ComfyUI. Terminating it on the idle clock is how a
            # pod gets killed mid-boot and the whole rental paid for nothing.
            # This protection applies only to the first boot. A provider can
            # restart a container after its worker idle timer exits. That
            # restart must not reset the paid resource's dispatcher idle clock.
            starting = any(
                worker.get("status") == "starting" for worker in matching_workers
            ) and instance_id not in self.runner_ready_instances
            running_jobs = sum(
                self._job_profile_name(job, profiles) == profile_name
                for job in self.queue.list_by_status(
                    JobStatus.RUNNING,
                    JobStatus.DISPATCHED,
                    provider=provider_name,
                )
            )
            queued_jobs = sum(
                self._job_profile_name(job, profiles) == profile_name
                for job in self.queue.list_by_status(
                    JobStatus.QUEUED, provider=provider_name
                )
            )
            if starting or running_jobs > 0 or queued_jobs > 0:
                self.last_activity[instance_id] = now
                continue
            if now - last_active > idle_threshold:
                logger.info(
                    f"Worker {instance_id} idle for "
                    f"{self.config.idle_shutdown_seconds}s, terminating"
                )
                self._terminate_worker(instance_id, reason="idle_timeout")

    def _terminate_worker(self, instance_id: str, *, reason: str = "dispatcher_shutdown"):
        """Terminate a worker instance."""
        lease_id = self.instance_leases.get(instance_id)
        if lease_id:
            lease = self.queue.request_lease_revocation(lease_id, reason)
            self._terminate_lease(lease)
            return
        provider_name = self.instance_providers[instance_id]
        if self.connectors[provider_name].terminate(instance_id):
            logger.info(f"Terminated worker {instance_id}")
        else:
            logger.warning(f"Failed to terminate worker {instance_id}")

        self._forget_instance(instance_id)

    def shutdown(self):
        """Terminate all workers and shut down."""
        logger.info("Dispatcher shutting down, terminating workers...")
        for instance_id in list(self.active_instances.keys()):
            self._terminate_worker(instance_id)
        if self._tunnel is not None:
            self._tunnel.close()
            self._tunnel = None

    def status(self) -> dict:
        """Get dispatcher status."""
        return {
            "queued_jobs": self.queue.count_by_status(JobStatus.QUEUED),
            "running_jobs": self.queue.count_by_status(JobStatus.RUNNING),
            "dead_letter_jobs": self.queue.count_by_status(JobStatus.DEAD_LETTER),
            "active_workers": len(self.active_instances),
            "workers": [
                {
                    "id": inst.id,
                    "gpu": inst.gpu_type,
                    "hourly_rate": inst.hourly_rate,
                    "status": inst.status,
                    "provider": self.instance_providers.get(inst.id),
                    "runtime_profile": self.instance_profiles.get(inst.id),
                }
                for inst in self.active_instances.values()
            ],
            "config": {
                "min_queue_depth": self.config.min_queue_depth,
                "provider": self.config.provider,
                "max_hourly_rate": self.config.max_hourly_rate,
            },
        }
