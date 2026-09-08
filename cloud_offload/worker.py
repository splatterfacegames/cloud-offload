"""
Worker - runs on cloud instances, processes jobs from the queue.

Claims ComfyUI jobs, runs the colocated headless ComfyUI executor, and
publishes results/artifacts back to the coordinator. The worker never loads a
3D model: generation rides inside the submitted subgraph.
"""

import base64
import hashlib
import logging
import os
import platform as platform_module
import shutil
import signal
import subprocess
import sys
import threading
import time
import tempfile
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath

from cloud_offload.config import CloudConfig
from cloud_offload.queue import Job, JobQueue, JobStatus, utc_now
from cloud_offload.storage import Storage, create_storage
from cloud_offload.profiles import WORKFLOW_CAPABILITIES, load_worker_manifest

logger = logging.getLogger(__name__)

DEFAULT_PARTITION_ROOT = "/opt/cloud-offload/partitions"
DEFAULT_ENVIRONMENT_ROOT = "/opt/cloud-offload/environment"
# Where a model file whose bytes contradict the job's manifest is set aside.
# Quarantine rather than overwrite: it preserves the evidence, and it makes
# "same filename, different weights" a loud event instead of a silent one.
QUARANTINE_DIRNAME = ".cloud-offload-quarantine"

# The Comfy Registry, which resolves a pack id and version to a download URL.
DEFAULT_REGISTRY_URL = "https://api.comfy.org"
REGISTRY_TIMEOUT_SECONDS = 60
GIT_TIMEOUT_SECONDS = 600
PIP_TIMEOUT_SECONDS = 1800
# Enough to see what pip actually did without turning one failed build into an
# event nobody can page through.
MAX_CAPTURED_OUTPUT = 4000


def resolve_worker_id(explicit: str | None = None) -> str:
    """The identity a runner answers to, shared by every phase of its startup.

    A runner boots in more than one process: something registers and stages the
    node packs before ComfyUI exists, and something else claims jobs once it
    does. They must be the same worker to the coordinator, or a startup failure
    is attributed to a worker nothing else ever mentions, so the id is taken
    from the environment when the entrypoint has already chosen one.
    """
    import os

    return (
        explicit
        or os.environ.get("CLOUD_OFFLOAD_WORKER_ID", "").strip()
        or f"worker-{uuid.uuid4().hex[:8]}"
    )


def worker_queue(config: CloudConfig, worker_id: str, lease_id: str | None = None):
    """The queue channel a runner with this configuration talks to."""
    if config.coordinator_url:
        from cloud_offload.coordinator import CoordinatorQueue

        if not config.worker_token:
            raise ValueError(
                "CLOUD_OFFLOAD_WORKER_TOKEN is required with CLOUD_OFFLOAD_COORDINATOR_URL"
            )
        return CoordinatorQueue(
            config.coordinator_url,
            config.worker_token,
            config.provider,
            worker_id,
            lease_id,
        )
    return JobQueue(config.queue_db_path)


def sha256_file(path: Path) -> str:
    """Digest a file in bounded memory. Never opens it as anything but bytes."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Worker:
    """
    Cloud worker that processes jobs from the queue.

    Lifecycle:
    1. Poll queue for jobs
    2. Claim batch of jobs
    3. Run the colocated ComfyUI executor
    4. Publish results/artifacts
    5. Mark complete
    6. If idle too long, self-terminate
    """

    def __init__(
        self,
        config: CloudConfig,
        queue: JobQueue | None = None,
        storage: Storage | None = None,
        worker_id: str | None = None,
    ):
        self.config = config
        self.worker_id = resolve_worker_id(worker_id)
        self.lease_id = os.environ.get("CLOUD_OFFLOAD_LEASE_ID", "").strip() or None
        self.queue = (
            queue
            if queue is not None
            else worker_queue(config, self.worker_id, self.lease_id)
        )
        self.storage = storage or create_storage(config)

        self.running = False
        self.last_job_time = utc_now()
        self.runtime_profile = config.worker_profile
        self.declared_capabilities = list(dict.fromkeys(config.worker_models))
        self._apply_image_manifest()
        self.capabilities = self._validated_capabilities()
        self.gpu_name, self.gpu_vram_gb = self._detect_gpu()
        # Pinned weights and node packs the launching profile wants on disk.
        # Staged lazily at the first claimed job so the progress is visible in
        # that job's events.
        self.weights = self._load_weights_env()
        self._weights_staged = False
        self.custom_nodes = self._load_custom_nodes_env()
        self._custom_nodes_staged = False
        self._restored_runtime_keys: set[tuple[str, str]] = set()
        self._initialize_prepared_cache()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _apply_image_manifest(self) -> None:
        manifest_path = Path(self.config.worker_manifest_path)
        if not manifest_path.is_file():
            logger.warning("Worker capability manifest not found: %s", manifest_path)
            return
        manifest = load_worker_manifest(manifest_path)
        requested_profile = self.runtime_profile
        expected_image_profile = str(
            getattr(self.config, "worker_image_profile", "") or requested_profile or ""
        ).strip()
        if expected_image_profile and manifest["profile"] != expected_image_profile:
            raise RuntimeError(
                f"Worker image profile {manifest['profile']} does not match "
                f"expected image profile {expected_image_profile} for configured "
                f"profile {requested_profile or '(unset)'}"
            )
        actual_runtime = {
            "platform": f"{sys.platform}-{platform_module.machine().lower()}",
            "python_abi": f"cp{sys.version_info.major}{sys.version_info.minor}",
        }
        configured_runtime = {
            "platform": os.environ.get("CLOUD_OFFLOAD_WORKER_PLATFORM", "").strip(),
            "python_abi": os.environ.get(
                "CLOUD_OFFLOAD_WORKER_PYTHON_ABI", ""
            ).strip(),
        }
        for field, actual in actual_runtime.items():
            declared = str(manifest.get(field) or "")
            configured = configured_runtime[field]
            if configured and declared != configured:
                raise RuntimeError(
                    f"Worker image {field} {declared or '(missing)'} does not match "
                    f"configured {field} {configured}"
                )
            if declared and declared != actual:
                raise RuntimeError(
                    f"Worker image declares {field} {declared}, but runtime is {actual}"
                )
        # Keep the configured routing name. Workers and leases must answer to
        # that name even when several configured profiles use one image family.
        self.runtime_profile = requested_profile or manifest["profile"]
        manifest_models = set(manifest["models"])
        self.declared_capabilities = [
            model for model in self.declared_capabilities if model in manifest_models
        ]
        if (
            manifest["profile"].startswith("comfyui")
            and "comfyui-workflow" in self.declared_capabilities
            and "comfyui-partition-v1" in manifest_models
        ):
            self.declared_capabilities.append("comfyui-partition-v1")

    @staticmethod
    def _load_json_list_env(name: str) -> list[dict]:
        """Read one of the launching profile's JSON list environment variables.

        An unset variable means the profile asked for nothing. Anything else —
        a blank value, a truncated document, a JSON object where a list belongs,
        an entry that is not an object — is a launch that believes it configured
        the runner and did not, and it raises with the offending value named. The
        alternative is an empty list that stages nothing and says nothing, which
        is indistinguishable from a profile that declared nothing at all.
        """
        import json
        import os

        if name not in os.environ:
            return []
        raw = os.environ[name].strip()
        if not raw:
            raise RuntimeError(f"{name} is set but empty: {os.environ[name]!r}")
        try:
            entries = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"{name} is not valid JSON ({exc}): {Worker._quote_env(raw)}"
            ) from exc
        if not isinstance(entries, list):
            raise RuntimeError(f"{name} must be a JSON list: {Worker._quote_env(raw)}")
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise RuntimeError(f"{name}[{index}] must be a JSON object: {entry!r}")
        return entries

    @staticmethod
    def _quote_env(value: str) -> str:
        """A variable's value, quoted and bounded, for an error that names it."""
        return repr(value if len(value) <= 300 else value[:300] + "...")

    @staticmethod
    def _load_weights_env() -> list[dict]:
        """Weight downloads requested by the launching profile, if any."""
        return Worker._load_json_list_env("CLOUD_OFFLOAD_WEIGHTS")

    @staticmethod
    def _load_custom_nodes_env() -> list[dict]:
        """Custom node packs requested by the launching profile, if any."""
        return Worker._load_json_list_env("CLOUD_OFFLOAD_CUSTOM_NODES")

    def _validated_capabilities(self) -> list[str]:
        """Only claim workflow capabilities this ComfyUI runner actually offers."""
        return list(
            dict.fromkeys(
                model
                for model in self.declared_capabilities
                if model in WORKFLOW_CAPABILITIES
            )
        )

    def register(
        self, status: str = "active", *, detail: str | None = None, idle: bool = False
    ) -> None:
        """Announce this worker to the coordinator without claiming anything.

        Registering as ``starting`` is how a pod that is still importing ComfyUI
        keeps the dispatcher from renting a second one for the same queue. It is
        deliberately not a claim: a worker that took a job it could not execute
        would turn a clean pre-execution failure into a paid one that also spends
        a retry, so the claim path stays gated on ComfyUI actually answering.

        Best effort. A worker whose registration does not land is a worker the
        coordinator has not heard from yet, which it already knows how to handle.
        """
        recorder = getattr(self.queue, "record_worker", None)
        if not callable(recorder):
            return
        try:
            recorder(
                self.worker_id,
                self.config.provider,
                status=status,
                runtime_profile=self.runtime_profile or "",
                capabilities=self.capabilities,
                idle=idle,
                detail=detail,
                lease_id=getattr(self, "lease_id", None),
            )
        except Exception as exc:
            logger.warning(
                "Could not register worker %s as %s: %s", self.worker_id, status, exc
            )

    def stage_node_packs(self) -> None:
        """Install the profile's declared node packs before ComfyUI starts.

        ComfyUI builds its node registry once, while it imports: a pack that
        lands in ``custom_nodes`` after the server is already up is invisible to
        it. That is how a runner that had installed both of its declared packs,
        with the events to prove it, still answered a prompt with "Node
        'LayerScope Decompose' not found. The custom node may not be installed."

        So the runner boot calls this before it launches ComfyUI. The first
        claimed job still runs staging, finds every directory already present,
        and says so in its own events.
        """
        report = self._boot_cache_report_path()
        if report:
            report.unlink(missing_ok=True)
        self._restored_runtime_keys = set()
        self._stage_custom_nodes(None)

    def _handle_signal(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    @staticmethod
    def _detect_gpu() -> tuple[str, float]:
        """Return the worker's real GPU identity for claim-time constraints."""
        try:
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=name,memory.total",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            )
            first = result.stdout.strip().splitlines()[0]
            name, memory_mib = first.rsplit(",", 1)
            return name.strip(), float(memory_mib.strip()) / 1024.0
        except (
            FileNotFoundError,
            IndexError,
            OSError,
            ValueError,
            subprocess.SubprocessError,
        ):
            logger.warning(
                "Unable to detect an NVIDIA GPU; constrained jobs will not be claimed"
            )
            return "", 0.0

    def run(
        self,
        poll_interval: int | None = None,
        max_jobs: int | None = None,
        once: bool = False,
    ):
        """
        Main worker loop.

        Args:
            poll_interval: Seconds between queue polls
            max_jobs: Maximum jobs to process before exiting (None = unlimited)
            once: Process one batch and exit
        """
        poll_interval = poll_interval or self.config.poll_interval_seconds
        jobs_processed = 0

        logger.info(
            "Worker %s starting (profile=%s, capabilities=%s)",
            self.worker_id,
            self.runtime_profile or "unconfigured",
            ",".join(self.capabilities) or "none",
        )
        if not self.capabilities:
            raise RuntimeError(
                "Worker has no validated capabilities; configure "
                "CLOUD_OFFLOAD_WORKER_PROFILE and CLOUD_OFFLOAD_WORKER_MODELS in a "
                "compatible image"
            )
        self.running = True
        self.last_job_time = utc_now()

        while self.running:
            try:
                # Check idle timeout
                if self._should_shutdown():
                    logger.info("Idle timeout reached, shutting down")
                    break

                # Claim jobs
                jobs = self.queue.claim_jobs(
                    self.worker_id,
                    limit=5,
                    token=self.config.worker_token or None,
                    provider=self.config.provider,
                    models=self.capabilities,
                    runtime_profile=self.runtime_profile,
                    gpu_vram_gb=self.gpu_vram_gb,
                    gpu_name=self.gpu_name,
                    cache_volume_id=self.cache_volume_id,
                    lease_id=getattr(self, "lease_id", None),
                )

                if jobs:
                    self.last_job_time = utc_now()

                    for job in jobs:
                        if not self.running:
                            break

                        try:
                            self._process_job(job)
                            jobs_processed += 1

                            if max_jobs and jobs_processed >= max_jobs:
                                logger.info(f"Processed {max_jobs} jobs, exiting")
                                self.running = False
                                break

                        except Exception as e:
                            logger.error(f"Job {job.id} failed: {e}")
                            self.queue.fail_job(job.id, error=str(e))
                        finally:
                            # Idle time starts when work ends, not when it is
                            # claimed. A job can run longer than the idle limit.
                            self.last_job_time = utc_now()
                else:
                    logger.debug("No jobs available")

            except Exception as e:
                logger.error(f"Worker error: {e}")

            if once:
                break

            if self.running:
                time.sleep(poll_interval)

        logger.info(
            f"Worker {self.worker_id} shutting down (processed {jobs_processed} jobs)"
        )

    def _should_shutdown(self) -> bool:
        """Check if worker should shut down due to idle timeout."""
        keep_warm = self.config.keep_warm
        idle_shutdown_seconds = self.config.idle_shutdown_seconds
        policy_reader = getattr(self.queue, "worker_policy", None)
        if callable(policy_reader):
            try:
                policy = policy_reader()
                keep_warm = bool(policy.get("keep_warm", keep_warm))
                idle_shutdown_seconds = max(
                    idle_shutdown_seconds,
                    int(policy.get("idle_shutdown_seconds", idle_shutdown_seconds)),
                )
            except Exception as exc:
                logger.warning("Could not refresh worker lifetime policy: %s", exc)
        if keep_warm:
            return False
        idle_time = utc_now() - self.last_job_time
        return idle_time.total_seconds() > idle_shutdown_seconds

    def _process_job(self, job: Job):
        """Process a single job."""
        logger.info(f"Processing job {job.id} (model={job.model})")

        current = self.queue.get(job.id)
        if current and current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}:
            logger.info("Skipping terminal job %s (%s)", job.id, current.status.value)
            return

        self._raise_if_cancelled(job)

        if job.model not in WORKFLOW_CAPABILITIES:
            raise RuntimeError(f"Unsupported job model: {job.model}")

        # The first claimed job pays the node pack install and the weight
        # download; later jobs skip both. Packs come first: they are what makes
        # the graph's node types exist at all, and they are far the smaller
        # download, so a profile that is wrong about them fails fast.
        self._phase_event(job, "staging_started")
        self._mounted_corruption_state = None
        try:
            self._begin_cache_restore(job)
            self._stage_custom_nodes(job)
            self._raise_if_cancelled(job)
            self._stage_profile_weights(job)
            self._raise_if_cancelled(job)
            self._flush_prepared_manifest(job)
        finally:
            self._complete_cache_restore(job)

        # Mark as running
        self._raise_if_cancelled(job)
        self.queue.update_status(job.id, JobStatus.RUNNING)
        self._phase_event(job, "comfyui_ready")
        self._phase_event(job, "execution_started")

        result = self._run_comfyui_workflow(job)
        self._raise_if_cancelled(job)
        self._phase_event(job, "result_available")
        self.queue.update_status(job.id, JobStatus.COMPLETED, result=result)
        logger.info(f"Job {job.id} completed")

    def _job_cancelled(self, job: Job) -> bool:
        queue = getattr(self, "queue", None)
        reader = getattr(queue, "get", None)
        if not callable(reader):
            return False
        try:
            current = reader(job.id)
        except Exception as exc:
            logger.warning("Could not read cancellation state for %s: %s", job.id, exc)
            return False
        return bool(
            current
            and current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}
            and str(current.error or "").lower().startswith("cancel")
        )

    def _raise_if_cancelled(self, job: Job) -> None:
        """Stop at every safe boundary after coordinator revocation."""
        if not self._job_cancelled(job):
            return
        if getattr(self, "lease_id", None):
            self.running = False
        raise RuntimeError("Cancelled")

    def _phase_event(self, job: Job, phase: str, **fields) -> None:
        writer = getattr(self.queue, "append_event", None)
        if callable(writer):
            writer(
                job.id,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": "phase_timing",
                    "phase": phase,
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    **fields,
                },
            )

    def _run_with_feedback(
        self,
        job: Job,
        event_type: str,
        operation,
        *,
        interval_seconds: float = 5.0,
        progress_reader=None,
        **fields,
    ):
        """Run blocking setup work while keeping the job visibly alive.

        Hugging Face and provider SDK calls are blocking and do not expose a
        stable progress callback. Run the operation in one helper thread so
        the worker's main thread can emit elapsed-time heartbeats through the
        normal coordinator channel. The operation's exception is re-raised in
        the caller exactly as before.
        """
        outcome: dict[str, object] = {}

        def run() -> None:
            try:
                outcome["value"] = operation()
            except BaseException as exc:  # re-raised on the worker thread
                outcome["error"] = exc

        started = time.monotonic()
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        while thread.is_alive():
            thread.join(timeout=max(0.01, float(interval_seconds)))
            if thread.is_alive():
                self._raise_if_cancelled(job)
                progress = {}
                if callable(progress_reader):
                    try:
                        progress["bytes_completed"] = max(0, int(progress_reader()))
                    except (OSError, TypeError, ValueError):
                        pass
                self._cache_event(
                    job,
                    event_type,
                    elapsed_seconds=round(time.monotonic() - started, 1),
                    indeterminate=True,
                    **fields,
                    **progress,
                )
        if "error" in outcome:
            raise outcome["error"]  # type: ignore[misc]
        completed = None
        if callable(progress_reader):
            try:
                completed = max(0, int(progress_reader()))
            except (OSError, TypeError, ValueError):
                completed = None
        try:
            declared_total = int(fields.get("bytes_total") or 0)
        except (TypeError, ValueError):
            declared_total = 0
        if completed is None and declared_total > 0:
            completed = declared_total
        self._cache_event(
            job,
            event_type,
            elapsed_seconds=round(time.monotonic() - started, 1),
            indeterminate=False,
            complete=True,
            **fields,
            **({"bytes_completed": completed} if completed is not None else {}),
        )
        return outcome.get("value")

    @staticmethod
    def _observed_bytes(path: Path) -> int:
        """Return bytes that currently exist below one download staging path."""
        try:
            if path.is_file():
                return int(path.stat().st_size)
            if not path.exists():
                return 0
            return sum(
                int(item.stat().st_size)
                for item in path.rglob("*")
                if item.is_file()
            )
        except OSError:
            return 0

    def _stage_custom_nodes(self, job: Job | None) -> None:
        """Install the profile's declared node packs, once per runner.

        The coordinator has already refused any partition whose required packs
        this profile does not declare, so the list here is the answer to "what
        does the graph need". What is left is putting the code on disk, pinned:
        a registry release by version, or a git checkout at an exact commit.

        Called with a job, progress rides that job's event stream as
        ``node_pack_staging`` events in the same 3..9 band weight staging uses,
        after the dispatcher's ``runner_starting`` (2) and under the 10 that
        marks the job running. Sharing one band keeps both phases of "preparing
        the runner" out of the range that means "executing". A failed install
        raises, which fails the job through the normal path in ``run``.

        Called with no job — the runner boot, before ComfyUI exists — the same
        progress goes to the log, because there is nothing yet to attach it to.

        Every exit publishes something. Skipping is a decision this makes, and a
        decision nobody can see is the reason a second attempt on an already
        staged runner looked exactly like a runner that never staged anything.
        """
        environment_ready = self._restore_environment_bundle(job)
        if self._custom_nodes_staged:
            self._publish_node_pack_skip(job, "already_staged")
            return
        if not self.custom_nodes:
            self._custom_nodes_staged = True
            self._publish_node_pack_skip(job, "none_declared")
            return

        from cloud_offload.comfyui import comfyui_custom_nodes_dir
        from cloud_offload.profiles import profile_pack_identifier

        root = comfyui_custom_nodes_dir().resolve()
        root.mkdir(parents=True, exist_ok=True)
        total_packs = len(self.custom_nodes)
        downloaded = 0

        def publish(pack_id: str | None, source: str | None, **extra) -> None:
            progress = 3 + round(6 * downloaded / max(1, total_packs))
            self._publish_staging_progress(job, progress)
            self._publish_staging_event(
                job,
                {
                    "type": "node_pack_staging",
                    "pack_id": pack_id,
                    "source": source,
                    "downloaded_packs": downloaded,
                    "total_packs": total_packs,
                    "overall_progress": progress,
                    **extra,
                },
            )

        for entry in self.custom_nodes:
            pack_id = profile_pack_identifier(entry)
            source = "registry" if entry.get("registry_id") else "git"
            if not pack_id:
                raise RuntimeError(f"Custom node pack entry names no pack: {entry!r}")
            # The pack directory is named for the pin, which is also the name
            # ComfyUI will report its nodes under and the name the coordinator
            # matched the partition's requirement against.
            target = (root / pack_id).resolve()
            try:
                target.relative_to(root)
            except ValueError:
                raise RuntimeError(
                    f"Custom node pack escapes the custom_nodes directory: {pack_id!r}"
                )
            restored = False
            if not target.exists():
                restored = self._restore_custom_node_bundle(pack_id, target, job)
            present = target.exists()
            publish(pack_id, source, present=present)
            if present:
                logger.info(
                    "Custom node pack already present, skipping %s (%s)",
                    pack_id,
                    target,
                )
                if (
                    restored
                    and not environment_ready
                    and entry.get("install_requirements", True)
                ):
                    self._install_pack_requirements(job, pack_id, target)
                if (
                    job is not None
                    and not restored
                    and ("custom-node-bundle", pack_id)
                    not in getattr(self, "_restored_runtime_keys", set())
                ):
                    self._populate_custom_node_bundle(pack_id, entry, target, job)
                downloaded += 1
                continue
            if source == "registry":
                self._install_registry_pack(entry, target)
            else:
                self._install_git_pack(entry, target)
            if entry.get("install_requirements", True):
                self._install_pack_requirements(job, pack_id, target)
            if job is not None:
                self._populate_custom_node_bundle(pack_id, entry, target, job)
            downloaded += 1

        publish(None, None)
        self._mark_environment_ready()
        if (
            job is not None
            and ("environment-bundle", self._dependency_lock())
            not in getattr(self, "_restored_runtime_keys", set())
        ):
            self._populate_environment_bundle(job)
        self._custom_nodes_staged = True
        logger.info("Staged %d custom node pack(s) into %s", total_packs, root)

    def _environment_root(self) -> Path:
        return Path(
            os.environ.get("CLOUD_OFFLOAD_ENV_ROOT", DEFAULT_ENVIRONMENT_ROOT)
        ).resolve()

    def _environment_marker(self) -> Path:
        return self._environment_root() / ".cloud-offload-environment.json"

    def _dependency_lock(self) -> str:
        return str((getattr(self, "cache_runtime", None) or {}).get("dependency_lock") or "")

    def _environment_is_ready(self) -> bool:
        import json

        try:
            marker = json.loads(self._environment_marker().read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return False
        return bool(self._dependency_lock()) and marker.get(
            "dependency_lock"
        ) == self._dependency_lock()

    def _mark_environment_ready(self) -> None:
        import json

        dependency_lock = self._dependency_lock()
        if not dependency_lock or not self.custom_nodes:
            return
        root = self._environment_root()
        root.mkdir(parents=True, exist_ok=True)
        self._environment_marker().write_text(
            json.dumps(
                {
                    "schema": "cloud-offload.environment-ready.v1",
                    "dependency_lock": dependency_lock,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )

    def _restore_environment_bundle(self, job: Job | None) -> bool:
        if not self.custom_nodes or self._environment_is_ready():
            return bool(self.custom_nodes)
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            self._environment_root().mkdir(parents=True, exist_ok=True)
            return False
        manifest = self._selected_prepared_manifest()
        dependency_lock = self._dependency_lock()
        artifact = next(
            (
                item
                for item in (manifest or {}).get("artifacts") or []
                if item.get("kind") == "environment-bundle"
                and (item.get("destination") or {}).get("dependency_lock")
                == dependency_lock
            ),
            None,
        )
        if not artifact:
            self._environment_root().mkdir(parents=True, exist_ok=True)
            return False
        from cloud_offload.prepared_state import CacheCorruptionError

        try:
            verification = self._restore_prepared_artifact(
                artifact,
                self._environment_root(),
                manifest=manifest,
            )
            self._remember_runtime_restore(("environment-bundle", dependency_lock))
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_hit",
                    digest=artifact["digest"],
                    kind="environment-bundle",
                    bytes=artifact["size"],
                    result="hit",
                    verification_mode=verification.get("mode"),
                    verification_bytes=verification.get("bytes_read"),
                )
                if self.cache_receipt:
                    self.cache_receipt.manifest_id = manifest["manifest_id"]
                    self.cache_receipt.record(
                        digest=artifact["digest"],
                        kind="environment-bundle",
                        result="hit",
                        bytes=artifact["size"],
                        reason=str(verification.get("mode") or "full_digest"),
                        verification_mode=verification.get("mode"),
                        verification_bytes=verification.get("bytes_read"),
                    )
            else:
                self._record_boot_cache_hit(artifact, manifest, verification)
            return self._environment_is_ready()
        except CacheCorruptionError as exc:
            cache.quarantine(
                artifact["digest"], str(exc), storage_key=artifact["storage_key"]
            )
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_quarantined",
                    kind="environment-bundle",
                    digest=artifact["digest"],
                    reason=str(exc),
                )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            self._environment_root().mkdir(parents=True, exist_ok=True)
            return False
        except Exception as exc:
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_refused",
                    kind="environment-bundle",
                    reason=str(exc),
                )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            self._environment_root().mkdir(parents=True, exist_ok=True)
            return False

    def _restore_custom_node_bundle(
        self, pack_id: str, target: Path, job: Job | None
    ) -> bool:
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return False
        from cloud_offload.prepared_state import CacheCorruptionError

        artifact = None
        started = time.monotonic()
        try:
            manifest = self._selected_prepared_manifest()
            if not manifest:
                return False
            artifact = next(
                (
                    item
                    for item in manifest["artifacts"]
                    if item.get("kind") == "custom-node-bundle"
                    and (item.get("destination") or {}).get("pack_id") == pack_id
                ),
                None,
            )
            if not artifact:
                return False
            verification = self._restore_prepared_artifact(
                artifact,
                target,
                manifest=manifest,
            )
            self._remember_runtime_restore(("custom-node-bundle", pack_id))
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_hit",
                    digest=artifact["digest"],
                    kind="custom-node-bundle",
                    bytes=artifact["size"],
                    result="hit",
                    verification_mode=verification.get("mode"),
                    verification_bytes=verification.get("bytes_read"),
                    background_sampled=verification.get("background_sampled"),
                )
            if self.cache_receipt:
                self.cache_receipt.manifest_id = manifest["manifest_id"]
                self.cache_receipt.record(
                    digest=artifact["digest"],
                    kind="custom-node-bundle",
                    result="hit",
                    bytes=artifact["size"],
                    reason=str(verification.get("mode") or "full_digest"),
                    verification_mode=verification.get("mode"),
                    verification_bytes=verification.get("bytes_read"),
                    background_sampled=verification.get("background_sampled"),
                    total_ms=round((time.monotonic() - started) * 1000, 3),
                )
            if job is None:
                self._record_boot_cache_hit(artifact, manifest, verification)
            return True
        except CacheCorruptionError as exc:
            if artifact:
                cache.quarantine(
                    artifact["digest"], str(exc), storage_key=artifact["storage_key"]
                )
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_quarantined",
                    kind="custom-node-bundle",
                    digest=artifact.get("digest") if artifact else None,
                    reason=str(exc),
                )
            if self.cache_receipt and artifact:
                self.cache_receipt.record(
                    digest=artifact["digest"],
                    kind="custom-node-bundle",
                    result="corruption",
                    bytes=0,
                    reason=str(exc),
                )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False
        except Exception as exc:
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_refused",
                    kind="custom-node-bundle",
                    reason=str(exc),
                )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False

    def _populate_custom_node_bundle(
        self,
        pack_id: str,
        entry: dict,
        target: Path,
        job: Job | None,
    ) -> None:
        cache = getattr(self, "prepared_cache", None)
        if cache is None or job is None:
            return
        from cloud_offload.prepared_state import bundle_key
        from cloud_offload.runtime_bundles import build_reproducible_bundle

        with tempfile.TemporaryDirectory(prefix="cloud-offload-node-bundle-") as root:
            archive = Path(root) / "custom-node.tar"
            built = build_reproducible_bundle(target, archive)
            digest = str(built["sha256"])
            cache.publish_blob(
                archive,
                digest,
                writer_id=self.worker_id,
                bundle=True,
                source_verified=True,
            )
        self._verified_prepared_digests.add("sha256:" + digest)
        self._pending_prepared_artifacts.append(
            {
                "digest": "sha256:" + digest,
                "kind": "custom-node-bundle",
                "size": int(built["size"]),
                "storage_key": bundle_key(digest),
                "portability": "portable",
                "requirements": {},
                "materialization": "extract",
                "policy": {
                    "tenant": str(self.cache_policy.get("tenant") or "default"),
                    "cacheable": True,
                    "private": False,
                },
                "source": {"pack_id": pack_id, **entry},
                "destination": {"pack_id": pack_id},
            }
        )

    def _populate_environment_bundle(self, job: Job) -> None:
        cache = getattr(self, "prepared_cache", None)
        dependency_lock = self._dependency_lock()
        root = self._environment_root()
        if cache is None or not dependency_lock or not root.is_dir():
            return
        from cloud_offload.prepared_state import bundle_key
        from cloud_offload.runtime_bundles import build_reproducible_bundle

        with tempfile.TemporaryDirectory(prefix="cloud-offload-env-bundle-") as temporary:
            archive = Path(temporary) / "environment.tar"
            built = build_reproducible_bundle(root, archive)
            digest = str(built["sha256"])
            cache.publish_blob(
                archive,
                digest,
                writer_id=self.worker_id,
                bundle=True,
                source_verified=True,
            )
        self._verified_prepared_digests.add("sha256:" + digest)
        self._pending_prepared_artifacts.append(
            {
                "digest": "sha256:" + digest,
                "kind": "environment-bundle",
                "size": int(built["size"]),
                "storage_key": bundle_key(digest),
                "portability": "runtime-bound",
                "requirements": {
                    key: self.cache_runtime.get(key, "")
                    for key in (
                        "image_digest",
                        "platform",
                        "python_abi",
                        "dependency_lock",
                    )
                },
                "materialization": "extract",
                "policy": {
                    "tenant": str(self.cache_policy.get("tenant") or "default"),
                    "cacheable": True,
                    "private": False,
                },
                "source": {"dependency_lock": dependency_lock},
                "destination": {"dependency_lock": dependency_lock},
            }
        )

    def _publish_node_pack_skip(self, job: Job | None, reason: str) -> None:
        """Say out loud that node pack staging did nothing, and why."""
        total_packs = len(self.custom_nodes)
        logger.info(
            "Node pack staging skipped (%s): %d declared pack(s)", reason, total_packs
        )
        self._publish_staging_event(
            job,
            {
                "type": "node_pack_staging",
                "pack_id": None,
                "source": None,
                "skipped": reason,
                "downloaded_packs": total_packs,
                "total_packs": total_packs,
                "overall_progress": 9,
            },
        )

    def _publish_staging_event(self, job: Job | None, event: dict) -> None:
        """Send one staging event to the job that is paying for it, if there is one."""
        if job is None:
            # Boot staging: no job exists yet, so the log takes the detail and
            # the worker record takes the fact that this runner is still alive.
            # A registration goes stale in ninety seconds, and a pod that looks
            # stale while it installs packs is a pod the dispatcher replaces.
            logger.info("Runner staging: %s", event)
            self.register("starting")
            return
        event_writer = getattr(self.queue, "append_event", None)
        if callable(event_writer):
            event_writer(job.id, event)

    def _publish_staging_progress(self, job: Job | None, progress: int) -> None:
        if job is None:
            return
        progress_setter = getattr(self.queue, "set_progress", None)
        if callable(progress_setter):
            progress_setter(job.id, progress)

    def _install_registry_pack(self, entry: dict, target: Path) -> None:
        """Install one Comfy Registry release, pinned by version.

        The registry's version list is the only place a release's artifact URL
        is published, so the version is resolved to a ``downloadUrl`` and the
        archive is unpacked through the traversal guard below.
        """
        import os
        import tempfile

        import requests

        registry_id = str(entry.get("registry_id") or "")
        version = str(entry.get("version") or "")
        base = os.environ.get(
            "CLOUD_OFFLOAD_REGISTRY_URL", DEFAULT_REGISTRY_URL
        ).rstrip("/")
        response = requests.get(
            f"{base}/nodes/{registry_id}/versions", timeout=REGISTRY_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        payload = response.json()
        versions = payload.get("versions") if isinstance(payload, dict) else payload
        download_url = ""
        for item in versions or ():
            if isinstance(item, dict) and str(item.get("version") or "") == version:
                download_url = str(item.get("downloadUrl") or "")
                break
        if not download_url:
            raise RuntimeError(
                f"Custom node pack {registry_id} has no registry version {version}"
            )

        handle = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        archive = Path(handle.name)
        handle.close()
        try:
            with requests.get(
                download_url, stream=True, timeout=REGISTRY_TIMEOUT_SECONDS
            ) as download:
                download.raise_for_status()
                with archive.open("wb") as sink:
                    for chunk in download.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            sink.write(chunk)
            self._extract_node_pack(archive, target, f"{registry_id}@{version}")
        finally:
            archive.unlink(missing_ok=True)

    @staticmethod
    def _extract_node_pack(archive: Path, target: Path, label: str) -> None:
        """Unpack a node pack archive, refusing anything that escapes ``target``.

        Every member is checked before a single one is written: an absolute path,
        a ``..`` component or a symlink entry aborts the whole install, naming the
        member. This is not a hypothetical class of bug — the pack this feature
        was designed around shipped a path-traversal fix — and the blast radius
        here is a runner's filesystem, so the archive is validated in full rather
        than sanitized member by member.
        """
        import stat
        import zipfile

        from pathlib import PureWindowsPath

        with zipfile.ZipFile(archive) as bundle:
            for info in bundle.infolist():
                name = info.filename
                # PureWindowsPath parses both separator styles and catches drive
                # letters, so one check covers archives from either platform.
                pure = PureWindowsPath(name)
                if pure.is_absolute() or pure.drive or name.startswith(("/", "\\")):
                    raise RuntimeError(
                        f"Custom node pack {label} archive member has an absolute "
                        f"path and was refused: {name}"
                    )
                if ".." in pure.parts:
                    raise RuntimeError(
                        f"Custom node pack {label} archive member traverses upward "
                        f"and was refused: {name}"
                    )
                if stat.S_ISLNK(info.external_attr >> 16):
                    raise RuntimeError(
                        f"Custom node pack {label} archive member is a symlink and "
                        f"was refused: {name}"
                    )
            target.mkdir(parents=True, exist_ok=True)
            bundle.extractall(target)

    def _install_git_pack(self, entry: dict, target: Path) -> None:
        """Clone one pack and check out the pinned commit, verifying HEAD.

        A blobless clone rather than ``--depth 1``: a depth-1 clone contains only
        the tip of the default branch, which need not include the pinned commit,
        while ``--filter=blob:none`` fetches the history cheaply and can reach any
        commit in it.

        HEAD is re-read afterwards because ``checkout`` succeeding is not proof of
        landing on the pin — an ambiguous ref or a rewritten remote would both
        leave the runner quietly executing code nobody pinned.

        Clone into a sibling staging directory and rename only after the pin is
        verified. A failed checkout must not leave ``target`` behind: container
        runtimes can restart the entrypoint, and the presence-only fast path
        would otherwise treat that partial clone as an installed node pack.
        """
        import shutil
        import tempfile

        url = str(entry.get("git") or "")
        commit = str(entry.get("commit") or "").lower()
        staging = Path(
            tempfile.mkdtemp(prefix=f".{target.name}-staging-", dir=target.parent)
        )
        try:
            self._run_git(
                ["clone", "--filter=blob:none", "--no-checkout", url, str(staging)],
                f"cloning {url}",
            )
            self._run_git(
                ["-C", str(staging), "checkout", "--detach", commit],
                f"checking out {commit[:12]} of {url}",
            )
            head = (
                self._run_git(
                    ["-C", str(staging), "rev-parse", "HEAD"], f"reading HEAD of {url}"
                )
                .strip()
                .lower()
            )
            if head != commit:
                raise RuntimeError(
                    f"Custom node pack {url} checked out {head} but the worker profile "
                    f"pins {commit}"
                )
            staging.replace(target)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    @staticmethod
    def _run_git(arguments: list[str], description: str) -> str:
        """Run one git command, failing loudly with its stderr."""
        result = subprocess.run(
            ["git", *arguments],
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Custom node pack staging failed while {description}: "
                f"{(result.stderr or result.stdout or '').strip()[:MAX_CAPTURED_OUTPUT]}"
            )
        return result.stdout or ""

    def _install_pack_requirements(
        self, job: Job | None, pack_id: str, target: Path
    ) -> None:
        """Install a pack's requirements.txt, with pip's output in the events.

        A pack whose dependencies are missing imports at ComfyUI startup and
        vanishes from the node registry, which surfaces later as "unknown node
        type" on a runner that is already rented. The output goes into the job's
        event stream because that is the only place an operator can read it: the
        runner is gone by the time anyone asks what happened.
        """
        import sys

        requirements = target / "requirements.txt"
        if not requirements.is_file():
            return
        environment_root = self._environment_root()
        environment_root.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--prefix",
                str(environment_root),
                "-r",
                str(requirements),
            ],
            capture_output=True,
            text=True,
            timeout=PIP_TIMEOUT_SECONDS,
        )
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        self._publish_staging_event(
            job,
            {
                "type": "node_pack_requirements",
                "pack_id": pack_id,
                "returncode": result.returncode,
                "output": output[-MAX_CAPTURED_OUTPUT:],
            },
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Custom node pack {pack_id} requirements install failed: "
                f"{output[-MAX_CAPTURED_OUTPUT:]}"
            )

    def _stage_profile_weights(self, job: Job) -> None:
        """Stage the profile's pinned weights and the job's declared assets.

        Two lists with different lifetimes meet here. The profile's ``weights``
        are what the operator pinned to the runner image and are staged once,
        at the first claimed job. A job's ``assets`` are what its graph actually
        declares, digest by digest, so they are checked on every job — cheaply,
        because a file whose digest already matches is left alone.

        Progress rides the job's event stream as ``weights_staging`` events in
        the 3..9 band, after the dispatcher's ``runner_starting`` (2) and under
        the 10 that marks the job running. A failed download raises, which
        fails the job through the normal path in ``run``.
        """
        assets = [
            asset
            for asset in ((job.request or {}).get("assets") or [])
            if isinstance(asset, dict)
        ]
        pending_weights = [] if self._weights_staged else list(self.weights)
        if not pending_weights and not assets:
            self._weights_staged = True
            return

        # Imported here, not at module top: huggingface_hub is a runner-side
        # extra ("cloud"), like aiohttp in the streaming executor.
        import huggingface_hub

        from cloud_offload.comfyui import comfyui_models_dir
        from cloud_offload.credentials import huggingface_token

        models_dir = comfyui_models_dir().resolve()
        token = huggingface_token() or None
        event_writer = getattr(self.queue, "append_event", None)
        progress_setter = getattr(self.queue, "set_progress", None)
        total_files = sum(
            len(entry["files"]) if entry.get("files") else 1
            for entry in pending_weights
        ) + len(assets)
        downloaded = 0

        def publish(
            repo_id: str | None,
            filename: str | None,
            category: str | None = None,
        ) -> None:
            progress = 3 + round(6 * downloaded / max(1, total_files))
            if callable(progress_setter):
                progress_setter(job.id, progress)
            if callable(event_writer):
                event_writer(
                    job.id,
                    {
                        "type": "weights_staging",
                        "repo_id": repo_id,
                        "file": filename,
                        "category": category,
                        "downloaded_files": downloaded,
                        "total_files": total_files,
                        "overall_progress": progress,
                    },
                )

        for entry in pending_weights:
            repo_id = str(entry.get("repo_id") or "")
            revision = str(entry.get("revision") or "")
            target_dir = (models_dir / str(entry.get("dest") or "")).resolve()
            try:
                target_dir.relative_to(models_dir)
            except ValueError:
                # The dispatcher validated dest at config load; re-check anyway.
                raise RuntimeError(
                    f"Weights dest escapes the models directory: {entry.get('dest')!r}"
                )
            files = entry.get("files")
            if files:
                for filename in files:
                    target = target_dir / filename
                    if target.is_file() and target.stat().st_size > 0:
                        logger.info(
                            "Weights already staged, skipping %s (%s)",
                            filename,
                            repo_id,
                        )
                        downloaded += 1
                        continue
                    if self._restore_profile_weight(entry, filename, target, job):
                        downloaded += 1
                        continue
                    publish(repo_id, filename)
                    try:
                        self._run_with_feedback(
                            job,
                            "weight_download_progress",
                            lambda: huggingface_hub.hf_hub_download(
                                repo_id=repo_id,
                                filename=filename,
                                revision=revision,
                                local_dir=str(target_dir),
                                token=token,
                            ),
                            repo_id=repo_id,
                            file=filename,
                        )
                    except Exception as exc:
                        raise RuntimeError(
                            f"Weights staging failed for {repo_id} ({filename}@{revision}): {exc}"
                        ) from exc
                    self._populate_profile_weight(entry, filename, target, job)
                    downloaded += 1
            else:
                if self._restore_profile_snapshot(entry, target_dir, job):
                    downloaded += 1
                    continue
                publish(repo_id, None)
                try:
                    self._run_with_feedback(
                        job,
                        "weight_download_progress",
                        lambda: huggingface_hub.snapshot_download(
                            repo_id=repo_id,
                            revision=revision,
                            local_dir=str(target_dir),
                            token=token,
                        ),
                        repo_id=repo_id,
                        file=None,
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Weights staging failed for {repo_id}@{revision}: {exc}"
                    ) from exc
                self._populate_profile_snapshot(entry, target_dir, job)
                downloaded += 1

        for asset in assets:
            publish(
                (asset.get("source") or {}).get("repo_id"),
                str(asset.get("filename") or ""),
                str(asset.get("category") or ""),
            )
            self._stage_declared_asset(asset, models_dir, token, job)
            downloaded += 1

        publish(None, None)
        self._weights_staged = True
        logger.info("Staged %d weight file(s) into %s", total_files, models_dir)

    def _restore_profile_weight(
        self, entry: dict, filename: str, target: Path, job: Job
    ) -> bool:
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return False
        manifest = self._selected_prepared_manifest()
        if not manifest:
            return False
        artifact = next(
            (
                item
                for item in manifest["artifacts"]
                if item.get("kind") == "profile-weight"
                and (item.get("source") or {}).get("repo_id") == entry.get("repo_id")
                and (item.get("source") or {}).get("revision") == entry.get("revision")
                and (item.get("source") or {}).get("filename") == filename
            ),
            None,
        )
        if not artifact:
            self._cache_event(
                job,
                "cache_artifact_miss",
                kind="profile-weight",
                repo_id=entry.get("repo_id"),
                file=filename,
                reason="profile_weight_not_in_manifest",
            )
            return False
        from cloud_offload.prepared_state import CacheCorruptionError

        started = time.monotonic()
        try:
            verification = self._restore_prepared_artifact(
                artifact,
                target,
                manifest=manifest,
            )
            self._cache_event(
                job,
                "cache_artifact_hit",
                kind="profile-weight",
                digest=artifact["digest"],
                bytes=artifact["size"],
                total_ms=round((time.monotonic() - started) * 1000, 3),
                result="hit",
                verification_mode=verification.get("mode"),
                verification_bytes=verification.get("bytes_read"),
                background_sampled=verification.get("background_sampled"),
            )
            if self.cache_receipt:
                self.cache_receipt.record(
                    digest=artifact["digest"],
                    kind="profile-weight",
                    result="hit",
                    bytes=artifact["size"],
                    reason=str(verification.get("mode") or "full_digest"),
                    verification_mode=verification.get("mode"),
                    verification_bytes=verification.get("bytes_read"),
                    background_sampled=verification.get("background_sampled"),
                    total_ms=round((time.monotonic() - started) * 1000, 3),
                )
            return True
        except CacheCorruptionError as exc:
            cache.quarantine(
                artifact["digest"], str(exc), storage_key=artifact["storage_key"]
            )
            self._cache_event(
                job,
                "cache_artifact_quarantined",
                kind="profile-weight",
                digest=artifact["digest"],
                reason=str(exc),
            )
            if self.cache_receipt:
                self.cache_receipt.record(
                    digest=artifact["digest"],
                    kind="profile-weight",
                    result="corruption",
                    bytes=0,
                    reason=str(exc),
                )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False
        except Exception as exc:
            self._cache_event(
                job, "cache_artifact_refused", kind="profile-weight", reason=str(exc)
            )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False

    def _populate_profile_weight(
        self, entry: dict, filename: str, target: Path, job: Job
    ) -> None:
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return
        gated = bool(entry.get("gated"))
        if gated and not self.cache_policy.get("cache_private_assets"):
            self._cache_event(
                job,
                "cache_artifact_refused",
                kind="profile-weight",
                file=filename,
                reason="private_cache_refused",
            )
            return
        from cloud_offload.prepared_state import blob_key, build_manifest
        from cloud_offload.service_config import VERSION

        digest = sha256_file(target)
        self._cache_event(
            job,
            "cache_population_started",
            kind="profile-weight",
            digest="sha256:" + digest,
        )
        cache.publish_blob(
            target,
            digest,
            writer_id=self.worker_id,
            source_verified=True,
            progress_callback=self._cache_population_reporter(
                job,
                "sha256:" + digest,
                target.stat().st_size,
                file=filename,
            ),
        )
        existing = self._selected_prepared_manifest()
        artifacts = list(existing.get("artifacts") or []) if existing else []
        source = {
            "repo_id": str(entry.get("repo_id") or ""),
            "revision": str(entry.get("revision") or ""),
            "filename": filename,
        }
        artifacts = [
            item
            for item in artifacts
            if not (
                item.get("kind") == "profile-weight" and item.get("source") == source
            )
        ]
        artifacts.append(
            {
                "digest": "sha256:" + digest,
                "kind": "profile-weight",
                "size": target.stat().st_size,
                "storage_key": blob_key(digest),
                "portability": "portable",
                "requirements": {},
                "policy": {
                    "tenant": str(self.cache_policy.get("tenant") or "default"),
                    "cacheable": True,
                    "private": gated,
                },
                "source": source,
                "destination": {"dest": entry.get("dest"), "filename": filename},
            }
        )
        profile = str(self.cache_requirements.get("profile_fingerprint") or "")
        try:
            manifest = build_manifest(
                profile_fingerprint=profile,
                producer={
                    "image_digest": self.cache_runtime.get("image_digest", ""),
                    "job_id": str(job.id) if job else "",
                    "lease_id": str(getattr(job, "params", {}).get("lease_id") or "") if job else "",
                    "cloud_offload_version": VERSION,
                    "python_abi": self.cache_runtime.get("python_abi", ""),
                    "platform": self.cache_runtime.get("platform", ""),
                    "torch": self.cache_runtime.get("torch", ""),
                    "cuda": self.cache_runtime.get("cuda", ""),
                },
                artifacts=artifacts,
                signer=cache.signer,
            )
            cache.publish_manifest(manifest)
        except Exception as exc:
            logger.warning("Profile-weight cache publication refused: %s", exc)
            self._cache_event(
                job,
                "cache_artifact_refused",
                kind="profile-weight",
                reason=str(exc),
            )
            return
        self._latest_prepared_manifest = manifest
        self._cache_event(
            job,
            "cache_population_completed",
            kind="profile-weight",
            digest="sha256:" + digest,
            manifest_id=manifest["manifest_id"],
            bytes=target.stat().st_size,
        )

    def _restore_profile_snapshot(
        self, entry: dict, target_dir: Path, job: Job | None
    ) -> bool:
        """Restore every file from a previously completed HF snapshot manifest."""
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return False
        manifest = self._selected_prepared_manifest()
        if not manifest:
            return False
        artifacts = [
            item
            for item in manifest.get("artifacts") or []
            if item.get("kind") == "profile-weight"
            and (item.get("source") or {}).get("repo_id") == entry.get("repo_id")
            and (item.get("source") or {}).get("revision") == entry.get("revision")
            and (item.get("source") or {}).get("snapshot") is True
        ]
        if not artifacts:
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_miss",
                    kind="profile-weight-snapshot",
                    repo_id=entry.get("repo_id"),
                    reason="profile_snapshot_not_in_manifest",
                )
            return False
        from cloud_offload.prepared_state import CacheCorruptionError

        started = time.monotonic()
        artifact = None
        verifications: list[dict] = []
        try:
            for artifact in sorted(
                artifacts,
                key=lambda item: str((item.get("source") or {}).get("filename")),
            ):
                filename = str((artifact.get("source") or {}).get("filename") or "")
                relative = PurePosixPath(filename)
                if not filename or relative.is_absolute() or ".." in relative.parts:
                    raise RuntimeError(
                        "Prepared snapshot contains an unsafe destination"
                    )
                target = (target_dir / relative).resolve()
                try:
                    target.relative_to(target_dir.resolve())
                except ValueError as exc:
                    raise RuntimeError(
                        "Prepared snapshot destination escapes the model directory"
                    ) from exc
                verification = self._restore_prepared_artifact(
                    artifact,
                    target,
                    manifest=manifest,
                )
                verifications.append(verification)
                if self.cache_receipt:
                    self.cache_receipt.record(
                        digest=artifact["digest"],
                        kind="profile-weight",
                        result="hit",
                        bytes=artifact["size"],
                        reason=str(verification.get("mode") or "full_digest"),
                        verification_mode=verification.get("mode"),
                        verification_bytes=verification.get("bytes_read"),
                        background_sampled=verification.get("background_sampled"),
                    )
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_hit",
                    kind="profile-weight-snapshot",
                    repo_id=entry.get("repo_id"),
                    files=len(artifacts),
                    bytes=sum(int(item["size"]) for item in artifacts),
                    total_ms=round((time.monotonic() - started) * 1000, 3),
                    result="hit",
                    verification_modes=sorted(
                        {
                            str(item.get("mode") or "full_digest")
                            for item in verifications
                        }
                    ),
                    verification_bytes=sum(
                        int(item.get("bytes_read") or 0) for item in verifications
                    ),
                )
            return True
        except CacheCorruptionError as exc:
            if artifact:
                cache.quarantine(
                    artifact["digest"], str(exc), storage_key=artifact["storage_key"]
                )
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_quarantined",
                    kind="profile-weight-snapshot",
                    digest=artifact.get("digest") if artifact else None,
                    reason=str(exc),
                )
            if self.cache_receipt and artifact:
                self.cache_receipt.record(
                    digest=artifact["digest"],
                    kind="profile-weight",
                    result="corruption",
                    bytes=0,
                    reason=str(exc),
                )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False
        except Exception as exc:
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_refused",
                    kind="profile-weight-snapshot",
                    repo_id=entry.get("repo_id"),
                    reason=str(exc),
                )
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False

    def _populate_profile_snapshot(
        self, entry: dict, target_dir: Path, job: Job | None
    ) -> None:
        """Publish a completed HF snapshot as portable, independently verified files."""
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return
        gated = bool(entry.get("gated"))
        if gated and not self.cache_policy.get("cache_private_assets"):
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_refused",
                    kind="profile-weight-snapshot",
                    reason="private_cache_refused",
                )
            return
        from cloud_offload.prepared_state import blob_key, build_manifest
        from cloud_offload.service_config import VERSION

        root = target_dir.resolve()
        files = [
            path
            for path in sorted(root.rglob("*"))
            if path.is_file() and ".cache" not in path.relative_to(root).parts
        ]
        if not files:
            return
        if job:
            self._cache_event(
                job,
                "cache_population_started",
                kind="profile-weight-snapshot",
                repo_id=entry.get("repo_id"),
                files=len(files),
            )
        additions = []
        total_bytes = 0
        for path in files:
            filename = path.relative_to(root).as_posix()
            digest = sha256_file(path)
            cache.publish_blob(path, digest, writer_id=self.worker_id)
            total_bytes += path.stat().st_size
            additions.append(
                {
                    "digest": "sha256:" + digest,
                    "kind": "profile-weight",
                    "size": path.stat().st_size,
                    "storage_key": blob_key(digest),
                    "portability": "portable",
                    "requirements": {},
                    "policy": {
                        "tenant": str(self.cache_policy.get("tenant") or "default"),
                        "cacheable": True,
                        "private": gated,
                    },
                    "source": {
                        "repo_id": str(entry.get("repo_id") or ""),
                        "revision": str(entry.get("revision") or ""),
                        "filename": filename,
                        "snapshot": True,
                    },
                    "destination": {
                        "dest": entry.get("dest"),
                        "filename": filename,
                    },
                }
            )
        existing = self._selected_prepared_manifest()
        artifacts = [
            item
            for item in (existing.get("artifacts") or [] if existing else [])
            if not (
                item.get("kind") == "profile-weight"
                and (item.get("source") or {}).get("repo_id") == entry.get("repo_id")
                and (item.get("source") or {}).get("revision") == entry.get("revision")
                and (item.get("source") or {}).get("snapshot") is True
            )
        ]
        artifacts.extend(additions)
        profile = str(self.cache_requirements.get("profile_fingerprint") or "")
        try:
            manifest = build_manifest(
                profile_fingerprint=profile,
                producer={
                    "image_digest": self.cache_runtime.get("image_digest", ""),
                    "job_id": str(job.id) if job else "",
                    "lease_id": str(getattr(job, "params", {}).get("lease_id") or "") if job else "",
                    "cloud_offload_version": VERSION,
                    "python_abi": self.cache_runtime.get("python_abi", ""),
                    "platform": self.cache_runtime.get("platform", ""),
                    "torch": self.cache_runtime.get("torch", ""),
                    "cuda": self.cache_runtime.get("cuda", ""),
                },
                artifacts=artifacts,
                signer=cache.signer,
            )
            cache.publish_manifest(manifest)
        except Exception as exc:
            logger.warning("Profile snapshot cache publication refused: %s", exc)
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_refused",
                    kind="profile-weight-snapshot",
                    reason=str(exc),
                )
            return
        self._latest_prepared_manifest = manifest
        if job:
            self._cache_event(
                job,
                "cache_population_completed",
                kind="profile-weight-snapshot",
                repo_id=entry.get("repo_id"),
                manifest_id=manifest["manifest_id"],
                files=len(additions),
                bytes=total_bytes,
            )

    def _stage_declared_asset(
        self, asset: dict, models_dir: Path, token: str | None, job: Job | None = None
    ) -> None:
        """Place one job-declared model file, verified by content digest.

        A file already at the target path is trusted only when its digest
        matches the manifest. A mismatch is quarantined, never overwritten in
        silence: two checkpoints sharing a filename is exactly the failure that
        otherwise produces confident, wrong output.

        Nothing here loads or executes what it fetches — staging moves bytes.
        """
        category = str(asset.get("category") or "")
        filename = str(asset.get("filename") or "")
        expected = str(asset.get("sha256") or "").lower()
        relative = PurePosixPath(category) / filename
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError(
                f"Declared asset escapes the models directory: {category}/{filename}"
            )
        # The category directory may itself be a symlink onto a prepared
        # network volume, so the escape check is lexical: resolving symlinks
        # here would reject the worker's own restored cache layout.
        target = models_dir / relative

        if not target.exists() and self._restore_declared_asset(asset, target):
            return

        # A symlinked target points into the prepared cache, whose restore
        # verifies content by trust receipt or digest; re-restoring through
        # the cache proves the bytes without re-hashing the whole file.
        if target.is_symlink() and self._restore_declared_asset(asset, target):
            return

        if target.is_file():
            present = sha256_file(target)
            if present == expected:
                logger.info("Declared asset already staged, skipping %s", target.name)
                # A previous cache publication can fail after the origin file
                # has been verified. A same-pod retry must finish publishing
                # that durable state instead of treating the container-local
                # copy as evidence that the volume has it.
                manifest = self._selected_prepared_manifest()
                represented = bool(
                    manifest
                    and any(
                        item.get("digest") == "sha256:" + expected
                        for item in manifest.get("artifacts") or []
                    )
                )
                if (
                    getattr(self, "prepared_cache", None) is not None
                    and not represented
                ):
                    self._populate_declared_asset(asset, target)
                return
            quarantine = models_dir / QUARANTINE_DIRNAME / present / filename
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            logger.warning(
                "Declared asset %s/%s holds sha256 %s but this job requires %s; "
                "quarantining it at %s and fetching the declared bytes",
                category,
                filename,
                present,
                expected,
                quarantine,
            )
            shutil.move(str(target), str(quarantine))

        target.parent.mkdir(parents=True, exist_ok=True)
        if job is None:
            # Preserve the small test/integration seam that supplies a simple
            # three-argument fetcher.
            self._fetch_declared_asset(asset, target, token)
        else:
            self._fetch_declared_asset(asset, target, token, job)
        written = sha256_file(target)
        if written != expected:
            raise RuntimeError(
                f"Declared asset {category}/{filename} hashed {written} after "
                f"staging but the job declared {expected}"
            )
        self._populate_declared_asset(asset, target)

    def _initialize_prepared_cache(self) -> None:
        """Consume an existing provider mount; never create or mount storage."""
        import json
        import os

        self.prepared_cache = None
        self.cache_volume_id = os.environ.get(
            "CLOUD_OFFLOAD_CACHE_VOLUME_ID", ""
        ).strip()
        self.cache_policy: dict = {}
        self.cache_requirements: dict = {}
        self.cache_runtime: dict = {}
        self.cache_receipt = None
        self.cache_manifest_instruction = ""
        self._latest_prepared_manifest = None
        root_value = os.environ.get("CLOUD_OFFLOAD_CACHE_ROOT", "").strip()
        if not root_value:
            return
        root = Path(root_value).resolve()
        expected = os.environ.get(
            "CLOUD_OFFLOAD_CACHE_EXPECTED_PROVIDER_VOLUME_ID", ""
        ).strip()
        self.cache_provider_volume_id = expected
        mounted = os.environ.get("RUNPOD_VOLUME_ID", "").strip()
        if expected and mounted != expected:
            from cloud_offload.prepared_state import CacheMountError

            raise CacheMountError(
                f"Expected cache volume {expected}, provider reported {mounted or 'none'}"
            )
        if not root.is_dir():
            if not expected or not root.parent.is_dir():
                from cloud_offload.prepared_state import CacheMountError

                raise CacheMountError(
                    f"Expected prepared cache mount is absent: {root}"
                )
            # The provider mounted /workspace and proved its identity. Creating
            # our namespaced child on an empty first-use volume is safe.
            root.mkdir(parents=False)
        try:
            self.cache_policy = json.loads(
                os.environ.get("CLOUD_OFFLOAD_CACHE_POLICY", "{}")
            )
            self.cache_requirements = json.loads(
                os.environ.get("CLOUD_OFFLOAD_CACHE_REQUIREMENTS", "{}")
            )
            self.cache_manifest_instruction = os.environ.get(
                "CLOUD_OFFLOAD_CACHE_MANIFEST", ""
            ).strip()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Prepared cache instructions are malformed JSON"
            ) from exc
        from cloud_offload.prepared_state import (
            CoordinatorManifestAuthority,
            PreparedStateCAS,
            fingerprint,
            runtime_fingerprint,
        )

        authority = CoordinatorManifestAuthority(self.queue)
        self.cache_authority = authority
        cache = PreparedStateCAS(root, authority)
        cache.verify_mount(expected or None)
        identity = self.cache_requirements.get("runtime_identity") or {}
        image = str(identity.get("image") or "")
        image_digest = ""
        if "@sha256:" in image:
            image_digest = "sha256:" + image.rsplit("@sha256:", 1)[1]
        dependency_lock = fingerprint(
            {
                "custom_nodes": identity.get("custom_nodes") or [],
                "wheelhouse_sha256": identity.get("wheelhouse_sha256"),
            }
        )
        self.cache_runtime = runtime_fingerprint(
            {"image_digest": image_digest, "dependency_lock": dependency_lock}
        )
        self.prepared_cache = cache

    def _selected_prepared_manifest(self):
        import os

        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return None
        profile = str(self.cache_requirements.get("profile_fingerprint") or "")
        selected = str(getattr(self, "cache_manifest_instruction", "") or "")
        latest = getattr(self, "_latest_prepared_manifest", None)
        if selected and selected != profile:
            # An exact placement promise is authoritative for this Pod. A
            # same-profile manifest that this worker publishes later cannot
            # replace it during the assigned job.
            manifest = cache.find_manifest(manifest_id=selected)
        elif latest and latest.get("profile_fingerprint") == profile:
            manifest = latest
        else:
            manifest = cache.find_manifest(profile_fingerprint=profile)
        expected_volume = os.environ.get("CLOUD_OFFLOAD_CACHE_VOLUME_ID", "").strip()
        if manifest and expected_volume:
            if str(manifest.get("cache_volume_id") or "") != expected_volume:
                from cloud_offload.prepared_state import ManifestError

                raise ManifestError(
                    "Prepared manifest is not authorized for the mounted cache volume"
                )
        return manifest

    def _restore_prepared_artifact(
        self,
        artifact: dict,
        destination: Path,
        *,
        manifest: dict,
    ) -> dict:
        """Restore one artifact and return its safe verification decision."""
        verification: dict = {}
        self.prepared_cache.restore_artifact(
            artifact,
            destination,
            runtime=self.cache_runtime,
            tenant=str(self.cache_policy.get("tenant") or "default"),
            allow_private=bool(self.cache_policy.get("cache_private_assets")),
            manifest=manifest,
            volume_id=(
                str(getattr(self, "cache_volume_id", "") or "")
                or str(manifest.get("cache_volume_id") or "")
            ),
            provider_volume_id=(
                str(getattr(self, "cache_provider_volume_id", "") or "")
                or str(manifest.get("cache_provider_volume_id") or "")
            ),
            verification_callback=verification.update,
        )
        return verification

    def _cache_event(self, job: Job, event_type: str, **fields) -> None:
        writer = getattr(self.queue, "append_event", None)
        if callable(writer):
            writer(
                job.id,
                {
                    "schema": "cloud-offload.phase-event.v1",
                    "type": event_type,
                    "phase": event_type.removeprefix("cache_"),
                    "monotonic_ms": round(time.monotonic() * 1000, 3),
                    **fields,
                },
            )

    @staticmethod
    def _runtime_bundle_key(artifact: dict) -> tuple[str, str] | None:
        kind = str(artifact.get("kind") or "")
        destination = artifact.get("destination") or {}
        if kind == "custom-node-bundle":
            value = str(destination.get("pack_id") or "")
        elif kind == "environment-bundle":
            value = str(destination.get("dependency_lock") or "")
        else:
            return None
        return (kind, value) if value else None

    def _remember_runtime_restore(self, runtime_key: tuple[str, str]) -> None:
        restored = set(getattr(self, "_restored_runtime_keys", set()))
        restored.add(runtime_key)
        self._restored_runtime_keys = restored

    @staticmethod
    def _boot_cache_report_path() -> Path | None:
        value = os.environ.get("CLOUD_OFFLOAD_BOOT_RESTORE_REPORT", "").strip()
        return Path(value).resolve() if value else None

    def _record_boot_cache_hit(
        self,
        artifact: dict,
        manifest: dict,
        verification: dict,
    ) -> None:
        """Persist a verified pre-ComfyUI restore for the first claimed job."""

        path = self._boot_cache_report_path()
        runtime_key = self._runtime_bundle_key(artifact)
        if path is None or runtime_key is None:
            return
        import json

        report = {
            "schema": "cloud-offload.boot-cache-restore.v1",
            "worker_id": self.worker_id,
            "profile_fingerprint": str(
                self.cache_requirements.get("profile_fingerprint") or ""
            ),
            "manifest_id": manifest["manifest_id"],
            "artifacts": [],
        }
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            if all(
                existing.get(key) == report[key]
                for key in (
                    "schema",
                    "worker_id",
                    "profile_fingerprint",
                    "manifest_id",
                )
            ):
                report["artifacts"] = list(existing.get("artifacts") or [])
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        entry = {
            "kind": artifact["kind"],
            "digest": artifact["digest"],
            "bytes": int(artifact["size"]),
            "destination": dict(artifact.get("destination") or {}),
            "verification_mode": verification.get("mode"),
            "verification_bytes": int(verification.get("bytes_read") or 0),
            "background_sampled": bool(verification.get("background_sampled")),
        }
        report["artifacts"] = [
            item
            for item in report["artifacts"]
            if self._runtime_bundle_key(item) != runtime_key
        ]
        report["artifacts"].append(entry)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
        temporary.write_text(
            json.dumps(report, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, path)

    def _consume_boot_cache_hits(self, job: Job) -> None:
        """Project trusted boot restores into the job event and receipt streams."""

        path = self._boot_cache_report_path()
        if path is None or not path.is_file():
            return
        import json

        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            manifest = self._selected_prepared_manifest()
            if (
                report.get("schema") != "cloud-offload.boot-cache-restore.v1"
                or report.get("worker_id") != self.worker_id
                or report.get("profile_fingerprint")
                != str(self.cache_requirements.get("profile_fingerprint") or "")
                or not manifest
                or report.get("manifest_id") != manifest.get("manifest_id")
            ):
                raise ValueError("Boot cache report does not match this job")
            manifest_artifacts = {
                self._runtime_bundle_key(artifact): artifact
                for artifact in manifest.get("artifacts") or []
                if self._runtime_bundle_key(artifact)
            }
            accepted = []
            for item in report.get("artifacts") or []:
                runtime_key = self._runtime_bundle_key(item)
                artifact = manifest_artifacts.get(runtime_key)
                if (
                    not runtime_key
                    or not artifact
                    or item.get("digest") != artifact.get("digest")
                    or int(item.get("bytes") or -1) != int(artifact.get("size") or 0)
                ):
                    raise ValueError("Boot cache artifact does not match the manifest")
                accepted.append((runtime_key, artifact, item))
            self.cache_receipt.manifest_id = manifest["manifest_id"]
            for runtime_key, artifact, item in accepted:
                self._remember_runtime_restore(runtime_key)
                self._cache_event(
                    job,
                    "cache_artifact_hit",
                    digest=artifact["digest"],
                    kind=artifact["kind"],
                    bytes=artifact["size"],
                    result="hit",
                    verification_mode=item.get("verification_mode"),
                    verification_bytes=item.get("verification_bytes"),
                    background_sampled=bool(item.get("background_sampled")),
                    restored_before_claim=True,
                )
                self.cache_receipt.record(
                    digest=artifact["digest"],
                    kind=artifact["kind"],
                    result="hit",
                    bytes=artifact["size"],
                    reason=str(item.get("verification_mode") or "full_digest"),
                    verification_mode=item.get("verification_mode"),
                    verification_bytes=int(item.get("verification_bytes") or 0),
                    background_sampled=bool(item.get("background_sampled")),
                    total_ms=0,
                    restored_before_claim=True,
                )
        except (
            AttributeError,
            OSError,
            ValueError,
            TypeError,
            json.JSONDecodeError,
        ) as exc:
            logger.warning("Boot cache restore report was refused: %s", exc)
            self._cache_event(
                job,
                "cache_boot_restore_report_refused",
                reason="invalid_boot_restore_report",
            )
        finally:
            path.unlink(missing_ok=True)

    def _begin_cache_restore(self, job: Job) -> None:
        self._active_cache_job = job
        self._pending_prepared_artifacts = []
        self._verified_prepared_digests = set()
        if getattr(self, "prepared_cache", None) is None:
            return
        import os

        from cloud_offload.prepared_state import RestoreReceipt

        self.cache_receipt = RestoreReceipt(
            manifest_id=None,
            volume_id=os.environ.get("CLOUD_OFFLOAD_CACHE_VOLUME_ID", ""),
            datacenter_id=os.environ.get("RUNPOD_DC_ID", ""),
            worker_class=self.gpu_name or "unknown",
        )
        self.cache_authority.set_context(
            job_id=job.id, volume_id=self.cache_receipt.volume_id
        )
        if os.environ.get("CLOUD_OFFLOAD_BENCHMARK_MOUNT_CORRUPTION") == "1":
            from cloud_offload.benchmark_mounted_corruption import inject

            manifest = self._selected_prepared_manifest()
            if manifest:
                self._mounted_corruption_state = inject(self.prepared_cache, manifest)
                if self._mounted_corruption_state:
                    self._cache_event(job, "benchmark_mounted_corruption_injected",
                                      digest=self._mounted_corruption_state["digest"])
        healed, pending = self.prepared_cache.retry_pending_announcements()
        if healed or pending:
            self._cache_event(
                job,
                "cache_inventory_projection",
                healed=healed,
                pending=pending,
            )
        self._cache_event(
            job,
            "cache_mount_ready",
            volume_id=self.cache_receipt.volume_id,
            datacenter_id=self.cache_receipt.datacenter_id,
        )
        self._cache_event(job, "cache_restore_started")
        self._consume_boot_cache_hits(job)

    def _complete_cache_restore(self, job: Job) -> None:
        if getattr(self, "_mounted_corruption_state", None):
            from cloud_offload.benchmark_mounted_corruption import cleanup

            result = cleanup(self.prepared_cache, self._mounted_corruption_state)
            self._mounted_corruption_state = None
            self._cache_event(job, "benchmark_mounted_corruption_cleaned", **result)
        if not getattr(self, "cache_receipt", None):
            self._active_cache_job = None
            return
        receipt = self.cache_receipt.to_dict()
        self._cache_event(job, "cache_restore_completed", receipt=receipt)
        recorder = getattr(self.queue, "record_cache_observation", None)
        if callable(recorder):
            for item in receipt["artifacts"]:
                try:
                    recorder(
                        job.id,
                        {
                            "schema": "cloud-offload.restore-observation.v1",
                            "volume_id": receipt["volume_id"],
                            "manifest_id": receipt["manifest_id"],
                            "digest": item.get("digest"),
                            "datacenter_id": receipt["datacenter_id"],
                            "worker_class": receipt["worker_class"],
                            "image_digest": self.cache_runtime.get("image_digest"),
                            "strategy": "symlink"
                            if item.get("kind") != "custom-node-bundle"
                            else "extract",
                            "result": item.get("result") or "unknown",
                            "verification_mode": item.get("verification_mode"),
                            "verification_bytes": int(
                                item.get("verification_bytes") or 0
                            ),
                            "background_sampled": bool(
                                item.get("background_sampled")
                            ),
                            "bytes": int(item.get("bytes") or 0),
                            "file_count": 1,
                            "lookup_ms": 0,
                            "transfer_ms": 0,
                            "verification_ms": float(item.get("total_ms") or 0),
                            "extraction_ms": 0,
                            "import_ms": 0,
                            "total_ms": float(item.get("total_ms") or 0),
                            "fallback_ms": item.get("fallback_ms"),
                        },
                    )
                except Exception as exc:
                    logger.warning("Could not record cache observation: %s", exc)
        self.cache_receipt = None
        self.cache_authority.set_context(job_id=None, volume_id=None)
        self._active_cache_job = None

    def _restore_declared_asset(self, asset: dict, target: Path) -> bool:
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return False
        from cloud_offload.prepared_state import (
            CacheCorruptionError,
            CachePolicyError,
            ManifestError,
            normalize_digest,
        )

        digest = "sha256:" + normalize_digest(str(asset.get("sha256") or ""))
        profile = str(self.cache_requirements.get("profile_fingerprint") or "")
        started = time.monotonic()
        artifact = None
        try:
            manifest = self._selected_prepared_manifest()
            source_manifest_id = manifest["manifest_id"] if manifest else None
            artifact_manifest = manifest
            artifact = (
                next(
                    (
                        item
                        for item in manifest["artifacts"]
                        if item["digest"] == digest
                    ),
                    None,
                )
                if manifest
                else None
            )
            shared = False
            if not artifact:
                shared_match = cache.find_portable_artifact(digest)
                if shared_match:
                    artifact, source_manifest_id = shared_match
                    artifact_manifest = cache.find_manifest(
                        manifest_id=source_manifest_id
                    )
                    shared = True
            if not artifact:
                self._record_cache_result(
                    asset,
                    "miss",
                    "artifact_not_in_manifest" if manifest else "manifest_not_found",
                    started,
                )
                return False
            if self.cache_receipt:
                self.cache_receipt.manifest_id = source_manifest_id
            self._cache_event(
                self._active_cache_job,
                "cache_manifest_verified",
                manifest_id=source_manifest_id,
                profile_fingerprint=profile,
                cross_profile=shared,
            ) if getattr(self, "_active_cache_job", None) else None
            if not artifact_manifest:
                raise ManifestError("Prepared artifact manifest is unavailable")
            verification = self._restore_prepared_artifact(
                artifact,
                target,
                manifest=artifact_manifest,
            )
            self._verified_prepared_digests = set(
                getattr(self, "_verified_prepared_digests", set())
            )
            self._verified_prepared_digests.add(digest)
            self._record_cache_result(
                asset,
                "hit",
                str(verification.get("mode") or "full_digest"),
                started,
                target.stat().st_size,
                verification=verification,
            )
            if shared:
                # Publish a profile-B reference only after policy and digest
                # verification. The immutable blob is reused; no origin call.
                self._populate_declared_asset(asset, target, blob_already_verified=True)
            return True
        except CacheCorruptionError as exc:
            cache.quarantine(
                digest,
                str(exc),
                storage_key=(artifact or {}).get("storage_key"),
            )
            self._record_cache_result(asset, "corruption", str(exc), started)
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False
        except (ManifestError, CachePolicyError) as exc:
            self._record_cache_result(asset, "refused", str(exc), started)
            if self.cache_policy.get("cold_fallback") == "deny":
                raise
            return False

    def _record_cache_result(
        self,
        asset: dict,
        result: str,
        reason: str,
        started: float,
        size: int = 0,
        verification: dict | None = None,
    ) -> None:
        elapsed = round((time.monotonic() - started) * 1000, 3)
        entry = {
            "digest": "sha256:"
            + str(asset.get("sha256") or "").removeprefix("sha256:"),
            "kind": "model-weight",
            "file": str(asset.get("filename") or ""),
            "category": str(asset.get("category") or ""),
            "result": result,
            "reason": reason,
            "bytes": int(size),
            "total_ms": elapsed,
            **(
                {
                    "verification_mode": verification.get("mode"),
                    "verification_bytes": verification.get("bytes_read"),
                    "background_sampled": verification.get("background_sampled"),
                    "trust_receipt_id": verification.get("receipt_id"),
                    "full_audit_due_at": verification.get("full_audit_due_at"),
                }
                if verification
                else {}
            ),
        }
        if self.cache_receipt:
            self.cache_receipt.record(**entry)
        job = getattr(self, "_active_cache_job", None)
        if job:
            event_type = {
                "hit": "cache_artifact_hit",
                "miss": "cache_artifact_miss",
                "refused": "cache_artifact_refused",
                "corruption": "cache_artifact_quarantined",
            }.get(result, "cache_artifact_miss")
            self._cache_event(job, event_type, **entry)

    def _populate_declared_asset(
        self,
        asset: dict,
        target: Path,
        *,
        blob_already_verified: bool = False,
    ) -> None:
        cache = getattr(self, "prepared_cache", None)
        if cache is None:
            return
        policy = {
            "tenant": str(
                asset.get("tenant") or self.cache_policy.get("tenant") or "default"
            ),
            "cacheable": bool(asset.get("cacheable", True)),
            "private": bool(asset.get("private") or asset.get("gated")),
        }
        if not policy["cacheable"] or (
            policy["private"] and not self.cache_policy.get("cache_private_assets")
        ):
            job = getattr(self, "_active_cache_job", None)
            if job:
                self._cache_event(
                    job,
                    "cache_artifact_refused",
                    digest="sha256:"
                    + str(asset.get("sha256") or "").removeprefix("sha256:"),
                    reason="asset_policy_refuses_population",
                )
            return
        from cloud_offload.prepared_state import (
            blob_key,
            normalize_digest,
        )

        digest = normalize_digest(str(asset.get("sha256") or ""))
        job = getattr(self, "_active_cache_job", None)
        if job:
            self._cache_event(
                job,
                "cache_population_started",
                digest="sha256:" + digest,
                file=str(asset.get("filename") or target.name),
                category=str(asset.get("category") or ""),
                bytes_total=target.stat().st_size,
            )
        if not blob_already_verified:
            cache.publish_blob(
                target,
                digest,
                writer_id=self.worker_id,
                source_verified=True,
                progress_callback=self._cache_population_reporter(
                    job,
                    "sha256:" + digest,
                    target.stat().st_size,
                    file=str(asset.get("filename") or target.name),
                    category=str(asset.get("category") or ""),
                )
                if job
                else None,
                commit_callback=self._cache_commit_reporter(
                    job,
                    "sha256:" + digest,
                    file=str(asset.get("filename") or target.name),
                )
                if job
                else None,
            )
        self._verified_prepared_digests = set(
            getattr(self, "_verified_prepared_digests", set())
        )
        self._verified_prepared_digests.add("sha256:" + digest)
        artifact = {
            "digest": "sha256:" + digest,
            "kind": "model-weight",
            "size": target.stat().st_size,
            "storage_key": blob_key(digest),
            "portability": "portable",
            "requirements": {},
            "policy": policy,
            "destination": {
                "category": str(asset.get("category") or ""),
                "filename": str(asset.get("filename") or target.name),
            },
        }
        if job:
            self._pending_prepared_artifacts.append(artifact)
            return
        self._publish_prepared_artifacts([artifact], None)

    def _publish_prepared_artifacts(
        self, additions: list[dict], job: Job | None
    ) -> None:
        if not additions:
            return
        if job is not None:
            self._raise_if_cancelled(job)
        from cloud_offload.prepared_state import build_manifest
        from cloud_offload.service_config import VERSION

        cache = self.prepared_cache
        profile = str(self.cache_requirements.get("profile_fingerprint") or "")
        existing = self._selected_prepared_manifest()
        artifacts = list(existing.get("artifacts") or []) if existing else []
        addition_digests = {item["digest"] for item in additions}
        artifacts = [
            item for item in artifacts if item.get("digest") not in addition_digests
        ]
        artifacts.extend(additions)
        manifest = build_manifest(
            profile_fingerprint=profile,
            producer={
                "image_digest": self.cache_runtime.get("image_digest", ""),
                "job_id": str(job.id) if job else "",
                "lease_id": str(getattr(job, "params", {}).get("lease_id") or "") if job else "",
                "cloud_offload_version": VERSION,
                "python_abi": self.cache_runtime.get("python_abi", ""),
                "platform": self.cache_runtime.get("platform", ""),
                "torch": self.cache_runtime.get("torch", ""),
                "cuda": self.cache_runtime.get("cuda", ""),
            },
            artifacts=artifacts,
            signer=cache.signer,
        )
        cache.publish_manifest(
            manifest,
            verified_digests=set(getattr(self, "_verified_prepared_digests", set())),
        )
        self._latest_prepared_manifest = manifest
        if job:
            for artifact in additions:
                destination = artifact.get("destination") or {}
                self._cache_event(
                    job,
                    "cache_population_completed",
                    kind=str(artifact.get("kind") or ""),
                    digest=artifact["digest"],
                    manifest_id=manifest["manifest_id"],
                    bytes=artifact["size"],
                    file=str(destination.get("filename") or ""),
                    category=str(destination.get("category") or ""),
                )

    def _flush_prepared_manifest(self, job: Job) -> None:
        additions = list(getattr(self, "_pending_prepared_artifacts", []))
        if not additions:
            return
        self._publish_prepared_artifacts(additions, job)
        self._pending_prepared_artifacts = []

    def _cache_population_reporter(self, job: Job, digest: str, total: int, **fields):
        started = time.monotonic()
        last_emit = 0.0

        def report(completed: int, measured_total: int) -> None:
            nonlocal last_emit
            now = time.monotonic()
            actual_total = int(measured_total or total)
            if completed < actual_total and now - last_emit < 2.0:
                return
            last_emit = now
            self._cache_event(
                job,
                "cache_population_progress",
                digest=digest,
                bytes_completed=int(completed),
                bytes_total=actual_total,
                percent=round(100 * completed / max(1, actual_total), 1),
                elapsed_seconds=round(now - started, 1),
                **fields,
            )

        return report

    def _cache_commit_reporter(self, job: Job, digest: str, **fields):
        started = time.monotonic()

        def report(stage: str) -> None:
            self._cache_event(
                job,
                "cache_population_commit",
                digest=digest,
                commit_stage=stage,
                elapsed_seconds=round(time.monotonic() - started, 1),
                **fields,
            )

        return report

    def _fetch_declared_asset(
        self, asset: dict, target: Path, token: str | None, job: Job | None = None
    ) -> None:
        """Fetch one declared asset from the origin the coordinator resolved."""
        source = asset.get("source") or {}
        label = f"{asset.get('category')}/{asset.get('filename')}"
        total_bytes = int(asset.get("size") or 0)

        def observed(operation, observed_path: Path):
            if job is None:
                return operation()
            baseline = self._observed_bytes(observed_path)
            return self._run_with_feedback(
                job,
                "weight_download_progress",
                operation,
                progress_reader=lambda: max(
                    0, self._observed_bytes(observed_path) - baseline
                ),
                file=str(asset.get("filename") or ""),
                bytes_total=total_bytes,
            )

        try:
            if source.get("artifact_id"):
                observed(
                    lambda: self._download_partition_artifact(
                        str(source["artifact_id"]), target
                    ),
                    target,
                )
            elif source.get("url"):
                observed(lambda: self._download_asset_url(str(source["url"]), target), target)
            elif source.get("repo_id"):
                import huggingface_hub

                # hf_hub_download names the file after its path in the repo,
                # which need not match the runner's filename, so it lands in a
                # scratch directory and is moved into place.
                staging = target.parent / ".cloud-offload-fetch"
                staging.mkdir(parents=True, exist_ok=True)
                try:

                    def download():
                        return huggingface_hub.hf_hub_download(
                            repo_id=str(source["repo_id"]),
                            filename=str(
                                source.get("filename") or asset.get("filename")
                            ),
                            revision=str(source.get("revision") or ""),
                            local_dir=str(staging),
                            token=token,
                        )

                    fetched = observed(download, staging)
                    shutil.move(str(fetched), str(target))
                finally:
                    shutil.rmtree(staging, ignore_errors=True)
            else:
                raise RuntimeError("no source was resolved for it")
        except Exception as exc:
            raise RuntimeError(
                f"Declared asset staging failed for {label}: {exc}"
            ) from exc

    @staticmethod
    def _download_asset_url(url: str, target: Path) -> None:
        """Stream a declared asset from a plain URL."""
        import requests

        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with target.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)

    def _run_comfyui_workflow(self, job: Job) -> dict:
        """Execute an arbitrary API-format workflow in the colocated ComfyUI."""
        from cloud_offload.comfyui import ComfyUIWorkflowExecutor
        from cloud_offload.workflow_capsule import workflow_capsule_digest

        request = job.request
        if request.get("kind") == "comfyui-partition":
            return self._run_comfyui_partition(job, ComfyUIWorkflowExecutor())
        capsule = (
            request.get("capsule")
            if request.get("kind") == "comfyui-workflow-capsule"
            else None
        )
        workflow = (capsule or {}).get("workflow") or request.get("workflow") or {}
        first_sampler = False
        total_nodes = max(1, len(workflow))
        finished_nodes: set[str] = set()
        last_progress_sent = 0.0
        last_cancel_check = 0.0
        cancel_requested = False

        def should_cancel() -> bool:
            nonlocal last_cancel_check, cancel_requested
            if cancel_requested:
                return True
            now = time.monotonic()
            if now - last_cancel_check < 1.0:
                return False
            last_cancel_check = now
            current = self.queue.get(job.id)
            cancel_requested = bool(
                current
                and current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}
                and str(current.error or "").lower().startswith("cancel")
            )
            return cancel_requested

        def relay(event: dict) -> None:
            nonlocal first_sampler, last_progress_sent
            writer = getattr(self.queue, "append_event", None)
            if callable(writer):
                writer(job.id, event)
            event_type = str(event.get("type") or "")
            node_id = str(event.get("node_id") or "")
            node = workflow.get(node_id) or {}
            if (
                not first_sampler
                and event_type == "executing"
                and "sampler" in str(node.get("class_type") or "").lower()
            ):
                first_sampler = True
                self._phase_event(job, "first_sampler", node_id=node_id)
            if event_type == "executed" and node_id:
                finished_nodes.add(node_id)
            elif event_type == "execution_cached":
                for cached in (event.get("data") or {}).get("nodes") or []:
                    finished_nodes.add(str(cached))
            now = time.monotonic()
            fraction = 0.0
            if event_type == "progress":
                data = event.get("data") or {}
                maximum = max(1, int(data.get("max") or 1))
                fraction = max(
                    0.0, min(1.0, float(data.get("value") or 0) / maximum)
                )
                if now - last_progress_sent < 0.25 and fraction < 1.0:
                    return
                last_progress_sent = now
            overall = (len(finished_nodes) + fraction) / total_nodes
            setter = getattr(self.queue, "set_progress", None)
            if callable(setter):
                setter(job.id, max(10, min(95, 10 + round(overall * 85))))

        inputs = request.get("inputs") or {}
        with tempfile.TemporaryDirectory(prefix="cloud-offload-workflow-") as root_name:
            root = Path(root_name).resolve()
            if capsule is not None:
                inputs = {}
                for filename, artifact_id in (
                    request.get("input_artifacts") or {}
                ).items():
                    target = (root / filename).resolve()
                    target.relative_to(root)
                    self._download_partition_artifact(str(artifact_id), target)
                    inputs[filename] = base64.b64encode(target.read_bytes()).decode(
                        "ascii"
                    )
            result = ComfyUIWorkflowExecutor().execute(
                workflow,
                inputs=inputs,
                timeout_seconds=int(request.get("timeout_seconds", 3600)),
                event_callback=relay,
                cancel_check=should_cancel,
            )
            if capsule is None:
                return result

            artifacts: list[dict] = []
            for output_kind, entries in (
                ("image", result.pop("images", [])),
                ("file", result.pop("files", [])),
            ):
                for index, entry in enumerate(entries):
                    self._raise_if_cancelled(job)
                    encoded = entry.pop("data", "")
                    try:
                        content = base64.b64decode(encoded, validate=True)
                    except ValueError as exc:
                        raise RuntimeError("ComfyUI returned an invalid output") from exc
                    target = root / f"output-{len(artifacts):04d}.artifact"
                    target.write_bytes(content)
                    stored = self._upload_partition_artifact(target)
                    artifacts.append(
                        {
                            **stored,
                            "node_id": str(entry.get("node_id") or ""),
                            "filename": str(entry.get("filename") or f"output-{index}"),
                            "subfolder": str(entry.get("subfolder") or ""),
                            "mime_type": str(
                                entry.get("mime_type") or "application/octet-stream"
                            ),
                            "output_kind": str(
                                entry.get("output_kind") or output_kind
                            ),
                        }
                    )

            for expected in capsule.get("outputs") or []:
                if not expected.get("required", True):
                    continue
                kind = str(expected.get("kind") or "any")
                found = any(
                    item["node_id"] == str(expected["node_id"])
                    and (
                        kind == "any"
                        or item["output_kind"] == kind
                        or (kind == "file" and item["output_kind"] != "image")
                    )
                    for item in artifacts
                )
                if not found:
                    raise RuntimeError(
                        "ComfyUI produced no required workflow output for node "
                        + str(expected["node_id"])
                    )
            return {
                "schema": "comfy.workflow.result.v1",
                "capsule_digest": workflow_capsule_digest(capsule),
                "prompt_id": result.get("prompt_id"),
                "uploaded_inputs": result.get("uploaded_inputs") or {},
                "outputs": result.get("outputs") or {},
                "artifacts": artifacts,
            }

    @staticmethod
    def _partition_artifact_key(artifact_id: str) -> str:
        if len(artifact_id) != 64 or any(
            c not in "0123456789abcdef" for c in artifact_id
        ):
            raise ValueError(f"Invalid partition artifact ID: {artifact_id}")
        return f"partition-artifacts/{artifact_id[:2]}/{artifact_id}.part"

    def _download_partition_artifact(self, artifact_id: str, destination: Path) -> Path:
        downloader = getattr(self.queue, "download_artifact", None)
        if callable(downloader):
            return downloader(artifact_id, destination)
        return self.storage.download(
            self._partition_artifact_key(artifact_id), destination
        )

    def _upload_partition_artifact(self, source: Path) -> dict:
        uploader = getattr(self.queue, "upload_artifact", None)
        if callable(uploader):
            return uploader(source)
        artifact_id = sha256_file(source)
        self.storage.upload(source, self._partition_artifact_key(artifact_id))
        return {
            "artifact_id": artifact_id,
            "sha256": artifact_id,
            "size": source.stat().st_size,
        }

    def _run_comfyui_partition(self, job: Job, executor) -> dict:
        """Stage typed boundaries, execute the extracted prompt, and publish outputs."""
        import copy
        import os

        request = job.request
        partition = request.get("partition") or {}
        if partition.get("schema") != "comfy.partition.job.v1":
            raise ValueError("Unsupported ComfyUI partition schema")
        workflow = copy.deepcopy(partition.get("workflow") or {})
        root = Path(
            os.environ.get("COMFY_PARTITION_ROOT", DEFAULT_PARTITION_ROOT)
        ).resolve()
        job_root = (root / job.id).resolve()
        job_root.relative_to(root)
        input_root = job_root / "inputs"
        output_root = job_root / "outputs"
        input_root.mkdir(parents=True, exist_ok=True)
        output_root.mkdir(parents=True, exist_ok=True)

        event_writer = getattr(self.queue, "append_event", None)

        def publish(event: dict) -> None:
            if callable(event_writer):
                event_writer(job.id, event)

        publish(
            {
                "type": "partition_staging",
                "partition_id": partition.get("partition_id"),
            }
        )

        input_artifacts = request.get("input_artifacts") or {}
        expected_inputs = {item["key"] for item in partition.get("inputs") or []}
        if set(input_artifacts) != expected_inputs:
            raise ValueError(
                "Partition input artifact keys do not match the compiled boundary"
            )
        input_paths = {}
        for boundary_key, artifact_id in input_artifacts.items():
            path = input_root / f"{boundary_key}.part"
            self._download_partition_artifact(str(artifact_id), path)
            input_paths[boundary_key] = path

        expected_outputs = {item["key"] for item in partition.get("outputs") or []}
        seen_inputs = set()
        seen_outputs = set()
        output_paths = {}
        for node in workflow.values():
            if node.get("class_type") == "CloudPartitionInput":
                key = str(node["inputs"]["boundary_key"])
                if key not in input_paths:
                    raise ValueError(f"Compiled partition input has no artifact: {key}")
                node["inputs"]["artifact_path"] = str(input_paths[key])
                seen_inputs.add(key)
            elif node.get("class_type") == "CloudPartitionOutput":
                key = str(node["inputs"]["boundary_key"])
                if key not in expected_outputs:
                    raise ValueError(f"Unexpected compiled partition output: {key}")
                path = output_root / f"{key}.part"
                node["inputs"]["output_path"] = str(path)
                output_paths[key] = path
                seen_outputs.add(key)
        if seen_inputs != expected_inputs or seen_outputs != expected_outputs:
            raise ValueError(
                "Compiled partition bridge nodes do not match its boundary manifest"
            )

        total_nodes = max(1, len(workflow))
        finished_nodes: set[str] = set()
        last_progress_sent = 0.0
        last_cancel_check = 0.0
        cancel_requested = False
        first_sampler = False

        def should_cancel() -> bool:
            nonlocal last_cancel_check, cancel_requested
            if cancel_requested:
                return True
            now = time.monotonic()
            if now - last_cancel_check < 1.0:
                return False
            last_cancel_check = now
            current = self.queue.get(job.id)
            cancel_requested = bool(
                current
                and current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}
                and str(current.error or "").lower().startswith("cancel")
            )
            return cancel_requested

        def relay(event: dict) -> None:
            nonlocal last_progress_sent, first_sampler
            event_type = str(event.get("type") or "")
            node_id = event.get("node_id")
            node = workflow.get(str(node_id)) or {}
            if (
                not first_sampler
                and event_type == "executing"
                and "sampler" in str(node.get("class_type") or "").lower()
            ):
                first_sampler = True
                self._phase_event(job, "first_sampler", node_id=str(node_id))
            if event_type == "executed" and node_id is not None:
                finished_nodes.add(str(node_id))
            elif event_type == "execution_cached":
                for cached in (event.get("data") or {}).get("nodes") or []:
                    finished_nodes.add(str(cached))
            now = time.monotonic()
            if event_type == "progress":
                data = event.get("data") or {}
                maximum = max(1, int(data.get("max") or 1))
                fraction = max(0.0, min(1.0, float(data.get("value") or 0) / maximum))
                overall = (len(finished_nodes) + fraction) / total_nodes
                if now - last_progress_sent < 0.25 and fraction < 1.0:
                    return
                last_progress_sent = now
            else:
                overall = len(finished_nodes) / total_nodes
            progress = max(10, min(95, 10 + round(overall * 85)))
            setter = getattr(self.queue, "set_progress", None)
            if callable(setter):
                setter(job.id, progress)
            publish(
                {
                    **event,
                    "partition_id": partition.get("partition_id"),
                    "overall_progress": progress,
                }
            )

        result = executor.execute(
            workflow,
            timeout_seconds=int(request.get("timeout_seconds", 3600)),
            event_callback=relay,
            cancel_check=should_cancel,
        )
        publish(
            {
                "type": "partition_uploading",
                "partition_id": partition.get("partition_id"),
            }
        )
        output_artifacts = {}
        for boundary_key, path in output_paths.items():
            self._raise_if_cancelled(job)
            if not path.is_file():
                raise RuntimeError(
                    f"ComfyUI produced no partition output: {boundary_key}"
                )
            output_artifacts[boundary_key] = self._upload_partition_artifact(path)[
                "artifact_id"
            ]
        return {
            "schema": "comfy.partition.result.v1",
            "partition_id": partition.get("partition_id"),
            "prompt_id": result.get("prompt_id"),
            "output_artifacts": output_artifacts,
            "outputs": result.get("outputs") or {},
        }


def run_worker(
    config_path: str | None = None,
    poll_interval: int = 10,
    max_jobs: int | None = None,
):
    """Entry point for running a worker."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if config_path:
        config = CloudConfig.from_file(config_path)
    else:
        config = CloudConfig.load()

    worker = Worker(config)
    worker.run(poll_interval=poll_interval, max_jobs=max_jobs)
