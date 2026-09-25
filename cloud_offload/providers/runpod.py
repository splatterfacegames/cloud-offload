"""RunPod cloud connector using the official REST API v2 and GraphQL.

RunPod is the default Cloud Offload provider. Everything the connector needs
rides on REST v2 (``https://api.runpod.io/v2``) except the account balance,
which v2 does not expose yet and still comes from GraphQL.
"""

from __future__ import annotations

import base64
import math
import os
import time
import uuid
from typing import Any
from urllib.parse import quote
from urllib.parse import urlencode

from cloud_offload.config import RUNPOD_NETWORK_VOLUME_MAX_GB
from cloud_offload.config import RUNPOD_NETWORK_VOLUME_MIN_GB
from cloud_offload.providers.base import (
    CloudConnector,
    Instance,
    PlacementConstraints,
    PlacementError,
    ProviderStorage,
)

ACCOUNT_QUERY = """
query CloudOffloadAccountBalance {
  myself {
    clientBalance
    currentSpendPerHr
  }
}
"""

_USER_AGENT = "cloud-offload-connector/0.1"

# A pod created with a host-local persistent mount: v2 rejects anything under
# 10 GB, so the connector refuses such a configuration before renting.
RUNPOD_PERSISTENT_MOUNT_MIN_GB = 10

# v2 reports stock as an AvailabilityLevel enum. Everything except NONE is
# treated as placeable; the legacy GraphQL wordings stay in the set so an
# unexpected string is still read conservatively.
_UNAVAILABLE_STOCK = {"", "none", "unavailable", "out of stock", "no stock"}

# RunPod publishes S3-compatible endpoints only for these datacenters. Keep
# discovery explicit: fabricating an endpoint for another volume produces an
# authentication-looking failure and hides that coordinator prepopulation is
# unsupported there.
RUNPOD_S3_ENDPOINTS = {
    dc: f"https://s3api-{dc.lower()}.runpod.io/"
    for dc in (
        "EU-CZ-1", "EU-RO-1", "EUR-IS-1", "EUR-NO-1", "US-CA-2",
        "US-GA-2", "US-IL-1", "US-KS-2", "US-MD-1", "US-MO-1",
        "US-MO-2", "US-NC-1", "US-NC-2", "US-NE-1", "US-WA-1",
    )
}


class RunPodApiError(RuntimeError):
    """A RunPod REST v2 error, carrying its RFC 9457 problem details."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class RunPodConnector(CloudConnector):
    """Provision Cloud Offload workers as RunPod GPU pods."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        graphql_url: str = "https://api.runpod.io/graphql",
        rest_url: str = "https://api.runpod.io/v2",
        cloud_type: str = "SECURE",
        container_disk_gb: int = 20,
        volume_gb: int = 0,
        registry_auth_id: str = "",
        launch_timeout: int = 300,
        poll_interval: float = 5.0,
        http_client: Any | None = None,
    ):
        self.api_key = api_key or os.environ.get("RUNPOD_API_KEY")
        if not self.api_key:
            raise ValueError("RUNPOD_API_KEY required")

        if http_client is None:
            try:
                import requests
            except ImportError as exc:
                raise ImportError("requests required: pip install requests") from exc
            http_client = requests

        cloud_type = cloud_type.upper()
        if cloud_type not in {"SECURE", "COMMUNITY"}:
            raise ValueError("RunPod cloud_type must be SECURE or COMMUNITY")

        self.http = http_client
        self.graphql_url = graphql_url.rstrip("/")
        self.rest_url = rest_url.rstrip("/")
        self.cloud_type = cloud_type
        self.container_disk_gb = container_disk_gb
        if 0 < int(volume_gb) < RUNPOD_PERSISTENT_MOUNT_MIN_GB:
            raise ValueError(
                "RunPod host-local volume must be 0 or at least "
                f"{RUNPOD_PERSISTENT_MOUNT_MIN_GB} GB"
            )
        self.volume_gb = volume_gb
        self.registry_auth_id = registry_auth_id.strip()
        self.launch_timeout = launch_timeout
        self.poll_interval = poll_interval

    @property
    def name(self) -> str:
        return "runpod"

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
        }

    def _graphql(self, query: str, variables: dict | None = None) -> dict:
        response = self.http.request(
            "POST",
            self.graphql_url,
            headers=self._headers,
            json={"query": query, "variables": variables or {}},
            timeout=30,
        )
        status_code = int(getattr(response, "status_code", 0) or 0)
        # RunPod retires GraphQL in early 2027 with a 410 whose body names the
        # replacement; surface that verbatim instead of a bare HTTP error.
        if status_code == 410:
            raise RunPodApiError(
                self._problem_message(response, status_code), status_code=410
            )
        response.raise_for_status()
        payload = response.json()
        errors = payload.get("errors") or []
        if errors:
            message = errors[0].get("message", "unknown GraphQL error")
            raise RuntimeError(f"RunPod API error: {message}")
        return payload.get("data", {})

    def _rest(self, method: str, endpoint: str, **kwargs) -> Any:
        timeout = kwargs.pop("timeout", 30)
        response = self.http.request(
            method,
            f"{self.rest_url}/{endpoint.lstrip('/')}",
            headers=self._headers,
            timeout=timeout,
            **kwargs,
        )
        status_code = int(getattr(response, "status_code", 0) or 0)
        if status_code >= 400:
            raise RunPodApiError(
                self._problem_message(response, status_code),
                status_code=status_code,
            )
        if status_code == 204 or not response.content:
            return None
        return response.json()

    @staticmethod
    def _problem_message(response: Any, status_code: int) -> str:
        """Render a v2 RFC 9457 problem document as one loggable sentence."""
        problem: Any = None
        try:
            problem = response.json()
        except Exception:
            problem = None
        if not isinstance(problem, dict):
            return f"RunPod API error {status_code}"
        parts = [
            str(problem.get("title") or "").strip(),
            str(problem.get("detail") or problem.get("message") or "").strip(),
        ]
        violations = problem.get("errors")
        if isinstance(violations, list) and violations:
            parts.append("; ".join(str(item) for item in violations))
        summary = ": ".join(part for part in parts if part)
        return f"RunPod API error {status_code}" + (f": {summary}" if summary else "")

    @staticmethod
    def _status_code(exc: Exception) -> int | None:
        if isinstance(exc, RunPodApiError):
            return exc.status_code
        return getattr(getattr(exc, "response", None), "status_code", None)

    def list_available(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int | None = None,
        max_hourly_rate: float | None = None,
        placement: PlacementConstraints | None = None,
    ) -> list[dict]:
        """List normalized GPU types with current on-demand prices."""
        self._validate_cloud_placement(placement)
        requested_datacenters = (
            set(placement.datacenter_ids)
            if placement is not None and placement.datacenter_ids
            else set()
        )
        if requested_datacenters:
            self._require_known_datacenters(requested_datacenters)
        # The catalog quotes a price per cloud tier and reports stock only for
        # the tier and product asked for, so both are stated: without them the
        # cheaper tier can be quoted even though launch() requests the other.
        gpu_types = (
            self._rest(
                "GET",
                "catalog/gpus",
                params={
                    "include": "AVAILABILITY",
                    "product": "POD",
                    "cloud": self.cloud_type,
                    "count": 1,
                },
            )
            or {}
        ).get("gpus") or []
        price_key = "secure" if self.cloud_type == "SECURE" else "community"
        offers: list[dict] = []
        for gpu in gpu_types:
            stock = self._datacenter_stock(gpu, requested_datacenters)
            if requested_datacenters and not stock:
                continue
            if gpu_type and not self._matches_gpu(gpu_type, gpu):
                continue
            memory_gb = int(gpu.get("memory") or 0)
            if min_gpu_ram is not None and memory_gb < min_gpu_ram:
                continue
            if self.cloud_type == "SECURE" and not gpu.get("secure"):
                continue
            if self.cloud_type == "COMMUNITY" and not gpu.get("community"):
                continue

            hourly_rate = (gpu.get("price") or {}).get(price_key)
            if hourly_rate is None:
                continue
            hourly_rate = float(hourly_rate)
            if max_hourly_rate is not None and hourly_rate > max_hourly_rate:
                continue

            offer = {
                    "id": str(gpu["id"]),
                    "provider": self.name,
                    "gpu_type": gpu.get("name") or gpu["id"],
                    "gpu_count": 1,
                    "gpu_ram_gb": memory_gb,
                    "hourly_rate": hourly_rate,
                    "cloud_type": self.cloud_type,
                    "raw": gpu,
                }
            if placement is not None:
                offer["datacenter_ids"] = list(placement.datacenter_ids)
                offer["storage_compatible"] = bool(placement.storage_attachments)
                if stock:
                    offer["datacenter_stock"] = stock
            offers.append(offer)
        return offers

    def _require_known_datacenters(self, requested: set[str]) -> None:
        """Refuse a datacenter RunPod does not currently offer.

        A retired or mistyped datacenter would otherwise look like an empty
        catalog, which reads as "no GPU is in stock there" rather than "that
        placement can never be satisfied".
        """
        data_centers = (self._rest("GET", "catalog/datacenters") or {}).get(
            "dataCenters"
        ) or []
        reported = {str(item.get("id")) for item in data_centers}
        missing = sorted(requested - reported)
        if missing:
            raise PlacementError(
                f"RunPod does not offer datacenter(s) {missing}"
            )

    def _datacenter_stock(
        self, gpu: dict, requested: set[str]
    ) -> list[dict[str, str]] | None:
        """Per-datacenter stock for one GPU type, restricted to ``requested``."""
        if not requested:
            return None
        stock: list[dict[str, str]] = []
        for availability in gpu.get("dataCenters") or []:
            datacenter_id = str(availability.get("id") or "")
            if datacenter_id not in requested:
                continue
            stock_status = str(availability.get("availability") or "").strip()
            if not self._stock_available(stock_status):
                continue
            stock.append(
                {"datacenter_id": datacenter_id, "stock_status": stock_status}
            )
        return stock

    @staticmethod
    def _stock_available(status: str) -> bool:
        return " ".join(status.lower().split()) not in _UNAVAILABLE_STOCK

    @staticmethod
    def _matches_gpu(requested: str, gpu: dict) -> bool:
        def normalize(value: str) -> str:
            return " ".join(value.replace("_", " ").replace("-", " ").lower().split())

        needle = normalize(requested)
        candidates = (
            normalize(str(gpu.get("id", ""))),
            normalize(str(gpu.get("name", ""))),
        )
        return any(needle == candidate or needle in candidate for candidate in candidates)

    def launch(
        self,
        offer_id: str,
        docker_image: str,
        env_vars: dict | None = None,
        startup_script: str | None = None,
        disk_gb: int | None = None,
        placement: PlacementConstraints | None = None,
        resource_name: str | None = None,
        min_cuda_version: str | None = None,
    ) -> Instance:
        """Launch a RunPod pod and wait until it reaches running state.

        ``disk_gb`` is the caller's planned container disk; without one the pod
        gets the configured default, which is what every launch used before
        partitions could be sized.
        """
        from cloud_offload.profiles import normalized_cuda_version

        min_cuda_version = normalized_cuda_version(min_cuda_version)
        self._ensure_image_pullable(docker_image)
        container_disk_gb = int(disk_gb) if disk_gb else self.container_disk_gb
        self._validate_cloud_placement(placement)
        attachment = (
            placement.storage_attachments[0]
            if placement and placement.storage_attachments
            else None
        )
        if attachment:
            volume = self.get_storage(attachment.provider_volume_id)
            if volume is None:
                raise PlacementError(
                    f"RunPod network volume {attachment.provider_volume_id} was not found"
                )
            allowed = set(placement.datacenter_ids or ())
            if allowed and volume.datacenter_id not in allowed:
                raise PlacementError(
                    f"RunPod network volume {volume.id} is in {volume.datacenter_id}, "
                    f"outside requested datacenters {sorted(allowed)}"
                )
            if attachment.datacenter_id and volume.datacenter_id != attachment.datacenter_id:
                raise PlacementError(
                    f"RunPod network volume {volume.id} is in {volume.datacenter_id}, "
                    f"not {attachment.datacenter_id}"
                )

        pod_input: dict[str, Any] = {
            "name": str(resource_name or f"cloud-offload-worker-{uuid.uuid4().hex[:8]}"),
            "image": docker_image,
            "cloud": self.cloud_type,
            "disk": container_disk_gb,
            "env": {str(key): str(value) for key, value in (env_vars or {}).items()},
            "gpu": {"id": offer_id, "count": 1},
            # ssh.direct, which the dispatcher connects over, needs the port
            # published explicitly; startSsh alone only injects PUBLIC_KEY.
            "ports": ["22/tcp"],
            "startSsh": True,
        }
        if min_cuda_version:
            pod_input["gpu"]["minCudaVersion"] = min_cuda_version
        if attachment:
            pod_input["mounts"] = {
                "network": [
                    {
                        "volumeId": attachment.provider_volume_id,
                        "path": attachment.mount_path,
                    }
                ]
            }
        elif self.volume_gb:
            pod_input["mounts"] = {
                "persistent": {"size": self.volume_gb, "path": "/workspace"}
            }
        if placement is not None and placement.datacenter_ids:
            pod_input["dataCenterIds"] = list(placement.datacenter_ids)
        if self.registry_auth_id:
            pod_input["registry"] = self.registry_auth_id
        if startup_script:
            encoded = base64.b64encode(startup_script.encode("utf-8")).decode("ascii")
            pod_input["args"] = "bash -lc 'echo " + encoded + " | base64 -d | bash'"
        try:
            pod = self._rest("POST", "pods", json=pod_input)
        except Exception as exc:
            if placement is None or not placement.datacenter_ids:
                raise
            raise PlacementError(
                "RunPod could not create a Pod in the storage-compatible "
                f"datacenter(s) {list(placement.datacenter_ids)}: {exc}"
            ) from exc
        if not pod or not pod.get("id"):
            raise RuntimeError("RunPod pod creation returned no pod ID")
        return self._wait_for_ready(str(pod["id"]))

    def _ensure_image_pullable(self, docker_image: str) -> None:
        """Fail before renting a pod when a private GHCR image has no auth ID."""
        if self.registry_auth_id or not docker_image.lower().startswith("ghcr.io/"):
            return
        repository = docker_image.split("@", 1)[0]
        tail = repository.rsplit("/", 1)[-1]
        if ":" in tail:
            repository = repository.rsplit(":", 1)[0]
        repository = repository[len("ghcr.io/") :]
        query = urlencode(
            {"service": "ghcr.io", "scope": f"repository:{repository}:pull"}
        )
        response = self.http.request(
            "GET",
            f"https://ghcr.io/token?{query}",
            headers={"User-Agent": _USER_AGENT},
            timeout=30,
        )
        if response.status_code in {401, 403}:
            raise RuntimeError(
                "GHCR image is private but RUNPOD_REGISTRY_AUTH_ID is not configured; "
                "refused to rent a pod that cannot pull its runner image"
            )
        response.raise_for_status()

    def _wait_for_ready(self, instance_id: str) -> Instance:
        deadline = time.monotonic() + self.launch_timeout
        while time.monotonic() < deadline:
            instance = self.get_instance(instance_id)
            if instance and instance.status == "running":
                return instance
            time.sleep(self.poll_interval)
        raise TimeoutError(
            f"RunPod instance {instance_id} did not start within {self.launch_timeout}s"
        )

    def get_instance(self, instance_id: str) -> Instance | None:
        """Get a RunPod pod by ID."""
        try:
            data = self._rest("GET", f"pods/{quote(instance_id, safe='')}")
        except Exception as exc:
            if self._status_code(exc) == 404:
                return None
            raise
        return self._parse_instance(data)

    def terminate(self, instance_id: str) -> bool:
        """Permanently terminate a RunPod pod."""
        try:
            self._rest("DELETE", f"pods/{quote(instance_id, safe='')}")
            return True
        except Exception:
            return False

    def list_instances(self) -> list[Instance]:
        """List active RunPod GPU pods."""
        pods = (self._rest("GET", "pods") or {}).get("pods") or []
        return [
            instance
            for pod in pods
            if pod.get("gpu")
            and (instance := self._parse_instance(pod)).status in {"pending", "running"}
        ]

    def list_instances_until(
        self,
        *,
        absolute_deadline: float,
        timeout_budget: float | None = None,
    ) -> list[Instance]:
        """List active pods without letting the HTTP call exceed the budget."""
        remaining = float(absolute_deadline) - time.monotonic()
        if timeout_budget is not None:
            remaining = min(remaining, float(timeout_budget))
        if remaining <= 0:
            raise TimeoutError("provider_inventory_deadline")
        attempt_total = min(30.0, remaining)
        connect_timeout = min(5.0, attempt_total / 2)
        read_timeout = attempt_total - connect_timeout
        pods = (
            self._rest(
                "GET", "pods", timeout=(connect_timeout, read_timeout)
            )
            or {}
        ).get("pods") or []
        return [
            instance
            for pod in pods
            if pod.get("gpu")
            and (instance := self._parse_instance(pod)).status
            in {"pending", "running"}
        ]

    @staticmethod
    def s3_endpoint(datacenter_id: str) -> str | None:
        """Return a published RunPod S3 endpoint, never a guessed endpoint."""
        return RUNPOD_S3_ENDPOINTS.get(str(datacenter_id).upper())

    def list_storage(self) -> list[ProviderStorage]:
        volumes = (self._rest("GET", "network-volumes") or {}).get(
            "networkVolumes"
        ) or []
        return [self._parse_storage(item) for item in volumes]

    def get_storage(self, storage_id: str) -> ProviderStorage | None:
        try:
            item = self._rest(
                "GET", f"network-volumes/{quote(str(storage_id), safe='')}"
            )
        except Exception as exc:
            if self._status_code(exc) == 404:
                return None
            raise
        return self._parse_storage(item) if item else None

    def create_storage(
        self, *, name: str, size_gb: int, datacenter_id: str
    ) -> ProviderStorage:
        if self.cloud_type != "SECURE":
            raise PlacementError("RunPod network volumes require Secure Cloud")
        if not (
            RUNPOD_NETWORK_VOLUME_MIN_GB
            <= int(size_gb)
            <= RUNPOD_NETWORK_VOLUME_MAX_GB
        ):
            raise ValueError(
                f"RunPod storage size must be {RUNPOD_NETWORK_VOLUME_MIN_GB}-"
                f"{RUNPOD_NETWORK_VOLUME_MAX_GB} GB"
            )
        if not str(datacenter_id).strip():
            raise ValueError("RunPod storage needs a datacenter")
        item = self._rest(
            "POST",
            "network-volumes",
            json={
                "name": str(name),
                "size": int(size_gb),
                "dataCenter": str(datacenter_id),
            },
        )
        if not item or not item.get("id"):
            raise RuntimeError("RunPod network volume creation returned no ID")
        return self._parse_storage(item)

    def delete_storage(self, storage_id: str) -> bool:
        try:
            self._rest("DELETE", f"network-volumes/{quote(str(storage_id), safe='')}")
            return True
        except Exception:
            return False

    def _parse_storage(self, item: dict) -> ProviderStorage:
        datacenter_id = str(item.get("dataCenter") or "")
        return ProviderStorage(
            id=str(item["id"]),
            provider=self.name,
            name=str(item.get("name") or item["id"]),
            size_gb=int(item.get("size") or 0),
            datacenter_id=datacenter_id,
            s3_compatible=self.s3_endpoint(datacenter_id) is not None,
            metadata={"s3_endpoint": self.s3_endpoint(datacenter_id)},
        )

    def _validate_cloud_placement(
        self, placement: PlacementConstraints | None
    ) -> None:
        if not placement:
            return
        if any(item.read_only for item in placement.storage_attachments):
            raise PlacementError(
                "RunPod Pod network-volume attachments cannot enforce read-only mode"
            )
        if placement.storage_attachments and self.cloud_type != "SECURE":
            raise PlacementError(
                "RunPod network volumes are incompatible with Community Cloud; "
                "select Secure Cloud before provisioning"
            )

    def account_balance(self) -> dict:
        """Return RunPod account balance and current hourly spend.

        REST v2 publishes spend (``/v2/billing``) but no credit balance, so this
        is the connector's last GraphQL call. Once GraphQL is retired the
        balance is reported as unavailable rather than failing the whole
        credentials probe, which otherwise only needs REST.
        """
        try:
            data = self._graphql(ACCOUNT_QUERY)
        except RunPodApiError as exc:
            if exc.status_code != 410:
                raise
            return {
                "available": False,
                "currency": "USD",
                "error": str(exc),
            }
        account = data.get("myself") or {}
        return {
            "available": True,
            "currency": "USD",
            "balance": float(account.get("clientBalance") or 0),
            "current_spend_per_hour": float(account.get("currentSpendPerHr") or 0),
        }

    def _parse_instance(self, data: dict) -> Instance:
        provider_state = str(data.get("status", "")).upper()
        status = {
            "PROVISIONING": "pending",
            "STARTING": "pending",
            "RUNNING": "running",
            "EXITED": "stopped",
            # An errored pod still bills while its resources exist, so it is
            # reported like a stopped one to make the dispatcher reclaim it.
            "ERROR": "stopped",
            "TERMINATED": "terminated",
        }.get(provider_state, "unknown")

        gpu = data.get("gpu") or {}
        ip_address, ssh_port = self._ssh_endpoint(data)
        runtime = data.get("runtime")
        raw_uptime = (
            runtime.get("uptimeInSeconds") if isinstance(runtime, dict) else None
        )
        try:
            container_uptime = (
                float(raw_uptime) if raw_uptime is not None else None
            )
        except (TypeError, ValueError):
            container_uptime = None
        if container_uptime is not None and (
            not math.isfinite(container_uptime) or container_uptime < 0
        ):
            container_uptime = None

        return Instance(
            id=str(data["id"]),
            provider=self.name,
            gpu_type=str(gpu.get("id") or "unknown"),
            gpu_count=int(gpu.get("count") or 1),
            hourly_rate=float(data.get("cost") or 0),
            status=status,
            ip_address=ip_address,
            ssh_port=int(ssh_port) if ssh_port is not None else None,
            metadata={
                "name": data.get("name"),
                "image": data.get("image"),
                "location": data.get("dataCenterId"),
                "container_uptime_seconds": container_uptime,
                "provider_state": provider_state or "UNKNOWN",
            },
        )

    def container_started(self, instance: Instance) -> bool | None:
        """RunPod reports container uptime when its telemetry is available.

        A pod can sit "RUNNING" (rented and billing) while its host never
        creates the container, so pod status alone cannot answer this. The
        API also omits the whole ``runtime`` block for pods whose containers
        are demonstrably executing, so an absent block means the telemetry is
        unavailable — not that the container never started.
        """
        uptime = instance.metadata.get("container_uptime_seconds")
        if uptime is None:
            return None
        uptime = float(uptime)
        if uptime == 0:
            return False
        if uptime > 0:
            return True
        return None

    @staticmethod
    def _ssh_endpoint(data: dict) -> tuple[str | None, int | None]:
        """Host and port for direct SSH, preferring the pod's own ssh block.

        ``ssh.direct`` is what RunPod publishes for the mapped 22/tcp port; the
        runtime port list is read as a fallback so a pod that reports ports
        before the ssh block is populated is still reachable.
        """
        direct = ((data.get("ssh") or {}).get("direct")) or {}
        if direct.get("host") and direct.get("port"):
            return str(direct["host"]), int(direct["port"])
        for port in (data.get("runtime") or {}).get("ports") or []:
            if int(port.get("private") or 0) == 22 and port.get("public"):
                return port.get("ip"), int(port["public"])
        return None, None


# Symmetric compatibility name for callers that still use provider terminology.
RunPodProvider = RunPodConnector
