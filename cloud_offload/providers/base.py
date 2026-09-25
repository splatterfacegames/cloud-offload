"""Base interface for pluggable cloud compute connectors."""

from abc import ABC, abstractmethod
from collections.abc import Collection
from dataclasses import dataclass, field


@dataclass(frozen=True)
class StorageAttachment:
    """One provider-owned durable volume attached by the control plane."""

    provider_volume_id: str
    mount_path: str = "/workspace"
    read_only: bool = False
    datacenter_id: str | None = None

    def __post_init__(self) -> None:
        if not self.provider_volume_id.strip():
            raise ValueError("Storage attachment requires provider_volume_id")
        if not self.mount_path.startswith("/"):
            raise ValueError("Storage attachment mount_path must be absolute")


@dataclass(frozen=True)
class PlacementConstraints:
    """Hard locality and storage constraints understood by capable connectors."""

    datacenter_ids: tuple[str, ...] = ()
    storage_attachments: tuple[StorageAttachment, ...] = ()

    def __post_init__(self) -> None:
        if len(self.storage_attachments) > 1:
            raise ValueError("Cloud Offload currently supports one storage attachment")
        attachment = self.storage_attachments[0] if self.storage_attachments else None
        if attachment and attachment.datacenter_id and self.datacenter_ids:
            if attachment.datacenter_id not in self.datacenter_ids:
                raise ValueError(
                    "Storage attachment datacenter is outside placement constraints"
                )


@dataclass(frozen=True)
class ProviderStorage:
    """Normalized durable storage returned by a provider connector."""

    id: str
    provider: str
    name: str
    size_gb: int
    datacenter_id: str
    s3_compatible: bool = False
    metadata: dict = field(default_factory=dict, compare=False)


class StorageUnsupportedError(NotImplementedError):
    """Connector does not offer provider-managed durable storage."""


class PlacementError(RuntimeError):
    """Requested storage or datacenter placement is invalid before launch."""


@dataclass
class Instance:
    """A cloud GPU instance."""

    id: str
    provider: str
    gpu_type: str
    gpu_count: int
    hourly_rate: float
    status: str  # pending, running, stopped, terminated
    ip_address: str | None = None
    ssh_port: int | None = None
    metadata: dict = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class CloudConnector(ABC):
    """Abstract connector for a cloud GPU compute service."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name."""

    @abstractmethod
    def list_available(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int | None = None,
        max_hourly_rate: float | None = None,
        placement: PlacementConstraints | None = None,
    ) -> list[dict]:
        """List available instances/offers. Returns list of offer dicts with pricing info."""

    @abstractmethod
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
        """Launch an instance with the given offer. Returns Instance object.

        ``disk_gb`` is the container disk the caller has planned for this job:
        the runner image plus everything it will stage, sized before renting
        rather than discovered when the pod runs out of space. A connector that
        cannot size its disk per launch may ignore it and use its configured
        default; ``None`` means the caller had nothing to say.
        ``resource_name`` is a durable, non-secret reconciliation key chosen
        before provider mutation.
        """

    @abstractmethod
    def get_instance(self, instance_id: str) -> Instance | None:
        """Get instance by ID."""

    @abstractmethod
    def terminate(self, instance_id: str) -> bool:
        """Terminate an instance."""

    def container_started(self, instance: Instance) -> bool | None:
        """Whether the instance's container has actually begun executing.

        A rented instance bills from creation even when its host never starts
        the container, so callers use this to distinguish "booting" from
        "stalled". ``None`` means this provider does not report container-level
        telemetry and no conclusion should be drawn.
        """
        return None

    @abstractmethod
    def list_instances(self) -> list[Instance]:
        """List all active instances for this provider."""

    def list_instances_until(
        self,
        *,
        absolute_deadline: float,
        timeout_budget: float | None = None,
    ) -> list[Instance]:
        """List active instances with a caller deadline when supported.

        Existing third-party connectors stay compatible. A connector that can
        control its transport timeout should override this method.
        """
        return self.list_instances()

    def find_cheapest(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int = 24,
        max_hourly_rate: float = 1.0,
        exclude: Collection[str] | None = None,
        placement: PlacementConstraints | None = None,
    ) -> dict | None:
        """Find the cheapest available offer matching criteria.

        ``exclude`` drops offers by id; the dispatcher uses it to route around
        hosts that recently refused a launch instead of retrying them forever.
        """
        arguments = {
            "gpu_type": gpu_type,
            "min_gpu_ram": min_gpu_ram,
            "max_hourly_rate": max_hourly_rate,
        }
        if placement is not None:
            arguments["placement"] = placement
        offers = self.list_available(**arguments)
        if exclude:
            excluded = {str(item) for item in exclude}
            offers = [o for o in offers if str(o.get("id")) not in excluded]
        if not offers:
            return None
        return min(offers, key=lambda x: x.get("hourly_rate", float("inf")))

    def list_storage(self) -> list[ProviderStorage]:
        raise StorageUnsupportedError(f"{self.name} does not manage durable storage")

    def get_storage(self, storage_id: str) -> ProviderStorage | None:
        raise StorageUnsupportedError(f"{self.name} does not manage durable storage")

    def create_storage(
        self, *, name: str, size_gb: int, datacenter_id: str
    ) -> ProviderStorage:
        raise StorageUnsupportedError(f"{self.name} does not manage durable storage")

    def delete_storage(self, storage_id: str) -> bool:
        raise StorageUnsupportedError(f"{self.name} does not manage durable storage")

    def account_balance(self) -> dict:
        """Return normalized account credit data when supported."""
        return {"available": False, "currency": "USD"}


# Backwards compatibility for code that implemented the original provider API.
CloudProvider = CloudConnector
