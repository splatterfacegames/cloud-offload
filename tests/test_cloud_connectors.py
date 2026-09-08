import base64

import pytest

from cloud_offload.benchmark import CoordinatorBenchmarkDriver
from cloud_offload.config import CloudConfig
from cloud_offload.providers import (
    connector_metadata,
    connector_names,
    create_connector,
    register_connector,
)
from cloud_offload.providers.base import (
    CloudConnector,
    PlacementConstraints,
    PlacementError,
)
from cloud_offload.providers.runpod import RunPodApiError
from cloud_offload.providers.runpod import RunPodConnector


class FakeResponse:
    def __init__(self, payload=None, *, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.content = b"" if payload is None else b"json"
        self.text = "" if payload is None else "json"

    def raise_for_status(self):
        if self.status_code >= 400:
            error = RuntimeError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def json(self):
        return self.payload


class FakeHttp:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError(f"Unexpected request: {method} {url}")
        return self.responses.pop(0)


def test_builtin_connector_registry_and_vast_compatibility():
    from cloud_offload.providers.declarative import DeclarativeRestConnector

    assert connector_names() == ("runpod", "vast.ai")

    config = CloudConfig(vast_api_key="vast-secret")
    connector = create_connector("vast", config)

    # Vast.ai is served by the bundled declarative spec now, but the canonical
    # name, the "vast" alias and the legacy credential field are unchanged, so
    # existing provider_order and VAST_API_KEY configuration keep working.
    assert isinstance(connector, DeclarativeRestConnector)
    assert connector.name == "vast.ai"
    assert connector_metadata("vast.ai")["kind"] == "declarative"


def test_runpod_is_the_default_provider():
    config = CloudConfig()

    assert config.provider == "runpod"
    assert config.provider_order[0] == "runpod"
    connector = create_connector("runpod", CloudConfig(runpod_api_key="secret"))
    assert connector.name == "runpod"


def test_runpod_inventory_clamps_real_http_timeout_to_absolute_deadline(monkeypatch):
    http = FakeHttp(FakeResponse({"pods": []}))
    connector = RunPodConnector(api_key="secret", http_client=http)
    monkeypatch.setattr(
        "cloud_offload.providers.runpod.time.monotonic", lambda: 100.0
    )

    assert connector.list_instances_until(absolute_deadline=101.0) == []

    assert len(http.requests) == 1
    timeout = http.requests[0][2]["timeout"]
    assert isinstance(timeout, tuple)
    assert 0 < sum(timeout) <= 1.0


def test_runpod_inventory_makes_no_http_call_after_deadline(monkeypatch):
    http = FakeHttp()
    connector = RunPodConnector(api_key="secret", http_client=http)
    monkeypatch.setattr(
        "cloud_offload.providers.runpod.time.monotonic", lambda: 100.0
    )

    with pytest.raises(TimeoutError):
        connector.list_instances_until(absolute_deadline=100.0)

    assert http.requests == []


def test_benchmark_inventory_propagates_deadline_through_runpod_http(monkeypatch):
    http = FakeHttp(FakeResponse({"pods": []}))
    connector = RunPodConnector(api_key="secret", http_client=http)
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    driver.connectors = {"runpod": connector}
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: 100.0)
    monkeypatch.setattr(
        "cloud_offload.providers.runpod.time.monotonic", lambda: 100.0
    )

    assert driver.inventory(("runpod",), absolute_deadline=101.0) == {
        "runpod": {}
    }

    assert len(http.requests) == 1
    timeout = http.requests[0][2]["timeout"]
    assert isinstance(timeout, tuple)
    assert 0 < sum(timeout) <= 1.0


def test_custom_connector_can_be_registered(tmp_path):
    class ExampleConnector(CloudConnector):
        name = "example"

        def list_available(self, **kwargs):
            return []

        def launch(self, *args, **kwargs):
            raise NotImplementedError

        def get_instance(self, instance_id):
            return None

        def terminate(self, instance_id):
            return True

        def list_instances(self):
            return []

    name = f"example-{tmp_path.name}"
    register_connector(name, lambda config: ExampleConnector())

    assert isinstance(create_connector(name, CloudConfig()), ExampleConnector)


def test_runpod_gpu_discovery_normalizes_and_filters_offers():
    http = FakeHttp(
        FakeResponse(
            {
                "gpus": [
                    {
                        "id": "NVIDIA GeForce RTX 4090",
                        "name": "RTX 4090",
                        "memory": 24,
                        "secure": True,
                        "community": True,
                        "price": {"secure": 0.44, "community": 0.31},
                    },
                    {
                        "id": "NVIDIA A100 80GB PCIe",
                        "name": "A100 PCIe",
                        "memory": 80,
                        "secure": True,
                        "community": True,
                        "price": {"secure": 1.19, "community": 1.19},
                    },
                ]
            }
        )
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    offers = connector.list_available(gpu_type="RTX_4090", min_gpu_ram=24, max_hourly_rate=0.50)

    assert offers == [
        {
            "id": "NVIDIA GeForce RTX 4090",
            "provider": "runpod",
            "gpu_type": "RTX 4090",
            "gpu_count": 1,
            "gpu_ram_gb": 24,
            "hourly_rate": 0.44,
            "cloud_type": "SECURE",
            "raw": {
                "id": "NVIDIA GeForce RTX 4090",
                "name": "RTX 4090",
                "memory": 24,
                "secure": True,
                "community": True,
                "price": {"secure": 0.44, "community": 0.31},
            },
        }
    ]
    method, url, kwargs = http.requests[0]
    assert (method, url) == ("GET", "https://api.runpod.io/v2/catalog/gpus")
    assert kwargs["params"] == {
        "include": "AVAILABILITY",
        "product": "POD",
        "cloud": "SECURE",
        "count": 1,
    }


def test_runpod_community_gpu_discovery_requests_community_price():
    http = FakeHttp(
        FakeResponse(
            {
                "gpus": [
                    {
                        "id": "NVIDIA GeForce RTX 4090",
                        "name": "RTX 4090",
                        "memory": 24,
                        "secure": True,
                        "community": True,
                        "price": {"secure": 0.44, "community": 0.31},
                    }
                ]
            }
        )
    )
    connector = RunPodConnector(
        api_key="secret", cloud_type="COMMUNITY", http_client=http
    )

    offers = connector.list_available()

    assert [offer["hourly_rate"] for offer in offers] == [0.31]
    assert http.requests[0][2]["params"]["cloud"] == "COMMUNITY"


def test_runpod_community_discovery_skips_secure_only_gpus():
    http = FakeHttp(
        FakeResponse(
            {
                "gpus": [
                    {
                        "id": "NVIDIA H100 80GB HBM3",
                        "name": "H100 SXM",
                        "memory": 80,
                        "secure": True,
                        "community": False,
                        "price": {"secure": 2.69},
                    }
                ]
            }
        )
    )
    connector = RunPodConnector(
        api_key="secret", cloud_type="COMMUNITY", http_client=http
    )

    assert connector.list_available() == []


def test_runpod_storage_placement_only_returns_stock_in_requested_datacenter():
    data_centers = {
        "dataCenters": [{"id": "US-MD-1"}, {"id": "US-GA-2"}]
    }
    gpu_types = {
        "gpus": [
            {
                "id": "NVIDIA RTX 2000 Ada Generation",
                "name": "RTX 2000 Ada",
                "memory": 16,
                "secure": True,
                "community": False,
                "price": {"secure": 0.24},
                "availability": "HIGH",
                "dataCenters": [
                    {"id": "US-MD-1", "availability": "NONE"},
                    {"id": "US-GA-2", "availability": "HIGH"},
                ],
            },
            {
                "id": "NVIDIA A100-SXM4-80GB",
                "name": "A100 SXM",
                "memory": 80,
                "secure": True,
                "community": False,
                "price": {"secure": 1.49},
                "availability": "LOW",
                "dataCenters": [{"id": "US-MD-1", "availability": "LOW"}],
            },
        ]
    }
    http = FakeHttp(FakeResponse(data_centers), FakeResponse(gpu_types))
    connector = RunPodConnector(api_key="secret", http_client=http)

    offers = connector.list_available(
        placement=PlacementConstraints(datacenter_ids=("US-MD-1",))
    )

    assert [offer["id"] for offer in offers] == ["NVIDIA A100-SXM4-80GB"]
    assert offers[0]["datacenter_stock"] == [
        {"datacenter_id": "US-MD-1", "stock_status": "LOW"}
    ]
    assert http.requests[0][0:2] == (
        "GET",
        "https://api.runpod.io/v2/catalog/datacenters",
    )


def test_runpod_storage_placement_rejects_unknown_datacenter():
    http = FakeHttp(FakeResponse({"dataCenters": [{"id": "US-KS-2"}]}))
    connector = RunPodConnector(api_key="secret", http_client=http)

    with pytest.raises(PlacementError, match="does not offer datacenter"):
        connector.list_available(
            placement=PlacementConstraints(datacenter_ids=("US-OLD-1",))
        )


@pytest.mark.parametrize("min_cuda_version", [None, "12.8"])
def test_runpod_launch_passes_worker_environment_and_startup_script(min_cuda_version):
    http = FakeHttp(
        FakeResponse({"id": "pod-1", "status": "PROVISIONING"}),
        FakeResponse(
            {
                "id": "pod-1",
                "name": "cloud-offload-worker-test",
                "status": "RUNNING",
                "image": "pytorch/image:latest",
                "gpu": {"id": "NVIDIA GeForce RTX 4090", "count": 1},
                "cost": 0.44,
                "dataCenterId": "US-KS-2",
                "ssh": {
                    "direct": {
                        "host": "203.0.113.10",
                        "port": 22022,
                        "username": "root",
                    }
                },
            }
        ),
    )
    connector = RunPodConnector(
        api_key="secret",
        registry_auth_id="registry-auth-1",
        http_client=http,
        launch_timeout=1,
        poll_interval=0,
    )

    instance = connector.launch(
        "NVIDIA GeForce RTX 4090",
        "pytorch/image:latest",
        env_vars={"CLOUD_OFFLOAD_WORKER_MODE": "true"},
        startup_script="cloud-offload worker --poll 10\n",
        min_cuda_version=min_cuda_version,
    )

    assert http.requests[0][0:2] == ("POST", "https://api.runpod.io/v2/pods")
    pod_input = http.requests[0][2]["json"]
    encoded_script = pod_input["args"].split("echo ", 1)[1].split(" ", 1)[0]
    assert base64.b64decode(encoded_script).decode() == "cloud-offload worker --poll 10\n"
    assert pod_input["env"] == {"CLOUD_OFFLOAD_WORKER_MODE": "true"}
    assert pod_input["gpu"] == {
        "id": "NVIDIA GeForce RTX 4090", "count": 1,
        **({"minCudaVersion": min_cuda_version} if min_cuda_version else {}),
    }
    assert pod_input["image"] == "pytorch/image:latest"
    assert pod_input["cloud"] == "SECURE"
    assert pod_input["disk"] == 20
    assert pod_input["ports"] == ["22/tcp"]
    assert pod_input["startSsh"] is True
    assert pod_input["registry"] == "registry-auth-1"
    assert "mounts" not in pod_input
    assert instance.status == "running"
    assert instance.gpu_type == "NVIDIA GeForce RTX 4090"
    assert instance.hourly_rate == 0.44
    assert instance.ip_address == "203.0.113.10"
    assert instance.ssh_port == 22022
    assert instance.metadata["location"] == "US-KS-2"


def test_runpod_launch_reads_ssh_endpoint_from_runtime_ports():
    http = FakeHttp(
        FakeResponse({"id": "pod-2", "status": "STARTING"}),
        FakeResponse(
            {
                "id": "pod-2",
                "status": "RUNNING",
                "gpu": {"id": "NVIDIA A40", "count": 1},
                "runtime": {
                    "ports": [
                        {
                            "private": 22,
                            "public": 34446,
                            "type": "tcp",
                            "ip": "203.0.113.20",
                        }
                    ]
                },
            }
        ),
    )
    connector = RunPodConnector(
        api_key="secret", http_client=http, launch_timeout=1, poll_interval=0
    )

    instance = connector.launch("NVIDIA A40", "pytorch/image:latest")

    assert (instance.ip_address, instance.ssh_port) == ("203.0.113.20", 34446)


def test_runpod_launch_reports_v2_problem_details():
    http = FakeHttp(
        FakeResponse(
            {
                "title": "Unprocessable Entity",
                "detail": "gpu.id is not available",
            },
            status_code=422,
        )
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    with pytest.raises(RunPodApiError, match="422: Unprocessable Entity: gpu.id"):
        connector.launch("NVIDIA A40", "pytorch/image:latest")


def test_runpod_refuses_private_ghcr_image_before_renting_pod():
    http = FakeHttp(FakeResponse(status_code=401))
    connector = RunPodConnector(api_key="secret", http_client=http)

    with pytest.raises(RuntimeError, match="RUNPOD_REGISTRY_AUTH_ID"):
        connector.launch(
            "NVIDIA GeForce RTX 4090",
            "ghcr.io/example/private@sha256:" + "a" * 64,
        )

    assert len(http.requests) == 1
    assert http.requests[0][0] == "GET"
    assert http.requests[0][1].startswith("https://ghcr.io/token?")


def test_runpod_lifecycle_uses_rest_api():
    http = FakeHttp(
        FakeResponse(
            {
                "pods": [
                    {
                        "id": "pod-running",
                        "status": "RUNNING",
                        "gpu": {"id": "NVIDIA A40", "count": 1},
                    },
                    {"id": "pod-exited", "status": "EXITED", "gpu": {"id": "NVIDIA A40"}},
                    {
                        "id": "pod-error",
                        "status": "ERROR",
                        "gpu": {"id": "NVIDIA A40"},
                    },
                    {"id": "pod-cpu", "status": "RUNNING", "cpu": {"id": "cpu3c-2-4"}},
                ]
            }
        ),
        FakeResponse(None, status_code=204),
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    instances = connector.list_instances()
    terminated = connector.terminate("pod-running")

    assert [instance.id for instance in instances] == ["pod-running"]
    assert terminated is True
    assert http.requests[0][0:2] == (
        "GET",
        "https://api.runpod.io/v2/pods",
    )
    assert http.requests[1][0:2] == (
        "DELETE",
        "https://api.runpod.io/v2/pods/pod-running",
    )


@pytest.mark.parametrize(
    ("pod_status", "expected"),
    [
        ("PROVISIONING", "pending"),
        ("STARTING", "pending"),
        ("RUNNING", "running"),
        ("EXITED", "stopped"),
        ("ERROR", "stopped"),
        ("TERMINATED", "terminated"),
        ("SOMETHING_NEW", "unknown"),
    ],
)
def test_runpod_maps_v2_pod_statuses(pod_status, expected):
    http = FakeHttp(FakeResponse({"id": "pod-1", "status": pod_status}))
    connector = RunPodConnector(api_key="secret", http_client=http)

    assert connector.get_instance("pod-1").status == expected


def test_runpod_reports_whether_the_container_actually_started():
    stalled_http = FakeHttp(
        FakeResponse(
            {"id": "pod-1", "status": "RUNNING", "runtime": {"uptimeInSeconds": 0}}
        )
    )
    started_http = FakeHttp(
        FakeResponse(
            {
                "id": "pod-2",
                "status": "RUNNING",
                "runtime": {"uptimeInSeconds": 42},
            }
        )
    )
    connector = RunPodConnector(api_key="secret", http_client=stalled_http)

    stalled = connector.get_instance("pod-1")
    started = RunPodConnector(
        api_key="secret", http_client=started_http
    ).get_instance("pod-2")

    assert connector.container_started(stalled) is False
    assert connector.container_started(started) is True
    assert stalled.metadata["provider_state"] == "RUNNING"
    assert started.metadata["provider_state"] == "RUNNING"


def test_runpod_absent_runtime_telemetry_is_unknown_not_stalled():
    """The API omits the runtime block for some pods whose containers are
    running, so a missing block must read as "telemetry unavailable" rather
    than "never started" — otherwise healthy pods get terminated."""
    http = FakeHttp(
        FakeResponse({"id": "pod-3", "status": "RUNNING", "runtime": None})
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    instance = connector.get_instance("pod-3")

    assert instance.metadata["container_uptime_seconds"] is None
    assert connector.container_started(instance) is None


def test_runpod_null_runtime_uptime_is_unknown_not_stalled():
    http = FakeHttp(
        FakeResponse(
            {
                "id": "pod-4",
                "status": "RUNNING",
                "runtime": {"uptimeInSeconds": None},
            }
        )
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    instance = connector.get_instance("pod-4")

    assert instance.metadata["container_uptime_seconds"] is None
    assert connector.container_started(instance) is None


def test_runpod_invalid_runtime_uptime_is_unknown_not_stalled():
    http = FakeHttp(
        FakeResponse(
            {
                "id": "pod-5",
                "status": "RUNNING",
                "runtime": {"uptimeInSeconds": -1},
            }
        )
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    instance = connector.get_instance("pod-5")

    assert instance.metadata["container_uptime_seconds"] is None
    assert connector.container_started(instance) is None


def test_runpod_get_instance_returns_none_for_missing_pod():
    http = FakeHttp(
        FakeResponse({"title": "Not Found", "detail": "pod not found"}, status_code=404)
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    assert connector.get_instance("pod-gone") is None


def test_connectors_normalize_account_balances():
    vast_http = FakeHttp(FakeResponse({"balance": 2.5, "credit": 8.0}))
    runpod_http = FakeHttp(
        FakeResponse(
            {"data": {"myself": {"clientBalance": 12.75, "currentSpendPerHr": 0.4}}}
        )
    )

    config = CloudConfig(vast_api_key="secret")
    vast_connector = create_connector("vast.ai", config)
    vast_connector.http = vast_http
    vast = vast_connector.account_balance()
    runpod = RunPodConnector(api_key="secret", http_client=runpod_http).account_balance()

    assert vast["balance"] == 2.5
    assert vast["credit"] == 8.0
    assert runpod["balance"] == 12.75
    assert runpod["current_spend_per_hour"] == 0.4


def test_runpod_balance_is_unavailable_once_graphql_is_retired():
    http = FakeHttp(
        FakeResponse(
            {"title": "Gone", "detail": "The GraphQL API has been retired"},
            status_code=410,
        )
    )

    balance = RunPodConnector(api_key="secret", http_client=http).account_balance()

    assert balance["available"] is False
    assert "retired" in balance["error"]


def test_cloud_config_loads_runpod_without_exposing_secret(monkeypatch):
    monkeypatch.setenv("CLOUD_OFFLOAD_PROVIDER", "runpod")
    monkeypatch.setenv("RUNPOD_API_KEY", "runpod-secret")
    monkeypatch.setenv("RUNPOD_CLOUD_TYPE", "community")
    monkeypatch.setenv("RUNPOD_REGISTRY_AUTH_ID", "registry-auth-1")

    config = CloudConfig.from_env()
    public = config.to_dict()

    assert config.provider == "runpod"
    assert config.runpod_api_key == "runpod-secret"
    assert config.runpod_cloud_type == "COMMUNITY"
    assert config.runpod_registry_auth_id == "registry-auth-1"
    assert public["provider_auth_configured"] is True
    assert "runpod-secret" not in repr(public)


def test_cloud_config_file_ignores_derived_public_status_fields(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(
        '{"cloud":{"provider":"runpod","provider_auth_configured":true,'
        '"worker_auth_configured":false,"runpod_container_disk_gb":40}}'
    )

    config = CloudConfig.from_file(path)

    assert config.provider == "runpod"
    assert config.runpod_container_disk_gb == 40
