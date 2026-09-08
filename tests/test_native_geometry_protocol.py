import pytest
import builtins

from cloud_offload import partition_protocol as protocol


@pytest.fixture(autouse=True)
def no_optional_runtime_imports(monkeypatch):
    original = builtins.__import__

    def checked(name, *args, **kwargs):
        if name.split(".")[0] in {"torch", "comfy", "comfy_api"}:
            raise AssertionError("Metadata validation must not load optional runtimes")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", checked)


def test_camera_metadata_validates_without_optional_runtime():
    protocol._validate_native_geometry("paint-cameras.v1", dict(
        elevs=[0, 90], azims=[0, 180], weights=[1, 0.5],
        ortho_scale=1.2, camera_distance=1.45, near=0.1, far=100,
    ))


@pytest.mark.parametrize("kind", ["mesh.v1", "paint-cameras.v1"])
def test_incomplete_native_geometry_rejected_before_import(kind):
    with pytest.raises(protocol.PartitionProtocolError, match="fields"):
        protocol._decode({"kind": kind, "fields": {"kind": "dict", "items": {}}}, {}, {})


@pytest.mark.parametrize("change", [
    {"weights": [1]}, {"azims": [float("nan")]}, {"near": 100},
    {"camera_distance": -1}, {"ortho_scale": True}, {"extra": 1},
])
def test_invalid_camera_contract_rejected(change):
    fields = dict(elevs=[0, 90], azims=[0, 180], weights=[1, 0.5],
                  ortho_scale=1.2, camera_distance=1.45, near=0.1, far=100)
    fields.update(change)
    with pytest.raises(protocol.PartitionProtocolError):
        protocol._validate_native_geometry("paint-cameras.v1", fields)
