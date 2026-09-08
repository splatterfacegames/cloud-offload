"""Safe, versioned value transport for Cloud Offload graph partitions."""

from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any


SCHEMA = "comfy.partition.bundle.v1"
MANIFEST_NAME = "manifest.json"
TENSORS_NAME = "tensors.safetensors"
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_TREE_DEPTH = 64
MAX_TREE_ITEMS = 100_000
NON_PORTABLE_COMFY_TYPES = frozenset(
    {
        "MODEL",
        "CLIP",
        "VAE",
        "CONTROL_NET",
        "SAMPLER",
        "SIGMAS",
        "GUIDER",
        "NOISE",
        "HOOKS",
        "MODEL_PATCH",
    }
)


class PartitionProtocolError(ValueError):
    """The value or bundle violates the cloud partition protocol."""


def validate_boundary_type(type_name: str) -> None:
    """Reject known live-object types before a paid runner can be provisioned."""
    normalized = str(type_name or "").strip().upper()
    if normalized in NON_PORTABLE_COMFY_TYPES:
        raise PartitionProtocolError(
            f"{normalized} cannot cross a cloud partition boundary; move its "
            "loader or producer inside the cloud partition"
        )
    if normalized == "*" or not normalized:
        raise PartitionProtocolError(
            "An unresolved wildcard boundary is not portable; connect a concrete type"
        )



MESH_FIELDS = (
    "vertices", "faces", "uvs", "vertex_colors", "texture", "metallic_roughness",
    "vertex_counts", "face_counts", "unlit", "normals", "tangents", "normal_map",
    "occlusion_in_mr", "material", "emissive",
)
CAMERA_FIELDS = ("elevs", "azims", "weights", "ortho_scale", "camera_distance", "near", "far")


def _native_geometry_type(kind):
    # These are fixed imports, never module or class names supplied by a bundle.
    if kind == "mesh.v1":
        from comfy_api.latest._util.geometry_types import MESH
        return MESH, MESH_FIELDS
    from comfy.ldm.hunyuan3d.paint.render import Cameras
    return Cameras, CAMERA_FIELDS


def _validate_native_geometry(kind, fields):
    import math

    expected = MESH_FIELDS if kind == "mesh.v1" else CAMERA_FIELDS
    if not isinstance(fields, dict) or set(fields) != set(expected):
        raise PartitionProtocolError("Invalid native geometry fields")
    if kind == "mesh.v1":
        import torch

        for name in MESH_FIELDS:
            value = fields[name]
            if name in {"unlit", "occlusion_in_mr"}:
                if type(value) is not bool:
                    raise PartitionProtocolError("Invalid mesh flag")
            elif name == "material":
                if value is not None and not isinstance(value, dict):
                    raise PartitionProtocolError("Invalid mesh material")
            elif value is not None and not isinstance(value, torch.Tensor):
                raise PartitionProtocolError("Invalid mesh tensor")
        for name in ("vertices", "faces"):
            value = fields[name]
            if not isinstance(value, torch.Tensor) or value.ndim != 3 or value.shape[-1] != 3:
                raise PartitionProtocolError("Invalid mesh geometry shape")
        if fields["vertices"].shape[0] != fields["faces"].shape[0]:
            raise PartitionProtocolError("Mismatched mesh batches")
        if (fields["vertex_counts"] is None) != (fields["face_counts"] is None):
            raise PartitionProtocolError("Incomplete mesh counts")
    else:
        for name in ("elevs", "azims", "weights"):
            value = fields[name]
            if not isinstance(value, list) or not 1 <= len(value) <= 6:
                raise PartitionProtocolError("Invalid paint camera count")
            if not all(type(x) in (float, int) and math.isfinite(x) for x in value):
                raise PartitionProtocolError("Invalid paint camera value")
        if len({len(fields[name]) for name in ("elevs", "azims", "weights")}) != 1:
            raise PartitionProtocolError("Mismatched paint camera arrays")
        for name in CAMERA_FIELDS[3:]:
            value = fields[name]
            if type(value) not in (float, int) or not math.isfinite(value) or value <= 0:
                raise PartitionProtocolError("Invalid paint camera projection")
        if fields["near"] >= fields["far"]:
            raise PartitionProtocolError("Invalid paint camera clipping planes")


class _Encoder:
    def __init__(self) -> None:
        self.tensors: dict[str, Any] = {}
        self.blobs: dict[str, bytes] = {}
        self.items = 0

    def encode(self, value: Any, depth: int = 0) -> dict[str, Any]:
        if depth > MAX_TREE_DEPTH:
            raise PartitionProtocolError("Partition value nesting is too deep")
        self.items += 1
        if self.items > MAX_TREE_ITEMS:
            raise PartitionProtocolError("Partition value contains too many items")
        if value is None:
            return {"kind": "none"}
        if isinstance(value, bool):
            return {"kind": "bool", "value": value}
        if isinstance(value, int):
            return {"kind": "int", "value": value}
        if isinstance(value, float):
            if value != value or value in (float("inf"), float("-inf")):
                raise PartitionProtocolError("Non-finite floats are not portable")
            return {"kind": "float", "value": value}
        if isinstance(value, str):
            return {"kind": "string", "value": value}
        if isinstance(value, (bytes, bytearray, memoryview)):
            name = f"blob-{len(self.blobs):08d}"
            self.blobs[name] = bytes(value)
            return {"kind": "bytes", "blob": name}
        try:
            import torch
        except ImportError:  # pragma: no cover - worker images always supply torch
            torch = None
        if torch is not None and isinstance(value, torch.Tensor):
            if value.layout != torch.strided:
                raise PartitionProtocolError("Sparse or non-strided tensors are not portable")
            name = f"tensor-{len(self.tensors):08d}"
            self.tensors[name] = value.detach().to(device="cpu").contiguous()
            return {"kind": "tensor", "tensor": name}
        native_kind = {
            ("comfy_api.latest._util.geometry_types", "MESH"): "mesh.v1",
            ("comfy.ldm.hunyuan3d.paint.render", "Cameras"): "paint-cameras.v1",
        }.get((type(value).__module__, type(value).__name__))
        if native_kind:
            native_type, names = _native_geometry_type(native_kind)
            if type(value) is not native_type:
                raise PartitionProtocolError("Unrecognized native geometry class")
            fields = {name: getattr(value, name) for name in names}
            _validate_native_geometry(native_kind, fields)
            return {"kind": native_kind, "fields": self.encode(fields, depth + 1)}
        if isinstance(value, list):
            return {"kind": "list", "items": [self.encode(item, depth + 1) for item in value]}
        if isinstance(value, tuple):
            return {"kind": "tuple", "items": [self.encode(item, depth + 1) for item in value]}
        if isinstance(value, dict):
            if not all(isinstance(key, str) for key in value):
                raise PartitionProtocolError("Partition dictionaries require string keys")
            return {
                "kind": "dict",
                "items": {
                    key: self.encode(item, depth + 1) for key, item in sorted(value.items())
                },
            }
        raise PartitionProtocolError(
            f"Unsupported partition value: {type(value).__module__}.{type(value).__qualname__}"
        )


def _decode(
    node: dict[str, Any],
    tensors: dict[str, Any],
    blobs: dict[str, bytes],
    depth: int = 0,
) -> Any:
    if depth > MAX_TREE_DEPTH:
        raise PartitionProtocolError("Partition value nesting is too deep")
    kind = node.get("kind")
    if kind == "none":
        return None
    if kind in {"bool", "int", "float", "string"}:
        return node.get("value")
    if kind == "bytes":
        name = node.get("blob")
        if name not in blobs:
            raise PartitionProtocolError(f"Missing declared blob: {name}")
        return blobs[name]
    if kind == "tensor":
        name = node.get("tensor")
        if name not in tensors:
            raise PartitionProtocolError(f"Missing declared tensor: {name}")
        return tensors[name]
    if kind in {"mesh.v1", "paint-cameras.v1"}:
        fields = _decode(node.get("fields") or {}, tensors, blobs, depth + 1)
        _validate_native_geometry(kind, fields)
        native_type, _ = _native_geometry_type(kind)
        return native_type(**fields)
    if kind in {"list", "tuple"}:
        values = [_decode(item, tensors, blobs, depth + 1) for item in node.get("items", [])]
        return values if kind == "list" else tuple(values)
    if kind == "dict":
        items = node.get("items")
        if not isinstance(items, dict):
            raise PartitionProtocolError("Invalid dictionary node")
        return {key: _decode(item, tensors, blobs, depth + 1) for key, item in items.items()}
    raise PartitionProtocolError(f"Unknown partition value kind: {kind!r}")


def dump_bundle(value: Any, destination: str | Path) -> dict[str, Any]:
    """Write a safe artifact bundle and return its immutable identity metadata."""
    from safetensors.torch import save

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoder = _Encoder()
    root = encoder.encode(value)
    manifest = json.dumps(
        {"schema": SCHEMA, "root": root},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(manifest) > MAX_MANIFEST_BYTES:
        raise PartitionProtocolError("Partition manifest is too large")
    with zipfile.ZipFile(destination, "w", allowZip64=True) as archive:
        archive.writestr(MANIFEST_NAME, manifest, compress_type=zipfile.ZIP_DEFLATED)
        if encoder.tensors:
            archive.writestr(
                TENSORS_NAME,
                save(encoder.tensors),
                compress_type=zipfile.ZIP_STORED,
            )
        for name, content in encoder.blobs.items():
            archive.writestr(f"blobs/{name}", content, compress_type=zipfile.ZIP_DEFLATED)
    digest = bundle_sha256(destination)
    return {"schema": SCHEMA, "sha256": digest, "size": destination.stat().st_size}


def load_bundle(source: str | Path, *, max_bytes: int | None = None) -> Any:
    """Load a bundle without pickle, rejecting unsafe or undeclared members."""
    from safetensors.torch import load

    source = Path(source)
    if max_bytes is not None and source.stat().st_size > max_bytes:
        raise PartitionProtocolError("Partition bundle exceeds the configured size limit")
    with zipfile.ZipFile(source, "r") as archive:
        infos = archive.infolist()
        names = [item.filename for item in infos]
        if len(names) != len(set(names)):
            raise PartitionProtocolError("Partition bundle contains duplicate members")
        if any(name.startswith(("/", "\\")) or ".." in Path(name).parts for name in names):
            raise PartitionProtocolError("Partition bundle contains an unsafe member path")
        if MANIFEST_NAME not in names:
            raise PartitionProtocolError("Partition bundle has no manifest")
        manifest_bytes = archive.read(MANIFEST_NAME)
        if len(manifest_bytes) > MAX_MANIFEST_BYTES:
            raise PartitionProtocolError("Partition manifest is too large")
        try:
            manifest = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PartitionProtocolError("Partition manifest is invalid JSON") from exc
        if manifest.get("schema") != SCHEMA:
            raise PartitionProtocolError(
                f"Unsupported partition schema: {manifest.get('schema')!r}"
            )
        tensors = load(archive.read(TENSORS_NAME)) if TENSORS_NAME in names else {}
        blobs = {
            name.removeprefix("blobs/"): archive.read(name)
            for name in names
            if name.startswith("blobs/")
        }
        allowed = {MANIFEST_NAME, TENSORS_NAME, *(f"blobs/{name}" for name in blobs)}
        undeclared = set(names) - allowed
        if undeclared:
            raise PartitionProtocolError(
                f"Partition bundle contains unexpected members: {sorted(undeclared)}"
            )
        return _decode(manifest.get("root") or {}, tensors, blobs)


def bundle_sha256(source: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(source).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
