"""Worker runtime-profile helpers.

Cloud Offload is model-agnostic: the coordinator never loads a 3D model.
Generation rides inside the submitted subgraph, so the only job "models" a
worker claims are the ComfyUI workflow capabilities below. Profiles are
declared in configuration (``worker_profiles``) and baked into runner images as
``runtime-profile.json`` manifests.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path, PureWindowsPath
from typing import Any

# The capabilities a ComfyUI runner can claim. ``comfyui-workflow`` runs an
# arbitrary API-format workflow; ``comfyui-partition-v1`` runs a compiled
# subgraph with typed boundary artifacts.
WORKFLOW_CAPABILITIES = frozenset({"comfyui-workflow", "comfyui-partition-v1"})
_RUNTIME_IDENTITY = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$")


def configured_worker_profiles(config: Any) -> dict[str, dict[str, Any]]:
    """Return normalized, explicitly configured cloud worker profiles.

    A profile is only usable when it pins its image by digest (``@sha256:``) and
    declares at least one known workflow capability.
    """
    result: dict[str, dict[str, Any]] = {}
    for name, value in (getattr(config, "worker_profiles", {}) or {}).items():
        if not isinstance(value, dict):
            continue
        image = str(value.get("image") or "").strip()
        models = [
            str(item)
            for item in value.get("models", [])
            if str(item) in WORKFLOW_CAPABILITIES
        ]
        providers = [
            "vast.ai" if str(item).lower() == "vast" else str(item).lower()
            for item in value.get("providers", ["runpod", "vast.ai"])
        ]
        if not image or "@sha256:" not in image or not models:
            continue
        result[str(name)] = {
            "name": str(name),
            # A configured profile is a routing name. It can use a compatible
            # immutable image whose baked manifest has a different family name.
            # Default to exact matching unless the operator declares the image
            # identity explicitly.
            "image_profile": str(value.get("image_profile") or "").strip() or str(name),
            "image": image,
            "platform": normalized_profile_runtime_identity(
                str(name), "platform", value.get("platform")
            ),
            "python_abi": normalized_profile_runtime_identity(
                str(name), "python_abi", value.get("python_abi")
            ),
            "models": models,
            "providers": list(dict.fromkeys(providers)),
            "gpu_type": str(value.get("gpu_type") or "").strip(),
            "min_gpu_ram_gb": float(value.get("min_gpu_ram_gb") or 0),
            "min_cuda_version": normalized_cuda_version(value.get("min_cuda_version")),
            "wheelhouse_url": str(value.get("wheelhouse_url") or ""),
            "wheelhouse_sha256": str(value.get("wheelhouse_sha256") or ""),
            "weights": normalized_profile_weights(str(name), value.get("weights")),
            "custom_nodes": normalized_profile_custom_nodes(
                str(name), value.get("custom_nodes")
            ),
            "extra_disk_gb": normalized_profile_disk_gb(
                str(name), "extra_disk_gb", value.get("extra_disk_gb")
            ),
            "image_size_gb": normalized_profile_disk_gb(
                str(name), "image_size_gb", value.get("image_size_gb")
            ),
            "object_info_digest": normalized_profile_digest(
                str(name), "object_info_digest", value.get("object_info_digest")
            ),
            "dependency_lock_digest": normalized_profile_digest(
                str(name),
                "dependency_lock_digest",
                value.get("dependency_lock_digest"),
            ),
        }
    return result


def normalized_cuda_version(value: Any) -> str:
    version = str(value or "").strip()
    if version and not re.fullmatch(r"[0-9]{1,2}\.[0-9]{1,2}", version):
        raise ValueError("min_cuda_version must be a major.minor CUDA version")
    return version


def normalized_profile_runtime_identity(name: str, field: str, value: Any) -> str:
    """Normalize one runtime identity that is fixed by a pinned image digest."""

    identity = str(value or "").strip().lower()
    if not identity:
        return ""
    if not _RUNTIME_IDENTITY.fullmatch(identity):
        raise ValueError(
            f"Worker profile {name!r}: {field} must be a simple runtime identity"
        )
    return identity


def normalized_profile_digest(name: str, field: str, value: Any) -> str:
    """Normalize one optional sha256 readiness identity on a worker profile."""

    digest = str(value or "").strip().lower()
    if not digest:
        return ""
    if digest.startswith("sha256:"):
        digest = digest[7:]
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"Worker profile {name!r}: {field} must be a sha256 digest")
    return "sha256:" + digest


def normalized_profile_disk_gb(name: str, field: str, value: Any) -> float:
    """Validate one of a profile's optional storage figures, in GB.

    ``extra_disk_gb`` is the operator's declaration of storage the coordinator
    cannot see. It exists because a custom node is free to download its own
    weights the first time it runs — a node calling diffusers
    ``from_pretrained`` pulls a repository nothing in the manifest mentions — and
    no static analysis of a partition can discover that. Zero is the honest
    default: it says nothing extra was declared, not that nothing extra exists.

    ``image_size_gb`` is the runner image's size, so sizing a pod's disk never
    depends on reaching a container registry. Zero means unknown, and the planner
    substitutes a conservative figure rather than treating it as free.
    """
    if value is None or value == "":
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"Worker profile {name!r}: {field} must be a number of GB, got {value!r}"
        )
    if not math.isfinite(number):
        raise ValueError(f"Worker profile {name!r}: {field} must be a finite number")
    if number < 0:
        raise ValueError(f"Worker profile {name!r}: {field} cannot be negative")
    return number


def profile_providing(profiles: dict, capability: str) -> dict | None:
    """The profile that provides ``capability``, chosen deterministically.

    Clients name the capability their job needs; only the operator knows what
    the profiles are called. Every component that reads a profile must resolve
    the two the same way, or a correctly configured worker is reported as
    missing by one component and never launched by another.
    """
    matches = [
        profile
        for _, profile in sorted(profiles.items())
        if capability in profile.get("models", [])
    ]
    return matches[0] if matches else None


def require_models_relative(label: str, field: str, value: str) -> None:
    """Reject absolute paths and upward traversal in a models-relative path."""
    # PureWindowsPath also parses forward slashes and catches drive letters, so
    # one check covers both separator styles regardless of the host OS.
    pure = PureWindowsPath(value)
    if pure.is_absolute() or pure.drive or value.startswith(("/", "\\")):
        raise ValueError(f"{label}: {field} must be a relative path, got {value!r}")
    if ".." in pure.parts:
        raise ValueError(f"{label}: {field} must not traverse upward, got {value!r}")


def normalized_profile_weights(name: str, entries: Any) -> list[dict[str, Any]]:
    """Validate and normalize a profile's optional pinned ``weights`` list.

    Every entry names a Hugging Face repo at a pinned revision and a destination
    under the runner's ComfyUI models directory; ``files: null`` means the whole
    snapshot. Invalid entries raise instead of being dropped: a profile that
    believes it stages weights but silently does not would fail every job at
    runtime with a far less useful error.
    """
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise ValueError(f"Worker profile {name!r}: weights must be a list")
    normalized: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        label = f"Worker profile {name!r} weights[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{label} must be an object")
        repo_id = str(entry.get("repo_id") or "").strip()
        if not repo_id:
            raise ValueError(f"{label}: repo_id is required")
        revision = str(entry.get("revision") or "").strip()
        if not revision:
            raise ValueError(
                f"{label}: revision is required; pin a commit hash, not a branch"
            )
        files = entry.get("files")
        if files is not None:
            if (
                not isinstance(files, list)
                or not files
                or not all(isinstance(item, str) and item.strip() for item in files)
            ):
                raise ValueError(
                    f"{label}: files must be null (whole snapshot) or a list of file paths"
                )
            files = [item.strip() for item in files]
            for item in files:
                require_models_relative(label, "files entry", item)
        dest = str(entry.get("dest") or "").strip()
        if not dest:
            raise ValueError(
                f"{label}: dest is required (a subdirectory of the ComfyUI models dir)"
            )
        require_models_relative(label, "dest", dest)
        normalized.append(
            {
                "repo_id": repo_id,
                "revision": revision,
                "files": files,
                "dest": dest,
                "gated": bool(entry.get("gated", False)),
            }
        )
    return normalized


def _full_commit_sha(label: str, value: str) -> str:
    """Reject anything but a complete commit sha.

    A branch or tag names whatever that ref points at today, which is not an
    identity: two runners launched an hour apart from the same profile would
    hold different code and both believe they matched the pin.
    """
    commit = value.strip().lower()
    if len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
        raise ValueError(
            f"{label}: commit must be a full 40-character sha, not a branch or tag"
        )
    return commit


def normalized_profile_custom_nodes(name: str, entries: Any) -> list[dict[str, Any]]:
    """Validate and normalize a profile's optional ``custom_nodes`` list.

    Each entry pins one custom node pack, either as a Comfy Registry release
    (``registry_id`` plus ``version``) or as a git checkout (``git`` plus a full
    ``commit``). Both source kinds are needed in practice: registry metadata can
    point at a repository URL that 404s, and a pack can exist in git before it is
    published at all.

    Exactly one source kind per entry, and never a floating ref. Invalid entries
    raise rather than being dropped, for the same reason ``weights`` does: a
    profile that believes it installs a pack but silently does not would fail
    every job that needs it, on a runner that is already being paid for.
    """
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise ValueError(f"Worker profile {name!r}: custom_nodes must be a list")
    normalized: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        label = f"Worker profile {name!r} custom_nodes[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{label} must be an object")
        registry_id = str(entry.get("registry_id") or "").strip()
        git_url = str(entry.get("git") or "").strip()
        if registry_id and git_url:
            raise ValueError(f"{label}: give either registry_id or git, not both")
        install_requirements = bool(entry.get("install_requirements", True))
        # A pack's published name need not match its repository or its install
        # directory, so an entry may state outright which pack it provides.
        explicit_id = str(entry.get("id") or "").strip()
        if registry_id:
            version = str(entry.get("version") or "").strip()
            if not version:
                raise ValueError(
                    f"{label}: version is required; pin a published release, not a range"
                )
            normalized.append(
                {
                    **({"id": explicit_id} if explicit_id else {}),
                    "registry_id": registry_id,
                    "version": version,
                    "install_requirements": install_requirements,
                }
            )
            continue
        if not git_url:
            raise ValueError(f"{label}: registry_id or git is required")
        if not git_url.startswith(("http://", "https://")):
            raise ValueError(f"{label}: git must be an http or https clone URL")
        commit = str(entry.get("commit") or "").strip()
        if not commit:
            raise ValueError(
                f"{label}: commit is required; pin a commit sha, not a branch"
            )
        normalized.append(
            {
                **({"id": explicit_id} if explicit_id else {}),
                "git": git_url,
                "commit": _full_commit_sha(label, commit),
                "install_requirements": install_requirements,
            }
        )
    return normalized


def profile_pack_identifier(entry: dict[str, Any]) -> str:
    """The name a profile entry answers to when a partition requires a pack.

    An explicit ``id`` always wins, because a pack's published name, its
    repository name and the directory it is installed into are all free to
    differ: ``eric-qwen-layer`` ships from a repository called
    ``Qwen_Layers_Diffuser_Pipeline_Comfyui``, so a git entry that could only
    answer to its URL could never match what ComfyUI reports. Failing that, a
    registry entry answers to its ``registry_id``, and a git entry to the last
    path segment of its clone URL with any ``.git`` suffix removed, which is the
    directory ``git clone`` would create.
    """
    explicit = str(entry.get("id") or "").strip()
    if explicit:
        return explicit
    registry_id = str(entry.get("registry_id") or "").strip()
    if registry_id:
        return registry_id
    segment = str(entry.get("git") or "").rstrip("/").rsplit("/", 1)[-1]
    return segment[:-4] if segment.lower().endswith(".git") else segment


def worker_profile_gpu_type(profile: dict[str, Any], default: str | None = None) -> str | None:
    """Return the GPU type filter for a worker profile."""
    gpu_type = str(profile.get("gpu_type") or default or "").strip()
    if not gpu_type:
        return None
    if gpu_type.lower() == "any":
        return None
    if gpu_type.lower() == "vast":
        return "vast.ai"
    return gpu_type


def worker_profile_min_gpu_ram(profile: dict[str, Any]) -> int:
    """Return the minimum GPU RAM a profile should be scheduled against."""
    return int(math.ceil(float(profile.get("min_gpu_ram_gb") or 0)))


def cloud_profiles_for_model(
    config: Any, model_name: str, provider: str | None = None
) -> list[dict[str, Any]]:
    """Return configured profiles that can run ``model_name`` on ``provider``."""
    profiles = []
    normalized_provider = "vast.ai" if provider == "vast" else provider
    for profile in configured_worker_profiles(config).values():
        if model_name not in profile["models"]:
            continue
        if normalized_provider and normalized_provider not in profile["providers"]:
            continue
        profiles.append(profile)
    return profiles


def load_worker_manifest(path: str | Path) -> dict[str, Any]:
    """Load and minimally validate a capability manifest baked into an image."""
    manifest_path = Path(path)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    profile = str(data.get("profile") or "").strip()
    models = [
        str(item)
        for item in data.get("models", [])
        if str(item) in WORKFLOW_CAPABILITIES
    ]
    if not profile or not models:
        raise ValueError(f"Invalid worker capability manifest: {manifest_path}")
    return {
        "profile": profile,
        "models": list(dict.fromkeys(models)),
        "version": str(data.get("version") or ""),
        "platform": normalized_profile_runtime_identity(
            profile, "platform", data.get("platform")
        ),
        "python_abi": normalized_profile_runtime_identity(
            profile, "python_abi", data.get("python_abi")
        ),
        "partition_protocol": str(data.get("partition_protocol") or ""),
        "source": str(manifest_path),
    }
