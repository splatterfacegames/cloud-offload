"""Scoped fault injection for a campaign cache that has no S3 endpoint.

The coordinator signs a fresh synthetic artifact. Only an explicitly enabled
benchmark worker can place its corrupt bytes on the mounted volume. Normal
cache verification, quarantine, and origin fallback then run unchanged.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import time

from cloud_offload import benchmark_faults as faults
from cloud_offload.benchmark_faults import _corruption_keys, corruption_canary_asset
from cloud_offload.config import CloudConfig
from cloud_offload.prepared_state import build_manifest, canonical_json, normalize_digest
from cloud_offload.storage import partition_artifact_key


ENABLED_ENV = "CLOUD_OFFLOAD_BENCHMARK_MOUNT_CORRUPTION"
CLAIM = "benchmark_mounted_corruption"


def _references_digest(value, digest: str) -> bool:
    if isinstance(value, dict):
        return value.get("digest") in {digest, "sha256:" + digest} or any(
            _references_digest(item, digest) for item in value.values()
        )
    if isinstance(value, list):
        return any(_references_digest(item, digest) for item in value)
    return False


def inject(cache, manifest: dict) -> dict | None:
    if os.environ.get(ENABLED_ENV) != "1" or CLAIM not in manifest:
        return None
    verified = cache.signer.verify(manifest)
    claim = verified[CLAIM]
    scenario, nonce = claim["scenario"], claim["nonce"]
    if not nonce:
        raise ValueError("Mounted corruption requires a fresh campaign nonce")
    asset = corruption_canary_asset(scenario, nonce=nonce)
    keys = _corruption_keys(scenario, canary_nonce=nonce)
    matches = [item for item in verified["artifacts"]
               if normalize_digest(item["digest"]) == keys["digest"]]
    if len(matches) != 1 or matches[0]["size"] != asset["size"]:
        raise ValueError("Mounted corruption manifest does not declare its synthetic artifact")
    if matches[0]["storage_key"] != keys["blob_key"]:
        raise ValueError("Mounted corruption artifact has a different storage key")
    original_generation = cache._resolve("indexes/latest").read_text(encoding="utf-8").strip()
    if not original_generation:
        raise ValueError("Mounted corruption requires an existing prepared inventory")
    state = {**keys, "original_generation": original_generation,
             "manifest_id": verified["manifest_id"]}
    state_path = cache._resolve(keys["state_key"])
    blob = cache._resolve(keys["blob_key"])
    if state_path.exists() or blob.exists():
        raise ValueError("Mounted corruption requires a previously unused synthetic object")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    blob.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("xb") as handle:
        handle.write(canonical_json(state))
    try:
        with blob.open("xb") as handle:
            handle.write(b"cloud-offload-benchmark-corrupt")
    except BaseException:
        state_path.unlink()
        raise
    return state


def cleanup(cache, state: dict) -> dict:
    """Restore the original index and remove only this nonce's synthetic objects."""
    digest = state["digest"]
    recorded = json.loads(cache._resolve(state["state_key"]).read_text(encoding="utf-8"))
    if recorded != state:
        raise ValueError("Mounted corruption cleanup identity changed")
    metadata = []
    for prefix in ("manifests", "indexes", "pending-announcements", "trust-receipts"):
        directory = cache._resolve(prefix)
        if not directory.exists():
            continue
        for path in directory.rglob("*.json"):
            path = cache._resolve(path.relative_to(cache.root).as_posix())
            document = json.loads(path.read_text(encoding="utf-8"))
            if _references_digest(document, digest):
                metadata.append(path)
    latest = cache._resolve("indexes/latest")
    current = latest.read_text(encoding="utf-8").strip()
    if current != state["original_generation"]:
        current_index = cache._resolve(f"indexes/{current}.json")
        if current_index not in metadata:
            raise ValueError("Prepared inventory changed independently of this canary")
        temporary = cache._resolve(state["state_key"] + ".restore")
        temporary.write_text(state["original_generation"], encoding="utf-8")
        os.replace(temporary, latest)
    for path in metadata:
        path.unlink()
    blob = cache._resolve(state["blob_key"])
    blob.unlink(missing_ok=True)
    quarantine = cache._resolve(state["quarantine_prefix"])
    removed = 0
    if quarantine.exists():
        for path in quarantine.rglob("*"):
            if path.is_file():
                cache._resolve(path.relative_to(cache.root).as_posix()).unlink()
                removed += 1
    cache._resolve(state["state_key"]).unlink()
    return {"digest": digest, "original_generation_restored": True,
            "original_generation": state["original_generation"],
            "canary_deleted": True, "quarantine_objects_deleted": removed}


def _state_path(scenario: str, nonce: str) -> Path:
    digest = corruption_canary_asset(scenario, nonce=nonce)["sha256"]
    home = Path(CloudConfig.load(resolve_secrets=False).queue_db_path).parent
    return home / "mounted-canaries" / (digest + ".json")


def prepare(client, scenario: str, declared: set[str], nonce: str, regions: set[str]) -> dict:
    volume, base = faults._corruption_target(client, scenario, declared_digests=declared,
        canary_nonce=nonce, allowed_regions=regions, require_s3=False)
    asset = corruption_canary_asset(scenario, nonce=nonce)
    keys = _corruption_keys(scenario, canary_nonce=nonce)
    path = _state_path(scenario, nonce)
    storage = faults._coordinator_storage()
    artifact_key = partition_artifact_key(keys["digest"])
    if path.exists() or storage.exists(artifact_key):
        raise RuntimeError("Mounted canary must use a fresh coordinator artifact")
    artifact = {"digest": "sha256:" + keys["digest"], "kind": "model-weight",
        "portability": "portable", "materialization": "copy", "storage_key": keys["blob_key"],
        "size": asset["size"], "requirements": {},
        "policy": {"cacheable": True, "private": False, "tenant": "default"},
        "source": {"kind": "local-artifact", "sha256": keys["digest"]},
        "destination": {"category": asset["category"], "filename": asset["filename"]}}
    profile = os.environ.get("CLOUD_OFFLOAD_BENCHMARK_PROFILE") or "comfyui-partition-v1"
    manifest = build_manifest(
        profile_fingerprint=faults._corruption_profile_fingerprint(profile, declared),
        producer=dict(base["producer"]), artifacts=list(base["artifacts"]) + [artifact],
        signer=faults._manifest_signer(), claims={"cache_volume_id": volume["id"],
            CLAIM: {"scenario": scenario, "nonce": nonce}})
    state = {"volume_id": volume["id"], "digest": keys["digest"],
             "artifact_key": artifact_key, "manifest_id": manifest["manifest_id"],
             "original_generation": volume.get("inventory_generation")}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json(state))
    with tempfile.NamedTemporaryFile(delete=False) as handle:
        handle.write(faults._corruption_valid_payload(scenario, nonce))
        temporary = Path(handle.name)
    try:
        storage.upload(temporary, artifact_key)
        faults._cache_registry().announce_manifest(volume["id"], f"{time.time_ns()}-mounted-canary", manifest)
    except BaseException:
        faults._cache_registry().remove_manifest(volume["id"], manifest["manifest_id"],
            inventory_generation=state["original_generation"])
        storage.delete(artifact_key)
        path.unlink()
        raise
    finally:
        temporary.unlink(missing_ok=True)
    return {"kind": "corruption", "stage": "prepare", "mounted": True,
            "fresh_object": True, "canary_manifest_published": True}


def evidence(client, job_id: str, digest: str) -> dict:
    found = {}
    cursor = 0
    while True:
        events = client.events(job_id, cursor)
        if not events:
            break
        next_cursor = max(int(item.get("sequence") or 0) for item in events)
        if next_cursor <= cursor:
            raise RuntimeError("Mounted canary event cursor did not advance")
        cursor = next_cursor
        for envelope in events:
            event = envelope.get("event") or {}
            if str(event.get("digest") or "").removeprefix("sha256:") == digest:
                found[event.get("type")] = event
    return found


def run_fault(client, job_id: str, scenario: str, stage: str,
              declared: set[str], nonce: str, regions: set[str]) -> dict:
    if stage == "prepare":
        return prepare(client, scenario, declared, nonce, regions)
    state_path = _state_path(scenario, nonce)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    if stage == "observe":
        deadline = time.monotonic() + faults.CORRUPTION_OBSERVE_TIMEOUT_SECONDS
        while True:
            events = evidence(client, job_id, state["digest"])
            cleaned = events.get("benchmark_mounted_corruption_cleaned") or {}
            if ("benchmark_mounted_corruption_injected" in events
                    and "cache_artifact_quarantined" in events
                    and cleaned.get("canary_deleted")
                    and cleaned.get("original_generation_restored")
                    and int(cleaned.get("quarantine_objects_deleted") or 0) > 0):
                return {"kind": "corruption", "stage": stage, "quarantine_observed": True,
                        "canary_deleted": True, "original_generation_restored": True}
            if client.job(job_id).get("status") in {"completed", "failed", "dead_letter"}:
                raise RuntimeError("Mounted canary finished without quarantine and cleanup proof")
            if time.monotonic() >= deadline:
                raise TimeoutError("Mounted canary quarantine and cleanup evidence timed out")
            time.sleep(1)
    if stage != "cleanup":
        raise ValueError("Unknown mounted canary stage")
    events = evidence(client, job_id, state["digest"]) if job_id else {}
    cleaned = events.get("benchmark_mounted_corruption_cleaned") or {}
    if "benchmark_mounted_corruption_injected" in events and not (
        cleaned.get("canary_deleted") and cleaned.get("original_generation_restored")
    ):
        raise RuntimeError("Mounted canary cleanup has not been confirmed by the worker")
    registry = faults._cache_registry()
    ids = faults._synthetic_registry_manifest_ids(registry, state["volume_id"], state["digest"])
    ids.add(state["manifest_id"])
    for manifest_id in ids:
        registry.remove_manifest(state["volume_id"], manifest_id,
            inventory_generation=cleaned.get("original_generation") or state.get("original_generation"))
    faults._coordinator_storage().delete(state["artifact_key"])
    state_path.unlink()
    return {"kind": "corruption", "stage": "cleanup", "registry_manifests_deleted": len(ids),
            "canary_deleted": True}
