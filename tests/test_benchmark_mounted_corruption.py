import hashlib
import json

import pytest

from cloud_offload.benchmark_faults import corruption_canary_asset
from cloud_offload.benchmark_mounted_corruption import CLAIM, ENABLED_ENV, cleanup, inject
from cloud_offload import benchmark_mounted_corruption as mounted
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.storage import LocalStorage
from cloud_offload.prepared_state import (
    CacheCorruptionError, ManifestError, ManifestSigner, PreparedStateCAS, blob_key, build_manifest,
)


def mounted_fixture(tmp_path):
    signer = ManifestSigner(b"x" * 32)
    cache = PreparedStateCAS(tmp_path / "cache", signer)
    (cache.root / "indexes/latest").write_text("original")
    (cache.root / "indexes/original.json").write_text(json.dumps({"generation": "original", "manifests": []}))
    claim = {"scenario": "japan-corruption", "nonce": "campaign-unique-1"}
    asset = corruption_canary_asset(claim["scenario"], nonce=claim["nonce"])
    artifact = {"digest": "sha256:" + asset["sha256"], "size": asset["size"],
        "kind": "model-weight", "portability": "portable", "materialization": "copy",
        "storage_key": blob_key(asset["sha256"]), "requirements": {},
        "policy": {"tenant": "default", "private": False, "cacheable": True},
        "source": {"kind": "local-artifact", "sha256": asset["sha256"]},
        "destination": {"category": asset["category"], "filename": asset["filename"]},
    }
    manifest = build_manifest(profile_fingerprint="sha256:" + "a" * 64,
        producer={"cloud_offload_version": "test"}, artifacts=[artifact], signer=signer,
        claims={CLAIM: claim})
    return cache, manifest, artifact


def test_mounted_canary_requires_explicit_enablement(tmp_path, monkeypatch):
    monkeypatch.delenv(ENABLED_ENV, raising=False)
    cache, manifest, artifact = mounted_fixture(tmp_path)
    assert inject(cache, manifest) is None
    assert not cache._resolve(artifact["storage_key"]).exists()


def test_mounted_canary_uses_real_verification_and_quarantine(tmp_path, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "1")
    cache, manifest, artifact = mounted_fixture(tmp_path)
    original_index = (cache.root / "indexes/original.json").read_bytes()
    state = inject(cache, manifest)
    corrupt = cache._resolve(artifact["storage_key"])
    assert hashlib.sha256(corrupt.read_bytes()).hexdigest() != state["digest"]
    with pytest.raises(CacheCorruptionError):
        cache.verify_object(artifact["digest"], expected_size=artifact["size"])
    cache.quarantine(artifact["digest"], "benchmark verification rejected corrupt bytes")
    assert not corrupt.exists()
    unrelated = cache.root / "manifests/unrelated.json"
    unrelated.write_text(json.dumps({"note": state["digest"]}))
    generated = cache.root / "indexes/generated.json"
    generated.write_text(json.dumps({"artifacts": [artifact]}))
    (cache.root / "indexes/latest").write_text("generated")

    result = cleanup(cache, state)
    assert result["original_generation_restored"]
    assert result["quarantine_objects_deleted"] > 0
    assert (cache.root / "indexes/latest").read_text() == "original"
    assert (cache.root / "indexes/original.json").read_bytes() == original_index
    assert unrelated.exists()
    assert not generated.exists()
    assert not cache._resolve(state["state_key"]).exists()


def test_mounted_canary_refuses_existing_object(tmp_path, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "1")
    cache, manifest, artifact = mounted_fixture(tmp_path)
    blob = cache._resolve(artifact["storage_key"])
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(b"existing")
    with pytest.raises(ValueError, match="previously unused"):
        inject(cache, manifest)
    assert blob.read_bytes() == b"existing"


def test_mounted_canary_rejects_changed_signed_claim(tmp_path, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "1")
    cache, manifest, _ = mounted_fixture(tmp_path)
    manifest[CLAIM]["nonce"] = "different-nonce"
    with pytest.raises(ManifestError):
        inject(cache, manifest)


def test_mounted_cleanup_refuses_unrelated_new_inventory(tmp_path, monkeypatch):
    monkeypatch.setenv(ENABLED_ENV, "1")
    cache, manifest, _ = mounted_fixture(tmp_path)
    state = inject(cache, manifest)
    (cache.root / "indexes/independent.json").write_text(json.dumps({"artifacts": []}))
    (cache.root / "indexes/latest").write_text("independent")
    with pytest.raises(ValueError, match="independently"):
        cleanup(cache, state)
    assert (cache.root / "indexes/latest").read_text() == "independent"


@pytest.mark.parametrize("quarantine_evidence", [True, False])
def test_mounted_campaign_roundtrip_preserves_base_and_checks_worker_evidence(
    tmp_path, monkeypatch, quarantine_evidence
):
    monkeypatch.setenv(ENABLED_ENV, "1")
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registry.upsert_volume(provider="runpod", provider_volume_id="provider-japan",
        datacenter_id="AP-JP-1", ownership="managed", capacity_bytes=1024, policy={},
        s3_compatible=False)
    signer = ManifestSigner(b"x" * 32)
    cache = PreparedStateCAS(tmp_path / "cache", signer)
    storage = LocalStorage(tmp_path / "artifacts")
    ordinary_bytes = b"ordinary verified bytes"
    ordinary_digest = hashlib.sha256(ordinary_bytes).hexdigest()
    ordinary = {"digest": "sha256:" + ordinary_digest, "size": len(ordinary_bytes),
        "kind": "model-weight", "portability": "portable", "materialization": "copy",
        "storage_key": blob_key(ordinary_digest), "requirements": {},
        "policy": {"tenant": "default", "private": False, "cacheable": True},
        "source": {"kind": "local-artifact", "sha256": ordinary_digest},
        "destination": {"category": "diffusion_models", "filename": "ordinary.bin"}}
    original_blob = cache._resolve(ordinary["storage_key"])
    original_blob.parent.mkdir(parents=True, exist_ok=True)
    original_blob.write_bytes(ordinary_bytes)
    profile = "sha256:" + "a" * 64
    base = build_manifest(profile_fingerprint=profile, producer={"cloud_offload_version": "test"},
        artifacts=[ordinary], signer=signer, claims={"cache_volume_id": volume.id})
    cache.publish_manifest(base)
    generation = cache.load_index()["generation"]
    registry.announce_manifest(volume.id, generation, base)
    events = []

    class Client:
        def prepared_storage(self):
            return {"enabled": True, "existing_volume_id": "provider-japan"}

        def get(self, path):
            if path == "/api/cache/status":
                return {"volumes": [dict(vars(registry.get_volume(volume.id)))]}
            assert path == "/api/cache/manifests"
            return {"manifests": registry.query_manifests()}

        def events(self, job_id, after):
            return [item for item in events if item["sequence"] > after]

        def job(self, job_id):
            return {"status": "completed"}

    client = Client()
    state_path = tmp_path / "state.json"
    monkeypatch.setattr(mounted, "_state_path", lambda *args: state_path)
    monkeypatch.setattr(mounted.faults, "_cache_registry", lambda: registry)
    monkeypatch.setattr(mounted.faults, "_coordinator_storage", lambda: storage)
    monkeypatch.setattr(mounted.faults, "_manifest_signer", lambda: signer)
    monkeypatch.setattr(mounted.faults, "_corruption_profile_fingerprint", lambda *args: profile)
    scenario, nonce = "japan-corruption", "fresh-roundtrip"
    asset = corruption_canary_asset(scenario, nonce=nonce)
    declared = {ordinary_digest, asset["sha256"]}
    mounted.prepare(client, scenario, declared, nonce, {"AP-JP-1"})
    controller_state = json.loads(state_path.read_text())
    canary = registry.get_manifest(volume.id, controller_state["manifest_id"])
    state = inject(cache, canary)
    artifact = canary["artifacts"][-1]
    with pytest.raises(CacheCorruptionError):
        cache.verify_object(artifact["digest"], expected_size=artifact["size"])
    cache.quarantine(artifact["digest"], "actual verification rejected fixture bytes")
    # The normal origin fallback can retrieve the already published valid bytes.
    restored_blob = cache._resolve(artifact["storage_key"])
    storage.download(controller_state["artifact_key"], restored_blob)
    cache.verify_object(artifact["digest"], expected_size=artifact["size"])
    cache.publish_manifest(canary)
    cleaned = cleanup(cache, state)
    payloads = [{"type": "benchmark_mounted_corruption_injected", "digest": state["digest"]}]
    if quarantine_evidence:
        payloads.append({"type": "cache_artifact_quarantined", "digest": "sha256:" + state["digest"]})
    payloads.append({"type": "benchmark_mounted_corruption_cleaned", **cleaned})
    events.extend({"sequence": index, "event": payload} for index, payload in enumerate(payloads, 1))
    if quarantine_evidence:
        assert mounted.run_fault(client, "job", scenario, "observe", declared, nonce, {"AP-JP-1"})["quarantine_observed"]
    else:
        with pytest.raises(RuntimeError, match="without quarantine"):
            mounted.run_fault(client, "job", scenario, "observe", declared, nonce, {"AP-JP-1"})
    mounted.run_fault(client, "job", scenario, "cleanup", declared, nonce, {"AP-JP-1"})
    assert not state_path.exists()
    assert original_blob.read_bytes() == ordinary_bytes
    assert cache.load_index()["generation"] == generation
    assert [item["manifest_id"] for item in registry.query_manifests()] == [base["manifest_id"]]
