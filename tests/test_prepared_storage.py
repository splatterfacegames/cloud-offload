"""Storage-aware prepared-state integrity, placement and provider contracts."""

import io
import json
import os
import tarfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from cloud_offload import config as config_module
from cloud_offload import prepared_state as prepared_state_module
from cloud_offload import server
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.cache_scheduler import (
    PlacementCandidate,
    choose_placement,
    resolve_prepared_requirements,
    scheduler_runtime,
)
from cloud_offload.config import CloudConfig, estimate_runpod_storage_monthly
from cloud_offload.prepared_state import (
    INDEX_SCHEMA,
    CacheCorruptionError,
    CacheMountError,
    TRUST_RECEIPT_SCHEMA,
    ManifestError,
    ManifestSigner,
    PreparedStateCAS,
    RunPodS3PreparedStore,
    artifact_compatibility,
    artifact_runtime_compatibility_key,
    blob_key,
    bundle_key,
    build_manifest,
    canonical_json,
    custom_node_requirement_key,
    fingerprint,
    manifest_by_id_key,
    manifest_signature_digest,
    profile_weight_requirement_key,
    trust_receipt_key,
)
from cloud_offload.providers.base import (
    CloudConnector,
    Instance,
    PlacementConstraints,
    PlacementError,
    ProviderStorage,
    StorageAttachment,
)
from cloud_offload.providers.runpod import RunPodConnector
from cloud_offload.dispatcher import Dispatcher
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.worker import Worker


def policy(**updates):
    return {
        "enabled": True,
        "provider": "runpod",
        "policy": "smart",
        "region": "US-KS-2",
        "cold_fallback": "allow",
        "managed_size_gb": 250,
        "existing_volume_id": "vol-1",
        "max_monthly_storage_cost": None,
        "confirmed": True,
        "tenant": "default",
        "cache_private_assets": False,
        "shadow_admission": True,
        **updates,
    }


def portable_artifact(data: bytes, **updates):
    import hashlib

    digest = hashlib.sha256(data).hexdigest()
    return {
        "digest": "sha256:" + digest,
        "kind": "model-weight",
        "size": len(data),
        "storage_key": blob_key(digest),
        "portability": "portable",
        "requirements": {},
        "policy": {"tenant": "default", "cacheable": True},
        **updates,
    }


def signed_manifest(signer, artifacts, profile=None, claims=None):
    return build_manifest(
        profile_fingerprint=profile or fingerprint({"profile": "test"}),
        producer={
            "image_digest": "sha256:" + "a" * 64,
            "cloud_offload_version": "test",
            "python_abi": "cp311",
            "platform": "linux-x86_64",
            "torch": "2.7",
            "cuda": "12.8",
        },
        artifacts=artifacts,
        signer=signer,
        claims=claims,
    )


def test_prepared_storage_is_opt_in_and_secret_free():
    config = CloudConfig()
    assert config.prepared_storage["enabled"] is False
    assert config.prepared_storage["policy"] == "smart"

    enabled = CloudConfig(prepared_storage=policy())
    assert enabled.to_dict()["prepared_storage"]["existing_volume_id"] == "vol-1"
    assert "api_key" not in json.dumps(enabled.to_dict()["prepared_storage"])

    with pytest.raises(ValueError, match="confirmed"):
        CloudConfig(prepared_storage={"enabled": True})
    with pytest.raises(ValueError, match="credentials"):
        CloudConfig(prepared_storage={"secret_access_key": "nope"})


def test_manifest_signature_binds_content_policy_and_compatibility():
    signer = ManifestSigner(b"k" * 32)
    manifest = signed_manifest(signer, [portable_artifact(b"model")])
    assert signer.verify(manifest)["manifest_id"] == manifest["manifest_id"]

    changed = json.loads(json.dumps(manifest))
    changed["artifacts"][0]["policy"]["tenant"] = "attacker"
    with pytest.raises(ManifestError, match="ID"):
        signer.verify(changed)


@pytest.mark.parametrize(
    ("portability", "requirements", "runtime", "accepted", "reason"),
    [
        ("portable", {}, {}, True, "compatible"),
        (
            "runtime-bound",
            {
                "image_digest": "i",
                "platform": "p",
                "python_abi": "a",
                "dependency_lock": "d",
            },
            {
                "image_digest": "i",
                "platform": "p",
                "python_abi": "a",
                "dependency_lock": "d",
            },
            True,
            "compatible",
        ),
        (
            "runtime-bound",
            {
                "image_digest": "i",
                "platform": "p",
                "python_abi": "a",
                "dependency_lock": "d",
            },
            {
                "image_digest": "other",
                "platform": "p",
                "python_abi": "a",
                "dependency_lock": "d",
            },
            False,
            "runtime_mismatch",
        ),
        ("gpu-class-bound", {}, {}, False, "unknown_compatibility"),
        ("process-bound", {}, {}, False, "process-bound_is_not_durable"),
        ("gpu-resident", {}, {}, False, "gpu-resident_is_not_durable"),
    ],
)
def test_compatibility_unknown_means_miss(
    portability, requirements, runtime, accepted, reason
):
    result = artifact_compatibility(
        {"portability": portability, "requirements": requirements}, runtime
    )
    assert result.accepted is accepted
    assert result.reason == reason


def test_two_fresh_worker_roots_consume_one_verified_object(tmp_path):
    signer = ManifestSigner(b"s" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "origin.safetensors"
    source.write_bytes(b"same pinned bytes")
    artifact = portable_artifact(source.read_bytes())
    volume.publish_blob(source, artifact["digest"], writer_id="first-worker")
    manifest = signed_manifest(signer, [artifact])
    volume.publish_manifest(manifest)

    selected = volume.find_manifest(profile_fingerprint=manifest["profile_fingerprint"])
    assert selected and selected["manifest_id"] == manifest["manifest_id"]
    destinations = []
    for worker_name in ("worker-a", "worker-b"):
        destination = tmp_path / worker_name / "models" / source.name
        volume.restore_artifact(
            selected["artifacts"][0],
            destination,
            runtime={},
            tenant="default",
            symlink_portable=False,
        )
        destinations.append(destination)
    assert [item.read_bytes() for item in destinations] == [source.read_bytes()] * 2


def test_signed_trust_receipt_uses_only_metadata_and_one_sample_on_hot_restore(
    tmp_path, monkeypatch
):
    signer = ManifestSigner(b"t" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "large.safetensors"
    source.write_bytes(os.urandom(6 * 1024 * 1024))
    artifact = portable_artifact(source.read_bytes())
    volume.publish_blob(source, artifact["digest"])
    manifest = signed_manifest(
        signer,
        [artifact],
        claims={
            "cache_volume_id": "vol-trusted",
            "cache_provider_volume_id": "provider-vol-trusted",
        },
    )
    volume.publish_manifest(manifest)

    receipt_path = volume.root / trust_receipt_key(artifact["digest"])
    receipt = signer.verify_trust_receipt(
        json.loads(receipt_path.read_text(encoding="utf-8"))
    )
    assert receipt["volume_id"] == "vol-trusted"

    monkeypatch.setattr(
        prepared_state_module,
        "sha256_file",
        lambda _path: (_ for _ in ()).throw(AssertionError("full read used")),
    )
    decisions = []
    destination = tmp_path / "worker-b" / source.name
    volume.restore_artifact(
        artifact,
        destination,
        runtime={},
        tenant="default",
        manifest=manifest,
        volume_id="vol-trusted",
        provider_volume_id="provider-vol-trusted",
        verification_callback=decisions.append,
    )

    assert destination.is_symlink()
    assert decisions[0]["mode"] == "trusted_metadata_sample"
    assert 0 < decisions[0]["bytes_read"] < artifact["size"]


def test_trust_receipt_tampering_falls_back_to_full_digest(tmp_path, monkeypatch):
    signer = ManifestSigner(b"r" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "object"
    source.write_bytes(os.urandom(2 * 1024 * 1024))
    artifact = portable_artifact(source.read_bytes())
    volume.publish_blob(source, artifact["digest"])
    manifest = signed_manifest(
        signer,
        [artifact],
        claims={
            "cache_volume_id": "vol-1",
            "cache_provider_volume_id": "provider-vol-1",
        },
    )
    volume.publish_manifest(manifest)
    receipt_path = volume.root / trust_receipt_key(artifact["digest"])
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["volume_id"] = "vol-attacker"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    original = prepared_state_module.sha256_file
    full_reads = []

    def observe(path):
        full_reads.append(Path(path))
        return original(path)

    monkeypatch.setattr(prepared_state_module, "sha256_file", observe)
    decisions = []
    volume.restore_artifact(
        artifact,
        tmp_path / "restored",
        runtime={},
        tenant="default",
        manifest=manifest,
        volume_id="vol-1",
        provider_volume_id="provider-vol-1",
        verification_callback=decisions.append,
    )

    assert decisions == [
        {
            "mode": "full_digest",
            "bytes_read": artifact["size"],
            "receipt_issued": True,
        }
    ]
    assert full_reads


def test_signed_sample_detects_same_generation_corruption_and_quarantine_removes_receipt(
    tmp_path,
):
    signer = ManifestSigner(b"c" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "object"
    source.write_bytes(os.urandom(2 * 1024 * 1024))
    artifact = portable_artifact(source.read_bytes())
    published = volume.publish_blob(source, artifact["digest"])
    manifest = signed_manifest(
        signer,
        [artifact],
        claims={
            "cache_volume_id": "vol-1",
            "cache_provider_volume_id": "provider-vol-1",
        },
    )
    volume.publish_manifest(manifest)
    original = published.stat()
    published.write_bytes(b"x" * artifact["size"])
    os.utime(
        published,
        ns=(original.st_atime_ns, original.st_mtime_ns),
    )

    with pytest.raises(CacheCorruptionError, match="signed trust sample"):
        volume.restore_artifact(
            artifact,
            tmp_path / "restored",
            runtime={},
            tenant="default",
            manifest=manifest,
            volume_id="vol-1",
            provider_volume_id="provider-vol-1",
        )
    volume.quarantine(
        artifact["digest"],
        "sample mismatch",
        storage_key=artifact["storage_key"],
    )
    assert not (volume.root / trust_receipt_key(artifact["digest"])).exists()


def test_background_sample_blocks_materialized_target_before_restore_returns(tmp_path):
    signer = ManifestSigner(b"b" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "object"
    source.write_bytes(os.urandom(12 * 1024 * 1024))
    artifact = portable_artifact(source.read_bytes())
    published = volume.publish_blob(source, artifact["digest"])
    manifest = signed_manifest(
        signer,
        [artifact],
        claims={
            "cache_volume_id": "vol-1",
            "cache_provider_volume_id": "provider-vol-1",
        },
    )
    volume.publish_manifest(manifest)
    receipt = json.loads(
        (volume.root / trust_receipt_key(artifact["digest"])).read_text(
            encoding="utf-8"
        )
    )
    samples = receipt["scrub"]["samples"]
    scrub_now = datetime.now(timezone.utc)
    bucket = int(scrub_now.timestamp() // 3600)
    selector = prepared_state_module.sha256_bytes(
        f"{receipt['receipt_id']}:{bucket}".encode("utf-8")
    )
    selected = int(selector[:8], 16) % len(samples)
    background = samples[(selected + 1) % len(samples)]
    original = published.stat()
    with published.open("r+b") as handle:
        handle.seek(background["offset"])
        handle.write(b"x" * background["size"])
    os.utime(published, ns=(original.st_atime_ns, original.st_mtime_ns))
    destination = tmp_path / "restored"

    with pytest.raises(CacheCorruptionError, match="background trust sample"):
        volume.restore_artifact(
            artifact,
            destination,
            runtime={},
            tenant="default",
            manifest=manifest,
            volume_id="vol-1",
            provider_volume_id="provider-vol-1",
            now=scrub_now,
        )

    assert not destination.exists()


def test_private_artifact_and_due_full_audit_never_use_fast_trust(tmp_path):
    signer = ManifestSigner(b"p" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "private"
    source.write_bytes(os.urandom(2 * 1024 * 1024))
    artifact = portable_artifact(
        source.read_bytes(),
        policy={
            "tenant": "default",
            "cacheable": True,
            "private": True,
        },
    )
    volume.publish_blob(source, artifact["digest"])
    manifest = signed_manifest(
        signer,
        [artifact],
        claims={
            "cache_volume_id": "vol-1",
            "cache_provider_volume_id": "provider-vol-1",
        },
    )
    volume.publish_manifest(manifest)
    assert not (volume.root / trust_receipt_key(artifact["digest"])).exists()
    decisions = []
    volume.restore_artifact(
        artifact,
        tmp_path / "private-restored",
        runtime={},
        tenant="default",
        allow_private=True,
        manifest=manifest,
        volume_id="vol-1",
        provider_volume_id="provider-vol-1",
        verification_callback=decisions.append,
    )
    assert decisions[0]["mode"] == "full_digest"
    assert decisions[0]["receipt_issued"] is False

    public = portable_artifact(b"public" * 1024 * 1024)
    public_source = tmp_path / "public"
    public_source.write_bytes(b"public" * 1024 * 1024)
    volume.publish_blob(public_source, public["digest"])
    public_manifest = signed_manifest(
        signer,
        [public],
        claims={
            "cache_volume_id": "vol-1",
            "cache_provider_volume_id": "provider-vol-1",
        },
    )
    volume.publish_manifest(public_manifest)
    future = datetime.now(timezone.utc) + timedelta(days=2)
    due_decisions = []
    volume.restore_artifact(
        public,
        tmp_path / "public-restored",
        runtime={},
        tenant="default",
        manifest=public_manifest,
        volume_id="vol-1",
        provider_volume_id="provider-vol-1",
        verification_callback=due_decisions.append,
        now=future,
    )
    assert due_decisions[0]["mode"] == "full_digest"


def test_exact_manifest_id_falls_back_to_immutable_direct_object(tmp_path):
    signer = ManifestSigner(b"m" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    manifest = signed_manifest(signer, [portable_artifact(b"direct")])
    direct = volume.root / manifest_by_id_key(manifest["manifest_id"])
    direct.parent.mkdir(parents=True, exist_ok=True)
    direct.write_bytes(canonical_json(manifest))

    assert volume.load_index()["manifests"] == []
    selected = volume.find_manifest(
        profile_fingerprint=manifest["profile_fingerprint"],
        manifest_id=manifest["manifest_id"],
    )
    assert selected and selected["manifest_id"] == manifest["manifest_id"]
    assert (
        volume.find_manifest(
            profile_fingerprint=fingerprint({"profile": "other"}),
            manifest_id=manifest["manifest_id"],
        )
        is None
    )


def test_exact_manifest_id_fetches_from_authority_when_mount_is_stale(tmp_path):
    signer = ManifestSigner(b"f" * 32)
    manifest = signed_manifest(signer, [portable_artifact(b"control-plane")])

    class FetchAuthority:
        def __init__(self):
            self.fetches = []

        def fetch(self, manifest_id):
            self.fetches.append(manifest_id)
            return manifest

        def verify(self, document):
            return signer.verify(document)

    authority = FetchAuthority()
    volume = PreparedStateCAS(tmp_path / "volume", authority)
    volume.publish_index(
        [manifest],
        generation="stale-mounted-index",
        manifest_keys={manifest["manifest_id"]: "manifests/missing.json"},
    )

    assert volume.load_index()["manifests"][0]["manifest_id"] == manifest["manifest_id"]
    selected = volume.find_manifest(manifest_id=manifest["manifest_id"])

    assert selected == manifest
    assert authority.fetches == [manifest["manifest_id"]]


def test_concurrent_writers_cannot_publish_partial_or_invalid_objects(tmp_path):
    signer = ManifestSigner(b"s" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "source"
    source.write_bytes(os.urandom(256 * 1024))
    digest = portable_artifact(source.read_bytes())["digest"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        paths = list(
            pool.map(
                lambda writer: volume.publish_blob(source, digest, writer_id=writer),
                ("writer-a", "writer-b"),
            )
        )
    assert paths[0] == paths[1]
    assert volume.verify_object(digest).read_bytes() == source.read_bytes()

    bad = tmp_path / "bad"
    bad.write_bytes(b"wrong")
    with pytest.raises(CacheCorruptionError, match="source"):
        volume.publish_blob(bad, digest, writer_id="bad-writer")


def test_blob_publication_reports_copy_progress_without_rehashing_verified_source(
    tmp_path,
):
    volume = PreparedStateCAS(tmp_path / "volume", ManifestSigner(b"p" * 32))
    source = tmp_path / "source"
    source.write_bytes(os.urandom(17 * 1024 * 1024))
    digest = portable_artifact(source.read_bytes())["digest"]
    updates = []

    published = volume.publish_blob(
        source,
        digest,
        writer_id="progress-writer",
        source_verified=True,
        progress_callback=lambda completed, total: updates.append((completed, total)),
    )

    assert published.read_bytes() == source.read_bytes()
    assert updates[-1] == (source.stat().st_size, source.stat().st_size)
    assert [completed for completed, _ in updates] == sorted(
        completed for completed, _ in updates
    )


def test_independent_cas_publishers_merge_inventory_without_lost_updates(tmp_path):
    signer = ManifestSigner(b"i" * 32)
    root = tmp_path / "shared-volume"
    publishers = [PreparedStateCAS(root, signer) for _ in range(8)]
    manifests = []
    for index, publisher in enumerate(publishers):
        source = tmp_path / f"object-{index}"
        source.write_bytes(f"object-{index}".encode())
        artifact = portable_artifact(source.read_bytes())
        publisher.publish_blob(source, artifact["digest"], writer_id=f"writer-{index}")
        manifests.append(
            signed_manifest(signer, [artifact], fingerprint({"profile": index}))
        )

    with ThreadPoolExecutor(max_workers=len(publishers)) as pool:
        list(
            pool.map(
                lambda pair: pair[0].publish_manifest(pair[1]),
                zip(publishers, manifests),
            )
        )

    inventory = PreparedStateCAS(root, signer).load_index()
    assert {item["manifest_id"] for item in inventory["manifests"]} == {
        item["manifest_id"] for item in manifests
    }


def test_announcement_failure_does_not_invalidate_publish_and_later_heals(tmp_path):
    class FlakyAuthority:
        def __init__(self):
            self.signer = ManifestSigner(b"a" * 32)
            self.calls = 0

        def sign(self, proposal):
            return self.signer.sign(proposal)

        def verify(self, manifest):
            return self.signer.verify(manifest)

        def announce(self, manifest, *, generation):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("coordinator temporarily unavailable")
            return {"manifest_id": manifest["manifest_id"], "generation": generation}

    authority = FlakyAuthority()
    volume = PreparedStateCAS(tmp_path / "volume", authority)
    source = tmp_path / "source"
    source.write_bytes(b"durable")
    artifact = portable_artifact(source.read_bytes())
    volume.publish_blob(source, artifact["digest"])
    manifest = signed_manifest(authority, [artifact])

    volume.publish_manifest(manifest)

    assert volume.find_manifest(manifest_id=manifest["manifest_id"])
    assert len(list((volume.root / "pending-announcements").glob("*.json"))) == 1
    assert volume.retry_pending_announcements() == (1, 0)
    assert list((volume.root / "pending-announcements").glob("*.json")) == []


def test_bundle_extraction_rejects_fifo_special_member(tmp_path):
    signer = ManifestSigner(b"f" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    archive = tmp_path / "bad-bundle.tar"
    with tarfile.open(archive, "w") as bundle:
        member = tarfile.TarInfo("poison")
        member.type = tarfile.FIFOTYPE
        bundle.addfile(member)
    import hashlib

    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    volume.publish_blob(archive, digest, bundle=True)
    artifact = {
        "digest": "sha256:" + digest,
        "kind": "custom-node-bundle",
        "size": archive.stat().st_size,
        "storage_key": bundle_key(digest),
        "portability": "runtime-bound",
        "requirements": {
            "image_digest": "image",
            "platform": "linux-x86_64",
            "python_abi": "cp311",
            "dependency_lock": "lock",
        },
        "policy": {"tenant": "default", "cacheable": True},
    }
    with pytest.raises(CacheCorruptionError, match="special filesystem member"):
        volume.restore_artifact(
            artifact,
            tmp_path / "restore",
            runtime=artifact["requirements"],
            tenant="default",
        )


def test_corruption_is_detected_and_quarantined(tmp_path):
    signer = ManifestSigner(b"s" * 32)
    volume = PreparedStateCAS(tmp_path / "volume", signer)
    source = tmp_path / "source"
    source.write_bytes(b"valid")
    artifact = portable_artifact(source.read_bytes())
    published = volume.publish_blob(source, artifact["digest"])
    published.write_bytes(b"corrupt")
    with pytest.raises(CacheCorruptionError):
        volume.verify_object(artifact["digest"])
    quarantined = volume.quarantine(artifact["digest"], "digest mismatch")
    assert quarantined and quarantined.read_bytes() == b"corrupt"
    assert not published.exists()


def test_expected_mount_identity_is_required(tmp_path, monkeypatch):
    monkeypatch.delenv("RUNPOD_VOLUME_ID", raising=False)
    cache = PreparedStateCAS(tmp_path / "cache", ManifestSigner(b"s" * 32))
    with pytest.raises(CacheMountError, match="identify"):
        cache.verify_mount("vol-expected")
    monkeypatch.setenv("RUNPOD_VOLUME_ID", "other")
    with pytest.raises(CacheMountError, match="Expected"):
        cache.verify_mount("vol-expected")


class Response:
    def __init__(self, payload=None, status=200):
        self.payload = payload
        self.status_code = status
        self.content = b"x" if payload is not None else b""
        self.response = self

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class Http:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def test_runpod_storage_crud_and_storage_aware_rest_launch():
    volume = {"id": "vol-1", "name": "cache", "size": 250, "dataCenter": "US-KS-2"}
    http = Http(
        Response(volume),
        Response({"id": "pod-1", "status": "PROVISIONING"}, status=201),
        Response(
            {
                "id": "pod-1",
                "status": "RUNNING",
                "gpu": {"id": "gpu", "count": 1},
                "cost": 0.5,
                "dataCenterId": "US-KS-2",
            }
        ),
    )
    connector = RunPodConnector(
        api_key="secret", http_client=http, launch_timeout=1, poll_interval=0
    )
    placement = PlacementConstraints(
        datacenter_ids=("US-KS-2",),
        storage_attachments=(StorageAttachment("vol-1", datacenter_id="US-KS-2"),),
    )
    instance = connector.launch(
        "gpu", "example/runner", env_vars={"X": "1"}, placement=placement
    )
    assert http.calls[0][:2] == (
        "GET",
        "https://api.runpod.io/v2/network-volumes/vol-1",
    )
    create = http.calls[1]
    assert create[:2] == ("POST", "https://api.runpod.io/v2/pods")
    assert create[2]["json"]["mounts"] == {
        "network": [{"volumeId": "vol-1", "path": "/workspace"}]
    }
    assert create[2]["json"]["dataCenterIds"] == ["US-KS-2"]
    assert instance.id == "pod-1"
    assert instance.hourly_rate == 0.5


def test_runpod_network_volume_crud_uses_v2_paths_and_fields():
    volume = {"id": "vol-9", "name": "cache", "size": 250, "dataCenter": "US-KS-2"}
    http = Http(
        Response({"networkVolumes": [volume]}),
        Response(volume, status=201),
        Response(None, status=204),
    )
    connector = RunPodConnector(api_key="secret", http_client=http)

    listed = connector.list_storage()
    created = connector.create_storage(
        name="cache", size_gb=250, datacenter_id="US-KS-2"
    )
    assert connector.delete_storage("vol-9") is True

    assert [item.id for item in listed] == ["vol-9"]
    assert listed[0].datacenter_id == "US-KS-2"
    assert created.size_gb == 250
    assert http.calls[0][:2] == ("GET", "https://api.runpod.io/v2/network-volumes")
    assert http.calls[1][:2] == ("POST", "https://api.runpod.io/v2/network-volumes")
    assert http.calls[1][2]["json"] == {
        "name": "cache",
        "size": 250,
        "dataCenter": "US-KS-2",
    }
    assert http.calls[2][:2] == (
        "DELETE",
        "https://api.runpod.io/v2/network-volumes/vol-9",
    )


def test_runpod_refuses_wrong_dc_community_and_read_only_before_create():
    wrong = Http(Response({"id": "vol", "size": 10, "dataCenter": "EU-RO-1"}))
    connector = RunPodConnector(api_key="secret", http_client=wrong)
    placement = PlacementConstraints(
        datacenter_ids=("US-KS-2",),
        storage_attachments=(StorageAttachment("vol"),),
    )
    with pytest.raises(PlacementError, match="outside requested"):
        connector.launch("gpu", "example/runner", placement=placement)
    assert len(wrong.calls) == 1

    community = RunPodConnector(
        api_key="secret", cloud_type="COMMUNITY", http_client=Http()
    )
    with pytest.raises(PlacementError, match="Secure Cloud"):
        community.list_available(placement=placement)
    readonly = PlacementConstraints(
        storage_attachments=(StorageAttachment("vol", read_only=True),)
    )
    with pytest.raises(PlacementError, match="read-only"):
        connector.list_available(placement=readonly)


@pytest.mark.parametrize("size", [0, 9, 4001])
def test_runpod_rejects_invalid_network_volume_size_before_rest_call(size):
    http = Http()
    connector = RunPodConnector(api_key="secret", http_client=http)
    with pytest.raises(ValueError, match="10-4000"):
        connector.create_storage(name="cache", size_gb=size, datacenter_id="US-KS-2")
    assert http.calls == []


def test_runpod_published_storage_estimate_tier_boundary():
    assert estimate_runpod_storage_monthly(1000) == 70.0
    assert estimate_runpod_storage_monthly(1001) == 70.05


def registered_volume(registry, provider_id, dc):
    return registry.upsert_volume(
        provider="runpod",
        provider_volume_id=provider_id,
        datacenter_id=dc,
        ownership="adopted",
        capacity_bytes=100 * 1024**3,
        policy=policy(),
    )


def test_scheduler_is_deterministic_complete_then_coverage_then_price(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    a = registered_volume(registry, "a", "A")
    b = registered_volume(registry, "b", "B")
    candidates = [
        PlacementCandidate(
            {"id": "cheap-partial", "provider": "runpod", "hourly_rate": 0.1},
            a,
            90,
            100,
            False,
        ),
        PlacementCandidate(
            {"id": "complete", "provider": "runpod", "hourly_rate": 0.5},
            b,
            100,
            100,
            True,
        ),
    ]
    started = time.perf_counter()
    decision = choose_placement(
        policy=policy(), cached_candidates=candidates, cold_offers=[]
    )
    assert time.perf_counter() - started < 1
    assert decision.candidate.offer["id"] == "complete"
    assert decision.reason == "complete_compatible_cache"

    smart = choose_placement(
        policy=policy(),
        cached_candidates=[],
        cold_offers=[{"id": "cold", "provider": "runpod", "hourly_rate": 0.2}],
    )
    assert smart.fallback and smart.action == "launch"
    strict = choose_placement(
        policy=policy(policy="strict"),
        cached_candidates=[],
        cold_offers=[{"id": "cold", "provider": "runpod", "hourly_rate": 0.2}],
    )
    assert strict.action == "unavailable"


def test_profile_fingerprint_changes_with_pinned_weight_revision():
    base = {
        "image": "runner@sha256:" + "a" * 64,
        "custom_nodes": [],
        "weights": [
            {
                "repo_id": "org/model",
                "revision": "sha-one",
                "dest": "checkpoints",
                "files": ["model.safetensors"],
            }
        ],
    }
    first = resolve_prepared_requirements("comfy", base, [])
    second = resolve_prepared_requirements(
        "comfy",
        {
            **base,
            "weights": [{**base["weights"][0], "revision": "sha-two"}],
        },
        [],
    )

    assert first["profile_fingerprint"] != second["profile_fingerprint"]


def test_coordinator_rejects_worker_spoofed_profile_weight_digest(monkeypatch):
    revision = "1" * 40
    config = CloudConfig(
        prepared_storage=policy(),
        worker_profiles={
            "comfy": {
                "image": "example/runner@sha256:" + "a" * 64,
                "models": ["comfyui-workflow"],
                "providers": ["runpod"],
                "weights": [
                    {
                        "repo_id": "org/model",
                        "revision": revision,
                        "files": ["model.safetensors"],
                        "dest": "checkpoints",
                    }
                ],
            }
        },
    )
    profile = fingerprint({"profile": "secure"})
    job = SimpleNamespace(
        params={
            "runtime_profile": "comfy",
            "cache_volume_id": "cache-volume",
            "prepared_requirement": {"profile_fingerprint": profile, "artifacts": []},
        }
    )
    artifact = portable_artifact(
        b"attacker bytes",
        kind="profile-weight",
        source={
            "repo_id": "org/model",
            "revision": revision,
            "filename": "model.safetensors",
        },
        destination={"dest": "checkpoints", "filename": "model.safetensors"},
    )
    proposal = {
        "schema": "cloud-offload.prepared-state.v1",
        "profile_fingerprint": profile,
        "created_at": "2999-01-01T00:00:00Z",
        "producer": {},
        "artifacts": [artifact],
    }
    monkeypatch.setattr(
        server,
        "_trusted_huggingface_digest",
        lambda *identity: "sha256:" + "e" * 64,
    )
    with pytest.raises(ValueError, match="coordinator-verified source"):
        server._validate_manifest_proposal(
            config, proposal, job=job, volume_id="cache-volume"
        )


def test_coordinator_overwrites_worker_timestamp_and_authority_fields():
    content = b"declared"
    artifact = portable_artifact(content)
    profile = fingerprint({"profile": "authority"})
    config = dispatcher_config(Path(os.getcwd()), policy())
    job = SimpleNamespace(
        params={
            "runtime_profile": "comfy",
            "cache_volume_id": "cache-volume",
            "prepared_requirement": {
                "profile_fingerprint": profile,
                "artifacts": [
                    {
                        "digest": artifact["digest"],
                        "policy": {"tenant": "default", "cacheable": True},
                    }
                ],
            },
        }
    )
    proposal = {
        "schema": "cloud-offload.prepared-state.v1",
        "profile_fingerprint": profile,
        "created_at": "2999-01-01T00:00:00Z",
        "producer": {"image_digest": "attacker", "python_abi": "attacker"},
        "artifacts": [artifact],
    }
    server._validate_manifest_proposal(
        config, proposal, job=job, volume_id="cache-volume"
    )
    assert not proposal["created_at"].startswith("2999")
    assert proposal["producer"] == {
        "image_digest": "sha256:" + "a" * 64,
        "cloud_offload_version": server.VERSION,
        "job_id": "",
        "lease_id": "",
        "worker_id": "",
    }
    assert proposal["cache_volume_id"] == "cache-volume"


def test_signed_worker_manifest_keeps_coordinator_job_lease_context_for_m7_proof(tmp_path):
    from cloud_offload.benchmark import CoordinatorBenchmarkDriver

    content = b"cold-cache-model"
    artifact = portable_artifact(content)
    profile = fingerprint({"profile": "authority-e2e"})
    config = dispatcher_config(tmp_path, policy())
    job = SimpleNamespace(
        id="cold-job",
        worker_id="worker-authenticated",
        params={
            "runtime_profile": "comfy",
            "lease_id": "cold-lease",
            "cache_volume_id": "cache-volume",
            "cache_provider_volume_id": "provider-volume",
            "prepared_requirement": {
                "profile_fingerprint": profile,
                "artifacts": [{"digest": artifact["digest"], "policy": {"tenant": "default", "cacheable": True}}],
            },
        },
    )
    proposal = {
        "schema": "cloud-offload.prepared-state.v1",
        "profile_fingerprint": profile,
        "created_at": "2999-01-01T00:00:00Z",
        "producer": {"job_id": "forged-job", "lease_id": "forged-lease", "worker_id": "forged-worker"},
        "artifacts": [artifact],
    }
    server._validate_manifest_proposal(config, proposal, job=job, volume_id="cache-volume")
    assert proposal["producer"] == {
        "image_digest": "sha256:" + "a" * 64,
        "cloud_offload_version": server.VERSION,
        "job_id": "cold-job",
        "lease_id": "cold-lease",
        "worker_id": "worker-authenticated",
    }
    signed = server._prepared_manifest_signer(config).sign(proposal)

    registry = CacheRegistry(config.queue_db_path)
    volume = registry.upsert_volume(
        provider="runpod", provider_volume_id="provider-volume", datacenter_id="US-MD-1",
        ownership="adopted", capacity_bytes=1024, policy={}, volume_id="cache-volume",
    )
    registry.reconcile_index(
        volume.id,
        {"schema": INDEX_SCHEMA, "generation": "generation-1", "manifests": [signed]},
        manifest_documents={signed["manifest_id"]: signed},
    )
    driver = CoordinatorBenchmarkDriver("http://coordinator", None, config, ())
    driver._submission_receipts["cold-job"] = {
        "profile_fingerprint": profile,
        "image_digest": "sha256:" + "a" * 64,
        "expected_model": "comfyui-partition-v1",
        "allowed_regions": ["US-MD-1"],
        "region": "US-MD-1",
    }
    unrelated = dict(signed)
    unrelated["manifest_id"] = "sha256:" + "u" * 64
    unrelated["producer"] = {**signed["producer"], "job_id": "other-job"}

    def request(method, path, **kwargs):
        if path == "/api/jobs/cold-job":
            return SimpleNamespace(
                    json=lambda: {"id": "cold-job", "model": "comfyui-partition-v1", "params": {
                    "lease_id": "cold-lease", "cache_volume_id": "cache-volume",
                    "cache_datacenter_id": "US-MD-1",
                }},
                raise_for_status=lambda: None,
            )
        assert path == "/api/cache/manifests"
        return SimpleNamespace(
            json=lambda: {"manifests": [unrelated, *registry.query_manifests(
                profile_fingerprint=profile, datacenter_id="US-MD-1"
            )]},
            raise_for_status=lambda: None,
        )

    driver._request = request
    result = driver.base_manifest({
        "job_id": "cold-job", "started_at": "2000-01-01T00:00:00+00:00",
        "submission_receipt": driver.submission_receipt("cold-job"),
    })
    assert result["manifest_id"] == signed["manifest_id"]


def test_huggingface_xet_metadata_is_downloaded_and_byte_hashed(monkeypatch, tmp_path):
    import hashlib
    import huggingface_hub

    source = tmp_path / "xet-weight"
    source.write_bytes(b"actual-xet-bytes")
    calls = []
    monkeypatch.setattr(
        huggingface_hub,
        "hf_hub_url",
        lambda **kwargs: "https://huggingface.co/resolved",
    )
    monkeypatch.setattr(
        huggingface_hub,
        "get_hf_file_metadata",
        lambda *args, **kwargs: SimpleNamespace(
            etag="f" * 64, xet_file_data={"hash": "x"}
        ),
    )
    monkeypatch.setattr(
        huggingface_hub,
        "hf_hub_download",
        lambda **kwargs: calls.append(kwargs) or str(source),
    )
    server._HF_SOURCE_DIGESTS.clear()
    digest = server._trusted_huggingface_digest("org/model", "1" * 40, "model")
    assert digest == "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    assert len(calls) == 1


def test_terminal_job_is_not_an_active_manifest_authority_context():
    terminal = SimpleNamespace(worker_id="worker", status=JobStatus.COMPLETED)
    active = SimpleNamespace(worker_id="worker", status=JobStatus.DISPATCHED)
    assert not server._is_active_worker_job(terminal, "worker")
    assert server._is_active_worker_job(active, "worker")


def test_registry_tracks_one_manifest_on_multiple_replica_volumes(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    first = registered_volume(registry, "first", "A")
    second = registered_volume(registry, "second", "B")
    data = b"shared"
    artifact = portable_artifact(data)
    signer = ManifestSigner(b"m" * 32)
    manifest = signed_manifest(signer, [artifact])
    index = {
        "schema": "cloud-offload.prepared-state.index.v1",
        "generation": "g1",
        "manifests": [
            {
                "manifest_id": manifest["manifest_id"],
                "profile_fingerprint": manifest["profile_fingerprint"],
                "created_at": manifest["created_at"],
                "artifacts": manifest["artifacts"],
            }
        ],
    }
    registry.reconcile_index(
        first.id, index, manifest_documents={manifest["manifest_id"]: manifest}
    )
    registry.reconcile_index(
        second.id, index, manifest_documents={manifest["manifest_id"]: manifest}
    )
    matches = registry.query_manifests(
        profile_fingerprint=manifest["profile_fingerprint"]
    )
    assert {item["volume_id"] for item in matches} == {first.id, second.id}


def test_registry_removes_temporary_manifest_and_restores_prior_projection(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registered_volume(registry, "temporary", "A")
    signer = ManifestSigner(b"t" * 32)
    profile = fingerprint({"profile": "temporary"})
    shared = portable_artifact(b"shared")
    synthetic = portable_artifact(b"synthetic")
    base = build_manifest(
        profile_fingerprint=profile,
        producer={"image_digest": "sha256:" + "a" * 64},
        artifacts=[shared],
        signer=signer,
        created_at="2026-01-01T00:00:00Z",
    )
    canary = build_manifest(
        profile_fingerprint=profile,
        producer={"image_digest": "sha256:" + "a" * 64},
        artifacts=[shared, synthetic],
        signer=signer,
        created_at="2026-01-02T00:00:00Z",
    )
    registry.reconcile_index(
        volume.id,
        {
            "schema": "cloud-offload.prepared-state.index.v1",
            "generation": "base-generation",
            "manifests": [base],
        },
        manifest_documents={base["manifest_id"]: base},
    )
    registry.announce_manifest(volume.id, "canary-generation", canary)
    registry.invalidate(volume.id, synthetic["digest"], "benchmark")

    result = registry.remove_manifest(
        volume.id,
        canary["manifest_id"],
        inventory_generation="base-generation",
    )

    assert result == {
        "manifests": 1,
        "artifacts_restored": 1,
        "artifacts_removed": 1,
    }
    assert [item["manifest_id"] for item in registry.query_manifests()] == [
        base["manifest_id"]
    ]
    with registry._connect() as connection:
        projected = connection.execute(
            "SELECT manifest_id FROM cache_artifacts WHERE volume_id=? AND digest=?",
            (volume.id, shared["digest"]),
        ).fetchone()
        removed = connection.execute(
            "SELECT 1 FROM cache_artifacts WHERE volume_id=? AND digest=?",
            (volume.id, synthetic["digest"]),
        ).fetchone()
        invalidation = connection.execute(
            "SELECT 1 FROM cache_invalidations WHERE volume_id=? AND digest=?",
            (volume.id, synthetic["digest"]),
        ).fetchone()
    assert projected["manifest_id"] == base["manifest_id"]
    assert removed is None
    assert invalidation is None
    assert registry.get_volume(volume.id).inventory_generation == "base-generation"


def test_removing_the_invalidated_manifest_restores_volume_health(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registered_volume(registry, "temporary", "A")
    signer = ManifestSigner(b"t" * 32)
    profile = fingerprint({"profile": "temporary"})
    synthetic = portable_artifact(b"synthetic")
    canary = build_manifest(
        profile_fingerprint=profile,
        producer={"image_digest": "sha256:" + "a" * 64},
        artifacts=[synthetic],
        signer=signer,
        created_at="2026-01-02T00:00:00Z",
    )
    registry.announce_manifest(volume.id, "canary-generation", canary)
    registry.invalidate(volume.id, synthetic["digest"], "benchmark")
    assert registry.get_volume(volume.id).status == "degraded"

    registry.remove_manifest(
        volume.id,
        canary["manifest_id"],
        inventory_generation="base-generation",
    )

    assert registry.get_volume(volume.id).status == "ready"


def test_detach_clears_matching_persisted_volume_binding(monkeypatch, tmp_path):
    prepared = policy(existing_volume_id="provider-volume")
    config = CloudConfig(
        prepared_storage=prepared,
        queue_db_path=str(tmp_path / "queue.db"),
    )
    (tmp_path / "config.json").write_text(
        json.dumps({"cloud": {"prepared_storage": prepared}}), encoding="utf-8"
    )
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registry.upsert_volume(
        provider="runpod",
        provider_volume_id="provider-volume",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=10,
        policy=prepared,
    )
    monkeypatch.setattr(config_module, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(server, "_config", lambda *args, **kwargs: config)
    monkeypatch.setattr(server, "_cache_registry", lambda *args, **kwargs: registry)

    response = TestClient(server.app).delete(f"/api/cache/volumes/{volume.id}")

    assert response.status_code == 200
    assert response.json()["cleared_existing_volume_id"] is True
    assert registry.get_volume(volume.id) is None
    persisted = json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))
    assert persisted["cloud"]["prepared_storage"]["existing_volume_id"] is None


def test_conditional_detach_does_not_clear_a_newer_volume_choice(monkeypatch, tmp_path):
    configured = policy(existing_volume_id="new-provider-volume")
    config = CloudConfig(prepared_storage=configured)
    (tmp_path / "config.json").write_text(
        json.dumps({"prepared_storage": configured}), encoding="utf-8"
    )
    monkeypatch.setattr(config_module, "CONFIG_DIR", tmp_path)

    changed = server._persist_prepared_volume_binding(
        config,
        None,
        expected_provider_volume_id="old-provider-volume",
    )

    assert changed is False
    persisted = json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))
    assert persisted["prepared_storage"]["existing_volume_id"] == "new-provider-volume"

    rolled_back = server._persist_prepared_volume_binding(
        config,
        "old-provider-volume",
        expected_provider_volume_id=None,
    )
    assert rolled_back is False
    persisted = json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))
    assert persisted["prepared_storage"]["existing_volume_id"] == "new-provider-volume"


def test_coverage_requires_and_selects_exact_profile_manifest(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registered_volume(registry, "profile-volume", "A")
    signer = ManifestSigner(b"p" * 32)
    old_profile = signed_manifest(signer, [], fingerprint({"profile": "old"}))
    wanted_profile = signed_manifest(signer, [], fingerprint({"profile": "wanted"}))
    index = {
        "schema": "cloud-offload.prepared-state.index.v1",
        "generation": "g-profile",
        "manifests": [
            {
                "manifest_id": manifest["manifest_id"],
                "profile_fingerprint": manifest["profile_fingerprint"],
                "created_at": manifest["created_at"],
                "artifacts": manifest["artifacts"],
            }
            for manifest in (old_profile, wanted_profile)
        ],
    }
    registry.reconcile_index(
        volume.id,
        index,
        manifest_documents={
            old_profile["manifest_id"]: old_profile,
            wanted_profile["manifest_id"]: wanted_profile,
        },
    )

    missing = registry.volume_coverage(
        {},
        runtime={},
        tenant="default",
        profile_fingerprint=fingerprint({"missing": 1}),
    )[0]
    assert not missing["complete"]
    assert missing["manifest_ids"] == []

    selected = registry.volume_coverage(
        {},
        runtime={},
        tenant="default",
        profile_fingerprint=wanted_profile["profile_fingerprint"],
    )[0]
    assert selected["complete"]
    assert selected["manifest_ids"] == [wanted_profile["manifest_id"]]


def test_partial_profile_weights_never_rank_as_complete(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    partial_volume = registered_volume(registry, "partial", "A")
    complete_volume = registered_volume(registry, "complete", "B")
    signer = ManifestSigner(b"l" * 32)
    profile = fingerprint({"profile": "multi-weight"})

    def weight(name, content):
        return portable_artifact(
            content,
            kind="profile-weight",
            source={
                "repo_id": "org/model",
                "revision": "immutable",
                "filename": name,
            },
            destination={"dest": "checkpoints", "filename": name},
        )

    first = weight("first.safetensors", b"first")
    second = weight("second.safetensors", b"second")
    partial = signed_manifest(signer, [first], profile)
    complete = signed_manifest(signer, [first, second], profile)
    for volume, manifest, generation in (
        (partial_volume, partial, "partial-generation"),
        (complete_volume, complete, "complete-generation"),
    ):
        registry.reconcile_index(
            volume.id,
            {
                "schema": "cloud-offload.prepared-state.index.v1",
                "generation": generation,
                "manifests": [manifest],
            },
            manifest_documents={manifest["manifest_id"]: manifest},
        )
    logical = [
        profile_weight_requirement_key("org/model", "immutable", filename)
        for filename in ("first.safetensors", "second.safetensors")
    ]
    coverage = {
        item["volume"].provider_volume_id: item
        for item in registry.volume_coverage(
            {},
            runtime={},
            tenant="default",
            profile_fingerprint=profile,
            logical_required=logical,
        )
    }
    assert coverage["partial"]["complete"] is False
    assert coverage["complete"]["complete"] is True
    assert coverage["complete"]["cached_bytes"] > coverage["partial"]["cached_bytes"]


def test_scheduler_normalizes_pinned_image_but_keeps_unknown_abi_as_miss(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registered_volume(registry, "runtime", "A")
    image_digest = "sha256:" + "a" * 64
    requirements = {
        "runtime_identity": {
            "image": "example/runner@" + image_digest,
            "custom_nodes": [{"id": "pack", "version": "1"}],
            "wheelhouse_sha256": "wheel-lock",
        }
    }
    runtime = scheduler_runtime(requirements)
    assert runtime["image_digest"] == image_digest
    assert "python_abi" not in runtime
    artifact = {
        **portable_artifact(b"bundle"),
        "kind": "custom-node-bundle",
        "storage_key": bundle_key(portable_artifact(b"bundle")["digest"]),
        "portability": "runtime-bound",
        "requirements": {
            "image_digest": image_digest,
            "platform": "linux-x86_64",
            "python_abi": "cp311",
            "dependency_lock": runtime["dependency_lock"],
        },
        "destination": {"pack_id": "pack"},
    }
    manifest = signed_manifest(ManifestSigner(b"r" * 32), [artifact])
    registry.reconcile_index(
        volume.id,
        {
            "schema": "cloud-offload.prepared-state.index.v1",
            "generation": "runtime-generation",
            "manifests": [manifest],
        },
        manifest_documents={manifest["manifest_id"]: manifest},
    )
    logical = [custom_node_requirement_key("pack")]
    unknown = registry.volume_coverage(
        {},
        runtime=runtime,
        tenant="default",
        profile_fingerprint=manifest["profile_fingerprint"],
        logical_required=logical,
    )[0]
    assert unknown["cached_bytes"] == 0
    assert not unknown["complete"]
    known = registry.volume_coverage(
        {},
        runtime={
            **runtime,
            "platform": "linux-x86_64",
            "python_abi": "cp311",
        },
        tenant="default",
        profile_fingerprint=manifest["profile_fingerprint"],
        logical_required=logical,
    )[0]
    assert known["cached_bytes"] == len(b"bundle")
    assert known["complete"]


def test_scheduler_uses_runtime_identity_declared_by_pinned_profile():
    image_digest = "sha256:" + "b" * 64
    requirements = {
        "runtime_identity": {
            "image": "example/runner@" + image_digest,
            "custom_nodes": [],
            "wheelhouse_sha256": None,
        },
        "runtime_constraints": {
            "platform": "linux-x86_64",
            "python_abi": "cp311",
        },
    }

    runtime = scheduler_runtime(requirements)

    assert runtime["image_digest"] == image_digest
    assert runtime["platform"] == "linux-x86_64"
    assert runtime["python_abi"] == "cp311"


def test_corruption_observation_invalidates_future_scheduler_coverage(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registered_volume(registry, "corrupt-volume", "A")
    artifact = portable_artifact(b"soon-corrupt")
    manifest = signed_manifest(ManifestSigner(b"o" * 32), [artifact])
    registry.reconcile_index(
        volume.id,
        {
            "schema": "cloud-offload.prepared-state.index.v1",
            "generation": "before-corruption",
            "manifests": [manifest],
        },
        manifest_documents={manifest["manifest_id"]: manifest},
    )
    required = {artifact["digest"]: artifact["size"]}
    before = registry.volume_coverage(
        required,
        runtime={},
        tenant="default",
        profile_fingerprint=manifest["profile_fingerprint"],
    )[0]
    assert before["complete"]
    registry.record_observation(
        {
            "schema": "cloud-offload.restore-observation.v1",
            "volume_id": volume.id,
            "manifest_id": manifest["manifest_id"],
            "digest": artifact["digest"],
            "datacenter_id": "A",
            "worker_class": "GPU",
            "strategy": "symlink",
            "result": "corruption",
            "bytes": 0,
            "file_count": 1,
            "lookup_ms": 0,
            "transfer_ms": 0,
            "verification_ms": 1,
            "extraction_ms": 0,
            "import_ms": 0,
            "total_ms": 1,
        }
    )
    after = registry.volume_coverage(
        required,
        runtime={},
        tenant="default",
        profile_fingerprint=manifest["profile_fingerprint"],
    )
    assert after == []
    assert registry.get_volume(volume.id).status == "degraded"
    registry.record_observation(
        {
            "schema": "cloud-offload.restore-observation.v1",
            "volume_id": volume.id,
            "manifest_id": manifest["manifest_id"],
            "digest": artifact["digest"],
            "datacenter_id": "A",
            "worker_class": "GPU",
            "strategy": "symlink",
            "result": "hit",
            "verification_mode": "full_digest",
            "bytes": artifact["size"],
            "file_count": 1,
            "lookup_ms": 0,
            "transfer_ms": 0,
            "verification_ms": 1,
            "extraction_ms": 0,
            "import_ms": 0,
            "total_ms": 1,
        }
    )
    recovered = registry.volume_coverage(
        required,
        runtime={},
        tenant="default",
        profile_fingerprint=manifest["profile_fingerprint"],
    )
    assert recovered[0]["complete"]
    assert registry.get_volume(volume.id).status == "ready"


class MissingObject(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class RunPodMissingObject(Exception):
    response = {"Error": {"Code": "InvalidArgument", "Message": "object not found"}}


def test_runpod_s3_default_client_uses_long_transfer_timeouts(monkeypatch, tmp_path):
    import boto3

    created = []

    def client(service, **kwargs):
        created.append({"service": service, **kwargs})
        return SimpleNamespace()

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setattr(boto3, "client", client)

    store = RunPodS3PreparedStore.from_environment(
        volume_id="volume",
        datacenter_id="EU-RO-1",
        endpoint_url="https://s3.example.invalid",
        scratch_dir=tmp_path / "scratch",
    )

    transfer = created[0]["config"]
    range_transfer = created[1]["config"]
    assert store.client is not None
    assert store.range_client is not store.client
    assert [item["service"] for item in created] == ["s3", "s3"]
    assert transfer.connect_timeout == 30
    assert transfer.read_timeout == 7200
    assert transfer.tcp_keepalive is True
    assert transfer.max_pool_connections == 32
    assert transfer.retries == {"max_attempts": 10, "mode": "standard"}
    assert range_transfer.connect_timeout == 30
    assert range_transfer.read_timeout == 90
    assert range_transfer.tcp_keepalive is True
    assert range_transfer.max_pool_connections == 32
    assert range_transfer.retries == {
        "total_max_attempts": 1,
        "mode": "standard",
    }
    assert store.transfer_config.multipart_threshold == 64 * 1024 * 1024
    assert store.transfer_config.multipart_chunksize == 50 * 1024 * 1024
    assert store.transfer_config.max_concurrency == 4
    assert store.transfer_config.use_threads is True
    assert store.scratch_dir == tmp_path / "scratch"


def test_s3_upload_recovers_completed_merge_after_wrapped_524(monkeypatch, tmp_path):
    class S3UploadFailedError(Exception):
        pass

    class CompletingS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.pending = None
            self.pending_head_calls = 0
            self.upload_config = None

        def upload_file(self, filename, bucket, key, Config=None):
            self.pending = (key, Path(filename).read_bytes())
            self.upload_config = Config
            raise S3UploadFailedError("upload failed after (524) completion timeout")

        def head_object(self, Bucket, Key):
            if self.pending and Key == self.pending[0]:
                self.pending_head_calls += 1
                if self.pending_head_calls == 3:
                    self.objects[Key] = self.pending[1]
            return super().head_object(Bucket=Bucket, Key=Key)

    sleeps = []
    monkeypatch.setattr(prepared_state_module.time, "sleep", sleeps.append)
    transfer_config = object()
    client = CompletingS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        prefix="cloud-offload",
        transfer_config=transfer_config,
    )
    source = tmp_path / "large-model"
    source.write_bytes(b"verified model bytes")
    artifact = portable_artifact(source.read_bytes())

    assert store.upload_verified(source, artifact["digest"]) == artifact["storage_key"]
    assert client.upload_config is transfer_config
    assert client.pending_head_calls == 3
    assert sleeps == [5, 5]


def test_s3_upload_recovers_delayed_invalid_part_after_exact_object_exists(tmp_path):
    class InvalidPartError(Exception):
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 400},
            "Error": {"Code": "InvalidPart"},
        }

    class CompletedS3(MemoryS3):
        def upload_file(self, filename, bucket, key, Config=None):
            self.objects[key] = Path(filename).read_bytes()
            raise InvalidPartError("completion retry arrived after the merge")

    client = CompletedS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        transfer_config=object(),
    )
    source = tmp_path / "model"
    source.write_bytes(b"verified object after multipart merge")
    artifact = portable_artifact(source.read_bytes())

    assert store.upload_verified(source, artifact["digest"]) == artifact["storage_key"]
    assert client.objects[artifact["storage_key"]] == source.read_bytes()


def test_s3_upload_does_not_hide_non_timeout_failure(tmp_path):
    class DeniedS3(MemoryS3):
        def upload_file(self, filename, bucket, key, Config=None):
            raise PermissionError("upload denied")

    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=DeniedS3(),
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        transfer_config=object(),
    )
    source = tmp_path / "model"
    source.write_bytes(b"model")

    with pytest.raises(PermissionError, match="upload denied"):
        store.upload_verified(source, portable_artifact(b"model")["digest"])


def test_s3_large_upload_resumes_completed_parts_after_gateway_failure(
    monkeypatch, tmp_path
):
    class GatewayError(Exception):
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 524},
            "Error": {"Code": "524"},
        }

    class ResumableS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.uploads = {}
            self.upload_attempts = []
            self.fail_part_two = True

        def list_multipart_uploads(self, Bucket, Prefix, **kwargs):
            return {
                "Uploads": [
                    {"Key": key, "UploadId": upload_id, "Initiated": upload_id}
                    for upload_id, (key, _parts) in self.uploads.items()
                    if key.startswith(Prefix)
                ],
                "IsTruncated": False,
            }

        def create_multipart_upload(self, Bucket, Key):
            upload_id = f"upload-{len(self.uploads) + 1}"
            self.uploads[upload_id] = (Key, {})
            return {"UploadId": upload_id}

        def list_parts(self, Bucket, Key, UploadId, **kwargs):
            upload_key, parts = self.uploads[UploadId]
            assert upload_key == Key
            return {
                "Parts": [
                    {"PartNumber": number, "ETag": etag, "Size": len(payload)}
                    for number, (etag, payload) in sorted(parts.items())
                ],
                "IsTruncated": False,
            }

        def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
            self.upload_attempts.append(PartNumber)
            if PartNumber == 2 and self.fail_part_two:
                raise GatewayError()
            etag = f'"part-{PartNumber}"'
            self.uploads[UploadId][1][PartNumber] = (etag, bytes(Body))
            return {"ETag": etag}

        def complete_multipart_upload(
            self, Bucket, Key, UploadId, MultipartUpload
        ):
            upload_key, parts = self.uploads.pop(UploadId)
            assert upload_key == Key
            assert [item["PartNumber"] for item in MultipartUpload["Parts"]] == list(
                range(1, len(parts) + 1)
            )
            self.objects[Key] = b"".join(
                payload for _etag, payload in dict(sorted(parts.items())).values()
            )
            return {"ETag": '"complete"'}

    class BoundedPartClient:
        def __init__(self, delegate):
            self.delegate = delegate
            self.part_calls = []

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        def upload_part(self, **kwargs):
            self.part_calls.append(kwargs["PartNumber"])
            return self.delegate.upload_part(**kwargs)

    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_THRESHOLD_BYTES", 1
    )
    monkeypatch.setattr(prepared_state_module, "RUNPOD_S3_MULTIPART_CHUNK_BYTES", 8)
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_MIN_PART_BYTES", 5
    )
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_CONCURRENCY", 2
    )
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_GATEWAY_MAX_BACKOFF_SECONDS", 0
    )
    monkeypatch.setattr(prepared_state_module.time, "sleep", lambda _seconds: None)
    client = ResumableS3()
    bounded = BoundedPartClient(client)
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        range_client=bounded,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
    )
    source = tmp_path / "large-model"
    source.write_bytes(b"0123456789abcdefghijklmn")
    artifact = portable_artifact(source.read_bytes())

    with pytest.raises(GatewayError):
        store.upload_verified(source, artifact["digest"])
    completed_before_retry = set(client.uploads["upload-1"][1])
    assert completed_before_retry
    assert 2 not in completed_before_retry

    client.fail_part_two = False
    assert store.upload_verified(source, artifact["digest"]) == artifact["storage_key"]
    assert all(client.upload_attempts.count(part) == 1 for part in completed_before_retry)
    assert bounded.part_calls == client.upload_attempts
    assert client.objects[artifact["storage_key"]] == source.read_bytes()


def test_s3_resume_prefers_the_valid_upload_with_the_most_completed_parts():
    class CandidateS3:
        def list_multipart_uploads(self, **kwargs):
            return {
                "Uploads": [
                    {"Key": "cloud-offload/blobs/model", "UploadId": "older", "Initiated": "1"},
                    {"Key": "cloud-offload/blobs/model", "UploadId": "newer", "Initiated": "2"},
                ],
                "IsTruncated": False,
            }

        def list_parts(self, UploadId, **kwargs):
            count = 2 if UploadId == "older" else 1
            return {
                "Parts": [
                    {"PartNumber": number, "ETag": f'"part-{number}"', "Size": 8}
                    for number in range(1, count + 1)
                ],
                "IsTruncated": False,
            }

    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=CandidateS3(),
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        prefix="cloud-offload",
    )

    upload_id, parts = store._find_resumable_multipart(
        "cloud-offload/blobs/model", source_size=24, part_size=8
    )

    assert upload_id == "older"
    assert parts == {1: '"part-1"', 2: '"part-2"'}


def test_s3_resume_ignores_sessions_older_than_the_maximum_copy_lifetime():
    stale_time = datetime.now(timezone.utc) - timedelta(days=8)

    class StaleCandidateS3:
        def list_multipart_uploads(self, **kwargs):
            return {
                "Uploads": [
                    {
                        "Key": "cloud-offload/blobs/model",
                        "UploadId": "stale",
                        "Initiated": stale_time,
                    }
                ],
                "IsTruncated": False,
            }

        def list_parts(self, **kwargs):
            raise AssertionError("stale multipart parts must not be listed")

    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=StaleCandidateS3(),
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
        prefix="cloud-offload",
    )

    upload_id, parts = store._find_resumable_multipart(
        "cloud-offload/blobs/model", source_size=24, part_size=8
    )

    assert upload_id is None
    assert parts == {}


def test_s3_resume_accepts_a_recent_provider_timestamp():
    recent_time = datetime.now(timezone.utc) - timedelta(minutes=5)

    class RecentCandidateS3:
        def list_multipart_uploads(self, **kwargs):
            return {
                "Uploads": [
                    {
                        "Key": "cloud-offload/blobs/model",
                        "UploadId": "recent",
                        "Initiated": recent_time,
                    }
                ],
                "IsTruncated": False,
            }

        def list_parts(self, **kwargs):
            return {
                "Parts": [
                    {"PartNumber": 1, "ETag": '"part-1"', "Size": 8},
                ],
                "IsTruncated": False,
            }

    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=RecentCandidateS3(),
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
        prefix="cloud-offload",
    )

    upload_id, parts = store._find_resumable_multipart(
        "cloud-offload/blobs/model", source_size=24, part_size=8
    )

    assert upload_id == "recent"
    assert parts == {1: '"part-1"'}


def test_s3_multipart_failure_does_not_start_queued_parts(monkeypatch, tmp_path):
    class BoundedFailureS3:
        def __init__(self):
            self.started = []
            self.barrier = threading.Barrier(2)

        def list_multipart_uploads(self, **kwargs):
            return {"Uploads": [], "IsTruncated": False}

        def create_multipart_upload(self, **kwargs):
            return {"UploadId": "new"}

        def upload_part(self, PartNumber, **kwargs):
            self.started.append(PartNumber)
            self.barrier.wait(timeout=2)
            if PartNumber == 1:
                raise PermissionError("part denied")
            return {"ETag": f'"part-{PartNumber}"'}

    monkeypatch.setattr(prepared_state_module, "RUNPOD_S3_MULTIPART_CHUNK_BYTES", 8)
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_MIN_PART_BYTES", 5
    )
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_CONCURRENCY", 2
    )
    client = BoundedFailureS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=client,
        range_client=client,
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
    )
    source = tmp_path / "large-model"
    source.write_bytes(b"0123456789abcdefghijklmnopqrstuv")

    with pytest.raises(PermissionError, match="part denied"):
        store._upload_resumable_multipart(
            source, "blobs/model", source.stat().st_size
        )

    assert sorted(client.started) == [1, 2]


def test_s3_worker_failure_stops_sibling_retry_before_controller_observes_it(
    monkeypatch, tmp_path
):
    class GatewayError(Exception):
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 524},
            "Error": {"Code": "524"},
        }

    class CoordinatedFailureS3:
        def __init__(self):
            self.barrier = threading.Barrier(2)
            self.started = []
            self.part_two_calls = 0

        def list_multipart_uploads(self, **kwargs):
            return {"Uploads": [], "IsTruncated": False}

        def create_multipart_upload(self, **kwargs):
            return {"UploadId": "new"}

        def upload_part(self, PartNumber, **kwargs):
            self.started.append(PartNumber)
            if PartNumber == 1:
                self.barrier.wait(timeout=2)
                raise PermissionError("part denied")
            if PartNumber == 2:
                self.part_two_calls += 1
                if self.part_two_calls == 1:
                    self.barrier.wait(timeout=2)
                raise GatewayError()
            return {"ETag": f'"part-{PartNumber}"'}

    controller_release = threading.Event()
    real_wait = prepared_state_module.wait

    def delayed_controller_wait(*args, **kwargs):
        assert controller_release.wait(timeout=2)
        return real_wait(*args, **kwargs)

    monkeypatch.setattr(prepared_state_module, "wait", delayed_controller_wait)
    monkeypatch.setattr(prepared_state_module, "RUNPOD_S3_MULTIPART_CHUNK_BYTES", 8)
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_MIN_PART_BYTES", 5
    )
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_CONCURRENCY", 2
    )
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_GATEWAY_MAX_BACKOFF_SECONDS", 0.05
    )
    client = CoordinatedFailureS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=client,
        range_client=client,
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
    )
    source = tmp_path / "large-model"
    source.write_bytes(b"0123456789abcdefghijklmnopqrstuv")
    errors = []

    def run_upload():
        try:
            store._upload_resumable_multipart(
                source, "blobs/model", source.stat().st_size
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_upload)
    thread.start()
    time.sleep(0.25)

    assert client.part_two_calls == 1
    assert sorted(client.started) == [1, 2]

    controller_release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], (PermissionError, GatewayError))
    assert str(errors[0]) != "Multipart upload was cancelled"


def test_s3_does_not_submit_a_replacement_after_worker_cancellation(
    monkeypatch, tmp_path
):
    cancellation_seen = threading.Event()
    release_failure = threading.Event()

    class PausingCancelEvent:
        def __init__(self):
            self.inner = threading.Event()
            self.set_calls = 0

        def is_set(self):
            return self.inner.is_set()

        def wait(self, timeout=None):
            return self.inner.wait(timeout)

        def set(self):
            self.set_calls += 1
            self.inner.set()
            if self.set_calls == 1:
                cancellation_seen.set()
                assert release_failure.wait(timeout=2)

    class OneSuccessOneFailureS3:
        def __init__(self):
            self.barrier = threading.Barrier(2)

        def list_multipart_uploads(self, **kwargs):
            return {"Uploads": [], "IsTruncated": False}

        def create_multipart_upload(self, **kwargs):
            return {"UploadId": "new"}

        def upload_part(self, PartNumber, **kwargs):
            self.barrier.wait(timeout=2)
            if PartNumber == 2:
                raise PermissionError("terminal provider failure")
            return {"ETag": f'"part-{PartNumber}"'}

    monkeypatch.setattr(prepared_state_module, "RUNPOD_S3_MULTIPART_CHUNK_BYTES", 8)
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_MIN_PART_BYTES", 5
    )
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_CONCURRENCY", 2
    )
    client = OneSuccessOneFailureS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=client,
        range_client=client,
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
    )
    monkeypatch.setattr(
        prepared_state_module,
        "threading",
        SimpleNamespace(Event=PausingCancelEvent),
    )
    source = tmp_path / "large-model"
    source.write_bytes(b"0123456789abcdefghijklmnopqrstuv")
    errors = []

    def run_upload():
        try:
            store._upload_resumable_multipart(
                source, "blobs/model", source.stat().st_size
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_upload)
    thread.start()

    assert cancellation_seen.wait(timeout=2)
    time.sleep(0.1)
    release_failure.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], PermissionError)
    assert str(errors[0]) == "terminal provider failure"


def test_s3_resume_reuses_exact_parts_and_ignores_partial_part_records():
    class PartialCandidateS3:
        def list_multipart_uploads(self, **kwargs):
            return {
                "Uploads": [
                    {
                        "Key": "cloud-offload/blobs/model",
                        "UploadId": "partial",
                        "Initiated": "1",
                    }
                ],
                "IsTruncated": False,
            }

        def list_parts(self, **kwargs):
            return {
                "Parts": [
                    {"PartNumber": 1, "ETag": '"part-1"', "Size": 8},
                    {"PartNumber": 2, "ETag": '"partial-2"', "Size": 3},
                    {"PartNumber": 3, "ETag": "", "Size": 8},
                    {"PartNumber": 4, "ETag": '"outside"', "Size": 8},
                ],
                "IsTruncated": False,
            }

    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=PartialCandidateS3(),
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
        prefix="cloud-offload",
    )

    upload_id, parts = store._find_resumable_multipart(
        "cloud-offload/blobs/model", source_size=24, part_size=8
    )

    assert upload_id == "partial"
    assert parts == {1: '"part-1"'}


def test_s3_large_upload_starts_new_session_when_resume_listing_is_unavailable(
    monkeypatch, tmp_path
):
    class ListingUnavailable(Exception):
        operation_name = "ListMultipartUploads"
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 530},
            "Error": {"Code": "530"},
        }

    class ReplacementS3:
        def __init__(self):
            self.list_calls = 0
            self.created = 0
            self.parts = {}
            self.completed = False

        def list_multipart_uploads(self, **kwargs):
            self.list_calls += 1
            raise ListingUnavailable()

        def create_multipart_upload(self, **kwargs):
            self.created += 1
            return {"UploadId": "replacement"}

        def upload_part(self, PartNumber, Body, **kwargs):
            self.parts[PartNumber] = bytes(Body)
            return {"ETag": f'"part-{PartNumber}"'}

        def complete_multipart_upload(self, MultipartUpload, **kwargs):
            assert [item["PartNumber"] for item in MultipartUpload["Parts"]] == [
                1,
                2,
                3,
            ]
            self.completed = True
            return {"ETag": '"complete"'}

    monkeypatch.setattr(prepared_state_module, "RUNPOD_S3_MULTIPART_CHUNK_BYTES", 8)
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_MIN_PART_BYTES", 5
    )
    monkeypatch.setattr(
        prepared_state_module, "RUNPOD_S3_MULTIPART_CONCURRENCY", 2
    )
    monkeypatch.setattr(prepared_state_module.time, "sleep", lambda _seconds: None)
    client = ReplacementS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=client,
        range_client=client,
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
    )
    source = tmp_path / "large-model"
    source.write_bytes(b"0123456789abcdefghijklmn")

    store._upload_resumable_multipart(source, "blobs/model", source.stat().st_size)

    assert client.list_calls == prepared_state_module.RUNPOD_S3_GATEWAY_ATTEMPTS
    assert client.created == 1
    assert b"".join(client.parts[number] for number in sorted(client.parts)) == (
        source.read_bytes()
    )
    assert client.completed is True


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.lock = threading.Lock()
        self.copy_calls = 0
        self.download_calls = 0
        self.get_calls = 0
        self.ranges = []

    def head_bucket(self, **kwargs):
        return {}

    def head_object(self, Bucket, Key):
        with self.lock:
            if Key not in self.objects:
                raise MissingObject()
            return {"ContentLength": len(self.objects[Key])}

    def upload_file(self, filename, bucket, key):
        with self.lock:
            self.objects[key] = Path(filename).read_bytes()

    def download_file(self, bucket, key, filename):
        self.download_calls += 1
        with self.lock:
            value = self.objects[key]
        Path(filename).write_bytes(value)

    def copy_object(self, Bucket, Key, CopySource):
        self.copy_calls += 1
        with self.lock:
            self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, Bucket, Key):
        with self.lock:
            self.objects.pop(Key, None)

    def put_object(self, Bucket, Key, Body):
        value = Body.read() if hasattr(Body, "read") else bytes(Body)
        with self.lock:
            self.objects[Key] = value

    def get_object(self, Bucket, Key, Range=None):
        self.get_calls += 1
        self.ranges.append(Range)
        with self.lock:
            if Key not in self.objects:
                raise MissingObject()
            value = self.objects[Key]
        if Range:
            start_text, end_text = Range.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = min(int(end_text), len(value) - 1)
            return {
                "Body": io.BytesIO(value[start : end + 1]),
                "ContentRange": f"bytes {start}-{end}/{len(value)}",
            }
        return {"Body": io.BytesIO(value)}


def test_s3_verified_download_uses_the_bounded_range_client(tmp_path):
    class SlowPrimary(MemoryS3):
        def get_object(self, **kwargs):
            raise AssertionError("range download used the long-timeout client")

    payload = b"bounded range response"
    artifact = portable_artifact(payload)
    bounded = MemoryS3()
    bounded.objects[artifact["storage_key"]] = payload
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=SlowPrimary(),
        range_client=bounded,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
    )
    destination = tmp_path / "downloaded"

    assert store.download_verified(
        artifact["storage_key"], artifact["digest"], destination
    ) == destination
    assert destination.read_bytes() == payload
    assert bounded.ranges == ["bytes=0-67108863"]


def test_s3_post_upload_verification_uses_configured_scratch_directory(
    tmp_path, monkeypatch
):
    scratch = tmp_path / "scratch"
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=MemoryS3(),
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        scratch_dir=scratch,
    )
    scratch.mkdir()
    observed = {}

    def download_verified(key, digest, destination):
        observed["destination"] = destination
        destination.write_bytes(b"x")

    monkeypatch.setattr(store, "download_verified", download_verified)

    digest = "sha256:2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
    store._download_verify("blobs/value", digest, 1)

    assert observed["destination"].is_relative_to(scratch)
    assert not observed["destination"].exists()


def test_independent_s3_stores_publish_concurrently_without_losing_inventory(tmp_path):
    client = MemoryS3()
    first = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="US-KS-2",
        client=client,
        endpoint_url="https://s3api-us-ks-2.runpod.io/",
    )
    second = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="US-KS-2",
        client=client,
        endpoint_url="https://s3api-us-ks-2.runpod.io/",
    )
    assert first.publication_lock is second.publication_lock
    signer = ManifestSigner(b"z" * 32)
    manifests = []
    for index, store in enumerate((first, second)):
        source = tmp_path / f"model-{index}"
        source.write_bytes(f"model {index}".encode())
        artifact = portable_artifact(source.read_bytes())
        store.upload_verified(source, artifact["digest"])
        manifests.append(
            (store, signed_manifest(signer, [artifact], fingerprint({"p": index})))
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(
            pool.map(lambda pair: pair[0].publish_manifest(pair[1], signer), manifests)
        )
    index = first.load_index()
    assert {item["manifest_id"] for item in index["manifests"]} == {
        item[1]["manifest_id"] for item in manifests
    }
    assert client.copy_calls == 0
    assert client.get_calls >= 2


def test_s3_verified_download_does_not_require_head_or_download_file(tmp_path):
    class GetOnlyS3(MemoryS3):
        def head_object(self, Bucket, Key):
            raise PermissionError("HEAD is not available")

        def download_file(self, bucket, key, filename):
            raise AssertionError("download_file sends HEAD before GET")

    client = GetOnlyS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=client,
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"runtime bundle that supports GET only"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload
    destination = tmp_path / "runtime-bundle"

    assert store.download_verified(
        artifact["storage_key"], artifact["digest"], destination
    ) == destination
    assert destination.read_bytes() == payload


def test_s3_verified_download_uses_bounded_ranges(monkeypatch, tmp_path):
    monkeypatch.setattr(prepared_state_module, "S3_VERIFICATION_RANGE_BYTES", 5)
    client = MemoryS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"0123456789abcdef"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload
    destination = tmp_path / "ranged-bundle"

    store.download_verified(artifact["storage_key"], artifact["digest"], destination)

    assert destination.read_bytes() == payload
    assert client.ranges[0] == "bytes=0-4"
    assert sorted(client.ranges[1:]) == sorted(
        [
            "bytes=5-9",
            "bytes=10-14",
            "bytes=15-15",
        ]
    )


def test_s3_verified_download_reads_ranges_concurrently(monkeypatch, tmp_path):
    monkeypatch.setattr(prepared_state_module, "S3_VERIFICATION_RANGE_BYTES", 5)
    monkeypatch.setattr(prepared_state_module, "S3_DOWNLOAD_CONCURRENCY", 3)

    class ConcurrentRangeS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.active = 0
            self.maximum_active = 0
            self.concurrent_entries = 0
            self.active_lock = threading.Lock()
            self.first_batch = threading.Barrier(3, timeout=2)

        def get_object(self, Bucket, Key, Range=None):
            if Range == "bytes=0-4" or Range is None:
                return super().get_object(Bucket=Bucket, Key=Key, Range=Range)
            with self.active_lock:
                self.active += 1
                self.maximum_active = max(self.maximum_active, self.active)
                self.concurrent_entries += 1
                entry = self.concurrent_entries
            try:
                if entry <= 3:
                    self.first_batch.wait()
                return super().get_object(Bucket=Bucket, Key=Key, Range=Range)
            finally:
                with self.active_lock:
                    self.active -= 1

    client = ConcurrentRangeS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=client,
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"0123456789abcdefghijklmnop"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload
    destination = tmp_path / "parallel-bundle"

    store.download_verified(artifact["storage_key"], artifact["digest"], destination)

    assert destination.read_bytes() == payload
    assert client.maximum_active == 3


def test_s3_verified_download_limits_each_range_body_timeout(monkeypatch, tmp_path):
    monkeypatch.setattr(prepared_state_module, "S3_VERIFICATION_RANGE_BYTES", 5)
    monkeypatch.setattr(prepared_state_module, "S3_RANGE_SOCKET_TIMEOUT_SECONDS", 7)

    class TimedBody(io.BytesIO):
        def __init__(self, value, observed):
            super().__init__(value)
            self.observed = observed

        def set_socket_timeout(self, seconds):
            self.observed.append(seconds)

    class TimedRangeS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.body_timeouts = []

        def get_object(self, Bucket, Key, Range=None):
            response = super().get_object(Bucket=Bucket, Key=Key, Range=Range)
            response["Body"] = TimedBody(
                response["Body"].read(), self.body_timeouts
            )
            return response

    client = TimedRangeS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"0123456789abcdef"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload

    store.download_verified(
        artifact["storage_key"], artifact["digest"], tmp_path / "timed-bundle"
    )

    assert client.body_timeouts == [7, 7, 7, 7]


@pytest.mark.parametrize("gateway_code", [520, 524])
def test_s3_verified_download_retries_runpod_gateway_5xx(
    monkeypatch, tmp_path, gateway_code
):
    class GatewayTimeout(Exception):
        response = {
            "ResponseMetadata": {"HTTPStatusCode": gateway_code},
            "Error": {"Code": str(gateway_code)},
        }

    class ColdObjectS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.remaining_timeouts = 2

        def get_object(self, Bucket, Key, Range=None):
            if self.remaining_timeouts:
                self.remaining_timeouts -= 1
                raise GatewayTimeout()
            return super().get_object(Bucket=Bucket, Key=Key, Range=Range)

    sleeps = []
    monkeypatch.setattr(prepared_state_module.time, "sleep", sleeps.append)
    client = ColdObjectS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="US-MD-1",
        client=client,
        endpoint_url="https://s3api-us-md-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"cold mounted object"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload
    destination = tmp_path / "cold-object"

    store.download_verified(artifact["storage_key"], artifact["digest"], destination)

    assert destination.read_bytes() == payload
    assert sleeps == [2, 4]


def test_s3_verified_download_retries_transient_get_signature_error(
    monkeypatch, tmp_path
):
    class SignatureMismatch(Exception):
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 403},
            "Error": {"Code": "SignatureDoesNotMatch"},
        }
        operation_name = "GetObject"

    class TransientSignatureS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.remaining_failures = 2

        def get_object(self, Bucket, Key, Range=None):
            if self.remaining_failures:
                self.remaining_failures -= 1
                raise SignatureMismatch()
            return super().get_object(Bucket=Bucket, Key=Key, Range=Range)

    sleeps = []
    monkeypatch.setattr(prepared_state_module.time, "sleep", sleeps.append)
    client = TransientSignatureS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"transient signature object"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload
    destination = tmp_path / "signature-object"

    store.download_verified(artifact["storage_key"], artifact["digest"], destination)

    assert destination.read_bytes() == payload
    assert sleeps == [2, 4]


def test_s3_signature_error_is_not_retryable_for_write_operations():
    class SignatureMismatch(Exception):
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 403},
            "Error": {"Code": "SignatureDoesNotMatch"},
        }
        operation_name = "PutObject"

    assert not RunPodS3PreparedStore._is_gateway_retryable(SignatureMismatch())


@pytest.mark.parametrize(
    "error_name",
    ["SSLError", "ConnectionClosedError", "EndpointConnectionError"],
)
def test_s3_gateway_retry_recovers_transient_transport_failures(
    monkeypatch, error_name
):
    transport_error = type(error_name, (Exception,), {})
    attempts = 0
    sleeps = []

    def operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise transport_error("transient RunPod S3 transport failure")
        return {"ETag": '"verified-part"'}

    monkeypatch.setattr(prepared_state_module.time, "sleep", sleeps.append)
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EUR-IS-1",
        client=MemoryS3(),
        endpoint_url="https://s3api-eur-is-1.runpod.io/",
    )

    assert store._call_with_gateway_retry(operation) == {
        "ETag": '"verified-part"'
    }
    assert attempts == 3
    assert sleeps == [2, 4]


def test_s3_verified_download_retries_incomplete_range_stream(monkeypatch, tmp_path):
    class ResponseStreamingError(Exception):
        pass

    class BrokenBody:
        def read(self, size=-1):
            raise ResponseStreamingError("incomplete range")

        def close(self):
            return None

    class InterruptedRangeS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.remaining_first_interruptions = 1
            self.remaining_later_interruptions = 2

        def get_object(self, Bucket, Key, Range=None):
            response = super().get_object(Bucket=Bucket, Key=Key, Range=Range)
            if Range == "bytes=0-4" and self.remaining_first_interruptions:
                self.remaining_first_interruptions -= 1
                response["Body"] = BrokenBody()
            elif Range != "bytes=0-4" and self.remaining_later_interruptions:
                self.remaining_later_interruptions -= 1
                response["Body"] = BrokenBody()
            return response

    sleeps = []
    monkeypatch.setattr(prepared_state_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(prepared_state_module, "S3_VERIFICATION_RANGE_BYTES", 5)
    client = InterruptedRangeS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"range stream recovers after interruption"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload
    destination = tmp_path / "recovered-range"

    store.download_verified(artifact["storage_key"], artifact["digest"], destination)

    assert destination.read_bytes() == payload
    assert sleeps == [2, 2, 4]


def test_s3_verified_download_resumes_after_partial_range_stream(monkeypatch, tmp_path):
    class ResponseStreamingError(Exception):
        pass

    class PartialBody(io.BytesIO):
        def __init__(self, value):
            super().__init__(value)
            self.read_count = 0

        def read(self, size=-1):
            self.read_count += 1
            if self.read_count == 1:
                return super().read(2)
            raise ResponseStreamingError("partial range")

    class PartialRangeS3(MemoryS3):
        def __init__(self):
            super().__init__()
            self.interrupted = False

        def get_object(self, Bucket, Key, Range=None):
            response = super().get_object(Bucket=Bucket, Key=Key, Range=Range)
            if Range == "bytes=5-9" and not self.interrupted:
                self.interrupted = True
                response["Body"] = PartialBody(response["Body"].read())
            return response

    sleeps = []
    monkeypatch.setattr(prepared_state_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(prepared_state_module, "S3_VERIFICATION_RANGE_BYTES", 5)
    monkeypatch.setattr(prepared_state_module, "S3_DOWNLOAD_CONCURRENCY", 1)
    client = PartialRangeS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="EU-RO-1",
        client=client,
        endpoint_url="https://s3api-eu-ro-1.runpod.io/",
        prefix="cloud-offload",
    )
    payload = b"0123456789"
    artifact = portable_artifact(payload)
    client.objects[store._key(artifact["storage_key"])] = payload
    destination = tmp_path / "resumed-range"

    store.download_verified(artifact["storage_key"], artifact["digest"], destination)

    assert destination.read_bytes() == payload
    assert client.ranges == ["bytes=0-4", "bytes=5-9", "bytes=7-9"]
    assert sleeps == [2]


def test_s3_replica_expiry_keeps_shared_objects_until_last_manifest(tmp_path):
    client = MemoryS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="US-KS-2",
        client=client,
        endpoint_url="https://s3api-us-ks-2.runpod.io/",
    )
    signer = ManifestSigner(b"e" * 32)
    artifacts = {
        name: portable_artifact(name.encode())
        for name in ("shared", "first", "second")
    }
    for name, artifact in artifacts.items():
        source = tmp_path / name
        source.write_bytes(name.encode())
        store.upload_verified(source, artifact["digest"])
    first = signed_manifest(
        signer,
        [artifacts["shared"], artifacts["first"]],
        fingerprint({"profile": "first"}),
    )
    second = signed_manifest(
        signer,
        [artifacts["shared"], artifacts["second"]],
        fingerprint({"profile": "second"}),
    )
    first_key = store.publish_manifest(first, signer)
    store.publish_manifest(second, signer)

    removed = store.remove_manifest(first["manifest_id"], signer, manifest=first)

    assert removed == {"manifests": 1, "objects": 1}
    assert first_key not in client.objects
    assert not store.exists(artifacts["first"]["storage_key"])
    assert store.exists(artifacts["shared"]["storage_key"])
    assert store.exists(artifacts["second"]["storage_key"])
    assert [item["manifest_id"] for item in store.load_index()["manifests"]] == [
        second["manifest_id"]
    ]

    removed = store.remove_manifest(second["manifest_id"], signer, manifest=second)

    assert removed == {"manifests": 1, "objects": 2}
    assert store.load_index()["manifests"] == []
    assert not store.exists(artifacts["shared"]["storage_key"])
    assert not store.exists(artifacts["second"]["storage_key"])


def test_s3_probe_exercises_write_read_and_delete():
    client = MemoryS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="US-MD-1",
        client=client,
        endpoint_url="https://s3api-us-md-1.runpod.io/",
    )

    assert store.probe()
    assert client.objects == {}


def test_s3_prefix_matches_mounted_cache_namespace(tmp_path):
    client = MemoryS3()
    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="US-MD-1",
        client=client,
        endpoint_url="https://s3api-us-md-1.runpod.io/",
        prefix="cloud-offload",
    )
    signer = ManifestSigner(b"n" * 32)
    source = tmp_path / "bundle.tar"
    source.write_bytes(b"prepared runtime bundle")
    artifact = portable_artifact(source.read_bytes())

    logical_key = store.upload_verified(source, artifact["digest"])
    manifest = signed_manifest(signer, [artifact])
    logical_manifest_key = store.publish_manifest(manifest, signer)

    assert logical_key == artifact["storage_key"]
    assert logical_manifest_key.startswith("manifests/")
    assert all(key.startswith("cloud-offload/") for key in client.objects)
    index = store.load_index()
    assert index["manifests"][0]["storage_key"] == logical_manifest_key
    assert (
        store.read_json(logical_manifest_key)["manifest_id"]
        == manifest["manifest_id"]
    )


def test_runpod_invalid_argument_object_not_found_is_an_empty_index():
    class RunPodMemoryS3(MemoryS3):
        def get_object(self, Bucket, Key):
            raise RunPodMissingObject()

    store = RunPodS3PreparedStore(
        volume_id="vol",
        datacenter_id="US-MD-1",
        client=RunPodMemoryS3(),
        endpoint_url="https://s3api-us-md-1.runpod.io/",
    )

    assert store.load_index() == {
        "schema": "cloud-offload.prepared-state.index.v1",
        "generation": None,
        "manifests": [],
    }


class PlacementProvider(CloudConnector):
    def __init__(self, volume=None, fail_cached=False):
        self.volume = volume
        self.fail_cached = fail_cached
        self.created = []
        self.launches = []
        self.launch_environments = []

    @property
    def name(self):
        return "runpod"

    def list_available(
        self, gpu_type=None, min_gpu_ram=None, max_hourly_rate=None, placement=None
    ):
        return [
            {
                "id": "gpu",
                "provider": "runpod",
                "gpu_type": "GPU",
                "gpu_ram_gb": 24,
                "hourly_rate": 0.4,
                **(
                    {"datacenter_ids": list(placement.datacenter_ids)}
                    if placement
                    else {}
                ),
            }
        ]

    def launch(
        self,
        offer_id,
        docker_image,
        env_vars=None,
        startup_script=None,
        disk_gb=None,
        placement=None,
        resource_name=None,
    ):
        self.launches.append(placement)
        self.launch_environments.append(dict(env_vars or {}))
        if placement and placement.storage_attachments and self.fail_cached:
            raise PlacementError("cached datacenter capacity disappeared")
        return Instance(
            "worker-cache" if placement and placement.storage_attachments else "worker-cold",
            "runpod",
            "GPU",
            1,
            0.4,
            "running",
            metadata={"name": resource_name},
        )

    def get_storage(self, storage_id):
        for volume in getattr(self, "volumes", [self.volume]):
            if volume and volume.id == storage_id:
                return volume
        return None

    def create_storage(self, *, name, size_gb, datacenter_id):
        self.created.append((name, size_gb, datacenter_id))
        self.volume = ProviderStorage(
            "managed-volume", "runpod", name, size_gb, datacenter_id, True
        )
        return self.volume

    def delete_storage(self, storage_id):
        return True

    def get_instance(self, instance_id):
        return None

    def terminate(self, instance_id):
        return True

    def list_instances(self):
        return []


def dispatcher_config(tmp_path, prepared):
    return CloudConfig(
        enabled=True,
        min_queue_depth=1,
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="secret",
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        coordinator_url="https://coordinator.example",
        prepared_storage=prepared,
        max_hourly_rate=1,
        worker_profiles={
            "comfy": {
                "image": "example/runner@sha256:" + "a" * 64,
                "models": ["comfyui-workflow"],
                "providers": ["runpod"],
                "image_size_gb": 10,
            }
        },
    )


@pytest.mark.parametrize("size", [0, 4001])
def test_cache_volume_api_rejects_invalid_override_before_provider_call(
    monkeypatch, tmp_path, size
):
    config = dispatcher_config(tmp_path, policy(existing_volume_id=None))
    provider = PlacementProvider()
    registry = CacheRegistry(config.queue_db_path)
    monkeypatch.setattr(server, "_config", lambda *args, **kwargs: config)
    monkeypatch.setattr(server, "_cache_connector", lambda *args, **kwargs: provider)
    monkeypatch.setattr(server, "_cache_registry", lambda *args, **kwargs: registry)
    response = TestClient(server.app).post(
        "/api/cache/volumes",
        json={
            "operation": "create",
            "confirmed": True,
            "datacenter_id": "US-KS-2",
            "size_gb": size,
        },
    )
    assert response.status_code == 409
    assert provider.created == []


@pytest.mark.parametrize(
    ("mode", "expected_action", "expected_fallback"),
    [("smart", "launch", True), ("strict", "unavailable", False)],
)
def test_deleted_adopted_volume_falls_back_only_under_smart(
    tmp_path, mode, expected_action, expected_fallback
):
    config = dispatcher_config(tmp_path, policy(policy=mode))
    provider = PlacementProvider(volume=None)
    dispatcher = Dispatcher(config, provider=provider)
    dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-1",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=10,
        policy=config.prepared_storage,
    )
    decision = dispatcher._choose_cache_placement(
        connector=provider,
        provider_name="runpod",
        gpu_type="any",
        minimum_vram=0,
        cooling=set(),
        requirements={
            "profile_fingerprint": fingerprint({"p": 1}),
            "runtime_identity": {"image": ""},
            "required": {},
        },
    )
    assert decision.action == expected_action
    assert decision.fallback is expected_fallback
    assert "configured_cache_volume_not_found" in decision.reason


def test_explicit_region_creates_managed_storage_before_cached_offer(tmp_path):
    prepared = policy(existing_volume_id=None, region="US-KS-2")
    config = dispatcher_config(tmp_path, prepared)
    provider = PlacementProvider()
    dispatcher = Dispatcher(config, provider=provider)
    decision = dispatcher._choose_cache_placement(
        connector=provider,
        provider_name="runpod",
        gpu_type="any",
        minimum_vram=0,
        cooling=set(),
        requirements={
            "profile_fingerprint": fingerprint({"p": 1}),
            "runtime_identity": {"image": ""},
            "required": {},
        },
    )
    assert provider.created == [("cloud-offload-us-ks-2", 250, "US-KS-2")]
    assert decision.candidate.volume.provider_volume_id == "managed-volume"
    assert decision.placement().datacenter_ids == ("US-KS-2",)


@pytest.mark.parametrize("allowed_regions", [[], ["AP-JP-1"]])
def test_smart_launch_capacity_race_immediately_retries_cold(tmp_path, allowed_regions):
    region = allowed_regions[0] if allowed_regions else "US-KS-2"
    volume = ProviderStorage("vol-1", "runpod", "cache", 100, region, True)
    config = dispatcher_config(tmp_path, policy(region=region))
    config.allowed_regions = allowed_regions
    provider = PlacementProvider(volume=volume, fail_cached=True)
    dispatcher = Dispatcher(config, provider=provider)
    dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-1",
        datacenter_id=region,
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
    )
    job = dispatcher.queue.create(
        "comfyui-workflow",
        "inline://request",
        params={"runtime_profile": "comfy"},
        request={"workflow": {}},
        provider="runpod",
    )
    instance = dispatcher._launch_worker("runpod", "comfy", [job])
    assert instance and instance.id == "worker-cold"
    assert provider.launches[0] is not None
    if allowed_regions:
        assert provider.launches[0].datacenter_ids == ("AP-JP-1",)
        assert provider.launches[1].datacenter_ids == ("AP-JP-1",)
        assert not provider.launches[1].storage_attachments
    else:
        assert provider.launches[1] is None
    assert "cache_manifest_id" not in dispatcher.queue.get(job.id).params
    event_types = [
        item["event"]["type"] for item in dispatcher.queue.list_events(job.id)
    ]
    assert "cache_cold_fallback" in event_types
    provider_events = [
        event_type
        for event_type in event_types
        if event_type.startswith("provider_request_")
    ]
    assert provider_events == [
        "provider_request_started",
        "provider_request_failed",
        "provider_request_started",
        "provider_request_completed",
    ]


def test_confirmed_prepared_launch_does_not_silently_fall_back_cold(tmp_path):
    volume = ProviderStorage("vol-1", "runpod", "cache", 100, "US-KS-2", True)
    config = dispatcher_config(tmp_path, policy())
    provider = PlacementProvider(volume=volume, fail_cached=True)
    dispatcher = Dispatcher(config, provider=provider)
    registered = dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-1",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
    )
    job = dispatcher.queue.create(
        "comfyui-workflow",
        "inline://request",
        params={
            "runtime_profile": "comfy",
            "preflight": {
                "candidate_id": "sha256:" + "c" * 64,
                "provider": "runpod",
                "offer_id": "gpu",
                "gpu_type": "GPU",
                "gpu_ram_gb": 24,
                "hourly_rate": 0.4,
                "region": "US-KS-2",
                "prepared_volume_id": registered.id,
                "expires_at": "2099-01-01T00:00:00Z",
                "request_policy": {"max_hourly_rate": 1.0},
            },
        },
        request={"workflow": {}},
        provider="runpod",
    )

    instance = dispatcher._launch_worker("runpod", "comfy", [job])

    assert instance is None
    assert len(provider.launches) == 1
    assert provider.launches[0] is not None
    assert all(placement is not None for placement in provider.launches)


def test_confirmed_prepared_launch_rejects_provider_volume_replacement_before_mutation(
    tmp_path,
):
    """A reused local registry id cannot redirect a confirmed launch."""
    original = ProviderStorage("provider-a", "runpod", "cache-a", 100, "US-KS-2", True)
    replacement = ProviderStorage("provider-b", "runpod", "cache-b", 100, "US-KS-2", True)
    config = dispatcher_config(tmp_path, policy())
    provider = PlacementProvider(volume=original)
    provider.volumes = [original, replacement]
    dispatcher = Dispatcher(config, provider=provider)
    registered = dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="provider-a",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
        volume_id="volume-1",
    )
    confirmed = {
        "candidate_id": "sha256:" + "c" * 64,
        "provider": "runpod",
        "offer_id": "gpu",
        "gpu_type": "GPU",
        "gpu_ram_gb": 24,
        "hourly_rate": 0.4,
        "region": "US-KS-2",
        "prepared_volume_id": registered.id,
        "prepared_provider_volume_id": "provider-a",
        "expires_at": "2099-01-01T00:00:00Z",
        "request_policy": {"max_hourly_rate": 1.0},
    }
    job = dispatcher.queue.create(
        "comfyui-workflow",
        "inline://request",
        params={"runtime_profile": "comfy", "preflight": confirmed},
        request={"workflow": {}},
        provider="runpod",
        status=JobStatus.QUEUED,
    )

    # The local row is replaced while retaining its stable registry id.
    dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="provider-b",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
        volume_id=registered.id,
    )

    instance = dispatcher._launch_worker("runpod", "comfy", [job])

    assert instance is None
    assert provider.launches == []
    assert dispatcher.queue.get_lease(job.id) is None
    assert dispatcher.queue.list_events(job.id) == [
        event for event in dispatcher.queue.list_events(job.id)
        if event["event"]["type"] == "job_created"
    ]

    # A deleted physical object is the same identity failure, even when the
    # registry row still points at the old provider id.
    dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="provider-a",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
        volume_id=registered.id,
    )
    provider.volumes = []
    deleted_instance = dispatcher._launch_worker("runpod", "comfy", [job])

    assert deleted_instance is None
    assert provider.launches == []
    assert dispatcher.queue.get_lease(job.id) is None
    assert dispatcher.queue.list_events(job.id) == [
        event for event in dispatcher.queue.list_events(job.id)
        if event["event"]["type"] == "job_created"
    ]


def test_rebound_strict_binding_fails_provisioning_instead_of_stale_launch(tmp_path):
    """Strict placement means exactly the configured volume, every launch.

    A quote confirmed against one volume must not launch after the strict
    binding is pointed elsewhere: the launch fails as provisioning (with
    retry) rather than renting a GPU against the stale confirmation.
    """
    volume = ProviderStorage("vol-1", "runpod", "cache", 100, "US-KS-2", True)
    config = dispatcher_config(tmp_path, policy(policy="strict"))
    provider = PlacementProvider(volume=volume)
    dispatcher = Dispatcher(config, provider=provider)
    registered = dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-1",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
    )
    job = dispatcher.queue.create(
        "comfyui-workflow",
        "inline://request",
        params={
            "runtime_profile": "comfy",
            "preflight": {
                "candidate_id": "sha256:" + "c" * 64,
                "provider": "runpod",
                "offer_id": "gpu",
                "gpu_type": "GPU",
                "gpu_ram_gb": 24,
                "hourly_rate": 0.4,
                "region": "US-KS-2",
                "prepared_volume_id": registered.id,
                "expires_at": "2099-01-01T00:00:00Z",
                "request_policy": {"max_hourly_rate": 1.0},
            },
        },
        request={"workflow": {}},
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    dispatcher.config.prepared_storage["existing_volume_id"] = "vol-missing"

    instance = dispatcher._launch_worker("runpod", "comfy", [job])

    assert instance is None
    assert provider.launches == []
    assert dispatcher.queue.get(job.id).status == JobStatus.QUEUED
    events = [item["type"] for item in dispatcher.queue.list_events(job.id)]
    assert "provisioning_failed" in events
    assert "provisioning_started" not in events


def test_strict_binding_in_another_region_does_not_block_regional_volume(tmp_path):
    """A strict binding naming another region's volume is not a stale binding.

    Multi-region prepared storage keeps one binding slot in the config while a
    registered volume serves each datacenter. A confirmation against the
    region's own volume must survive the binding naming a sibling region.
    """
    volume = ProviderStorage("vol-1", "runpod", "cache", 100, "US-KS-2", True)
    config = dispatcher_config(tmp_path, policy(policy="strict"))
    provider = PlacementProvider(volume=volume)
    provider.volumes = [
        volume,
        ProviderStorage("vol-2", "runpod", "sibling", 100, "EU-RO-1", True),
    ]
    dispatcher = Dispatcher(config, provider=provider)
    registered = dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-1",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
    )
    dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-2",
        datacenter_id="EU-RO-1",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
    )
    dispatcher.config.prepared_storage["existing_volume_id"] = "vol-2"
    confirmed = {
        "candidate_id": "sha256:" + "c" * 64,
        "provider": "runpod",
        "offer_id": "gpu",
        "gpu_type": "GPU",
        "gpu_ram_gb": 24,
        "hourly_rate": 0.4,
        "region": "US-KS-2",
        "prepared_volume_id": registered.id,
        "expires_at": "2099-01-01T00:00:00Z",
        "request_policy": {"max_hourly_rate": 1.0},
    }

    decision = dispatcher._confirmed_cache_placement(
        connector=dispatcher.connectors["runpod"],
        provider_name="runpod",
        gpu_type="GPU",
        minimum_vram=24,
        cooling=set(),
        requirements={
            "required": {},
            "logical_required": [],
            "profile_fingerprint": "fp",
        },
        confirmed=confirmed,
    )

    assert decision.reason != "confirmed_prepared_binding_changed"


def test_strict_cross_region_binding_rejects_provider_missing_bound_volume(
    tmp_path,
):
    volume = ProviderStorage("vol-1", "runpod", "cache", 100, "US-KS-2", True)
    config = dispatcher_config(tmp_path, policy(policy="strict"))
    provider = PlacementProvider(volume=volume)
    dispatcher = Dispatcher(config, provider=provider)
    registered = dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-1",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
    )
    dispatcher.cache_registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-2",
        datacenter_id="EU-RO-1",
        ownership="adopted",
        capacity_bytes=100,
        policy=config.prepared_storage,
    )
    dispatcher.config.prepared_storage["existing_volume_id"] = "vol-2"
    confirmed = {
        "candidate_id": "sha256:" + "c" * 64,
        "provider": "runpod",
        "offer_id": "gpu",
        "gpu_type": "GPU",
        "gpu_ram_gb": 24,
        "hourly_rate": 0.4,
        "region": "US-KS-2",
        "prepared_volume_id": registered.id,
        "request_policy": {"max_hourly_rate": 1.0},
    }

    decision = dispatcher._confirmed_cache_placement(
        connector=dispatcher.connectors["runpod"],
        provider_name="runpod",
        gpu_type="GPU",
        minimum_vram=24,
        cooling=set(),
        requirements={
            "required": {},
            "logical_required": [],
            "profile_fingerprint": "fp",
        },
        confirmed=confirmed,
    )

    assert decision.reason == "confirmed_prepared_binding_changed"


def test_worker_manifest_announcement_drives_next_exact_cache_placement(
    monkeypatch, tmp_path
):
    content = b"announced-model"
    asset = {
        "category": "checkpoints",
        "filename": "announced.safetensors",
        "sha256": portable_artifact(content)["digest"].removeprefix("sha256:"),
        "size": len(content),
    }
    config = dispatcher_config(tmp_path, policy())
    provider_volume = ProviderStorage("vol-1", "runpod", "cache", 100, "US-KS-2", True)
    provider = PlacementProvider(volume=provider_volume)
    queue = JobQueue(config.queue_db_path)
    registry = CacheRegistry(config.queue_db_path)
    cache_volume = registry.upsert_volume(
        provider="runpod",
        provider_volume_id="vol-1",
        datacenter_id="US-KS-2",
        ownership="adopted",
        capacity_bytes=100 * 1024**3,
        policy=config.prepared_storage,
    )
    profile = config.worker_profiles["comfy"]
    requirements = resolve_prepared_requirements(
        "comfy", profile, [SimpleNamespace(request={"assets": [asset]})]
    )
    queue.create(
        "comfyui-workflow",
        "inline://request",
        params={
            "runtime_profile": "comfy",
            "prepared_requirement": requirements,
            "cache_volume_id": cache_volume.id,
        },
        request={"assets": [asset], "workflow": {}},
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    queue.set_worker_token("worker-secret")
    claimed = queue.claim_jobs(
        "worker-one",
        token="worker-secret",
        limit=1,
        provider="runpod",
        models=["comfyui-workflow"],
        runtime_profile="comfy",
    )[0]
    artifact = portable_artifact(content)
    signer = server._prepared_manifest_signer(config)
    manifest = build_manifest(
        profile_fingerprint=requirements["profile_fingerprint"],
        producer={
            "image_digest": "sha256:" + "a" * 64,
            "cloud_offload_version": "test",
        },
        artifacts=[artifact],
        signer=signer,
        claims={"cache_volume_id": cache_volume.id},
    )
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda *args, **kwargs: config)
    monkeypatch.setattr(server, "_cache_registry", lambda *args, **kwargs: registry)

    response = TestClient(server.app).post(
        "/api/workers/cache/manifests/announce",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "job_id": claimed.id,
            "worker_id": "worker-one",
            "volume_id": cache_volume.id,
            "generation": "worker-generation",
            "manifest": manifest,
        },
    )
    assert response.status_code == 200, response.text

    claimed.params["cache_manifest_id"] = manifest["manifest_id"]
    queue.update(claimed)
    fetched = TestClient(server.app).post(
        "/api/workers/cache/manifests/fetch",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "job_id": claimed.id,
            "worker_id": "worker-one",
            "volume_id": cache_volume.id,
            "manifest_id": manifest["manifest_id"],
        },
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["manifest_id"] == manifest["manifest_id"]
    refused = TestClient(server.app).post(
        "/api/workers/cache/manifests/fetch",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "job_id": claimed.id,
            "worker_id": "worker-one",
            "volume_id": cache_volume.id,
            "manifest_id": "sha256:" + "f" * 64,
        },
    )
    assert refused.status_code == 403

    queue.complete_job(claimed.id, {"outputs": {}})
    other_requirements = {
        **requirements,
        "profile_fingerprint": fingerprint({"profile": "different-next-job"}),
    }
    next_job = queue.create(
        "comfyui-workflow",
        "inline://next",
        params={
            "runtime_profile": "comfy",
            "prepared_requirement": other_requirements,
            "cache_volume_id": cache_volume.id,
        },
        request={"workflow": {}},
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    queue.claim_jobs(
        "worker-one",
        token="worker-secret",
        limit=1,
        provider="runpod",
        models=["comfyui-workflow"],
        runtime_profile="comfy",
    )
    healed = TestClient(server.app).post(
        "/api/workers/cache/manifests/announce",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "job_id": next_job.id,
            "worker_id": "worker-one",
            "volume_id": cache_volume.id,
            "generation": "healed-on-different-profile",
            "manifest": manifest,
        },
    )
    assert healed.status_code == 200, healed.text

    dispatcher = Dispatcher(config, provider=provider)
    decision = dispatcher._choose_cache_placement(
        connector=provider,
        provider_name="runpod",
        gpu_type="any",
        minimum_vram=0,
        cooling=set(),
        requirements=requirements,
    )
    assert decision.candidate.complete
    assert decision.candidate.manifest_ids == (manifest["manifest_id"],)

    launch_job = dispatcher.queue.create(
        "comfyui-workflow",
        "inline://launch",
        params={"runtime_profile": "comfy"},
        request={"assets": [asset], "workflow": {}},
        provider="runpod",
    )
    launched = dispatcher._launch_worker("runpod", "comfy", [launch_job])

    assert launched and launched.id == "worker-cache"
    assert (
        dispatcher.queue.get(launch_job.id).params["cache_manifest_id"]
        == manifest["manifest_id"]
    )
    assert (
        provider.launch_environments[-1]["CLOUD_OFFLOAD_CACHE_MANIFEST"]
        == manifest["manifest_id"]
    )


def test_worker_trust_receipt_is_bound_to_active_worker_manifest_and_volume(
    monkeypatch, tmp_path
):
    content = b"signed-sample"
    config = dispatcher_config(tmp_path, policy())
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("worker-secret")
    queue.create(
        "comfyui-workflow",
        "inline://request",
        params={
            "runtime_profile": "comfy",
            "cache_volume_id": "cache-vol",
            "cache_provider_volume_id": "provider-cache-vol",
        },
        request={"workflow": {}},
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    claimed = queue.claim_jobs(
        "worker-one",
        token="worker-secret",
        limit=1,
        provider="runpod",
        models=["comfyui-workflow"],
        runtime_profile="comfy",
    )[0]
    artifact = portable_artifact(content)
    signer = server._prepared_manifest_signer(config)
    manifest = signed_manifest(
        signer,
        [artifact],
        claims={
            "cache_volume_id": "cache-vol",
            "cache_provider_volume_id": "provider-cache-vol",
        },
    )
    proposal = {
        "schema": TRUST_RECEIPT_SCHEMA,
        "manifest_id": manifest["manifest_id"],
        "manifest_signature_digest": manifest_signature_digest(manifest),
        "artifact_digest": artifact["digest"],
        "artifact_size": artifact["size"],
        "storage_key": artifact["storage_key"],
        "volume_id": "cache-vol",
        "provider_volume_id": "provider-cache-vol",
        "runtime_compatibility": artifact_runtime_compatibility_key(artifact),
        "object_generation": {
            "storage_key": artifact["storage_key"],
            "size": artifact["size"],
            "modified_ns": 123,
        },
        "verified_at": "2000-01-01T00:00:00Z",
        "expires_at": "2100-01-01T00:00:00Z",
        "scrub": {
            "full_audit_due_at": "2100-01-01T00:00:00Z",
            "samples": [
                {
                    "offset": 0,
                    "size": len(content),
                    "sha256": portable_artifact(content)["digest"],
                }
            ],
        },
    }
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda *args, **kwargs: config)
    client = TestClient(server.app)
    body = {
        "job_id": claimed.id,
        "worker_id": "worker-one",
        "volume_id": "cache-vol",
        "receipt": proposal,
        "manifest": manifest,
    }

    signed = client.post(
        "/api/workers/cache/trust-receipts/sign",
        headers={"Authorization": "Bearer worker-secret"},
        json=body,
    )
    assert signed.status_code == 200, signed.text
    receipt = signed.json()
    assert receipt["verified_at"] != proposal["verified_at"]
    assert receipt["volume_id"] == "cache-vol"
    assert receipt["provider_volume_id"] == "provider-cache-vol"

    verified = client.post(
        "/api/workers/cache/trust-receipts/verify",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "job_id": claimed.id,
            "worker_id": "worker-one",
            "volume_id": "cache-vol",
            "receipt": receipt,
        },
    )
    assert verified.status_code == 200, verified.text
    assert verified.json()["receipt_id"] == receipt["receipt_id"]

    wrong_volume = client.post(
        "/api/workers/cache/trust-receipts/verify",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "job_id": claimed.id,
            "worker_id": "worker-one",
            "volume_id": "another-volume",
            "receipt": receipt,
        },
    )
    assert wrong_volume.status_code == 403
    tampered = json.loads(json.dumps(receipt))
    tampered["artifact_size"] += 1
    refused = client.post(
        "/api/workers/cache/trust-receipts/verify",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "job_id": claimed.id,
            "worker_id": "worker-one",
            "volume_id": "cache-vol",
            "receipt": tampered,
        },
    )
    assert refused.status_code == 400


def test_result_available_phase_precedes_job_completion(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "inline://request", request={"workflow": {}})
    worker = Worker.__new__(Worker)
    worker.queue = queue
    worker._stage_custom_nodes = lambda active: None
    worker._stage_profile_weights = lambda active: None
    worker._begin_cache_restore = lambda active: None
    worker._complete_cache_restore = lambda active: None
    worker._run_comfyui_workflow = lambda active: {"outputs": {}}

    worker._process_job(job)

    phases = [
        item["event"].get("phase")
        for item in queue.list_events(job.id)
        if item["event"].get("type") == "phase_timing"
    ]
    assert phases[-1] == "result_available"
    assert queue.get(job.id).status == JobStatus.COMPLETED


def cache_worker(cas, profile_fingerprint, worker_id):
    worker = Worker.__new__(Worker)
    worker.prepared_cache = cas
    worker.cache_policy = {
        "tenant": "default",
        "cache_private_assets": False,
        "cold_fallback": "allow",
    }
    worker.cache_requirements = {"profile_fingerprint": profile_fingerprint}
    worker.cache_manifest_instruction = profile_fingerprint
    worker._latest_prepared_manifest = None
    worker.cache_runtime = {
        "image_digest": "sha256:" + "a" * 64,
        "python_abi": "cp311",
        "platform": "linux-x86_64",
        "dependency_lock": fingerprint({}),
        "torch": "",
        "cuda": "",
    }
    worker.worker_id = worker_id
    worker.cache_receipt = None
    worker._active_cache_job = None
    return worker


def test_exact_placement_manifest_precedes_same_profile_worker_publication(tmp_path):
    signer = ManifestSigner(b"e" * 32)
    cas = PreparedStateCAS(tmp_path / "volume", signer)
    profile = fingerprint({"profile": "exact"})
    selected = signed_manifest(signer, [portable_artifact(b"selected")], profile)
    latest = signed_manifest(signer, [portable_artifact(b"later")], profile)
    direct = cas.root / manifest_by_id_key(selected["manifest_id"])
    direct.parent.mkdir(parents=True, exist_ok=True)
    direct.write_bytes(canonical_json(selected))
    worker = cache_worker(cas, profile, "worker-exact")
    worker.cache_manifest_instruction = selected["manifest_id"]
    worker._latest_prepared_manifest = latest

    assert (
        worker._selected_prepared_manifest()["manifest_id"] == selected["manifest_id"]
    )


def test_two_jobs_accumulate_manifest_and_second_fresh_worker_never_fetches_origin(
    tmp_path,
):
    signer = ManifestSigner(b"w" * 32)
    cas = PreparedStateCAS(tmp_path / "volume", signer)
    profile = fingerprint({"aggregate": ["a", "b"]})
    first = cache_worker(cas, profile, "worker-first")
    fetches = []

    def fetch(asset, target, token):
        fetches.append(asset["filename"])
        target.write_bytes(asset["payload"])

    first._fetch_declared_asset = fetch
    assets = [
        {
            "category": "checkpoints",
            "filename": f"{name}.safetensors",
            "sha256": portable_artifact(payload)["digest"].removeprefix("sha256:"),
            "payload": payload,
        }
        for name, payload in (("a", b"model-a"), ("b", b"model-b"))
    ]
    first_root = tmp_path / "first-models"
    for asset in assets:
        first._stage_declared_asset(asset, first_root, None)
    latest = first._selected_prepared_manifest()
    assert {item["digest"] for item in latest["artifacts"]} == {
        "sha256:" + item["sha256"] for item in assets
    }

    second = cache_worker(cas, profile, "worker-second")
    second._fetch_declared_asset = lambda *args: pytest.fail("origin/HF was called")
    second_root = tmp_path / "second-models"
    for asset in assets:
        second._stage_declared_asset(asset, second_root, None)
    assert fetches == ["a.safetensors", "b.safetensors"]
    assert [
        (second_root / "checkpoints" / item["filename"]).read_bytes() for item in assets
    ] == [b"model-a", b"model-b"]


def test_same_pod_retry_finishes_cache_population_for_an_already_downloaded_asset(
    tmp_path,
):
    cas = PreparedStateCAS(tmp_path / "volume", ManifestSigner(b"r" * 32))
    worker = cache_worker(cas, fingerprint({"retry": True}), "retry-worker")
    payload = b"download-succeeded-publication-failed"
    artifact = portable_artifact(payload)
    asset = {
        "category": "checkpoints",
        "filename": "retry.safetensors",
        "sha256": artifact["digest"].removeprefix("sha256:"),
    }
    target = tmp_path / "models" / "checkpoints" / asset["filename"]
    target.parent.mkdir(parents=True)
    target.write_bytes(payload)

    worker._fetch_declared_asset = lambda *args: pytest.fail("origin was called")
    worker._stage_declared_asset(asset, tmp_path / "models", None)

    manifest = worker._selected_prepared_manifest()
    assert manifest
    assert [item["digest"] for item in manifest["artifacts"]] == [artifact["digest"]]


def test_a_cache_symlinked_asset_is_reverified_through_the_cache_not_rehashed(
    tmp_path, monkeypatch
):
    cas = PreparedStateCAS(tmp_path / "volume", ManifestSigner(b"y" * 32))
    profile = fingerprint({"symlinked": True})
    payload = b"already-restored-through-the-cache"
    artifact = portable_artifact(payload)
    asset = {
        "category": "checkpoints",
        "filename": "linked.safetensors",
        "sha256": artifact["digest"].removeprefix("sha256:"),
    }
    writer = cache_worker(cas, profile, "symlink-writer")
    writer._fetch_declared_asset = lambda a, target, token: target.write_bytes(payload)
    writer._stage_declared_asset(asset, tmp_path / "writer-models", None)

    reader = cache_worker(cas, profile, "symlink-reader")
    reader._fetch_declared_asset = lambda *args: pytest.fail("origin was called")
    reader._stage_declared_asset(asset, tmp_path / "models", None)
    target = tmp_path / "models" / "checkpoints" / asset["filename"]
    assert target.is_symlink()

    import cloud_offload.worker as worker_module

    monkeypatch.setattr(
        worker_module,
        "sha256_file",
        lambda path: pytest.fail("staging re-hashed the materialized file"),
    )
    reader._stage_declared_asset(asset, tmp_path / "models", None)

    assert target.read_bytes() == payload


def test_hf_snapshot_profile_is_restored_on_a_fresh_worker_without_origin(
    monkeypatch, tmp_path
):
    monkeypatch.delenv("CLOUD_OFFLOAD_CACHE_VOLUME_ID", raising=False)
    signer = ManifestSigner(b"s" * 32)
    cas = PreparedStateCAS(tmp_path / "volume", signer)
    profile = fingerprint({"snapshot": "org/model@revision"})
    entry = {
        "repo_id": "org/model",
        "revision": "immutable-revision",
        "dest": "snapshot-model",
        "gated": False,
    }
    first = cache_worker(cas, profile, "snapshot-writer")
    downloaded = tmp_path / "downloaded"
    (downloaded / "tokenizer").mkdir(parents=True)
    (downloaded / "model.safetensors").write_bytes(b"same-content")
    (downloaded / "tokenizer" / "vocab.json").write_bytes(b"same-content")
    (downloaded / ".cache" / "huggingface").mkdir(parents=True)
    (downloaded / ".cache" / "huggingface" / "lock").write_text("transient")

    first._populate_profile_snapshot(entry, downloaded, None)

    manifest = first._selected_prepared_manifest()
    snapshot_artifacts = [
        item
        for item in manifest["artifacts"]
        if (item.get("source") or {}).get("snapshot") is True
    ]
    assert {item["source"]["filename"] for item in snapshot_artifacts} == {
        "model.safetensors",
        "tokenizer/vocab.json",
    }

    second = cache_worker(cas, profile, "snapshot-reader")
    restored = tmp_path / "restored"
    assert second._restore_profile_snapshot(entry, restored, None)
    assert (restored / "model.safetensors").read_bytes() == b"same-content"
    assert (restored / "tokenizer" / "vocab.json").read_bytes() == b"same-content"
    assert not (restored / ".cache").exists()


def test_custom_node_bundle_restores_exact_destination_without_double_nesting(tmp_path):
    signer = ManifestSigner(b"n" * 32)
    cas = PreparedStateCAS(tmp_path / "volume", signer)
    profile = fingerprint({"nodes": "pack"})
    source = tmp_path / "custom_nodes" / "example-pack"
    source.mkdir(parents=True)
    (source / "__init__.py").write_text("NODE_CLASS_MAPPINGS = {}", encoding="utf-8")
    archive = tmp_path / "node-bundle.tar"
    with tarfile.open(archive, "w") as bundle:
        for child in source.iterdir():
            bundle.add(child, arcname=child.name)
    import hashlib

    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    cas.publish_blob(archive, digest, bundle=True)
    runtime = cache_worker(cas, profile, "runtime-template").cache_runtime
    artifact = {
        "digest": "sha256:" + digest,
        "kind": "custom-node-bundle",
        "size": archive.stat().st_size,
        "storage_key": bundle_key(digest),
        "portability": "runtime-bound",
        "requirements": {
            key: runtime[key]
            for key in ("image_digest", "platform", "python_abi", "dependency_lock")
        },
        "policy": {"tenant": "default", "cacheable": True},
        "destination": {"pack_id": "example-pack"},
    }
    cas.publish_manifest(signed_manifest(signer, [artifact], profile))

    second = cache_worker(cas, profile, "node-reader")
    destination = tmp_path / "fresh" / "custom_nodes" / "example-pack"
    assert second._restore_custom_node_bundle("example-pack", destination, None)
    assert (destination / "__init__.py").is_file()
    assert not (destination / "example-pack").exists()


def test_corrupt_profile_weight_is_quarantined_and_falls_back(tmp_path):
    signer = ManifestSigner(b"q" * 32)
    cas = PreparedStateCAS(tmp_path / "volume", signer)
    profile = fingerprint({"weight": "model"})
    entry = {
        "repo_id": "org/model",
        "revision": "immutable",
        "dest": "checkpoints",
        "gated": False,
    }
    first = cache_worker(cas, profile, "weight-writer")
    first.queue = SimpleNamespace(append_event=lambda *args, **kwargs: None)
    source = tmp_path / "model.safetensors"
    source.write_bytes(b"verified-weight")
    first._populate_profile_weight(
        entry, source.name, source, SimpleNamespace(id="job-weight")
    )
    manifest = first._selected_prepared_manifest()
    artifact = manifest["artifacts"][0]
    cas._resolve(artifact["storage_key"]).write_bytes(b"corrupt")

    second = cache_worker(cas, profile, "weight-reader")
    second.queue = SimpleNamespace(append_event=lambda *args, **kwargs: None)
    restored = second._restore_profile_weight(
        entry,
        source.name,
        tmp_path / "fresh-model.safetensors",
        SimpleNamespace(id="job-weight-2"),
    )
    assert restored is False
    assert not cas._resolve(artifact["storage_key"]).exists()
    assert list((cas.root / "quarantine").rglob("*.reason"))


def test_portable_digest_is_shared_across_profiles_without_origin_or_extra_artifacts(
    tmp_path,
):
    signer = ManifestSigner(b"x" * 32)
    cas = PreparedStateCAS(tmp_path / "volume", signer)
    profile_a = fingerprint({"profile": "A"})
    profile_b = fingerprint({"profile": "B"})
    first = cache_worker(cas, profile_a, "profile-a")
    first._fetch_declared_asset = lambda asset, target, token: target.write_bytes(
        asset["payload"]
    )
    shared = {
        "category": "checkpoints",
        "filename": "shared.safetensors",
        "payload": b"shared-model",
    }
    extra = {
        "category": "loras",
        "filename": "only-a.safetensors",
        "payload": b"profile-a-extra",
    }
    for asset in (shared, extra):
        asset["sha256"] = portable_artifact(asset["payload"])["digest"].removeprefix(
            "sha256:"
        )
        first._stage_declared_asset(asset, tmp_path / "profile-a-models", None)

    second = cache_worker(cas, profile_b, "profile-b")
    second._fetch_declared_asset = lambda *args: pytest.fail("origin was called")
    second._stage_declared_asset(shared, tmp_path / "profile-b-models", None)

    selected = second._selected_prepared_manifest()
    assert selected["profile_fingerprint"] == profile_b
    assert [item["digest"] for item in selected["artifacts"]] == [
        "sha256:" + shared["sha256"]
    ]
    assert (
        tmp_path / "profile-b-models" / "checkpoints" / "shared.safetensors"
    ).read_bytes() == b"shared-model"


def test_cross_profile_bytes_affect_coverage_without_selecting_foreign_manifest(
    tmp_path,
):
    registry = CacheRegistry(tmp_path / "queue.db")
    volume = registered_volume(registry, "shared-volume", "A")
    shared = portable_artifact(b"shared")
    extra = portable_artifact(b"extra")
    manifest_a = signed_manifest(
        ManifestSigner(b"c" * 32),
        [shared, extra],
        fingerprint({"profile": "A"}),
    )
    registry.reconcile_index(
        volume.id,
        {
            "schema": "cloud-offload.prepared-state.index.v1",
            "generation": "profile-a",
            "manifests": [manifest_a],
        },
        manifest_documents={manifest_a["manifest_id"]: manifest_a},
    )
    coverage = registry.volume_coverage(
        {shared["digest"]: shared["size"]},
        runtime={},
        tenant="default",
        profile_fingerprint=fingerprint({"profile": "B"}),
    )[0]
    assert coverage["cached_bytes"] == shared["size"]
    assert coverage["complete"] is False
    assert coverage["manifest_ids"] == []
    assert coverage["coverage_manifest_ids"] == [manifest_a["manifest_id"]]
