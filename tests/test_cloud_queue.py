import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from cloud_offload.config import CloudConfig
from cloud_offload.coordinator import CoordinatorQueue
from cloud_offload.dispatcher import Dispatcher
from cloud_offload.providers.base import CloudProvider
from cloud_offload.queue import JobQueue, JobStatus, utc_now
from cloud_offload.router import select_provider
from cloud_offload.worker import Worker


class DummyProvider(CloudProvider):
    @property
    def name(self) -> str:
        return "dummy"

    def list_available(self, *args, **kwargs):
        return []

    def launch(self, *args, **kwargs):
        raise NotImplementedError

    def terminate(self, instance_id: str) -> bool:
        return True

    def get_instance(self, instance_id: str):
        return None

    def list_instances(self):
        return []


def test_config_load_merges_preferences_with_environment(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "cloud": {
                    "enabled": True,
                    "provider_order": ["runpod", "vast"],
                    "routing_policy": "cheapest",
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("RUNPOD_API_KEY", "secret")

    config = CloudConfig.load(config_path)

    assert config.enabled is True
    assert config.provider_order == ["runpod", "vast.ai"]
    assert config.routing_policy == "cheapest"
    assert config.runpod_api_key == "secret"


def test_keep_warm_config_round_trips_and_supports_environment(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "cloud": {
                    "keep_warm": True,
                    "keep_warm_warning_seconds": 3600,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CLOUD_OFFLOAD_KEEP_WARM_WARNING", "7200")

    config = CloudConfig.load(config_path, resolve_secrets=False)

    assert config.keep_warm is True
    assert config.keep_warm_warning_seconds == 7200
    assert config.to_dict()["keep_warm"] is True


def test_scratch_directory_round_trips_and_supports_environment(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    from_file = tmp_path / "from-file"
    from_environment = tmp_path / "from-environment"
    config_path.write_text(
        json.dumps({"cloud": {"scratch_dir": f"  {from_file}  "}}),
        encoding="utf-8",
    )

    file_config = CloudConfig.from_file(config_path)

    assert file_config.scratch_dir == str(from_file)
    assert file_config.to_dict()["scratch_dir"] == str(from_file)

    monkeypatch.setenv(
        "CLOUD_OFFLOAD_SCRATCH_DIR",
        f"  {from_environment}  ",
    )
    environment_config = CloudConfig.load(config_path, resolve_secrets=False)

    assert environment_config.scratch_dir == str(from_environment)
    assert environment_config.to_dict()["scratch_dir"] == str(from_environment)


def test_scratch_directory_rejects_non_string_value():
    with pytest.raises(ValueError, match="scratch_dir must be a string"):
        CloudConfig(scratch_dir=[])


def test_config_resolves_provider_credentials_from_the_keychain(monkeypatch, tmp_path):
    """Credentials come from the OS keychain, never from config.json."""
    from cloud_offload import credentials as creds

    config_path = tmp_path / "config.json"
    config_path.write_text('{"cloud":{"enabled":true}}', encoding="utf-8")
    monkeypatch.delenv("VAST_API_KEY", raising=False)
    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    monkeypatch.delenv("CLOUD_OFFLOAD_RUNPOD_API_KEY", raising=False)

    vault = {}
    monkeypatch.setattr(
        creds,
        "_keyring",
        lambda: SimpleNamespace(
            get_password=lambda service, user: vault.get(user),
            set_password=lambda service, user, secret: vault.__setitem__(user, secret),
            delete_password=lambda service, user: vault.pop(user, None),
        ),
    )
    monkeypatch.setattr(
        creds, "legacy_credentials_file", lambda: tmp_path / "none.json"
    )
    vault["runpod"] = "runpod-secret"

    config = CloudConfig.load(config_path)

    assert config.api_key_for("runpod") == "runpod-secret"
    assert config.api_key_for("vast.ai") == ""
    # A credential must never reach the serialized config.
    assert "runpod-secret" not in repr(config.to_dict())


def test_env_var_overrides_the_keychain_for_headless_workers(monkeypatch, tmp_path):
    """A rented worker has no keychain, so the env var has to win."""
    from cloud_offload import credentials as creds

    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    monkeypatch.setenv("CLOUD_OFFLOAD_RUNPOD_API_KEY", "from-env")
    monkeypatch.setattr(
        creds,
        "_keyring",
        lambda: SimpleNamespace(
            get_password=lambda service, user: "from-keychain",
            set_password=lambda *a: None,
            delete_password=lambda *a: None,
        ),
    )
    monkeypatch.setattr(
        creds, "legacy_credentials_file", lambda: tmp_path / "none.json"
    )

    assert CloudConfig().api_key_for("runpod") == "from-env"


def test_cheapest_router_compares_configured_connectors(monkeypatch):
    class Connector:
        def __init__(self, rate):
            self.rate = rate

        def find_cheapest(self, **kwargs):
            return {"id": str(self.rate), "hourly_rate": self.rate}

    connectors = {"vast.ai": Connector(0.45), "runpod": Connector(0.32)}
    monkeypatch.setattr(
        "cloud_offload.router.create_connector", lambda name, config: connectors[name]
    )
    config = CloudConfig(
        provider_order=["vast.ai", "runpod"],
        routing_policy="cheapest",
        vast_api_key="vast",
        runpod_api_key="runpod",
    )

    route = select_provider(config)

    assert route.provider == "runpod"
    assert route.offer["hourly_rate"] == 0.32


def test_router_only_considers_providers_with_compatible_worker_profile(monkeypatch):
    class Connector:
        def find_cheapest(self, **kwargs):
            return {"id": "offer", "hourly_rate": 0.25}

    monkeypatch.setattr(
        "cloud_offload.router.create_connector", lambda name, config: Connector()
    )
    config = CloudConfig(
        provider_order=["vast.ai", "runpod"],
        routing_policy="preferred",
        vast_api_key="vast",
        runpod_api_key="runpod",
        worker_profiles={
            "comfyui": {
                "image": "registry.example/cloud-offload-comfyui@sha256:abc",
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )

    route = select_provider(config, model="comfyui-partition-v1")

    assert route.provider == "runpod"
    assert route.profile["name"] == "comfyui"

    with pytest.raises(ValueError, match="vast.ai worker profile"):
        select_provider(config, requested="vast.ai", model="comfyui-partition-v1")


def test_queue_migrates_legacy_schema(tmp_path):
    db_path = tmp_path / "queue.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                status TEXT NOT NULL,
                input_path TEXT NOT NULL,
                params TEXT,
                preview_path TEXT,
                result_path TEXT,
                created_at TEXT,
                updated_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                error TEXT,
                worker_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO jobs (
                id, model, status, input_path, params, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-job",
                "comfyui-workflow",
                "queued",
                "inline://legacy",
                "{}",
                "2026-07-28T23:59:00",
                "2026-07-28T23:59:00",
            ),
        )
        conn.execute(
            """
            CREATE TABLE job_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                event_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO job_events (job_id, event_json, created_at)
            VALUES (?, ?, ?)
            """,
            (
                "legacy-job",
                '{"type":"progress","phase":"execution","value":1,"max":4}',
                "2026-07-29T00:00:00",
            ),
        )

    queue = JobQueue(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        version = conn.execute(
            "SELECT value FROM queue_meta WHERE key = 'schema_version'"
        ).fetchone()[0]

    assert {
        "attempts",
        "max_attempts",
        "schema_version",
        "request_json",
        "provider",
        "result_json",
        "progress",
    } <= columns
    with sqlite3.connect(db_path) as conn:
        event_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(job_events)").fetchall()
        }

    assert {
        "producer_id",
        "producer_sequence",
        "occurred_at",
        "observed_at",
        "event_type",
        "phase",
    } <= event_columns
    assert version == "8"
    migrated_events = queue.list_events("legacy-job")
    legacy_event = migrated_events[0]
    assert legacy_event["schema"] == "cloud-offload.job-event.v2"
    assert legacy_event["producer"] == {"id": "legacy", "sequence": None}
    assert legacy_event["occurred_at"] == "2026-07-29T00:00:00"
    assert legacy_event["observed_at"] == "2026-07-29T00:00:00"
    assert legacy_event["type"] == "progress"
    assert legacy_event["phase"] == "execution"
    assert migrated_events[1]["type"] == "job_state_seeded"
    assert migrated_events[1]["status"] == "queued"
    assert queue.event_snapshot("legacy-job")["state_source"] == "journal"


def test_job_events_are_ordered_and_resumable(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "partition://input")

    first = queue.append_event(job.id, {"type": "executing", "node_id": "4"})
    second = queue.append_event(job.id, {"type": "progress", "value": 3, "max": 10})

    assert first["sequence"] < second["sequence"]
    assert [item["event"]["type"] for item in queue.list_events(job.id)] == [
        "job_created",
        "executing",
        "progress",
    ]
    assert queue.list_events(job.id, after=first["sequence"]) == [second]


def test_job_event_v2_is_idempotent_per_producer_sequence(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "partition://input")
    event = {
        "type": "weight_download_progress",
        "phase": "dependency_preparation",
        "bytes": 1024,
        "total_bytes": 4096,
        "provider": "runpod",
    }

    first = queue.append_event(
        job.id,
        event,
        producer_id="worker:one:process-a",
        producer_sequence=7,
        occurred_at="2026-07-29T00:00:00",
    )
    duplicate = queue.append_event(
        job.id,
        event,
        producer_id="worker:one:process-a",
        producer_sequence=7,
        occurred_at="2026-07-29T00:00:01",
    )

    assert duplicate == first
    assert first["schema"] == "cloud-offload.job-event.v2"
    assert first["producer"] == {"id": "worker:one:process-a", "sequence": 7}
    assert first["phase_owner"] == "worker"
    assert first["event"]["phase_owner"] == "worker"
    assert first["metrics"] == {"bytes": 1024, "total_bytes": 4096}
    assert first["resources"] == {"provider": "runpod"}
    assert len(queue.list_events(job.id)) == 2

    with pytest.raises(ValueError, match="reused with different data"):
        queue.append_event(
            job.id,
            {**event, "bytes": 2048},
            producer_id="worker:one:process-a",
            producer_sequence=7,
        )


def test_job_event_snapshot_projects_cursor_phase_and_monotonic_progress(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "partition://input")
    queue.append_event(
        job.id,
        {"type": "runner_starting", "phase": "worker_boot", "overall_progress": 2},
    )
    last = queue.append_event(
        job.id,
        {
            "type": "weight_download_progress",
            "phase": "dependency_preparation",
            "overall_progress": 24,
        },
    )

    snapshot = queue.event_snapshot(job.id)

    assert snapshot["schema"] == "cloud-offload.job-snapshot.v1"
    assert snapshot["event_cursor"] == last["sequence"]
    assert snapshot["event_count"] == 3
    assert snapshot["lifecycle_phase"] == "dependency_preparation"
    assert snapshot["progress"] == 24
    assert snapshot["last_event"] == last

    queue.update_status(job.id, JobStatus.COMPLETED)
    assert queue.event_snapshot(job.id)["progress"] == 100


def test_lifecycle_journal_is_authoritative_across_reload_and_row_drift(tmp_path):
    db_path = tmp_path / "queue.db"
    queue = JobQueue(db_path)
    job = queue.create(
        "comfyui-partition-v1",
        "partition://input",
        provider="runpod",
        request={"partition": {"partition_id": "part-7"}},
        status=JobStatus.QUEUED,
    )
    claimed = queue.claim_jobs(
        "worker-7", provider="runpod", models=["comfyui-partition-v1"]
    )[0]
    queue.update_status(claimed.id, JobStatus.RUNNING, progress=10)
    queue.complete_job(claimed.id, {"partition_id": "part-7"})

    lifecycle = [
        item for item in queue.list_events(job.id) if item["type"].startswith("job_")
    ]
    assert [item["status"] for item in lifecycle] == [
        "queued",
        "dispatched",
        "running",
        "completed",
    ]
    assert all(item["partition_id"] == "part-7" for item in lifecycle)
    assert lifecycle[1]["resources"] == {
        "provider": "runpod",
        "worker_id": "worker-7",
    }

    # Simulate a stale/corrupted projection row. Reload still derives lifecycle
    # state from the immutable journal instead of trusting that row.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, progress = ? WHERE id = ?",
            ("pending", 0, job.id),
        )
    snapshot = JobQueue(db_path).event_snapshot(job.id)

    assert snapshot["state_source"] == "journal"
    assert snapshot["status"] == "completed"
    assert snapshot["progress"] == 100
    assert snapshot["lifecycle_phase"] == "result_transfer"


def test_concurrent_event_retries_collapse_to_one_journal_entry(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "partition://input")

    def publish():
        return queue.append_event(
            job.id,
            {"type": "runner_ready", "phase": "worker_boot"},
            producer_id="worker:one:process-a",
            producer_sequence=9,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: publish(), range(2)))

    assert results[0] == results[1]
    assert [item["type"] for item in queue.list_events(job.id)] == [
        "job_created",
        "runner_ready",
    ]


def test_progress_snapshot_is_reconstructed_from_journal_not_projection_row(tmp_path):
    db_path = tmp_path / "queue.db"
    queue = JobQueue(db_path)
    job = queue.create("comfyui-workflow", "partition://input")

    queue.set_progress(job.id, 37)
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE jobs SET progress = 0 WHERE id = ?", (job.id,))

    events = queue.list_events(job.id)
    assert events[-1]["type"] == "job_progress_changed"
    assert events[-1]["metrics"] == {"overall_progress": 37, "progress": 37}
    assert events[-1]["phase_owner"] == "coordinator"
    assert JobQueue(db_path).event_snapshot(job.id)["progress"] == 37


def test_reordered_worker_events_cannot_regress_snapshot_phase_or_progress(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "partition://input")
    queue.append_event(
        job.id,
        {"type": "executing", "phase": "execution", "overall_progress": 61},
        producer_id="worker:one:process-a",
        producer_sequence=2,
    )
    delayed = queue.append_event(
        job.id,
        {"type": "runner_starting", "phase": "worker_boot", "overall_progress": 5},
        producer_id="worker:one:process-a",
        producer_sequence=1,
    )

    snapshot = queue.event_snapshot(job.id)

    assert snapshot["event_cursor"] == delayed["sequence"]
    assert snapshot["last_event"] == delayed
    assert snapshot["lifecycle_phase"] == "execution"
    assert snapshot["progress"] == 61


def test_status_row_rolls_back_when_lifecycle_event_cannot_be_persisted(
    monkeypatch, tmp_path
):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "partition://input")

    def refuse_event(*args, **kwargs):
        raise sqlite3.OperationalError("journal unavailable")

    monkeypatch.setattr(queue, "_append_event_in_transaction", refuse_event)

    with pytest.raises(sqlite3.OperationalError, match="journal unavailable"):
        queue.update_status(job.id, JobStatus.QUEUED)

    assert queue.get(job.id).status == JobStatus.PENDING
    assert [item["type"] for item in queue.list_events(job.id)] == ["job_created"]


def test_coordinator_queue_publishes_process_scoped_monotonic_event_ids(monkeypatch):
    queue = CoordinatorQueue(
        "https://coordinator.invalid",
        "worker-secret",
        "runpod",
        "worker-7",
    )
    requests = []

    def record(path, payload):
        requests.append((path, payload))
        return payload

    monkeypatch.setattr(queue, "_post", record)

    first = queue.append_event("job-1", {"type": "runner_starting"})
    second = queue.append_event("job-1", {"type": "runner_ready"})

    assert first["producer_id"] == second["producer_id"]
    assert first["producer_id"].startswith("worker:worker-7:")
    assert [item[1]["producer_sequence"] for item in requests] == [1, 2]
    assert all(item[1]["occurred_at"] for item in requests)


def test_coordinator_queue_reports_its_prepared_volume_when_claiming(monkeypatch):
    queue = CoordinatorQueue(
        "https://coordinator.invalid",
        "worker-secret",
        "runpod",
        "worker-7",
    )
    requests = []

    def record(path, payload):
        requests.append((path, payload))
        return []

    monkeypatch.setattr(queue, "_post", record)

    assert queue.claim_jobs("worker-7", cache_volume_id="volume-1") == []
    assert requests[0][1]["cache_volume_id"] == "volume-1"


def test_claim_increments_attempts_and_assigns_worker(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "input.json")
    queue.update_status(job.id, JobStatus.QUEUED)

    claimed = queue.claim_jobs("worker-a", limit=1)

    assert len(claimed) == 1
    assert claimed[0].status == JobStatus.DISPATCHED
    assert claimed[0].worker_id == "worker-a"
    assert claimed[0].attempts == 1


def test_claim_is_scoped_to_worker_provider(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    vast = queue.create(
        "comfyui-workflow",
        "inline://request",
        provider="vast.ai",
        status=JobStatus.QUEUED,
    )
    runpod = queue.create(
        "comfyui-workflow",
        "inline://request",
        provider="runpod",
        status=JobStatus.QUEUED,
    )

    claimed = queue.claim_jobs("worker-r", provider="runpod")

    assert [job.id for job in claimed] == [runpod.id]
    assert queue.get(vast.id).status == JobStatus.QUEUED


def test_claim_is_scoped_to_worker_model_capabilities(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    supported = queue.create(
        "comfyui-partition-v1",
        "inline://request",
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    unsupported = queue.create(
        "comfyui-workflow",
        "inline://request",
        provider="runpod",
        status=JobStatus.QUEUED,
    )

    claimed = queue.claim_jobs(
        "worker-r", provider="runpod", models=["comfyui-partition-v1"]
    )

    assert [job.id for job in claimed] == [supported.id]
    assert queue.get(unsupported.id).status == JobStatus.QUEUED


def test_worker_heartbeat_reports_profile_and_capabilities(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")

    queue.record_worker(
        "worker-r",
        "runpod",
        runtime_profile="comfyui",
        capabilities=["comfyui-partition-v1"],
    )

    workers = queue.list_active_workers()
    assert workers[0]["runtime_profile"] == "comfyui"
    assert workers[0]["capabilities"] == ["comfyui-partition-v1"]


def test_completed_result_round_trips(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create(
        "comfyui-workflow",
        "inline://request",
        request={"workflow": {}, "provider": "runpod"},
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    job.error = "previous attempt failed"
    queue.update(job)

    completed = queue.complete_job(job.id, {"prompt_id": "xyz", "outputs": {}})

    assert completed.progress == 100
    assert completed.error is None
    assert queue.get(job.id).result["prompt_id"] == "xyz"


def test_worker_heartbeat_tracks_continuous_idle_time(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")

    queue.record_worker("worker-1", "runpod", idle=True)
    first = queue.list_active_workers()[0]
    queue.record_worker("worker-1", "runpod", idle=True)
    second = queue.list_active_workers()[0]

    assert first["idle_since"] is not None
    assert second["idle_since"] == first["idle_since"]

    queue.record_worker("worker-1", "runpod", idle=False)
    active = queue.list_active_workers()[0]
    assert active["idle_since"] is None
    assert active["idle_seconds"] == 0


def test_worker_live_policy_can_keep_an_idle_worker_alive():
    class PolicyQueue:
        def worker_policy(self):
            return {"keep_warm": True, "idle_shutdown_seconds": 1}

    worker = Worker.__new__(Worker)
    worker.config = CloudConfig(idle_shutdown_seconds=1)
    worker.queue = PolicyQueue()
    worker.last_job_time = utc_now() - timedelta(hours=2)

    assert worker._should_shutdown() is False


def test_worker_idle_time_starts_after_a_job_ends():
    class SingleJobQueue:
        failed = []

        def claim_jobs(self, *args, **kwargs):
            return [SimpleNamespace(id="job-1")]

        def fail_job(self, job_id, error):
            self.failed.append((job_id, error))

    worker = Worker.__new__(Worker)
    worker.config = CloudConfig(poll_interval_seconds=1)
    worker.queue = SingleJobQueue()
    worker.worker_id = "worker-1"
    worker.runtime_profile = "comfyui"
    worker.capabilities = ["comfyui-partition-v1"]
    worker.gpu_vram_gb = 80
    worker.gpu_name = "A100"
    worker.cache_volume_id = None
    old_time = utc_now() - timedelta(hours=1)

    def complete_long_job(job):
        worker.last_job_time = old_time

    worker._process_job = complete_long_job
    worker.run(once=True)

    assert worker.last_job_time > old_time


def test_live_idle_policy_cannot_remove_dispatcher_cleanup_grace():
    class PolicyQueue:
        def worker_policy(self):
            return {"keep_warm": False, "idle_shutdown_seconds": 1}

    worker = Worker.__new__(Worker)
    worker.config = CloudConfig(idle_shutdown_seconds=61)
    worker.queue = PolicyQueue()
    worker.last_job_time = utc_now() - timedelta(seconds=30)

    assert worker._should_shutdown() is False


def test_dispatcher_does_not_terminate_pinned_workers(tmp_path):
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        keep_warm=True,
        idle_shutdown_seconds=1,
    )
    dispatcher = Dispatcher(config, provider=DummyProvider())
    dispatcher.active_instances["pod-1"] = SimpleNamespace(id="pod-1")
    dispatcher.instance_providers["pod-1"] = config.provider
    dispatcher.instance_profiles["pod-1"] = "comfyui"
    dispatcher.last_activity["pod-1"] = utc_now() - timedelta(hours=2)

    dispatcher._check_idle_workers()

    assert "pod-1" in dispatcher.active_instances


def test_dispatcher_reuses_generated_worker_token_across_restarts(tmp_path):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))

    first = Dispatcher(config, provider=DummyProvider())
    second = Dispatcher(config, provider=DummyProvider())

    assert second.worker_token == first.worker_token
    second.queue.authorize_worker(first.worker_token)
    token_path = tmp_path / "worker-token"
    assert token_path.read_text(encoding="utf-8").strip() == first.worker_token


def test_dispatcher_does_not_launch_over_registered_warm_worker(monkeypatch, tmp_path):
    class LaunchCountingProvider(DummyProvider):
        def __init__(self):
            self.launches = 0

        def find_cheapest(self, **kwargs):
            return {"id": "offer", "gpu_type": "RTX 4090", "hourly_rate": 0.69}

        def launch(self, *args, **kwargs):
            self.launches += 1
            raise AssertionError("an active coordinator worker must be reused")

    provider = LaunchCountingProvider()
    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        min_queue_depth=1,
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui"},
        status=JobStatus.QUEUED,
    )
    queue.record_worker(
        "warm-worker",
        "runpod",
        runtime_profile="comfyui",
        capabilities=["comfyui-partition-v1"],
        idle=True,
    )
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load",
        lambda *args, **kwargs: config,
    )

    dispatcher._tick()

    assert provider.launches == 0


def test_dispatcher_launches_legacy_workers_with_effectively_indefinite_timeout(
    tmp_path,
):
    class LaunchProvider(DummyProvider):
        def __init__(self):
            self.env_vars = None

        def find_cheapest(self, **kwargs):
            return {
                "id": "offer-1",
                "gpu_type": "RTX A6000",
                "hourly_rate": 0.49,
            }

        def launch(self, *args, **kwargs):
            self.env_vars = kwargs["env_vars"]
            return SimpleNamespace(
                id="pod-1",
                provider="vast.ai",
                gpu_type="RTX A6000",
                hourly_rate=0.49,
                status="running",
            )

    provider = LaunchProvider()
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        keep_warm=True,
        provider="vast.ai",
        worker_profiles={
            "comfyui": {
                "image": "registry.invalid/comfyui@sha256:abc",
                "models": ["comfyui-partition-v1"],
                "providers": ["vast.ai"],
            }
        },
    )
    dispatcher = Dispatcher(config, provider=provider)

    dispatcher._launch_worker("vast.ai", "comfyui")

    assert provider.env_vars["CLOUD_OFFLOAD_KEEP_WARM"] == "true"
    assert (
        int(provider.env_vars["CLOUD_OFFLOAD_IDLE_SHUTDOWN"]) == 10 * 365 * 24 * 60 * 60
    )


def test_dispatcher_gives_worker_idle_fail_safe_a_cleanup_grace(tmp_path):
    class LaunchProvider(DummyProvider):
        def __init__(self):
            self.env_vars = None

        def find_cheapest(self, **kwargs):
            return {"id": "offer-1", "gpu_type": "A100", "hourly_rate": 1.49}

        def launch(self, *args, **kwargs):
            self.env_vars = kwargs["env_vars"]
            return SimpleNamespace(
                id="pod-1",
                provider="runpod",
                gpu_type="A100",
                hourly_rate=1.49,
                status="running",
            )

    provider = LaunchProvider()
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        idle_shutdown_seconds=300,
        provider="runpod",
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:abc",
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )

    Dispatcher(config, provider=provider)._launch_worker("runpod", "comfyui")

    assert int(provider.env_vars["CLOUD_OFFLOAD_IDLE_SHUTDOWN"]) == 360


def test_dispatcher_passes_configured_and_image_profile_names(tmp_path):
    class LaunchProvider(DummyProvider):
        def __init__(self):
            self.env_vars = None

        def find_cheapest(self, **kwargs):
            return {"id": "offer-1", "gpu_type": "A100", "hourly_rate": 1.49}

        def launch(self, *args, **kwargs):
            self.env_vars = kwargs["env_vars"]
            return SimpleNamespace(
                id="pod-1",
                provider="runpod",
                gpu_type="A100",
                hourly_rate=1.49,
                status="running",
            )

    provider = LaunchProvider()
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        provider="runpod",
        worker_profiles={
            "comfyui-runtime-proof": {
                "image_profile": "comfyui",
                "image": "ghcr.io/example/comfyui@sha256:abc",
                "platform": "linux-x86_64",
                "python_abi": "cp311",
                "models": ["comfyui-workflow"],
                "providers": ["runpod"],
            }
        },
    )

    Dispatcher(config, provider=provider)._launch_worker(
        "runpod", "comfyui-runtime-proof"
    )

    assert provider.env_vars["CLOUD_OFFLOAD_WORKER_PROFILE"] == "comfyui-runtime-proof"
    assert provider.env_vars["CLOUD_OFFLOAD_WORKER_IMAGE_PROFILE"] == "comfyui"
    assert provider.env_vars["CLOUD_OFFLOAD_WORKER_PLATFORM"] == "linux-x86_64"
    assert provider.env_vars["CLOUD_OFFLOAD_WORKER_PYTHON_ABI"] == "cp311"


def test_dispatcher_reports_provisioning_and_backs_off_after_launch_failure(tmp_path):
    class FailingProvider(DummyProvider):
        def __init__(self):
            self.launch_count = 0

        def find_cheapest(self, **kwargs):
            return {
                "id": "offer-1",
                "gpu_type": "RTX 4090",
                "hourly_rate": 0.34,
            }

        def launch(self, *args, **kwargs):
            self.launch_count += 1
            raise RuntimeError("private image cannot be pulled")

    provider = FailingProvider()
    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        min_queue_depth=1,
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/private@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
                "min_gpu_ram_gb": 16,
            }
        },
    )
    queue = JobQueue(config.queue_db_path)
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui", "min_gpu_ram_gb": 16},
        status=JobStatus.QUEUED,
    )
    dispatcher = Dispatcher(config, queue=queue, provider=provider)

    dispatcher._tick()
    dispatcher._tick()

    all_envelopes = queue.list_events(job.id)
    envelopes = [item for item in all_envelopes if item["type"] != "job_created"]
    events = [item["event"] for item in envelopes]
    assert provider.launch_count == 1
    assert [item["type"] for item in events] == [
        "provisioning_started",
        "lease_created",
        "provider_request_started",
        "provider_request_failed",
        "lease_closed_without_resource",
        "provisioning_failed",
    ]
    assert events[-1]["retry_seconds"] == 10
    assert events[-1]["error"] == "private image cannot be pulled"
    assert all(item["producer"]["id"].startswith("dispatcher:") for item in envelopes)
    assert all(item["phase_owner"] == "dispatcher" for item in envelopes)
    assert [item["producer"]["sequence"] for item in envelopes] == [
        1,
        None,
        2,
        3,
        None,
        4,
    ]


def test_worker_token_required_when_queue_is_configured(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    assert queue.worker_auth_configured() is False
    job = queue.create("comfyui-workflow", "input.json")
    queue.update_status(job.id, JobStatus.QUEUED)
    queue.set_worker_token("secret")
    assert queue.worker_auth_configured() is True

    with pytest.raises(PermissionError):
        queue.claim_jobs("worker-a", limit=1)

    with pytest.raises(PermissionError):
        queue.claim_jobs("worker-a", limit=1, token="wrong")

    claimed = queue.claim_jobs("worker-a", limit=1, token="secret")
    assert claimed[0].id == job.id


def test_claim_jobs_enforces_partition_gpu_constraints(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    small = queue.create(
        "comfyui-partition-v1",
        "small.part",
        provider="runpod",
        params={"gpu_type": "any", "min_gpu_ram_gb": 12},
    )
    large = queue.create(
        "comfyui-partition-v1",
        "large.part",
        provider="runpod",
        params={"gpu_type": "RTX_4090", "min_gpu_ram_gb": 30},
    )
    for job in (small, large):
        queue.update_status(job.id, JobStatus.QUEUED)

    claimed = queue.claim_jobs(
        "worker-a",
        provider="runpod",
        models=["comfyui-partition-v1"],
        gpu_vram_gb=24,
        gpu_name="NVIDIA GeForce RTX 4090",
    )

    assert [job.id for job in claimed] == [small.id]
    assert queue.get(large.id).status == JobStatus.QUEUED


def test_claim_jobs_matches_normalized_gpu_name(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"gpu_type": "RTX_4090", "min_gpu_ram_gb": 16},
    )
    queue.update_status(job.id, JobStatus.QUEUED)

    claimed = queue.claim_jobs(
        "worker-a",
        provider="runpod",
        models=["comfyui-partition-v1"],
        gpu_vram_gb=24,
        gpu_name="NVIDIA GeForce RTX 4090",
    )

    assert [item.id for item in claimed] == [job.id]


@pytest.mark.parametrize("gpu_name,vram,claims", [
    ("NVIDIA H100 80GB HBM3", 79.6, True),
    ("H100 SXM", 80, True),
    ("NVIDIA H100 PCIe", 80, False),
    ("NVIDIA H200", 141, False),
    ("NVIDIA H100 80GB HBM3", 78, False),
])
def test_runpod_h100_catalog_name_matches_driver_without_widening_gpu_constraint(tmp_path, gpu_name, vram, claims):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-partition-v1", "input.part", provider="runpod",
                       params={"gpu_type": "H100 SXM", "min_gpu_ram_gb": 80},
                       status=JobStatus.QUEUED)
    result = queue.claim_jobs("worker-japan", provider="runpod", models=["comfyui-partition-v1"],
                              gpu_name=gpu_name, gpu_vram_gb=vram)
    assert [item.id for item in result] == ([job.id] if claims else [])


def test_prepared_job_can_only_be_claimed_by_its_confirmed_volume(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={
            "gpu_type": "any",
            "preflight": {"prepared_volume_id": "volume-1"},
        },
        status=JobStatus.QUEUED,
    )

    cold = queue.claim_jobs(
        "worker-cold",
        provider="runpod",
        models=["comfyui-partition-v1"],
        cache_volume_id="",
    )
    wrong = queue.claim_jobs(
        "worker-wrong",
        provider="runpod",
        models=["comfyui-partition-v1"],
        cache_volume_id="volume-2",
    )
    correct = queue.claim_jobs(
        "worker-right",
        provider="runpod",
        models=["comfyui-partition-v1"],
        cache_volume_id="volume-1",
    )

    assert cold == []
    assert wrong == []
    assert [item.id for item in correct] == [job.id]


def test_fail_job_requeues_then_dead_letters(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "input.json")
    job.max_attempts = 2
    queue.update(job)

    queue.update_status(job.id, JobStatus.QUEUED)
    claimed = queue.claim_jobs("worker-a", limit=1)[0]
    failed_once = queue.fail_job(claimed.id, "boom")

    assert failed_once.status == JobStatus.QUEUED
    assert failed_once.error == "boom"
    assert failed_once.completed_at is None

    claimed = queue.claim_jobs("worker-b", limit=1)[0]
    failed_twice = queue.fail_job(claimed.id, "still boom")

    assert failed_twice.status == JobStatus.DEAD_LETTER
    assert failed_twice.error == "still boom"
    assert failed_twice.completed_at is not None


def test_cancelled_job_is_not_resurrected_by_late_worker_callbacks(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        params={"partition_cache_key": "cancelled-result"},
        status=JobStatus.QUEUED,
    )
    claimed = queue.claim_jobs("worker-a", limit=1)[0]
    cancelled = queue.update_status(claimed.id, JobStatus.FAILED, error="Cancelled")

    assert cancelled.status == JobStatus.FAILED
    assert queue.update_status(claimed.id, JobStatus.RUNNING).status == JobStatus.FAILED
    assert queue.set_progress(claimed.id, 99).progress == cancelled.progress
    assert queue.fail_job(claimed.id, "late failure").status == JobStatus.FAILED
    assert queue.complete_job(claimed.id, {"output": "late"}).status == JobStatus.FAILED
    assert queue.get(claimed.id).error == "Cancelled"
    assert queue.get_partition_cache("cancelled-result") is None


def test_dispatcher_startup_script_uses_wheelhouse_only(tmp_path):
    config = CloudConfig(
        queue_db_path=str(tmp_path / "queue.db"),
        worker_wheelhouse_url="https://example.invalid/cloud-offload-wheelhouse.tar.gz",
        worker_wheelhouse_sha256="0" * 64,
    )
    dispatcher = Dispatcher(config, provider=DummyProvider())

    script = dispatcher._build_startup_script()

    assert "--no-index" in script
    assert "cloud-offload[cloud]" in script
    assert "$CLOUD_OFFLOAD_WORKER_WHEELHOUSE_SHA256" in script


def test_dispatcher_refuses_live_registry_worker_install(tmp_path):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    dispatcher = Dispatcher(config, provider=DummyProvider())

    with pytest.raises(RuntimeError, match="live registry installs are disabled"):
        dispatcher._build_startup_script()


def test_dispatcher_builds_selected_connector_from_registry(tmp_path):
    config = CloudConfig(
        provider="runpod",
        runpod_api_key="runpod-secret",
        queue_db_path=str(tmp_path / "queue.db"),
    )

    dispatcher = Dispatcher(config)

    assert dispatcher.connector.name == "runpod"
    assert dispatcher.provider is dispatcher.connector


def test_profile_startup_assumes_dependencies_are_baked_into_image(tmp_path):
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    dispatcher = Dispatcher(config, provider=DummyProvider())
    profile = {
        "name": "comfyui",
        "image": "registry.example/cloud-offload-comfyui@sha256:abc",
        "models": ["comfyui-partition-v1"],
        "providers": ["vast.ai"],
        "wheelhouse_url": "",
        "wheelhouse_sha256": "",
    }

    script = dispatcher._build_startup_script(profile)

    assert script is None


# === Offer cooldown after failed launches ===


class TwoOfferProvider(DummyProvider):
    """Cheapest offer's host refuses to launch; the pricier one works."""

    def __init__(self):
        self.launch_attempts = []

    @property
    def name(self) -> str:
        return "runpod"

    def list_available(self, *args, **kwargs):
        return [
            {"id": "offer-dead", "gpu_type": "RTX 4000 Ada", "hourly_rate": 0.28},
            {"id": "offer-good", "gpu_type": "RTX 2000 Ada", "hourly_rate": 0.30},
        ]

    def launch(self, offer_id, *args, **kwargs):
        self.launch_attempts.append(offer_id)
        if offer_id == "offer-dead":
            raise RuntimeError("This machine does not have the resources")
        return SimpleNamespace(
            id="pod-1",
            provider="runpod",
            gpu_type="RTX 2000 Ada",
            hourly_rate=0.30,
            status="pending",
        )


def _cooldown_dispatcher(tmp_path, provider):
    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        worker_profiles={
            "comfyui": {
                "image": "registry.invalid/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )
    return Dispatcher(config, provider=provider)


def test_failed_offer_cools_down_and_next_launch_routes_around_it(tmp_path):
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)

    assert dispatcher._launch_worker("runpod", "comfyui") is None
    assert ("runpod", "offer-dead") in dispatcher.offer_cooldowns

    instance = dispatcher._launch_worker("runpod", "comfyui")
    assert instance is not None and instance.id == "pod-1"
    assert provider.launch_attempts == ["offer-dead", "offer-good"]


def _confirmed_job(
    queue,
    *,
    rate=0.30,
    max_rate=0.5,
    expires_at="2099-01-01T00:00:00Z",
):
    return queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={
            "runtime_profile": "comfyui",
            "gpu_type": "RTX 2000 Ada",
            "preflight": {
                "candidate_id": "sha256:" + "c" * 64,
                "provider": "runpod",
                "offer_id": "offer-good",
                "gpu_type": "RTX 2000 Ada",
                "gpu_ram_gb": 0,
                "hourly_rate": rate,
                "region": None,
                "prepared_volume_id": None,
                "expires_at": expires_at,
                "request_policy": {"max_hourly_rate": max_rate},
            },
        },
        status=JobStatus.QUEUED,
    )


def test_dispatcher_launches_only_the_exact_confirmed_offer(tmp_path):
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)
    job = _confirmed_job(dispatcher.queue)

    instance = dispatcher._launch_worker("runpod", "comfyui", [job])

    assert instance is not None
    assert provider.launch_attempts == ["offer-good"]


def test_dispatcher_refuses_changed_confirmed_price_before_launch(tmp_path):
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)
    job = _confirmed_job(dispatcher.queue, rate=0.29)

    instance = dispatcher._launch_worker("runpod", "comfyui", [job])

    assert instance is None
    assert provider.launch_attempts == []
    failed = dispatcher.queue.get(job.id)
    assert failed.status == JobStatus.FAILED
    assert "Preflight confirmation required" in failed.error
    events = dispatcher.queue.list_events(job.id)
    assert any(
        item["event"]["type"] == "preflight_confirmation_required"
        for item in events
    )


def test_fail_fast_clock_includes_provider_readiness_wait(tmp_path):
    dispatcher = _cooldown_dispatcher(tmp_path, TwoOfferProvider())
    lease = dispatcher.queue.create_lease(
        provider="runpod",
        runtime_profile="comfyui",
        ttl_seconds=900,
    )
    rental_started = utc_now() - timedelta(seconds=300)
    with sqlite3.connect(dispatcher.queue.db_path) as conn:
        conn.execute(
            "UPDATE job_leases SET created_at=? WHERE id=?",
            (rental_started.isoformat(), lease.id),
        )

    instance = SimpleNamespace(
        id="pod-readiness-wait",
        provider="runpod",
        gpu_type="GPU",
        hourly_rate=0.3,
        status="running",
    )
    dispatcher._remember_launched_instance(
        instance, "runpod", "comfyui", [], lease.id
    )

    assert dispatcher.launched_at[instance.id] == rental_started
    assert (utc_now() - dispatcher.last_activity[instance.id]).total_seconds() < 10


def test_grouped_jobs_with_mixed_confirmed_prices_are_not_launched_together(
    tmp_path,
):
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)
    first = _confirmed_job(dispatcher.queue, rate=0.30)
    second = _confirmed_job(dispatcher.queue, rate=0.29)

    instance = dispatcher._launch_worker("runpod", "comfyui", [first, second])

    assert instance is None
    assert provider.launch_attempts == []
    assert dispatcher.queue.get(first.id).status == JobStatus.FAILED
    assert dispatcher.queue.get(second.id).status == JobStatus.FAILED


def test_grouped_jobs_with_mixed_rate_ceilings_cannot_exceed_one_confirmation(
    tmp_path,
):
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)
    first = _confirmed_job(dispatcher.queue, max_rate=0.5)
    second = _confirmed_job(dispatcher.queue, max_rate=0.29)

    instance = dispatcher._launch_worker("runpod", "comfyui", [first, second])

    assert instance is None
    assert provider.launch_attempts == []
    assert dispatcher.queue.get(first.id).status == JobStatus.FAILED
    assert dispatcher.queue.get(second.id).status == JobStatus.FAILED


def test_expired_confirmed_quote_still_launches_the_matching_live_offer(tmp_path):
    """A quote is volatile by contract; freshness comes from re-validating the
    exact offer and price against the live catalog, so a retry long after
    confirmation (e.g. after a stalled pod was terminated) must still launch
    when the live offer is unchanged."""
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)
    job = _confirmed_job(dispatcher.queue, expires_at="2000-01-01T00:00:00Z")

    instance = dispatcher._launch_worker("runpod", "comfyui", [job])

    assert instance is not None
    assert provider.launch_attempts == ["offer-good"]


def test_expired_confirmed_quote_with_changed_price_is_still_refused(tmp_path):
    provider = TwoOfferProvider()
    dispatcher = _cooldown_dispatcher(tmp_path, provider)
    job = _confirmed_job(
        dispatcher.queue, rate=0.29, expires_at="2000-01-01T00:00:00Z"
    )

    instance = dispatcher._launch_worker("runpod", "comfyui", [job])

    assert instance is None
    assert provider.launch_attempts == []
    assert dispatcher.queue.get(job.id).status == JobStatus.FAILED


def test_confirmed_job_launches_without_waiting_for_queue_batch(
    monkeypatch, tmp_path
):
    provider = TwoOfferProvider()
    config = _profile_config(tmp_path)
    config.coordinator_url = "https://coordinator.invalid"
    config.min_queue_depth = 3
    queue = JobQueue(config.queue_db_path)
    _confirmed_job(queue)
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load", lambda *a, **k: config
    )

    dispatcher._tick()

    assert provider.launch_attempts == ["offer-good"]


def test_offer_cooldown_expires_and_is_pruned(tmp_path):
    import time as time_module

    dispatcher = _cooldown_dispatcher(tmp_path, TwoOfferProvider())
    dispatcher.offer_cooldowns[("runpod", "offer-dead")] = time_module.monotonic() - 1
    assert dispatcher._offers_on_cooldown("runpod") == set()
    assert dispatcher.offer_cooldowns == {}


def test_cooldown_is_scoped_to_its_provider(tmp_path):
    import time as time_module

    dispatcher = _cooldown_dispatcher(tmp_path, TwoOfferProvider())
    dispatcher.offer_cooldowns[("vast.ai", "offer-dead")] = time_module.monotonic() + 60
    assert dispatcher._offers_on_cooldown("runpod") == set()
    assert dispatcher._offers_on_cooldown("vast.ai") == {"offer-dead"}


def test_find_cheapest_exclude_filters_offers(tmp_path):
    provider = TwoOfferProvider()
    assert provider.find_cheapest()["id"] == "offer-dead"
    assert provider.find_cheapest(exclude={"offer-dead"})["id"] == "offer-good"
    assert provider.find_cheapest(exclude={"offer-dead", "offer-good"}) is None


# === Profile resolution: capability or name ===


def _profile_config(tmp_path, profile_names=("comfyui",)):
    profiles = {
        name: {
            "image": "registry.invalid/comfyui@sha256:" + "a" * 64,
            "models": ["comfyui-workflow", "comfyui-partition-v1"],
            "providers": ["runpod"],
        }
        for name in profile_names
    }
    return CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="test-key",
        queue_db_path=str(tmp_path / "queue.db"),
        worker_profiles=profiles,
    )


def test_a_capability_resolves_to_the_profile_providing_it(tmp_path):
    # What a box actually stamps is the capability it needs; only the operator
    # knows what their profiles are called.
    from cloud_offload.router import select_profile_provider

    route = select_profile_provider(_profile_config(tmp_path), "comfyui-partition-v1")

    assert route.profile["name"] == "comfyui"


def test_an_exact_profile_name_still_wins(tmp_path):
    from cloud_offload.router import select_profile_provider

    route = select_profile_provider(_profile_config(tmp_path), "comfyui")

    assert route.profile["name"] == "comfyui"


def test_capability_resolution_is_deterministic_across_profiles(tmp_path):
    from cloud_offload.router import select_profile_provider

    config = _profile_config(tmp_path, profile_names=("zeta", "alpha"))
    route = select_profile_provider(config, "comfyui-partition-v1")

    assert route.profile["name"] == "alpha"


def test_an_unknown_profile_names_what_is_configured(tmp_path):
    from cloud_offload.router import select_profile_provider

    with pytest.raises(ValueError, match="configured profiles: comfyui"):
        select_profile_provider(_profile_config(tmp_path), "nonsense")


def test_dispatcher_launches_for_a_capability_named_job(tmp_path, monkeypatch):
    """A job stamped with a capability must still provision a worker.

    The dispatcher looked its profile up by the raw name, so a job naming
    comfyui-partition-v1 sat queued forever while the log repeated that the
    profile was unknown — with a correctly configured profile providing it.
    """
    provider = TwoOfferProvider()
    config = _profile_config(tmp_path)
    config.coordinator_url = "https://coordinator.invalid"
    config.min_queue_depth = 1
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load", lambda *a, **k: config
    )

    dispatcher._tick()

    assert provider.launch_attempts, "no worker was launched"


def test_a_card_sold_as_24gb_claims_a_job_needing_24(tmp_path):
    """Advertised size and driver-reported size are not the same number.

    An A5000 is sold as 24 GB and reports 24564 MiB (23.99 GiB). Compared raw,
    a worker refused every job its own GPU had been rented to run, and the job
    waited in the queue with an idle worker beside it.
    """
    queue = JobQueue(str(tmp_path / "queue.db"))
    queue.set_worker_token("t" * 40)
    for vram, expected in ((23.99, 1), (23.69, 1), (22.4, 0), (15.99, 0)):
        job = queue.create(
            "comfyui-partition-v1",
            "input.part",
            provider="runpod",
            params={"runtime_profile": "comfyui", "min_gpu_ram_gb": 24},
            status=JobStatus.QUEUED,
        )
        claimed = queue.claim_jobs(
            "worker-1",
            limit=1,
            token="t" * 40,
            provider="runpod",
            models=["comfyui-partition-v1"],
            gpu_vram_gb=vram,
        )
        assert len(claimed) == expected, f"{vram} GiB against a 24 GiB requirement"
        queue.update_status(job.id, JobStatus.FAILED)


# === A runner that is still coming up is neither absent nor idle ===


class StartingConnector(DummyProvider):
    """A connector that records launches without performing any."""

    def __init__(self):
        self.launches = 0
        self.launch_kwargs = []

    @property
    def name(self):
        return "runpod"

    def list_available(self, *args, **kwargs):
        return []

    def find_cheapest(self, **kwargs):
        return {"id": "offer-1", "gpu_type": "RTX A5000", "hourly_rate": 0.27}

    def launch(self, *args, **kwargs):
        self.launches += 1
        self.launch_kwargs.append(kwargs)
        return SimpleNamespace(
            id="pod-1",
            provider="runpod",
            gpu_type="RTX A5000",
            hourly_rate=0.27,
            status="running",
        )

    def get_instance(self, instance_id):
        return SimpleNamespace(
            id=instance_id,
            provider="runpod",
            gpu_type="RTX A5000",
            hourly_rate=0.27,
            status="running",
        )

    def terminate(self, instance_id):
        self.terminated = True
        return True

    def list_instances(self):
        return []


def starting_config(tmp_path):
    return CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="secret",
        coordinator_url="https://coordinator.invalid",
        min_queue_depth=1,
        idle_shutdown_seconds=1,
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        },
    )


def test_a_starting_runner_stops_a_second_pod_being_rented(tmp_path, monkeypatch):
    from cloud_offload.dispatcher import Dispatcher

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui"},
        status=JobStatus.QUEUED,
    )
    queue.record_worker(
        "worker-boot", "runpod", status="starting", runtime_profile="comfyui"
    )
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load", lambda *args, **kwargs: config
    )
    provider = StartingConnector()

    Dispatcher(config, queue=queue, provider=provider)._tick()

    assert provider.launches == 0


def test_dispatcher_reloads_worker_profile_before_launch(tmp_path, monkeypatch):
    config = starting_config(tmp_path)
    persisted = starting_config(tmp_path)
    persisted.worker_profiles["comfyui"]["image"] = (
        "ghcr.io/example/comfyui@sha256:" + "b" * 64
    )
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    monkeypatch.setattr(
        "cloud_offload.dispatcher.CloudConfig.load", lambda *args, **kwargs: persisted
    )
    provider = StartingConnector()

    Dispatcher(config, queue=queue, provider=provider)._tick()

    assert (
        provider.launch_kwargs[0]["docker_image"]
        == persisted.worker_profiles["comfyui"]["image"]
    )


def long_idle_dispatcher(config, queue):
    from datetime import datetime, timedelta

    from cloud_offload.dispatcher import Dispatcher

    provider = StartingConnector()
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    dispatcher.active_instances["pod-1"] = provider.get_instance("pod-1")
    dispatcher.instance_providers["pod-1"] = "runpod"
    dispatcher.instance_profiles["pod-1"] = "comfyui"
    dispatcher.last_activity["pod-1"] = utc_now() - timedelta(hours=1)
    return dispatcher


def test_a_pod_that_never_registers_is_terminated(tmp_path, monkeypatch):
    from datetime import datetime, timedelta

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    dispatcher = long_idle_dispatcher(config, queue)
    dispatcher.launched_at["pod-1"] = utc_now() - timedelta(minutes=61)
    monkeypatch.setattr(
        "cloud_offload.dispatcher.RUNNER_REGISTRATION_TIMEOUT_SECONDS", 3600
    )

    dispatcher._check_unregistered_workers()

    assert "pod-1" not in dispatcher.active_instances
    assert dispatcher.provider.terminated is True
    assert queue.get(job.id).status == JobStatus.QUEUED
    events = queue.list_events(job.id)
    assert events[-1]["event"]["type"] == "provisioning_failed"
    assert events[-1]["event"]["error"] == "Runner did not register within 3600s"


class StalledHostConnector(StartingConnector):
    """A host that rents the pod but never starts its container."""

    def container_started(self, instance):
        return False


def test_a_rented_pod_whose_container_never_starts_is_terminated_early(
    tmp_path, monkeypatch
):
    """A pod can bill for an hour at full GPU price while its host never
    creates the container. Container telemetry ends that rental at the start
    fail-fast, well before the registration deadline."""
    from datetime import timedelta

    from cloud_offload.dispatcher import Dispatcher

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    provider = StalledHostConnector()
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    dispatcher.active_instances["pod-1"] = provider.get_instance("pod-1")
    dispatcher.instance_providers["pod-1"] = "runpod"
    dispatcher.instance_profiles["pod-1"] = "comfyui"
    dispatcher.launched_at["pod-1"] = utc_now() - timedelta(minutes=11)

    dispatcher._check_unregistered_workers()

    assert "pod-1" not in dispatcher.active_instances
    assert provider.terminated is True
    assert queue.get(job.id).status == JobStatus.QUEUED
    events = queue.list_events(job.id)
    assert events[-1]["event"]["type"] == "provisioning_failed"
    assert "never started the container within 600s" in events[-1]["event"]["error"]


def test_a_started_but_unregistered_container_keeps_the_full_boot_deadline(
    tmp_path,
):
    from datetime import timedelta

    from cloud_offload.dispatcher import Dispatcher

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)

    class StartedHostConnector(StartingConnector):
        def container_started(self, instance):
            return True

    provider = StartedHostConnector()
    dispatcher = Dispatcher(config, queue=queue, provider=provider)
    dispatcher.active_instances["pod-1"] = provider.get_instance("pod-1")
    dispatcher.instance_providers["pod-1"] = "runpod"
    dispatcher.instance_profiles["pod-1"] = "comfyui"
    dispatcher.launched_at["pod-1"] = utc_now() - timedelta(minutes=11)

    dispatcher._check_unregistered_workers()

    assert "pod-1" in dispatcher.active_instances


def test_a_provider_without_container_telemetry_keeps_the_full_boot_deadline(
    tmp_path,
):
    from datetime import timedelta

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    dispatcher = long_idle_dispatcher(config, queue)
    dispatcher.launched_at["pod-1"] = utc_now() - timedelta(minutes=11)

    dispatcher._check_unregistered_workers()

    assert "pod-1" in dispatcher.active_instances


def test_a_paid_starting_pod_emits_elapsed_feedback_before_registration(tmp_path):
    from datetime import datetime, timedelta

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    job = queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    dispatcher = long_idle_dispatcher(config, queue)
    dispatcher.launched_at["pod-1"] = utc_now() - timedelta(seconds=35)
    dispatcher.active_instances["pod-1"].metadata = {
        "provider_state": "RUNNING https://private.invalid/token"
    }

    dispatcher._check_unregistered_workers()

    events = [item["event"] for item in queue.list_events(job.id)]
    diagnostic = next(
        item for item in events if item["type"] == "provider_startup_observation"
    )
    assert diagnostic["startup_phases"] == {
        "allocation": {"state": "confirmed", "provider_state": "UNKNOWN"},
        "image_pull": {"state": "unknown"},
        "container_start": {"state": "unknown"},
        "runner_callback": {"state": "unknown"},
        "comfyui_readiness": {"state": "unknown"},
    }
    event = events[-1]
    assert event["type"] == "runner_starting_progress"
    assert 34 <= event["elapsed_seconds"] <= 36
    assert event["message"] == "Waiting for runner callback and ComfyUI readiness"
    assert "private" not in json.dumps(events).lower()
    assert "pod-1" in dispatcher.active_instances


def test_a_registered_starting_runner_is_not_terminated_by_the_boot_deadline(
    tmp_path,
):
    from datetime import datetime, timedelta

    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.record_worker(
        "worker-boot", "runpod", status="starting", runtime_profile="comfyui"
    )
    dispatcher = long_idle_dispatcher(config, queue)
    dispatcher.launched_at["pod-1"] = utc_now() - timedelta(hours=1)

    dispatcher._check_unregistered_workers()

    assert "pod-1" in dispatcher.active_instances


def test_a_runner_that_is_still_starting_is_not_terminated_as_idle(tmp_path):
    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.record_worker(
        "worker-boot", "runpod", status="starting", runtime_profile="comfyui"
    )
    dispatcher = long_idle_dispatcher(config, queue)

    dispatcher._check_idle_workers()

    assert "pod-1" in dispatcher.active_instances


def test_a_restarted_runner_does_not_reset_paid_resource_cleanup(tmp_path):
    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    dispatcher = long_idle_dispatcher(config, queue)
    dispatcher.runner_ready_instances.add("pod-1")
    queue.record_worker(
        "worker-restarted", "runpod", status="starting", runtime_profile="comfyui"
    )

    dispatcher._check_idle_workers()

    assert "pod-1" not in dispatcher.active_instances
    assert dispatcher.provider.terminated is True


def test_a_pod_is_not_killed_as_idle_while_its_own_work_is_queued(tmp_path):
    """The job carries the capability a client stamped, the pod carries the
    operator's profile name. Comparing them raw matched nothing, so a pod was
    terminated for being idle while the queue held exactly the work it had been
    rented for — and the next tick rented another one."""
    config = starting_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui-partition-v1"},
        status=JobStatus.QUEUED,
    )
    dispatcher = long_idle_dispatcher(config, queue)

    dispatcher._check_idle_workers()

    assert "pod-1" in dispatcher.active_instances
