"""SQLite-based job queue."""

import hashlib
import hmac
import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    """Current UTC time as a naive datetime.

    Timestamps are persisted as ISO strings without a UTC offset and compared
    lexically in SQLite, so the naive form is part of the storage format.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _canonical_gpu_name(value: str | None) -> str:
    name = str(value or "").lower().replace("_", " ").replace("-", " ")
    # RunPod's catalog display name differs from the CUDA driver name for
    # this card. Preserve specific GPU matching and the separate VRAM check.
    return {
        "h100 sxm": "nvidia h100 80gb hbm3",
        "nvidia h100 sxm": "nvidia h100 80gb hbm3",
    }.get(name, name)


# What a worker may report about itself. ``starting`` is a runner that has told
# the coordinator it exists but is still bringing ComfyUI up, and ``failed`` is
# one that never managed to; only the first two are a worker the dispatcher can
# still expect work from.
WORKER_STATUSES = ("starting", "active", "failed")
LIVE_WORKER_STATUSES = ("starting", "active")

JOB_EVENT_SCHEMA = "cloud-offload.job-event.v2"
JOB_EVENT_METRIC_FIELDS = (
    "overall_progress",
    "progress",
    "bytes",
    "total_bytes",
    "percent",
    "elapsed_seconds",
    "downloaded_files",
    "total_files",
    "value",
    "max",
)
JOB_EVENT_RESOURCE_FIELDS = (
    "provider",
    "datacenter_id",
    "region",
    "worker_instance_id",
    "worker_id",
    "pod_id",
    "cache_volume_id",
    "cache_provider_volume_id",
    "gpu_type",
    "hourly_rate",
    "lease_id",
)
JOB_LIFECYCLE_EVENT_TYPES = (
    "job_created",
    "job_state_seeded",
    "job_status_changed",
)
JOB_PHASE_ORDER = {
    "readiness": 0,
    "preflight": 0,
    "provisioning": 10,
    "provider_request": 10,
    "worker_boot": 20,
    "dependency_preparation": 30,
    "weights_staging": 30,
    "node_pack_staging": 30,
    "cache_restore": 30,
    "execution": 40,
    "result_transfer": 50,
    "resource_closure": 60,
}


def _status_phase(status: "JobStatus") -> str:
    return {
        JobStatus.PENDING: "readiness",
        JobStatus.PREVIEW_DONE: "readiness",
        JobStatus.QUEUED: "readiness",
        JobStatus.DISPATCHED: "provisioning",
        JobStatus.RUNNING: "execution",
        JobStatus.COMPLETED: "result_transfer",
        JobStatus.FAILED: "failure",
        JobStatus.DEAD_LETTER: "failure",
    }[status]


def _partition_id(job: "Job") -> str | None:
    partition = job.request.get("partition") if isinstance(job.request, dict) else None
    if isinstance(partition, dict) and partition.get("partition_id") is not None:
        return str(partition["partition_id"])
    return None


def _lifecycle_event(
    job: "Job",
    event_type: str,
    *,
    previous_status: "JobStatus | None" = None,
) -> dict[str, Any]:
    event = {
        "type": event_type,
        "phase": _status_phase(job.status),
        "phase_owner": "coordinator",
        "status": job.status.value,
        "previous_status": previous_status.value if previous_status else None,
        "overall_progress": int(job.progress or 0),
        "attempts": int(job.attempts or 0),
    }
    partition_id = _partition_id(job)
    if partition_id:
        event["partition_id"] = partition_id
    if job.provider:
        event["provider"] = job.provider
    if job.worker_id:
        event["worker_id"] = job.worker_id
    if job.error:
        event["error"] = job.error
    return event


def _progress_event(job: "Job", previous_progress: int) -> dict[str, Any]:
    event = {
        "type": "job_progress_changed",
        "phase": _status_phase(job.status),
        "phase_owner": "coordinator",
        "status": job.status.value,
        "progress": int(job.progress or 0),
        "overall_progress": int(job.progress or 0),
        "previous_progress": int(previous_progress or 0),
    }
    partition_id = _partition_id(job)
    if partition_id:
        event["partition_id"] = partition_id
    if job.worker_id:
        event["worker_id"] = job.worker_id
    return event


def _job_event_envelope(row: tuple) -> dict[str, Any]:
    (
        sequence,
        job_id,
        event_json,
        created_at,
        producer_id,
        producer_sequence,
        occurred_at,
        observed_at,
        event_type,
        phase,
    ) = row
    event = json.loads(event_json)
    metrics = {
        key: event[key] for key in JOB_EVENT_METRIC_FIELDS if event.get(key) is not None
    }
    resources = {
        key: event[key]
        for key in JOB_EVENT_RESOURCE_FIELDS
        if event.get(key) is not None
    }
    evidence = event.get("evidence")
    return {
        "schema": JOB_EVENT_SCHEMA,
        "sequence": int(sequence),
        "job_id": job_id,
        # ``created_at`` remains as a compatibility alias for older clients.
        "created_at": created_at,
        "occurred_at": occurred_at or created_at,
        "observed_at": observed_at or created_at,
        "producer": {
            "id": producer_id or "legacy",
            "sequence": (
                int(producer_sequence) if producer_sequence is not None else None
            ),
        },
        "type": event_type or str(event.get("type") or "unknown"),
        "phase": phase or event.get("phase"),
        "phase_owner": event.get("phase_owner"),
        "partition_id": event.get("partition_id"),
        "status": event.get("status"),
        "metrics": metrics,
        "resources": resources,
        "evidence": evidence if isinstance(evidence, dict) else {},
        # Preserve the original event while producers migrate incrementally.
        "event": event,
    }


class JobStatus(str, Enum):
    """Job lifecycle states."""

    PENDING = "pending"  # Created, needs local preview
    PREVIEW_DONE = "preview_done"  # Local preview complete, waiting for user
    QUEUED = "queued"  # User approved, waiting for cloud
    DISPATCHED = "dispatched"  # Sent to cloud worker
    RUNNING = "running"  # Worker processing
    COMPLETED = "completed"  # Done, result available
    FAILED = "failed"  # Error occurred
    DEAD_LETTER = "dead_letter"  # Retry limit exceeded


LEASE_OPEN_STATUSES = (
    "provisioning",
    "active",
    "revocation_requested",
    "terminating",
)


@dataclass(frozen=True)
class JobLease:
    """Durable control-plane authority for one paid provider resource."""

    id: str
    provider: str
    resource_name: str
    runtime_profile: str
    status: str
    created_at: str
    updated_at: str
    expires_at: str
    renewed_at: str
    instance_id: str | None = None
    worker_id: str | None = None
    hourly_rate: float | None = None
    max_runtime_seconds: int | None = None
    max_cost_usd: float | None = None
    runtime_deadline: str | None = None
    cost_deadline: str | None = None
    revoked_at: str | None = None
    termination_requested_at: str | None = None
    termination_confirmed_at: str | None = None
    termination_attempts: int = 0
    reason: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Job:
    """A cloud offload job."""

    id: str
    model: str
    status: JobStatus

    # Input
    input_path: str  # Path/URI to input
    params: dict = field(default_factory=dict)  # Scheduling/routing params
    request: dict = field(default_factory=dict)
    provider: str | None = None

    # Output
    preview_path: str | None = None
    result_path: str | None = None
    result: dict | None = None
    progress: int = 0

    # Metadata
    created_at: str = ""
    updated_at: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    worker_id: str | None = None
    attempts: int = 0
    max_attempts: int = 3
    schema_version: int = 3

    def __post_init__(self):
        if not self.created_at:
            self.created_at = utc_now().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at
        if isinstance(self.status, str):
            self.status = JobStatus(self.status)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        return cls(**d)


class JobQueue:
    """SQLite-backed job queue."""

    SCHEMA_VERSION = 8

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
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
                    worker_id TEXT,
                    attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 3,
                    schema_version INTEGER DEFAULT 3,
                    request_json TEXT,
                    provider TEXT,
                    result_json TEXT,
                    progress INTEGER DEFAULT 0
                )
            """)
            existing_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
            }
            migrations = {
                "attempts": "ALTER TABLE jobs ADD COLUMN attempts INTEGER DEFAULT 0",
                "max_attempts": "ALTER TABLE jobs ADD COLUMN max_attempts INTEGER DEFAULT 3",
                "schema_version": "ALTER TABLE jobs ADD COLUMN schema_version INTEGER DEFAULT 3",
                "request_json": "ALTER TABLE jobs ADD COLUMN request_json TEXT",
                "provider": "ALTER TABLE jobs ADD COLUMN provider TEXT",
                "result_json": "ALTER TABLE jobs ADD COLUMN result_json TEXT",
                "progress": "ALTER TABLE jobs ADD COLUMN progress INTEGER DEFAULT 0",
            }
            for column, statement in migrations.items():
                if column not in existing_columns:
                    conn.execute(statement)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queue_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            conn.execute(
                """
                INSERT INTO queue_meta (key, value) VALUES ('schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(self.SCHEMA_VERSION),),
            )
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_provider_status ON jobs(provider, status)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    producer_id TEXT NOT NULL DEFAULT 'legacy',
                    producer_sequence INTEGER,
                    occurred_at TEXT,
                    observed_at TEXT,
                    event_type TEXT,
                    phase TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
            """)
            event_columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(job_events)").fetchall()
            }
            event_migrations = {
                "producer_id": (
                    "ALTER TABLE job_events ADD COLUMN producer_id "
                    "TEXT NOT NULL DEFAULT 'legacy'"
                ),
                "producer_sequence": (
                    "ALTER TABLE job_events ADD COLUMN producer_sequence INTEGER"
                ),
                "occurred_at": "ALTER TABLE job_events ADD COLUMN occurred_at TEXT",
                "observed_at": "ALTER TABLE job_events ADD COLUMN observed_at TEXT",
                "event_type": "ALTER TABLE job_events ADD COLUMN event_type TEXT",
                "phase": "ALTER TABLE job_events ADD COLUMN phase TEXT",
            }
            for column, statement in event_migrations.items():
                if column not in event_columns:
                    conn.execute(statement)
            conn.execute(
                """
                UPDATE job_events
                SET occurred_at = COALESCE(occurred_at, created_at),
                    observed_at = COALESCE(observed_at, created_at),
                    event_type = COALESCE(
                        event_type, json_extract(event_json, '$.type'), 'unknown'
                    ),
                    phase = COALESCE(phase, json_extract(event_json, '$.phase'))
                WHERE occurred_at IS NULL
                   OR observed_at IS NULL
                   OR event_type IS NULL
                """
            )
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_job_events_job_sequence
                ON job_events(job_id, sequence)
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_job_events_producer_sequence
                ON job_events(job_id, producer_id, producer_sequence)
                WHERE producer_sequence IS NOT NULL
            """)
            # Existing databases predate journaled state changes. Seed one
            # lifecycle event per job so snapshots can immediately treat the
            # journal as authoritative after migration. New jobs emit
            # ``job_created`` in the same transaction as their row.
            conn.execute(
                """
                INSERT INTO job_events (
                    job_id, event_json, created_at, producer_id,
                    producer_sequence, occurred_at, observed_at, event_type, phase
                )
                SELECT
                    jobs.id,
                    json_object(
                        'type', 'job_state_seeded',
                        'phase', CASE jobs.status
                            WHEN 'pending' THEN 'readiness'
                            WHEN 'preview_done' THEN 'readiness'
                            WHEN 'queued' THEN 'readiness'
                            WHEN 'dispatched' THEN 'provisioning'
                            WHEN 'running' THEN 'execution'
                            WHEN 'completed' THEN 'result_transfer'
                            ELSE 'failure'
                        END,
                        'phase_owner', 'coordinator',
                        'status', jobs.status,
                        'overall_progress', CASE
                            WHEN jobs.status = 'completed' THEN 100
                            ELSE COALESCE(jobs.progress, 0)
                        END,
                        'attempts', COALESCE(jobs.attempts, 0),
                        'provider', jobs.provider,
                        'worker_id', jobs.worker_id
                    ),
                    COALESCE(jobs.updated_at, jobs.created_at, CURRENT_TIMESTAMP),
                    'coordinator:migration',
                    NULL,
                    COALESCE(jobs.updated_at, jobs.created_at, CURRENT_TIMESTAMP),
                    COALESCE(jobs.updated_at, jobs.created_at, CURRENT_TIMESTAMP),
                    'job_state_seeded',
                    CASE jobs.status
                        WHEN 'pending' THEN 'readiness'
                        WHEN 'preview_done' THEN 'readiness'
                        WHEN 'queued' THEN 'readiness'
                        WHEN 'dispatched' THEN 'provisioning'
                        WHEN 'running' THEN 'execution'
                        WHEN 'completed' THEN 'result_transfer'
                        ELSE 'failure'
                    END
                FROM jobs
                WHERE NOT EXISTS (
                    SELECT 1 FROM job_events
                    WHERE job_events.job_id = jobs.id
                      AND job_events.event_type IN (
                          'job_created', 'job_state_seeded', 'job_status_changed'
                      )
                )
                """
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS partition_cache (
                    cache_key TEXT PRIMARY KEY,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    idle_since TEXT,
                    runtime_profile TEXT,
                    capabilities TEXT,
                    detail TEXT
                )
            """)
            worker_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(workers)").fetchall()
            }
            if "runtime_profile" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN runtime_profile TEXT")
            if "capabilities" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN capabilities TEXT")
            if "idle_since" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN idle_since TEXT")
            if "detail" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN detail TEXT")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_leases (
                    id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    instance_id TEXT,
                    resource_name TEXT NOT NULL,
                    runtime_profile TEXT NOT NULL,
                    worker_id TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    renewed_at TEXT NOT NULL,
                    hourly_rate REAL,
                    max_runtime_seconds INTEGER,
                    max_cost_usd REAL,
                    runtime_deadline TEXT,
                    cost_deadline TEXT,
                    revoked_at TEXT,
                    termination_requested_at TEXT,
                    termination_confirmed_at TEXT,
                    termination_attempts INTEGER NOT NULL DEFAULT 0,
                    reason TEXT,
                    last_error TEXT
                )
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_job_leases_instance
                ON job_leases(provider, instance_id)
                WHERE instance_id IS NOT NULL
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_job_leases_status
                ON job_leases(status, updated_at)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_lease_jobs (
                    lease_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    attached_at TEXT NOT NULL,
                    PRIMARY KEY (lease_id, job_id),
                    FOREIGN KEY(lease_id) REFERENCES job_leases(id) ON DELETE CASCADE,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_job_lease_jobs_job
                ON job_lease_jobs(job_id, lease_id)
            """)

    def _get_meta(self, key: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT value FROM queue_meta WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO queue_meta (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    @staticmethod
    def _hash_worker_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def set_worker_token(self, token: str) -> None:
        """Require workers to present this token before claiming jobs."""
        if not token:
            raise ValueError("Worker token must not be empty")
        self._set_meta("worker_token_sha256", self._hash_worker_token(token))

    def worker_auth_configured(self) -> bool:
        """Return whether the shared queue has a worker credential."""
        return bool(self._get_meta("worker_token_sha256"))

    def _verify_worker_token(self, token: str | None) -> None:
        expected = self._get_meta("worker_token_sha256")
        if not expected:
            return
        if not token:
            raise PermissionError("Worker token is required to claim jobs")
        actual = self._hash_worker_token(token)
        if not hmac.compare_digest(actual, expected):
            raise PermissionError("Worker token is invalid")

    def _row_to_job(self, row: tuple) -> Job:
        """Convert database row to Job object."""
        return Job(
            id=row[0],
            model=row[1],
            status=JobStatus(row[2]),
            input_path=row[3],
            params=json.loads(row[4]) if row[4] else {},
            preview_path=row[5],
            result_path=row[6],
            created_at=row[7],
            updated_at=row[8],
            started_at=row[9],
            completed_at=row[10],
            error=row[11],
            worker_id=row[12],
            attempts=row[13] if len(row) > 13 and row[13] is not None else 0,
            max_attempts=row[14] if len(row) > 14 and row[14] is not None else 3,
            schema_version=(
                row[15]
                if len(row) > 15 and row[15] is not None
                else self.SCHEMA_VERSION
            ),
            request=json.loads(row[16]) if len(row) > 16 and row[16] else {},
            provider=row[17] if len(row) > 17 else None,
            result=json.loads(row[18]) if len(row) > 18 and row[18] else None,
            progress=row[19] if len(row) > 19 and row[19] is not None else 0,
        )

    @staticmethod
    def _write_job(conn: sqlite3.Connection, job: Job) -> None:
        conn.execute(
            """
            UPDATE jobs SET
                model = ?, status = ?, input_path = ?, params = ?,
                preview_path = ?, result_path = ?, updated_at = ?,
                started_at = ?, completed_at = ?, error = ?, worker_id = ?,
                attempts = ?, max_attempts = ?, schema_version = ?,
                request_json = ?, provider = ?, result_json = ?, progress = ?
            WHERE id = ?
            """,
            (
                job.model,
                job.status.value,
                job.input_path,
                json.dumps(job.params),
                job.preview_path,
                job.result_path,
                job.updated_at,
                job.started_at,
                job.completed_at,
                job.error,
                job.worker_id,
                job.attempts,
                job.max_attempts,
                job.schema_version,
                json.dumps(job.request),
                job.provider,
                json.dumps(job.result) if job.result is not None else None,
                job.progress,
                job.id,
            ),
        )

    @staticmethod
    def _append_event_in_transaction(
        conn: sqlite3.Connection,
        job_id: str,
        event: dict[str, Any],
        *,
        producer_id: str | None = None,
        producer_sequence: int | None = None,
        occurred_at: str | None = None,
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(event, dict) or not event.get("type"):
            raise ValueError("Job events require a non-empty type")
        plan_row = conn.execute(
            "SELECT model FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if plan_row and plan_row[0] == "comfyui-plan":
            # The journal is itself part of the public plan projection.  Apply
            # the finite event allow-list before persisting, even for trusted
            # internal callers, so a malformed worker callback cannot leave a
            # secret-bearing event in SQLite.
            from cloud_offload.plan_protocol import public_plan_event

            safe = public_plan_event({**event, "job_id": job_id})
            normalized_event = {
                "type": safe["type"],
                "phase": safe["phase"],
                "status": safe["status"],
                **({"metrics": safe["metrics"]} if safe.get("metrics") else {}),
            }
            event = normalized_event
            # Keep producer sequence numbers so retries replay the same
            # journal row.  The producer identity is hashed before storage;
            # the raw worker identity is not a public plan field.
            raw_producer = str(producer_id or "coordinator:plan")
            if raw_producer.startswith("worker:"):
                from cloud_offload.plan_protocol import binding_digest

                producer_id = "plan-worker:" + binding_digest(raw_producer).removeprefix("sha256:")[:32]
            else:
                producer_id = "coordinator:plan"
            occurred_at = None
            observed_at = None
        normalized_producer = str(producer_id or "coordinator").strip()
        if not normalized_producer or len(normalized_producer) > 256:
            raise ValueError("Job event producer_id must contain 1 to 256 characters")
        normalized_event = dict(event)
        if "phase_owner" not in normalized_event:
            if normalized_producer.startswith("worker:"):
                normalized_event["phase_owner"] = "worker"
            elif normalized_producer.startswith("dispatcher:"):
                normalized_event["phase_owner"] = "dispatcher"
            else:
                normalized_event["phase_owner"] = "coordinator"
        if producer_sequence is not None:
            if isinstance(producer_sequence, bool):
                raise ValueError("Job event producer_sequence must be an integer")
            try:
                producer_sequence = int(producer_sequence)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "Job event producer_sequence must be an integer"
                ) from exc
            if producer_sequence < 0:
                raise ValueError("Job event producer_sequence cannot be negative")
        try:
            encoded = json.dumps(
                normalized_event, allow_nan=False, separators=(",", ":")
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("Job event is not finite JSON") from exc
        if len(encoded.encode("utf-8")) > 4 * 1024 * 1024:
            raise ValueError("Job event exceeds the 4 MiB limit")
        normalized_observed_at = str(observed_at or utc_now().isoformat())
        normalized_occurred_at = str(
            occurred_at or normalized_event.get("occurred_at") or normalized_observed_at
        )
        if len(normalized_occurred_at) > 128:
            raise ValueError("Job event occurred_at is too long")
        if len(normalized_observed_at) > 128:
            raise ValueError("Job event observed_at is too long")
        event_type = str(normalized_event["type"])
        phase = (
            str(normalized_event["phase"])
            if normalized_event.get("phase") is not None
            else None
        )
        if producer_sequence is not None:
            existing = conn.execute(
                """
                SELECT sequence, job_id, event_json, created_at,
                       producer_id, producer_sequence, occurred_at,
                       observed_at, event_type, phase
                FROM job_events
                WHERE job_id = ? AND producer_id = ? AND producer_sequence = ?
                """,
                (job_id, normalized_producer, producer_sequence),
            ).fetchone()
            if existing:
                if existing[2] != encoded:
                    raise ValueError(
                        "Job event producer sequence was reused with different data"
                    )
                return _job_event_envelope(existing)
        cursor = conn.execute(
            """
            INSERT INTO job_events (
                job_id, event_json, created_at, producer_id,
                producer_sequence, occurred_at, observed_at, event_type, phase
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                encoded,
                normalized_observed_at,
                normalized_producer,
                producer_sequence,
                normalized_occurred_at,
                normalized_observed_at,
                event_type,
                phase,
            ),
        )
        sequence = int(cursor.lastrowid or 0)
        row = conn.execute(
            """
            SELECT sequence, job_id, event_json, created_at,
                   producer_id, producer_sequence, occurred_at,
                   observed_at, event_type, phase
            FROM job_events WHERE sequence = ?
            """,
            (sequence,),
        ).fetchone()
        return _job_event_envelope(row)

    @staticmethod
    def _row_to_lease(row: tuple) -> JobLease:
        return JobLease(
            id=row[0],
            provider=row[1],
            instance_id=row[2],
            resource_name=row[3],
            runtime_profile=row[4],
            worker_id=row[5],
            status=row[6],
            created_at=row[7],
            updated_at=row[8],
            expires_at=row[9],
            renewed_at=row[10],
            hourly_rate=row[11],
            max_runtime_seconds=row[12],
            max_cost_usd=row[13],
            runtime_deadline=row[14],
            cost_deadline=row[15],
            revoked_at=row[16],
            termination_requested_at=row[17],
            termination_confirmed_at=row[18],
            termination_attempts=int(row[19] or 0),
            reason=row[20],
            last_error=row[21],
        )

    @staticmethod
    def _lease_select() -> str:
        return (
            "SELECT id, provider, instance_id, resource_name, runtime_profile, "
            "worker_id, status, created_at, updated_at, expires_at, renewed_at, "
            "hourly_rate, max_runtime_seconds, max_cost_usd, runtime_deadline, "
            "cost_deadline, revoked_at, termination_requested_at, "
            "termination_confirmed_at, termination_attempts, reason, last_error "
            "FROM job_leases"
        )

    @staticmethod
    def _attached_job_ids(conn: sqlite3.Connection, lease_id: str) -> list[str]:
        return [
            str(row[0])
            for row in conn.execute(
                "SELECT job_id FROM job_lease_jobs WHERE lease_id = ? ORDER BY attached_at",
                (lease_id,),
            ).fetchall()
        ]

    def _append_lease_event(
        self,
        conn: sqlite3.Connection,
        lease_id: str,
        event: dict[str, Any],
        *,
        observed_at: str,
    ) -> None:
        for job_id in self._attached_job_ids(conn, lease_id):
            self._append_event_in_transaction(
                conn,
                job_id,
                event,
                producer_id="dispatcher:lease-control",
                occurred_at=observed_at,
                observed_at=observed_at,
            )

    def create_lease(
        self,
        *,
        provider: str,
        runtime_profile: str,
        job_ids: list[str] | None = None,
        hourly_rate: float | None = None,
        max_runtime_seconds: int | None = None,
        max_cost_usd: float | None = None,
        ttl_seconds: int = 300,
        lease_id: str | None = None,
    ) -> JobLease:
        """Create durable authority before the provider mutation starts."""
        normalized_provider = str(provider).strip()
        normalized_profile = str(runtime_profile).strip()
        if not normalized_provider or not normalized_profile:
            raise ValueError("A lease requires provider and runtime profile")
        ttl = max(1, int(ttl_seconds))
        now = utc_now()
        now_text = now.isoformat()
        identifier = str(lease_id or uuid.uuid4())
        resource_name = f"cloud-offload-{identifier.replace('-', '')[:16]}"
        rate = float(hourly_rate) if hourly_rate is not None else None
        runtime_limit = (
            max(1, int(max_runtime_seconds))
            if max_runtime_seconds is not None
            else None
        )
        cost_limit = float(max_cost_usd) if max_cost_usd is not None else None
        if rate is not None and rate < 0:
            raise ValueError("Lease hourly rate cannot be negative")
        if cost_limit is not None and cost_limit <= 0:
            raise ValueError("Lease cost limit must be greater than zero")
        if cost_limit is not None and (rate is None or rate <= 0):
            raise ValueError("A lease cost limit requires a positive hourly rate")
        runtime_deadline = (
            (now + timedelta(seconds=runtime_limit)).isoformat()
            if runtime_limit is not None
            else None
        )
        cost_deadline = (
            (now + timedelta(seconds=3600 * cost_limit / rate)).isoformat()
            if cost_limit is not None and rate is not None and rate > 0
            else None
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO job_leases (
                    id, provider, instance_id, resource_name, runtime_profile,
                    worker_id, status, created_at, updated_at, expires_at,
                    renewed_at, hourly_rate, max_runtime_seconds, max_cost_usd,
                    runtime_deadline, cost_deadline, revoked_at,
                    termination_requested_at, termination_confirmed_at,
                    termination_attempts, reason, last_error
                ) VALUES (?, ?, NULL, ?, ?, NULL, 'provisioning', ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          NULL, NULL, NULL, 0, NULL, NULL)
                """,
                (
                    identifier,
                    normalized_provider,
                    resource_name,
                    normalized_profile,
                    now_text,
                    now_text,
                    (now + timedelta(seconds=ttl)).isoformat(),
                    now_text,
                    rate,
                    runtime_limit,
                    cost_limit,
                    runtime_deadline,
                    cost_deadline,
                ),
            )
            for job_id in dict.fromkeys(str(item) for item in (job_ids or [])):
                if not conn.execute(
                    "SELECT 1 FROM jobs WHERE id = ?", (job_id,)
                ).fetchone():
                    raise KeyError(f"Job not found: {job_id}")
                conn.execute(
                    """
                    INSERT OR IGNORE INTO job_lease_jobs (lease_id, job_id, attached_at)
                    VALUES (?, ?, ?)
                    """,
                    (identifier, job_id, now_text),
                )
            self._append_lease_event(
                conn,
                identifier,
                {
                    "type": "lease_created",
                    "phase": "provisioning",
                    "lease_id": identifier,
                    "provider": normalized_provider,
                    "runtime_profile": normalized_profile,
                    "hourly_rate": rate,
                    "runtime_deadline": runtime_deadline,
                    "cost_deadline": cost_deadline,
                },
                observed_at=now_text,
            )
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (identifier,)
            ).fetchone()
        return self._row_to_lease(row)

    def get_lease(self, lease_id: str) -> JobLease | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
        return self._row_to_lease(row) if row else None

    def list_open_leases(self) -> list[JobLease]:
        placeholders = ",".join("?" * len(LEASE_OPEN_STATUSES))
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"{self._lease_select()} WHERE status IN ({placeholders}) ORDER BY created_at",
                LEASE_OPEN_STATUSES,
            ).fetchall()
        return [self._row_to_lease(row) for row in rows]

    def leases_for_job(self, job_id: str, *, open_only: bool = False) -> list[JobLease]:
        clause = ""
        values: list[Any] = [str(job_id)]
        if open_only:
            clause = f" AND lease.status IN ({','.join('?' * len(LEASE_OPEN_STATUSES))})"
            values.extend(LEASE_OPEN_STATUSES)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                self._lease_select().replace(
                    "FROM job_leases",
                    "FROM job_leases AS lease JOIN job_lease_jobs AS link ON link.lease_id = lease.id",
                )
                + " WHERE link.job_id = ?"
                + clause
                + " ORDER BY lease.created_at",
                values,
            ).fetchall()
        return [self._row_to_lease(row) for row in rows]

    def jobs_for_lease(self, lease_id: str) -> list[Job]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT jobs.* FROM jobs
                JOIN job_lease_jobs ON job_lease_jobs.job_id = jobs.id
                WHERE job_lease_jobs.lease_id = ?
                ORDER BY jobs.created_at
                """,
                (str(lease_id),),
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def attach_job_to_lease(self, lease_id: str, job_id: str) -> None:
        now = utc_now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if not conn.execute(
                "SELECT 1 FROM job_leases WHERE id = ?", (str(lease_id),)
            ).fetchone():
                raise KeyError(f"Lease not found: {lease_id}")
            if not conn.execute("SELECT 1 FROM jobs WHERE id = ?", (str(job_id),)).fetchone():
                raise KeyError(f"Job not found: {job_id}")
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO job_lease_jobs (lease_id, job_id, attached_at)
                VALUES (?, ?, ?)
                """,
                (str(lease_id), str(job_id), now),
            )
            if cursor.rowcount:
                self._append_event_in_transaction(
                    conn,
                    str(job_id),
                    {
                        "type": "lease_job_attached",
                        "phase": "provisioning",
                        "lease_id": str(lease_id),
                    },
                    producer_id="coordinator:lease-control",
                    occurred_at=now,
                    observed_at=now,
                )

    def bind_lease(
        self,
        lease_id: str,
        instance_id: str,
        *,
        worker_id: str | None = None,
        ttl_seconds: int = 300,
    ) -> JobLease:
        """Bind the pre-mutation lease to the exact provider resource."""
        now = utc_now()
        now_text = now.isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
            if not row:
                raise KeyError(f"Lease not found: {lease_id}")
            current = self._row_to_lease(row)
            if current.instance_id and current.instance_id != str(instance_id):
                raise ValueError("Lease is already bound to a different provider resource")
            if current.status not in {"provisioning", "active"}:
                return current
            conn.execute(
                """
                UPDATE job_leases SET instance_id = ?, worker_id = COALESCE(worker_id, ?),
                    status = 'active', updated_at = ?, renewed_at = ?, expires_at = ?
                WHERE id = ?
                """,
                (
                    str(instance_id),
                    str(worker_id) if worker_id else None,
                    now_text,
                    now_text,
                    (now + timedelta(seconds=max(1, int(ttl_seconds)))).isoformat(),
                    str(lease_id),
                ),
            )
            self._append_lease_event(
                conn,
                str(lease_id),
                {
                    "type": "lease_bound",
                    "phase": "provisioning",
                    "lease_id": str(lease_id),
                    "provider": current.provider,
                    "worker_instance_id": str(instance_id),
                    "hourly_rate": current.hourly_rate,
                },
                observed_at=now_text,
            )
            updated = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
        return self._row_to_lease(updated)

    def renew_lease(
        self,
        lease_id: str,
        *,
        worker_id: str | None = None,
        ttl_seconds: int = 300,
    ) -> JobLease:
        now = utc_now()
        now_text = now.isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
            if not row:
                raise KeyError(f"Lease not found: {lease_id}")
            lease = self._row_to_lease(row)
            if lease.status != "active":
                return lease
            if lease.worker_id and worker_id and lease.worker_id != str(worker_id):
                raise PermissionError("Lease is bound to a different worker")
            conn.execute(
                """
                UPDATE job_leases SET worker_id = COALESCE(worker_id, ?),
                    updated_at = ?, renewed_at = ?, expires_at = ?
                WHERE id = ? AND status = 'active'
                """,
                (
                    str(worker_id) if worker_id else None,
                    now_text,
                    now_text,
                    (now + timedelta(seconds=max(1, int(ttl_seconds)))).isoformat(),
                    str(lease_id),
                ),
            )
            updated = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
        return self._row_to_lease(updated)

    def request_lease_revocation(self, lease_id: str, reason: str) -> JobLease:
        now = utc_now().isoformat()
        safe_reason = str(reason).strip()[:256] or "revoked"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
            if not row:
                raise KeyError(f"Lease not found: {lease_id}")
            current = self._row_to_lease(row)
            if current.status not in LEASE_OPEN_STATUSES:
                return current
            if current.status not in {"revocation_requested", "terminating"}:
                conn.execute(
                    """
                    UPDATE job_leases SET status = 'revocation_requested',
                        revoked_at = ?, updated_at = ?, expires_at = ?, reason = ?
                    WHERE id = ?
                    """,
                    (now, now, now, safe_reason, str(lease_id)),
                )
                self._append_lease_event(
                    conn,
                    str(lease_id),
                    {
                        "type": "lease_revoked",
                        "phase": "resource_closure",
                        "lease_id": str(lease_id),
                        "provider": current.provider,
                        "worker_instance_id": current.instance_id,
                        "reason": safe_reason,
                    },
                    observed_at=now,
                )
            updated = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
        return self._row_to_lease(updated)

    def request_job_lease_revocation(self, job_id: str, reason: str) -> list[JobLease]:
        leases = self.leases_for_job(job_id, open_only=True)
        return [self.request_lease_revocation(item.id, reason) for item in leases]

    def record_termination_attempt(
        self, lease_id: str, *, error: str | None = None
    ) -> JobLease:
        now = utc_now().isoformat()
        safe_error = str(error)[:256] if error else None
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
            if not row:
                raise KeyError(f"Lease not found: {lease_id}")
            current = self._row_to_lease(row)
            if current.status not in LEASE_OPEN_STATUSES:
                return current
            conn.execute(
                """
                UPDATE job_leases SET status = 'terminating', updated_at = ?,
                    termination_requested_at = COALESCE(termination_requested_at, ?),
                    termination_attempts = termination_attempts + 1,
                    last_error = ? WHERE id = ?
                """,
                (now, now, safe_error, str(lease_id)),
            )
            updated = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
            updated_lease = self._row_to_lease(updated)
            self._append_lease_event(
                conn,
                str(lease_id),
                {
                    "type": "provider_termination_requested",
                    "phase": "resource_closure",
                    "lease_id": str(lease_id),
                    "provider": current.provider,
                    "worker_instance_id": current.instance_id,
                    "attempt": updated_lease.termination_attempts,
                },
                observed_at=now,
            )
        return updated_lease

    def confirm_lease_termination(
        self,
        lease_id: str,
        *,
        observed_state: str,
        provider_absent: bool,
    ) -> JobLease:
        """Persist the provider observation that ends the billing claim."""
        now = utc_now().isoformat()
        state = str(observed_state).strip()[:64] or "absent"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
            if not row:
                raise KeyError(f"Lease not found: {lease_id}")
            current = self._row_to_lease(row)
            if current.status == "closed" and current.termination_confirmed_at:
                return current
            conn.execute(
                """
                UPDATE job_leases SET status = 'closed', updated_at = ?,
                    termination_confirmed_at = ?, expires_at = ?, last_error = NULL
                WHERE id = ?
                """,
                (now, now, now, str(lease_id)),
            )
            self._append_lease_event(
                conn,
                str(lease_id),
                {
                    "type": "provider_termination_completed",
                    "phase": "resource_closure",
                    "lease_id": str(lease_id),
                    "provider": current.provider,
                    "worker_instance_id": current.instance_id,
                    "evidence": {
                        "provider_acknowledged": True,
                        "provider_absent": bool(provider_absent),
                        "observed_state": state,
                        "termination_attempts": current.termination_attempts,
                    },
                },
                observed_at=now,
            )
            updated = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
        return self._row_to_lease(updated)

    def close_unbound_lease(self, lease_id: str, reason: str) -> JobLease:
        """Close a launch intent only after the provider reports no resource."""
        now = utc_now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
            if not row:
                raise KeyError(f"Lease not found: {lease_id}")
            current = self._row_to_lease(row)
            if current.instance_id:
                raise ValueError("A bound lease requires provider termination evidence")
            conn.execute(
                """
                UPDATE job_leases SET status = 'closed', updated_at = ?, expires_at = ?,
                    reason = ? WHERE id = ?
                """,
                (now, now, str(reason)[:256], str(lease_id)),
            )
            self._append_lease_event(
                conn,
                str(lease_id),
                {
                    "type": "lease_closed_without_resource",
                    "phase": "provisioning",
                    "lease_id": str(lease_id),
                    "provider": current.provider,
                    "reason": str(reason)[:256],
                },
                observed_at=now,
            )
            updated = conn.execute(
                f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
            ).fetchone()
        return self._row_to_lease(updated)

    def create(
        self,
        model: str,
        input_path: str,
        params: dict | None = None,
        request: dict | None = None,
        provider: str | None = None,
        status: JobStatus = JobStatus.PENDING,
        job_id: str | None = None,
    ) -> Job:
        """Create a new job."""
        job = Job(
            id=str(job_id or uuid.uuid4()),
            model=model,
            status=status,
            input_path=input_path,
            params=params or {},
            request=request or {},
            provider=provider,
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, model, status, input_path, params,
                    preview_path, result_path, created_at, updated_at,
                    started_at, completed_at, error, worker_id,
                    attempts, max_attempts, schema_version, request_json,
                    provider, result_json, progress
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id,
                    job.model,
                    job.status.value,
                    job.input_path,
                    json.dumps(job.params),
                    job.preview_path,
                    job.result_path,
                    job.created_at,
                    job.updated_at,
                    job.started_at,
                    job.completed_at,
                    job.error,
                    job.worker_id,
                    job.attempts,
                    job.max_attempts,
                    job.schema_version,
                    json.dumps(job.request),
                    job.provider,
                    json.dumps(job.result) if job.result is not None else None,
                    job.progress,
                ),
            )
            self._append_event_in_transaction(
                conn,
                job.id,
                _lifecycle_event(job, "job_created"),
                producer_id="coordinator:job-queue",
                occurred_at=job.created_at,
                observed_at=job.created_at,
            )
        return job

    def submit_plan_atomic(
        self,
        *,
        plan_digest: str,
        preflight_id: str,
        candidate_id: str,
        idempotency_key: str,
        request_digest: str,
        job_id: str,
        plan_public: dict,
        preflight_public: dict,
        request_binding: dict,
        provider_digest: str | None = None,
        candidate_digest: str | None = None,
        input_digest: str | None = None,
        timeout_seconds: int = 3600,
        input_artifacts: dict[str, str] | None = None,
    ) -> tuple[str, bool, dict]:
        """Accept a plan and create its queue job in one SQLite transaction.

        This is deliberately a queue API rather than a route-level sequence:
        both tables and the initial event use this connection and therefore
        commit or roll back together, including on ``BaseException``.  The
        private plan remains in ``cloud_plan_authority``; only its projection
        is written into the public job request.
        """

        from cloud_offload.plan_protocol import (
            PlanError,
            binding_digest,
            _parse_expiry,
            _json_dump,
            _json_load,
            ensure_plan_schema,
            reproject_public_preflight,
            validate_public_plan_summary,
            validate_public_preflight_projection,
        )

        # This method is the last boundary before public queue persistence.
        # The HTTP route validates its inputs, but direct callers must not be
        # able to smuggle a full plan, path, prompt, token, or provider body
        # into a public job row.
        validate_public_plan_summary(plan_public, expected_digest=plan_digest)
        validate_public_preflight_projection(
            preflight_public,
            expected_plan_digest=plan_digest,
            expected_preflight_id=preflight_id,
            expected_candidate_id=candidate_id,
        )
        if not isinstance(idempotency_key, str) or not idempotency_key.strip() or len(idempotency_key) > 128:
            raise PlanError("idempotency key is invalid")
        _parse_expiry(preflight_public["expires_at"])
        if not isinstance(request_binding, dict):
            raise PlanError("request binding is invalid")
        _json_dump(request_binding)
        from cloud_offload.plan_protocol import _digest, _safe_opaque

        _digest(plan_digest, "plan digest")
        _safe_opaque(preflight_id, "preflight id")
        _safe_opaque(candidate_id, "candidate id")
        _digest(request_digest, "request digest")
        for label, value in (
            ("provider digest", provider_digest),
            ("candidate digest", candidate_digest),
            ("input digest", input_digest),
        ):
            if value is not None:
                _digest(value, label)
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= 86400:
            raise PlanError("timeout_seconds is invalid")
        if input_artifacts is not None:
            if not isinstance(input_artifacts, dict):
                raise PlanError("input_artifacts is invalid")
            for name, value in input_artifacts.items():
                _safe_opaque(name, "input artifact name")
                _digest(value, "input artifact digest")

        stored_key = binding_digest(idempotency_key)
        now = utc_now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            ensure_plan_schema(conn)
            existing = conn.execute(
                "SELECT job_id,plan_digest,preflight_json,request_digest,state FROM cloud_plans WHERE idempotency_key = ?",
                (stored_key,),
            ).fetchone()
            if existing:
                if existing[1] != plan_digest or existing[3] != request_digest:
                    raise PlanError("idempotency key conflicts with a different request")
                if existing[0]:
                    return str(existing[0]), True, reproject_public_preflight(
                        _json_load(existing[2], "stored preflight")
                    )

            row = conn.execute(
                "SELECT plan_digest,plan_json,preflight_json,job_id,idempotency_key,request_digest,state FROM cloud_plans WHERE plan_digest = ?",
                (plan_digest,),
            ).fetchone()
            if not row:
                raise PlanError("accepted preflight is required")
            if row[3] is not None:
                if row[4] == stored_key and row[5] == request_digest:
                    return str(row[3]), True, _json_load(row[2], "stored preflight")
                raise PlanError("plan was already submitted with different request data")
            if row[6] != "preflighted":
                raise PlanError("plan authority is not preflighted")
            stored = reproject_public_preflight(
                _json_load(row[2], "stored preflight")
            )
            if stored.get("preflight_id") != preflight_id or stored.get("candidate_id") != candidate_id:
                raise PlanError("accepted preflight binding is invalid")
            if _parse_expiry(stored.get("expires_at")) <= datetime.now(timezone.utc):
                raise PlanError("preflight quote has expired")

            # Construct the queue row while the write lock is held.  The
            # initial lifecycle event is part of the same transaction.
            public_inputs = {
                binding_digest(name): value
                for name, value in (input_artifacts or {}).items()
            }
            job = Job(
                id=str(job_id),
                model="comfyui-plan",
                status=JobStatus.QUEUED,
                input_path="sha256:" + str(plan_digest).removeprefix("sha256:"),
                params={
                    "plan_digest": plan_digest,
                    "preflight_id": preflight_id,
                    "candidate_id": candidate_id,
                    "request_digest": request_digest,
                    "provider_digest": provider_digest,
                },
                request={
                    "kind": "comfy.workflow.plan.v1",
                    "plan": dict(plan_public),
                    "input_artifacts": public_inputs,
                    "timeout_seconds": int(timeout_seconds),
                },
                provider=None,
            )
            conn.execute(
                """
                INSERT INTO jobs (
                    id, model, status, input_path, params,
                    preview_path, result_path, created_at, updated_at,
                    started_at, completed_at, error, worker_id,
                    attempts, max_attempts, schema_version, request_json,
                    provider, result_json, progress
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id, job.model, job.status.value, job.input_path,
                    _json_dump(job.params), job.preview_path, job.result_path,
                    job.created_at, job.updated_at, job.started_at,
                    job.completed_at, job.error, job.worker_id, job.attempts,
                    job.max_attempts, job.schema_version, _json_dump(job.request),
                    job.provider, None, job.progress,
                ),
            )
            self._append_event_in_transaction(
                conn,
                job.id,
                _lifecycle_event(job, "job_created"),
                producer_id="coordinator:job-queue",
                occurred_at=job.created_at,
                observed_at=job.created_at,
            )
            plan_update = conn.execute(
                "UPDATE cloud_plans SET job_id=?,idempotency_key=?,request_digest=?,state='submitting',provider_digest=?,candidate_digest=?,input_digest=?,updated_at=? WHERE plan_digest=? AND state='preflighted' AND job_id IS NULL",
                (job.id, stored_key, request_digest, provider_digest, candidate_digest, input_digest, now, plan_digest),
            )
            if plan_update.rowcount != 1:
                raise PlanError("plan authority changed during submission")
            authority_update = conn.execute(
                "UPDATE cloud_plan_authority SET request_json=?,job_id=? WHERE plan_digest=?",
                (_json_dump(request_binding), job.id, plan_digest),
            )
            if authority_update.rowcount != 1:
                raise PlanError("private plan authority is missing")
        return job.id, False, preflight_public

    def cancel_plan_atomic(self, job_id: str, plan_digest: str, receipt: dict) -> Job | None:
        """Close a plan cancellation and its queue state in one transaction."""

        from cloud_offload.plan_protocol import PlanError, _json_dump, ensure_plan_schema, validate_closure_receipt

        normalized_receipt = validate_closure_receipt(receipt)
        now = utc_now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            ensure_plan_schema(conn)
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            job = self._row_to_job(row)
            if job.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.DEAD_LETTER}:
                return job
            previous_status = job.status
            self._append_event_in_transaction(
                conn,
                job.id,
                {"type": "cancellation_requested", "phase": _status_phase(job.status), "status": job.status.value},
                producer_id="coordinator:job-queue",
                occurred_at=now,
                observed_at=now,
            )
            job.status = JobStatus.FAILED
            # Plan jobs expose only the finite cancelled state; no free-text
            # error is placed in their public queue projection.
            job.error = None
            job.completed_at = now
            job.updated_at = now
            self._write_job(conn, job)
            self._append_event_in_transaction(
                conn,
                job.id,
                _lifecycle_event(job, "job_status_changed", previous_status=previous_status),
                producer_id="coordinator:job-queue",
                occurred_at=now,
                observed_at=now,
            )
            updated = conn.execute(
                "UPDATE cloud_plans SET state='cancelled',closure_json=?,updated_at=? WHERE plan_digest=? AND job_id=? AND state NOT IN ('completed','cancelled','failed','terminal')",
                (_json_dump(normalized_receipt), now, plan_digest, job_id),
            )
            if updated.rowcount != 1:
                raise PlanError("plan cancellation authority is stale")
        return job

    def complete_plan_atomic(
        self,
        job_id: str,
        plan_digest: str,
        result: dict,
        receipt: dict,
    ) -> Job | None:
        """Commit a plan result, queue terminal state, and closure together."""

        from cloud_offload.plan_protocol import (
            PlanError,
            _json_dump,
            ensure_plan_schema,
            validate_closure_receipt,
            validate_result_manifest,
        )

        normalized_result = validate_result_manifest(result, expected_job_id=job_id)
        normalized_receipt = validate_closure_receipt(receipt)
        if normalized_receipt["status"] != "completed":
            raise PlanError("completed plan requires a completed closure receipt")
        now = utc_now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            ensure_plan_schema(conn)
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            authority = conn.execute(
                "SELECT state,job_id FROM cloud_plans WHERE plan_digest = ?",
                (plan_digest,),
            ).fetchone()
            if not authority or authority[1] != job_id:
                raise PlanError("plan completion authority is invalid")
            job = self._row_to_job(row)
            if job.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}:
                return job
            if job.status == JobStatus.COMPLETED:
                if authority[0] not in {"completed", "cancelled", "failed", "terminal"}:
                    conn.execute(
                        "UPDATE cloud_plans SET state='completed',closure_json=?,updated_at=? WHERE plan_digest=? AND job_id=? AND state NOT IN ('cancelled','failed','terminal')",
                        (_json_dump(normalized_receipt), now, plan_digest, job_id),
                    )
                return job
            previous_status = job.status
            job.status = JobStatus.COMPLETED
            job.result = normalized_result
            job.progress = 100
            job.error = None
            job.completed_at = now
            job.updated_at = now
            self._write_job(conn, job)
            self._append_event_in_transaction(
                conn,
                job.id,
                _lifecycle_event(job, "job_status_changed", previous_status=previous_status),
                producer_id="coordinator:job-queue",
                occurred_at=now,
                observed_at=now,
            )
            updated = conn.execute(
                "UPDATE cloud_plans SET state='completed',closure_json=?,updated_at=? WHERE plan_digest=? AND job_id=? AND state NOT IN ('cancelled','failed','terminal')",
                (_json_dump(normalized_receipt), now, plan_digest, job_id),
            )
            if updated.rowcount != 1:
                raise PlanError("plan completion authority is stale")
        return job

    def fail_plan_atomic(
        self,
        job_id: str,
        plan_digest: str,
        receipt: dict | None = None,
    ) -> Job | None:
        """Commit a plan retry/failure and its authority transition together."""

        from cloud_offload.plan_protocol import PlanError, _json_dump, ensure_plan_schema, validate_closure_receipt

        normalized_receipt = validate_closure_receipt(receipt) if receipt is not None else None
        if normalized_receipt is not None and normalized_receipt["status"] != "failed":
            raise PlanError("failed plan requires a failed closure receipt")
        now = utc_now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            ensure_plan_schema(conn)
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            authority = conn.execute(
                "SELECT state,job_id FROM cloud_plans WHERE plan_digest = ?",
                (plan_digest,),
            ).fetchone()
            if not authority or authority[1] != job_id:
                raise PlanError("plan failure authority is invalid")
            job = self._row_to_job(row)
            if job.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.DEAD_LETTER}:
                if job.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER} and normalized_receipt is not None and authority[0] not in {"completed", "cancelled", "failed", "terminal"}:
                    conn.execute(
                        "UPDATE cloud_plans SET state='failed',closure_json=?,updated_at=? WHERE plan_digest=? AND job_id=? AND state NOT IN ('cancelled','completed','terminal')",
                        (_json_dump(normalized_receipt), now, plan_digest, job_id),
                    )
                return job
            previous_status = job.status
            if job.attempts >= job.max_attempts:
                job.status = JobStatus.DEAD_LETTER
                job.completed_at = now
                next_state = "failed"
            else:
                job.status = JobStatus.QUEUED
                job.completed_at = None
                next_state = "submitted"
            job.error = None
            job.worker_id = None
            job.updated_at = now
            self._write_job(conn, job)
            self._append_event_in_transaction(
                conn,
                job.id,
                _lifecycle_event(job, "job_status_changed", previous_status=previous_status),
                producer_id="coordinator:job-queue",
                occurred_at=now,
                observed_at=now,
            )
            if next_state == "failed":
                if normalized_receipt is None:
                    raise PlanError("terminal plan failure requires a closure receipt")
                updated = conn.execute(
                    "UPDATE cloud_plans SET state='failed',closure_json=?,updated_at=? WHERE plan_digest=? AND job_id=? AND state NOT IN ('cancelled','completed','terminal')",
                    (_json_dump(normalized_receipt), now, plan_digest, job_id),
                )
            else:
                updated = conn.execute(
                    "UPDATE cloud_plans SET state='submitted',updated_at=? WHERE plan_digest=? AND job_id=? AND state NOT IN ('cancelled','completed','failed','terminal')",
                    (now, plan_digest, job_id),
                )
            if updated.rowcount != 1:
                raise PlanError("plan failure authority is stale")
        return job

    def get(self, job_id: str) -> Job | None:
        """Get job by ID."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return self._row_to_job(row) if row else None

    def update(self, job: Job) -> None:
        """Update a job and journal a status change in the same transaction."""
        job.updated_at = utc_now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, progress FROM jobs WHERE id = ?", (job.id,)
            ).fetchone()
            previous_status = JobStatus(row[0]) if row else None
            previous_progress = int(row[1] or 0) if row else 0
            self._write_job(conn, job)
            if previous_status is not None and previous_status != job.status:
                self._append_event_in_transaction(
                    conn,
                    job.id,
                    _lifecycle_event(
                        job,
                        "job_status_changed",
                        previous_status=previous_status,
                    ),
                    producer_id="coordinator:job-queue",
                    occurred_at=job.updated_at,
                    observed_at=job.updated_at,
                )
            elif previous_status is not None and previous_progress != job.progress:
                self._append_event_in_transaction(
                    conn,
                    job.id,
                    _progress_event(job, previous_progress),
                    producer_id="coordinator:job-queue",
                    occurred_at=job.updated_at,
                    observed_at=job.updated_at,
                )

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        error: str | None = None,
        **kwargs,
    ) -> Job | None:
        """Update job status and optional fields."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            job = self._row_to_job(row)

            # Terminal states are immutable. In particular, a worker may finish a
            # blocking download just after the user cancels; its late `running`,
            # `complete`, or `fail` callback must not resurrect the job or enqueue
            # another retry behind the user's back.
            if job.status in {
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.DEAD_LETTER,
            }:
                return job

            previous_status = job.status
            previous_progress = int(job.progress or 0)
            job.status = status
            if error is not None:
                job.error = error
            elif status in (JobStatus.RUNNING, JobStatus.COMPLETED):
                job.error = None

            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)

            now = utc_now().isoformat()
            job.updated_at = now
            if status == JobStatus.RUNNING:
                job.started_at = now
            elif status in (
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.DEAD_LETTER,
            ):
                job.completed_at = now
            else:
                job.completed_at = None

            self._write_job(conn, job)
            if previous_status != job.status:
                self._append_event_in_transaction(
                    conn,
                    job.id,
                    _lifecycle_event(
                        job,
                        "job_status_changed",
                        previous_status=previous_status,
                    ),
                    producer_id="coordinator:job-queue",
                    occurred_at=now,
                    observed_at=now,
                )
            elif previous_progress != job.progress:
                self._append_event_in_transaction(
                    conn,
                    job.id,
                    _progress_event(job, previous_progress),
                    producer_id="coordinator:job-queue",
                    occurred_at=now,
                    observed_at=now,
                )
            return job

    def list_by_status(
        self, *statuses: JobStatus, provider: str | None = None
    ) -> list[Job]:
        """Get all jobs with given status(es)."""
        placeholders = ",".join("?" * len(statuses))
        status_values = [s.value for s in statuses]

        provider_clause = " AND provider = ?" if provider else ""
        values: list[Any] = [*status_values]
        if provider:
            values.append(provider)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders})"
                f"{provider_clause} ORDER BY created_at",
                values,
            ).fetchall()
            return [self._row_to_job(row) for row in rows]

    def list_recent(self, *, limit: int = 50, active_only: bool = False) -> list[Job]:
        """Return active jobs first, then the newest terminal jobs."""
        bounded_limit = max(1, min(200, int(limit)))
        terminal = (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.DEAD_LETTER.value,
        )
        where = "WHERE status NOT IN (?, ?, ?)" if active_only else ""
        values: list[Any] = list(terminal) if active_only else []
        values.append(bounded_limit)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM jobs
                {where}
                ORDER BY
                    CASE WHEN status IN (?, ?, ?) THEN 1 ELSE 0 END,
                    updated_at DESC,
                    created_at DESC
                LIMIT ?
                """,
                [*terminal, *values],
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def count_by_status(self, *statuses: JobStatus, provider: str | None = None) -> int:
        """Count jobs with given status(es)."""
        placeholders = ",".join("?" * len(statuses))
        status_values = [s.value for s in statuses]

        provider_clause = " AND provider = ?" if provider else ""
        values: list[Any] = [*status_values]
        if provider:
            values.append(provider)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM jobs WHERE status IN ({placeholders})"
                f"{provider_clause}",
                values,
            ).fetchone()
            return row[0] if row else 0

    def claim_jobs(
        self,
        worker_id: str,
        limit: int = 5,
        token: str | None = None,
        provider: str | None = None,
        models: list[str] | None = None,
        runtime_profile: str | None = None,
        gpu_vram_gb: float | None = None,
        gpu_name: str | None = None,
        cache_volume_id: str | None = None,
        lease_id: str | None = None,
        lease_ttl_seconds: int = 300,
    ) -> list[Job]:
        """
        Atomically claim queued jobs for a worker.
        Returns list of claimed jobs.
        """
        self._verify_worker_token(token)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = None
            now = utc_now()
            now_text = now.isoformat()
            if lease_id:
                lease_row = conn.execute(
                    f"{self._lease_select()} WHERE id = ?", (str(lease_id),)
                ).fetchone()
                if not lease_row:
                    raise PermissionError("Worker lease does not exist")
                lease = self._row_to_lease(lease_row)
                if lease.status != "active":
                    raise PermissionError("Worker lease is not active")
                if provider and lease.provider != provider:
                    raise PermissionError("Worker lease provider does not match")
                if lease.worker_id and lease.worker_id != worker_id:
                    raise PermissionError("Worker lease is bound to a different worker")
                if datetime.fromisoformat(lease.expires_at) <= now:
                    raise PermissionError("Worker lease expired")
                conn.execute(
                    """
                    UPDATE job_leases SET worker_id = COALESCE(worker_id, ?),
                        updated_at = ?, renewed_at = ?, expires_at = ?
                    WHERE id = ? AND status = 'active'
                    """,
                    (
                        worker_id,
                        now_text,
                        now_text,
                        (
                            now
                            + timedelta(seconds=max(1, int(lease_ttl_seconds)))
                        ).isoformat(),
                        str(lease_id),
                    ),
                )
            # Get job IDs to claim
            provider_clause = ""
            models_clause = ""
            gpu_clause = ""
            cache_clause = ""
            values: list[Any] = [JobStatus.QUEUED.value]
            if provider:
                # Plan jobs keep the provider name out of the queue row.  Use
                # the private binding digest for provider-scoped claiming.
                from cloud_offload.plan_protocol import binding_digest

                provider_clause = " AND (provider = ? OR (model = 'comfyui-plan' AND provider IS NULL AND json_extract(params, '$.provider_digest') = ?))"
                values.extend((provider, binding_digest(provider)))
            if models is not None:
                if not models:
                    return []
                model_placeholders = ",".join("?" * len(models))
                models_clause = f" AND model IN ({model_placeholders})"
                values.extend(models)
            if gpu_vram_gb is not None:
                gpu_clause += (
                    " AND COALESCE(CAST(json_extract(params, '$.min_gpu_ram_gb') "
                    "AS REAL), 0) <= ?"
                )
                # A requirement is typed in the size a card is sold as, while a
                # driver reports what it can actually address: an A5000 sold as
                # 24 GB reports 24564 MiB, or 23.99 GiB. Comparing those raw made
                # a worker refuse every job its own GPU was rented to run, and
                # the job simply waited. Round to the nearest whole GiB so both
                # sides speak the same units.
                values.append(round(max(0.0, float(gpu_vram_gb))))
            if gpu_name:
                # Treat "any"/missing as unconstrained. Normalizing separators makes
                # provider labels such as RTX_4090 match NVIDIA GeForce RTX 4090.
                conn.create_function("canonical_gpu_name", 1, _canonical_gpu_name, deterministic=True)
                gpu_clause += """
                    AND (
                        COALESCE(lower(json_extract(params, '$.gpu_type')), 'any') = 'any'
                        OR canonical_gpu_name(?)
                           LIKE '%' || canonical_gpu_name(
                               json_extract(params, '$.gpu_type')
                           ) || '%'
                    )
                """
                values.append(str(gpu_name))
            if cache_volume_id is not None:
                cache_clause = """
                    AND (
                        json_extract(params, '$.preflight.prepared_volume_id') IS NULL
                        OR COALESCE(
                            json_extract(params, '$.preflight.prepared_volume_id'), ''
                        ) = ?
                    )
                """
                values.append(str(cache_volume_id))
            values.append(limit)
            rows = conn.execute(
                f"""
                SELECT id, model FROM jobs
                WHERE status = ?
                {provider_clause}
                {models_clause}
                {gpu_clause}
                {cache_clause}
                ORDER BY created_at
                LIMIT ?
                """,
                values,
            ).fetchall()

            job_ids = [row[0] for row in rows]
            if not job_ids:
                return []
            if any(row[1] == "comfyui-plan" for row in rows):
                from cloud_offload.plan_protocol import ensure_plan_schema

                ensure_plan_schema(conn)

            # Claim them atomically
            claimed_at = utc_now().isoformat()
            placeholders = ",".join("?" * len(job_ids))
            conn.execute(
                f"""
                UPDATE jobs SET
                    status = ?,
                    worker_id = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id IN ({placeholders})
                """,
                [JobStatus.DISPATCHED.value, worker_id, claimed_at] + job_ids,
            )
            claimed = []
            for job_id in job_ids:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()
                if not row:
                    continue
                job = self._row_to_job(row)
                if lease is not None:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO job_lease_jobs (lease_id, job_id, attached_at)
                        VALUES (?, ?, ?)
                        """,
                        (lease.id, job.id, claimed_at),
                    )
                self._append_event_in_transaction(
                    conn,
                    job.id,
                    _lifecycle_event(
                        job,
                        "job_status_changed",
                        previous_status=JobStatus.QUEUED,
                    ),
                    producer_id="coordinator:job-queue",
                    occurred_at=claimed_at,
                    observed_at=claimed_at,
                )
                if job.model == "comfyui-plan" and isinstance(job.params, dict):
                    plan_digest = job.params.get("plan_digest")
                    if isinstance(plan_digest, str):
                        conn.execute(
                            "UPDATE cloud_plans SET state='submitted',updated_at=? WHERE plan_digest=? AND job_id=? AND state='submitting'",
                            (claimed_at, plan_digest, job.id),
                        )
                if lease is not None:
                    self._append_event_in_transaction(
                        conn,
                        job.id,
                        {
                            "type": "lease_job_claimed",
                            "phase": "worker_boot",
                            "lease_id": lease.id,
                            "provider": lease.provider,
                            "worker_instance_id": lease.instance_id,
                            "worker_id": worker_id,
                        },
                        producer_id="coordinator:lease-control",
                        occurred_at=claimed_at,
                        observed_at=claimed_at,
                    )
                claimed.append(job)

        return claimed

    def authorize_worker(self, token: str | None) -> None:
        """Verify a worker credential for coordinator operations."""
        if not self._get_meta("worker_token_sha256"):
            raise PermissionError("Worker authentication is not configured")
        self._verify_worker_token(token)

    def authorize_worker_job(
        self,
        job_id: str,
        *,
        worker_id: str | None,
        lease_id: str | None,
        lease_ttl_seconds: int = 300,
    ) -> Job:
        """Bind a worker callback to its claimed job and live resource lease."""
        job = self.get(job_id)
        if not job:
            raise KeyError(f"Job not found: {job_id}")
        leases = self.leases_for_job(job_id, open_only=True)
        if not leases:
            if self.leases_for_job(job_id):
                raise PermissionError("Worker lease for this job is closed")
            # Local and pre-M3 workers have no lease. Keep that established path.
            return job
        if not worker_id or job.worker_id != str(worker_id):
            raise PermissionError("Worker identity does not match the claimed job")
        lease = next((item for item in leases if item.id == str(lease_id or "")), None)
        if lease is None:
            raise PermissionError("Worker lease does not match the claimed job")
        if lease.status in {"revocation_requested", "terminating"}:
            return job
        if datetime.fromisoformat(lease.expires_at) <= utc_now():
            raise PermissionError("Worker lease expired")
        renewed = self.renew_lease(
            lease.id,
            worker_id=str(worker_id),
            ttl_seconds=lease_ttl_seconds,
        )
        if renewed.status != "active":
            raise PermissionError("Worker lease is not active")
        return job

    def set_progress(self, job_id: str, progress: int) -> Job | None:
        """Update bounded job progress."""
        job = self.get(job_id)
        if not job:
            return None
        if job.status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.DEAD_LETTER,
        }:
            return job
        job.progress = max(0, min(100, int(progress)))
        self.update(job)
        return job

    def append_event(
        self,
        job_id: str,
        event: dict[str, Any],
        *,
        producer_id: str | None = None,
        producer_sequence: int | None = None,
        occurred_at: str | None = None,
    ) -> dict[str, Any]:
        """Append an immutable, resumable, idempotent execution event."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if not conn.execute(
                "SELECT 1 FROM jobs WHERE id = ?", (job_id,)
            ).fetchone():
                raise KeyError(f"Job not found: {job_id}")
            return self._append_event_in_transaction(
                conn,
                job_id,
                event,
                producer_id=producer_id,
                producer_sequence=producer_sequence,
                occurred_at=occurred_at,
            )

    def list_events(
        self, job_id: str, *, after: int = 0, limit: int = 250
    ) -> list[dict[str, Any]]:
        """Read an ordered event page, allowing clients to resume after disconnects."""
        bounded_limit = max(1, min(1000, int(limit)))
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT sequence, job_id, event_json, created_at,
                       producer_id, producer_sequence, occurred_at,
                       observed_at, event_type, phase
                FROM job_events
                WHERE job_id = ? AND sequence > ?
                ORDER BY sequence
                LIMIT ?
                """,
                (job_id, max(0, int(after)), bounded_limit),
            ).fetchall()
        return [_job_event_envelope(row) for row in rows]

    def list_recent_events(
        self, job_id: str, *, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Read the newest bounded event window in chronological order."""
        bounded_limit = max(1, min(1000, int(limit)))
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT sequence, job_id, event_json, created_at,
                       producer_id, producer_sequence, occurred_at,
                       observed_at, event_type, phase
                FROM job_events
                WHERE job_id = ?
                ORDER BY sequence DESC
                LIMIT ?
                """,
                (job_id, bounded_limit),
            ).fetchall()
        return [_job_event_envelope(row) for row in reversed(rows)]

    def event_bounds(self, job_id: str) -> tuple[int, int]:
        """Return total event count and the latest resumable cursor."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(sequence), 0)
                FROM job_events WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)

    def event_snapshot(self, job_id: str) -> dict[str, Any] | None:
        """Project current replay state without making the client scan history."""
        job = self.get(job_id)
        if not job:
            return None
        with sqlite3.connect(self.db_path) as conn:
            stats = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(sequence), 0),
                       COALESCE(MAX(
                           CASE
                               WHEN json_type(event_json, '$.overall_progress')
                                    IN ('integer', 'real')
                               THEN CAST(
                                   json_extract(event_json, '$.overall_progress')
                                   AS INTEGER
                               )
                               ELSE 0
                           END
                       ), 0)
                FROM job_events WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
            latest = conn.execute(
                """
                SELECT sequence, job_id, event_json, created_at,
                       producer_id, producer_sequence, occurred_at,
                       observed_at, event_type, phase
                FROM job_events
                WHERE job_id = ?
                ORDER BY sequence DESC LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            lifecycle_row = conn.execute(
                """
                SELECT sequence, event_json, observed_at FROM job_events
                WHERE job_id = ?
                  AND event_type IN (?, ?, ?)
                  AND json_extract(event_json, '$.status') IS NOT NULL
                ORDER BY sequence DESC LIMIT 1
                """,
                (job_id, *JOB_LIFECYCLE_EVENT_TYPES),
            ).fetchone()
            phase_rows = conn.execute(
                """
                SELECT phase, sequence FROM job_events
                WHERE job_id = ? AND phase IS NOT NULL
                """,
                (job_id,),
            ).fetchall()
        event_count, event_cursor, event_progress = stats
        journal_status = None
        if lifecycle_row:
            candidate = json.loads(lifecycle_row[1]).get("status")
            if candidate in {item.value for item in JobStatus}:
                journal_status = str(candidate)
        status = journal_status or job.status.value
        progress = (
            int(event_progress or 0) if journal_status else int(job.progress or 0)
        )
        if status == JobStatus.COMPLETED.value:
            progress = 100
        ranked_phases = [
            (JOB_PHASE_ORDER[phase], int(sequence), phase)
            for phase, sequence in phase_rows
            if phase in JOB_PHASE_ORDER
        ]
        lifecycle_phase = (
            max(ranked_phases)[2]
            if ranked_phases
            else (latest[9] if latest and latest[9] else status)
        )
        result = {
            "schema": "cloud-offload.job-snapshot.v1",
            "job": job.to_dict(),
            "status": status,
            "state_source": "journal" if journal_status else "job_row",
            "lifecycle_phase": lifecycle_phase,
            "progress": max(0, min(100, progress)),
            "event_cursor": int(event_cursor),
            "event_count": int(event_count),
            "last_event": _job_event_envelope(latest) if latest else None,
            "updated_at": lifecycle_row[2] if lifecycle_row else job.updated_at,
        }
        if job.model == "comfyui-plan" and isinstance(job.params, dict):
            from cloud_offload.plan_protocol import PlanProtocolStore, public_plan_event, public_plan_job

            digest = job.params.get("plan_digest")
            authority = PlanProtocolStore(str(self.db_path)).get(str(digest)) if digest else None
            state = str(authority.get("state")) if authority else str(result["status"])
            if state not in {"preflighted", "submitting", "submitted", "running", "cancelling", "cancelled", "completed", "failed", "terminal"}:
                state = "terminal"
            result["job"] = public_plan_job(
                job,
                state=state,
                closure=authority.get("closure") if authority else None,
            )
            result["status"] = state
            result["state_source"] = "plan_authority"
            last_event = result.get("last_event")
            if isinstance(last_event, dict):
                result["last_event"] = public_plan_event(last_event)
        return result

    def complete_job(self, job_id: str, result: dict) -> Job | None:
        """Store a completed worker result."""
        completed = self.update_status(
            job_id,
            JobStatus.COMPLETED,
            result=result,
            progress=100,
        )
        cache_key = (
            completed.params.get("partition_cache_key")
            if completed and completed.status == JobStatus.COMPLETED
            else None
        )
        if cache_key:
            self.put_partition_cache(str(cache_key), result)
        return completed

    def get_partition_cache(self, cache_key: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT result_json FROM partition_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def put_partition_cache(self, cache_key: str, result: dict[str, Any]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO partition_cache (cache_key, result_json, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    result_json = excluded.result_json,
                    created_at = excluded.created_at
                """,
                (cache_key, json.dumps(result), utc_now().isoformat()),
            )

    def record_worker(
        self,
        worker_id: str,
        provider: str,
        status: str = "active",
        runtime_profile: str | None = None,
        capabilities: list[str] | None = None,
        idle: bool = False,
        detail: str | None = None,
        lease_id: str | None = None,
        lease_ttl_seconds: int = 300,
    ) -> None:
        """Record a worker heartbeat, or its own report of what went wrong.

        ``detail`` is where a runner that never got as far as a job leaves its
        reason. A container that dies takes its logs with it, so the row is the
        only place the answer can survive.
        """
        if lease_id:
            renewed = self.renew_lease(
                str(lease_id),
                worker_id=str(worker_id),
                ttl_seconds=lease_ttl_seconds,
            )
            if renewed.status != "active":
                raise PermissionError("Worker lease is not active")
        now = utc_now().isoformat()
        idle_since = now if idle else None
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO workers (
                    worker_id, provider, status, last_seen,
                    idle_since, runtime_profile, capabilities, detail
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    provider = excluded.provider,
                    status = excluded.status,
                    last_seen = excluded.last_seen,
                    idle_since = CASE
                        WHEN excluded.idle_since IS NULL THEN NULL
                        ELSE COALESCE(workers.idle_since, excluded.idle_since)
                    END,
                    runtime_profile = excluded.runtime_profile,
                    capabilities = excluded.capabilities,
                    detail = excluded.detail
                """,
                (
                    worker_id,
                    provider,
                    status,
                    now,
                    idle_since,
                    runtime_profile,
                    json.dumps(capabilities or []),
                    detail,
                ),
            )

    def list_active_workers(self, max_age_seconds: int = 90) -> list[dict]:
        """Return workers that have sent a recent heartbeat and are still alive.

        A worker that has registered as ``starting`` counts: it is a pod already
        being paid for, coming up for this profile, and treating it as absent is
        what makes a dispatcher rent a second one for the same queue.
        """
        return self._list_workers(max_age_seconds, LIVE_WORKER_STATUSES)

    def list_recent_workers(self, max_age_seconds: int = 900) -> list[dict]:
        """Return every worker seen recently, including ones that failed to start."""
        return self._list_workers(max_age_seconds, None)

    def _list_workers(
        self, max_age_seconds: int, statuses: tuple[str, ...] | None
    ) -> list[dict]:
        cutoff = (utc_now() - timedelta(seconds=max_age_seconds)).isoformat()
        clause = ""
        parameters: list[Any] = [cutoff]
        if statuses:
            clause = f" AND status IN ({','.join('?' * len(statuses))})"
            parameters.extend(statuses)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT worker_id, provider, status, last_seen, idle_since,
                       runtime_profile, capabilities, detail
                FROM workers
                WHERE last_seen >= ?{clause}
                ORDER BY provider, worker_id
                """,
                parameters,
            ).fetchall()
        now = utc_now()
        return [
            {
                "worker_id": row[0],
                "provider": row[1],
                "status": row[2],
                "last_seen": row[3],
                "idle_since": row[4],
                "idle_seconds": max(
                    0,
                    int((now - datetime.fromisoformat(row[4])).total_seconds()),
                )
                if row[4]
                else 0,
                "runtime_profile": row[5],
                "capabilities": json.loads(row[6]) if row[6] else [],
                "detail": row[7],
            }
            for row in rows
        ]

    def fail_job(self, job_id: str, error: str) -> Job | None:
        """Record a worker failure, requeueing until retry attempts are exhausted."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            job = self._row_to_job(row)
            if job.status in {
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.DEAD_LETTER,
            }:
                return job

            previous_status = job.status
            job.error = error
            job.worker_id = None
            now = utc_now().isoformat()
            if job.attempts >= job.max_attempts:
                job.status = JobStatus.DEAD_LETTER
                job.completed_at = now
            else:
                job.status = JobStatus.QUEUED
                job.completed_at = None
            job.updated_at = now
            self._write_job(conn, job)
            if previous_status != job.status:
                self._append_event_in_transaction(
                    conn,
                    job.id,
                    _lifecycle_event(
                        job,
                        "job_status_changed",
                        previous_status=previous_status,
                    ),
                    producer_id="coordinator:job-queue",
                    occurred_at=now,
                    observed_at=now,
                )
            return job

    def delete(self, job_id: str) -> bool:
        """Delete a job."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            return cursor.rowcount > 0

    def cleanup_old(self, days: int = 7) -> int:
        """Delete terminal jobs older than N days."""
        cutoff = (utc_now() - timedelta(days=days)).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM jobs
                WHERE status IN (?, ?, ?)
                AND completed_at < ?
                """,
                (
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.DEAD_LETTER.value,
                    cutoff,
                ),
            )
            return cursor.rowcount
