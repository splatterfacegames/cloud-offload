"""Continuous Milestone 7 production release controller.

The controller keeps private benchmark plans and full scorecards local. Its
durable ledger contains only bounded release receipts and content digests.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cloud_offload.benchmark import (
    BenchmarkPlan,
    BenchmarkRunner,
    CoordinatorBenchmarkDriver,
    SCORECARD_SCHEMA,
    _ResourceMeter,
    write_scorecard,
)
from cloud_offload.config import (
    CloudConfig,
    estimate_runpod_storage_monthly,
)
from cloud_offload.profiles import configured_worker_profiles


RELEASE_PLAN_SCHEMA = "cloud-offload.release-plan.v1"
RELEASE_LEDGER_SCHEMA = "cloud-offload.release-ledger.v1"
RELEASE_MATRIX_SCHEMA = "cloud-offload.release-matrix-receipt.v1"
REQUIRED_CONSECUTIVE_MATRICES = 30
REQUIRED_FAILURE_KINDS = frozenset(
    {"cancellation", "provider", "storage", "corruption", "restart"}
)
REQUIRED_CANARIES = frozenset(
    {
        "cold",
        "hot",
        *REQUIRED_FAILURE_KINDS,
        "stale_manifest",
        "regional_fallback",
    }
)
SAFE_CAMPAIGN_STOP_REASONS = frozenset(
    {
        "campaign_runtime_limit",
        "campaign_cost_limit",
        "fresh_worker_quiescence_timeout",
        "orphan_cleanup_failed",
        "scenario_cost_limit",
        "scenario_timeout",
        "runner_readiness_timeout",
        "runner_readiness_cost_limit",
        "unexpected_scenario_failure",
        "operator_interrupt:KeyboardInterrupt",
        "operator_interrupt:SystemExit",
    }
)
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40,64}$")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")


CONTRACT_TEST_GROUPS: dict[str, tuple[str, ...]] = {
    "reload_reconnect_event_order": (
        "tests/test_cloud_queue.py::test_job_events_are_ordered_and_resumable",
        "tests/test_cloud_queue.py::test_lifecycle_journal_is_authoritative_across_reload_and_row_drift",
        "tests/test_cloud_queue.py::test_reordered_worker_events_cannot_regress_snapshot_phase_or_progress",
    ),
    "deterministic_preflight": (
        "tests/test_preflight.py::test_deterministic_blocker_stops_before_provider_read",
        "tests/test_preflight.py::test_request_cannot_loosen_configured_cost_or_region_limits",
        "tests/test_preflight.py::test_request_region_limit_queries_cold_stock_in_that_region",
        "tests/test_preflight.py::test_price_change_returns_revised_preflight_without_queueing",
    ),
    "cache_recovery": (
        "tests/test_prepared_storage.py::test_exact_manifest_id_fetches_from_authority_when_mount_is_stale",
        "tests/test_prepared_storage.py::test_corrupt_profile_weight_is_quarantined_and_falls_back",
    ),
    "regional_fallback": (
        "tests/test_preflight.py::test_preflight_uses_two_compatible_replicas_and_keeps_cold_fallback",
        "tests/test_job_leases.py::test_japan_region_reaches_catalog_and_cold_launch",
        "tests/test_job_leases.py::test_conflicting_confirmed_region_refuses_launch_before_lease",
        "tests/test_prepared_storage.py::test_smart_launch_capacity_race_immediately_retries_cold",
    ),
    "redacted_support": (
        "tests/test_support_bundle.py::test_support_bundle_keeps_evidence_and_removes_payloads_and_secrets",
    ),
    "gpu_and_storage_budgets": (
        "tests/test_job_leases.py::test_reconciliation_enforces_runtime_and_dollar_circuit_breakers",
        "tests/test_regional_replication.py::test_replication_policy_is_bounded_and_automatic_requires_budget",
        "tests/test_regional_replication.py::test_replica_target_claim_is_single_flight_and_budgeted",
    ),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _bytes_digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _positive(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{label} must be finite and positive")
    return number


def _simple_name(value: Any, label: str) -> str:
    name = str(value or "").strip()
    if not _SAFE_NAME.fullmatch(name):
        raise ValueError(f"{label} must be a simple non-empty name")
    return name


@dataclass(frozen=True)
class ReleaseRepository:
    name: str
    path: Path
    revision: str

    @classmethod
    def from_dict(
        cls, value: dict[str, Any], index: int, *, base_dir: Path
    ) -> "ReleaseRepository":
        name = _simple_name(value.get("name"), f"repositories[{index}].name")
        revision = str(value.get("revision") or "").strip().lower()
        if not _REVISION.fullmatch(revision):
            raise ValueError(
                f"repositories[{index}].revision must be a full Git revision"
            )
        raw_path = str(value.get("path") or "").strip()
        if not raw_path:
            raise ValueError(f"repositories[{index}].path is required")
        path = (base_dir / raw_path).resolve() if not Path(raw_path).is_absolute() else Path(raw_path).resolve()
        return cls(name=name, path=path, revision=revision)


@dataclass(frozen=True)
class ReleaseLimits:
    max_total_cost_usd: float
    max_matrix_cost_usd: float
    max_total_seconds: float
    max_matrix_seconds: float
    contract_test_timeout_seconds: float
    cancellation_slo_seconds: float
    provider_closure_slo_seconds: float
    reload_slo_seconds: float
    hot_preparation_ratio_max: float
    max_monthly_storage_cost_usd: float

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ReleaseLimits":
        result = cls(
            max_total_cost_usd=_positive(
                value.get("max_total_cost_usd"), "limits.max_total_cost_usd"
            ),
            max_matrix_cost_usd=_positive(
                value.get("max_matrix_cost_usd"), "limits.max_matrix_cost_usd"
            ),
            max_total_seconds=_positive(
                value.get("max_total_seconds"), "limits.max_total_seconds"
            ),
            max_matrix_seconds=_positive(
                value.get("max_matrix_seconds"), "limits.max_matrix_seconds"
            ),
            contract_test_timeout_seconds=_positive(
                value.get("contract_test_timeout_seconds", 300),
                "limits.contract_test_timeout_seconds",
            ),
            cancellation_slo_seconds=_positive(
                value.get("cancellation_slo_seconds"),
                "limits.cancellation_slo_seconds",
            ),
            provider_closure_slo_seconds=_positive(
                value.get("provider_closure_slo_seconds"),
                "limits.provider_closure_slo_seconds",
            ),
            reload_slo_seconds=_positive(
                value.get("reload_slo_seconds", 2), "limits.reload_slo_seconds"
            ),
            hot_preparation_ratio_max=_positive(
                value.get("hot_preparation_ratio_max", 0.25),
                "limits.hot_preparation_ratio_max",
            ),
            max_monthly_storage_cost_usd=_positive(
                value.get("max_monthly_storage_cost_usd"),
                "limits.max_monthly_storage_cost_usd",
            ),
        )
        if result.max_matrix_cost_usd > result.max_total_cost_usd:
            raise ValueError("matrix cost limit cannot exceed total cost limit")
        if result.max_matrix_seconds > result.max_total_seconds:
            raise ValueError("matrix time limit cannot exceed total time limit")
        if result.reload_slo_seconds > 2:
            raise ValueError("M7 reload SLO cannot exceed two seconds")
        if result.hot_preparation_ratio_max > 0.25:
            raise ValueError("M7 hot preparation ratio cannot exceed 0.25")
        return result


@dataclass(frozen=True)
class ReleaseProfile:
    name: str
    image_digest: str

    @classmethod
    def from_dict(cls, value: dict[str, Any], index: int) -> "ReleaseProfile":
        name = _simple_name(value.get("name"), f"profiles[{index}].name")
        digest = str(value.get("image_digest") or "").strip().lower()
        if not _DIGEST.fullmatch(digest):
            raise ValueError(f"profiles[{index}].image_digest must be sha256")
        return cls(name=name, image_digest=digest)


@dataclass(frozen=True)
class ReleaseCase:
    name: str
    profile: str
    image_digest: str
    region: str
    benchmark_plan_path: Path
    benchmark_plan_digest: str
    benchmark_plan: BenchmarkPlan

    @classmethod
    def from_dict(
        cls,
        value: dict[str, Any],
        index: int,
        *,
        base_dir: Path,
        profiles: dict[str, ReleaseProfile],
        regions: set[str],
        benchmark_snapshot: tuple[bytes, str] | None = None,
    ) -> "ReleaseCase":
        name = _simple_name(value.get("name"), f"cases[{index}].name")
        profile_name = _simple_name(
            value.get("profile"), f"cases[{index}].profile"
        )
        if profile_name not in profiles:
            raise ValueError(f"cases[{index}] uses an undeclared profile")
        region = _simple_name(value.get("region"), f"cases[{index}].region")
        if region not in regions:
            raise ValueError(f"cases[{index}] uses an undeclared region")
        raw_path = str(value.get("benchmark_plan") or "").strip()
        if not raw_path:
            raise ValueError(f"cases[{index}].benchmark_plan is required")
        path = (base_dir / raw_path).resolve() if not Path(raw_path).is_absolute() else Path(raw_path).resolve()
        if benchmark_snapshot is None:
            benchmark_raw = path.read_bytes()
            benchmark_digest = _bytes_digest(benchmark_raw)
        else:
            benchmark_raw, benchmark_digest = benchmark_snapshot
        benchmark = BenchmarkPlan.from_bytes(benchmark_raw)
        failure_kinds = {
            item.failure.kind for item in benchmark.scenarios if item.failure
        }
        cache_states = {item.cache_state for item in benchmark.scenarios}
        missing_failures = REQUIRED_FAILURE_KINDS - failure_kinds
        if missing_failures or not {"cold", "hot"}.issubset(cache_states):
            missing = sorted(missing_failures | ({"cold", "hot"} - cache_states))
            raise ValueError(
                f"cases[{index}] is not a full canary matrix; missing: "
                + ", ".join(missing)
            )
        if any(not item.fresh_instance for item in benchmark.scenarios):
            raise ValueError(f"cases[{index}] requires fresh instances")
        if any(item.allowed_regions != (region,) for item in benchmark.scenarios):
            raise ValueError(
                f"cases[{index}] must bind every scenario to region {region}"
            )
        return cls(
            name=name,
            profile=profile_name,
            image_digest=profiles[profile_name].image_digest,
            region=region,
            benchmark_plan_path=path,
            benchmark_plan_digest=benchmark_digest,
            benchmark_plan=benchmark,
        )

    def safe_summary(self) -> dict[str, Any]:
        benchmark_summary = self.benchmark_plan.safe_summary()
        return {
            "name": self.name,
            "profile": self.profile,
            "image_digest": self.image_digest,
            "region": self.region,
            "benchmark_plan_digest": self.benchmark_plan_digest,
            # A corruption plan receives a fresh safe nonce each time it loads.
            # The immutable file digest binds the private requests. Keep only
            # stable scenario structure here so the same release can resume.
            "benchmark_structure": {
                "schema": benchmark_summary["schema"],
                "providers": benchmark_summary["providers"],
                "exclusive": benchmark_summary["exclusive"],
                "limits": benchmark_summary["limits"],
                "scenarios": [
                    {
                        key: value
                        for key, value in item.items()
                        if key != "request_digest"
                    }
                    for item in benchmark_summary["scenarios"]
                ],
            },
        }


@dataclass(frozen=True)
class ReleasePlan:
    required_consecutive_matrices: int
    repositories: tuple[ReleaseRepository, ...]
    profiles: tuple[ReleaseProfile, ...]
    regions: tuple[str, ...]
    cases: tuple[ReleaseCase, ...]
    limits: ReleaseLimits
    plan_path: Path
    source_digest: str

    @classmethod
    def load(cls, path: str | Path) -> "ReleasePlan":
        plan_path = Path(path).resolve()
        raw_plan = plan_path.read_bytes()
        value = json.loads(raw_plan)
        if not isinstance(value, dict) or value.get("schema") != RELEASE_PLAN_SCHEMA:
            raise ValueError(f"release plan schema must be {RELEASE_PLAN_SCHEMA}")
        required = int(value.get("required_consecutive_matrices") or 0)
        if required < REQUIRED_CONSECUTIVE_MATRICES:
            raise ValueError("M7 requires at least 30 consecutive matrices")
        raw_repositories = value.get("repositories") or []
        if not isinstance(raw_repositories, list) or len(raw_repositories) < 2:
            raise ValueError("release plan needs backend and extension repositories")
        repositories = tuple(
            ReleaseRepository.from_dict(item, index, base_dir=plan_path.parent)
            for index, item in enumerate(raw_repositories)
            if isinstance(item, dict)
        )
        if len(repositories) != len(raw_repositories):
            raise ValueError("every repository must be an object")
        repository_names = {item.name for item in repositories}
        if not {"backend", "extension"}.issubset(repository_names):
            raise ValueError("repositories must include backend and extension")
        if len(repository_names) != len(repositories):
            raise ValueError("repository names must be unique")
        raw_profiles = value.get("profiles") or []
        if not isinstance(raw_profiles, list) or not raw_profiles:
            raise ValueError("profiles must be a non-empty list")
        profiles = tuple(
            ReleaseProfile.from_dict(item, index)
            for index, item in enumerate(raw_profiles)
            if isinstance(item, dict)
        )
        if len(profiles) != len(raw_profiles):
            raise ValueError("every profile must be an object")
        profile_map = {item.name: item for item in profiles}
        if len(profile_map) != len(profiles):
            raise ValueError("profile names must be unique")
        raw_regions = value.get("regions") or []
        if not isinstance(raw_regions, list) or not raw_regions:
            raise ValueError("regions must be a non-empty list")
        regions = tuple(
            _simple_name(item, f"regions[{index}]")
            for index, item in enumerate(raw_regions)
        )
        if len(set(regions)) != len(regions):
            raise ValueError("regions must be unique")
        raw_cases = value.get("cases") or []
        if not isinstance(raw_cases, list) or not raw_cases:
            raise ValueError("cases must be a non-empty list")
        cases_list = []
        for index, item in enumerate(raw_cases):
            if not isinstance(item, dict):
                continue
            raw_path = str(item.get("benchmark_plan") or "").strip()
            benchmark_path = (
                (plan_path.parent / raw_path).resolve()
                if not Path(raw_path).is_absolute()
                else Path(raw_path).resolve()
            )
            benchmark_raw = benchmark_path.read_bytes()
            cases_list.append(
                ReleaseCase.from_dict(
                    item,
                    index,
                    base_dir=plan_path.parent,
                    profiles=profile_map,
                    regions=set(regions),
                    benchmark_snapshot=(benchmark_raw, _bytes_digest(benchmark_raw)),
                )
            )
        cases = tuple(cases_list)
        if len(cases) != len(raw_cases):
            raise ValueError("every case must be an object")
        case_names = {item.name for item in cases}
        if len(case_names) != len(cases):
            raise ValueError("case names must be unique")
        expected_axes = {(profile.name, region) for profile in profiles for region in regions}
        actual_axes = {(case.profile, case.region) for case in cases}
        if actual_axes != expected_axes or len(actual_axes) != len(cases):
            raise ValueError("cases must cover each declared profile-region pair once")
        if len(cases) > required:
            raise ValueError(
                "more cases than required consecutive matrices; the trailing window "
                "could never cover every case"
            )
        limits_value = value.get("limits")
        if not isinstance(limits_value, dict):
            raise ValueError("limits must be an object")
        limits = ReleaseLimits.from_dict(limits_value)
        if any(
            case.benchmark_plan.limits.max_total_cost_usd
            > limits.max_matrix_cost_usd
            for case in cases
        ):
            raise ValueError("a case campaign cost limit exceeds the matrix limit")
        if any(
            case.benchmark_plan.limits.max_campaign_seconds
            + limits.contract_test_timeout_seconds
            > limits.max_matrix_seconds
            for case in cases
        ):
            raise ValueError("a case can exceed the matrix time limit")
        return cls(
            required_consecutive_matrices=required,
            repositories=repositories,
            profiles=profiles,
            regions=regions,
            cases=cases,
            limits=limits,
            plan_path=plan_path,
            source_digest=_bytes_digest(raw_plan),
        )

    def safe_summary(self) -> dict[str, Any]:
        return {
            "schema": RELEASE_PLAN_SCHEMA,
            "required_consecutive_matrices": self.required_consecutive_matrices,
            "repositories": [
                {"name": item.name, "revision": item.revision}
                for item in self.repositories
            ],
            "profiles": [
                {"name": item.name, "image_digest": item.image_digest}
                for item in self.profiles
            ],
            "regions": list(self.regions),
            "cases": [item.safe_summary() for item in self.cases],
            "limits": {
                field: getattr(self.limits, field)
                for field in self.limits.__dataclass_fields__
            },
            "required_canaries": sorted(REQUIRED_CANARIES),
            "contract_test_groups": sorted(CONTRACT_TEST_GROUPS),
        }

    @property
    def digest(self) -> str:
        return _canonical_digest(self.safe_summary())


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=path.name + ".", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def new_ledger(plan: ReleasePlan) -> dict[str, Any]:
    return {
        "schema": RELEASE_LEDGER_SCHEMA,
        "release_plan_digest": plan.digest,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "required_consecutive_matrices": plan.required_consecutive_matrices,
        "matrices": [],
        "consecutive_passes": 0,
        "passed": False,
        "total_estimated_compute_cost_upper_usd": 0.0,
        "total_duration_seconds": 0.0,
    }


def load_ledger(path: Path, plan: ReleasePlan) -> dict[str, Any]:
    if not path.exists():
        return new_ledger(plan)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != RELEASE_LEDGER_SCHEMA:
        raise ValueError("release ledger schema is invalid")
    if value.get("release_plan_digest") != plan.digest:
        raise ValueError("release ledger belongs to a different release plan")
    matrices = value.get("matrices")
    if not isinstance(matrices, list):
        raise ValueError("release ledger matrices must be a list")
    return value


def _trailing_passes(matrices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in reversed(matrices):
        if not item.get("passed"):
            break
        result.append(item)
    return list(reversed(result))


def update_ledger(ledger: dict[str, Any], plan: ReleasePlan) -> None:
    matrices = ledger["matrices"]
    trailing = _trailing_passes(matrices)
    ledger["consecutive_passes"] = len(trailing)
    ledger["total_estimated_compute_cost_upper_usd"] = round(
        sum(float(item.get("estimated_compute_cost_upper_usd") or 0) for item in matrices),
        6,
    )
    ledger["total_duration_seconds"] = round(
        sum(float(item.get("duration_seconds") or 0) for item in matrices), 6
    )
    required_window = trailing[-plan.required_consecutive_matrices :]
    covered_cases = {item.get("case") for item in required_window}
    covered_profiles = {item.get("profile") for item in required_window}
    covered_regions = {item.get("region") for item in required_window}
    ledger["passed"] = bool(
        len(trailing) >= plan.required_consecutive_matrices
        and covered_cases == {item.name for item in plan.cases}
        and covered_profiles == {item.name for item in plan.profiles}
        and covered_regions == set(plan.regions)
        and ledger["total_estimated_compute_cost_upper_usd"]
        <= plan.limits.max_total_cost_usd
        and ledger["total_duration_seconds"] <= plan.limits.max_total_seconds
    )
    ledger["coverage"] = {
        "cases": sorted(str(item) for item in covered_cases if item),
        "profiles": sorted(str(item) for item in covered_profiles if item),
        "regions": sorted(str(item) for item in covered_regions if item),
    }
    ledger["updated_at"] = _utc_now()


def _git_receipt(repository: ReleaseRepository) -> dict[str, Any]:
    revision = subprocess.run(
        ["git", "-C", str(repository.path), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout.strip().lower()
    dirty = subprocess.run(
        [
            "git",
            "-C",
            str(repository.path),
            "status",
            "--porcelain",
            "--untracked-files=no",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout.strip()
    return {
        "name": repository.name,
        "revision": revision,
        "expected_revision": repository.revision,
        "revision_matches": revision == repository.revision,
        "tracked_worktree_clean": not dirty,
    }


def _configured_profile_receipt(
    config: CloudConfig, case: ReleaseCase
) -> dict[str, Any]:
    profiles = configured_worker_profiles(config)
    selected = profiles.get(case.profile) or {}
    image = str(selected.get("image") or "")
    digest = (
        "sha256:" + image.rsplit("@sha256:", 1)[1].lower()
        if "@sha256:" in image
        else ""
    )
    return {
        "profile": case.profile,
        "image_digest": digest,
        "expected_image_digest": case.image_digest,
        "matches": digest == case.image_digest,
    }


def _run_contract_tests(repo_root: Path, log_path: Path, timeout: float) -> dict[str, Any]:
    nodes = [node for group in CONTRACT_TEST_GROUPS.values() for node in group]
    started = time.monotonic()
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", *nodes],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        output = (result.stdout or "") + (result.stderr or "")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(output, encoding="utf-8")
        match = re.search(r"(\d+) passed", output)
        return {
            "passed": result.returncode == 0,
            "exit_code": result.returncode,
            "test_count": int(match.group(1)) if match else 0,
            "groups": {name: True for name in CONTRACT_TEST_GROUPS}
            if result.returncode == 0
            else {name: False for name in CONTRACT_TEST_GROUPS},
            "duration_seconds": round(time.monotonic() - started, 6),
            "test_set_digest": _canonical_digest(nodes),
            "output_omitted": True,
        }
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + (exc.stderr or "")
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(str(output), encoding="utf-8")
        return {
            "passed": False,
            "exit_code": None,
            "test_count": 0,
            "groups": {name: False for name in CONTRACT_TEST_GROUPS},
            "duration_seconds": round(time.monotonic() - started, 6),
            "test_set_digest": _canonical_digest(nodes),
            "timed_out": True,
            "output_omitted": True,
        }


def _replay_probe(
    driver: CoordinatorBenchmarkDriver,
    scorecard: dict[str, Any],
    reload_slo_seconds: float,
) -> dict[str, Any]:
    durations: list[float] = []
    ordered = True
    resumable = True
    status_matches = True
    job_count = 0
    for result in scorecard.get("results") or []:
        job_id = result.get("job_id")
        if not job_id:
            continue
        job_count += 1
        started = time.monotonic()
        snapshot = driver.snapshot(str(job_id))
        durations.append(time.monotonic() - started)
        status_matches = status_matches and snapshot.get("status") == result.get("status")
        events = driver.events(str(job_id), 0)
        sequences = [int(item.get("sequence") or 0) for item in events]
        ordered = (
            ordered
            and bool(sequences)
            and sequences == sorted(set(sequences))
            and all(sequences)
        )
        if sequences:
            pivot_index = max(0, len(sequences) - min(10, len(sequences)))
            cursor = sequences[pivot_index] - 1
            replayed = driver.events(str(job_id), cursor)
            replayed_sequences = [int(item.get("sequence") or 0) for item in replayed]
            resumable = resumable and replayed_sequences == [
                item for item in sequences if item > cursor
            ]
    maximum = max(durations, default=0.0)
    return {
        "passed": bool(
            job_count
            and maximum <= reload_slo_seconds
            and ordered
            and resumable
            and status_matches
        ),
        "job_count": job_count,
        "max_reload_seconds": round(maximum, 6),
        "reload_slo_seconds": reload_slo_seconds,
        "events_strictly_ordered": ordered,
        "cursor_reconnect_resumed": resumable,
        "snapshot_status_matches": status_matches,
    }


def _storage_budget_receipt(
    driver: CoordinatorBenchmarkDriver, release_limit: float
) -> dict[str, Any]:
    status = driver.cache_status()
    volumes = status.get("volumes") or []
    unknown_provider = any(
        str(item.get("provider") or "").lower() != "runpod"
        for item in volumes
        if item.get("status") in {"creating", "ready", "degraded", "deleting"}
    )
    monthly = sum(
        estimate_runpod_storage_monthly(float(item.get("capacity_bytes") or 0) / 1024**3)
        for item in volumes
        if str(item.get("provider") or "").lower() == "runpod"
        and item.get("status") in {"creating", "ready", "degraded", "deleting"}
    )
    policy_limit = (status.get("policy") or {}).get("max_monthly_storage_cost")
    policy_limit = float(policy_limit) if policy_limit is not None else None
    return {
        "passed": bool(
            not unknown_provider
            and policy_limit is not None
            and monthly <= policy_limit
            and monthly <= release_limit
        ),
        "active_volume_count": sum(
            1
            for item in volumes
            if item.get("status") in {"creating", "ready", "degraded", "deleting"}
        ),
        "estimated_monthly_storage_cost_usd": round(monthly, 6),
        "configured_budget_usd": policy_limit,
        "release_budget_usd": release_limit,
        "unknown_provider_cost": unknown_provider,
    }


def _scorecard_receipt(
    scorecard: dict[str, Any],
    case: ReleaseCase,
    limits: ReleaseLimits,
    replay: dict[str, Any],
    storage: dict[str, Any],
) -> dict[str, Any]:
    failures: list[str] = []
    if scorecard.get("schema") != SCORECARD_SCHEMA or not scorecard.get("passed"):
        failures.append("benchmark_failed")
    if scorecard.get("orphaned_resources") or scorecard.get("final_audit_error"):
        failures.append("orphan_or_audit_failure")
    cost = float(scorecard.get("estimated_compute_cost_upper_usd") or 0)
    if cost > limits.max_matrix_cost_usd:
        failures.append("matrix_cost_limit")
    results = scorecard.get("results") or []
    plan_scenarios = {
        item.get("name"): item for item in (scorecard.get("plan") or {}).get("scenarios") or []
    }
    canaries: set[str] = set()
    cold: list[float] = []
    hot: list[float] = []
    maximum_closure = 0.0
    cancellation_closure = 0.0
    support_count = 0
    axis_receipts = 0
    for result in results:
        name = result.get("name")
        plan_scenario = plan_scenarios.get(name) or {}
        if result.get("passed"):
            cache_state = result.get("cache_state")
            if cache_state in {"cold", "hot"}:
                canaries.add(str(cache_state))
            failure_kind = plan_scenario.get("failure_kind")
            if failure_kind:
                canaries.add(str(failure_kind))
        closure = result.get("resource_closure_seconds")
        if closure is None:
            failures.append("provider_closure_measurement_missing")
        else:
            maximum_closure = max(maximum_closure, float(closure))
        if plan_scenario.get("failure_kind") == "cancellation":
            cancellation_to_absence = result.get(
                "cancellation_to_provider_absence_seconds"
            )
            if cancellation_to_absence is None:
                failures.append("cancellation_closure_measurement_missing")
            else:
                cancellation_closure = max(
                    cancellation_closure, float(cancellation_to_absence)
                )
        preparation = result.get("preparation_seconds")
        if preparation is not None and result.get("cache_state") == "cold":
            cold.append(float(preparation))
        if preparation is not None and result.get("cache_state") == "hot":
            hot.append(float(preparation))
        receipt = result.get("submission_receipt") or {}
        if (
            receipt.get("profile") == case.profile
            and receipt.get("image_digest") == case.image_digest
            and receipt.get("region") == case.region
            and receipt.get("allowed_regions") == [case.region]
        ):
            axis_receipts += 1
        bundle = result.get("support_bundle") or {}
        if bundle.get("schema") == "cloud-offload.support-bundle.v1":
            support_count += 1
        if plan_scenario.get("failure_kind") == "corruption" and result.get("passed"):
            event_types = {
                str(item.get("type") or "") for item in bundle.get("events") or []
            }
            if "cache_artifact_quarantined" in event_types:
                canaries.add("stale_manifest")
        if result.get("cache_state") == "cold" and result.get("passed"):
            if receipt.get("cold_fallback_available"):
                canaries.add("regional_fallback")
    ratio = None
    if cold and hot and statistics.median(cold) > 0:
        ratio = statistics.median(hot) / statistics.median(cold)
    if ratio is None or ratio > limits.hot_preparation_ratio_max:
        failures.append("hot_acceleration_target")
    if maximum_closure > limits.provider_closure_slo_seconds:
        failures.append("provider_closure_slo")
    if cancellation_closure > limits.cancellation_slo_seconds:
        failures.append("cancellation_slo")
    if axis_receipts != len(results):
        failures.append("image_region_receipt")
    if support_count != len(results):
        failures.append("support_bundle_missing")
    missing_canaries = REQUIRED_CANARIES - canaries
    if missing_canaries:
        failures.append("missing_canaries")
    if not replay.get("passed"):
        failures.append("reload_reconnect_event_order")
    if not storage.get("passed"):
        failures.append("storage_budget")
    return {
        "passed": not failures,
        "failure_codes": sorted(set(failures)),
        "canaries": sorted(canaries),
        "missing_canaries": sorted(missing_canaries),
        "scenario_count": len(results),
        "axis_receipt_count": axis_receipts,
        "support_bundle_receipt_count": support_count,
        "estimated_compute_cost_upper_usd": round(cost, 6),
        "maximum_provider_closure_seconds": round(maximum_closure, 6),
        "cancellation_closure_seconds": round(cancellation_closure, 6),
        "hot_preparation_ratio": round(ratio, 6) if ratio is not None else None,
        "hot_preparation_ratio_limit": limits.hot_preparation_ratio_max,
    }


def _harness_failure_receipt(
    index: int, case: ReleaseCase, exc: Exception
) -> dict[str, Any]:
    """A matrix whose harness dies mid-flight is still a failed matrix.

    The receipt stays redacted: only the exception class reaches the ledger,
    never its message, which can carry URLs, paths, or provider payloads.
    """

    now = _utc_now()
    return {
        "schema": RELEASE_MATRIX_SCHEMA,
        "index": index,
        "case": case.name,
        "profile": case.profile,
        "image_digest": case.image_digest,
        "region": case.region,
        "started_at": now,
        "completed_at": now,
        "duration_seconds": 0.0,
        "passed": False,
        "failure_codes": ["release_harness_error:" + type(exc).__name__],
        "estimated_compute_cost_upper_usd": 0.0,
        "provider_mutation": False,
    }


class ReleaseExecutor:
    def __init__(
        self,
        plan: ReleasePlan,
        ledger_path: str | Path,
        output_dir: str | Path,
        config: CloudConfig,
        service: dict[str, Any],
        *,
        allow_hooks: bool,
    ):
        self.plan = plan
        self.ledger_path = Path(ledger_path).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.config = config
        self.service = service
        self.allow_hooks = allow_hooks

    def run(self, *, max_matrices: int | None = None) -> dict[str, Any]:
        ledger = load_ledger(self.ledger_path, self.plan)
        executed = 0
        stop_reason = "release_already_passed" if ledger.get("passed") else None
        while not ledger.get("passed"):
            if max_matrices is not None and executed >= max_matrices:
                stop_reason = "requested_matrix_limit"
                break
            if float(ledger.get("total_estimated_compute_cost_upper_usd") or 0) >= self.plan.limits.max_total_cost_usd:
                stop_reason = "total_cost_limit"
                break
            if float(ledger.get("total_duration_seconds") or 0) >= self.plan.limits.max_total_seconds:
                stop_reason = "total_runtime_limit"
                break
            index = len(ledger["matrices"]) + 1
            case = self.plan.cases[(index - 1) % len(self.plan.cases)]
            self._require_reviewed_hooks(case)
            matrix_started = time.monotonic()
            try:
                receipt = self._run_matrix(index, case)
            except (KeyboardInterrupt, SystemExit) as exc:
                receipt = self._release_interrupt_receipt(
                    index, case, exc, matrix_started=matrix_started
                )
            except Exception as exc:
                receipt = self._release_interrupt_receipt(
                    index, case, exc, matrix_started=matrix_started
                )
            ledger["matrices"].append(receipt)
            update_ledger(ledger, self.plan)
            _atomic_json(self.ledger_path, ledger)
            executed += 1
            if not receipt["passed"]:
                stop_reason = str(
                    receipt.get("matrix_stop_reason") or "matrix_failed"
                )
                break
        if ledger.get("passed") and stop_reason != "release_already_passed":
            stop_reason = "release_passed"
        ledger["last_stop_reason"] = stop_reason
        ledger["last_run_matrix_count"] = executed
        ledger["updated_at"] = _utc_now()
        _atomic_json(self.ledger_path, ledger)
        return ledger

    def _release_interrupt_receipt(
        self,
        index: int,
        case: ReleaseCase,
        exc: BaseException,
        *,
        matrix_started: float | None = None,
    ) -> dict[str, Any]:
        """Publish finite failure evidence if an interrupt escapes the runner."""

        reason = (
            f"operator_interrupt:{type(exc).__name__}"
            if isinstance(exc, (KeyboardInterrupt, SystemExit))
            else f"release_harness_error:{type(exc).__name__}"
        )
        now = _utc_now()
        matrix_dir = self.output_dir / f"matrix-{index:04d}-{case.name}"
        scorecard_path = matrix_dir / "benchmark-scorecard.json"
        scorecard: dict[str, Any] | None = None
        if scorecard_path.exists():
            try:
                candidate = json.loads(scorecard_path.read_text(encoding="utf-8"))
                if isinstance(candidate, dict) and candidate.get("schema") == SCORECARD_SCHEMA:
                    scorecard = candidate
            except (OSError, ValueError):
                scorecard = None
        detailed_evidence_available = scorecard is not None
        if scorecard is None:
            scorecard = {
                "schema": SCORECARD_SCHEMA,
                "started_at": now,
                "plan": case.benchmark_plan.safe_summary(),
                "estimated_compute_cost_upper_usd": self.plan.limits.max_matrix_cost_usd,
                "results": [],
                "distributions": {},
                "final_cleanup": [],
            }

        cleanup_error: str | None = None
        cleanup_receipts: list[dict[str, Any]] = []
        orphans: list[dict[str, str]] = []
        try:
            driver = CoordinatorBenchmarkDriver(
                self.service["url"],
                self.service.get("token"),
                self.config,
                case.benchmark_plan.providers,
                allow_hooks=self.allow_hooks,
            )
            attributed = {
                (str(item.get("provider") or ""), str(item.get("instance_id") or "")): _ResourceMeter(
                    id=str(item.get("instance_id") or ""),
                    provider=str(item.get("provider") or ""),
                    hourly_rate=float(item.get("hourly_rate") or 0),
                    first_seen=matrix_started or time.monotonic(),
                    last_seen=time.monotonic(),
                    source="release_fallback",
                    lease_id=str(item.get("lease_id") or "") or None,
                )
                for result in scorecard.get("results") or []
                for item in result.get("resources") or []
                if item.get("provider") and item.get("instance_id") and item.get("lease_id")
            }
            if attributed:
                cleanup_receipts = BenchmarkRunner(driver)._cleanup_resources(
                    case.benchmark_plan.providers,
                    {provider: {} for provider in case.benchmark_plan.providers},
                    attributed,
                    case.benchmark_plan.limits.cleanup_timeout_seconds,
                    include_untracked=False,
                )
            final_inventory = driver.inventory(case.benchmark_plan.providers)
            if detailed_evidence_available:
                orphans = [
                    {"provider": provider, "instance_id": instance_id}
                    for provider, instance_id in attributed
                    if instance_id in final_inventory.get(provider, {})
                ]
                orphans.extend(
                    dict(item)
                    for result in scorecard.get("results") or []
                    for item in result.get("unknown_paid_resources") or []
                )
            else:
                orphans = [
                    {"provider": provider, "instance_id": instance.id}
                    for provider, instances in final_inventory.items()
                    for instance in instances.values()
                    if instance.managed
                ]
        except Exception as cleanup_exc:  # noqa: BLE001
            cleanup_error = type(cleanup_exc).__name__

        cleanup_state = (
            "confirmed" if cleanup_error is None and not orphans else "failed"
        )
        ongoing_orphan_cost = cleanup_state != "confirmed"
        conservative_cost = (
            self.plan.limits.max_total_cost_usd
            if ongoing_orphan_cost
            else self.plan.limits.max_matrix_cost_usd
        )
        scorecard["estimated_compute_cost_upper_usd"] = max(
            float(scorecard.get("estimated_compute_cost_upper_usd") or 0),
            conservative_cost,
        )
        scorecard.update(
            {
                "completed_at": now,
                "passed": False,
                "campaign_abort": reason,
                "final_cleanup": list(scorecard.get("final_cleanup") or [])
                + cleanup_receipts,
                "final_audit_error": cleanup_error,
                "orphaned_resources": orphans,
                "cleanup_proof": {"state": cleanup_state},
            }
        )
        scorecard_path = write_scorecard(scorecard_path, scorecard)
        estimated_cost = float(scorecard["estimated_compute_cost_upper_usd"])
        wall_elapsed = (
            max(0.0, time.monotonic() - matrix_started)
            if matrix_started is not None
            else 0.0
        )
        completed_duration = sum(
            float(item.get("duration_seconds") or 0)
            for item in scorecard.get("results") or []
        )
        duration_ceiling = (
            self.plan.limits.max_total_seconds
            if ongoing_orphan_cost
            else self.plan.limits.max_matrix_seconds
        )
        estimated_duration = max(wall_elapsed, completed_duration, duration_ceiling)
        duration_basis = "total_limit_conservative" if ongoing_orphan_cost else "matrix_limit_conservative"
        failure_codes = [reason]
        if cleanup_state != "confirmed":
            failure_codes.append("cleanup_unverified")
        return {
            "schema": RELEASE_MATRIX_SCHEMA,
            "index": index,
            "case": case.name,
            "profile": case.profile,
            "image_digest": case.image_digest,
            "region": case.region,
            "started_at": now,
            "completed_at": now,
            "duration_seconds": round(estimated_duration, 6),
            "duration_basis": duration_basis,
            "passed": False,
            "failure_codes": failure_codes,
            "matrix_stop_reason": (
                reason if reason.startswith("operator_interrupt:") else None
            ),
            "estimated_compute_cost_upper_usd": round(estimated_cost, 6),
            "ongoing_orphan_cost": ongoing_orphan_cost,
            "provider_mutation": "unknown",
            "cleanup_proof": {"state": cleanup_state},
            "detailed_scenario_evidence_available": detailed_evidence_available,
            "benchmark_scorecard_digest": _file_digest(scorecard_path),
            "benchmark_plan_digest": case.benchmark_plan_digest,
        }

    def _require_reviewed_hooks(self, case: ReleaseCase) -> None:
        hook_scenarios = [
            item.name
            for item in case.benchmark_plan.scenarios
            if item.failure and item.failure.hook_argv
        ]
        if hook_scenarios and not self.allow_hooks:
            raise RuntimeError("full release matrix needs reviewed benchmark hooks")

    def _run_matrix(self, index: int, case: ReleaseCase) -> dict[str, Any]:
        started_at = _utc_now()
        started = time.monotonic()
        matrix_dir = self.output_dir / f"matrix-{index:04d}-{case.name}"
        matrix_dir.mkdir(parents=True, exist_ok=True)
        repositories = [_git_receipt(item) for item in self.plan.repositories]
        profile = _configured_profile_receipt(self.config, case)
        contract = _run_contract_tests(
            next(item.path for item in self.plan.repositories if item.name == "backend"),
            matrix_dir / "contract-tests.log",
            self.plan.limits.contract_test_timeout_seconds,
        )
        precheck_passed = bool(
            all(
                item["revision_matches"] and item["tracked_worktree_clean"]
                for item in repositories
            )
            and profile["matches"]
            and contract["passed"]
        )
        if not precheck_passed:
            return {
                "schema": RELEASE_MATRIX_SCHEMA,
                "index": index,
                "case": case.name,
                "profile": case.profile,
                "image_digest": case.image_digest,
                "region": case.region,
                "started_at": started_at,
                "completed_at": _utc_now(),
                "duration_seconds": round(time.monotonic() - started, 6),
                "passed": False,
                "failure_codes": ["release_precheck_failed"],
                "repositories": repositories,
                "configured_profile": profile,
                "contract_tests": contract,
                "estimated_compute_cost_upper_usd": 0.0,
                "provider_mutation": False,
            }
        self._require_reviewed_hooks(case)
        driver = CoordinatorBenchmarkDriver(
            self.service["url"],
            self.service.get("token"),
            self.config,
            case.benchmark_plan.providers,
            allow_hooks=self.allow_hooks,
        )
        scorecard = BenchmarkRunner(driver).run(case.benchmark_plan)
        scorecard_path = write_scorecard(matrix_dir / "benchmark-scorecard.json", scorecard)
        replay = _replay_probe(driver, scorecard, self.plan.limits.reload_slo_seconds)
        storage = _storage_budget_receipt(
            driver, self.plan.limits.max_monthly_storage_cost_usd
        )
        evaluation = _scorecard_receipt(
            scorecard, case, self.plan.limits, replay, storage
        )
        duration = time.monotonic() - started
        failures = list(evaluation["failure_codes"])
        if duration > self.plan.limits.max_matrix_seconds:
            failures.append("matrix_runtime_limit")
        passed = evaluation["passed"] and not failures
        campaign_abort = str(scorecard.get("campaign_abort") or "")
        matrix_stop_reason = (
            campaign_abort if campaign_abort in SAFE_CAMPAIGN_STOP_REASONS else None
        )
        return {
            "schema": RELEASE_MATRIX_SCHEMA,
            "index": index,
            "case": case.name,
            "profile": case.profile,
            "image_digest": case.image_digest,
            "region": case.region,
            "started_at": started_at,
            "completed_at": _utc_now(),
            "duration_seconds": round(duration, 6),
            "passed": passed,
            "matrix_stop_reason": matrix_stop_reason,
            "failure_codes": sorted(set(failures)),
            "repositories": repositories,
            "configured_profile": profile,
            "contract_tests": contract,
            "benchmark_scorecard_digest": _file_digest(scorecard_path),
            "benchmark_plan_digest": case.benchmark_plan_digest,
            "deterministic_preflight_false_readiness_count": 0,
            "replay_probe": replay,
            "storage_budget": storage,
            **{key: value for key, value in evaluation.items() if key not in {"passed", "failure_codes"}},
        }


def write_release_projection(path: str | Path, ledger: dict[str, Any]) -> Path:
    target = Path(path)
    _atomic_json(target, ledger)
    return target
