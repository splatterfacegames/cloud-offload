"""Spend-capped production benchmarking for Cloud Offload.

The harness is deliberately control-plane-first: it never launches a provider
resource itself. It submits an ordinary job, observes the durable JobEventV2
journal, accounts conservatively from the quoted hourly rate, and terminates only
the exact Cloud Offload resources attributable to the campaign.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import statistics
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import requests

from cloud_offload.config import CloudConfig
from cloud_offload.providers import create_connector


PLAN_SCHEMA = "cloud-offload.benchmark-plan.v1"
SCORECARD_SCHEMA = "cloud-offload.benchmark-scorecard.v1"
SCENARIO_SCHEMA = "cloud-offload.benchmark-scenario-result.v1"
TERMINAL_STATUSES = {"completed", "failed", "dead_letter"}
FAILURE_KINDS = {"cancellation", "provider", "storage", "corruption", "restart"}
CACHE_STATES = {"cold", "hot", "failure"}
PREPARED_STORAGE_POLICIES = {"off", "smart", "strict", "pinned"}
SUBMISSION_ENDPOINTS = {"/api/partitions", "/api/workflows"}
MANAGED_INSTANCE_PREFIX = "cloud-offload-worker-"
DEFAULT_HOOK_TIMEOUT_SECONDS = 120
CORRUPTION_OBSERVE_HOOK_TIMEOUT_SECONDS = 270
PREFLIGHT_OFFER_RETRY_SECONDS = 300
PREFLIGHT_OFFER_RETRY_INTERVAL_SECONDS = 15


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _bounded_positive(value: Any, label: str, *, allow_zero: bool = False) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc
    minimum = 0 if allow_zero else 0.000001
    if not math.isfinite(number) or number < minimum:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{label} must be a finite {qualifier} number")
    return number


@dataclass(frozen=True)
class BenchmarkLimits:
    max_total_cost_usd: float
    max_scenario_cost_usd: float
    max_campaign_seconds: float
    poll_seconds: float = 2.0
    cleanup_timeout_seconds: float = 90.0
    fresh_worker_timeout_seconds: float = 120.0
    runner_readiness_timeout_seconds: float = 300.0
    max_runner_readiness_cost_usd: float | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BenchmarkLimits":
        max_scenario_cost = _bounded_positive(
            value.get("max_scenario_cost_usd"),
            "limits.max_scenario_cost_usd",
        )
        readiness_cost = _bounded_positive(
            value.get("max_runner_readiness_cost_usd", max_scenario_cost),
            "limits.max_runner_readiness_cost_usd",
        )
        if readiness_cost > max_scenario_cost:
            raise ValueError(
                "runner readiness cost limit cannot exceed scenario cost limit"
            )
        return cls(
            max_total_cost_usd=_bounded_positive(
                value.get("max_total_cost_usd"), "limits.max_total_cost_usd"
            ),
            max_scenario_cost_usd=max_scenario_cost,
            max_campaign_seconds=_bounded_positive(
                value.get("max_campaign_seconds"), "limits.max_campaign_seconds"
            ),
            poll_seconds=_bounded_positive(
                value.get("poll_seconds", 2), "limits.poll_seconds"
            ),
            cleanup_timeout_seconds=_bounded_positive(
                value.get("cleanup_timeout_seconds", 90),
                "limits.cleanup_timeout_seconds",
            ),
            fresh_worker_timeout_seconds=_bounded_positive(
                value.get("fresh_worker_timeout_seconds", 120),
                "limits.fresh_worker_timeout_seconds",
            ),
            runner_readiness_timeout_seconds=_bounded_positive(
                value.get("runner_readiness_timeout_seconds", 300),
                "limits.runner_readiness_timeout_seconds",
            ),
            max_runner_readiness_cost_usd=readiness_cost,
        )


@dataclass(frozen=True)
class FailureInjection:
    kind: str
    trigger_phase: str | None = None
    trigger_event: str | None = None
    after_seconds: float = 0.0
    hook_argv: tuple[str, ...] = ()
    before_submit: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "FailureInjection":
        kind = str(value.get("kind") or "").strip().lower()
        if kind not in FAILURE_KINDS:
            raise ValueError(
                f"failure.kind must be one of {', '.join(sorted(FAILURE_KINDS))}"
            )
        hook = value.get("hook_argv") or []
        if not isinstance(hook, list) or any(not str(item).strip() for item in hook):
            raise ValueError("failure.hook_argv must be a list of non-empty strings")
        if kind in {"storage", "corruption", "restart"} and not hook:
            raise ValueError(f"failure kind {kind!r} requires hook_argv")
        if kind in {"cancellation", "provider"} and hook:
            raise ValueError(
                f"failure kind {kind!r} uses a built-in action, not a hook"
            )
        before_submit = bool(value.get("before_submit", False))
        if before_submit and kind not in {"storage", "corruption", "restart"}:
            raise ValueError("failure.before_submit requires an external hook kind")
        trigger_event = (
            str(value["trigger_event"]) if value.get("trigger_event") else None
        )
        if (
            kind == "corruption"
            and not value.get("trigger_phase")
            and not trigger_event
        ):
            trigger_event = "cache_mount_ready"
        return cls(
            kind=kind,
            trigger_phase=(
                str(value["trigger_phase"]) if value.get("trigger_phase") else None
            ),
            trigger_event=trigger_event,
            after_seconds=_bounded_positive(
                value.get("after_seconds", 0),
                "failure.after_seconds",
                allow_zero=True,
            ),
            hook_argv=tuple(str(item) for item in hook),
            before_submit=before_submit,
        )


@dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    cache_state: str
    endpoint: str
    request: dict[str, Any]
    timeout_seconds: float
    expected_statuses: tuple[str, ...]
    fresh_instance: bool = True
    prepared_storage_policy: str | None = None
    allowed_regions: tuple[str, ...] = ()
    failure: FailureInjection | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any], index: int) -> "BenchmarkScenario":
        name = str(value.get("name") or "").strip()
        if not name:
            raise ValueError(f"scenarios[{index}].name is required")
        cache_state = str(value.get("cache_state") or "").strip().lower()
        if cache_state not in CACHE_STATES:
            raise ValueError(
                f"scenarios[{index}].cache_state must be cold, hot, or failure"
            )
        endpoint = str(value.get("endpoint") or "").strip()
        if endpoint not in SUBMISSION_ENDPOINTS:
            raise ValueError(
                f"scenarios[{index}].endpoint must be /api/partitions or /api/workflows"
            )
        request = value.get("request")
        if not isinstance(request, dict):
            raise ValueError(f"scenarios[{index}].request must be an object")
        if (
            endpoint == "/api/partitions"
            and bool(value.get("fresh_instance", True))
            and request.get("force_execution") is not True
        ):
            raise ValueError(
                f"scenarios[{index}] requires request.force_execution=true "
                "to prove a fresh-Pod run"
            )
        try:
            _canonical_digest(request)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"scenarios[{index}].request must contain finite JSON"
            ) from exc
        statuses = value.get("expected_statuses") or ["completed"]
        if not isinstance(statuses, list) or not statuses:
            raise ValueError(f"scenarios[{index}].expected_statuses must be a list")
        normalized_statuses = tuple(str(item) for item in statuses)
        unknown = set(normalized_statuses) - TERMINAL_STATUSES
        if unknown:
            raise ValueError(
                f"scenarios[{index}].expected_statuses contains non-terminal values: "
                + ", ".join(sorted(unknown))
            )
        failure = value.get("failure")
        if failure is not None and not isinstance(failure, dict):
            raise ValueError(f"scenarios[{index}].failure must be an object")
        failure_injection = FailureInjection.from_dict(failure) if failure else None
        if failure_injection and failure_injection.kind == "corruption":
            if endpoint != "/api/partitions" or not failure_injection.before_submit:
                raise ValueError(
                    f"scenarios[{index}] corruption requires a pre-submit partition hook"
                )
            from cloud_offload.benchmark_faults import (
                CORRUPTION_NONCE_FIELD,
                corruption_canary_asset,
            )

            request = json.loads(json.dumps(request, allow_nan=False))
            partition = request.get("partition")
            if not isinstance(partition, dict):
                raise ValueError(
                    f"scenarios[{index}] corruption requires request.partition"
                )
            assets = partition.setdefault("assets", [])
            if not isinstance(assets, list):
                raise ValueError(
                    f"scenarios[{index}] corruption requires partition.assets"
                )
            assets[:] = [
                item
                for item in assets
                if not (
                    isinstance(item, dict)
                    and (
                        item.get(CORRUPTION_NONCE_FIELD)
                        or str(item.get("filename") or "").startswith(
                            "cloud_offload_benchmark_canary_"
                        )
                    )
                )
            ]
            assets.append(corruption_canary_asset(name, nonce=uuid.uuid4().hex))
            _canonical_digest(request)
        raw_storage_policy = value.get("prepared_storage_policy")
        storage_policy = (
            str(raw_storage_policy).strip().lower()
            if raw_storage_policy is not None
            else None
        )
        if (
            storage_policy is not None
            and storage_policy not in PREPARED_STORAGE_POLICIES
        ):
            raise ValueError(
                f"scenarios[{index}].prepared_storage_policy must be one of "
                + ", ".join(sorted(PREPARED_STORAGE_POLICIES))
            )
        if cache_state == "cold" and storage_policy != "off":
            raise ValueError(
                f"scenarios[{index}] cold runs require prepared_storage_policy='off'"
            )
        if cache_state == "hot" and storage_policy not in {
            "smart",
            "strict",
            "pinned",
        }:
            raise ValueError(
                f"scenarios[{index}] hot runs require an enabled "
                "prepared_storage_policy"
            )
        raw_regions = value.get("allowed_regions") or []
        if not isinstance(raw_regions, list):
            raise ValueError(f"scenarios[{index}].allowed_regions must be a list")
        allowed_regions = tuple(
            dict.fromkeys(str(item).strip() for item in raw_regions if str(item).strip())
        )
        if len(allowed_regions) != len(raw_regions):
            raise ValueError(
                f"scenarios[{index}].allowed_regions must contain unique non-empty names"
            )
        return cls(
            name=name,
            cache_state=cache_state,
            endpoint=endpoint,
            request=request,
            timeout_seconds=_bounded_positive(
                value.get("timeout_seconds"), f"scenarios[{index}].timeout_seconds"
            ),
            expected_statuses=normalized_statuses,
            fresh_instance=bool(value.get("fresh_instance", True)),
            prepared_storage_policy=storage_policy,
            allowed_regions=allowed_regions,
            failure=failure_injection,
        )


@dataclass(frozen=True)
class BenchmarkPlan:
    providers: tuple[str, ...]
    scenarios: tuple[BenchmarkScenario, ...]
    limits: BenchmarkLimits
    exclusive: bool = True

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BenchmarkPlan":
        if value.get("schema") != PLAN_SCHEMA:
            raise ValueError(f"benchmark plan schema must be {PLAN_SCHEMA!r}")
        providers = value.get("providers") or []
        if not isinstance(providers, list) or not providers:
            raise ValueError("providers must be a non-empty list")
        normalized_providers = tuple(str(item).strip() for item in providers)
        if any(not item for item in normalized_providers):
            raise ValueError("providers cannot contain empty names")
        if len(set(normalized_providers)) != len(normalized_providers):
            raise ValueError("providers must be unique")
        raw_scenarios = value.get("scenarios") or []
        if not isinstance(raw_scenarios, list) or not raw_scenarios:
            raise ValueError("scenarios must be a non-empty list")
        scenarios = tuple(
            BenchmarkScenario.from_dict(item, index)
            for index, item in enumerate(raw_scenarios)
            if isinstance(item, dict)
        )
        if len(scenarios) != len(raw_scenarios):
            raise ValueError("every scenario must be an object")
        names = [item.name for item in scenarios]
        if len(set(names)) != len(names):
            raise ValueError("scenario names must be unique")
        alternating = [
            scenario.cache_state
            for scenario in scenarios
            if scenario.cache_state in {"cold", "hot"}
        ]
        if alternating:
            if alternating[0] != "cold":
                raise ValueError("cold/hot benchmark scenarios must begin with cold")
            if any(left == right for left, right in zip(alternating, alternating[1:])):
                raise ValueError("cold/hot benchmark scenarios must alternate")
        limits = value.get("limits")
        if not isinstance(limits, dict):
            raise ValueError("limits must be an object")
        return cls(
            providers=normalized_providers,
            scenarios=scenarios,
            limits=BenchmarkLimits.from_dict(limits),
            exclusive=bool(value.get("exclusive", True)),
        )

    @classmethod
    def load(cls, path: str | Path) -> "BenchmarkPlan":
        payload = json.loads(Path(path).read_bytes())
        if not isinstance(payload, dict):
            raise ValueError("benchmark plan must contain a JSON object")
        return cls.from_dict(payload)

    @classmethod
    def from_bytes(cls, raw: bytes) -> "BenchmarkPlan":
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("benchmark plan must contain a JSON object")
        return cls.from_dict(payload)

    def safe_summary(self) -> dict[str, Any]:
        return {
            "schema": PLAN_SCHEMA,
            "providers": list(self.providers),
            "exclusive": self.exclusive,
            "limits": {
                "max_total_cost_usd": self.limits.max_total_cost_usd,
                "max_scenario_cost_usd": self.limits.max_scenario_cost_usd,
                "max_campaign_seconds": self.limits.max_campaign_seconds,
                "fresh_worker_timeout_seconds": self.limits.fresh_worker_timeout_seconds,
                "runner_readiness_timeout_seconds": self.limits.runner_readiness_timeout_seconds,
                "max_runner_readiness_cost_usd": self.limits.max_runner_readiness_cost_usd,
            },
            "scenarios": [
                {
                    "name": item.name,
                    "cache_state": item.cache_state,
                    "endpoint": item.endpoint,
                    "request_digest": _canonical_digest(item.request),
                    "timeout_seconds": item.timeout_seconds,
                    "fresh_instance": item.fresh_instance,
                    "prepared_storage_policy": item.prepared_storage_policy,
                    "allowed_regions": list(item.allowed_regions),
                    "failure_kind": item.failure.kind if item.failure else None,
                    "failure_before_submit": (
                        item.failure.before_submit if item.failure else False
                    ),
                }
                for item in self.scenarios
            ],
        }


@dataclass(frozen=True)
class InstanceObservation:
    id: str
    provider: str
    hourly_rate: float
    status: str
    managed: bool
    name: str | None = None
    provider_state: str | None = None
    container_started: bool | None = None


class BenchmarkDriver(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...

    def inventory(
        self, providers: tuple[str, ...]
    ) -> dict[str, dict[str, InstanceObservation]]: ...

    def active_workers(self, providers: tuple[str, ...]) -> list[dict[str, Any]]: ...

    def prepare_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]: ...

    def restore_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]: ...

    def submit(self, scenario: BenchmarkScenario) -> str: ...

    def snapshot(self, job_id: str) -> dict[str, Any]: ...

    def events(self, job_id: str, after: int) -> list[dict[str, Any]]: ...

    def cancel(self, job_id: str) -> dict[str, Any]: ...

    def terminate(self, provider: str, instance_id: str) -> bool: ...

    def support_bundle(self, job_id: str) -> dict[str, Any] | None: ...

    def run_hook(
        self, injection: FailureInjection, context: dict[str, str]
    ) -> dict[str, Any]: ...


@dataclass
class _ResourceMeter:
    id: str
    provider: str
    hourly_rate: float
    first_seen: float
    last_seen: float
    source: str
    job_id: str | None = None
    lease_id: str | None = None


class _PreSubmitLimit(Exception):
    """Internal control flow for a guard that stops before submission."""


def _managed_ids(
    inventory: dict[str, dict[str, InstanceObservation]],
) -> set[tuple[str, str]]:
    return {
        (provider, instance.id)
        for provider, instances in inventory.items()
        for instance in instances.values()
        if instance.managed
    }


def _distribution(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    ordered = sorted(values)

    def percentile(fraction: float) -> float:
        index = max(0, math.ceil(fraction * len(ordered)) - 1)
        return ordered[index]

    return {
        "count": len(ordered),
        "min": round(ordered[0], 6),
        "mean": round(statistics.fmean(ordered), 6),
        "p50": round(percentile(0.50), 6),
        "p95": round(percentile(0.95), 6),
        "max": round(ordered[-1], 6),
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)
    except ValueError:
        return None


def _phase_durations(events: list[dict[str, Any]]) -> dict[str, float]:
    first_seen: dict[str, datetime] = {}
    final_time: datetime | None = None
    for item in events:
        observed = _parse_timestamp(item.get("observed_at"))
        if observed is None:
            continue
        final_time = max(final_time, observed) if final_time else observed
        phase = item.get("phase")
        if phase and phase not in first_seen:
            first_seen[str(phase)] = observed
    ordered = sorted(first_seen.items(), key=lambda item: item[1])
    durations: dict[str, float] = {}
    for index, (phase, started) in enumerate(ordered):
        ended = ordered[index + 1][1] if index + 1 < len(ordered) else final_time
        durations[phase] = round(max(0.0, (ended - started).total_seconds()), 6)
    return durations


def _preparation_seconds(events: list[dict[str, Any]]) -> float | None:
    """Measure worker staging until the durable execution transition."""

    started: datetime | None = None
    for item in events:
        observed = _parse_timestamp(item.get("observed_at"))
        if observed is None:
            continue
        phase = str(item.get("phase") or "")
        if started is None:
            if phase == "staging_started":
                started = observed
            continue
        if phase in {"execution", "execution_started"}:
            return round(max(0.0, (observed - started).total_seconds()), 6)
    return None


def _seconds_from_event_to(
    events: list[dict[str, Any]], event_type: str, completed_at: str
) -> float | None:
    completed = _parse_timestamp(completed_at)
    if completed is None:
        return None
    for item in events:
        if str(item.get("type") or "") != event_type:
            continue
        observed = _parse_timestamp(item.get("observed_at"))
        if observed is not None:
            return round(max(0.0, (completed - observed).total_seconds()), 6)
    return None


def _initial_startup_phases() -> dict[str, dict[str, Any]]:
    """Finite startup facts; unknown is explicit when no authority proves a phase."""

    return {
        "allocation": {"state": "unknown"},
        # RunPod REST v2 does not expose an authoritative image-pull state.
        "image_pull": {"state": "unknown"},
        "container_start": {"state": "unknown"},
        "runner_callback": {"state": "unknown"},
        "comfyui_readiness": {"state": "unknown"},
    }


def _safe_provider_state(value: Any) -> str:
    state = str(value or "UNKNOWN").upper()
    return state if state in {
        "PROVISIONING",
        "STARTING",
        "RUNNING",
        "EXITED",
        "ERROR",
        "TERMINATED",
        "UNKNOWN",
    } else "UNKNOWN"


def _observe_startup_phases(
    phases: dict[str, dict[str, Any]],
    inventory: dict[str, dict[str, InstanceObservation]],
    workers: list[dict[str, Any]],
    events: list[dict[str, Any]],
    identities: dict[tuple[str, str], str] | None = None,
) -> None:
    """Merge only provider enums, container telemetry, and worker status facts."""

    allowed = set(identities) if identities is not None else None
    instances = [
        instance
        for provider, provider_instances in inventory.items()
        for instance in provider_instances.values()
        if instance.managed
        and (allowed is None or (provider, instance.id) in allowed)
    ]
    if instances:
        provider_states = sorted(
            {
                _safe_provider_state(instance.provider_state or instance.status)
                for instance in instances
            }
        )
        phases["allocation"] = {
            "state": "confirmed",
            "provider_state": provider_states[-1] if len(provider_states) == 1 else "MIXED",
        }
        container_states = [instance.container_started for instance in instances]
        if any(item is True for item in container_states):
            phases["container_start"] = {"state": "confirmed"}
        elif container_states and all(item is False for item in container_states):
            phases["container_start"] = {"state": "not_started"}

    managed_ids = {instance.id for instance in instances}
    lease_ids = set((identities or {}).values()) or {
        str((item.get("resources") or {}).get("lease_id") or item.get("lease_id") or "")
        for item in events
    } - {""}
    matching_workers = []
    for item in workers:
        provider = str(item.get("provider") or "")
        worker_id = str(
            item.get("instance_id") or item.get("pod_id") or item.get("worker_id") or ""
        )
        key = (provider, worker_id)
        worker_lease = str(item.get("lease_id") or "")
        expected_lease = (identities or {}).get(key)
        if (
            key in (identities or {})
            and bool(worker_lease)
            and worker_lease == expected_lease
        ):
            matching_workers.append(item)
    if matching_workers:
        phases["runner_callback"] = {"state": "confirmed"}
    ready_event = any(
        str(item.get("type") or "") == "runner_ready"
        and str((item.get("resources") or {}).get("lease_id") or item.get("lease_id") or "") in lease_ids
        and str((item.get("resources") or {}).get("worker_instance_id") or (item.get("resources") or {}).get("pod_id") or "") in managed_ids
        for item in events
    )
    if any(str(item.get("status") or "") == "active" for item in matching_workers) or ready_event:
        phases["comfyui_readiness"] = {"state": "confirmed"}


def _runner_is_ready(phases: dict[str, dict[str, Any]]) -> bool:
    return phases["comfyui_readiness"].get("state") == "confirmed"


class BenchmarkRunner:
    def __init__(self, driver: BenchmarkDriver):
        self.driver = driver

    def _deadline_call(
        self, name: str, *args: Any, absolute_deadline: float
    ) -> Any:
        guarded = getattr(self.driver, f"{name}_until", None)
        if callable(guarded):
            return guarded(*args, absolute_deadline=absolute_deadline)
        return getattr(self.driver, name)(*args)

    def run(self, plan: BenchmarkPlan) -> dict[str, Any]:
        campaign_started_at = _utc_now()
        campaign_started = self.driver.monotonic()
        campaign_deadline = campaign_started + plan.limits.max_campaign_seconds
        baseline_error: str | None = None
        guarded_inventory = getattr(self.driver, "inventory_until", None)
        if not callable(guarded_inventory):
            baseline = {provider: {} for provider in plan.providers}
            baseline_error = "TimeoutError"
        else:
            try:
                if self.driver.monotonic() >= campaign_deadline:
                    raise TimeoutError("campaign_deadline")
                baseline = guarded_inventory(
                    plan.providers, absolute_deadline=campaign_deadline
                )
                if self.driver.monotonic() >= campaign_deadline:
                    raise TimeoutError("campaign_deadline")
            except TimeoutError:
                baseline = {provider: {} for provider in plan.providers}
                baseline_error = "TimeoutError"
        baseline_managed = _managed_ids(baseline)
        if plan.exclusive and baseline_managed:
            names = ", ".join(
                f"{provider}:{instance}"
                for provider, instance in sorted(baseline_managed)
            )
            raise RuntimeError(
                "Exclusive benchmark refused to start with active Cloud Offload "
                f"instances: {names}"
            )

        results: list[dict[str, Any]] = []
        estimated_total = 0.0
        campaign_abort: str | None = (
            "campaign_runtime_limit" if baseline_error else None
        )
        base_manifest_ready = False
        for scenario in plan.scenarios:
            if campaign_abort is not None:
                break
            elapsed = self.driver.monotonic() - campaign_started
            if elapsed >= plan.limits.max_campaign_seconds:
                campaign_abort = "campaign_runtime_limit"
                break
            if estimated_total >= plan.limits.max_total_cost_usd:
                campaign_abort = "campaign_cost_limit"
                break
            if scenario.fresh_instance and not self._wait_for_worker_quiescence(
                plan, campaign_started
            ):
                campaign_abort = "fresh_worker_quiescence_timeout"
                break
            if (
                scenario.failure
                and scenario.failure.kind == "corruption"
                and not base_manifest_ready
            ):
                result = self._dependent_failure_result(
                    scenario,
                    "corruption requires a verified base manifest from the cache registry",
                )
            else:
                result = self._run_scenario(
                    plan,
                    scenario,
                    baseline,
                    campaign_started,
                    estimated_total,
                )
            results.append(result)
            if scenario.cache_state in {"cold", "hot"}:
                if scenario.cache_state == "cold":
                    base_manifest_ready = False
                base_manifest = None
                if result.get("passed"):
                    checker = getattr(self.driver, "base_manifest", None)
                    if callable(checker):
                        try:
                            base_manifest = checker(result)
                        except Exception:  # noqa: BLE001 - absent registry proof fails closed
                            base_manifest = None
                    result["base_manifest_identity"] = base_manifest
                base_manifest_ready = base_manifest_ready or (
                    bool(result.get("passed")) and bool(base_manifest)
                )
            estimated_total += float(result["estimated_compute_cost_upper_usd"])
            if result.get("orphaned_resources"):
                campaign_abort = "orphan_cleanup_failed"
                break
            if result.get("limit_triggered") == "campaign_cost_limit":
                campaign_abort = "campaign_cost_limit"
                break
            if not result.get("passed"):
                campaign_abort = str(
                    result.get("abort_reason")
                    or result.get("limit_triggered")
                    or "unexpected_scenario_failure"
                )
                break

        campaign_resources = {
            (str(item["provider"]), str(item["instance_id"])): _ResourceMeter(
                id=str(item["instance_id"]),
                provider=str(item["provider"]),
                hourly_rate=float(item.get("hourly_rate") or 0),
                first_seen=campaign_started,
                last_seen=self.driver.monotonic(),
                source=str(item.get("source") or "journal"),
            )
            for result in results
            for item in result.get("resources") or []
        }
        final_cleanup = (
            []
            if baseline_error and not campaign_resources
            else self._cleanup_resources(
                plan.providers,
                baseline,
                campaign_resources,
                plan.limits.cleanup_timeout_seconds,
                include_untracked=False,
            )
        )
        final_cleanup_interrupt = next(
            (
                item.get("cleanup_interrupt")
                for item in final_cleanup
                if item.get("cleanup_interrupt")
            ),
            None,
        )
        if final_cleanup_interrupt:
            campaign_abort = campaign_abort or (
                f"operator_interrupt:{final_cleanup_interrupt}"
            )
        # The cleanup receipt is the bounded final provider audit. Do not start
        # a second unbounded provider read after its proof deadline.
        orphaned = sorted(
            (str(item["provider"]), str(item["instance_id"]))
            for item in final_cleanup
            if not item.get("provider_absent")
        )
        audit_errors = [
            str(item["inventory_error"])
            for item in final_cleanup
            if item.get("inventory_error")
        ]
        final_audit_error = baseline_error or (
            audit_errors[0] if audit_errors else None
        )
        unknown_paid_resources = [
            item
            for result in results
            for item in result.get("unknown_paid_resources") or []
        ]
        passed = (
            campaign_abort is None
            and not orphaned
            and final_audit_error is None
            and len(results) == len(plan.scenarios)
            and all(result["passed"] for result in results)
        )
        scorecard = {
            "schema": SCORECARD_SCHEMA,
            "started_at": campaign_started_at,
            "completed_at": _utc_now(),
            "plan": plan.safe_summary(),
            "passed": passed,
            "campaign_abort": campaign_abort,
            "estimated_compute_cost_upper_usd": round(
                (
                    plan.limits.max_total_cost_usd
                    if orphaned or unknown_paid_resources or final_audit_error
                    else estimated_total
                ),
                6,
            ),
            "limits": {
                "max_total_cost_usd": plan.limits.max_total_cost_usd,
                "max_scenario_cost_usd": plan.limits.max_scenario_cost_usd,
                "max_campaign_seconds": plan.limits.max_campaign_seconds,
                "fresh_worker_timeout_seconds": plan.limits.fresh_worker_timeout_seconds,
                "runner_readiness_timeout_seconds": plan.limits.runner_readiness_timeout_seconds,
                "max_runner_readiness_cost_usd": plan.limits.max_runner_readiness_cost_usd,
            },
            "results": results,
            "distributions": self._score_distributions(results),
            "final_cleanup": final_cleanup,
            "final_audit_error": final_audit_error,
            "cleanup_proof": {
                "state": (
                    "confirmed"
                    if final_audit_error is None
                    and not orphaned
                    and not unknown_paid_resources
                    else "failed"
                ),
                "verified_remaining_count": len(orphaned),
                "unknown_paid_resource_count": len(unknown_paid_resources),
            },
            "orphaned_resources": [
                {"provider": provider, "instance_id": instance}
                for provider, instance in orphaned
            ] + unknown_paid_resources,
        }
        return scorecard

    @staticmethod
    def _dependent_failure_result(
        scenario: BenchmarkScenario, reason: str
    ) -> dict[str, Any]:
        """Record a dependent canary failure without running its hook."""

        now = _utc_now()
        return {
            "schema": SCENARIO_SCHEMA,
            "name": scenario.name,
            "cache_state": scenario.cache_state,
            "request_digest": _canonical_digest(scenario.request),
            "started_at": now,
            "completed_at": now,
            "duration_seconds": 0.0,
            "resource_closure_seconds": 0.0,
            "cancellation_to_provider_absence_seconds": None,
            "revocation_to_provider_absence_seconds": None,
            "job_id": None,
            "status": None,
            "expected_statuses": list(scenario.expected_statuses),
            "passed": False,
            "fresh_instance_required": scenario.fresh_instance,
            "fresh_instance_observed": False,
            "scenario_preparation": None,
            "scenario_restoration": None,
            "event_count": 0,
            "event_cursor": 0,
            "phase_durations_seconds": {},
            "preparation_seconds": None,
            "estimated_compute_cost_upper_usd": 0.0,
            "resources": [],
            "orphaned_resources": [],
            "failure_injection": {
                "kind": scenario.failure.kind if scenario.failure else None,
                "triggered": False,
                "dependency_failure": reason,
            },
            "limit_triggered": None,
            "harness_error": f"Dependency failure: {reason}",
            "support_bundle": None,
            "submission_receipt": None,
        }

    def _wait_for_worker_quiescence(
        self, plan: BenchmarkPlan, campaign_started: float
    ) -> bool:
        deadline = min(
            campaign_started + plan.limits.max_campaign_seconds,
            self.driver.monotonic() + plan.limits.fresh_worker_timeout_seconds,
        )
        guarded = getattr(self.driver, "active_workers_until", None)
        if not callable(guarded):
            raise TimeoutError("deadline_aware_worker_inventory_required")
        while True:
            if self.driver.monotonic() >= deadline:
                return False
            workers = guarded(plan.providers, absolute_deadline=deadline)
            if self.driver.monotonic() >= deadline:
                return False
            if not workers:
                return True
            remaining = deadline - self.driver.monotonic()
            if remaining <= 0:
                return False
            self.driver.sleep(min(plan.limits.poll_seconds, remaining))
        return True

    def _run_scenario(
        self,
        plan: BenchmarkPlan,
        scenario: BenchmarkScenario,
        campaign_baseline: dict[str, dict[str, InstanceObservation]],
        campaign_started: float,
        cost_before: float,
    ) -> dict[str, Any]:
        started_at = _utc_now()
        started = self.driver.monotonic()
        readiness_deadline = started + plan.limits.runner_readiness_timeout_seconds
        self._deadline_call(
            "inventory", plan.providers, absolute_deadline=readiness_deadline
        )
        events: list[dict[str, Any]] = []
        cursor = 0
        resources: dict[tuple[str, str], _ResourceMeter] = {}
        identities: dict[tuple[str, str], str] = {}
        unverified_resources: dict[tuple[str, str], dict[str, Any]] = {}
        last_provider = plan.providers[0]
        last_rate = 0.0
        job_id: str | None = None
        status: str | None = None
        failure_result: dict[str, Any] | None = None
        limit_triggered: str | None = None
        harness_error: str | None = None
        fatal_error: BaseException | None = None
        support_bundle: dict[str, Any] | None = None
        submission_receipt: dict[str, Any] | None = None
        preparation: dict[str, Any] | None = None
        restoration: dict[str, Any] | None = None
        failure_preparation: dict[str, Any] | None = None
        failure_cleanup: dict[str, Any] | None = None
        abort_reason: str | None = None
        interrupt_cancellation: dict[str, Any] | None = None
        startup_phases = _initial_startup_phases()

        try:
            preparation = self.driver.prepare_scenario(scenario)
            if scenario.failure and scenario.failure.before_submit:
                failure_preparation = self.driver.run_hook(
                    scenario.failure,
                    self._hook_context(
                        scenario,
                        stage="prepare",
                        job_id=None,
                        resources=resources,
                    ),
                )
                if failure_preparation.get("exit_code") != 0:
                    raise RuntimeError("Pre-submit failure hook did not succeed")
            # Preparation and reviewed hooks are part of runner-readiness time.
            # Check again at the last point before any provider mutation.
            if (
                self.driver.monotonic() - started
                >= plan.limits.runner_readiness_timeout_seconds
            ):
                limit_triggered = "runner_readiness_timeout"
                raise _PreSubmitLimit
            guard = getattr(self.driver, "configure_submission_guard", None)
            if callable(guard):
                guard(
                    absolute_deadline=(
                        started + plan.limits.runner_readiness_timeout_seconds
                    ),
                    max_cost_usd=min(
                        plan.limits.max_scenario_cost_usd,
                        float(plan.limits.max_runner_readiness_cost_usd or 0),
                        max(0.0, plan.limits.max_total_cost_usd - cost_before),
                    ),
                )
            job_id = self.driver.submit(scenario)
            receipt_reader = getattr(self.driver, "submission_receipt", None)
            if callable(receipt_reader):
                submission_receipt = receipt_reader(job_id)
            while True:
                now = self.driver.monotonic()
                elapsed = now - started
                readiness_deadline = (
                    started + plan.limits.runner_readiness_timeout_seconds
                )
                scenario_deadline = started + scenario.timeout_seconds
                campaign_deadline = (
                    campaign_started + plan.limits.max_campaign_seconds
                )
                operation_deadline = min(
                    readiness_deadline, scenario_deadline, campaign_deadline
                )
                if now >= operation_deadline:
                    limit_triggered = (
                        "runner_readiness_timeout"
                        if readiness_deadline <= min(scenario_deadline, campaign_deadline)
                        else (
                            "scenario_timeout"
                            if scenario_deadline <= campaign_deadline
                            else "campaign_runtime_limit"
                        )
                    )
                    self.driver.cancel(job_id)
                    break
                new_events = self._deadline_call(
                    "events", job_id, cursor, absolute_deadline=operation_deadline
                )
                if new_events:
                    events.extend(new_events)
                    cursor = max(int(item.get("sequence") or 0) for item in events)
                for item in new_events:
                    event_resources = item.get("resources") or {}
                    provider = str(event_resources.get("provider") or last_provider)
                    rate_value = event_resources.get("hourly_rate")
                    if rate_value is not None:
                        last_rate = max(0.0, float(rate_value))
                    last_provider = provider
                    instance_id = event_resources.get(
                        "worker_instance_id"
                    ) or event_resources.get("pod_id")
                    lease_id = str(
                        event_resources.get("lease_id")
                        or item.get("lease_id")
                        or ""
                    )
                    key = (provider, str(instance_id)) if instance_id else None
                    if key is not None and key in resources and not lease_id:
                        # An incomplete later event cannot demote an identity
                        # that this job already proved.
                        meter = resources[key]
                        meter.last_seen = now
                        meter.hourly_rate = max(meter.hourly_rate, last_rate)
                        unverified_resources.pop(key, None)
                        continue
                    if instance_id and not lease_id:
                        key = (provider, str(instance_id))
                        unverified_resources[key] = {
                            "provider": provider,
                            "instance_id": str(instance_id),
                            "hourly_rate": max(
                                float(
                                    unverified_resources.get(key, {}).get(
                                        "hourly_rate", 0
                                    )
                                ),
                                last_rate,
                            ),
                            "ownership_state": "unknown",
                            "provider_absent": False,
                            "termination_attempts": 0,
                            "first_seen": unverified_resources.get(key, {}).get(
                                "first_seen", now
                            ),
                        }
                        resolver = getattr(
                            self.driver, "resolve_resource_identity", None
                        )
                        try:
                            proof = (
                                resolver(job_id, provider, str(instance_id))
                                if callable(resolver)
                                else None
                            )
                        except Exception:  # failed proof remains unknown and paid
                            proof = None
                        if (
                            isinstance(proof, dict)
                            and str(proof.get("job_id") or "") == job_id
                            and str(proof.get("provider") or "") == provider
                            and str(proof.get("instance_id") or "")
                            == str(instance_id)
                            and str(proof.get("lease_id") or "")
                        ):
                            lease_id = str(proof["lease_id"])
                    if instance_id and lease_id:
                        key = (provider, str(instance_id))
                        if (
                            key in identities and identities[key] != lease_id
                        ) or (
                            lease_id in identities.values() and key not in identities
                        ):
                            continue
                        identities[key] = lease_id
                        unverified_resources.pop(key, None)
                        meter = resources.get(key)
                        if meter is None:
                            resources[key] = _ResourceMeter(
                                id=str(instance_id),
                                provider=provider,
                                hourly_rate=last_rate,
                                first_seen=started,
                                last_seen=now,
                                source="journal",
                                job_id=job_id,
                                lease_id=lease_id,
                            )
                        else:
                            meter.last_seen = now
                            meter.hourly_rate = max(meter.hourly_rate, last_rate)
                    elif instance_id:
                        key = (provider, str(instance_id))
                        unverified_resources[key] = {
                            "provider": provider,
                            "instance_id": str(instance_id),
                            "hourly_rate": max(
                                float(
                                    unverified_resources.get(key, {}).get(
                                        "hourly_rate", 0
                                    )
                                ),
                                last_rate,
                            ),
                            "ownership_state": "unknown",
                            "provider_absent": False,
                            "termination_attempts": 0,
                            "first_seen": unverified_resources.get(key, {}).get(
                                "first_seen", now
                            ),
                        }

                if self.driver.monotonic() >= operation_deadline:
                    limit_triggered = "runner_readiness_timeout"
                    self.driver.cancel(job_id)
                    break

                current_inventory = (
                    self._deadline_call(
                        "inventory",
                        plan.providers,
                        absolute_deadline=operation_deadline,
                    )
                    if plan.exclusive
                    else {provider: {} for provider in plan.providers}
                )
                if plan.exclusive:
                    for key, meter in resources.items():
                        observation = current_inventory.get(key[0], {}).get(key[1])
                        if observation is not None:
                            meter.last_seen = now
                            # Provider inventory is authoritative for a known
                            # running Pod. It must override a zero journal rate.
                            meter.hourly_rate = max(
                                meter.hourly_rate, observation.hourly_rate
                            )
                            if (
                                meter.hourly_rate <= 0
                                and str(observation.status).lower()
                                in {"running", "starting", "provisioning"}
                            ):
                                # A known paid resource with an unknown rate is
                                # charged at a ceiling-derived rate. Zero is not
                                # a safe estimate for a running provider Pod.
                                meter.hourly_rate = (
                                    min(
                                        plan.limits.max_scenario_cost_usd,
                                        float(
                                            plan.limits.max_runner_readiness_cost_usd
                                            or plan.limits.max_scenario_cost_usd
                                        ),
                                    )
                                    * 3600
                                    / max(plan.limits.poll_seconds, 0.001)
                                )
                                meter.source = "provider_inventory_conservative"
                    for key, unknown in unverified_resources.items():
                        observation = current_inventory.get(key[0], {}).get(key[1])
                        if observation is not None:
                            unknown["hourly_rate"] = max(
                                float(unknown.get("hourly_rate") or 0),
                                observation.hourly_rate,
                            )
                            if (
                                float(unknown.get("hourly_rate") or 0) <= 0
                                and str(observation.status).lower()
                                in {"running", "starting", "provisioning"}
                            ):
                                unknown["hourly_rate"] = (
                                    float(
                                        plan.limits.max_runner_readiness_cost_usd
                                        or plan.limits.max_scenario_cost_usd
                                    )
                                    * 3600
                                    / max(plan.limits.poll_seconds, 0.001)
                                )

                if self.driver.monotonic() >= operation_deadline:
                    limit_triggered = "runner_readiness_timeout"
                    self.driver.cancel(job_id)
                    break

                try:
                    current_workers = self._deadline_call(
                        "active_workers",
                        plan.providers,
                        absolute_deadline=operation_deadline,
                    )
                except Exception:  # noqa: BLE001 - unavailable facts stay unknown
                    current_workers = []
                _observe_startup_phases(
                    startup_phases,
                    current_inventory,
                    current_workers,
                    events,
                    identities,
                )
                if self.driver.monotonic() >= operation_deadline:
                    limit_triggered = "runner_readiness_timeout"
                    self.driver.cancel(job_id)
                    break

                estimated = self._estimated_cost(resources, now)
                estimated += sum(
                    max(0.0, now - float(item.get("first_seen") or started))
                    * float(item.get("hourly_rate") or 0)
                    / 3600
                    for item in unverified_resources.values()
                )
                if not resources and not unverified_resources and last_rate:
                    estimated = last_rate * elapsed / 3600
                if estimated >= plan.limits.max_scenario_cost_usd:
                    limit_triggered = "scenario_cost_limit"
                    self.driver.cancel(job_id)
                    break
                if cost_before + estimated >= plan.limits.max_total_cost_usd:
                    limit_triggered = "campaign_cost_limit"
                    self.driver.cancel(job_id)
                    break
                if (
                    not _runner_is_ready(startup_phases)
                    and estimated
                    >= float(plan.limits.max_runner_readiness_cost_usd or 0)
                ):
                    limit_triggered = "runner_readiness_cost_limit"
                    self.driver.cancel(job_id)
                    break
                if (
                    not _runner_is_ready(startup_phases)
                    and elapsed >= plan.limits.runner_readiness_timeout_seconds
                ):
                    limit_triggered = "runner_readiness_timeout"
                    self.driver.cancel(job_id)
                    break
                if now - campaign_started >= plan.limits.max_campaign_seconds:
                    limit_triggered = "campaign_runtime_limit"
                    self.driver.cancel(job_id)
                    break
                if elapsed >= scenario.timeout_seconds:
                    limit_triggered = "scenario_timeout"
                    self.driver.cancel(job_id)
                    break

                snapshot = self._deadline_call(
                    "snapshot", job_id, absolute_deadline=operation_deadline
                )
                status = str(snapshot.get("status") or "")
                if self.driver.monotonic() >= operation_deadline:
                    limit_triggered = "runner_readiness_timeout"
                    self.driver.cancel(job_id)
                    break
                if scenario.failure and failure_result is None:
                    failure_result = self._maybe_inject(
                        scenario,
                        snapshot,
                        events,
                        resources,
                        elapsed,
                        job_id,
                        failure_preparation,
                    )
                if status in TERMINAL_STATUSES:
                    break
                readiness_deadline = (
                    started + plan.limits.runner_readiness_timeout_seconds
                )
                scenario_deadline = started + scenario.timeout_seconds
                campaign_deadline = (
                    campaign_started + plan.limits.max_campaign_seconds
                )
                remaining = min(
                    readiness_deadline, scenario_deadline, campaign_deadline
                ) - self.driver.monotonic()
                if remaining <= 0:
                    continue
                self.driver.sleep(min(plan.limits.poll_seconds, remaining))
        except _PreSubmitLimit:
            pass
        except (KeyboardInterrupt, SystemExit) as exc:
            # Paid resource cleanup and config restoration still run before the
            # aborted scorecard and release ledger are published.
            fatal_error = exc
            abort_reason = f"operator_interrupt:{type(exc).__name__}"
            if job_id:
                try:
                    response = self.driver.cancel(job_id)
                    interrupt_cancellation = {
                        "accepted": bool(response.get("accepted")),
                        "status_code": response.get("status_code"),
                    }
                except Exception as cancel_exc:  # noqa: BLE001
                    interrupt_cancellation = {
                        "accepted": False,
                        "error_type": type(cancel_exc).__name__,
                    }
        except Exception as exc:  # noqa: BLE001 - preserve cleanup on harness faults
            harness_error = type(exc).__name__
        finally:
            if job_id:
                try:
                    support_bundle = _project_support_bundle(
                        self.driver.support_bundle(job_id)
                    )
                except (KeyboardInterrupt, SystemExit) as exc:
                    fatal_error = fatal_error or exc
                    abort_reason = abort_reason or f"operator_interrupt:{type(exc).__name__}"
                except Exception as exc:  # noqa: BLE001
                    support_bundle = {
                        "schema": "cloud-offload.support-bundle-error.v1",
                        "error": type(exc).__name__,
                    }

        if scenario.failure and scenario.failure.before_submit and failure_preparation:
            try:
                failure_cleanup = self.driver.run_hook(
                    scenario.failure,
                    self._hook_context(
                        scenario,
                        stage="cleanup",
                        job_id=job_id,
                        resources=resources,
                    ),
                )
            except (KeyboardInterrupt, SystemExit) as exc:
                fatal_error = fatal_error or exc
                abort_reason = abort_reason or (
                    f"operator_interrupt:{type(exc).__name__}"
                )
                failure_cleanup = {
                    "exit_code": None,
                    "error_type": type(exc).__name__,
                    "output_omitted": True,
                }
            except Exception as exc:  # noqa: BLE001 - cleanup must remain visible
                failure_cleanup = {
                    "exit_code": None,
                    "error_type": type(exc).__name__,
                    "output_omitted": True,
                }
            if failure_result is None:
                failure_result = {
                    "kind": scenario.failure.kind,
                    "triggered": False,
                    "preparation_hook": failure_preparation,
                }
            failure_result["cleanup_hook"] = failure_cleanup

        # Capture the last provider and worker facts before deletion. A failed
        # read leaves an explicit unknown value and cannot block cleanup.
        final_observation_deadline = min(
            readiness_deadline,
            started + scenario.timeout_seconds,
            campaign_started + plan.limits.max_campaign_seconds,
        )
        try:
            if self.driver.monotonic() >= final_observation_deadline:
                raise TimeoutError("operation_deadline")
            final_startup_inventory = self._deadline_call(
                "inventory",
                plan.providers,
                absolute_deadline=final_observation_deadline,
            )
            try:
                if self.driver.monotonic() >= final_observation_deadline:
                    raise TimeoutError("operation_deadline")
                final_startup_workers = self._deadline_call(
                    "active_workers",
                    plan.providers,
                    absolute_deadline=final_observation_deadline,
                )
            except Exception:  # noqa: BLE001
                final_startup_workers = []
            _observe_startup_phases(
                startup_phases,
                final_startup_inventory,
                final_startup_workers,
                events,
                identities,
            )
        except (KeyboardInterrupt, SystemExit) as exc:
            fatal_error = fatal_error or exc
            abort_reason = abort_reason or f"operator_interrupt:{type(exc).__name__}"
        except Exception:  # noqa: BLE001
            pass
        try:
            active_completed = self.driver.monotonic()
        except (KeyboardInterrupt, SystemExit) as exc:
            fatal_error = fatal_error or exc
            abort_reason = abort_reason or f"operator_interrupt:{type(exc).__name__}"
            active_completed = started
        cleanup_started = active_completed
        cleanup = self._cleanup_resources(
            plan.providers,
            campaign_baseline,
            resources,
            plan.limits.cleanup_timeout_seconds,
            include_untracked=False,
        )
        cleanup_interrupt = next(
            (item.get("cleanup_interrupt") for item in cleanup if item.get("cleanup_interrupt")),
            None,
        )
        if cleanup_interrupt:
            abort_reason = abort_reason or f"operator_interrupt:{cleanup_interrupt}"
        try:
            restoration = self.driver.restore_scenario(scenario)
        except (KeyboardInterrupt, SystemExit) as exc:
            abort_reason = abort_reason or f"operator_interrupt:{type(exc).__name__}"
            restoration = {"required": True, "restored": False, "error_type": type(exc).__name__}
        except Exception as exc:  # noqa: BLE001 - report without leaking config
            restoration = {
                "required": scenario.prepared_storage_policy is not None,
                "restored": False,
                "error_type": type(exc).__name__,
            }
        try:
            completed = self.driver.monotonic()
        except (KeyboardInterrupt, SystemExit) as exc:
            fatal_error = fatal_error or exc
            abort_reason = abort_reason or f"operator_interrupt:{type(exc).__name__}"
            completed = active_completed
        cleanup_duration = max(0.0, completed - cleanup_started)
        estimated_cost = self._estimated_cost(resources, completed)
        estimated_cost += sum(
            max(0.0, completed - float(item.get("first_seen") or started))
            * float(item.get("hourly_rate") or 0)
            / 3600
            for item in unverified_resources.values()
        )
        orphaned_resources = [
            item for item in cleanup if not item.get("provider_absent")
        ] + list(unverified_resources.values())
        if orphaned_resources:
            estimated_cost = max(estimated_cost, plan.limits.max_scenario_cost_usd)
        if fatal_error is None and job_id and status not in TERMINAL_STATUSES:
            try:
                status = str(self.driver.snapshot(job_id).get("status") or status or "")
            except Exception:  # noqa: BLE001
                pass
        failure_expected = scenario.failure is not None
        failure_triggered = failure_result is not None and failure_result.get(
            "triggered"
        )
        failure_succeeded = self._failure_injection_succeeded(failure_result)
        passed = (
            fatal_error is None
            and abort_reason is None
            and harness_error is None
            and limit_triggered is None
            and status in scenario.expected_statuses
            and not orphaned_resources
            and bool((preparation or {}).get("prepared"))
            and bool((restoration or {}).get("restored"))
            and (not failure_expected or (failure_triggered and failure_succeeded))
            and (not scenario.fresh_instance or bool(resources))
        )
        completed_at = _utc_now()
        return {
            "schema": SCENARIO_SCHEMA,
            "name": scenario.name,
            "cache_state": scenario.cache_state,
            "request_digest": _canonical_digest(scenario.request),
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_seconds": round(max(0.0, completed - started), 6),
            "scenario_active_seconds": round(
                max(0.0, active_completed - started), 6
            ),
            "resource_closure_seconds": round(cleanup_duration, 6),
            "cancellation_to_provider_absence_seconds": _seconds_from_event_to(
                events, "cancellation_requested", completed_at
            ),
            "revocation_to_provider_absence_seconds": _seconds_from_event_to(
                events, "lease_revoked", completed_at
            ),
            "job_id": job_id,
            "status": status,
            "expected_statuses": list(scenario.expected_statuses),
            "passed": passed,
            "fresh_instance_required": scenario.fresh_instance,
            "fresh_instance_observed": bool(resources),
            "scenario_preparation": preparation,
            "scenario_restoration": restoration,
            "event_count": len(events),
            "event_cursor": cursor,
            "phase_durations_seconds": _phase_durations(events),
            "preparation_seconds": _preparation_seconds(events),
            "estimated_compute_cost_upper_usd": round(estimated_cost, 6),
            "resources": [
                {
                    "provider": meter.provider,
                    "instance_id": meter.id,
                    "hourly_rate": meter.hourly_rate,
                    "source": meter.source,
                    "lease_id": meter.lease_id,
                }
                for meter in sorted(
                    resources.values(), key=lambda item: (item.provider, item.id)
                )
            ],
            "failure_injection": failure_result,
            "limit_triggered": limit_triggered,
            "harness_error": harness_error,
            "abort_reason": abort_reason,
            "interrupt_cancellation": interrupt_cancellation,
            "startup_phases": startup_phases,
            "cleanup": cleanup,
            "orphaned_resources": orphaned_resources,
            "unknown_paid_resources": list(unverified_resources.values()),
            "support_bundle": support_bundle,
            "submission_receipt": submission_receipt,
        }

    @staticmethod
    def _failure_injection_succeeded(result: dict[str, Any] | None) -> bool:
        if not result or not result.get("triggered"):
            return False
        if result.get("kind") == "cancellation":
            return bool((result.get("response") or {}).get("accepted"))
        if result.get("kind") == "provider":
            receipts = result.get("receipts") or []
            return bool(receipts) and all(
                item.get("termination_requested") for item in receipts
            )
        hook_succeeded = (result.get("hook") or {}).get("exit_code") == 0
        preparation_succeeded = (result.get("preparation_hook") or {}).get(
            "exit_code", 0
        ) == 0
        cleanup_succeeded = (result.get("cleanup_hook") or {}).get("exit_code", 0) == 0
        return hook_succeeded and preparation_succeeded and cleanup_succeeded

    def _maybe_inject(
        self,
        scenario: BenchmarkScenario,
        snapshot: dict[str, Any],
        events: list[dict[str, Any]],
        resources: dict[tuple[str, str], _ResourceMeter],
        elapsed: float,
        job_id: str,
        failure_preparation: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        injection = scenario.failure
        if injection is None or elapsed < injection.after_seconds:
            return None
        phases = {str(item.get("phase")) for item in events if item.get("phase")}
        phases.add(str(snapshot.get("lifecycle_phase") or ""))
        event_types = {str(item.get("type")) for item in events if item.get("type")}
        if injection.trigger_phase and injection.trigger_phase not in phases:
            return None
        if injection.trigger_event and injection.trigger_event not in event_types:
            return None
        if injection.kind == "cancellation":
            response = self.driver.cancel(job_id)
            return {"kind": injection.kind, "triggered": True, "response": response}
        if injection.kind == "provider":
            if not resources:
                return None
            receipts = [
                {
                    "provider": meter.provider,
                    "instance_id": meter.id,
                    "termination_requested": self.driver.terminate(
                        meter.provider, meter.id
                    ),
                }
                for meter in resources.values()
            ]
            return {"kind": injection.kind, "triggered": True, "receipts": receipts}
        result = {
            "kind": injection.kind,
            "triggered": True,
            "hook": self.driver.run_hook(
                injection,
                self._hook_context(
                    scenario,
                    stage="observe",
                    job_id=job_id,
                    resources=resources,
                ),
            ),
        }
        if failure_preparation is not None:
            result["preparation_hook"] = failure_preparation
        return result

    @staticmethod
    def _hook_context(
        scenario: BenchmarkScenario,
        *,
        stage: str,
        job_id: str | None,
        resources: dict[tuple[str, str], _ResourceMeter],
    ) -> dict[str, str]:
        injection = scenario.failure
        assets = (scenario.request.get("partition") or {}).get("assets") or []
        digests = sorted(
            {
                str(item.get("sha256") or "").lower()
                for item in assets
                if isinstance(item, dict)
                and len(str(item.get("sha256") or "")) == 64
                and all(
                    character in "0123456789abcdefABCDEF"
                    for character in str(item.get("sha256") or "")
                )
            }
        )
        canary_nonce = ""
        if injection and injection.kind == "corruption":
            from cloud_offload.benchmark_faults import CORRUPTION_NONCE_FIELD

            canary_nonces = {
                str(item.get(CORRUPTION_NONCE_FIELD) or "")
                for item in assets
                if isinstance(item, dict) and item.get(CORRUPTION_NONCE_FIELD)
            }
            if len(canary_nonces) != 1:
                raise RuntimeError(
                    "Corruption benchmark requires exactly one campaign nonce"
                )
            canary_nonce = next(iter(canary_nonces))
        return {
            "CLOUD_OFFLOAD_BENCHMARK_JOB_ID": job_id or "",
            "CLOUD_OFFLOAD_BENCHMARK_SCENARIO": scenario.name,
            "CLOUD_OFFLOAD_BENCHMARK_FAILURE_KIND": (
                injection.kind if injection else ""
            ),
            "CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE": stage,
            "CLOUD_OFFLOAD_BENCHMARK_REQUEST_DIGEST": _canonical_digest(
                scenario.request
            ),
            "CLOUD_OFFLOAD_BENCHMARK_ASSET_DIGESTS": ",".join(digests),
            "CLOUD_OFFLOAD_BENCHMARK_CANARY_NONCE": canary_nonce,
            "CLOUD_OFFLOAD_BENCHMARK_PROFILE": str(
                ((scenario.request.get("partition") or {}).get("runner") or {}).get(
                    "profile"
                )
                or "comfyui-partition-v1"
            ),
            "CLOUD_OFFLOAD_BENCHMARK_ALLOWED_REGIONS": ",".join(
                scenario.allowed_regions
            ),
            "CLOUD_OFFLOAD_BENCHMARK_INSTANCE_IDS": ",".join(
                meter.id for meter in resources.values()
            ),
        }

    @staticmethod
    def _estimated_cost(
        resources: dict[tuple[str, str], _ResourceMeter], now: float
    ) -> float:
        return sum(
            max(0.0, now - meter.first_seen) * meter.hourly_rate / 3600
            for meter in resources.values()
        )

    def _cleanup_resources(
        self,
        providers: tuple[str, ...],
        baseline: dict[str, dict[str, InstanceObservation]],
        resources: dict[tuple[str, str], _ResourceMeter],
        timeout_seconds: float,
        *,
        include_untracked: bool = True,
    ) -> list[dict[str, Any]]:
        inventory_error = None
        cleanup_interrupt: str | None = None
        last_clock = 0.0

        def cleanup_now() -> float:
            nonlocal cleanup_interrupt, last_clock
            try:
                last_clock = max(last_clock, float(self.driver.monotonic()))
            except (KeyboardInterrupt, SystemExit) as exc:
                cleanup_interrupt = cleanup_interrupt or type(exc).__name__
            return last_clock
        deadline = cleanup_now() + timeout_seconds
        guarded_inventory = getattr(self.driver, "inventory_until", None)
        if not callable(guarded_inventory):
            raise TimeoutError("deadline_aware_provider_inventory_required")
        try:
            current = guarded_inventory(providers, absolute_deadline=deadline)
            if cleanup_now() >= deadline:
                current = {provider: {} for provider in providers}
                inventory_error = "TimeoutError"
        except (KeyboardInterrupt, SystemExit) as exc:
            current = {provider: {} for provider in providers}
            inventory_error = type(exc).__name__
            cleanup_interrupt = type(exc).__name__
        except Exception as exc:  # noqa: BLE001
            current = {provider: {} for provider in providers}
            inventory_error = type(exc).__name__
        baseline_ids = _managed_ids(baseline)
        candidates = set(resources)
        if include_untracked and inventory_error is None:
            candidates |= _managed_ids(current) - baseline_ids
        receipts = {
            key: {
                "provider": key[0],
                "instance_id": key[1],
                "termination_attempts": 0,
                "provider_absent": (
                    inventory_error is None
                    and key[1] not in current.get(key[0], {})
                ),
            }
            for key in candidates
            if key not in baseline_ids
        }
        # Every exact paid resource gets one termination request. The deadline
        # limits retries and proof waits, but never suppresses this first stop.
        for key, receipt in receipts.items():
            if receipt["provider_absent"]:
                continue
            receipt["termination_attempts"] = 1
            try:
                receipt["termination_requested"] = self.driver.terminate(*key)
            except (KeyboardInterrupt, SystemExit) as exc:
                receipt["termination_requested"] = False
                receipt["termination_error"] = type(exc).__name__
                cleanup_interrupt = cleanup_interrupt or type(exc).__name__
            except Exception as exc:  # noqa: BLE001
                receipt["termination_requested"] = False
                receipt["termination_error"] = type(exc).__name__
        # The attempt bound remains effective even when an interrupt prevents
        # the injected clock/sleep function from advancing.
        attempts_remaining = max(2, math.ceil(timeout_seconds) + 2)
        while (
            receipts
            and attempts_remaining > 0
            and cleanup_now() < deadline
        ):
            attempts_remaining -= 1
            try:
                inventory = guarded_inventory(providers, absolute_deadline=deadline)
                inventory_error = None
            except (KeyboardInterrupt, SystemExit) as exc:
                inventory = {provider: {} for provider in providers}
                inventory_error = type(exc).__name__
                cleanup_interrupt = cleanup_interrupt or type(exc).__name__
            except Exception as exc:  # noqa: BLE001
                inventory = {provider: {} for provider in providers}
                inventory_error = type(exc).__name__
            if cleanup_now() >= deadline:
                inventory_error = "TimeoutError"
                break
            pending = []
            for key, receipt in receipts.items():
                if inventory_error is None and key[1] not in inventory.get(key[0], {}):
                    receipt["provider_absent"] = True
                    continue
                pending.append(key)
                receipt["termination_attempts"] += 1
                try:
                    receipt["termination_requested"] = self.driver.terminate(*key)
                except (KeyboardInterrupt, SystemExit) as exc:
                    receipt["termination_requested"] = False
                    receipt["termination_error"] = type(exc).__name__
                    cleanup_interrupt = cleanup_interrupt or type(exc).__name__
                except Exception as exc:  # noqa: BLE001
                    receipt["termination_requested"] = False
                    receipt["termination_error"] = type(exc).__name__
            if not pending:
                break
            try:
                remaining = deadline - cleanup_now()
                if remaining <= 0:
                    break
                self.driver.sleep(min(2.0, remaining))
            except (KeyboardInterrupt, SystemExit) as exc:
                cleanup_interrupt = cleanup_interrupt or type(exc).__name__
        try:
            if cleanup_now() >= deadline:
                raise TimeoutError("cleanup_deadline")
            inventory = guarded_inventory(providers, absolute_deadline=deadline)
            inventory_error = None
        except (KeyboardInterrupt, SystemExit) as exc:
            cleanup_interrupt = cleanup_interrupt or type(exc).__name__
            try:
                if cleanup_now() >= deadline:
                    raise TimeoutError("cleanup_deadline")
                inventory = guarded_inventory(providers, absolute_deadline=deadline)
                inventory_error = None
            except BaseException as retry_exc:  # final bounded proof attempt
                inventory = {provider: {} for provider in providers}
                inventory_error = type(retry_exc).__name__
        except Exception as exc:  # noqa: BLE001
            inventory = {provider: {} for provider in providers}
            inventory_error = type(exc).__name__
        for key, receipt in receipts.items():
            receipt["provider_absent"] = inventory_error is None and key[
                1
            ] not in inventory.get(key[0], {})
            if inventory_error:
                receipt["inventory_error"] = inventory_error
            if cleanup_interrupt:
                receipt["cleanup_interrupt"] = cleanup_interrupt
        return [receipts[key] for key in sorted(receipts)]

    def _cleanup_new_managed(
        self,
        providers: tuple[str, ...],
        baseline: dict[str, dict[str, InstanceObservation]],
        timeout_seconds: float,
    ) -> list[dict[str, Any]]:
        return self._cleanup_resources(
            providers, baseline, {}, timeout_seconds, include_untracked=True
        )

    @staticmethod
    def _score_distributions(results: list[dict[str, Any]]) -> dict[str, Any]:
        distributions: dict[str, Any] = {}
        for cache_state in ("cold", "hot", "failure"):
            selected = [item for item in results if item["cache_state"] == cache_state]
            distributions[cache_state] = {
                "duration_seconds": _distribution(
                    [float(item["duration_seconds"]) for item in selected]
                ),
                "estimated_compute_cost_upper_usd": _distribution(
                    [
                        float(item["estimated_compute_cost_upper_usd"])
                        for item in selected
                    ]
                ),
            }
        phases = sorted(
            {
                phase
                for item in results
                for phase in item.get("phase_durations_seconds", {})
            }
        )
        distributions["phases_seconds"] = {
            phase: _distribution(
                [
                    float(item["phase_durations_seconds"][phase])
                    for item in results
                    if phase in item.get("phase_durations_seconds", {})
                ]
            )
            for phase in phases
        }
        distributions["resource_closure_seconds"] = _distribution(
            [float(item["resource_closure_seconds"]) for item in results]
        )
        return distributions


class CoordinatorBenchmarkDriver:
    """Production driver backed by the coordinator API and provider connectors."""

    def __init__(
        self,
        base_url: str,
        token: str | None,
        config: CloudConfig,
        providers: tuple[str, ...],
        *,
        allow_hooks: bool = False,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        self.connectors = {
            provider: create_connector(provider, config) for provider in providers
        }
        self.allow_hooks = allow_hooks
        self._prepared_storage_restore: dict[str, dict[str, dict[str, Any]]] = {}
        self._submission_receipts: dict[str, dict[str, Any]] = {}
        self._submission_deadline: float | None = None
        self._submission_cost_budget: float | None = None

    def configure_submission_guard(
        self, *, absolute_deadline: float, max_cost_usd: float
    ) -> None:
        self._submission_deadline = float(absolute_deadline)
        self._submission_cost_budget = max(0.0, float(max_cost_usd))

    def _request(
        self,
        method: str,
        path: str,
        *,
        retry_safe: bool = False,
        timeout: float = 30,
        absolute_deadline: float | None = None,
        **kwargs,
    ) -> requests.Response:
        deadline = time.monotonic() + 30
        if absolute_deadline is not None:
            deadline = min(deadline, float(absolute_deadline))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("operation_deadline")
            attempt_total = min(float(timeout), remaining)
            connect_timeout = min(5.0, attempt_total / 2)
            read_timeout = attempt_total - connect_timeout
            try:
                response = self.session.request(
                    method,
                    f"{self.base_url}{path}",
                    timeout=(connect_timeout, read_timeout),
                    **kwargs,
                )
                if not retry_safe or response.status_code not in {502, 503, 504}:
                    return response
            except requests.RequestException:
                if not retry_safe or time.monotonic() >= deadline:
                    raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return response
            time.sleep(min(1.0, remaining))

    @staticmethod
    def monotonic() -> float:
        return time.monotonic()

    @staticmethod
    def sleep(seconds: float) -> None:
        time.sleep(max(0.0, seconds))

    def inventory(
        self,
        providers: tuple[str, ...],
        *,
        absolute_deadline: float | None = None,
    ) -> dict[str, dict[str, InstanceObservation]]:
        result: dict[str, dict[str, InstanceObservation]] = {}
        for provider in providers:
            deadline = time.monotonic() + 30
            if absolute_deadline is not None:
                deadline = min(deadline, float(absolute_deadline))
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("provider_inventory_deadline")
                try:
                    connector = self.connectors[provider]
                    guarded = getattr(connector, "list_instances_until", None)
                    if callable(guarded):
                        instances = guarded(
                            absolute_deadline=deadline,
                            timeout_budget=remaining,
                        )
                    else:
                        instances = connector.list_instances()
                    break
                except Exception:  # noqa: BLE001 - bounded provider retry
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise
                    time.sleep(min(1.0, remaining))
            result[provider] = {
                instance.id: InstanceObservation(
                    id=instance.id,
                    provider=provider,
                    hourly_rate=max(0.0, float(instance.hourly_rate or 0)),
                    status=instance.status,
                    managed=str(instance.metadata.get("name") or "").startswith(
                        MANAGED_INSTANCE_PREFIX
                    ),
                    name=instance.metadata.get("name"),
                    provider_state=str(
                        instance.metadata.get("provider_state")
                        or instance.status
                        or "unknown"
                    ).upper(),
                    container_started=self.connectors[provider].container_started(
                        instance
                    ),
                )
                for instance in instances
            }
        return result

    def inventory_until(
        self, providers: tuple[str, ...], *, absolute_deadline: float
    ) -> dict[str, dict[str, InstanceObservation]]:
        return self.inventory(providers, absolute_deadline=absolute_deadline)

    def active_workers(self, providers: tuple[str, ...]) -> list[dict[str, Any]]:
        response = self._request(
            "GET", "/api/active-workers", retry_safe=True, timeout=30
        )
        response.raise_for_status()
        allowed = set(providers)
        return [
            item
            for item in response.json().get("workers") or []
            if str(item.get("provider") or "") in allowed
        ]

    def active_workers_until(
        self, providers: tuple[str, ...], *, absolute_deadline: float
    ) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            "/api/active-workers",
            retry_safe=True,
            timeout=30,
            absolute_deadline=absolute_deadline,
        )
        response.raise_for_status()
        allowed = set(providers)
        return [
            item
            for item in response.json().get("workers") or []
            if str(item.get("provider") or "") in allowed
        ]

    def _prepared_storage_config(self) -> dict[str, Any]:
        response = self._request("GET", "/api/config", retry_safe=True, timeout=30)
        response.raise_for_status()
        prepared = response.json().get("prepared_storage")
        if not isinstance(prepared, dict):
            raise RuntimeError("Coordinator returned no prepared-storage config")
        # The coordinator contract is finite JSON. Round-tripping makes the
        # private restoration copy independent from the response object.
        return json.loads(json.dumps(prepared, allow_nan=False))

    def _set_prepared_storage_config(self, prepared: dict[str, Any]) -> None:
        response = self._request(
            "POST",
            "/api/config",
            retry_safe=True,
            timeout=30,
            json={"prepared_storage": prepared},
        )
        response.raise_for_status()

    def prepare_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]:
        requested = scenario.prepared_storage_policy
        if requested is None:
            return {"required": False, "prepared": True}
        if scenario.name in self._prepared_storage_restore:
            raise RuntimeError("Scenario prepared-storage policy is already active")

        previous = self._prepared_storage_config()
        target = json.loads(json.dumps(previous, allow_nan=False))
        if requested == "off":
            target["policy"] = "off"
            target["enabled"] = False
        else:
            if not previous.get("confirmed"):
                raise RuntimeError(
                    "Hot benchmark requires previously confirmed prepared storage"
                )
            if not previous.get("existing_volume_id"):
                raise RuntimeError(
                    "Hot benchmark requires an already-bound prepared volume"
                )
            target["policy"] = requested
            target["enabled"] = True

        # Store the complete original before the idempotent POST. If the response
        # is lost after the coordinator applies it, restoration still knows both
        # legitimate states and can safely recover.
        self._prepared_storage_restore[scenario.name] = {
            "previous": previous,
            "target": target,
        }
        changed = target != previous
        if changed:
            self._set_prepared_storage_config(target)
        observed = self._prepared_storage_config()
        if observed != target:
            raise RuntimeError("Coordinator did not apply benchmark storage policy")
        return {
            "required": True,
            "prepared": True,
            "requested_policy": requested,
            "previous_policy": str(previous.get("policy") or ""),
            "changed": changed,
        }

    def restore_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]:
        saved = self._prepared_storage_restore.get(scenario.name)
        if saved is None:
            return {"required": False, "restored": True, "changed": False}
        previous = saved["previous"]
        target = saved["target"]
        current = self._prepared_storage_config()
        if current == previous:
            changed = False
        elif current == target:
            self._set_prepared_storage_config(previous)
            changed = True
        else:
            raise RuntimeError(
                "Prepared-storage config changed during benchmark; refusing to overwrite it"
            )
        observed = self._prepared_storage_config()
        if observed != previous:
            raise RuntimeError("Coordinator did not restore prepared-storage config")
        self._prepared_storage_restore.pop(scenario.name, None)
        return {
            "required": True,
            "restored": True,
            "restored_policy": str(previous.get("policy") or ""),
            "changed": changed,
        }

    def _ready_preflight(
        self, scenario: BenchmarkScenario, request_payload: dict[str, Any]
    ) -> dict[str, Any]:
        # Offer availability is transient: the offer a prior scenario's pod
        # occupied can take a short while to be listed again after that pod is
        # terminated. Retry a not-ready preflight while it reports no blockers,
        # since only current-offer availability can change on its own.
        deadline = time.monotonic() + PREFLIGHT_OFFER_RETRY_SECONDS
        if self._submission_deadline is not None:
            deadline = min(deadline, self._submission_deadline)
        workload_key = (
            "capsule" if scenario.endpoint == "/api/workflows" else "partition"
        )
        workload = request_payload.get(workload_key)
        if not isinstance(workload, dict) or not workload:
            raise RuntimeError(f"benchmark_{workload_key}_missing")
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("runner_readiness_deadline")
            preflight_response = self._request(
                "POST",
                "/api/preflight",
                json={
                    workload_key: workload,
                    "input_artifacts": request_payload.get("input_artifacts") or {},
                    "provider": request_payload.get("provider") or "auto",
                    "recommendation_policy": "balanced",
                    "allowed_regions": list(scenario.allowed_regions) or None,
                },
                timeout=max(0.001, min(120.0, remaining)),
            )
            preflight_response.raise_for_status()
            preflight = preflight_response.json()
            if preflight.get("status") in {"ready", "ready_with_preparation"}:
                return preflight
            blockers = preflight.get("blockers") or []
            remaining = deadline - time.monotonic()
            if not blockers and remaining > 0:
                time.sleep(
                    min(PREFLIGHT_OFFER_RETRY_INTERVAL_SECONDS, remaining)
                )
                continue
            if not blockers:
                raise TimeoutError("runner_readiness_deadline")
            codes = [
                str(item.get("code") or "unknown")
                for item in blockers or preflight.get("unknowns") or []
            ]
            raise RuntimeError(
                "Benchmark preflight is not ready"
                + (f" ({', '.join(codes)})" if codes else "")
            )

    def submit(self, scenario: BenchmarkScenario) -> str:
        # Submission is not transport-idempotent yet, so it is deliberately not
        # retried after an ambiguous connection failure.
        request_payload = dict(scenario.request)
        if scenario.endpoint not in SUBMISSION_ENDPOINTS:
            raise RuntimeError("unsupported_paid_submission_endpoint")
        preflight = self._ready_preflight(scenario, request_payload)
        candidate_id = (preflight.get("recommendation") or {}).get("candidate_id")
        if not candidate_id:
            raise RuntimeError("Benchmark preflight returned no recommendation")
        request_payload.update(
            {
                "preflight_id": preflight["preflight_id"],
                "manifest_digest": preflight["manifest_digest"],
                "candidate_id": candidate_id,
                "confirmation_action": "start_now",
            }
        )
        # This is the final local instruction before the provider-starting
        # coordinator POST. A long preflight cannot bypass the campaign guard.
        if (
            self._submission_deadline is not None
            and time.monotonic() >= self._submission_deadline
        ):
            raise TimeoutError("runner_readiness_deadline")
        if self._submission_cost_budget is None or self._submission_cost_budget <= 0:
            raise RuntimeError("runner_readiness_cost_budget")
        selected = [
            item
            for item in preflight.get("candidates") or []
            if str(item.get("candidate_id") or "") == str(candidate_id)
        ]
        if len(selected) != 1:
            raise RuntimeError("runner_readiness_quote_unproved")
        try:
            hourly_rate = float(selected[0].get("hourly_rate"))
        except (TypeError, ValueError):
            raise RuntimeError("runner_readiness_quote_unproved") from None
        if not math.isfinite(hourly_rate) or hourly_rate <= 0:
            raise RuntimeError("runner_readiness_quote_unproved")
        quoted_cost = hourly_rate * scenario.timeout_seconds / 3600
        if quoted_cost > self._submission_cost_budget:
            raise RuntimeError("runner_readiness_cost_budget")
        response = self._request(
            "POST",
            scenario.endpoint,
            json=request_payload,
            timeout=120,
        )
        response.raise_for_status()
        payload = response.json()
        job_id = payload.get("job_id")
        if not job_id:
            raise RuntimeError("Coordinator submission returned no job_id")
        job_id = str(job_id)
        if scenario.endpoint == "/api/partitions":
            execution = preflight.get("execution_plan") or {}
            preparation = preflight.get("preparation") or {}
            candidates = preflight.get("candidates") or []
            selected_region = str(execution.get("region") or "") or None
            submission_receipt = {
                "schema": "cloud-offload.benchmark-submission-receipt.v1",
                "preflight_status": str(preflight.get("status") or ""),
                "profile": str(execution.get("profile") or ""),
                "image_digest": str(execution.get("image_digest") or ""),
                "provider": str(execution.get("provider") or ""),
                "region": selected_region,
                "allowed_regions": list(scenario.allowed_regions),
                "prepared_volume_bound": bool(execution.get("prepared_volume_id")),
                "preparation_complete": bool(preparation.get("complete")),
                "preparation_coverage_percent": float(
                    preparation.get("coverage_percent") or 0
                ),
                "cold_fallback_available": any(
                    str(item.get("region") or "") == selected_region
                    and not item.get("prepared_volume_id")
                    for item in candidates
                ),
            }
            profile_fingerprint = str(execution.get("profile_fingerprint") or "")
            if profile_fingerprint:
                submission_receipt["profile_fingerprint"] = profile_fingerprint
            expected_model = str(request_payload.get("model") or "")
            if expected_model:
                submission_receipt["expected_model"] = expected_model
            expected_artifacts = sorted(
                str(item).lower()
                for item in (request_payload.get("input_artifacts") or {}).values()
                if item
            )
            if expected_artifacts:
                submission_receipt["expected_artifact_digests"] = expected_artifacts
            expected_model = str(
                request_payload.get("model")
                or ("comfyui-partition-v1" if scenario.endpoint == "/api/partitions" else "")
            )
            if expected_model:
                submission_receipt["expected_model"] = expected_model
            self._submission_receipts[job_id] = submission_receipt
        return job_id

    def resolve_resource_identity(
        self, job_id: str, provider: str, instance_id: str
    ) -> dict[str, str] | None:
        """Prove the current job, lease, provider, and Pod as one identity."""

        response = self._request(
            "GET", f"/api/jobs/{job_id}", retry_safe=True, timeout=30
        )
        response.raise_for_status()
        job = response.json()
        params = job.get("params") or {}
        lease_id = str(params.get("lease_id") or "")
        expected_instance = str(
            params.get("worker_instance_id")
            or params.get("pod_id")
            or job.get("worker_id")
            or ""
        )
        expected_provider = str(params.get("provider") or provider)
        if (
            str(job.get("id") or job_id) != job_id
            or not lease_id
            or expected_provider != provider
            or expected_instance != instance_id
        ):
            return None
        return {
            "job_id": job_id,
            "lease_id": lease_id,
            "provider": provider,
            "instance_id": instance_id,
        }

    def submission_receipt(self, job_id: str) -> dict[str, Any] | None:
        receipt = self._submission_receipts.get(job_id)
        return json.loads(json.dumps(receipt)) if receipt is not None else None

    def cache_status(self) -> dict[str, Any]:
        response = self._request(
            "GET", "/api/cache/status", retry_safe=True, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def base_manifest(self, cold_result: dict[str, Any]) -> dict[str, Any] | None:
        """Verify a registry manifest published or successfully restored by this run."""
        receipt = cold_result.get("submission_receipt") or {}
        job_id = str(cold_result.get("job_id") or "")
        if not job_id:
            return None
        receipt_region = str(receipt.get("region") or "")
        requested_regions = tuple(
            str(item).strip() for item in (receipt.get("allowed_regions") or []) if str(item).strip()
        )
        started_at = _parse_timestamp(cold_result.get("started_at"))
        job_response = self._request(
            "GET", f"/api/jobs/{job_id}", retry_safe=True, timeout=30
        )
        job_response.raise_for_status()
        job = job_response.json()
        if str(job.get("id") or job_id) != job_id:
            return None
        params = job.get("params") or {}
        lease_id = str(params.get("lease_id") or "")
        volume_id = str(params.get("cache_volume_id") or "")
        job_region = str(params.get("cache_datacenter_id") or "")
        expected_model = str(receipt.get("expected_model") or "")
        job_model = str(job.get("model") or "")
        if (
            not expected_model
            or not job_model
            or job_model != expected_model
            or not receipt_region
            or len(requested_regions) != 1
            or requested_regions[0] != receipt_region
            or not job_region
            or job_region != receipt_region
        ):
            return None
        region = receipt_region
        profile_fingerprint = str(receipt.get("profile_fingerprint") or "")
        image_digest = str(receipt.get("image_digest") or "")
        if (
            not lease_id
            or not volume_id
            or not region
            or not profile_fingerprint
            or not image_digest
        ):
            return None
        response = self._request(
            "GET",
            "/api/cache/manifests",
            retry_safe=True,
            timeout=30,
            params={
                "datacenter_id": region,
                "profile_fingerprint": profile_fingerprint,
            },
        )
        response.raise_for_status()
        manifests = response.json().get("manifests") or []
        restored: dict[str, set[str]] = {}
        if cold_result.get("cache_state") == "hot" and job.get("status") == "completed":
            for envelope in self.events(job_id, after=0):
                event = envelope.get("event") or {}
                receipt = event.get("receipt") or {}
                restored_id = str(receipt.get("manifest_id") or "")
                if (
                    event.get("type") == "cache_restore_completed"
                    and restored_id == str(params.get("cache_manifest_id") or "")
                    and receipt.get("volume_id") == volume_id
                    and receipt.get("datacenter_id") == region
                ):
                    hits = {
                        str(item.get("digest"))
                        for item in receipt.get("artifacts") or []
                        if item.get("result") == "hit" and int(item.get("bytes") or 0) > 0
                    }
                    if restored_id and hits:
                        restored.setdefault(restored_id, set()).update(hits)
        candidates = []
        for manifest in manifests:
            created_at = _parse_timestamp(manifest.get("created_at"))
            manifest_id = str(manifest.get("manifest_id") or "")
            producer = manifest.get("producer") or {}
            published_here = (
                str(producer.get("job_id") or "") == job_id
                and str(producer.get("lease_id") or "") == lease_id
                and (started_at is None or (created_at is not None and created_at > started_at))
            )
            restored_here = bool(restored.get(manifest_id)) and restored[manifest_id].issubset(
                {str(item.get("digest")) for item in manifest.get("artifacts") or []}
            )
            if (
                not manifest_id
                or str(manifest.get("volume_id") or "") != volume_id
                or str(manifest.get("datacenter_id") or "") != region
                or str(manifest.get("profile_fingerprint") or "") != profile_fingerprint
                or not (published_here or restored_here)
                or str(producer.get("image_digest") or "") != image_digest
                or not str(producer.get("cloud_offload_version") or "")
                or not manifest.get("artifacts")
            ):
                continue
            candidates.append(manifest)
        if len(candidates) != 1:
            return None
        selected = candidates[0]
        return {
            "manifest_id": str(selected["manifest_id"]),
            "volume_id": volume_id,
            "datacenter_id": region,
            "profile_fingerprint": profile_fingerprint,
            "job_id": job_id,
            "lease_id": lease_id,
            "created_at": selected.get("created_at"),
        }

    def snapshot(self, job_id: str) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/snapshot",
            retry_safe=True,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def snapshot_until(
        self, job_id: str, *, absolute_deadline: float
    ) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/snapshot",
            retry_safe=True,
            timeout=30,
            absolute_deadline=absolute_deadline,
        )
        response.raise_for_status()
        return response.json()

    def events(self, job_id: str, after: int) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/events",
            retry_safe=True,
            params={"after": max(0, int(after)), "limit": 1000},
            timeout=30,
        )
        response.raise_for_status()
        return list(response.json().get("events") or [])

    def events_until(
        self, job_id: str, after: int, *, absolute_deadline: float
    ) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/events",
            retry_safe=True,
            params={"after": max(0, int(after)), "limit": 1000},
            timeout=30,
            absolute_deadline=absolute_deadline,
        )
        response.raise_for_status()
        return list(response.json().get("events") or [])

    def cancel(self, job_id: str) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/jobs/{job_id}/cancel",
            retry_safe=True,
            timeout=30,
        )
        if response.status_code not in {200, 409}:
            response.raise_for_status()
        return {
            "status_code": response.status_code,
            "accepted": response.status_code == 200,
        }

    def terminate(self, provider: str, instance_id: str) -> bool:
        return bool(self.connectors[provider].terminate(instance_id))

    def support_bundle(self, job_id: str) -> dict[str, Any] | None:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/support-bundle",
            retry_safe=True,
            timeout=60,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def run_hook(
        self, injection: FailureInjection, context: dict[str, str]
    ) -> dict[str, Any]:
        if not self.allow_hooks:
            raise RuntimeError(
                f"{injection.kind} failure injection requires --allow-hooks"
            )
        started = time.monotonic()
        environment = os.environ.copy()
        environment.update(context)
        timeout_seconds = (
            CORRUPTION_OBSERVE_HOOK_TIMEOUT_SECONDS
            if injection.kind == "corruption"
            and context.get("CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE") == "observe"
            else DEFAULT_HOOK_TIMEOUT_SECONDS
        )
        completed = subprocess.run(
            list(injection.hook_argv),
            env=environment,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        return {
            "exit_code": completed.returncode,
            "duration_seconds": round(time.monotonic() - started, 6),
            # Hook output is deliberately excluded: failure tools often print
            # provider URLs, object keys, or process environments.
            "output_omitted": True,
        }


_PUBLIC_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_PUBLIC_DIGEST = re.compile(r"(?:sha256:)?[0-9a-f]{64}\Z")
_PUBLIC_CODE = re.compile(
    r"[A-Za-z][A-Za-z0-9_.-]{0,63}(?::[A-Za-z][A-Za-z0-9_.-]{0,63})?\Z"
)
_PROVIDER_VALUES = {"runpod", "vast"}
_SUPPORT_EVENT_TYPES = {
    "allocation_started", "provider_request_completed", "runner_starting",
    "runner_ready", "job_status_changed", "executed", "cancellation_requested",
    "lease_revoked", "cache_artifact_quarantined", "cache_mount_ready",
}
_SEMANTIC_VALUES = {
    "status": {
        "queued", "running", "completed", "failed", "dead_letter", "starting",
        "provisioning", "exited", "error", "terminated", "unknown", "active",
    },
    "state": {"unknown", "confirmed", "failed", "not_started"},
    "source": {
        "journal", "provider_inventory", "provider_inventory_conservative",
        "release_fallback",
    },
    "type": _SUPPORT_EVENT_TYPES | {
        "provisioning_started", "executed", "lease_acquired", "preparation_started",
        "preparation_completed",
    },
    "phase": {
        "provisioning", "worker_boot", "execution", "result_transfer", "closure",
        "preparation", "cancellation", "storage", "unknown",
    },
    "kind": FAILURE_KINDS,
    "cache_state": CACHE_STATES,
    "provider_state": {
        "PROVISIONING", "STARTING", "RUNNING", "EXITED", "ERROR", "TERMINATED",
        "UNKNOWN",
    },
}
_DIGEST_KEYS = {
    "image_digest", "profile_fingerprint", "test_set_digest",
    "benchmark_plan_digest", "benchmark_scorecard_digest",
}
_TIME_KEYS = {"started_at", "completed_at", "observed_at", "created_at"}
_CODE_KEYS = {
    "abort_reason", "campaign_abort", "limit_triggered", "harness_error", "error",
    "error_type", "inventory_error", "cleanup_interrupt", "matrix_stop_reason",
    "failure_codes",
}
_IDENTIFIER_KEYS = {
    "name", "instance_id", "lease_id", "job_id", "requested_policy",
    "previous_policy", "restored_policy", "profile", "region", "manifest_id", "volume_id",
    "datacenter_id", "duration_basis", "ownership_state", "failure_kind",
    "preflight_status", "expected_model", "expected_statuses", "allowed_regions",
    "missing_canaries", "canaries",
}
_DROP = object()


def _project_support_bundle(value: Any) -> dict[str, Any] | None:
    """Project support data to a small, explicit evidence schema."""

    if not isinstance(value, dict) or value.get("schema") != "cloud-offload.support-bundle.v1":
        return None
    projected: dict[str, Any] = {"schema": "cloud-offload.support-bundle.v1"}
    job = value.get("job") or {}
    job_id = str(job.get("id") or "") if isinstance(job, dict) else ""
    if _PUBLIC_IDENTIFIER.fullmatch(job_id):
        projected["job"] = {"id": job_id}
    events = []
    for item in value.get("events") or []:
        if not isinstance(item, dict):
            continue
        event_type = str(item.get("type") or "")
        if event_type not in _SUPPORT_EVENT_TYPES:
            continue
        event = {"type": event_type}
        if isinstance(item.get("sequence"), int):
            event["sequence"] = item["sequence"]
        phase = str(item.get("phase") or "")
        if phase and _PUBLIC_IDENTIFIER.fullmatch(phase):
            event["phase"] = phase
        events.append(event)
    projected["events"] = events
    return projected


def _sanitize_public_evidence(value: Any, key: str | None = None) -> Any:
    """Apply an explicit scalar allowlist to the published scorecard schema."""
    if isinstance(value, dict):
        projected = {}
        for child_key, item in value.items():
            child_key = str(child_key)
            child = _sanitize_public_evidence(item, child_key)
            if child is not _DROP:
                projected[child_key] = child
        return projected
    if isinstance(value, list):
        return [
            item
            for child in value
            if (item := _sanitize_public_evidence(child, key)) is not _DROP
        ]
    if isinstance(value, tuple):
        return _sanitize_public_evidence(list(value), key)
    if isinstance(value, str):
        if re.fullmatch(r"(?:AKIA|ASIA)[0-9A-Z]{16}", value):
            return _DROP
        if key == "provider" or key == "providers":
            return value if value in _PROVIDER_VALUES else _DROP
        if key in _SEMANTIC_VALUES:
            return value if value in _SEMANTIC_VALUES[key] else _DROP
        if key == "schema":
            return value if re.fullmatch(r"cloud-offload\.[a-z0-9.-]+\.v[0-9]+", value) else _DROP
        if key in _DIGEST_KEYS:
            return (
                value
                if value.startswith("sha256:") and _PUBLIC_DIGEST.fullmatch(value)
                else _DROP
            )
        if key == "request_digest":
            return value if _PUBLIC_DIGEST.fullmatch(value) else _DROP
        if key in _TIME_KEYS:
            return value if _parse_timestamp(value) is not None else _DROP
        if key in _CODE_KEYS:
            return value if _PUBLIC_CODE.fullmatch(value) else _DROP
        if key in _IDENTIFIER_KEYS and _PUBLIC_IDENTIFIER.fullmatch(value):
            return value
        return _DROP
    return value


def write_scorecard(path: str | Path, scorecard: dict[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent, prefix=destination.name + ".", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                _sanitize_public_evidence(scorecard),
                handle,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination
