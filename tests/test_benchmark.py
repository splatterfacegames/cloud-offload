import json
import sys
import threading
from dataclasses import dataclass

import pytest

from cloud_offload.benchmark import (
    CORRUPTION_OBSERVE_HOOK_TIMEOUT_SECONDS,
    DEFAULT_HOOK_TIMEOUT_SECONDS,
    PLAN_SCHEMA,
    BenchmarkPlan,
    BenchmarkRunner,
    CoordinatorBenchmarkDriver,
    InstanceObservation,
    _initial_startup_phases,
    _observe_startup_phases,
    _sanitize_public_evidence,
    _preparation_seconds,
    _ResourceMeter,
    _seconds_from_event_to,
    write_scorecard,
)
from cloud_offload.config import CloudConfig
from cloud_offload.benchmark_faults import CORRUPTION_NONCE_FIELD
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.prepared_state import INDEX_SCHEMA


def event(
    sequence,
    event_type,
    phase,
    seconds,
    *,
    instance_id="pod-1",
    hourly_rate=0.36,
):
    resources = {"provider": "runpod", "hourly_rate": hourly_rate}
    if instance_id:
        resources["worker_instance_id"] = instance_id
        resources["lease_id"] = f"lease-{instance_id}"
    return {
        "schema": "cloud-offload.job-event.v2",
        "sequence": sequence,
        "type": event_type,
        "phase": phase,
        "observed_at": f"2026-07-29T00:00:{seconds:02d}+00:00",
        "resources": resources,
    }


@dataclass
class Script:
    steps: list[dict]
    terminate_succeeds: bool = True


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeDriver:
    def __init__(self, scripts):
        self.clock = 0.0
        self.scripts = scripts
        self.jobs = {}
        self.current_job = None
        self.cancelled = set()
        self.terminated = set()
        self.termination_attempts = []
        self.hooks = []
        self.active_until = 0.0
        self.prepared_storage_policy = "smart"
        self.preparation_calls = []
        self.restoration_calls = []
        self._restore_policies = {}
        self.submit_calls = []

    def monotonic(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += seconds

    def inventory(self, providers):
        result = {provider: {} for provider in providers}
        if self.current_job is None:
            return result
        state = self.jobs[self.current_job]
        step = state["script"].steps[state["index"]]
        for item in step.get("instances", []):
            key = (item.get("provider", "runpod"), item["id"])
            if key in self.terminated:
                continue
            observation = InstanceObservation(
                id=item["id"],
                provider=key[0],
                hourly_rate=float(item.get("hourly_rate", 0.36)),
                status=item.get("status", "running"),
                managed=item.get("managed", True),
                name=item.get("name", "cloud-offload-worker-test"),
            )
            result[key[0]][item["id"]] = observation
        return result

    def inventory_until(self, providers, *, absolute_deadline):
        return self.inventory(providers)

    def active_workers(self, providers):
        return (
            [{"provider": "runpod", "worker_id": "stale-worker"}]
            if self.clock < self.active_until
            else []
        )

    def active_workers_until(self, providers, *, absolute_deadline):
        return self.active_workers(providers)

    def prepare_scenario(self, scenario):
        previous = self.prepared_storage_policy
        self._restore_policies[scenario.name] = previous
        if scenario.prepared_storage_policy is not None:
            self.prepared_storage_policy = scenario.prepared_storage_policy
        self.preparation_calls.append(
            (scenario.name, scenario.prepared_storage_policy, previous)
        )
        return {
            "required": scenario.prepared_storage_policy is not None,
            "prepared": True,
            "requested_policy": scenario.prepared_storage_policy,
            "previous_policy": previous,
            "changed": self.prepared_storage_policy != previous,
        }

    def restore_scenario(self, scenario):
        previous = self._restore_policies.pop(scenario.name, None)
        changed = previous is not None and previous != self.prepared_storage_policy
        if previous is not None:
            self.prepared_storage_policy = previous
        self.restoration_calls.append((scenario.name, previous))
        return {
            "required": scenario.prepared_storage_policy is not None,
            "restored": True,
            "restored_policy": previous,
            "changed": changed,
        }

    def submit(self, scenario):
        self.submit_calls.append(scenario.name)
        job_id = f"job-{len(self.jobs) + 1}"
        self.jobs[job_id] = {
            "script": self.scripts[scenario.name],
            "index": 0,
            "scenario": scenario,
        }
        self.current_job = job_id
        return job_id

    def snapshot(self, job_id):
        if job_id in self.cancelled:
            return {"status": "failed", "lifecycle_phase": "execution"}
        state = self.jobs[job_id]
        step = state["script"].steps[state["index"]]
        snapshot = dict(step["snapshot"])
        if state["index"] + 1 < len(state["script"].steps):
            state["index"] += 1
        return snapshot

    def events(self, job_id, after):
        state = self.jobs[job_id]
        available = [
            item
            for step in state["script"].steps[: state["index"] + 1]
            for item in step.get("events", [])
        ]
        return [item for item in available if item["sequence"] > after]

    def cancel(self, job_id):
        self.cancelled.add(job_id)
        return {"accepted": True, "status_code": 200}

    def terminate(self, provider, instance_id):
        self.termination_attempts.append((provider, instance_id))
        state = self.jobs[self.current_job]
        if state["script"].terminate_succeeds:
            self.terminated.add((provider, instance_id))
            return True
        return False

    def support_bundle(self, job_id):
        return {
            "schema": "cloud-offload.support-bundle.v1",
            "job": {"id": job_id},
        }

    def run_hook(self, injection, context):
        self.hooks.append((injection.kind, context))
        return {"exit_code": 0, "duration_seconds": 0.01, "output_omitted": True}

    def base_manifest(self, cold_result):
        return {"manifest_id": f"base-{cold_result['job_id']}"}


def plan_dict(scenarios, **limit_overrides):
    limits = {
        "max_total_cost_usd": 1.0,
        "max_scenario_cost_usd": 0.5,
        "max_campaign_seconds": 120,
        "poll_seconds": 1,
        "cleanup_timeout_seconds": 3,
    }
    limits.update(limit_overrides)
    return {
        "schema": PLAN_SCHEMA,
        "providers": ["runpod"],
        "exclusive": True,
        "limits": limits,
        "scenarios": scenarios,
    }


def scenario(name, cache_state, **overrides):
    value = {
        "name": name,
        "cache_state": cache_state,
        "endpoint": "/api/partitions",
        "request": {
            "partition": {"partition_id": name},
            "private_prompt": f"private-{name}",
            "force_execution": True,
        },
        "timeout_seconds": 20,
        "expected_statuses": ["completed"],
        "fresh_instance": True,
    }
    if cache_state == "cold":
        value["prepared_storage_policy"] = "off"
    elif cache_state == "hot":
        value["prepared_storage_policy"] = "smart"
    value.update(overrides)
    return value


def successful_script(pod_id, *, rate=0.36):
    return Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [
                    event(
                        1,
                        "provisioning_started",
                        "provisioning",
                        0,
                        instance_id=None,
                        hourly_rate=rate,
                    ),
                    event(
                        2,
                        "runner_starting",
                        "worker_boot",
                        1,
                        instance_id=pod_id,
                        hourly_rate=rate,
                    ),
                ],
                "instances": [{"id": pod_id, "hourly_rate": rate}],
            },
            {
                "snapshot": {
                    "status": "completed",
                    "lifecycle_phase": "result_transfer",
                },
                "events": [
                    event(
                        3,
                        "executed",
                        "execution",
                        2,
                        instance_id=pod_id,
                        hourly_rate=rate,
                    )
                ],
                "instances": [{"id": pod_id, "hourly_rate": rate}],
            },
        ]
    )


def test_plan_requires_alternating_cold_hot_and_explicit_failure_hooks():
    with pytest.raises(ValueError, match="must alternate"):
        BenchmarkPlan.from_dict(
            plan_dict([scenario("cold-1", "cold"), scenario("cold-2", "cold")])
        )

    restart = scenario(
        "restart",
        "failure",
        failure={"kind": "restart", "trigger_phase": "execution"},
    )
    with pytest.raises(ValueError, match="requires hook_argv"):
        BenchmarkPlan.from_dict(plan_dict([restart]))

    unsafe_fresh = scenario("fresh", "cold")
    unsafe_fresh["request"].pop("force_execution")
    with pytest.raises(ValueError, match="force_execution=true"):
        BenchmarkPlan.from_dict(plan_dict([unsafe_fresh]))

    unproven_cold = scenario("cold", "cold")
    unproven_cold.pop("prepared_storage_policy")
    with pytest.raises(ValueError, match="cold runs require"):
        BenchmarkPlan.from_dict(plan_dict([unproven_cold]))

    unproven_hot = scenario("hot", "hot", prepared_storage_policy="off")
    with pytest.raises(ValueError, match="hot runs require"):
        BenchmarkPlan.from_dict(plan_dict([unproven_hot]))

    invalid_pre_submit = scenario(
        "cancel-before-submit",
        "failure",
        failure={"kind": "cancellation", "before_submit": True},
    )
    with pytest.raises(ValueError, match="before_submit"):
        BenchmarkPlan.from_dict(plan_dict([invalid_pre_submit]))


def test_cold_hot_campaign_records_comparable_json_and_removes_exact_pods(tmp_path):
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold"), scenario("hot", "hot")])
    )
    driver = FakeDriver(
        {
            "cold": successful_script("pod-cold"),
            "hot": successful_script("pod-hot"),
        }
    )

    scorecard = BenchmarkRunner(driver).run(plan)
    destination = write_scorecard(tmp_path / "scorecard.json", scorecard)
    encoded = destination.read_text(encoding="utf-8")

    assert scorecard["passed"] is True
    assert [item["cache_state"] for item in scorecard["results"]] == ["cold", "hot"]
    assert all(item["fresh_instance_observed"] for item in scorecard["results"])
    assert scorecard["distributions"]["cold"]["duration_seconds"]["count"] == 1
    assert scorecard["distributions"]["hot"]["duration_seconds"]["count"] == 1
    assert scorecard["distributions"]["resource_closure_seconds"]["count"] == 2
    assert driver.termination_attempts == [
        ("runpod", "pod-cold"),
        ("runpod", "pod-hot"),
    ]
    assert scorecard["orphaned_resources"] == []
    assert driver.preparation_calls == [
        ("cold", "off", "smart"),
        ("hot", "smart", "smart"),
    ]
    assert driver.restoration_calls == [("cold", "smart"), ("hot", "smart")]
    assert driver.prepared_storage_policy == "smart"
    assert scorecard["results"][0]["scenario_preparation"] == {
        "required": True,
        "prepared": True,
        "requested_policy": "off",
        "previous_policy": "smart",
        "changed": True,
    }
    assert scorecard["results"][0]["scenario_restoration"]["restored"] is True
    assert "private-cold" not in encoded
    assert "private-hot" not in encoded
    assert json.loads(encoded)["schema"] == "cloud-offload.benchmark-scorecard.v1"


def test_first_unexpected_scenario_failure_stops_before_second_submission():
    failed = Script(
        [
            {
                "snapshot": {"status": "failed", "lifecycle_phase": "worker_boot"},
                "events": [event(1, "runner_starting", "worker_boot", 1,
                                 instance_id="pod-failed")],
                "instances": [{"id": "pod-failed"}],
            }
        ]
    )
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold"), scenario("hot", "hot")])
    )
    driver = FakeDriver(
        {"cold": failed, "hot": successful_script("pod-must-not-start")}
    )

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is False
    assert scorecard["campaign_abort"] == "unexpected_scenario_failure"
    assert driver.submit_calls == ["cold"]
    assert len(scorecard["results"]) == 1


def test_runner_readiness_timeout_uses_total_active_scenario_time_and_cleans_up():
    waiting = Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [event(1, "runner_starting", "worker_boot", 1,
                                 instance_id="pod-waiting")],
                "instances": [{"id": "pod-waiting", "hourly_rate": 0.36}],
            }
        ]
    )
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold", timeout_seconds=20)],
            runner_readiness_timeout_seconds=2,
            max_runner_readiness_cost_usd=0.25,
        )
    )
    driver = FakeDriver({"cold": waiting})

    scorecard = BenchmarkRunner(driver).run(plan)
    result = scorecard["results"][0]

    assert result["limit_triggered"] == "runner_readiness_timeout"
    assert result["scenario_active_seconds"] == 2
    assert result["duration_seconds"] == 2
    assert driver.cancelled == {"job-1"}
    assert driver.termination_attempts == [("runpod", "pod-waiting")]


def test_readiness_poll_sleep_is_clamped_to_absolute_deadline():
    class RecordingDriver(FakeDriver):
        def __init__(self, scripts):
            super().__init__(scripts)
            self.sleeps = []

        def sleep(self, seconds):
            self.sleeps.append(seconds)
            super().sleep(seconds)

    waiting = Script([{
        "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
        "events": [event(1, "provisioning_started", "provisioning", 0,
                         instance_id=None, hourly_rate=0.1)],
        "instances": [],
    }])
    plan = BenchmarkPlan.from_dict(plan_dict(
        [scenario("cold", "cold")], poll_seconds=15,
        runner_readiness_timeout_seconds=2,
    ))
    driver = RecordingDriver({"cold": waiting})

    result = BenchmarkRunner(driver).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_timeout"
    assert result["scenario_active_seconds"] == pytest.approx(2, abs=0.05)
    assert driver.sleeps[0] == pytest.approx(2, abs=0.001)
    assert all(item > 0 for item in driver.sleeps)
    assert driver.cancelled == {"job-1"}


def test_slow_events_call_crossing_deadline_cancels_without_next_poll():
    class SlowEventsDriver(FakeDriver):
        event_calls = 0

        def events(self, job_id, after):
            self.event_calls += 1
            self.clock += 10
            return super().events(job_id, after)

    driver = SlowEventsDriver({"cold": successful_script("pod-slow-events")})
    plan = BenchmarkPlan.from_dict(plan_dict(
        [scenario("cold", "cold")], poll_seconds=10,
        runner_readiness_timeout_seconds=2,
    ))

    result = BenchmarkRunner(driver).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_timeout"
    assert result["scenario_active_seconds"] == pytest.approx(10, abs=0.05)
    assert result["duration_seconds"] >= 10
    assert driver.event_calls == 1
    assert driver.cancelled == {"job-1"}


def test_runner_readiness_cost_guard_stops_before_general_scenario_budget():
    waiting = Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [event(1, "runner_starting", "worker_boot", 1,
                                 instance_id="pod-cost", hourly_rate=1.0)],
                "instances": [{"id": "pod-cost", "hourly_rate": 1.0}],
            }
        ]
    )
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold", timeout_seconds=20)],
            runner_readiness_timeout_seconds=15,
            max_runner_readiness_cost_usd=0.0001,
        )
    )
    driver = FakeDriver({"cold": waiting})

    result = BenchmarkRunner(driver).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_cost_limit"
    assert driver.cancelled == {"job-1"}
    assert driver.termination_attempts == [("runpod", "pod-cost")]


def test_provider_rate_overrides_zero_journal_rate_before_readiness_cost_guard():
    waiting = Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [
                    event(
                        1,
                        "runner_starting",
                        "worker_boot",
                        1,
                        instance_id="pod-rate",
                        hourly_rate=0,
                    )
                ],
                "instances": [
                    {"id": "pod-rate", "hourly_rate": 3600, "status": "running"}
                ],
            }
        ]
    )
    waiting.steps[0]["events"][0]["resources"]["lease_id"] = "lease-rate"
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold", timeout_seconds=20)],
            max_total_cost_usd=3,
            max_scenario_cost_usd=2,
            max_runner_readiness_cost_usd=0.5,
            runner_readiness_timeout_seconds=15,
        )
    )
    driver = FakeDriver({"cold": waiting})

    result = BenchmarkRunner(driver).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_cost_limit"
    assert result["estimated_compute_cost_upper_usd"] >= 1
    assert result["resources"] == [
        {
            "provider": "runpod",
            "instance_id": "pod-rate",
                "hourly_rate": 3600,
                "source": "journal",
                "lease_id": "lease-rate",
        }
    ]
    assert driver.cancelled == {"job-1"}


def test_known_running_pod_never_keeps_a_zero_cost_rate():
    waiting = Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [event(1, "runner_starting", "worker_boot", 1,
                                 instance_id="pod-unknown-rate", hourly_rate=0)],
                "instances": [
                    {"id": "pod-unknown-rate", "hourly_rate": 0, "status": "running"}
                ],
            }
        ]
    )
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold")],
            max_runner_readiness_cost_usd=0.1,
            runner_readiness_timeout_seconds=15,
        )
    )

    result = BenchmarkRunner(FakeDriver({"cold": waiting})).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_cost_limit"
    assert result["resources"][0]["hourly_rate"] > 0
    assert result["resources"][0]["source"] == "provider_inventory_conservative"


@pytest.mark.parametrize("ownership_proved", [True, False, "lookup_error"])
def test_missing_event_lease_is_resolved_authoritatively_or_charged_as_unknown(ownership_proved):
    class LeaseResolverDriver(FakeDriver):
        def resolve_resource_identity(self, job_id, provider, instance_id):
            if ownership_proved == "lookup_error":
                raise ConnectionError("unsafe provider details")
            if ownership_proved is True:
                return {"job_id": job_id, "provider": provider,
                        "instance_id": instance_id, "lease_id": "lease-authoritative"}
            return None

    waiting = successful_script("pod-missing-lease")
    for step in waiting.steps:
        for item in step.get("events", []):
            item["resources"].pop("lease_id", None)
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))
    driver = LeaseResolverDriver({"cold": waiting})

    scorecard = BenchmarkRunner(driver).run(plan)
    result = scorecard["results"][0]

    if ownership_proved is True:
        assert driver.termination_attempts == [("runpod", "pod-missing-lease")]
        assert result["resources"][0]["lease_id"] == "lease-authoritative"
    else:
        assert driver.termination_attempts == []
        assert result["orphaned_resources"][0]["ownership_state"] == "unknown"
        assert scorecard["estimated_compute_cost_upper_usd"] == plan.limits.max_total_cost_usd
        assert scorecard["passed"] is False


def test_unknown_paid_pod_uses_provider_rate_in_live_readiness_cost_guard():
    waiting = successful_script("pod-unknown-expensive", rate=0)
    for step in waiting.steps:
        for item in step.get("events", []):
            item["resources"].pop("lease_id", None)
        step["instances"] = [
            {"id": "pod-unknown-expensive", "hourly_rate": 3600, "status": "running"}
        ]
        step["snapshot"] = {"status": "running", "lifecycle_phase": "worker_boot"}
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")],
                  max_runner_readiness_cost_usd=0.10,
                  runner_readiness_timeout_seconds=10,
                  max_scenario_cost_usd=2, max_total_cost_usd=3)
    )

    scorecard = BenchmarkRunner(FakeDriver({"cold": waiting})).run(plan)
    result = scorecard["results"][0]

    assert result["limit_triggered"] == "runner_readiness_cost_limit"
    assert result["scenario_active_seconds"] < 10
    assert result["unknown_paid_resources"][0]["hourly_rate"] == 3600
    assert result["estimated_compute_cost_upper_usd"] >= 1
    assert scorecard["cleanup_proof"]["state"] == "failed"
    assert scorecard["cleanup_proof"]["unknown_paid_resource_count"] == 1


@pytest.mark.parametrize("pod_count", [2, 3, 5])
def test_live_cost_sums_each_unknown_pod_exactly_once(pod_count):
    events = []
    instances = []
    for index in range(pod_count):
        item = event(index + 1, "runner_starting", "worker_boot", 1,
                     instance_id=f"pod-{index}", hourly_rate=0)
        item["resources"].pop("lease_id", None)
        events.append(item)
        instances.append({"id": f"pod-{index}", "hourly_rate": 0.6,
                          "status": "running"})
    waiting = Script([{
        "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
        "events": events, "instances": instances,
    }])
    one_second_cost = pod_count * 0.6 / 3600
    plan = BenchmarkPlan.from_dict(plan_dict(
        [scenario("cold", "cold")], max_runner_readiness_cost_usd=one_second_cost * 0.9,
        runner_readiness_timeout_seconds=10, max_scenario_cost_usd=2,
        max_total_cost_usd=3,
    ))

    result = BenchmarkRunner(FakeDriver({"cold": waiting})).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_cost_limit"
    assert result["scenario_active_seconds"] == 1
    assert len(result["unknown_paid_resources"]) == pod_count
    assert sum(item["hourly_rate"] for item in result["unknown_paid_resources"]) == pytest.approx(pod_count * 0.6)


@pytest.mark.parametrize("order", ["verified_then_incomplete", "incomplete_then_verified"])
def test_resource_identity_promotion_and_incomplete_events_are_exactly_once(order):
    script = successful_script("pod-promoted")
    if order == "verified_then_incomplete":
        extra = event(4, "executed", "execution", 3, instance_id="pod-promoted")
        extra["resources"].pop("lease_id", None)
        script.steps[1]["events"].append(extra)
    else:
        script.steps[0]["events"][1]["resources"].pop("lease_id", None)
    driver = FakeDriver({"cold": script})

    scorecard = BenchmarkRunner(driver).run(
        BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))
    )
    result = scorecard["results"][0]

    assert len(result["resources"]) == 1
    assert result["unknown_paid_resources"] == []
    assert result["orphaned_resources"] == []
    assert driver.termination_attempts == [("runpod", "pod-promoted")]
    assert scorecard["estimated_compute_cost_upper_usd"] < 1


def test_worker_with_current_lease_but_wrong_pod_never_proves_readiness():
    phases = _initial_startup_phases()
    observation = InstanceObservation(
        id="pod-current", provider="runpod", hourly_rate=0.5,
        status="running", managed=True,
    )

    _observe_startup_phases(
        phases,
        {"runpod": {"pod-current": observation}},
        [{"worker_id": "pod-wrong", "provider": "runpod", "status": "active",
          "lease_id": "lease-current"}],
        [],
        {("runpod", "pod-current"): "lease-current"},
    )

    assert phases["runner_callback"] == {"state": "unknown"}
    assert phases["comfyui_readiness"] == {"state": "unknown"}


def test_exact_worker_pod_without_lease_never_proves_readiness():
    phases = _initial_startup_phases()
    observation = InstanceObservation(
        id="pod-current", provider="runpod", hourly_rate=0.5,
        status="running", managed=True,
    )
    _observe_startup_phases(
        phases, {"runpod": {"pod-current": observation}},
        [{"worker_id": "pod-current", "provider": "runpod", "status": "active"}],
        [], {("runpod", "pod-current"): "lease-current"},
    )
    assert phases["comfyui_readiness"] == {"state": "unknown"}


def test_startup_diagnostics_keep_authoritative_provider_facts_and_mark_gaps_unknown():
    class StartupDriver(FakeDriver):
        def inventory(self, providers):
            inventory = super().inventory(providers)
            current = inventory["runpod"].get("pod-phase")
            if current is not None:
                inventory["runpod"]["pod-phase"] = InstanceObservation(
                    id=current.id,
                    provider=current.provider,
                    hourly_rate=current.hourly_rate,
                    status=current.status,
                    managed=current.managed,
                    name=current.name,
                    provider_state="RUNNING",
                    container_started=True,
                )
            return inventory

        def active_workers(self, providers):
            if self.current_job is None:
                return []
            return [
                {
                    "worker_id": "pod-phase",
                    "provider": "runpod",
                    "status": "starting",
                    "lease_id": "lease-phase",
                }
            ]

    waiting = Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [event(1, "runner_starting", "worker_boot", 1,
                                 instance_id="pod-phase")],
                "instances": [{"id": "pod-phase"}],
            }
        ]
    )
    waiting.steps[0]["events"][0]["resources"]["lease_id"] = "lease-phase"
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold")],
            runner_readiness_timeout_seconds=1,
        )
    )

    result = BenchmarkRunner(StartupDriver({"cold": waiting})).run(plan)["results"][0]

    assert result["startup_phases"] == {
        "allocation": {"state": "confirmed", "provider_state": "RUNNING"},
        "image_pull": {"state": "unknown"},
        "container_start": {"state": "confirmed"},
        "runner_callback": {"state": "confirmed"},
        "comfyui_readiness": {"state": "unknown"},
    }


def test_startup_diagnostics_reject_provider_state_text_outside_finite_enum():
    phases = _initial_startup_phases()
    observation = InstanceObservation(
        id="pod-safe",
        provider="runpod",
        hourly_rate=0.5,
        status="unknown",
        managed=True,
        provider_state="RUNNING https://private.invalid/token",
    )

    _observe_startup_phases(
        phases,
        {"runpod": {observation.id: observation}},
        [],
        [],
    )

    assert phases["allocation"] == {
        "state": "confirmed",
        "provider_state": "UNKNOWN",
    }
    assert "private" not in json.dumps(phases).lower()


def test_startup_diagnostics_do_not_claim_unmatched_provider_worker_callback():
    phases = _initial_startup_phases()
    observation = InstanceObservation(
        id="pod-campaign",
        provider="runpod",
        hourly_rate=0.5,
        status="running",
        managed=True,
    )

    _observe_startup_phases(
        phases,
        {"runpod": {observation.id: observation}},
        [
            {
                "worker_id": "worker-unrelated",
                "provider": "runpod",
                "status": "active",
                "lease_id": "lease-unrelated",
            }
        ],
        [{"resources": {"lease_id": "lease-campaign"}}],
    )

    assert phases["runner_callback"] == {"state": "unknown"}
    assert phases["comfyui_readiness"] == {"state": "unknown"}


def test_current_job_identity_ignores_and_never_cleans_an_unrelated_concurrent_pod():
    class ConcurrentDriver(FakeDriver):
        def active_workers(self, providers):
            if self.current_job is None:
                return []
            return [
                {
                    "worker_id": "worker-campaign",
                    "provider": "runpod",
                    "status": "starting",
                    "lease_id": "lease-campaign",
                },
                {
                    "worker_id": "pod-unrelated",
                    "provider": "runpod",
                    "status": "active",
                    "lease_id": "lease-unrelated",
                },
            ]

    waiting = Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [event(1, "runner_starting", "worker_boot", 1,
                                 instance_id="pod-campaign")],
                "instances": [
                    {"id": "pod-campaign", "hourly_rate": 0.5},
                    {"id": "pod-unrelated", "hourly_rate": 99.0},
                ],
            }
        ]
    )
    waiting.steps[0]["events"][0]["resources"]["lease_id"] = "lease-campaign"
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")], runner_readiness_timeout_seconds=1)
    )
    driver = ConcurrentDriver({"cold": waiting})

    result = BenchmarkRunner(driver).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_timeout"
    assert [item["instance_id"] for item in result["resources"]] == ["pod-campaign"]
    assert driver.termination_attempts == [("runpod", "pod-campaign")]
    assert ("runpod", "pod-unrelated") not in driver.terminated
    assert result["startup_phases"]["comfyui_readiness"] == {"state": "unknown"}


@pytest.mark.parametrize("delay_stage", ["preparation", "hook"])
def test_runner_readiness_time_guard_is_checked_immediately_before_submit(delay_stage):
    class DelayedDriver(FakeDriver):
        def prepare_scenario(self, scenario):
            result = super().prepare_scenario(scenario)
            if delay_stage == "preparation":
                self.clock += 2
            return result

        def run_hook(self, injection, context):
            result = super().run_hook(injection, context)
            if delay_stage == "hook" and context["CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE"] == "prepare":
                self.clock += 2
            return result

    definition = scenario("cold", "cold")
    if delay_stage == "hook":
        definition = scenario(
            "cold", "failure",
            failure={
                "kind": "restart",
                "before_submit": True,
                "hook_argv": [sys.executable, "safe-hook.py"],
            },
        )
    plan = BenchmarkPlan.from_dict(
        plan_dict([definition], runner_readiness_timeout_seconds=1)
    )
    driver = DelayedDriver({"cold": successful_script("pod-never-submit")})

    result = BenchmarkRunner(driver).run(plan)["results"][0]

    assert result["limit_triggered"] == "runner_readiness_timeout"
    assert result["submission_receipt"] is None
    assert driver.submit_calls == []
    assert driver.termination_attempts == []


def test_exception_and_support_evidence_is_redacted_before_atomic_publication(tmp_path):
    hostile = (
        "https://user:SECRET@private.invalid/path?token=SECRET "
        "HF_TOKEN=SECRET C:\\Users\\jetha\\private.txt"
    )

    class HostileDriver(FakeDriver):
        def prepare_scenario(self, scenario):
            raise RuntimeError(hostile)

    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))
    scorecard = BenchmarkRunner(HostileDriver({})).run(plan)
    path = write_scorecard(tmp_path / "scorecard.json", scorecard)
    encoded = path.read_text(encoding="utf-8")

    for forbidden in ("private.invalid", "SECRET", "HF_TOKEN", "jetha", "C:\\\\Users"):
        assert forbidden not in encoded
    assert json.loads(encoded)["results"][0]["harness_error"] == "RuntimeError"


def test_support_projection_drops_arbitrary_cloud_text_aws_keys_unc_paths_and_urls(tmp_path):
    class HostileBundleDriver(FakeDriver):
        def support_bundle(self, job_id):
            return {
                "schema": "cloud-offload.support-bundle.v1",
                "job": {"id": job_id, "detail": "arbitrary provider payload"},
                "events": [{"sequence": 1, "type": "runner_starting",
                            "detail": "AKIAABCDEFGHIJKLMNOP",
                            "path": r"\\server\private\model",
                            "url": "https://user:token@example.invalid/x"}],
            }

    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))
    scorecard = BenchmarkRunner(
        HostileBundleDriver({"cold": successful_script("pod-safe-bundle")})
    ).run(plan)
    encoded = write_scorecard(tmp_path / "scorecard.json", scorecard).read_text()

    for forbidden in ("AKIA", "server", "example.invalid", "arbitrary provider payload", "token"):
        assert forbidden not in encoded


@pytest.mark.parametrize("hostile", [
    "https://example.invalid/x", "AKIAABCDEFGHIJKLMNOP", "user@example.com",
    "/home/private/model", r"C:\\Users\\private", r"\\server\share", "private text",
])
def test_every_public_string_field_uses_semantic_validation(hostile):
    projected = _sanitize_public_evidence(
        {key: hostile for key in (
            "provider", "instance_id", "job_id", "lease_id", "name", "schema",
            "status", "source", "phase", "failure_codes", "region", "image_digest",
        )}
    )
    assert projected == {}


@pytest.mark.parametrize("key", ["status", "state", "source", "type", "phase"])
def test_semantic_public_fields_reject_arbitrary_identifier_looking_values(key):
    assert _sanitize_public_evidence({key: "privateLookingButSyntactic"}) == {}


def test_request_digest_projection_preserves_canonical_bare_and_prefixed_sha256():
    bare = "a" * 64
    assert _sanitize_public_evidence({"request_digest": bare}) == {
        "request_digest": bare
    }
    assert _sanitize_public_evidence({"request_digest": "sha256:" + bare}) == {
        "request_digest": "sha256:" + bare
    }


@pytest.mark.parametrize("key", [
    "image_digest", "profile_fingerprint", "test_set_digest",
    "benchmark_plan_digest", "benchmark_scorecard_digest",
])
def test_release_digest_fields_require_prefixed_sha256(key):
    bare = "a" * 64
    assert _sanitize_public_evidence({key: bare}) == {}
    assert _sanitize_public_evidence({key: "sha256:" + bare}) == {
        key: "sha256:" + bare
    }


@pytest.mark.parametrize("boundary", ["clock", "inventory", "terminate", "sleep", "verify"])
def test_interrupt_at_each_cleanup_boundary_retries_exact_pod_and_publishes_abort(boundary):
    class CleanupInterruptDriver(FakeDriver):
        cleanup_started = False
        cleanup_inventory_calls = 0
        injected = False

        def support_bundle(self, job_id):
            self.cleanup_started = True
            return super().support_bundle(job_id)

        def monotonic(self):
            if self.cleanup_started and boundary == "clock" and not self.injected:
                self.injected = True
                raise KeyboardInterrupt
            return super().monotonic()

        def inventory(self, providers):
            if self.cleanup_started:
                self.cleanup_inventory_calls += 1
                target = 1 if boundary == "inventory" else 3
                if boundary in {"inventory", "verify"} and self.cleanup_inventory_calls == target and not self.injected:
                    self.injected = True
                    raise KeyboardInterrupt
            return super().inventory(providers)

        def terminate(self, provider, instance_id):
            if self.cleanup_started and boundary == "terminate" and not self.injected:
                self.injected = True
                raise KeyboardInterrupt
            if self.cleanup_started and boundary == "sleep" and not self.injected:
                return False
            return super().terminate(provider, instance_id)

        def sleep(self, seconds):
            if self.cleanup_started and boundary == "sleep" and not self.injected:
                self.injected = True
                raise KeyboardInterrupt
            return super().sleep(seconds)

    driver = CleanupInterruptDriver({"cold": successful_script("pod-cleanup")})
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is False
    assert scorecard["campaign_abort"] == "operator_interrupt:KeyboardInterrupt"
    assert driver.submit_calls == ["cold"]
    assert ("runpod", "pod-cleanup") in driver.terminated
    assert scorecard["orphaned_resources"] == []


def test_keyboard_interrupt_in_failure_cleanup_hook_still_cleans_exact_pod():
    class HookStopDriver(FakeDriver):
        def run_hook(self, injection, context):
            if context["CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE"] == "cleanup":
                raise KeyboardInterrupt
            return super().run_hook(injection, context)

    definition = scenario(
        "failure", "failure",
        failure={"kind": "restart", "before_submit": True,
                 "hook_argv": [sys.executable, "safe-hook.py"]},
    )
    driver = HookStopDriver({"failure": successful_script("pod-hook-stop")})
    scorecard = BenchmarkRunner(driver).run(
        BenchmarkPlan.from_dict(plan_dict([definition]))
    )

    assert scorecard["campaign_abort"] == "operator_interrupt:KeyboardInterrupt"
    assert driver.termination_attempts == [("runpod", "pod-hook-stop")]
    assert scorecard["cleanup_proof"]["state"] == "confirmed"
def test_scenario_cost_limit_cancels_and_still_removes_the_pod():
    expensive = successful_script("pod-expensive", rate=3600)
    expensive.steps[1]["snapshot"] = {
        "status": "running",
        "lifecycle_phase": "execution",
    }
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold")],
            max_scenario_cost_usd=0.5,
            max_total_cost_usd=0.75,
        )
    )
    driver = FakeDriver({"cold": expensive})

    scorecard = BenchmarkRunner(driver).run(plan)
    result = scorecard["results"][0]

    assert result["limit_triggered"] == "scenario_cost_limit"
    assert result["passed"] is False
    assert "job-1" in driver.cancelled
    assert ("runpod", "pod-expensive") in driver.terminated
    assert scorecard["orphaned_resources"] == []


def test_cancellation_and_hook_failures_are_injected_at_the_requested_phase():
    scripts = {
        "cancel": successful_script("pod-cancel"),
        "restart": successful_script("pod-restart"),
    }
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario(
                    "cancel",
                    "failure",
                    expected_statuses=["failed"],
                    failure={"kind": "cancellation", "trigger_phase": "worker_boot"},
                ),
                scenario(
                    "restart",
                    "failure",
                    failure={
                        "kind": "restart",
                        "trigger_phase": "worker_boot",
                        "hook_argv": ["restart-coordinator"],
                    },
                ),
            ]
        )
    )
    driver = FakeDriver(scripts)

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is True
    assert scorecard["results"][0]["failure_injection"]["kind"] == "cancellation"
    assert scorecard["results"][1]["failure_injection"]["hook"]["exit_code"] == 0
    assert driver.hooks[0][0] == "restart"
    assert driver.hooks[0][1]["CLOUD_OFFLOAD_BENCHMARK_JOB_ID"] == "job-2"


def test_two_phase_hook_prepares_before_submission_and_always_cleans_up():
    selected = scenario(
        "corruption",
        "failure",
        failure={
            "kind": "corruption",
            "before_submit": True,
            "hook_argv": ["cloud-offload", "benchmark-hook", "corruption"],
        },
    )
    selected["allowed_regions"] = ["EU-RO-1"]
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold"), selected])
    )
    script = successful_script("pod-corruption")
    script.steps[0]["events"].append(
        event(
            3,
            "cache_mount_ready",
            "model_prepare",
            2,
            instance_id="pod-corruption",
            hourly_rate=0.36,
        )
    )
    script.steps[1]["events"][0]["sequence"] = 4
    driver = FakeDriver({"cold": successful_script("pod-cold"), "corruption": script})

    assets = plan.scenarios[1].request["partition"]["assets"]
    assert len(assets) == 1
    assert assets[0]["filename"].startswith("cloud_offload_benchmark_canary_")
    assert len(assets[0][CORRUPTION_NONCE_FIELD]) == 32
    assert plan.scenarios[1].failure.trigger_event == "cache_mount_ready"

    scorecard = BenchmarkRunner(driver).run(plan)
    result = scorecard["results"][1]

    assert scorecard["passed"] is True
    assert [
        context["CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE"] for _, context in driver.hooks
    ] == [
        "prepare",
        "observe",
        "cleanup",
    ]
    assert driver.hooks[0][1]["CLOUD_OFFLOAD_BENCHMARK_JOB_ID"] == ""
    assert (
        driver.hooks[0][1]["CLOUD_OFFLOAD_BENCHMARK_PROFILE"] == "comfyui-partition-v1"
    )
    assert (
        driver.hooks[0][1]["CLOUD_OFFLOAD_BENCHMARK_ALLOWED_REGIONS"]
        == "EU-RO-1"
    )
    assert (
        assets[0]["sha256"]
        in driver.hooks[0][1]["CLOUD_OFFLOAD_BENCHMARK_ASSET_DIGESTS"]
    )
    assert (
        driver.hooks[0][1]["CLOUD_OFFLOAD_BENCHMARK_CANARY_NONCE"]
        == assets[0][CORRUPTION_NONCE_FIELD]
    )
    assert driver.hooks[1][1]["CLOUD_OFFLOAD_BENCHMARK_JOB_ID"] == "job-2"
    assert result["failure_injection"]["preparation_hook"]["exit_code"] == 0
    assert result["failure_injection"]["hook"]["exit_code"] == 0
    assert result["failure_injection"]["cleanup_hook"]["exit_code"] == 0


def test_corruption_hook_runs_only_after_a_successful_cold_base_manifest():
    class OrderedDriver(FakeDriver):
        def __init__(self, scripts):
            super().__init__(scripts)
            self.cold_base_manifest_ready = False

        def snapshot(self, job_id):
            snapshot = super().snapshot(job_id)
            if (
                self.jobs[job_id]["scenario"].name == "cold"
                and snapshot.get("status") == "completed"
            ):
                self.cold_base_manifest_ready = True
            return snapshot

        def run_hook(self, injection, context):
            if injection.kind == "corruption":
                assert self.cold_base_manifest_ready
            return super().run_hook(injection, context)

    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario("cold", "cold"),
                scenario(
                    "corruption",
                    "failure",
                    failure={
                        "kind": "corruption",
                        "before_submit": True,
                        "hook_argv": ["corruption-canary"],
                    },
                ),
            ]
        )
    )
    driver = OrderedDriver(
        {"cold": successful_script("pod-cold"), "corruption": successful_script("pod-corruption")}
    )

    BenchmarkRunner(driver).run(plan)

    assert driver.cold_base_manifest_ready
    assert [kind for kind, _ in driver.hooks] == ["corruption", "corruption"]


def test_corruption_uses_verified_hot_manifest_when_cold_storage_is_off():
    class PreparedDriver(FakeDriver):
        def base_manifest(self, result):
            return {"manifest_id": "verified-hot-manifest"} if result["cache_state"] == "hot" else None

    plan = BenchmarkPlan.from_dict(plan_dict([
        scenario("cold", "cold"),
        scenario("hot", "hot"),
        scenario("corruption", "failure", failure={
            "kind": "corruption", "before_submit": True, "trigger_event": "executed",
            "hook_argv": ["corruption-canary"],
        }),
    ]))
    driver = PreparedDriver({name: successful_script("pod-" + name) for name in ("cold", "hot", "corruption")})
    scorecard = BenchmarkRunner(driver).run(plan)
    assert all(result["passed"] for result in scorecard["results"]), scorecard["results"]
    assert [kind for kind, _ in driver.hooks] == ["corruption"] * 3


@pytest.mark.parametrize("drift", [None, "miss", "volume", "manifest", "digest", "image", "incomplete"])
def test_hot_base_manifest_requires_completed_restore_evidence(drift):
    driver = CoordinatorBenchmarkDriver("http://127.0.0.1:11435", None, CloudConfig(), ())
    digest = "sha256:" + "a" * 64
    manifest_id = "sha256:" + "b" * 64
    image = "sha256:" + "c" * 64
    profile = "sha256:" + "d" * 64
    job = {"id": "hot", "status": "completed", "model": "comfyui-partition-v1", "params": {
        "lease_id": "hot-lease", "cache_volume_id": "volume", "cache_datacenter_id": "AP-JP-1",
        "cache_manifest_id": manifest_id,
    }}
    manifest = {"manifest_id": manifest_id, "volume_id": "volume", "datacenter_id": "AP-JP-1",
        "profile_fingerprint": profile, "created_at": "2026-08-01T00:00:00+00:00",
        "producer": {"job_id": "warmup", "lease_id": "warmup-lease", "image_digest": image,
                     "cloud_offload_version": "test"},
        "artifacts": [{"digest": digest, "size": 10}],
    }
    receipt = {"manifest_id": manifest_id, "volume_id": "volume", "datacenter_id": "AP-JP-1",
        "artifacts": [{"digest": digest, "result": "hit", "bytes": 10}],
    }
    if drift == "miss":
        receipt["artifacts"][0]["result"] = "miss"
    elif drift == "volume":
        receipt["volume_id"] = "other"
    elif drift == "manifest":
        receipt["manifest_id"] = "other"
    elif drift == "digest":
        receipt["artifacts"][0]["digest"] = "other"
    elif drift == "image":
        manifest["producer"]["image_digest"] = "other"
    elif drift == "incomplete":
        job["status"] = "running"

    def request(method, path, **kwargs):
        if path == "/api/jobs/hot":
            return FakeResponse(job)
        if path == "/api/cache/manifests":
            return FakeResponse({"manifests": [manifest]})
        assert path == "/api/jobs/hot/events"
        return FakeResponse({"events": [{"event": {"type": "cache_restore_completed", "receipt": receipt}}]})

    driver._request = request
    result = driver.base_manifest({"job_id": "hot", "cache_state": "hot",
        "started_at": "2026-08-01T00:01:00+00:00", "submission_receipt": {
            "region": "AP-JP-1", "allowed_regions": ["AP-JP-1"], "image_digest": image,
            "profile_fingerprint": profile, "expected_model": "comfyui-partition-v1",
        }})
    assert bool(result) is (drift is None)


def test_corruption_requires_registry_manifest_identity_after_cold_pass(tmp_path):
    class RegistryDriver(FakeDriver):
        def __init__(self, scripts, registry):
            super().__init__(scripts)
            self.registry = registry
            self.manifest_queries = []

        def base_manifest(self, cold_result):
            self.manifest_queries.append(cold_result["job_id"])
            manifests = self.registry.query_manifests(datacenter_id="EU-RO-1")
            return manifests[0] if len(manifests) == 1 else None

    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario("cold", "cold"),
                scenario(
                    "corruption",
                    "failure",
                    failure={
                        "kind": "corruption",
                        "before_submit": True,
                        "trigger_event": "executed",
                        "hook_argv": ["corruption-canary"],
                    },
                ),
            ]
        )
    )
    registry = CacheRegistry(tmp_path / "cache.db")
    volume = registry.upsert_volume(
        provider="runpod",
        provider_volume_id="provider-volume",
        datacenter_id="EU-RO-1",
        ownership="adopted",
        capacity_bytes=1024,
        policy={},
        volume_id="volume-1",
    )
    registry.reconcile_index(
        volume.id,
        {
            "schema": INDEX_SCHEMA,
            "generation": "generation-1",
            "manifests": [
                {
                    "manifest_id": "manifest-1",
                    "profile_fingerprint": "profile-1",
                    "created_at": "2026-07-29T00:00:01+00:00",
                    "artifacts": [],
                }
            ],
        },
    )
    driver = RegistryDriver(
        {"cold": successful_script("pod-cold"), "corruption": successful_script("pod-corruption")},
        registry=registry,
    )

    scorecard = BenchmarkRunner(driver).run(plan)

    assert driver.manifest_queries == ["job-1"]
    assert len(driver.hooks) == 3
    assert [kind for kind, _ in driver.hooks] == ["corruption"] * 3
    assert scorecard["results"][1]["passed"] is True


def test_cold_failure_stops_before_dependent_corruption_mutation():
    class ColdFailureDriver(FakeDriver):
        def submit(self, scenario):
            if scenario.name == "cold":
                raise RuntimeError("cold base manifest was not created")
            return super().submit(scenario)

    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario("cold", "cold"),
                scenario(
                    "corruption",
                    "failure",
                    failure={
                        "kind": "corruption",
                        "before_submit": True,
                        "hook_argv": ["corruption-canary"],
                    },
                ),
            ]
        )
    )
    driver = ColdFailureDriver({"corruption": successful_script("pod-corruption")})

    scorecard = BenchmarkRunner(driver).run(plan)

    assert driver.hooks == []
    assert len(scorecard["results"]) == 1
    assert scorecard["campaign_abort"] == "unexpected_scenario_failure"
    assert driver.submit_calls == []


def test_corruption_observation_hook_has_a_longer_bounded_timeout():
    assert DEFAULT_HOOK_TIMEOUT_SECONDS == 120
    assert CORRUPTION_OBSERVE_HOOK_TIMEOUT_SECONDS == 270


def test_corruption_plan_load_creates_a_new_canary_for_each_campaign():
    raw = plan_dict(
        [
            scenario(
                "corruption",
                "failure",
                failure={
                    "kind": "corruption",
                    "before_submit": True,
                    "hook_argv": ["corruption-canary"],
                },
            )
        ]
    )

    first = BenchmarkPlan.from_dict(raw).scenarios[0].request["partition"]["assets"]
    second = BenchmarkPlan.from_dict(raw).scenarios[0].request["partition"]["assets"]

    assert len(first) == len(second) == 1
    assert first[0][CORRUPTION_NONCE_FIELD] != second[0][CORRUPTION_NONCE_FIELD]
    assert first[0]["sha256"] != second[0]["sha256"]


def test_two_phase_hook_cleans_up_when_submission_never_creates_a_job():
    class BrokenSubmitDriver(FakeDriver):
        def submit(self, scenario):
            if scenario.name == "corruption":
                raise RuntimeError("submission failed")
            return super().submit(scenario)

    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario("cold", "cold"),
                scenario(
                    "corruption",
                    "failure",
                    failure={
                        "kind": "corruption",
                        "before_submit": True,
                        "hook_argv": ["corruption-canary"],
                    },
                )
            ]
        )
    )
    driver = BrokenSubmitDriver({"cold": successful_script("pod-cold")})

    result = BenchmarkRunner(driver).run(plan)["results"][1]

    assert result["passed"] is False
    assert [
        context["CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE"] for _, context in driver.hooks
    ] == [
        "prepare",
        "cleanup",
    ]
    assert result["failure_injection"]["triggered"] is False
    assert result["failure_injection"]["cleanup_hook"]["exit_code"] == 0


def test_provider_failure_terminates_only_the_observed_instance():
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario(
                    "provider",
                    "failure",
                    failure={"kind": "provider", "trigger_phase": "worker_boot"},
                )
            ]
        )
    )
    driver = FakeDriver({"provider": successful_script("pod-provider")})

    scorecard = BenchmarkRunner(driver).run(plan)
    injection = scorecard["results"][0]["failure_injection"]

    assert scorecard["passed"] is True
    assert injection["receipts"] == [
        {
            "provider": "runpod",
            "instance_id": "pod-provider",
            "termination_requested": True,
        }
    ]
    assert driver.termination_attempts == [("runpod", "pod-provider")]


def test_cleanup_failure_is_a_hard_campaign_failure_and_reports_the_orphan():
    driver = FakeDriver(
        {"cold": Script(successful_script("pod-stuck").steps, terminate_succeeds=False)}
    )
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is False
    assert scorecard["campaign_abort"] == "orphan_cleanup_failed"
    assert scorecard["results"][0]["orphaned_resources"]
    assert scorecard["orphaned_resources"] == [
        {"provider": "runpod", "instance_id": "pod-stuck"}
    ]


def test_fresh_pod_waits_for_stale_worker_heartbeat_without_charging_scenario_time():
    driver = FakeDriver({"cold": successful_script("pod-cold")})
    driver.active_until = 3
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is True
    # The result includes one scenario polling second and excludes the three
    # seconds spent aging out the stale worker. Exact absence needs no wait.
    assert scorecard["results"][0]["duration_seconds"] == 1
    assert driver.clock >= 4


def test_scenario_restores_storage_policy_when_submission_fails():
    class BrokenSubmitDriver(FakeDriver):
        def submit(self, scenario):
            raise RuntimeError("submission failed")

    driver = BrokenSubmitDriver({})
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))

    scorecard = BenchmarkRunner(driver).run(plan)
    result = scorecard["results"][0]

    assert result["passed"] is False
    assert result["harness_error"] == "RuntimeError"
    assert result["scenario_restoration"]["restored"] is True
    assert driver.prepared_storage_policy == "smart"
    assert driver.restoration_calls == [("cold", "smart")]


def test_operator_interrupt_returns_aborted_scorecard_after_cleanup_and_restore():
    class InterruptDriver(FakeDriver):
        snapshot_calls = 0

        def snapshot(self, job_id):
            self.snapshot_calls += 1
            if self.snapshot_calls == 1:
                raise KeyboardInterrupt
            return {"status": "failed", "lifecycle_phase": "closure"}

    driver = InterruptDriver(
        {
            "cold": successful_script("pod-interrupted"),
            "hot": successful_script("pod-race-must-not-start"),
        }
    )
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold"), scenario("hot", "hot")])
    )

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is False
    assert scorecard["campaign_abort"] == "operator_interrupt:KeyboardInterrupt"
    assert scorecard["cleanup_proof"]["state"] == "confirmed"
    result = scorecard["results"][0]
    assert result["abort_reason"] == "operator_interrupt:KeyboardInterrupt"
    assert result["passed"] is False
    assert driver.submit_calls == ["cold"]
    assert driver.cancelled == {"job-1"}
    assert driver.termination_attempts == [("runpod", "pod-interrupted")]
    assert ("runpod", "pod-interrupted") in driver.terminated
    assert driver.prepared_storage_policy == "smart"
    assert driver.restoration_calls == [("cold", "smart")]


def test_concurrent_stop_signal_cannot_race_into_next_scenario_submission():
    entered_snapshot = threading.Event()
    release_interrupt = threading.Event()
    completed: list[dict] = []

    class StopRaceDriver(FakeDriver):
        def snapshot(self, job_id):
            entered_snapshot.set()
            assert release_interrupt.wait(timeout=2)
            raise KeyboardInterrupt

    driver = StopRaceDriver(
        {
            "cold": successful_script("pod-stop-race"),
            "hot": successful_script("pod-second-must-not-start"),
        }
    )
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold"), scenario("hot", "hot")])
    )
    runner_thread = threading.Thread(
        target=lambda: completed.append(BenchmarkRunner(driver).run(plan))
    )

    runner_thread.start()
    assert entered_snapshot.wait(timeout=2)
    release_interrupt.set()
    runner_thread.join(timeout=3)

    assert not runner_thread.is_alive()
    assert driver.submit_calls == ["cold"]
    assert completed[0]["campaign_abort"] == "operator_interrupt:KeyboardInterrupt"
    assert driver.cancelled == {"job-1"}
    assert driver.termination_attempts == [("runpod", "pod-stop-race")]


def test_coordinator_driver_restores_the_exact_prepared_storage_object():
    original = CloudConfig(
        prepared_storage={
            "enabled": True,
            "provider": "runpod",
            "policy": "smart",
            "region": "EU-RO-1",
            "cold_fallback": "ask",
            "managed_size_gb": 321,
            "existing_volume_id": "volume-existing",
            "max_monthly_storage_cost": 24.5,
            "confirmed": True,
            "tenant": "benchmark-tenant",
            "cache_private_assets": True,
            "shadow_admission": False,
        }
    ).prepared_storage
    state = {"prepared_storage": dict(original)}
    posts = []
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )

    def request(method, path, **kwargs):
        assert path == "/api/config"
        if method == "POST":
            posted = json.loads(json.dumps(kwargs["json"]["prepared_storage"]))
            posts.append(posted)
            state["prepared_storage"] = posted
        return FakeResponse(json.loads(json.dumps(state)))

    driver._request = request
    cold = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")])).scenarios[0]

    prepared = driver.prepare_scenario(cold)

    assert prepared == {
        "required": True,
        "prepared": True,
        "requested_policy": "off",
        "previous_policy": "smart",
        "changed": True,
    }
    assert state["prepared_storage"]["policy"] == "off"
    assert state["prepared_storage"]["enabled"] is False
    assert state["prepared_storage"]["existing_volume_id"] == "volume-existing"
    assert state["prepared_storage"]["tenant"] == "benchmark-tenant"

    restored = driver.restore_scenario(cold)

    assert restored == {
        "required": True,
        "restored": True,
        "restored_policy": "smart",
        "changed": True,
    }
    assert state["prepared_storage"] == original
    assert posts[-1] == original


def test_production_submit_rechecks_absolute_deadline_after_long_preflight(monkeypatch):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    calls = []
    clock = {"now": 0.0}
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: clock["now"])

    def preflight(scenario, request_payload):
        clock["now"] = 10.0
        return {
            "status": "ready", "preflight_id": "preflight-1",
            "manifest_digest": "sha256:" + "a" * 64,
            "recommendation": {"candidate_id": "candidate-1"},
        }

    driver._ready_preflight = preflight
    driver._request = lambda *args, **kwargs: calls.append((args, kwargs))
    driver.configure_submission_guard(absolute_deadline=2.0, max_cost_usd=0.5)
    definition = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]

    with pytest.raises(TimeoutError):
        driver.submit(definition)

    assert calls == []


@pytest.mark.parametrize("quote", [None, 0, "bad", float("nan")])
def test_production_submit_rejects_missing_or_invalid_quote_before_mutation(quote):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    calls = []
    candidate = {"candidate_id": "sha256:" + "b" * 64}
    if quote is not None:
        candidate["hourly_rate"] = quote
    driver._ready_preflight = lambda *args: {
        "status": "ready", "preflight_id": "preflight-1",
        "manifest_digest": "sha256:" + "a" * 64,
        "recommendation": {"candidate_id": candidate["candidate_id"]},
        "candidates": [candidate],
    }
    driver._request = lambda *args, **kwargs: calls.append((args, kwargs))
    driver.configure_submission_guard(
        absolute_deadline=driver.monotonic() + 100.0, max_cost_usd=0.01
    )
    definition = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]

    with pytest.raises((RuntimeError, ValueError)):
        driver.submit(definition)
    assert calls == []


@pytest.mark.parametrize("workflow_rate", [None, 3600])
def test_workflow_submission_uses_same_quote_guard_before_paid_post(workflow_rate):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    posts = []
    candidate = {"candidate_id": "candidate-workflow"}
    if workflow_rate is not None:
        candidate["hourly_rate"] = workflow_rate
    driver._ready_preflight = lambda *args: {
        "status": "ready", "preflight_id": "preflight-workflow",
        "manifest_digest": "sha256:" + "a" * 64,
        "recommendation": {"candidate_id": "candidate-workflow"},
        "candidates": [candidate],
    }

    def request(method, path, **kwargs):
        posts.append(path)
        return FakeResponse({"job_id": "must-not-exist"})

    driver._request = request
    driver.configure_submission_guard(
        absolute_deadline=driver.monotonic() + 100, max_cost_usd=0.000001
    )
    definition = scenario("workflow", "cold", endpoint="/api/workflows")
    definition["request"] = {"capsule": {"schema": "cloud-offload.workflow-capsule.v1"}}
    selected = BenchmarkPlan.from_dict(plan_dict([definition])).scenarios[0]

    with pytest.raises(RuntimeError):
        driver.submit(selected)
    assert "/api/workflows" not in posts


@pytest.mark.parametrize(
    ("endpoint", "workload_key"),
    [("/api/partitions", "partition"), ("/api/workflows", "capsule")],
)
def test_preflight_wire_sends_exactly_one_workload_shape(endpoint, workload_key):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    payloads = []

    def request(method, path, **kwargs):
        payloads.append(kwargs["json"])
        return FakeResponse({"status": "not_ready", "blockers": [{"code": "stop"}]})

    driver._request = request
    definition = scenario("shape", "cold", endpoint=endpoint)
    definition["request"] = {workload_key: {"schema": "cloud-offload.shape.v1"}}
    if endpoint == "/api/partitions":
        definition["request"]["force_execution"] = True
    selected = BenchmarkPlan.from_dict(plan_dict([definition])).scenarios[0]

    with pytest.raises(RuntimeError):
        driver._ready_preflight(selected, selected.request)

    assert workload_key in payloads[0]
    assert ({"partition", "capsule"} & set(payloads[0])) == {workload_key}


@pytest.mark.parametrize("initial_remaining", [0, -1])
def test_preflight_makes_no_call_when_deadline_has_no_positive_time(
    monkeypatch, initial_remaining
):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    calls = []
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: 10.0)
    driver._request = lambda *args, **kwargs: calls.append((args, kwargs))
    driver.configure_submission_guard(
        absolute_deadline=10.0 + initial_remaining, max_cost_usd=1
    )
    selected = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]

    with pytest.raises(TimeoutError):
        driver._ready_preflight(selected, selected.request)
    assert calls == []


def test_preflight_clamps_retry_sleep_and_request_timeout_to_deadline(monkeypatch):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    clock = {"now": 0.0}
    calls = []
    sleeps = []
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: clock["now"])

    def sleep(seconds):
        assert seconds > 0
        sleeps.append(seconds)
        clock["now"] += seconds

    def request(*args, **kwargs):
        calls.append(kwargs["timeout"])
        clock["now"] += 0.1
        return FakeResponse({"status": "not_ready", "blockers": [], "unknowns": []})

    monkeypatch.setattr("cloud_offload.benchmark.time.sleep", sleep)
    driver._request = request
    driver.configure_submission_guard(absolute_deadline=2.0, max_cost_usd=1)
    selected = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]

    with pytest.raises(TimeoutError):
        driver._ready_preflight(selected, selected.request)

    assert calls == [pytest.approx(2.0, abs=0.001)]
    assert sleeps == [pytest.approx(1.9, abs=0.001)]
    assert clock["now"] == pytest.approx(2.0, abs=0.001)


def test_preflight_clock_jump_never_retries_or_sleeps(monkeypatch):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    clock = {"now": 0.0}
    calls = []
    sleeps = []
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("cloud_offload.benchmark.time.sleep", lambda value: sleeps.append(value))

    def request(*args, **kwargs):
        calls.append(kwargs["timeout"])
        clock["now"] = 3.0
        return FakeResponse({"status": "not_ready", "blockers": [], "unknowns": []})

    driver._request = request
    driver.configure_submission_guard(absolute_deadline=2.0, max_cost_usd=1)
    selected = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]

    with pytest.raises(TimeoutError):
        driver._ready_preflight(selected, selected.request)
    assert len(calls) == 1
    assert sleeps == []


def test_preflight_propagates_provider_request_timeout_without_backoff(monkeypatch):
    import requests

    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    sleeps = []
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: 5.0)
    monkeypatch.setattr("cloud_offload.benchmark.time.sleep", lambda value: sleeps.append(value))

    def request(*args, **kwargs):
        assert kwargs["timeout"] == pytest.approx(2.0, abs=0.001)
        raise requests.Timeout("provider timeout")

    driver._request = request
    driver.configure_submission_guard(absolute_deadline=7.0, max_cost_usd=1)
    selected = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]

    with pytest.raises(requests.Timeout):
        driver._ready_preflight(selected, selected.request)
    assert sleeps == []


def test_request_retry_does_not_start_second_call_after_slow_attempt(monkeypatch):
    import requests

    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    clock = {"now": 0.0}
    calls = []
    sleeps = []
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("cloud_offload.benchmark.time.sleep", lambda value: sleeps.append(value))

    def request(*args, **kwargs):
        calls.append(kwargs["timeout"])
        clock["now"] = 10.0
        raise requests.Timeout("slow provider")

    driver.session.request = request
    with pytest.raises(requests.Timeout):
        driver._request(
            "GET", "/safe", retry_safe=True, timeout=30, absolute_deadline=2.0
        )

    assert len(calls) == 1
    assert sum(calls[0]) <= 2.001
    assert sleeps == []


def test_inventory_retry_does_not_start_second_provider_call_after_deadline(monkeypatch):
    class SlowConnector:
        calls = 0

        def list_instances(self):
            self.calls += 1
            clock["now"] = 10.0
            raise TimeoutError("slow inventory")

    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    clock = {"now": 0.0}
    connector = SlowConnector()
    driver.connectors = {"runpod": connector}
    sleeps = []
    monkeypatch.setattr("cloud_offload.benchmark.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("cloud_offload.benchmark.time.sleep", lambda value: sleeps.append(value))

    with pytest.raises(TimeoutError):
        driver.inventory(("runpod",), absolute_deadline=2.0)

    assert connector.calls == 1
    assert sleeps == []


def test_cleanup_deadline_never_suppresses_first_exact_termination():
    class JumpCleanupDriver:
        clock = 0.0
        inventory_calls = 0
        termination_calls = []

        def monotonic(self):
            return self.clock

        def inventory(self, providers):
            self.inventory_calls += 1
            return {
                "runpod": {
                    "pod-paid": InstanceObservation(
                        id="pod-paid", provider="runpod", hourly_rate=1,
                        status="running", managed=True,
                    )
                }
            }

        def inventory_until(self, providers, *, absolute_deadline):
            return self.inventory(providers)

        def terminate(self, provider, instance_id):
            self.termination_calls.append((provider, instance_id))
            self.clock = 10.0
            return True

        def sleep(self, seconds):
            raise AssertionError("no wait is allowed after the deadline")

    driver = JumpCleanupDriver()
    meter = _ResourceMeter(
        id="pod-paid", provider="runpod", hourly_rate=1,
        first_seen=0, last_seen=0, source="journal", lease_id="lease-paid",
    )
    receipt = BenchmarkRunner(driver)._cleanup_resources(
        ("runpod",), {"runpod": {}}, {("runpod", "pod-paid"): meter}, 1,
        include_untracked=False,
    )[0]

    assert driver.termination_calls == [("runpod", "pod-paid")]
    assert receipt["termination_attempts"] == 1
    assert receipt["provider_absent"] is False
    assert receipt["inventory_error"] == "TimeoutError"


def test_worker_quiescence_uses_deadline_aware_worker_call_and_stops_after_overrun():
    class SlowWorkerDriver:
        clock = 0.0
        guarded_calls = []
        sleeps = []

        def monotonic(self):
            return self.clock

        def active_workers(self, providers):
            raise AssertionError("deadline-bound worker wait used the plain method")

        def active_workers_until(self, providers, *, absolute_deadline):
            self.guarded_calls.append((providers, absolute_deadline))
            self.clock = 30.0
            return [{"provider": "runpod", "worker_id": "paid-worker"}]

        def sleep(self, seconds):
            self.sleeps.append(seconds)

    driver = SlowWorkerDriver()
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold")],
            fresh_worker_timeout_seconds=1,
            max_campaign_seconds=10,
        )
    )

    assert BenchmarkRunner(driver)._wait_for_worker_quiescence(plan, 0.0) is False
    assert driver.guarded_calls == [(('runpod',), 1.0)]
    assert driver.sleeps == []


def test_cleanup_clamps_initial_inventory_and_does_not_prove_after_overrun():
    class SlowCleanupDriver:
        clock = 0.0
        guarded_calls = []
        termination_calls = []

        def monotonic(self):
            return self.clock

        def inventory(self, providers):
            raise AssertionError("deadline-bound cleanup used the plain method")

        def inventory_until(self, providers, *, absolute_deadline):
            self.guarded_calls.append((providers, absolute_deadline))
            self.clock = 30.0
            return {
                "runpod": {
                    "pod-paid": InstanceObservation(
                        id="pod-paid", provider="runpod", hourly_rate=1,
                        status="running", managed=True,
                    )
                }
            }

        def terminate(self, provider, instance_id):
            self.termination_calls.append((provider, instance_id))
            return True

        def sleep(self, seconds):
            raise AssertionError("cleanup cannot wait after its deadline")

    driver = SlowCleanupDriver()
    meter = _ResourceMeter(
        id="pod-paid", provider="runpod", hourly_rate=1,
        first_seen=0, last_seen=0, source="journal", lease_id="lease-paid",
    )

    receipt = BenchmarkRunner(driver)._cleanup_resources(
        ("runpod",), {"runpod": {}}, {("runpod", "pod-paid"): meter}, 1,
        include_untracked=False,
    )[0]

    assert driver.guarded_calls == [(('runpod',), 1.0)]
    assert driver.termination_calls == [("runpod", "pod-paid")]
    assert receipt["termination_attempts"] == 1
    assert receipt["provider_absent"] is False
    assert receipt["inventory_error"] == "TimeoutError"


def test_late_empty_cleanup_inventory_cannot_suppress_first_exact_termination():
    class LateEmptyCleanupDriver:
        clock = 0.0
        guarded_calls = []
        termination_calls = []

        def monotonic(self):
            return self.clock

        def inventory_until(self, providers, *, absolute_deadline):
            self.guarded_calls.append((providers, absolute_deadline))
            self.clock = 30.0
            return {"runpod": {}}

        def terminate(self, provider, instance_id):
            self.termination_calls.append((provider, instance_id))
            return True

        def sleep(self, seconds):
            raise AssertionError("cleanup cannot wait after its deadline")

    driver = LateEmptyCleanupDriver()
    meter = _ResourceMeter(
        id="pod-paid", provider="runpod", hourly_rate=1,
        first_seen=0, last_seen=0, source="journal", lease_id="lease-paid",
    )

    receipt = BenchmarkRunner(driver)._cleanup_resources(
        ("runpod",), {"runpod": {}}, {("runpod", "pod-paid"): meter}, 1,
        include_untracked=False,
    )[0]

    assert driver.guarded_calls == [(('runpod',), 1.0)]
    assert driver.termination_calls == [("runpod", "pod-paid")]
    assert receipt["termination_attempts"] == 1
    assert receipt["provider_absent"] is False
    assert receipt["inventory_error"] == "TimeoutError"


def test_cleanup_does_not_retry_termination_after_slow_proof_crosses_deadline():
    class SlowProofDriver:
        clock = 0.0
        inventory_calls = 0
        termination_calls = []

        def monotonic(self):
            return self.clock

        def inventory_until(self, providers, *, absolute_deadline):
            self.inventory_calls += 1
            if self.inventory_calls == 2:
                self.clock = 30.0
            return {
                "runpod": {
                    "pod-paid": InstanceObservation(
                        id="pod-paid", provider="runpod", hourly_rate=1,
                        status="running", managed=True,
                    )
                }
            }

        def terminate(self, provider, instance_id):
            self.termination_calls.append((provider, instance_id))
            return True

        def sleep(self, seconds):
            raise AssertionError("cleanup cannot wait after its deadline")

    driver = SlowProofDriver()
    meter = _ResourceMeter(
        id="pod-paid", provider="runpod", hourly_rate=1,
        first_seen=0, last_seen=0, source="journal", lease_id="lease-paid",
    )

    receipt = BenchmarkRunner(driver)._cleanup_resources(
        ("runpod",), {"runpod": {}}, {("runpod", "pod-paid"): meter}, 1,
        include_untracked=False,
    )[0]

    assert driver.inventory_calls == 2
    assert driver.termination_calls == [("runpod", "pod-paid")]
    assert receipt["provider_absent"] is False
    assert receipt["inventory_error"] == "TimeoutError"


def test_paid_scenario_uses_only_deadline_aware_inventory_and_worker_reads():
    class DeadlineOnlyDriver(FakeDriver):
        plain_paid_inventory_calls = 0
        plain_worker_calls = 0

        def inventory(self, providers):
            if self.current_job is not None:
                self.plain_paid_inventory_calls += 1
                raise AssertionError("paid inventory read has no deadline")
            return FakeDriver.inventory(self, providers)

        def inventory_until(self, providers, *, absolute_deadline):
            return FakeDriver.inventory(self, providers)

        def active_workers(self, providers):
            self.plain_worker_calls += 1
            raise AssertionError("worker read has no deadline")

        def active_workers_until(self, providers, *, absolute_deadline):
            return FakeDriver.active_workers(self, providers)

    driver = DeadlineOnlyDriver({"cold": successful_script("pod-deadline")})
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["results"][0]["passed"] is True
    assert driver.plain_paid_inventory_calls == 0
    assert driver.plain_worker_calls == 0


def test_campaign_baseline_overrun_aborts_after_one_bounded_inventory_call():
    class SlowBaselineDriver:
        clock = 0.0
        guarded_calls = []
        plain_calls = 0
        submit_calls = []

        def monotonic(self):
            return self.clock

        def inventory(self, providers):
            self.plain_calls += 1
            raise AssertionError("campaign baseline used the plain method")

        def inventory_until(self, providers, *, absolute_deadline):
            self.guarded_calls.append((providers, absolute_deadline))
            self.clock = 10.0
            return {"runpod": {}}

        def submit(self, scenario):
            self.submit_calls.append(scenario.name)
            raise AssertionError("scenario cannot start after baseline deadline")

    driver = SlowBaselineDriver()
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")], max_campaign_seconds=1)
    )

    scorecard = BenchmarkRunner(driver).run(plan)

    assert driver.guarded_calls == [(('runpod',), 1.0)]
    assert driver.plain_calls == 0
    assert driver.submit_calls == []
    assert scorecard["campaign_abort"] == "campaign_runtime_limit"
    assert scorecard["results"] == []
    assert scorecard["final_audit_error"] == "TimeoutError"
    assert scorecard["estimated_compute_cost_upper_usd"] == 1.0


def test_real_preflight_server_rejects_both_workload_shapes(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from cloud_offload import server
    from tests.test_preflight import config_for_preflight

    config = config_for_preflight(tmp_path)
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    response = TestClient(server.app).post(
        "/api/preflight",
        json={"partition": {"partition_id": "p"},
              "capsule": {"schema": "cloud-offload.workflow-capsule.v1"}},
    )

    assert response.status_code == 400
    assert "exactly one" in response.text


def test_coordinator_driver_refuses_to_overwrite_a_concurrent_config_change():
    original = CloudConfig(
        prepared_storage={
            "enabled": True,
            "policy": "smart",
            "confirmed": True,
            "existing_volume_id": "volume-existing",
        }
    ).prepared_storage
    state = {"prepared_storage": dict(original)}
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )

    def request(method, path, **kwargs):
        if method == "POST":
            state["prepared_storage"] = json.loads(
                json.dumps(kwargs["json"]["prepared_storage"])
            )
        return FakeResponse(json.loads(json.dumps(state)))

    driver._request = request
    cold = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")])).scenarios[0]
    driver.prepare_scenario(cold)
    state["prepared_storage"]["tenant"] = "changed-concurrently"

    with pytest.raises(RuntimeError, match="refusing to overwrite"):
        driver.restore_scenario(cold)

    assert state["prepared_storage"]["tenant"] == "changed-concurrently"


def test_coordinator_driver_preflights_partition_before_submission():
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    calls = []

    def request(method, path, **kwargs):
        calls.append((method, path, kwargs.get("json")))
        if path == "/api/preflight":
            return FakeResponse(
                {
                    "schema": "cloud-offload.preflight.v1",
                    "preflight_id": "preflight-1",
                    "manifest_digest": "sha256:" + "a" * 64,
                    "status": "ready",
                    "recommendation": {"candidate_id": "sha256:" + "b" * 64},
                    "execution_plan": {
                        "profile": "comfyui",
                        "image_digest": "sha256:" + "c" * 64,
                        "provider": "runpod",
                        "region": "US-MD-1",
                        "prepared_volume_id": None,
                    },
                    "preparation": {
                        "complete": False,
                        "coverage_percent": 0,
                    },
                        "candidates": [
                            {
                                "candidate_id": "sha256:" + "b" * 64,
                                "hourly_rate": 0.1,
                                "region": "US-MD-1",
                            "prepared_volume_id": None,
                        }
                    ],
                }
            )
        return FakeResponse({"job_id": "job-1"})

    driver._request = request
    selected_value = scenario("cold", "cold")
    selected_value["allowed_regions"] = ["US-MD-1"]
    selected = BenchmarkPlan.from_dict(plan_dict([selected_value])).scenarios[0]
    driver.configure_submission_guard(
        absolute_deadline=driver.monotonic() + 100, max_cost_usd=1
    )

    assert driver.submit(selected) == "job-1"
    assert [item[1] for item in calls] == ["/api/preflight", "/api/partitions"]
    submitted = calls[1][2]
    assert submitted["preflight_id"] == "preflight-1"
    assert submitted["manifest_digest"] == "sha256:" + "a" * 64
    assert submitted["candidate_id"] == "sha256:" + "b" * 64
    assert submitted["private_prompt"] == "private-cold"
    assert calls[0][2]["allowed_regions"] == ["US-MD-1"]
    assert driver.submission_receipt("job-1") == {
        "schema": "cloud-offload.benchmark-submission-receipt.v1",
        "preflight_status": "ready",
        "profile": "comfyui",
        "image_digest": "sha256:" + "c" * 64,
        "expected_model": "comfyui-partition-v1",
        "provider": "runpod",
        "region": "US-MD-1",
        "allowed_regions": ["US-MD-1"],
        "prepared_volume_bound": False,
        "preparation_complete": False,
        "preparation_coverage_percent": 0.0,
        "cold_fallback_available": True,
    }


def test_coordinator_driver_accepts_only_manifest_bound_to_cold_job_and_lease():
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    profile = "sha256:" + "p" * 64
    driver._submission_receipts["job-1"] = {
        "profile_fingerprint": profile,
        "image_digest": "sha256:" + "i" * 64,
        "expected_model": "comfyui-partition-v1",
        "allowed_regions": ["US-MD-1"],
        "region": "US-MD-1",
    }
    valid = {
        "manifest_id": "sha256:" + "m" * 64,
        "profile_fingerprint": profile,
        "created_at": "2026-08-01T00:00:02+00:00",
        "producer": {
            "job_id": "job-1", "lease_id": "lease-1",
            "image_digest": "sha256:" + "i" * 64,
            "cloud_offload_version": "test",
        },
        "artifacts": [{"digest": "sha256:" + "a" * 64, "size": 1}],
        "volume_id": "volume-1",
        "datacenter_id": "US-MD-1",
    }
    unrelated = {**valid, "manifest_id": "sha256:" + "u" * 64,
                 "producer": {"job_id": "other", "lease_id": "other"}}

    def request(method, path, **kwargs):
        if path == "/api/jobs/job-1":
            return FakeResponse({
                "id": "job-1",
                "params": {
                    "lease_id": "lease-1",
                    "cache_volume_id": "volume-1",
                    "cache_datacenter_id": "US-MD-1",
                },
                "model": "comfyui-partition-v1",
            })
        assert path == "/api/cache/manifests"
        return FakeResponse({"manifests": [unrelated, valid]})

    driver._request = request
    result = driver.base_manifest({
        "job_id": "job-1",
        "started_at": "2026-08-01T00:00:01+00:00",
        "submission_receipt": driver.submission_receipt("job-1"),
    })
    assert result["manifest_id"] == valid["manifest_id"]
    assert result["lease_id"] == "lease-1"


@pytest.mark.parametrize(
    ("job_model", "receipt_region", "job_region", "manifest_region"),
    [
        ("wrong-model", "US-MD-1", "US-MD-1", "US-MD-1"),
        ("comfyui-partition-v1", "EU-RO-1", "US-MD-1", "US-MD-1"),
        ("comfyui-partition-v1", "US-MD-1", "EU-RO-1", "EU-RO-1"),
    ],
)
def test_base_manifest_rejects_model_or_region_identity_mismatch(
    job_model, receipt_region, job_region, manifest_region
):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    profile = "sha256:" + "p" * 64
    image = "sha256:" + "i" * 64
    driver._submission_receipts["job-1"] = {
        "profile_fingerprint": profile,
        "image_digest": image,
        "expected_model": "comfyui-partition-v1",
        "allowed_regions": [receipt_region],
        "region": receipt_region,
    }
    manifest = {
        "manifest_id": "sha256:" + "m" * 64,
        "profile_fingerprint": profile,
        "created_at": "2026-08-01T00:00:02+00:00",
        "producer": {
            "job_id": "job-1", "lease_id": "lease-1",
            "image_digest": image, "cloud_offload_version": "test",
        },
        "artifacts": [{"digest": "sha256:" + "a" * 64, "size": 1}],
        "volume_id": "volume-1",
        "datacenter_id": manifest_region,
    }

    def request(method, path, **kwargs):
        if path == "/api/jobs/job-1":
            return FakeResponse({
                "id": "job-1", "model": job_model,
                "params": {
                    "lease_id": "lease-1", "cache_volume_id": "volume-1",
                    "cache_datacenter_id": job_region,
                },
            })
        return FakeResponse({"manifests": [manifest]})

    driver._request = request
    assert driver.base_manifest({
        "job_id": "job-1", "started_at": "2026-08-01T00:00:01+00:00",
        "submission_receipt": driver.submission_receipt("job-1"),
    }) is None


def _ready_preflight_response():
    return FakeResponse(
        {
            "schema": "cloud-offload.preflight.v1",
            "preflight_id": "preflight-1",
            "manifest_digest": "sha256:" + "a" * 64,
            "status": "ready",
            "recommendation": {"candidate_id": "sha256:" + "b" * 64},
            "execution_plan": {
                "profile": "comfyui",
                "image_digest": "sha256:" + "c" * 64,
                "provider": "runpod",
                "region": "US-MD-1",
                "prepared_volume_id": None,
            },
            "preparation": {"complete": False, "coverage_percent": 0},
            "candidates": [{
                "candidate_id": "sha256:" + "b" * 64,
                "hourly_rate": 0.1,
                "region": "US-MD-1", "prepared_volume_id": None,
            }],
        }
    )


def test_a_transient_no_offer_preflight_is_retried_until_an_offer_returns(
    monkeypatch,
):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    monkeypatch.setattr(
        "cloud_offload.benchmark.time.sleep", lambda seconds: None
    )
    preflight_calls = []

    def request(method, path, **kwargs):
        if path == "/api/preflight":
            preflight_calls.append(path)
            if len(preflight_calls) < 3:
                return FakeResponse(
                    {
                        "status": "not_ready",
                        "blockers": [],
                        "unknowns": [{"code": "no_current_viable_offer"}],
                    }
                )
            return _ready_preflight_response()
        return FakeResponse({"job_id": "job-1"})

    driver._request = request
    selected = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]
    driver.configure_submission_guard(
        absolute_deadline=driver.monotonic() + 100, max_cost_usd=1
    )

    assert driver.submit(selected) == "job-1"
    assert len(preflight_calls) == 3


def test_a_blocked_preflight_fails_immediately_without_retry(monkeypatch):
    driver = CoordinatorBenchmarkDriver(
        "http://127.0.0.1:11435", None, CloudConfig(), ()
    )
    monkeypatch.setattr(
        "cloud_offload.benchmark.time.sleep",
        lambda seconds: pytest.fail("a blocked preflight must not be retried"),
    )
    preflight_calls = []

    def request(method, path, **kwargs):
        preflight_calls.append(path)
        return FakeResponse(
            {
                "status": "blocked",
                "blockers": [{"code": "huggingface_credential_missing"}],
                "unknowns": [],
            }
        )

    driver._request = request
    selected = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold")])
    ).scenarios[0]

    with pytest.raises(RuntimeError, match="huggingface_credential_missing"):
        driver.submit(selected)
    assert preflight_calls == ["/api/preflight"]


def test_preparation_measurement_uses_staging_to_execution_transition():
    events = [
        event(1, "job_created", "readiness", 1),
        event(2, "phase_timing", "staging_started", 10),
        event(3, "weight_download_progress", "weight_download_progress", 25),
        event(4, "job_status_changed", "execution", 40),
        event(5, "execution_success", "result_transfer", 50),
    ]

    assert _preparation_seconds(events) == 30.0
    assert _preparation_seconds(events[:3]) is None
    assert (
        _seconds_from_event_to(
            [event(1, "cancellation_requested", "resource_closure", 10)],
            "cancellation_requested",
            "2026-07-29T00:00:50+00:00",
        )
        == 40.0
    )


def test_benchmark_regions_are_validated_and_safe_summary_keeps_them():
    selected = scenario("cold", "cold")
    selected["allowed_regions"] = ["EU-RO-1"]
    plan = BenchmarkPlan.from_dict(plan_dict([selected]))

    assert plan.scenarios[0].allowed_regions == ("EU-RO-1",)
    assert plan.safe_summary()["scenarios"][0]["allowed_regions"] == ["EU-RO-1"]

    duplicate = scenario("cold", "cold")
    duplicate["allowed_regions"] = ["EU-RO-1", "EU-RO-1"]
    with pytest.raises(ValueError, match="unique non-empty"):
        BenchmarkPlan.from_dict(plan_dict([duplicate]))


def test_cli_validation_redacts_request_and_run_requires_spend_confirmation(
    monkeypatch, capsys, tmp_path
):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(plan_dict([scenario("cold", "cold")])), encoding="utf-8"
    )
    from cloud_offload.__main__ import main

    monkeypatch.setattr(
        sys,
        "argv",
        ["cloud-offload", "benchmark", "validate", "--plan", str(plan_path)],
    )
    main()
    validated = capsys.readouterr().out

    assert "private-cold" not in validated
    assert "request_digest" in validated

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cloud-offload",
            "benchmark",
            "run",
            "--plan",
            str(plan_path),
            "--output",
            str(tmp_path / "scorecard.json"),
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        main()

    assert stopped.value.code == 2
    assert "--confirm-spend" in capsys.readouterr().err

    hooked_path = tmp_path / "hooked.json"
    hooked_path.write_text(
        json.dumps(
            plan_dict(
                [
                    scenario(
                        "restart",
                        "failure",
                        failure={
                            "kind": "restart",
                            "hook_argv": ["restart-coordinator"],
                        },
                    )
                ]
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cloud-offload",
            "benchmark",
            "run",
            "--plan",
            str(hooked_path),
            "--output",
            str(tmp_path / "scorecard.json"),
            "--confirm-spend",
        ],
    )
    with pytest.raises(SystemExit) as hook_stopped:
        main()

    assert hook_stopped.value.code == 2
    assert "--allow-hooks" in capsys.readouterr().err
