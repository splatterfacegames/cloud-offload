# Production benchmark and scorecard

Cloud Offload's production benchmark exercises the same coordinator, dispatcher,
worker image, provider connector, and prepared storage as an ordinary job. It is
not a synthetic timing loop. Its output is a comparable, redacted JSON scorecard
and its first obligation is to leave no paid compute behind.

## Safety contract

Every plan must declare finite positive ceilings for:

- total estimated campaign compute cost;
- estimated compute cost per scenario;
- total campaign runtime;
- scenario runtime;
- runner readiness runtime and compute cost;
- cleanup verification time; and
- the time allowed for a terminated worker heartbeat to age out before the next
  fresh-Pod scenario.

`benchmark run` refuses to start without `--confirm-spend`. The default
`exclusive: true` mode also refuses to start while a provider account has an
active instance named `cloud-offload-worker-*`. Baseline resources are never
terminated. During a campaign the harness attributes resources from JobEventV2
and, in exclusive mode, from new provider inventory entries.

The harness cancels a job when a scenario, campaign-cost, campaign-runtime, or
scenario-runtime limit is reached. After every scenario it independently asks
the provider to terminate the exact attributable instance IDs, retries, and
records provider-absence receipts. It repeats the audit at campaign end. Any
remaining Cloud Offload instance makes the scorecard fail and stops subsequent
scenarios.

Cost is intentionally an upper estimate, accruing each observed Pod from scenario
submission through verified provider absence. Provider billing granularity and a
termination request's latency can create a small overshoot beyond a locally
observed threshold; the limit is a circuit breaker, not a prepaid provider
escrow. Use a short poll interval and leave headroom below the amount you can
actually spend.

## Plan

A plan uses `cloud-offload.benchmark-plan.v1`:

```json
{
  "schema": "cloud-offload.benchmark-plan.v1",
  "providers": ["runpod"],
  "exclusive": true,
  "limits": {
    "max_total_cost_usd": 1.00,
    "max_scenario_cost_usd": 0.25,
    "max_campaign_seconds": 3600,
    "poll_seconds": 2,
    "cleanup_timeout_seconds": 90,
    "fresh_worker_timeout_seconds": 120,
    "runner_readiness_timeout_seconds": 180,
    "max_runner_readiness_cost_usd": 0.05
  },
  "scenarios": [
    {
      "name": "fresh-pod-cold-1",
      "cache_state": "cold",
      "endpoint": "/api/partitions",
      "request": {
        "partition": { "...": "ordinary request body" },
        "force_execution": true
      },
      "prepared_storage_policy": "off",
      "timeout_seconds": 900,
      "expected_statuses": ["completed"],
      "fresh_instance": true
    },
    {
      "name": "fresh-pod-hot-1",
      "cache_state": "hot",
      "endpoint": "/api/partitions",
      "request": {
        "partition": { "...": "same manifest and inputs" },
        "force_execution": true
      },
      "prepared_storage_policy": "smart",
      "timeout_seconds": 600,
      "expected_statuses": ["completed"],
      "fresh_instance": true
    }
  ]
}
```

Corruption requires a manifest verified against the cache registry after a
successful run. A hot run can establish this dependency through its completed
restore receipt: the manifest, volume, region, runtime image, and restored
artifact digests must match. A storage-disabled cold run need not publish a
prepared manifest.

Cold and hot scenarios must begin cold and alternate. A cold scenario must set
`prepared_storage_policy: off`; a hot scenario must select `smart`, `strict`, or
`pinned`. This makes the cache-state label an enforced control rather than a
comment. Before submission, the harness snapshots the complete prepared-storage
object, applies and verifies the scenario policy, and records only a safe policy
receipt. A hot scenario refuses to proceed unless storage was already confirmed
and an existing volume is bound, so a benchmark cannot silently create durable
storage. After exact Pod cleanup, the harness restores and verifies the complete
prior object on success, failure, or operator interrupt. If settings changed
concurrently, it fails rather than overwriting the newer state.

Every fresh scenario waits until the coordinator reports no active heartbeat for
the selected providers; this prevents a recently terminated Pod's stale worker
record from suppressing the next rental. The harness verifies that a fresh
provider instance actually appeared, so a result-cache hit cannot masquerade as
a fresh-Pod hot restore.
Fresh partition scenarios are rejected at plan validation unless their ordinary
submission request explicitly sets `force_execution: true`; the coordinator then
bypasses only the completed-result cache while leaving prepared-state caching in
place. This flag exists for measurement and intentional recomputation, not as a
general performance setting.

The request body remains local in the plan. Validation and scorecards include
only its canonical SHA-256 digest, never the workflow, prompt, or input values.
The canonical request digest is the lowercase bare 64-hex SHA-256 value produced
by the harness. Public evidence readers also accept the normalized
`sha256:<64-hex>` form; all other digest syntax is rejected.

`runner_readiness_timeout_seconds` defaults to 300 seconds. Its clock starts at
the start of the scenario, before preparation and submission, and stops when an
active worker or `runner_ready` event proves that ComfyUI is ready. The readiness
cost ceiling defaults to `max_scenario_cost_usd`. A startup-only validation can
set both values lower, as in the example, without changing the dispatcher's
normal one-hour runner-registration policy. `scenario_active_seconds` measures
this same start through terminal state or cancellation request. The scenario
`duration_seconds` also includes verified provider cleanup and config restore.

The scorecard records five startup facts: allocation, image pull, container
start, runner callback, and ComfyUI readiness. It uses only normalized provider
state, container uptime telemetry, and worker status. A phase is `unknown` when
the provider API cannot prove it. RunPod REST v2 does not publish a separate
image-pull state, so the harness never infers one from `RUNNING`.

## Failure injection

The five Milestone 0 failure classes are represented explicitly:

| `kind` | Action |
|---|---|
| `cancellation` | Calls the ordinary job cancellation API. |
| `provider` | Terminates the exact observed provider instance. |
| `storage` | Runs an operator-supplied hook at the selected phase/event. |
| `corruption` | Runs an operator-supplied hook at the selected phase/event. |
| `restart` | Runs an operator-supplied hook at the selected phase/event. |

An injection can declare `trigger_phase`, `trigger_event`, `after_seconds`, or a
combination. Storage, corruption, and restart hooks require `hook_argv` in the
plan and the run additionally requires `--allow-hooks`. Commands are executed as
an argument vector without a shell. The harness passes job, scenario, failure
kind, and observed instance IDs through `CLOUD_OFFLOAD_BENCHMARK_*` environment
variables. Hook arguments and output are deliberately absent from the scorecard
because either can contain object keys, URLs, or credentials. A non-zero hook
exit code fails the scenario.

An external hook may also set `before_submit: true`. The harness then invokes it
in three explicit stages: `prepare` before the job exists, `observe` at the
configured post-submit trigger, and `cleanup` on every exit path. Context includes
only the request digest and declared asset digests, never the request body. The
scorecard requires all three receipts to succeed. This is used when provider
state must settle before Pod creation, such as a corruption canary on a newly
attached volume.

Hooks are an explicit operator boundary: they may mutate external state. Keep
them narrow, idempotent, and reversible, and make the hook wait until its intended
failure or restart is observable before it exits.

Cloud Offload ships three reviewed canaries for its own production matrix. They
still require a matching benchmark-hook environment and `--allow-hooks`; direct
invocation is refused:

```json
{"kind": "storage", "hook_argv": ["cloud-offload", "benchmark-hook", "storage"]}
{"kind": "corruption", "before_submit": true, "hook_argv": ["cloud-offload", "benchmark-hook", "corruption"]}
{"kind": "restart", "hook_argv": ["cloud-offload", "benchmark-hook", "restart"]}
```

- `storage` requires strict prepared placement, temporarily substitutes a
  nonexistent volume binding, requires `provisioning_failed` before any provider
  launch, and restores the complete scenario config. It never mutates provider
  storage.
- `corruption` is configured with `before_submit: true`. Its prepare stage selects
  the smallest digest-addressed model object required by the plan, makes a
  server-side backup, writes and settles a wrong-sized canary, and only then
  permits job submission. Its observe stage requires the worker to emit
  `cache_artifact_quarantined`; cleanup restores the canonical object if the
  worker has not already repopulated it and deletes the backup on every path.
- `restart` supports a local HTTP coordinator. It requires the service-file PID
  and authenticated health PID to agree, stops that exact process, starts a
  replacement on the same address, and succeeds only when health and the active
  job journal are available again.

The corruption canary is intentionally integrity-destructive for one immutable
object during its bounded window. Its backup and `finally` recovery path are why
it is suitable for the production matrix; arbitrary corruption commands are not.

## Commands

Validate without submitting work or loading provider credentials:

```bash
cloud-offload benchmark validate --plan benchmark.json
```

Run and atomically write a scorecard:

```bash
cloud-offload benchmark run \
  --plan benchmark.json \
  --output scorecards/production.json \
  --confirm-spend
```

Add `--allow-hooks` only for a reviewed plan that contains external failure-hook
commands. A passing process exits zero. A failed scenario, untriggered injection,
budget circuit breaker, missing fresh Pod, or orphaned provider resource writes a
failed scorecard and exits non-zero.

The first unexpected scenario failure, limit, or operator interrupt stops the
matrix before another submission. `KeyboardInterrupt` and `SystemExit` request
job cancellation, run exact provider cleanup, restore scenario policy, and then
atomically publish an aborted scorecard. The release controller atomically adds
the matching safe stop reason, completed-scenario facts, cost, and cleanup proof
to its ledger. Exception messages, commands, tokens, URLs, and local paths do not
enter that interrupt receipt.

## Milestone 7 release controller

One benchmark campaign is not a production release claim. The M7 controller
adds an atomic ledger and requires at least 30 trailing full matrices. A failed
matrix resets the consecutive count to zero. The controller stops after the
first failure so that it does not spend money on evidence that cannot extend the
release window.

A release plan uses `cloud-offload.release-plan.v1`. It declares:

- the exact backend and ComfyUI extension Git revisions;
- the public worker profiles and pinned image digests under release;
- the supported release regions;
- one case for every declared profile and region pair;
- one private benchmark-plan path for each case; and
- total, per-matrix, time, closure, reload, acceleration, GPU, and storage
  limits.

Each case is a full matrix. It must include fresh-Pod cold and hot runs plus
cancellation, provider, storage, corruption, and coordinator-restart canaries.
Every scenario must constrain preflight to the case region. The benchmark
scorecard records a safe submission receipt with the selected profile, image
digest, and region. The release controller rejects a different selection.

Cases run in a stable round-robin order. The trailing 30-pass window must cover
every declared case, profile, and region. Thus a release cannot collect all 30
passes from only the easiest region.

Before each paid case, fixed internal contract tests must pass for:

- reload, cursor reconnect, and event order;
- deterministic preflight blockers;
- stale and corrupt cache recovery;
- regional cold fallback;
- support-bundle redaction; and
- GPU and storage budget enforcement.

The paid scorecard must also prove exact provider cleanup, cancellation through
provider absence within its SLO, hot preparation at no more than 25% of cold,
corrupt-object quarantine, cold fallback, safe support bundles, current-state
reload within two seconds, resumable ordered events, and storage spend inside
both configured and release budgets.

Validate a release plan without reading credentials or starting a Pod:

```bash
cloud-offload release validate --plan .runlogs/m7-release-plan.json
```

Run a bounded number of matrices and keep all detailed material under
`.runlogs/`:

```bash
cloud-offload release run \
  --plan .runlogs/m7-release-plan.json \
  --ledger .runlogs/m7-release-ledger.json \
  --output-dir .runlogs/m7-matrices \
  --max-matrices 1 \
  --confirm-spend \
  --allow-hooks
```

Resume with the same plan and ledger. A changed repository revision, worker
image, benchmark plan, region set, limit, or test set changes the release-plan
digest. The controller refuses to mix that work with the earlier ledger.

```bash
cloud-offload release status \
  --plan .runlogs/m7-release-plan.json \
  --ledger .runlogs/m7-release-ledger.json
```

Raw plans, workflows, full scorecards, hooks, and test output stay local. The
ledger contains only digests, finite measurements, opaque release identity,
cleanup and budget receipts, and explicit pass or failure codes.

## Scorecard

`cloud-offload.benchmark-scorecard.v1` contains:

- safe plan summary and request digests;
- cold, hot, and failure scenario results;
- lifecycle/event cursor and redacted support bundle;
- phase durations from observed JobEventV2 timestamps;
- safe provider startup facts with explicit unknown phases;
- provider, Pod ID, rate, and attribution source;
- conservative compute-cost estimate;
- failure-trigger receipt;
- safe scenario preparation and full-config restoration receipts;
- exact termination attempts and provider-absence receipt;
- cold/hot duration and cost distributions;
- phase distributions; and
- final orphan inventory.

The benchmark does not claim provider-final billed cost. That becomes possible
only after Milestone 3 defines and persists the provider's authoritative billing
closure receipt.
