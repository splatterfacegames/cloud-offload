# M7 production-release campaign runbook

This is the operator guide for running the Milestone 7 release gate
(`RELEASE-1`): thirty consecutive full canary matrices across every declared
worker profile, pinned image, and region, with zero orphaned Pods and every
release SLO intact. The controller lives in `cloud_offload/release_gate.py`
and is driven entirely through the `cloud-offload release` CLI. The gate
criteria themselves are defined in
[the product goal](cloud-offload-product-goal.md#milestone-7--production-release-gate),
and the underlying benchmark harness in
[Production benchmark and scorecard](production-benchmark.md).

## Prerequisites

The current campaign targets only RunPod `AP-JP-1` and H100 SXM 80 GB
(`NVIDIA H100 80GB HBM3`). Declare one `comfyui` profile-region case and pin
every paid scenario to Japan and this GPU. Keep the thirty consecutive full
matrices and all reliability SLOs. Cold fallback must stay in Japan; unavailable
Japan capacity must not trigger an overseas rental. Cross-region failure
contracts run locally and do not expand the paid release scope. The historical
three-region plans and ledgers are previous campaign evidence, not the current
release scope. Generate a new plan and ledger with the final image and source
pins before starting this campaign.

Japan's mounted cache can run the corruption canary without an S3 endpoint.
Set `CLOUD_OFFLOAD_BENCHMARK_MOUNT_CORRUPTION=1` in the campaign coordinator
and hook environment. The worker verifies a signed synthetic-artifact claim,
injects only that fresh artifact, and exercises normal verification, quarantine,
and origin recovery. The hook requires worker cleanup evidence and restores the
original inventory. Use a separate campaign-owned volume; never inject faults
into an existing user volume. Keep this flag unset for ordinary operation.

The worker image omits Conda installation caches and ComfyUI's optional workflow
example media and embedded documentation. These are browser resources, not graph
execution dependencies. Keep the frontend package and model runtime libraries;
verify native GPU inference and boundary codecs after changing image contents.
An image already cached by the provider does not prove uncached pull readiness.

- **A healthy local coordinator.** Start it with `cloud-offload serve`. The
  release runner discovers it through the service-discovery file or the
  `CLOUD_OFFLOAD_URL` environment variable and refuses to start when the
  service is not healthy.
- **Provider credentials.** The RunPod connector reads `RUNPOD_API_KEY` (or the
  OS-keychain entry / `CLOUD_OFFLOAD_RUNPOD_API_KEY`). If a case's benchmark
  plan lists other providers (for example `vast.ai` via `VAST_API_KEY`),
  those credentials must be present too. `release validate` and
  `release status` never read credentials; only `release run` does.
- **Configured worker profiles.** Every profile named in the release plan must
  exist in the coordinator configuration with an image pinned by the *same*
  `@sha256:` digest the plan declares. A mismatch fails the matrix precheck
  before any Pod is created.
- **Clean checked-out repositories.** The plan pins exact backend and
  extension Git revisions. Before each matrix the controller runs
  `git rev-parse HEAD` and `git status --porcelain` in both working trees; a
  wrong revision or a dirty tracked file fails the precheck (a recorded,
  count-resetting failure) without spending money.
- **Contract tests must pass.** Before each paid matrix the controller runs
  the fixed internal contract-test node list (reload/reconnect/event order,
  deterministic preflight, cache recovery, regional fallback, support-bundle
  redaction, GPU and storage budgets) with `python -m pytest` in the backend
  repository. Any failure is a precheck failure.

## Release-plan schema (`cloud-offload.release-plan.v1`)

A release plan is a single JSON object:

| Field | Meaning |
| --- | --- |
| `schema` | Must be `cloud-offload.release-plan.v1`. |
| `required_consecutive_matrices` | At least 30. The trailing window length. |
| `repositories` | At least `backend` and `extension`, each with `name`, `path` (absolute or plan-relative), and a full 40–64 hex `revision`. |
| `profiles` | The public worker profiles under release: `name` plus `image_digest` (`sha256:<64 hex>`). |
| `regions` | The declared release regions. |
| `cases` | Exactly one case per profile-region pair: `name`, `profile`, `region`, `benchmark_plan` (path to a private benchmark plan). The case count cannot exceed `required_consecutive_matrices`, otherwise the window could never cover every case. |
| `limits` | Release ceilings, below. |

Limits:

| Limit | Meaning |
| --- | --- |
| `max_total_cost_usd` | Whole-campaign estimated upper compute cost ceiling. The runner stops (and the gate cannot pass) beyond it. |
| `max_matrix_cost_usd` | Per-matrix ceiling. Every case's benchmark `max_total_cost_usd` must fit under it. |
| `max_total_seconds` / `max_matrix_seconds` | Campaign and per-matrix wall-clock ceilings. A case's `max_campaign_seconds` plus the contract-test timeout must fit inside `max_matrix_seconds`. |
| `contract_test_timeout_seconds` | Timeout for the pre-matrix contract-test run (default 300). |
| `cancellation_slo_seconds` | Maximum cancellation-to-provider-absence time. |
| `provider_closure_slo_seconds` | Maximum resource-closure time for any scenario. |
| `reload_slo_seconds` | Maximum snapshot reload time in the replay probe (capped at 2). |
| `hot_preparation_ratio_max` | Maximum median hot/cold preparation ratio (capped at 0.25). |
| `max_monthly_storage_cost_usd` | Release ceiling for estimated monthly RunPod storage spend. |

Each case's benchmark plan must itself be a *full* canary matrix: fresh-Pod
cold and hot runs plus cancellation, provider, storage, corruption, and
restart failure scenarios, with every scenario bound to exactly the case's
region via `allowed_regions`.

For the first startup-only paid validation, set the benchmark plan's
`runner_readiness_timeout_seconds` and `max_runner_readiness_cost_usd` to a
smaller explicit budget, for example 180 seconds and USD 0.05. These are
scenario controls. They do not reduce the normal dispatcher's worker startup
policy. The readiness clock includes preparation and submission time. Provider
cleanup and config restore are measured separately and remain mandatory.

### Worked example

```json
{
  "schema": "cloud-offload.release-plan.v1",
  "required_consecutive_matrices": 30,
  "repositories": [
    {"name": "backend", "path": "/home/op/cloud-offload", "revision": "6ced336…40 hex…"},
    {"name": "extension", "path": "/home/op/ComfyUI-Cloud-Offload", "revision": "7b2a60f…40 hex…"}
  ],
  "profiles": [
    {"name": "comfyui", "image_digest": "sha256:1039f1e218587b4a08eb6dabd8d4e47e722c0b808d6457fd8922072dfe9c24b1"}
  ],
  "regions": ["US-MD-1", "EU-RO-1"],
  "cases": [
    {"name": "comfyui-us-md-1", "profile": "comfyui", "region": "US-MD-1",
     "benchmark_plan": "m7-benchmark-us-md-1.json"},
    {"name": "comfyui-eu-ro-1", "profile": "comfyui", "region": "EU-RO-1",
     "benchmark_plan": "m7-benchmark-eu-ro-1.json"}
  ],
  "limits": {
    "max_total_cost_usd": 55,
    "max_matrix_cost_usd": 1.8,
    "max_total_seconds": 172800,
    "max_matrix_seconds": 3600,
    "contract_test_timeout_seconds": 300,
    "cancellation_slo_seconds": 90,
    "provider_closure_slo_seconds": 90,
    "reload_slo_seconds": 2,
    "hot_preparation_ratio_max": 0.25,
    "max_monthly_storage_cost_usd": 10
  }
}
```

## Commands

Before starting an isolated coordinator, import the boundary bundles referenced
by the private benchmark plans into that coordinator's configured local artifact
root. The source root is read-only; the command verifies each source digest and
size, publishes through the same content-addressed `partition-artifacts/` layout
used by `/api/artifacts`, and accepts only exact duplicate objects on repeat
runs. Missing sources, digest/size mismatches, interrupted copies, and conflicting
destination bytes stop the command without publishing a partial object. It emits
only artifact digests, sizes, roles, and duplicate status—never source paths or
credentials.

```bash
cloud-offload release bootstrap-artifacts \
  --plan .runlogs/m7-release-plan.json \
  --source-root /read-only/prior-cloud-offload/job_files \
  --home /isolated/m7 \
  --config /isolated/m7/config.json
```

The explicit `--home` makes a blank `storage_path` resolve to
`/isolated/m7/job_files`; it is never resolved from the process-global home.
The command writes a durable receipt bound to the release-plan digest, effective
redacted config, destination, and every declared artifact. A receipt mismatch,
missing receipt, or missing/mutated artifact refuses startup.

Start the coordinator only through the receipt-enforcing isolated path:

```bash
cloud-offload serve \
  --config /isolated/m7/config.json \
  --home /isolated/m7 \
  --release-plan .runlogs/m7-release-plan.json
```

Then run the release wrapper with the same enforced identity:

```bash
cloud-offload release run \
  --plan .runlogs/m7-release-plan.json \
  --ledger .runlogs/m7-ledger.json \
  --output-dir .runlogs/m7 \
  --config /isolated/m7/config.json \
  --home /isolated/m7 \
  --confirm-spend --allow-hooks
```

Do not repoint an isolated campaign at a prior mutable Cloud Offload home.

Validate a plan (free, no credentials, no provider reads; prints only the
redacted safe summary):

```bash
cloud-offload release validate --plan .runlogs/m7-release-plan.json
```

Run matrices (paid). `--confirm-spend` is mandatory; `--allow-hooks` is
required because the storage/corruption/restart canaries use reviewed external
failure hooks — without it the controller refuses to run a full matrix:

```bash
cloud-offload release run \
  --plan .runlogs/m7-release-plan.json \
  --ledger .runlogs/m7-release-ledger.json \
  --output-dir .runlogs/m7-matrices \
  --max-matrices 1 \
  --confirm-spend \
  --allow-hooks
```

Check progress at any time (free):

```bash
cloud-offload release status \
  --plan .runlogs/m7-release-plan.json \
  --ledger .runlogs/m7-release-ledger.json
```

`release run` exits zero when it stops for a good reason
(`release_passed`, `release_already_passed`, `requested_matrix_limit`) and
non-zero for `matrix_failed`, `total_cost_limit`, `total_runtime_limit`, a
scenario limit, or `operator_interrupt:KeyboardInterrupt` / `SystemExit`.

## The ledger and `.runlogs/`

- The **ledger** is the durable, redacted record. Every matrix appends one
  bounded receipt (case, axes, pass/fail, failure codes, digests, SLO
  measurements, budget receipts) and the file is rewritten atomically
  (write-temp + fsync + rename), so a crash can never leave a torn ledger.
  It contains no raw plans, prompts, workflows, paths, tokens, or provider
  payloads — only content digests and finite measurements.
- The ledger is **bound to the release-plan digest**. Changing a repository
  revision, image digest, benchmark plan, region set, limit, or the contract
  test set changes the digest, and the controller refuses to reuse the old
  ledger. A new plan means a new ledger and a fresh count.
- **`.runlogs/`** holds everything private: the release plan, each matrix's
  directory (`matrix-0001-<case>/`) with the full benchmark scorecard and the
  contract-test log. It is git- and docker-ignored and must never be
  committed. Only the ledger is eligible as durable release evidence.

## Failure and resume semantics

- Cases rotate in a stable round-robin order keyed by the absolute matrix
  index, so the rotation continues correctly across stops and resumes.
- **One failed matrix resets the consecutive count to zero** and stops the
  run immediately so no money is spent on evidence that cannot extend the
  window. Precheck failures (wrong revision, dirty tree, failed contract
  tests, image-digest mismatch) are recorded as failed matrices too, but cost
  $0 because they stop before any provider call.
- The first failed or interrupted scenario stops before the next scenario can
  submit a job. An operator interrupt still cancels the current job, verifies
  provider absence, atomically writes the aborted scorecard, and atomically
  appends its redacted receipt to the ledger. The ledger's `last_stop_reason`
  names the exact safe interrupt or limit code.
- Resource ownership requires one journal identity with the current job ID,
  lease ID, provider, and exact provider instance ID. Startup facts and cleanup
  use only that identity. A concurrent managed Pod is not inferred to belong to
  the campaign and is never terminated by an inventory difference.
- If a Pod event omits its lease, the harness queries the current job record and
  accepts the Pod only when the job, lease, provider, and instance all match. If
  that proof is not available, the Pod is an unknown paid resource: no delete is
  attempted, the release is blocked, and the campaign cost ceiling is charged.
- Unknown paid Pods also enter the live readiness cost meter. Provider inventory
  supplies the rate when available; otherwise the meter uses a nonzero
  ceiling-derived rate. They can stop a startup validation on cost before its
  time limit, and are never accounted at zero.
- Worker readiness needs the exact attributed Pod. A worker with the current
  lease but a different Pod ID cannot prove a callback or ComfyUI readiness.
- Cleanup retries are limited by both elapsed time and attempt count. An
  interrupt at inventory, termination, wait, or final verification causes an
  aborted receipt. The exact attributed Pod is either proved absent or recorded
  as an orphan. An orphan or an unavailable final audit charges the conservative
  total campaign cost ceiling.
- If an interrupt escapes before the benchmark can retain resource attribution,
  the release fallback performs a read-only provider audit. It never deletes an
  unattributed resource. It marks cleanup failed when a managed resource remains
  and charges the matrix time and cost ceilings as conservative evidence. If a
  possible orphan remains, it charges the release total time and cost ceilings
  for possible ongoing spend. Older partial scorecards can never reduce these
  bounds.
- A nonzero provider inventory rate overrides a zero journal rate. If a running
  attributed Pod has no known rate, the harness assigns a nonzero
  ceiling-derived rate. It never accounts a known running campaign Pod at zero.
- The production submission driver receives an absolute startup deadline and a
  cost budget. It checks both after preflight and immediately before the
  provider-starting POST. A slow preflight therefore cannot start a Pod after a
  startup-only validation limit has expired.
- Preflight retry waits and benchmark poll waits use only the positive time that
  remains before the absolute deadline. A 15-second retry interval is shortened
  when less than 15 seconds remain. At zero remaining time, the harness makes no
  request and does not sleep. The remaining time also bounds the preflight HTTP
  request timeout.
- Submission also requires exactly one selected candidate with a finite,
  positive hourly rate. Its worst-case scenario cost must fit the remaining
  readiness, scenario, and campaign budgets. Missing, malformed, zero, or
  unaffordable quotes stop before the provider-starting POST.
- The free preflight wire carries exactly one workload: `partition` for
  `/api/partitions`, or `capsule` for `/api/workflows`. It never sends an empty
  field for the other workload type.
- Published scorecards use explicit field and finite-value projections. Support
  bundles keep only their schema, job ID, and approved event facts. Provider
  detail text, commands, URLs, paths, environment data, and exception messages
  are not publication fields.
- Public strings use field-specific rules: finite provider values, strict
  bounded identifiers, SHA-256 digest syntax, parsed timestamps, and bounded
  stop codes. URL syntax, paths, whitespace-bearing detail text, email-style
  values, and AWS access-key forms are rejected even when placed under an
  otherwise public field name.
- Request digests preserve the harness's canonical lowercase bare 64-hex form.
  The public projection also accepts `sha256:<64-hex>` as a normalized input;
  malformed or non-hex request digests are dropped.
- Image, profile, test-set, plan, scorecard, and other release digests require
  the contracted `sha256:<64-hex>` form; only `request_digest` also accepts the
  canonical bare form.
- An exception in replay, storage checks, or another post-submit release step
  uses the same durable scorecard audit as an operator stop. Exact attributed
  Pods receive bounded cleanup. Unknown ownership or ongoing spend charges the
  conservative release ceiling; a normal exception cannot publish a zero-cost,
  zero-duration, no-mutation receipt without proof.
- Startup evidence separates provider allocation, image pull, container start,
  runner callback, and ComfyUI readiness. `unknown` means the provider or
  coordinator did not prove that phase. In particular, RunPod REST v2 does not
  expose an authoritative image-pull phase.
- To **resume**, rerun the same `release run` command with the same plan and
  ledger. Passed history is kept; the gate needs 30 *trailing* passes, and
  that trailing window alone must cover every case, profile, and region.
- The gate also requires the whole ledger's summed cost and duration to stay
  inside `max_total_cost_usd` / `max_total_seconds` — failures that burned
  budget count against the campaign.

## Expected wall-clock time and spend (estimate)

Derived from the committed M0–M6 evidence (single-region RTX-class matrices;
all figures are **estimates**, not quotes):

Per-matrix compute, from the accepted M0 full-canary campaign
(`docs/evidence/m0-production-evidence-2026-07-29.json`) and the M3 lease
campaign:

- cold + hot pair: **$0.342** (cold $0.179 + hot $0.163)
- cancellation ≈ **$0.016** and provider ≈ **$0.018** (M3 measured
  $0.0158/$0.0180 per short-lived Pod)
- storage canary ≈ **$0.05** (short fresh-Pod run, same shape as corruption)
- corruption: **$0.056**, restart: **$0.012**

Best-case per matrix ≈ 0.342 + 0.016 + 0.018 + 0.05 + 0.056 + 0.012 ≈
**$0.49**. The M0 failure campaign actually billed $0.920 including retries,
so a conservative per-matrix figure is 0.342 + 0.920 + 0.056 + 0.012 ≈
**$1.33**.

Thirty matrices:

- optimistic: 30 × $0.49 ≈ **$15**
- conservative (every matrix pays M0-style retry overhead): 30 × $1.33 ≈
  **$40**

Wall-clock per matrix, from M0 phase timings: cold ≈ 433 s, hot ≈ 395 s,
five failure canaries ≈ 40–120 s each (≈ 400 s total), plus contract tests
(≤ 300 s) and probes ≈ **1 500–1 800 s ≈ 25–30 min**. Thirty sequential
matrices ≈ 30 × 27 min ≈ **13.5 hours** of continuous running, before any
failed-matrix rework. Budget roughly a weekend of supervised running and
$15–$40 of RunPod compute, plus the standing network-volume storage cost
(50 GB ≈ $3.50/month) that the storage-budget receipt checks.
