# Cloud Offload product goal and delivery plan

> Status: **canonical product goal**
> Last updated: **2026-07-30**
> Program status: **in progress — Milestone 6 is complete and Milestone 7 production release is the current gate**
> Scope: the end-to-end Cloud Offload product across the coordinator, dispatcher,
> workers, provider connectors, prepared storage, and ComfyUI extension.
>
> The detailed storage subsystem design remains in
> [Storage-aware Cloud Offload](storage-aware-cloud-offload.md). That PRD is a
> supporting design; this document defines the product outcome it serves.

This is the single source of truth for the goal, product promises, requirements,
decisions, delivery sequence, current implementation state, and release evidence.
Supporting design documents may add implementation detail but may not weaken an
acceptance criterion here. A milestone is complete only when its exit criteria
have evidence; merged code alone is not completion.

## Goal control

This document is also the program control surface. It answers four questions
without requiring reconstruction from chat history, local logs, or merged pull
requests:

1. **What outcome are we pursuing?** The product contract and promise stack
   below.
2. **What has already been decided?** The decisions, non-goals, requirements,
   and milestone exits in this document.
3. **What has actually been proved?** The delivery ledger and validation record,
   which distinguish merged implementation from accepted production evidence.
4. **What happens next?** The current execution state and the first unmet exit
   criterion in milestone order.

The active program gate is **M7 production release**. M0 through M6 are
complete. M7 remains part of this same goal; it is not a backlog that can be
silently deferred or a new goal that must be rediscovered later. M7 may close
only after thirty consecutive full canary matrices pass across the supported
images and regions with zero orphaned Pods and all release SLOs intact.

Raw benchmark plans, workflows, hooks, support bundles, and service logs remain
local under `.runlogs/`. The durable record committed to the repository contains
only safe request digests, aggregate timings and cost, opaque resource/job IDs,
test results, cleanup receipts, and explicit pass/fail conclusions. Credential
values, raw prompts/workflows, private paths, signed URLs, hook commands, and
secret endpoints are never goal-document evidence.

## Product goal

Cloud Offload rents and operates the right cloud GPU for a ComfyUI workflow. It
checks everything knowable before the paid resource starts, explains what is
happening while it runs, proves that the resource stopped when the run ends, and
makes repeated runs materially faster.

GPU rental and execution are the center of the product. Readiness, visibility,
closure, and acceleration are additional promises that make that central promise
predictable and trustworthy; they do not replace it.

The complete product contract is:

> **Cloud Offload rents the right GPU, proves the workflow is ready before the
> meter starts, shows exactly what is happening while it runs, proves billing
> stopped when it ends or is cancelled, and accelerates the next compatible
> run.**

## Definition of program completion

The goal is complete only when all eight milestones in this document have met
their exits and the Milestone 7 production release gate has passed. In
particular, completion requires all of the following at the same time:

- a ComfyUI user can submit a supported workflow without managing provider
  infrastructure;
- deterministic readiness failures stop before paid compute is created;
- Cloud Offload recommends a compatible GPU and discloses expected total cost;
- the default ten-second confirmation behaves as specified and can be controlled
  by both “Don't show again” and persistent settings;
- the lifecycle survives reload and explains progress, uncertainty, spend, and
  resource identity from an authoritative journal;
- cancellation and normal completion end with provider-confirmed billing closure;
- compatible repeat runs are measurably faster without weakening integrity;
- storage placement, replication, retention, and spend stay within user policy;
- cold, hot, cancellation, provider, storage, corruption, restart, and regional
  fallback canaries pass continuously; and
- the coordinator and ComfyUI extension changes are released together wherever
  the user contract crosses both repositories.

Anything less is an intermediate delivery, not the completed goal.

## Promise stack

| Promise | User expectation | Product definition of done |
| --- | --- | --- |
| **Execution** | “Rent and operate the right GPU for me.” | A compatible worker executes the workflow without the user managing provider infrastructure. |
| **Readiness** | “Check everything knowable before charging me.” | Deterministic blockers stop before Pod creation; preparation, cost, and uncertainty are disclosed. |
| **Visibility** | “Always tell me what is happening.” | Status survives reload and shows stage, elapsed time, measurable progress, ETA confidence, resource identity, and spend. |
| **Closure** | “Prove the paid resource stopped.” | Completion and cancellation end with provider-confirmed termination evidence, not only a local status change. |
| **Acceleration** | “Make compatible repeat runs faster.” | Verified prepared state, reproducible bundles, and storage-aware placement reduce measured time to result. |

## Current baseline

Cloud Offload already has the load-bearing execution path:

- partition compilation and authenticated coordinator submission;
- provider routing and immutable worker-image selection;
- RunPod provisioning and disposable worker lifecycle;
- authenticated Hugging Face model downloads;
- incremental worker and ComfyUI execution events;
- opt-in RunPod network-volume prepared storage;
- signed prepared-state manifests and content-addressed artifacts;
- storage-aware regional placement and explicit cold fallback;
- cache population, restore, verification, and quarantine primitives;
- visible startup, download, cache, verification, and execution feedback; and
- terminal cancellation semantics that prevent late callbacks from resurrecting
  a cancelled job.

A reference inpainting run has validated the connected path with five prepared
cache hits and one authenticated 4.23 GB model download. Durable copy completed
in approximately 7.5 seconds and read-back verification and publication in
approximately 15.3 seconds. This proves the direction, but one successful run is
not a production-readiness claim.

The remaining beta gaps are:

- regional demand and measured replication benefit are not yet recorded;
- multi-region prepared-state placement and shadow replication are incomplete;
- replication budget, TTL, and automatic deletion controls are incomplete;
- adaptive placement does not yet use safe multi-region replica state; and
- validation is not yet a continuous cold, hot, and failure-injection matrix.

## Why these requirements exist

The roadmap is grounded in observed end-to-end failures rather than hypothetical
polish work.

| Observation | What it revealed | Product consequence |
| --- | --- | --- |
| ComfyUI reported `Cloud Offload partition references missing node 70` for the `inpainting` workflow. | Visible boxed-subgraph node IDs differ from the executable IDs emitted by `graphToPrompt`; model declarations can also live inside subgraph definitions. | Compilation must expand nested boxed subgraphs and preflight must resolve the exact executable closure. This was fixed and proven by extension PR #2. |
| Prompt `4009468b-129a-4018-b5dd-d9e7fe1a2c13` existed in ComfyUI but returned 404 from the coordinator job APIs. | A local prompt ID can be mistaken for a cloud job ID, making support and cancellation ambiguous. | Every surface and support bundle must expose correlated prompt, job, partition, lease, and provider-resource identities. |
| ComfyUI was started without the dispatcher during an end-to-end debugging session. | A healthy UI and coordinator do not prove the rental path is operating. | Readiness and operator status must cover the dispatcher, ingress, provider credentials, worker profile, and ability to launch—not just HTTP health. |
| Healthy provisioning and model preparation appeared frozen at a static early percentage. | Canvas-only coarse progress makes long paid startup look hung. | Durable phase events, byte telemetry, elapsed time, ETA confidence, and a persistent Cloud Jobs surface are required. Extension PR #4 added an initial canvas improvement; it is not the final visibility milestone. |
| A large model download was slow and authentication was uncertain. | Hugging Face authentication affects throttling as well as gated-model access. | Preflight must verify credential presence, workers must receive authenticated download capability, and transfer telemetry must expose bytes and throughput without exposing tokens. |
| Prepared-state restore controls were hard to discover. | A capability is not complete when its control and current state are obscure. | Prepared storage needs an explicit settings entry, action-bar access, policy explanation, cache status, and visible cold fallback. |
| Local interruption did not originally guarantee cloud cancellation. | Stopping a prompt and stopping provider billing are different operations. | ComfyUI interruption must revoke the cloud job, and the coordinator must independently terminate and reconcile the exact paid resource. |
| The first prepared-storage run still performed a 4.23 GB authenticated download and complete verification. | Durable storage helps only after population, and a nominal cache hit can remain I/O-bound. | Cold and hot paths must be measured separately; trusted hot restore, capsules, and placement must prove time and cost saved. |

## System boundary and repositories

The product is one system delivered through multiple components:

| Component | Repository or artifact | Responsibility |
| --- | --- | --- |
| Coordinator, dispatcher, providers, worker, storage controller, benchmark | `jethac/cloud-offload` | Authoritative job state, preflight, recommendation, scheduling, paid lifecycle, prepared state, and evidence. |
| ComfyUI extension | `jethac/ComfyUI-Cloud-Offload` | Partition compilation, settings, confirmation, persistent job UX, cancellation intent, and local/cloud identity correlation. |
| Worker runtime | `ghcr.io/jethac/cloud-offload-worker-comfyui` | Reproducible ComfyUI environment, authenticated staging, prepared-state restore/population, execution, and cooperative abort. |
| RunPod | Provider API, Pods, network volumes, and S3-compatible object API | Paid compute truth, region-constrained volume attachment, durable prepared bytes, and termination evidence. |
| Hugging Face | Authenticated artifact source | Immutable model resolution and authenticated transfers for gated and rate-sensitive downloads. |

An end-to-end acceptance test uses the extension to submit through the real
coordinator and dispatcher to a real provider worker. Starting ComfyUI alone,
calling the coordinator directly, or passing backend tests does not by itself
validate the product journey.

## Requirement inventory and traceability

| ID | Requirement | Primary milestone | Current state |
| --- | --- | --- | --- |
| `EXEC-1` | Rent and operate a compatible GPU without user-managed infrastructure. | M1 | Complete for the M1 contract; the accepted merged-stack journey used the confirmed offer and prepared volume and returned the result. |
| `READY-1` | Prove deterministic requirements before provider mutation. | M1 | Accepted merged-stack production evidence proves free preflight, exact paid binding, and launch-time revalidation. |
| `RECOMMEND-1` | Recommend provider/GPU/region using expected total time and cost, including prepared-state locality. | M1 | Complete; two matched production runs now produce medium-confidence measured timing, prepared-local ranking, and complete RunPod compute, transfer, and container-storage cost. |
| `CONFIRM-1` | Show recommendation, cost, rationale, and a default ten-second auto-start confirmation. | M1 | Complete across backend PRs #25–#27 and extension PR #5; the accepted production replay proves the default confirmation. |
| `CONFIRM-2` | Provide Start now, Cancel, Choose another GPU, Don't show again, and equivalent persistent settings. | M1 | Complete; extension PR #5, backend PR #27, and the accepted merged-stack journey prove the interaction and persistent policy. |
| `JOURNAL-1` | Persist an idempotent, replayable, lifecycle-authoritative `JobEventV2` journal. | M0 | Complete; tests and accepted production canaries prove replay and lifecycle authority. |
| `VISIBLE-1` | Reconstruct a persistent job surface with phases, bytes, throughput, ETA confidence, spend, and identities. | M2 | Complete; the persistent Cloud Jobs surface reconstructs safe authoritative state after reload and exposes complete M2 telemetry. |
| `CLOSE-1` | Revoke work and prove provider termination before claiming billing stopped. | M3 | Complete; backend PR #34 and extension PR #8 implement the contract, and the accepted paid RunPod cancellation and restart matrix proves exact provider-confirmed closure. |
| `STORAGE-1` | Opt into or adopt RunPod storage before cached rental and attach it to compatible future Pods. | M4 foundation | Initial managed/adopted-volume MVP merged. |
| `STORAGE-2` | Track prepared contents and location, and prefer offers near compatible state with explicit cold fallback. | M4/M6 | Complete; paid multi-region demand, a copied compatible replica, prepared-local ranking, visible cold fallback, TTL expiry, and regional-loss recovery have accepted production evidence. |
| `ACCEL-1` | Make compatible repeat runs measurably faster with trusted restores and capsules. | M4/M5 | Complete; signed trusted restore, canonical workflow capsules, and fresh-Pod runtime-bundle restore have paid production proof. |
| `REPLICA-1` | Replicate prepared state only for measured benefit, within budget and TTL. | M6 | Complete; three mature recommendations reached 100% precision, automatic S3 copy stayed within both budgets, repeat cycles did not copy twice, expiry kept the source, and loss removed the replica from placement. |
| `EVIDENCE-1` | Produce redacted, comparable cold/hot/failure scorecards without orphaned resources. | M0 | Complete; all seven scenarios are accepted and the compact redacted projection is committed. |
| `RELEASE-1` | Pass the continuous production matrix and budget gates. | M7 | Pending. |

## Product principles

1. **Execution remains central.** Preflight and confirmation must not turn a
   one-click offload into infrastructure administration.
2. **The happy path is automatic.** Healthy preflight proceeds through a short,
   understandable rental confirmation rather than a multi-page wizard.
3. **Proof, estimate, and unknown are distinct.** The UI must never present a
   volatile provider observation as a guarantee.
4. **Fail free when possible.** A deterministic blocker discovered after Pod
   creation is a product defect.
5. **Provider truth closes billing.** Local cancellation alone cannot justify a
   “billing stopped” claim.
6. **The coordinator journal is authoritative.** Canvas titles and transient
   notifications render state; they do not own it.
7. **Progress describes remaining time.** A percentage must be monotonic,
   phase-aware, and calibrated by measurements rather than arbitrary counters.
8. **Prepared state must prove value.** Cache admission, retention, and
   replication respond to measured time and cost saved.
9. **Storage automation is budgeted and reversible.** Every automatic replica
   needs a reason, spend ceiling, and expiry.
10. **Every paid lifecycle is auditable.** A support bundle should explain what
    was checked, rented, transferred, executed, and terminated without requiring
    raw server logs.

## Canonical user journey

```text
Queue workflow
  → Checking readiness
  → Recommendation: RunPod A100 80 GB, US-MD-1, $1.49/hour
  → Estimated total: $0.12–$0.24; 88% prepared; 4.2 GB to download
  → Starting automatically in 10 seconds
  → Revalidating price, availability, region, and storage
  → Renting GPU
  → Pulling image and starting ComfyUI
  → Restoring prepared state and downloading misses
  → Running graph
  → Uploading result
  → Terminating Pod
  → Complete — provider confirmed termination; billing stopped
```

If deterministic readiness fails, the journey stops before `Renting GPU` and
offers an actionable correction. If provider capacity, price, or storage changes
materially during the confirmation window, Cloud Offload presents the revised
recommendation instead of silently substituting it.

## Readiness and preflight

### Purpose

Preflight converts a submitted partition into a canonical, hash-addressed
execution plan before any paid provider mutation. It proves deterministic
requirements, reports volatile observations, estimates preparation and cost,
and supplies the facts used by the GPU recommendation.

### Proposed API

`POST /api/preflight` returns a versioned report such as:

```json
{
  "schema": "cloud-offload.preflight.v1",
  "preflight_id": "...",
  "manifest_digest": "sha256:...",
  "status": "ready_with_preparation",
  "blockers": [],
  "warnings": [],
  "unknowns": [],
  "execution_plan": {
    "profile": "comfyui",
    "image_digest": "sha256:...",
    "gpu_requirement": {"minimum_vram_gb": 80},
    "provider": "runpod",
    "region": "US-MD-1",
    "prepared_volume_id": "..."
  },
  "preparation": {
    "required_bytes": 36600000000,
    "cached_bytes": 32300000000,
    "missing_bytes": 4300000000
  },
  "estimate": {
    "startup_seconds": [80, 150],
    "execution_seconds": [150, 300],
    "hourly_rate": 1.49,
    "total_job_cost": [0.12, 0.24],
    "confidence": "medium"
  },
  "expires_at": "..."
}
```

### Deterministic proof

Preflight must establish, without renting a Pod:

- the runtime profile supports the submitted partition capability;
- the worker image is pinned by digest and passes policy;
- required node types map to known, pinned custom-node sources;
- required model artifacts have known digests, sizes, and eligible sources;
- necessary provider, Hugging Face, registry, and S3 credentials are configured;
- residency, privacy, tenant, and cacheability policies permit offload;
- the disk plan fits configured limits;
- the selected prepared volume exists in the required datacenter;
- cached manifests are signed, compatible, and internally consistent; and
- hard user price, provider, GPU, and region constraints can be satisfied by the
  proposed plan.

### Volatile observations and estimates

These facts may change between preflight and launch and must be labeled as such:

- current GPU capacity and hourly price;
- provider launch reliability;
- image-pull duration;
- Hugging Face and S3 throughput;
- estimated dependency-preparation time;
- historical execution duration; and
- expected total job cost.

Immediately before Pod creation, the coordinator revalidates price, availability,
region, volume attachment, and expiration. A material change invalidates the
recommendation and creates a revised confirmation unless policy explicitly
permits it within configured tolerances.

### Status behavior

| Status | Default behavior |
| --- | --- |
| `ready` | Show the rental recommendation and begin the confirmation countdown. |
| `ready_with_preparation` | Do the same, clearly showing cold work; interrupt only when time or cost exceeds policy. |
| `blocked` | Do not rent; show actionable blockers. |
| `uncertain` | Require confirmation when the uncertainty can plausibly cause a paid failure. |

The partition submission binds its `preflight_id` and `manifest_digest`, allowing
the coordinator to reject a stale or materially different plan.

## GPU recommendation and rental confirmation

### Recommendation objective

After removing incompatible offers, Cloud Offload recommends the viable option
that best satisfies the user's selected policy:

| Policy | Objective |
| --- | --- |
| `balanced` | Best expected time to result for reasonable total cost. Recommended default. |
| `cheapest` | Lowest estimated total job cost, not merely lowest hourly price. |
| `fastest` | Lowest estimated time to completed result. |
| `manual` | Present compatible choices without selecting one automatically. |

The ranking considers:

- minimum VRAM and required GPU capabilities;
- measured execution speed for comparable workflows;
- hourly price and expected total runtime;
- image-pull and worker-start history;
- prepared-state region and byte coverage;
- missing transfer volume and measured source throughput;
- provider and regional reliability; and
- user provider, region, hourly-rate, and per-job cost limits.

The primary cost estimate is:

```text
expected total job cost =
    hourly rate × expected paid lifetime
  + incremental transfer cost
  + attributable incremental storage cost
```

The recommendation must include an explanation record so the user and support
can see why it won over cheaper or faster alternatives.

### Confirmation experience

The default confirmation shows:

- recommended GPU, provider, and region;
- hourly price;
- estimated total job cost as a range;
- estimated startup and execution time;
- prepared-cache coverage and missing bytes;
- recommendation rationale;
- confidence and meaningful uncertainty; and
- a visible countdown, approximately ten seconds by default.

Example:

```text
Recommended: RunPod A100 80 GB in US-MD-1
$1.49/hour · estimated total $0.12–$0.24
Approximately 1–2 minutes of preparation; 88% already cached
Selected for fastest expected result under your $1.75/hour limit

Starting in 10 seconds…
```

Actions are:

- **Start now**;
- **Cancel**;
- **Choose another GPU**; and
- **Don't show this confirmation again**.

Untouched confirmation automatically starts when the countdown ends. Inspecting
or changing the GPU, provider, region, or cost details pauses the countdown.
“Don't show again” sets the persistent rental-confirmation setting to `never` and
explains where to restore it.

### Settings

The settings surface includes:

- rental confirmation: `always`, `material_changes`, or `never`;
- confirmation countdown duration;
- recommendation policy: `balanced`, `cheapest`, `fastest`, or `manual`;
- maximum hourly price;
- maximum estimated total job cost;
- preferred or allowed providers;
- preferred or allowed regions; and
- material-change tolerances for price and estimated cost.

Skipping confirmation never disables hard spend, residency, provider, or GPU
constraints.

### Mandatory interruption

Even when normal confirmation is disabled, Cloud Offload must interrupt automatic
launch when:

- hourly price or estimated total cost exceeds a hard limit;
- the recommendation changes materially after the quote;
- the required GPU class changes materially;
- prepared storage becomes unavailable and preparation unexpectedly becomes cold;
- preflight reports uncertainty likely to cause a paid failure;
- the quote expires; or
- provider capacity disappears and the replacement is outside allowed tolerance.

## Visibility and job journal

### Authoritative event model

Every job becomes an append-only flight recorder. The coordinator persists an
event before broadcasting it and assigns a monotonically increasing job sequence.
Dispatchers and workers submit idempotent events with producer IDs and local
sequence numbers so retries cannot duplicate or reorder the visible lifecycle.

`JobEventV2` contains at least:

- job and partition identity;
- coordinator sequence;
- producer identity and producer sequence;
- event type, lifecycle phase, and phase ownership;
- occurred-at and observed-at timestamps;
- completed and total work units;
- completed and total bytes;
- throughput and ETA inputs;
- provider, region, Pod, volume, and lease identities;
- cache decision and artifact identity;
- price and spend observations;
- provider acknowledgements and other evidence; and
- schema version.

On reload or reconnect, the client requests events after its last acknowledged
sequence, reconstructs current state, then joins the live stream without a gap.
Terminal-state precedence is explicit: a delayed producer cannot reopen a closed
job.

### User interface

The primary surface is a persistent **Cloud Jobs** drawer or panel, independent
of the canvas. It displays:

- preflight and recommendation result;
- current lifecycle stage;
- elapsed time;
- bytes transferred and total bytes;
- smoothed throughput;
- ETA range and confidence;
- GPU, provider, region, and Pod identity;
- hourly rate and estimated spend;
- cache hits, misses, and preparation work;
- cancellation, provider termination, and billing state; and
- resumable event history.

The canvas group title remains a compact secondary indicator rather than the only
place where state exists.

### Progress model

Overall progress is a semantic critical path:

1. readiness;
2. provisioning;
3. worker boot;
4. dependency preparation;
5. execution;
6. result transfer; and
7. resource closure.

Within a phase, use measurable work such as bytes, files, nodes, or sampler steps.
Across phases, use historical timings from comparable completed jobs. Progress is
monotonic, terminal success is 100%, and ETA is a range with confidence rather
than a falsely precise countdown.

For authenticated Hugging Face transfers, resolve expected size where possible
and observe temporary-file growth or instrument the transport. Emit bytes and
rolling throughput periodically. When total size is unknowable, preserve honest
elapsed-only feedback rather than manufacturing a percentage.

## Closure and billing assurance

Logical cancellation is necessary but insufficient. Paid execution is governed
by a persisted, revocable `JobLease`:

```text
ACTIVE
  → REVOKED
  → TERMINATING
  → PROVIDER_TERMINATED
  → CLOSED
```

A lease binds:

- job and worker profile;
- exact provider and Pod ID;
- expiry and renewal token/state;
- runtime and dollar budget;
- termination attempts and deadlines;
- provider state; and
- provider acknowledgement used as the billing-stop receipt.

Cancellation:

1. atomically revokes the lease;
2. stops future renewals;
3. emits `cancellation_requested`;
4. asks the worker to abort at download-chunk, cache-phase, and graph-node
   boundaries;
5. independently requests provider termination;
6. retries termination idempotently;
7. reconciles the Pod against provider inventory; and
8. closes only after recording provider acknowledgement.

The UI states are explicit:

```text
Cancellation requested
→ Aborting workflow
→ Terminating RunPod Pod
→ Pod terminated — billing stopped
```

Coordinator startup reconciles every non-closed lease against provider inventory
and terminates orphans. The product must verify which RunPod state transition is
the authoritative billing boundary before promising more precision than the
provider supplies.

## Repeat-run acceleration

### Fast cache trust

First-seen or changed objects receive full digest verification. A subsequent hot
restore may use a signed trust receipt that binds:

- manifest signature;
- artifact digest and size;
- provider volume and object identity/generation;
- runtime compatibility;
- verification timestamp; and
- scrub policy and expiry.

Recently attested immutable objects use metadata verification on the hot path.
Background sampling and scheduled full audits preserve integrity pressure. Any
mismatch quarantines the artifact, degrades the volume, and falls back safely.
Policy may still require full verification for sensitive assets.

Probabilistic verification must never be described as equivalent to full
verification without an explicit threat model and confidence semantics.

### Prepared-state capsules

A canonical preflight manifest becomes the identity of a reproducible workflow
closure. Frequently used or expensive cold manifests may be promoted into signed
prepared-state capsules containing:

- worker image digest;
- model digests and sources;
- custom-node repositories and pinned commits;
- locked Python dependencies;
- CUDA, Python, platform, and runtime ABI;
- deterministic setup and verification hooks;
- privacy and cache policy; and
- compatible storage locations.

Cloud Offload begins with its own capsule and bundle format rather than attempting
to standardize the whole ComfyUI ecosystem.

Custom-node authors may optionally provide a readiness manifest declaring node
types, models, runtime downloads, system packages, Python dependencies, ABI
constraints, and setup hooks. Undeclared dynamic behavior remains a visible
preflight uncertainty rather than being silently declared ready.

### Regional replication

Replication starts in shadow recommendation mode. The controller records demand,
misses, transfer cost, storage cost, avoided GPU-idle time, regional capacity,
and replica age. A recommendation includes source, destination, bytes, cost,
expected hits, expected time saved, expiry, and budget impact.

Automatic replication is enabled only after shadow recommendations demonstrate
value. Copies use provider object APIs such as RunPod S3 and do not rent a GPU.
Every replica has a spend ceiling, single-flight protection, and expiry or
eviction plan. Scheduling prefers compatible replicas while preserving explicit
cold fallback.

## Delivery ledger

This ledger records merged implementation evidence across both repositories.
“Merged” means the code landed; it does not override the milestone exits below.

### Coordinator, dispatcher, worker, and provider repository

| PR | Delivered | Evidence status |
| --- | --- | --- |
| [#1](https://github.com/jethac/cloud-offload/pull/1) | Worker reliability, runner readiness, node-pack staging, VRAM/profile matching, and early worker status. | Merged as `4c3911a`. |
| [#2](https://github.com/jethac/cloud-offload/pull/2) | Storage-aware Cloud Offload PRD. | Merged as `60a2588`. |
| [#3](https://github.com/jethac/cloud-offload/pull/3) | RunPod volume lifecycle, prepared manifests/CAS, storage-aware placement, worker restore/population, and API foundations. | Merged as `efc8ceb`; live validation followed in #4. |
| [#4](https://github.com/jethac/cloud-offload/pull/4) | Durable publication and detailed prepared-storage/startup events. | Merged as `0059e31`; reference prepared run completed. |
| [#5](https://github.com/jethac/cloud-offload/pull/5) | Initial canonical product goal. | Merged as `5854bce`; this document now supersedes that initial snapshot. |
| [#6](https://github.com/jethac/cloud-offload/pull/6) | `JobEventV2`, producer idempotency, replay/snapshot/support-bundle APIs, redaction, anti-spoofing, and terminal precedence. | Merged as `92bbbf9`; 499 tests passed at merge. |
| [#7](https://github.com/jethac/cloud-offload/pull/7) | Atomic lifecycle journal, semantic phase protection, state seeding, duplicate collapse, and rollback proof. | Merged as `91864a9`; 504 tests passed at merge. |
| [#8](https://github.com/jethac/cloud-offload/pull/8) | Spend-capped production benchmark/scorecard, provider attribution and cleanup, cold/hot validation, and five-class failure injection. | Merged as `c1383fe`; 513 tests passed at merge. |
| [#9](https://github.com/jethac/cloud-offload/pull/9) | Explicit `force_execution` for fresh-Pod partition benchmarks without disabling prepared-state caching. | Merged as `781f212`; 513 tests passed at merge. |
| [#10](https://github.com/jethac/cloud-offload/pull/10) | Runtime `keyring` dependency required for Windows credential resolution. | Merged as `ce20543`; credential tests passed. |
| [#11](https://github.com/jethac/cloud-offload/pull/11) | Consolidated the full product goal, promise hierarchy, traceable requirements, M0–M7 exits, decisions, and release gate into this canonical document. | Merged as `3e42f61`; documentation is authoritative, but its milestones still require evidence. |
| [#12](https://github.com/jethac/cloud-offload/pull/12) | Made benchmark cache state authoritative: cold forces prepared storage off, hot requires an existing confirmed volume, full configuration is restored, and exact Pods are cleaned up. | Merged as `9675fdf`; 517 tests passed. |
| [#13](https://github.com/jethac/cloud-offload/pull/13) | Added reversible storage, corruption, and coordinator-restart production canaries plus health PID identity. | Merged as `429d405`; 520 tests passed. |
| [#14](https://github.com/jethac/cloud-offload/pull/14) | Added two-phase pre-submit failure hooks so reviewed corruption setup settles before job submission and Pod creation, with unconditional idempotent cleanup. | Merged as `4900aa6`; 522 tests passed. |
| [#15](https://github.com/jethac/cloud-offload/pull/15) | Consolidated the complete program record and replaced the unsafe Windows signal-zero PID probe with native process-state inspection. | Merged as `79faa25`; 525 tests and real live/absent PID probes passed. |
| [#16](https://github.com/jethac/cloud-offload/pull/16) | Made restart canaries prove journal replay and persist cancellation through the replacement coordinator instead of depending on unrelated image startup. | Merged as `7fdc37c`; 526 tests passed and the production restart canary passed. |
| [#17](https://github.com/jethac/cloud-offload/pull/17) | Made corruption canaries inject an isolated tiny artifact, publish a temporary signed manifest, provide a valid coordinator fallback, and remove all synthetic state during idempotent cleanup. | Merged as `0ea1754`; 527 tests passed. The first production run proved fresh-object isolation and cleanup but exposed stale manifest discovery, so corruption is not yet accepted. |
| [#18](https://github.com/jethac/cloud-offload/pull/18) | Recomputes the injected requirement profile, publishes signed manifests by immutable exact ID, falls back to that verified object when a mounted index is stale, and starts corruption observation at `cache_mount_ready`; also brings this goal record through the fresh-object campaign. | Merged as `f4d9cf9`; 529 tests passed. A bounded replay proved that the hook must fingerprint the configured launch profile name, not its requested capability name. |
| [#19](https://github.com/jethac/cloud-offload/pull/19) | Resolves the requested worker capability to the normalized configured launch profile before computing the corruption canary fingerprint and records the bounded failed exact-ID replay. | Merged as `3e43ff5`; 530 tests passed. The next replay proved exact selection and direct loading, then exposed first-write object caching. |
| [#20](https://github.com/jethac/cloud-offload/pull/20) | Writes corrupt bytes as the first and only value of the synthetic S3 key, keeps valid bytes only in coordinator fallback storage, and records the exact-selection replay. | Merged as `b1e6509`; 530 tests passed. Its bounded replay proved that a deterministic canary still reused the same digest and mounted object identity across campaigns. |
| [#21](https://github.com/jethac/cloud-offload/pull/21) | Gives every corruption campaign a safe random nonce and uses it to create a new payload, digest, object key, file name, and state path across all hook stages. It also records the deterministic-identity replay. | Merged as `a33fd16`; 531 tests passed. Its replay proved unique identity and exact placement, but the mounted view could not read the new manifest and automated cleanup missed one fallback-created registry projection. |
| [#22](https://github.com/jethac/cloud-offload/pull/22) | Persists the exact placement manifest on the assigned job, adds an active-job and volume-bound signed manifest fetch for stale mounted views, keeps exact placement authoritative in the worker, and removes fallback-created synthetic manifests during cleanup. It also records the unique-identity replay. | Merged as `3a60f7e`; 533 tests passed. Two bounded replays proved exact fetch, quarantine, fallback, and complete cleanup, then showed that the 105-second observation window is shorter than a worst-case prepared-asset verification pass. |
| [#23](https://github.com/jethac/cloud-offload/pull/23) | Gives only corruption observation a 240-second event window inside a 270-second hook process limit and records both PR #22 production replays. | Merged as `c0114c5`; 534 tests passed. The bounded replay then produced accepted corruption evidence. |
| [#24](https://github.com/jethac/cloud-offload/pull/24) | Commits the compact redacted seven-scenario production projection, checks its completeness and redaction contract, records the accepted corruption replay, and audits every M0 exit. | 535 tests passed; M0 is complete and M1 is active. |
| [#25](https://github.com/jethac/cloud-offload/pull/25) | Adds the read-only `cloud-offload.preflight.v1` report and endpoint, deterministic blockers, safe volatile offer reads, storage-local candidate ranking, cost/time ranges, quote expiry, and explicit unknowns. | 541 tests passed. Provider-mutation guard tests pass; submission binding is the next M1 slice. |
| [#26](https://github.com/jethac/cloud-offload/pull/26) | Persists the safe preflight report, binds each paid cache miss to one confirmed candidate, revalidates provider facts before queue creation and launch, prevents silent offer or storage substitution, constrains prepared workers to the exact volume, and makes benchmarks use preflight. | 552 tests passed. Failed revalidation creates no job or provider resource; confirmation policy and UI remain. |
| [#27](https://github.com/jethac/cloud-offload/pull/27) | Adds durable rental-confirmation, countdown, recommendation, hard cost, region, and material-change settings; enforces Start now or server-timed countdown completion; and makes material changes restart mandatory confirmation even under `never`. | 558 tests passed. Early or missing confirmation and disjoint region policy create no job or provider resource. |
| [#28](https://github.com/jethac/cloud-offload/pull/28) | Records the merged ComfyUI confirmation delivery and updates the active M1 handoff. | Merged as `5d71637`; extension PR #5 is part of the canonical delivery record. |
| [#29](https://github.com/jethac/cloud-offload/pull/29) | Makes free preflight accept the stable worker credential that the dispatcher stores beside the shared queue database. | Merged as `2ab4db4`; 76 focused tests and 559 full tests passed. The failed preflight created no job or provider resource. |
| [#30](https://github.com/jethac/cloud-offload/pull/30) | Starts worker idle time after each job, gives dispatcher cleanup a 60-second margin, and prevents a provider-restarted container from resetting the paid resource idle clock. It also records the first controlled M1 run. | Merged as `8193bfa`; 63 focused tests and 563 full tests passed. The bounded replay below proves automatic exact-Pod cleanup. |
| [#31](https://github.com/jethac/cloud-offload/pull/31) | Records the accepted merged-stack replay and its automatic exact-Pod cleanup receipt. | Merged as `de49afa`; the accepted replay below closes the paid journey and cleanup evidence. |
| [#32](https://github.com/jethac/cloud-offload/pull/32) | Adds private-data-free workload identity, read-only matched timing history, candidate-class measurement, full RunPod compute/transfer/container-storage estimates, the paid idle window, and the compact M1 evidence record. | 25 focused tests and 571 full tests passed; two repeated live free preflights were stable and created no job or Pod. M1 is complete. |
| [#33](https://github.com/jethac/cloud-offload/pull/33) | Adds a reloadable safe job projection with lifecycle phase, monotonic progress, transfer telemetry, ETA confidence, resource identity, estimated spend, cache results, cancellation state, billing state, and bounded event summaries. It also records the compact M2 evidence. | 579 full tests passed. A live same-origin read returned 20 jobs in 350.7 ms, and privacy tests reject raw requests, workflows, prompts, paths, URLs, and provider payloads. M2 is complete. |
| [#34](https://github.com/jethac/cloud-offload/pull/34) | Adds durable pre-mutation provider-resource leases, worker renewal and identity binding, exact cancellation revocation, restart and uncertain-launch reconciliation, independent idempotent termination, provider closure receipts, hard runtime and dollar limits, and cancelled-cache publication guards. | 593 tests passed. The automated M3 matrix covers five cancellation phases, delayed closure, a stopped but present resource, provider loss, restart recovery, both circuit breakers, late state, and cache safety. |
| [#35](https://github.com/jethac/cloud-offload/pull/35) | Records the accepted bounded paid RunPod worker-boot cancellation and coordinator-restart campaign, exact lease closure receipts, empty final provider inventory, and the compact redacted M3 evidence. | 594 tests passed. Both exact Pods became provider-absent, both leases received provider termination confirmation, the conservative cost upper bound was USD 0.033868, and no manual cleanup was required. M3 is complete. |
| [#36](https://github.com/jethac/cloud-offload/pull/36) | Adds coordinator-signed prepared-cache trust receipts, exact manifest/artifact/provider-volume/runtime/generation binding, a rotating sampled hot path, full-verification fallbacks, sensitive-asset policy, and safe verification telemetry. | 599 tests passed. A 6 MiB eligible hot artifact reads one 1 MiB signed sample instead of a complete digest; tampering, metadata change, expiry, audit due state, and private policy return to full verification. Background scrub enforcement and paid M4 performance proof remain. |
| [#37](https://github.com/jethac/cloud-offload/pull/37) | Adds a second signed cache sample during materialization, blocks return of a corrupt target, degrades affected cache volumes, and restores placement eligibility only after a new full digest verification. | 600 tests passed. The focused corruption test places damage only in the background-selected range and proves that the target is removed before return. The registry test proves that corruption removes the volume from placement and that full verification restores it. The paid M4 performance proof remains. |
| [#38](https://github.com/jethac/cloud-offload/pull/38) | Records the redacted paid M4 preparation evidence, exact trusted-read telemetry, complete first-seen verification, bounded campaign incidents, and provider closure. It marks M4 complete and makes M5 active. | 601 tests passed. Four cold samples have a 176.482397-second median. Three hot samples have a 15.179662-second median. Hot is 8.601% of cold. Two trusted restores each materialized 36,664,522,522 bytes with 12,582,912 verification bytes, six background scrubs, zero full-digest hits, and provider-confirmed absence. |
| [#39](https://github.com/jethac/cloud-offload/pull/39) | Adds canonical prepared workflow capsules, full-workflow preflight and confirmed submit, prepared inputs, result artifacts, readiness identities, and cooperative cancellation. | Merged as `13048da`; capsule digest and blocker tests pass. |
| [#40](https://github.com/jethac/cloud-offload/pull/40) | Adds reproducible signed custom-node and Python environment bundles, exact profile/runtime binding, first-rent publication, and later-Pod restore before ComfyUI starts. | Merged as `e7afc73`; paid production proof follows in this evidence change. |
| [#41](https://github.com/jethac/cloud-offload/pull/41) | Filters RunPod offers against current stock in the prepared-storage data center. | Merged as `6a41b66`; stale region-only offers no longer cause a known paid launch failure. |
| [#42](https://github.com/jethac/cloud-offload/pull/42) | Preserves the first-runner registration lease after provider binding. | Merged as `7adf4e5`; long image pulls can register against the same paid launch. |
| [#43](https://github.com/jethac/cloud-offload/pull/43) | Splits the runner image into parallel pull layers. | Merged as `c084014`; large independent layers can transfer in parallel. |
| [#44](https://github.com/jethac/cloud-offload/pull/44) | Adds explicit runner image profile aliases. | Merged as `28658f2`; capsule capability names can select a pinned image profile. |
| [#45](https://github.com/jethac/cloud-offload/pull/45) | Aligns the RunPod S3 cache namespace and identifies runtime bundle population events by artifact kind. | Merged as `a6d52b0`; 617 tests passed. |
| [#46](https://github.com/jethac/cloud-offload/pull/46) | Declares and validates the pinned runner platform and Python ABI. | Merged as `253d4cc`; 622 tests passed. |
| [#47](https://github.com/jethac/cloud-offload/pull/47) | Projects manifest-checked pre-ComfyUI boot restores into the claimed job event stream and restore receipt. | Merged as `d1e462e`; 623 tests passed. |
| [#48](https://github.com/jethac/cloud-offload/pull/48) | Moves the boot-report setting after stable image layers so the evidence fix does not invalidate the large apt, ComfyUI, PyTorch, and CUDA cache layers. | Merged as `b197c91`; 30 focused image and shell checks passed. |
| [#49](https://github.com/jethac/cloud-offload/pull/49) | Records the redacted paid M5 population and fresh-Pod restore proof, exact image identity, result transport, bounded incident, and provider closure. It marks M5 complete and makes M6 active. | 624 tests passed. Both runtime bundles restored before job claim with complete digest checks, no duplicate population, and zero final provider resources. |
| [#50](https://github.com/jethac/cloud-offload/pull/50) | Adds safe paid-region demand observations, bounded replication policy, and read-only shadow recommendations with source, destination, bytes, expected benefit, cost, budget, expiry, and decision reasons. | 632 tests passed. Existing compatible targets suppress recommendations, duplicate paid observations collapse, and the shadow endpoints make no provider mutation. |
| [#51](https://github.com/jethac/cloud-offload/pull/51) | Adds durable single-flight regional replica actions, exact current-recommendation binding, monthly budget and concurrency enforcement, shadow precision gating, provider-object copy, safe status, and TTL expiry. | 639 tests passed. Repeated requests do not copy twice, automatic mode stays locked before the accuracy gate, expiry keeps source state, and the controller never rents a GPU. |
| [#52](https://github.com/jethac/cloud-offload/pull/52) | Adds accuracy-gated automatic target creation, target budget reservation, exact provider ownership, scheduled controller cycles, stale-copy recovery, empty-target deletion, multi-region placement, and provider-confirmed regional-loss recovery. | 645 tests passed. Automatic targets stay within both storage budgets, failed creation keeps exact cleanup ownership, lost targets leave placement and release copy claims, cold fallback remains visible, and no replica operation rents a GPU. |
| [#53](https://github.com/jethac/cloud-offload/pull/53) | Makes a real later paid demand validate its matching shadow recommendation immediately, while the validation window remains the conservative maturity rule for recommendations with no later demand. | 646 tests passed. Repeated real demand can open the accuracy gate without an arbitrary delay, but one recent untested recommendation cannot. |
| [#54](https://github.com/jethac/cloud-offload/pull/54) | Sends the requested region allowlist to the provider as a hard cold-stock placement constraint. | 647 tests passed. Live free quotes returned current stock in EU-RO-1, EUR-IS-1, and US-GA-2 without provider mutation. |
| [#55](https://github.com/jethac/cloud-offload/pull/55) | Includes measured bytes from complete compatible runtime-bundle manifests in cold preparation and regional demand. | 648 tests passed. Cold regional demand now records the exact 3,840,000 runtime bytes instead of zero bytes. |
| [#56](https://github.com/jethac/cloud-offload/pull/56) | Streams verified RunPod S3 downloads through `GetObject` so a valid object does not depend on `HeadObject`, while retaining full digest and size checks. | 649 tests passed. The live 3,420,160-byte environment bundle returned HEAD 403 and GET success; the merged fix completed the regional copy. |
| [#57](https://github.com/jethac/cloud-offload/pull/57) | Records the redacted paid M6 demand, shadow-accuracy, automatic-copy, repeat-cycle, placement, loss, TTL-expiry, source-preservation, and final-cleanup proof. It marks M6 complete and makes M7 active. | 650 tests pass, including a strict finite and redacted M6 evidence test. Final RunPod state has zero active GPUs and no automatic test volume. |
| [#58](https://github.com/jethac/cloud-offload/pull/58) | Adds the M7 continuous release controller. | Merged as `d820d33`. |
| [#59](https://github.com/jethac/cloud-offload/pull/59) | Binds the M7 corruption canary to the declared release region. | Merged as `f80796f`. |
| [#60](https://github.com/jethac/cloud-offload/pull/60) | Hardens long prepared-state S3 transfers. | Merged as `27cf7d0`. |
| [#61](https://github.com/jethac/cloud-offload/pull/61) | Fixes complete verification for large S3 prepared-state objects. | Merged as `d5d93ce`. |
| [#62](https://github.com/jethac/cloud-offload/pull/62) | Hardens large RunPod multipart uploads. | Merged as `3dbbc4c`. |
| [#63](https://github.com/jethac/cloud-offload/pull/63) | Retries RunPod 524 responses during range reads. | Merged as `fdb1c99`. |
| [#64](https://github.com/jethac/cloud-offload/pull/64) | Resumes regional copies at missing objects. | Merged as `98d8c48`. |
| [#65](https://github.com/jethac/cloud-offload/pull/65) | Adds concurrent prepared-state S3 range downloads. | Merged as `75778de`. |
| [#66](https://github.com/jethac/cloud-offload/pull/66) | Bounds prepared-state range body waits. | Merged as `e4469e3`. |
| [#67](https://github.com/jethac/cloud-offload/pull/67) | Retries incomplete prepared-state range streams. | Merged as `de2c5a8`. |
| [#68](https://github.com/jethac/cloud-offload/pull/68) | Resumes partial prepared-state ranges. | Merged as `20df22b`. |
| [#69](https://github.com/jethac/cloud-offload/pull/69) | Retries RunPod S3 gateway 5xx responses. | Merged as `fb6fed6`. |
| [#70](https://github.com/jethac/cloud-offload/pull/70) | Resumes RunPod multipart uploads. | Merged as `809f9e0`. |
| [#71](https://github.com/jethac/cloud-offload/pull/71) | Redacts replication failure records. | Merged as `9ab58a8`. |
| [#72](https://github.com/jethac/cloud-offload/pull/72) | Refreshes prepared manifests for current profiles. | Merged as `b93a9b6`. |
| [#73](https://github.com/jethac/cloud-offload/pull/73) | Recovers completed RunPod multipart merges. | Merged as `17df05f`. |
| [#74](https://github.com/jethac/cloud-offload/pull/74) | Uses multipart uploads for medium RunPod objects. | Merged as `f2edcaa`. |
| [#75](https://github.com/jethac/cloud-offload/pull/75) | Bounds RunPod range request waits. | Merged as `f6a2ad1`. |
| [#76](https://github.com/jethac/cloud-offload/pull/76) | Bounds RunPod multipart part waits. | Merged as `8ae9cf0`. |
| [#77](https://github.com/jethac/cloud-offload/pull/77) | Adds safe per-job result routes. | Merged as `c62771a`. |
| [#78](https://github.com/jethac/cloud-offload/pull/78) | Prefers the valid multipart session with the most completed bytes. | Merged as `0305722`. |
| [#79](https://github.com/jethac/cloud-offload/pull/79) | Reduces RunPod range request pressure. | Merged as `e17990f`. |
| [#80](https://github.com/jethac/cloud-offload/pull/80) | Increases resumable S3 upload throughput. | Merged as `ea5d72a`. |
| [#81](https://github.com/jethac/cloud-offload/pull/81) | Restores stable RunPod upload pressure. | Merged as `e90f442`. |
| [#82](https://github.com/jethac/cloud-offload/pull/82) | Records safe S3 replication failure routing. | Merged as `d5b4cb6`. |
| [#83](https://github.com/jethac/cloud-offload/pull/83) | Starts a bounded new multipart session when RunPod multipart listing is unavailable. | Merged as `1f109ec`. |
| [#84](https://github.com/jethac/cloud-offload/pull/84) | Extends the bounded RunPod gateway retry budget. | Merged as `067fbff`. |
| [#85](https://github.com/jethac/cloud-offload/pull/85) | Matches the RunPod large-file multipart part size. | Merged as `00f87cd`. |
| [#86](https://github.com/jethac/cloud-offload/pull/86) | Resumes multipart uploads around partial provider part records. | Merged as `9f1a9af`. |
| [#87](https://github.com/jethac/cloud-offload/pull/87) | Migrates the RunPod connector to REST API v2. | Merged as `b2e33bd`. |
| [#89](https://github.com/jethac/cloud-offload/pull/89) | Places coordinator replication scratch data on a configured volume and validates its capacity contract. | Merged as `9b04ffd`; 701 tests passed. |
| [#90](https://github.com/jethac/cloud-offload/pull/90) | Ignores stale multipart sessions and bounds active multipart work and cancellation. | Merged as `96f152c`; 706 tests passed. |
| [#91](https://github.com/jethac/cloud-offload/pull/91) | Retries transient TLS, closed-connection, and endpoint-connection failures inside the existing bounded RunPod S3 gateway budget. | 709 tests and 94 prepared-storage tests pass. Independent review found no Critical or Important issue. |
| [#92](https://github.com/jethac/cloud-offload/pull/92) | Routes current-profile manifest refresh verification through the configured scratch directory. | 709 tests pass. A live 36.66 GB refresh reproduced system temporary-drive exhaustion, then used the configured B-drive scratch path after the fix. |

### ComfyUI extension repository

| PR | Delivered | Evidence status |
| --- | --- | --- |
| [#1](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/1) | Cancel the associated cloud job when ComfyUI execution is interrupted. | Merged as `66b1814`; provider-confirmed closure remains M3. |
| [#2](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/2) | Expand nested boxed subgraphs and resolve workflow-declared Hugging Face assets. | Merged as `d976769`; 73 Python tests passed, 3 skipped, 45 JavaScript tests passed, and live inpainting completed. |
| [#3](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/3) | Prepared-storage opt-in and policy controls. | Merged as `21e66ca`; broader settings/visibility remain. |
| [#4](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/4) | Action-bar discovery, write-only RunPod S3 credential setup, and monotonic startup/cache feedback in the partition title. | Merged as `40c3cf6`; 74 Python tests passed, 3 skipped, and 5 focused JavaScript tests passed. |
| [#5](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/5) | Runs free preflight after final boundary upload; presents one-time GPU rental confirmation with cost, timing, prepared coverage, rationale, uncertainty, countdown, alternate GPU choice, cancellation, and persistent settings; submits only the exact confirmed plan. | Merged as `ff872b4`; 81 Python tests passed, 3 skipped, 60 JavaScript tests passed, syntax and compile checks passed, and the settings, confirmation, details, and GPU-choice states passed a 1440×1000 visual check. Cancellation during the countdown cannot retry paid submission. |
| [#6](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/6) | Binds ComfyUI `api.fetchApi` before the one-time rental decision POST. | Merged as `5c17b31`; 81 Python tests passed, 3 skipped, and 61 JavaScript tests passed. A failed unbound POST created no paid job. |
| [#7](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/7) | Adds the persistent Cloud Jobs panel, safe same-origin job and cancellation routes, reload reconstruction, stable details, active/idle polling, and complete M2 telemetry. | 82 Python tests passed, 3 skipped, and 67 JavaScript tests passed. Live reload restored the open panel and 20 safe jobs; expanded details stayed open across polling. |
| [#8](https://github.com/jethac/ComfyUI-Cloud-Offload/pull/8) | Adds the hard paid-runtime control and shows durable resource-lease identity and provider-confirmed closure time in Cloud Jobs. | 82 Python tests passed, 3 skipped, and 68 JavaScript tests passed; syntax checks passed. |

## Validation record

The following production observations are evidence for specific capabilities,
not blanket production-readiness claims:

- Inpainting job `f86f0bc6-860e-403f-aa1a-87b4b57505d4` completed at 100% on
  RunPod with six resolved assets after the boxed-subgraph/Hugging Face fix.
- Prepared-storage job `b9d44715-7161-4f7c-994a-bd7dc1792d3f` completed on
  RunPod using partition `fdf3fdd1-35b0-44bd-83ef-81dddbb87666` and the
  `comfyui-partition-v1` runtime profile. It recorded five prepared-cache hits
  and one authenticated 4.23 GB model download. Durable copy took approximately
  7.5 seconds; read-back verification and publication took approximately 15.3
  seconds.
- The validated worker artifact for that path is
  `ghcr.io/jethac/cloud-offload-worker-comfyui@sha256:ee3c8b1e4288509c5dd6e0b9d7640933d33be86a459826c23179265b82e2b705`.
- Controlled M1 inpainting job `596b0191-d227-4486-b055-f5bb8c8dfa0e`
  used the merged coordinator, dispatcher, and ComfyUI extension. Free preflight
  recommended a RunPod A100 SXM in `US-MD-1` at $1.49/hour, showed an estimated
  total of $0.11-$0.36, reported 100% prepared coverage and zero missing bytes,
  and required the default ten-second confirmation. The exact confirmed plan
  launched Pod `oxdqu7hn70119x`; worker `worker-c4536488` claimed it, verified
  the prepared manifest and cache hits, completed at 100% with two output nodes
  and two output-artifact groups, and returned the visible ComfyUI image
  `ComfyUI_temp_sobpc_00001_.png`. The ComfyUI queue returned to zero active
  jobs and showed a 353.08-second run time.
- That M1 run is a **partial pass**, not accepted end-to-end evidence. RunPod
  restarted the container after the worker idle timer expired. A second worker
  process delayed dispatcher cleanup, so the exact Pod required manual
  termination. Provider lookup then proved the Pod absent. Backend PR #30 fixes
  the race by starting worker idle time after job completion, giving dispatcher
  cleanup a safety margin, and preventing a restarted runner from resetting the
  paid resource idle clock. That first run remains failed evidence; the next
  bounded replay supplies the accepted automatic-cleanup evidence.
- Controlled replay job `25e58a78-4ddb-49d3-b7d1-46f564a63319` is the accepted
  M1 merged-stack journey. The default ten-second confirmation again selected a
  RunPod A100 SXM in `US-MD-1` at $1.49/hour with a $0.11-$0.36 estimate, 100%
  prepared coverage, and zero missing bytes. Pod `shx3qb2m66iyeg` used worker
  image `ghcr.io/jethac/cloud-offload-worker-comfyui@sha256:30107fcfdda1ce4b03fe6a1c7d6cc42983177309d9f54591164e69326de516e4`
  from merge `8193bfa`. Worker `worker-ebdd770f` verified the exact manifest,
  recorded six prepared cache hits, completed with two output nodes and two
  output-artifact groups, and returned visible image
  `ComfyUI_temp_ppkpc_00001_.png`. ComfyUI showed zero active jobs and a
  297.24-second run time.
- Automatic closure passed. The dispatcher reached the configured 300-second
  idle limit at 00:26:10 local time, accepted exact Pod termination at 00:26:12,
  and a fresh RunPod lookup proved `shx3qb2m66iyeg` absent at 00:26:18. No
  manual cleanup occurred. The coordinator then reported zero queued, running,
  or pending jobs. Worker identity did not change during the idle window.
- Two repeated free inpainting preflights against the measured-history
  implementation returned the same safe workload digest and manifest digest.
  Both calls were `ready`, selected the prepared-local RunPod A100 SXM in
  `US-MD-1`, created no job, and left provider resource count and identity
  unchanged. The recommendation matched two completed jobs, raised confidence
  to medium, and used measured startup, preparation, and execution ranges.
- The corrected estimate includes 300 seconds of paid idle time. Its expected
  paid lifetime is 539.283-721.679 seconds. Compute is $0.223203-$0.298695,
  RunPod ingress and egress are a known $0.00, prorated container storage is
  $0.001352-$0.001810, and expected total job cost is
  $0.224556-$0.300505. No history or cost unknown remains for the selected
  candidate. The compact redacted record is committed as
  [M1 production evidence](evidence/m1-production-evidence-2026-07-30.json).
- Replay, snapshot, support-bundle, journal-authority, concurrency, rollback,
  benchmark validation, force-execution, and credential-resolution tests pass in
  the merged backend history shown above.
- A spend-capped fresh-Pod cold/hot campaign used the same safe request digest,
  `db85d75dd5ee549db250d27fdf1679a7b1ddea16a01e0afcba4ade0a53afa527`,
  for jobs `34172fcd-4a6b-4ffb-aa6e-562ff709f841` and
  `c431bfb2-98df-490a-a7d5-bde3a950e485`. It passed configuration restoration,
  exact-Pod cleanup, final empty provider inventory, and redaction audit. Cold
  took 432.500 seconds at a conservative $0.179007; hot took 394.531 seconds at
  $0.163292, for a conservative campaign total of $0.342299.
- The cold run spent 80.864 seconds in weight download. The hot run produced six
  prepared-artifact hits with no misses or downloads, but still spent 64.425
  seconds reading and verifying prepared artifacts. This is an accepted M0
  baseline and evidence that current prepared state works. An 8.8% end-to-end
  improvement from one pair does **not** satisfy the M4 acceleration target.
- A spend-capped five-class failure campaign conservatively accounted for
  $0.920037 and ended with prepared-storage policy restored, empty provider
  inventory, and no orphan audit errors. It accepted three canaries:
  cancellation removed the exact Pod in 34.500 seconds; a terminated provider
  Pod was replaced and the job completed in 347.391 seconds; and a deliberately
  missing strict storage binding failed before provider launch, restored the
  valid binding, then launched and completed in 395.375 seconds.
- That same campaign did **not** accept its corruption or coordinator-restart
  cases. The corruption mutation reached object storage but the mounted path
  served previously cached valid bytes, so no quarantine event was observed and
  the original object was restored. The restart case encountered an unusually
  long worker startup and reached its $0.30 scenario ceiling before execution,
  so the restart hook never fired. Both exact Pods were removed. These outcomes
  are retained as failed evidence rather than rewritten as passes.
- A restart-only retry that still waited for `execution_start` remained in
  `runner_starting` for 1,012.250 seconds, reached its scenario timeout, and was
  cancelled. The exact Pod was absent after 9.328 seconds, policy was restored,
  provider inventory was empty, and the conservative cost was $0.418959. This
  is startup-latency evidence, not restart evidence.
- The corrected restart canary triggered immediately after provider creation,
  stopped coordinator PID `38032`, brought replacement PID `49336` healthy,
  proved job `05156c58-cff6-4164-915d-5273ec72519f` was replayable, and persisted
  its cancellation through that replacement. Exact Pod `tpoita6ieh2bq5` was
  provider-absent after one termination request. The case passed in 29.234
  seconds with a conservative $0.012100 compute upper bound, restored policy,
  empty final inventory, and no orphan or audit error.
- The PR #17 fresh-object corruption campaign used safe request digest
  `8c44c8ee4ddca337b148212dca24e2d9c6e8c068ea5300091f3b33c172310106`
  for job `d37b5054-d80f-4d59-87f4-dbd18e6d310c` and exact Pod
  `arlvyvhu8lm1we`. The pre-submit hook published a unique 31-byte corrupt blob,
  temporary signed generation, registry projection, and valid coordinator
  fallback. The run completed in 420.500 seconds with a conservative $0.174040
  compute upper bound, and cleanup removed the Pod, generation, blob, manifest,
  registry/invalidation/quarantine state, and coordinator fallback. Provider
  inventory was empty and prepared-storage policy was restored to `smart`.
- That fresh-object run is safe failed evidence, not an accepted corruption
  canary. Placement selected no prepared manifest and the worker emitted
  `cache_artifact_miss` with reason `manifest_not_found`; no
  `cache_artifact_quarantined` event occurred. The injected asset changed the
  actual requirement profile to
  `sha256:77ea27b92fac7d4b45f8e941c1a545d5368d985252453b0bf9d8419912f386f2`,
  while the canary manifest retained the prior profile. Cross-profile lookup
  then depended on the mounted mutable `indexes/latest`, which did not contain
  the newly published manifest. This proves the fresh blob removed cache aliasing
  and isolates the remaining defect to exact manifest selection and visibility.
- A worker image built from PR #18 was pushed as
  `ghcr.io/jethac/cloud-offload-worker-comfyui@sha256:2ecdf8b88ef758257a50aae9e24e90cea914dad731a16157bc5d84683d1db509`.
  Its in-image smoke test confirmed the exact manifest-by-ID code and source
  revision `f4d9cf9751a00d31e24f287140ec19993ee129a3`.
- The first PR #18 production replay created job
  `d66cf2c0-67d9-4272-b785-2b912f3e7327` and exact Pod `ni6c882lcryxyu`.
  The benchmark was stopped as soon as placement reported `complete: false` and
  `manifest_ids: []`; waiting for worker startup could not turn that state into
  an accepted exact-manifest canary. The exact Pod became provider-absent, the
  job was closed as failed, provider inventory returned empty, and the synthetic
  state, blob, coordinator fallback, and policy were clean. Because the operator
  stopped the command before scorecard completion, this run has no accepted cost
  figure; its configured scenario and campaign ceiling was $0.50.
- The replay isolated a second name-identity defect. The hook fingerprinted the
  requested capability `comfyui-partition-v1` as
  `sha256:fed17d04a6deb7117c56bd13256da11bed6b12d22088813bce89d5f8a3db5e24`.
  The dispatcher resolved that capability to configured launch profile
  `comfyui`, so the job fingerprint was
  `sha256:86a2004c166c05ec60f84d39c4fd0f3a1b565afd175ab3425689245b54185ed8`.
  The launch profile name is part of both the runtime identity and profile key.
  The canary must therefore use the normalized resolved profile's `name`, which
  is the same value used by `Dispatcher._launch_worker`.
- The PR #19 replay created job `e4c057bd-5cf1-4eff-90db-38f4e64e6637` and
  exact Pod `4qx520ec84h0lh`. Placement selected exact manifest
  `sha256:798a0d6878db984ed3b1804e823ec53619a3d2af0c4e9bf271d023e64966f664`;
  the worker emitted `cache_mount_ready`, verified the direct manifest, and
  found the synthetic digest
  `sha256:cec70f0e391431bde36301228ab7b83025c929f2ca61a549300fd55b30860311`.
  This directly proves the profile-name and immutable manifest-by-ID corrections.
- That PR #19 replay is still safe failed evidence. The worker reported the
  synthetic artifact as a verified cache hit instead of quarantining it. The
  canary had written valid bytes to the new S3 key to verify publication, then
  replaced them with corrupt bytes. The mounted RunPod volume retained the first
  valid object value. The operator stopped the case immediately; the exact Pod
  became provider-absent, the job closed as failed, provider inventory returned
  empty, storage policy returned `smart`, and all synthetic state was absent.
  The interrupted command produced no accepted cost figure; the configured
  scenario and campaign ceiling was $0.50.
- The merged PR #20 replay created job
  `c9dbd0d7-8628-4273-b87a-dfd87d1b0a33` and exact Pod `fqh9x6qm5xtn7c`.
  Placement selected exact manifest
  `sha256:e1af79787de255c0dbb7e288abfc540829451b1e3c5b83682a5d821f1dd9d477`,
  and the worker loaded it through the immutable direct path. The worker again
  reported synthetic digest
  `sha256:cec70f0e391431bde36301228ab7b83025c929f2ca61a549300fd55b30860311`
  as a valid hit instead of quarantining it.
- The third replay proved a second object-identity error. The canary derived its
  payload, digest, and object key only from the scenario name. All three recent
  campaigns therefore used the same synthetic digest. Deleting the control-plane
  object did not prove that a later mounted volume view had discarded valid
  bytes from an earlier campaign. The operator stopped the replay immediately.
  The job closed as failed, the exact Pod became provider-absent, provider
  inventory returned empty, storage policy returned `smart`, and the synthetic
  state, blob, and coordinator fallback were absent. The interrupted command has
  no accepted cost figure; its configured scenario and campaign ceiling was
  $0.50.
- The PR #21 replay created job `73228360-51f3-4522-adbb-4b1161bc3002` and
  exact Pod `3nalcbc4qt2iom`. It used new 82-byte synthetic digest
  `sha256:d1562543c90e65de8384031c75c4f1f9e9470362fe3619cb49b6349429d76f1f`.
  This proves that campaign identity no longer aliases any earlier canary.
  Placement selected exact manifest
  `sha256:bfe5146c7b935c3cd942875ce72233ba25f29121ff3719f66afee3d6179fb16d`,
  and the worker emitted `cache_mount_ready`.
- The PR #21 replay is still failed evidence. The mounted worker view verified
  base manifest
  `sha256:5efb685fc493e5c12882587f2365849e8b4a4f94a0809a334fb6236adf996631`
  but could not read the new exact manifest. It reported the canary as
  `cache_artifact_miss` with reason `manifest_not_found`, then populated the
  valid coordinator fallback. The operator cancelled immediately because no
  later quarantine could satisfy the canary. The scorecard failed after 294.609
  seconds with a conservative $0.121935 compute upper bound. Preparation and
  cleanup hooks exited successfully, the observation hook failed as required,
  no orphan was reported, the exact Pod became provider-absent, provider
  inventory returned empty, and policy returned `smart`.
- Automated cleanup was not complete. Fallback population announced registry
  manifest
  `sha256:c4a4ba7f13f8146abafae257dbd033f651c5d5ff15d3fdd8e74a4ff4cb13b7b7`
  with the synthetic artifact after the hook recorded its original cleanup
  target. It had no durable index entry or manifest object. An exact manual removal
  removed that artifact, restored six normal artifact projections, and left no
  synthetic manifest, artifact, invalidation, object, quarantine, fallback,
  state object, active job, worker, or provider instance. This manual repair is
  cleanup evidence, not an accepted automated canary.
- The PR #22 worker image was pushed and pinned as
  `ghcr.io/jethac/cloud-offload-worker-comfyui@sha256:321f6931d08b159359ed6df15f4bac890872affff9af17252fbf3fd934320c8d`.
  Its image label and smoke test proved merged source revision
  `3a60f7e23da5800e2ccae8809a589f57cfdc3df3`, coordinator manifest fetch,
  manifest authority fetch, and exact worker selection were present.
- The first PR #22 production replay created job
  `b2afa678-6841-475f-ba0c-6e0885d2cd0f` and exact Pod `0jpn0hsx6n4g54`.
  The dispatcher selected and persisted exact manifest
  `sha256:1a49ce995de9d8b79064445d1f90656377170a21570e53dc7993f1e009c6c986`.
  The worker used that manifest for every normal artifact, emitted
  `cache_artifact_quarantined` for unique 82-byte canary digest
  `sha256:33d3f498b22d80f4dcac85fd092ce0e19d0142a3760d83c280a7b91e28ee89ab`,
  populated the valid fallback, and completed cache restore. All three hooks
  exited successfully.
- That replay ended in `dead_letter` because the existing workload declared an
  asset path that failed models-directory validation. This is the documented
  explicit safe terminal behavior after quarantine, but the local plan still
  expected only `completed`. The scorecard therefore failed after 286.953
  seconds with a conservative $0.118767 compute upper bound. Automated cleanup
  passed: the exact Pod, provider inventory, state, blob, manifest, registry
  projection, invalidation, quarantine, fallback, and storage-policy checks were
  all clean without manual repair.
- The plan was corrected to accept only `completed` or `dead_letter`. The next
  replay created job `99911bdb-d835-47ca-b424-3702b7d2e4a6` and exact Pod
  `tuw2a5vj7ysmbn`. Placement and the assigned job used exact manifest
  `sha256:522768dfd4ba55e7fba029f0ddd950710b4c0ad0c55bf1d708c8bfe573c29f71`.
  The worker again verified that exact manifest and emitted quarantine for new
  82-byte digest
  `sha256:5ebabe4f3455e5decb2e8d7747634394b7b3ee5f506586fed9b81c9a5a398e4d`.
- The second PR #22 replay failed only its observation-hook time limit. Normal
  prepared-asset reads placed quarantine 135.756 seconds after
  `cache_mount_ready`, but the hook allowed 105 seconds. The job reached the now
  accepted `dead_letter` status, cleanup succeeded, no orphan or audit error was
  reported, and every direct synthetic-state check was zero. The failed
  scorecard took 326.718 seconds with a conservative $0.135225 compute upper
  bound. A 240-second corruption observation window inside a 270-second hook
  process limit now passes 534 tests on the next PR branch.
- The merged PR #23 replay created job
  `64c04e33-d5d2-4b98-9798-a875bbd6f949` and exact Pod
  `ogiwnmulhvcars`. Placement and the assigned job used only exact manifest
  `sha256:a02980bac25a108253aff5f214ddf0f7db93ec1df587b5bf8a1e310e9b113eda`.
  The worker verified that manifest, quarantined unique 82-byte digest
  `sha256:3924e26961a9aa28806d7113e3ca367b3aa5124ad291f4138bdf0c685ef09bad`
  40.374 seconds after mount, and reached the documented safe `dead_letter`
  status. Preparation, observation, and cleanup actions all exited successfully.
  The scorecard passed in 135.656 seconds with a conservative $0.056147 compute
  upper bound.
- The accepted corruption cleanup removed the exact Pod and every synthetic
  blob, manifest, registry projection, invalidation, quarantine object,
  coordinator fallback, and local state object. Provider inventory and the
  queued/running job count were zero, the storage policy was restored to
  `smart`, and no orphan or audit error remained.

The accepted cold/hot scorecard and all five failure classes now prove M0. The
compact projection in
[`evidence/m0-production-evidence-2026-07-29.json`](evidence/m0-production-evidence-2026-07-29.json)
makes the accepted results durable and comparable without committing sensitive
operational data.

### M0 evidence matrix

| Evidence | Result | Durable conclusion |
| --- | --- | --- |
| Fresh-Pod cold/hot pair | **Accepted** | Prepared state produces real hits, but the measured 8.8% end-to-end improvement is only a baseline and does not meet the M4 acceleration target. |
| Cancellation | **Accepted** | The attributable Pod was removed in 34.500 seconds and cleanup completed. |
| Provider interruption | **Accepted** | A terminated Pod was replaced and the job completed. |
| Strict storage failure | **Accepted** | Invalid storage failed before provider launch; restored configuration then completed. |
| Coordinator restart | **Accepted** | Replacement health, journal replay, replacement-owned cancellation, and exact provider cleanup passed. |
| Corruption, cached-object attempts | **Failed safely** | Mounted valid bytes defeated mutation; exact resources and configuration were cleaned up. |
| Corruption, fresh-object attempt | **Failed safely** | Unique-object isolation worked; stale mutable-index discovery prevented exact manifest restore and quarantine. Cleanup was complete. |
| Corruption, first exact-ID attempt | **Failed safely** | The immutable worker path was present, but capability-name fingerprinting did not match the dispatcher's configured launch-profile fingerprint. The run stopped at placement and cleanup was complete. |
| Corruption, first-write attempt | **Failed safely** | Exact manifest selection and loading passed, but writing valid bytes before corrupt bytes let the mount retain the valid first value. The run stopped after the synthetic verified hit and cleanup was complete. |
| Corruption, deterministic-campaign attempt | **Failed safely** | Corrupt-first publication worked, but each campaign reused the same digest and object key. A mounted volume view retained earlier valid bytes. The run stopped after the synthetic verified hit and cleanup was complete. |
| Corruption, unique campaign attempt | **Failed; cleaned manually** | Unique digest and exact placement passed, but the mounted view missed the new manifest and used coordinator fallback. Automated cleanup missed its derived registry projection; exact manual cleanup removed it. |
| Corruption, authority-fetch attempt | **Behavior passed; scorecard failed** | Exact assigned-manifest fetch, quarantine, fallback, and automated cleanup passed. The job reached documented safe `dead_letter`, but the plan expected only `completed`. |
| Corruption, observation-window attempt | **Behavior passed; harness timed out** | Exact fetch and quarantine passed again, but six normal prepared reads delayed quarantine to 135.756 seconds after mount, beyond the 105-second hook window. Cleanup was complete. |
| Corruption, accepted replay | **Accepted** | Exact manifest authority, unique-object quarantine, explicit safe terminal behavior, automated cleanup, restored policy, and empty provider inventory all passed. |
| Compact redacted projection | **Accepted** | The committed safe projection compares cold, hot, cancellation, provider, storage, restart, and corruption evidence. A test enforces its scenario set, finite values, cleanup receipts, and redaction rules. |

## Current execution state and immediate next work

Status snapshot as of 2026-07-30:

- M0 journal transport and lifecycle authority are merged.
- M0 benchmark and scorecard automation are merged, including authoritative
  cold/hot policy orchestration, exact-Pod cleanup, and reversible failure
  hooks.
- The coordinator and dispatcher were restarted from merged `main`; coordinator
  health and `/api/active-workers` passed at restart, and provider inventory was
  empty. This is an operational handoff observation, not durable acceptance
  evidence.
- RunPod, Hugging Face, RunPod S3, and provider credentials resolve through the
  configured environment/keychain paths without logging their values.
- Local runtime plans, scorecards, and service logs belong under `.runlogs/` and
  must never be committed because they may contain workflow or operational data.
- The cold/hot campaign is accepted. Cancellation, provider recovery, and
  storage failure canaries are accepted. The first corruption and restart
  attempts failed safely and were cleaned up exactly.
- A focused corruption/restart rerun against merged PR #14 stayed within its
  $1.00 campaign and $0.50 scenario ceilings and conservatively accounted for
  $0.257885. It ended with empty provider inventory and both exact Pods absent,
  but both cases correctly remained failed. The fresh Pod again served valid
  cached bytes rather than the pre-submit corruption canary, so no quarantine
  event occurred. The restart canary stopped the exact healthy coordinator but
  a Windows process-liveness probe raised while that process was exiting, so the
  replacement was not launched. Manual recovery restored a healthy coordinator
  with a new PID; prepared-storage policy remained `smart`.
- The Windows restart probe and canary contract were corrected in PRs #15 and
  #16. A focused production rerun now directly proves replacement health,
  journal replay, replacement-owned cancellation, and exact provider cleanup.
- PR #17 removed cached-object aliasing from the corruption canary. Its first
  production run narrowed the failure to exact-profile and mutable-index
  identity. PR #18 merged the direct manifest path and produced the pinned
  worker image above. Its first replay showed that the hook and dispatcher used
  different names for the same profile. The launch-profile-name correction now
  passes 530 tests in merged PR #19. Its replay proved exact manifest selection
  and loading, then showed that a valid first write can remain cached after an
  object update. PR #20 made corrupt bytes the first and only S3 write. Its
  replay then proved that the deterministic scenario digest still reused one
  mounted object identity across campaigns. PR #21 corrected campaign identity.
  Its replay proved that a fresh mounted view can still miss a newly published
  exact manifest and that fallback publication expands the cleanup target set.
  PR #22 added authenticated exact-manifest fetch and complete derived-manifest
  cleanup. Its pinned worker passed those behaviors twice in production. PR #23
  then extended only the bounded corruption observation window. The accepted
  replay proved exact manifest authority, quarantine, safe terminal behavior,
  and complete automated cleanup in production.
- M0 through M3 are complete. The accepted merged-stack paid run proves the
  recommendation, confirmation, exact launch, prepared restore, execution,
  result return, and automatic exact-Pod closure. Two repeated free preflights
  prove stable identity, two-sample measured timing, medium confidence,
  complete RunPod compute/transfer/container-storage cost, and no provider
  mutation. The persistent Cloud Jobs surface now reconstructs safe lifecycle,
  transfer, ETA, resource, cost, cache, cancellation, and billing state after a
  reload. The M3 paid matrix then proved worker-boot cancellation, coordinator
  restart after provider creation, persisted leases, provider-confirmed closure,
  and empty final RunPod inventory. M4 fast trusted restore is now the active
  gate.

### Completed M0 corruption contract

The accepted direct-manifest corruption implementation is bounded to these
contracts:

1. The benchmark hook receives only the safe requested worker capability and
   declared asset digests, never the workflow body or credentials.
2. The canary resolves that capability to the normalized configured launch
   profile and recomputes the same prepared-requirement fingerprint the
   dispatcher will use after synthetic-asset injection. The configured profile's
   `name`, not the requested capability, is the fingerprint identity.
3. The signed canary manifest is published under an immutable deterministic
   `manifests/by-id/sha256/...` key, and its registry entry references that key.
4. The synthetic S3 key receives corrupt bytes as its first and only value. The
   valid artifact exists only in coordinator fallback storage. The canary never
   depends on a mounted volume observing an update to an existing key.
5. Each campaign injects one safe random nonce. That nonce creates a new payload,
   digest, object key, file name, and state path. The same nonce is passed to the
   prepare, observe, and cleanup hooks. A campaign cannot reuse a synthetic
   mounted object identity from an earlier run.
6. The dispatcher persists the selected exact manifest ID in the assigned job.
   An authenticated worker may fetch only that ID, for its active job and bound
   volume, from the coordinator registry. The coordinator verifies the signed
   document and volume claim before it returns the manifest.
7. `PreparedStateCAS.find_manifest(manifest_id=...)` first uses a mounted indexed
   or immutable direct object. If either mounted view is stale, it may fetch the
   exact assigned manifest through the coordinator authority. A mismatched ID,
   profile, volume claim, or signature remains a hard failure. A later
   same-profile worker publication cannot replace the exact placement promise.
8. Corruption cleanup removes every registry manifest, durable metadata object,
   invalidation, quarantine object, blob, and fallback that references the
   unique digest, including manifests created by valid fallback population.
9. Corruption observation defaults to the durable `cache_mount_ready` event so
   a finite observation window is not consumed by unrelated image startup. The
   240-second event window and 270-second process limit cover the measured
   prepared-asset verification path while remaining below scenario and campaign
   limits.
10. Existing manifest/index behavior remains the normal path. The coordinator
    fetch is a narrow exact-ID recovery path, not a second source of unsigned
    truth or a general manifest query surface.

Acceptance required focused and full tests, a reviewed PR, and one spend-capped
production replay. The replay showed a nonempty exact `manifest_ids` placement, a
`cache_artifact_quarantined` event for the synthetic digest, successful valid
fallback or explicit safe terminal behavior, hook cleanup success, exact Pod
absence, empty provider inventory, restored storage policy, and absence of every
synthetic manifest, blob, registry projection, invalidation, quarantine object,
and coordinator fallback. A completed job without quarantine remains a failed
canary. The accepted replay satisfied all of these conditions.

### M0 evidence audit

1. **Proved:** a validated safe summary identifies the repeated workload by
   request digest while its workflow body stays local.
2. **Proved:** one alternating fresh-Pod cold/hot campaign ran within explicit
   campaign, scenario, runtime, spend, and cleanup ceilings.
3. **Proved for the accepted pair:** startup, preparation, execution, closure,
   and conservative compute cost are separately represented in its scorecard.
4. **Proved:** the full prepared-storage configuration was restored and verified
   after every completed scenario and campaign cleanup.
5. **Proved:** cancellation, provider, storage, corruption, and
   coordinator-restart canaries passed with direct accepted evidence.
6. **Proved for completed campaigns:** final provider inventory was empty and
   attributable Pods had provider-absence receipts.
7. **Proved:** the compact committed projection is comparable across runs and
   contains no prompts, raw workflow, asset paths, failure action
   arguments/output, credentials, or secret endpoints. A repository test checks
   the redaction contract.

### M0 exit audit

1. **Explainable critical path:** the cold and hot records separate provider
   request, provisioning, readiness, runner startup, preparation, execution,
   result availability, and provider closure time.
2. **Authoritative reload:** journal reload tests and the accepted coordinator
   restart canary reconstruct current state from persisted events.
3. **Order safety:** duplicate-event collapse and reordered-event
   non-regression tests pass.
4. **Direct production evidence:** cancellation, provider, storage, corruption,
   and restart passed with finite runtime and spend limits.
5. **Safe comparable evidence:** the compact redacted projection contains all
   seven accepted scenario classes.
6. **Resource closure:** every accepted scenario has provider-absence receipts;
   campaign audits found no orphaned Pod, and corruption cleanup left zero
   synthetic state objects.

### Active engineering handoff

PR #25 started M1 with the read-only backend contract. PR #26 binds that
contract to paid execution. The coordinator now persists only the safe report,
requires a current `preflight_id`, matching `manifest_digest`, and selected
`candidate_id`, and re-reads volatile facts before it creates a job. The
dispatcher then requires the exact confirmed offer, price, GPU, region, and
prepared volume before provider launch. A changed plan fails safely and asks
for a new confirmation. It does not silently select another offer or use cold
storage. A prepared worker can claim the job only when it mounted the exact
confirmed volume. Confirmed work starts at queue depth one so that normal batch
delay does not consume the 60-second quote lifetime.

PR #27 adds the durable confirmation policy and enforces it at the coordinator.
The server controls countdown timing, accepts explicit Start now, and records
the accepted action. Preflight requests can tighten but cannot loosen configured
hourly, total-cost, or region limits. A material price, cost, storage, capacity,
quote, or confirmation-policy change returns a revised report with mandatory
confirmation, even when normal confirmation is set to `never`.

Extension PR #5 completes the user interaction. It runs free preflight after
the final boundary artifacts exist, sends only the safe report to one active
ComfyUI browser, and uses a random one-time decision ID. The panel shows the
recommended provider, GPU, region, hourly price, total-cost range, startup and
execution ranges, prepared coverage, rationale, confidence, and uncertainty.
It provides Start now, Cancel, Choose another GPU, Don't show again, a default
countdown, mandatory changed-plan review, and the equivalent persistent settings.
Opening details or choosing another GPU pauses automatic start. A required
confirmation with no active browser creates no job, and cancellation during the
server countdown cannot retry paid submission.

Backend PR #29 made preflight use the same stable worker credential as the
dispatcher. Extension PR #6 bound the ComfyUI decision POST to its API object.
Both defects failed before job creation. With both fixes merged, controlled job
`596b0191-d227-4486-b055-f5bb8c8dfa0e` proved recommendation, confirmation,
exact offer launch, exact prepared-volume restore, graph execution, and result
return. It also exposed the cleanup race recorded above.

The merged-stack journey and recommendation-accuracy slice are now accepted.
Equivalent workflow shapes share history without hashing node IDs, prompts,
seeds, artifact IDs, or private paths. History is matched by provider, GPU,
region, and prepared/cold class. One observation remains visible but cannot
change ranking; two through four observations give medium confidence, and five
or more give high confidence.

### M1 evidence and exit audit

The safe evidence projection is
[M1 production evidence](evidence/m1-production-evidence-2026-07-30.json).
It contains only safe digests, aggregate timings and cost, opaque lifecycle
identifiers, cleanup state, test counts, and pass conclusions.

1. **Deterministic blockers create no Pod:** provider-mutation guard tests pass;
   failed readiness and revalidation paths create no job or provider resource;
   two live free reference preflights also left both counts and provider
   identity unchanged.
2. **Stable manifest:** two consecutive live reference preflights produced the
   same `manifest_digest` and the same private-data-free `workload_digest`.
3. **Explainable recommendation:** the selected prepared-local RunPod A100 SXM
   carries provider, GPU, region, price, preparation coverage, policy rationale,
   matched history count, confidence, timing range, and complete cost parts.
4. **Confirmation boundary:** coordinator tests reject missing or early
   confirmation without queue or provider mutation. The accepted paid journey
   used the default ten-second confirmation before exact launch.
5. **Launch revalidation:** price, cost, storage, capacity, quote, and policy
   change tests force a revised report and mandatory fresh confirmation.
6. **Healthy-path friction:** the extension runs free preflight automatically,
   displays one short confirmation, and submits the exact choice. The accepted
   inpainting journey required no provider infrastructure action from the user.

All M1 exits pass. M1 is complete.

### M2 evidence and exit audit

The safe evidence projection is
[M2 visibility evidence](evidence/m2-visibility-evidence-2026-07-30.json).
It contains aggregate latency, test counts, safe field coverage, persistence
results, privacy conclusions, and explicit M3 deferrals. It contains no raw job
IDs, request bodies, workflows, prompts, private paths, signed URLs, digests, or
provider payloads.

1. **Reload:** a live same-origin request returned 20 recent jobs in 350.7 ms.
   Browser reload restored the open Cloud Jobs panel and its job list.
2. **Progress:** stage bands and time-based estimates keep active early phases
   moving without regressing. Successful terminal state is 100%. Automated tests
   cover monotonic merge and projection behavior.
3. **Transfers:** declared artifact, URL, and authenticated Hugging Face transfers
   report observed bytes when file size or a local path makes measurement
   possible. The projection calculates smoothed throughput and transfer ETA.
4. **Persistent truth:** the coordinator projection and Cloud Jobs panel work
   without canvas state. Explicit job details stay open across idle polling.
5. **Safe uncertainty:** missing values remain unknown. Billing closure remains
   unconfirmed until M3 adds authoritative provider termination receipts.

The backend passed 579 tests. The extension passed 82 Python tests with 3 skips
and 67 JavaScript tests. Syntax checks also passed. All M2 exits pass. M2 is
complete. M3 was the next milestone at the time of this evidence.

### M3 evidence and exit audit

The safe evidence projection is
[M3 lease closure evidence](evidence/m3-lease-closure-evidence-2026-07-30.json).
It combines the complete automated phase matrix with a spend-capped merged-stack
RunPod campaign. It contains only safe counts, limits, timings, immutable build
identity, opaque job/lease/resource IDs, and provider-closure receipts.

1. **Cancellation phases:** automated tests cover provisioning, worker boot,
   dependency preparation, execution, and result transfer. The paid campaign
   directly cancelled during worker boot and removed exact Pod `ha9hgesfdxgey9`.
2. **Provider truth:** both paid jobs show `termination_confirmed: true` and a
   provider confirmation time. Paid elapsed time freezes at that receipt.
3. **Restart recovery:** the second paid case restarted the coordinator after
   RunPod created Pod `k4qg73i4szqq33`. The replacement coordinator became
   healthy, cancellation persisted, and exact cleanup completed.
4. **No orphan:** the two-case campaign passed with no manual cleanup, no orphan
   audit error, and zero RunPod resources in the final independent inventory.
5. **Terminal and cache safety:** automated late-callback and cancelled-cache
   publication tests pass. A stopped but still present provider resource does
   not count as closure.

The campaign had a USD 0.30 total limit and a USD 0.15 per-case limit. Its
conservative compute-cost upper bound was USD 0.033868. All M3 exits pass. M3 is
complete.

### M4 evidence and exit audit

The safe evidence projection is
[M4 fast restore evidence](evidence/m4-fast-restore-evidence-2026-07-30.json).
It combines the complete automated integrity matrix with two spend-capped
merged-stack RunPod preparation campaigns. It contains only safe counts, limits,
aggregate timings, immutable build identity, and provider-closure conclusions.

1. **No complete hot read:** two fresh-Pod trusted restores each materialized six
   artifacts and 36,664,522,522 bytes while reading 12,582,912 verification
   bytes. Both show six trusted hits, six background scrubs, and zero full-digest
   hits.
2. **First-seen integrity:** the receipt-issue run performed complete digest
   verification for all six artifacts and all 36,664,522,522 bytes before it
   issued trust receipts.
3. **Corruption safety:** same-generation foreground and background sample tests
   detect corruption, remove a materialized target before return, quarantine the
   object, remove the volume from placement, and permit recovery only after a new
   complete verification. Private and audit-due assets remain on complete digest
   verification. The accepted M0 production corruption canary remains durable.
4. **Measured speed:** four cold preparation samples have a median of 176.482397
   seconds. Three hot samples, including receipt issue, have a median of 15.179662
   seconds. Hot preparation is 8.601% of cold, or 91.399% faster.
5. **Bounded closure:** eight attributed Pods received provider-absence receipts,
   both campaigns stayed within their combined USD 2.15 limit, and independent
   final inventory contained zero resources. A 300-second initial-start lease
   expired before one worker became ready; the production lease setting was
   corrected to 900 seconds before the accepted preparation observations.

The campaign scorecards also record two post-preparation workflow outcomes: one
execution failure and one scenario cost limit. They do not change the completed
preparation measurements, integrity observations, or provider closure. They remain
visible for the production release gate. All M4 exits pass. M4 is complete. M5
was the next milestone at the time of this evidence.

### M5 evidence and exit audit

The safe evidence projection is
[M5 workflow capsule evidence](evidence/m5-workflow-capsule-evidence-2026-07-30.json).
It records the exact merged backend and worker image, free restore preflight,
paid first-rent bundle population, paid fresh-Pod restore, result transport,
cost bounds, one bounded image-start incident, and provider cleanup. It contains
no job ID, provider resource ID, raw workflow, private path, endpoint, or secret.

1. **Free blockers:** automated tests cover deterministic credential, artifact,
   disk, and node blockers. The accepted restore preflight was ready, reported a
   complete prepared volume with 100% coverage, and made no provider mutation.
2. **Canonical closure:** identical capsules have identical digests. Undeclared
   dynamic behavior remains explicit uncertainty.
3. **First rent:** a fresh Pod built and published one custom-node bundle and one
   Python environment bundle. The accepted job completed in 132.047 seconds with
   an estimated compute cost upper bound of USD 0.054653.
4. **Later rent:** a different fresh Pod restored both bundles before it claimed
   the job. Both complete digests passed. The worker emitted two cache hits, no
   population event, and a restore receipt with both artifacts. The workflow
   returned one digested PNG result in 57.344 seconds with an estimated compute
   cost upper bound of USD 0.023734.
5. **Bounded incident:** an earlier exact-image attempt did not reach worker
   registration. It is not accepted as a successful population. Its estimated
   compute cost upper bound was USD 0.265588. Cleanup left no provider resource.
6. **Closure:** all three attributed attempts left zero orphaned resources. The
   independent final provider inventory was empty. No manual cleanup was needed.

All M5 exits pass. M5 is complete. M6 was the next milestone at the time of this
evidence.

### M6 evidence and exit audit

The safe evidence projection is
[M6 regional replication evidence](evidence/m6-regional-replication-evidence-2026-07-30.json).
It records free regional stock, bounded paid demand, shadow accuracy, automatic
provider-object copy, repeat-cycle safety, prepared placement, regional loss,
controlled TTL expiry, source preservation, and final cleanup. It contains no
job ID, provider resource ID, raw workflow, private path, endpoint, or secret.

1. **Measured demand:** six positive-byte paid observations covered EU-RO-1,
   EUR-IS-1, and US-GA-2. Each observation measured 3,840,000 missing runtime
   bytes. The complete paid campaign used a USD 0.12 per-scenario limit and a
   USD 0.061635 estimated compute-cost upper bound.
2. **Shadow accuracy:** three unique mature recommendations received a later
   matching paid observation. Precision was 1.0 against a 0.8 gate, so automatic
   mode opened only after the required three recommendations passed.
3. **Budgeted copy:** the controller created a 50 GB EUR-IS-1 target with a USD
   3.50 monthly estimate under a USD 4 replication limit and a USD 11 complete
   storage limit. It copied and verified two artifacts and 3,840,000 bytes
   through RunPod S3 without a GPU.
4. **Repeat safety and placement:** a repeat controller cycle created no action
   and left the completed-action count at one. Free preflight then returned
   seven complete prepared choices and seven cold fallback choices in the same
   region.
5. **Regional loss:** deletion of the exact managed test target produced one
   lost target and one lost action. A lower total budget blocked replacement.
   Free preflight removed all prepared choices and retained seven cold choices.
6. **TTL and cleanup:** a controlled clock-injection canary preserved the real
   one-day TTL, moved only the exact completed action past expiry, and ran the
   normal expiry path. One action expired, zero expiry failures occurred, one
   empty provider target was deleted, and the two-artifact source manifest
   remained ready in US-MD-1.
7. **Closure:** the saved user configuration was restored to shadow mode.
   ComfyUI, the coordinator, and the dispatcher were healthy. RunPod had zero
   active GPUs, only the three original volumes, and no automatic test volume.

All M6 exits pass. M6 is complete. M7 is the first unmet milestone.

### Operational safety rules

- A paid benchmark never starts without explicit spend confirmation and finite
  circuit breakers. Limits reduce risk but are not provider-side escrow.
- Attribute provider resources before terminating them; never delete an
  unverified baseline or unrelated Pod.
- Terminate attributable Pods after each scenario and audit provider inventory
  again at campaign end.
- Never persist or print credential values. Configuration and support surfaces
  expose presence, provenance category, and actionable absence only.
- Never include raw workflow bodies, prompts, private asset paths, signed URLs,
  hook arguments, or hook output in a scorecard or support bundle.
- Managed-volume deletion and adopted-volume detachment are distinct; destructive
  provider storage deletion always requires explicit user intent.
- Benchmark policy mutation must be reversible and restore the full prior object,
  not a reconstructed subset that could lose region, volume, tenant, or privacy
  settings.

## Delivery milestones

| Milestone | Status on 2026-07-30 | Gate |
| --- | --- | --- |
| M0 — measurement and scorecard | **Complete** | All exits passed; seven accepted scenarios and the redacted projection are durable. |
| M1 — preflight, recommendation, confirmation | **Complete** | All exits passed; the merged-stack paid journey, automatic cleanup, stable free preflight, measured recommendation history, and complete RunPod cost are durable. |
| M2 — persistent visibility | **Complete** | All exits passed; the persistent Cloud Jobs surface, reload reconstruction, complete safe telemetry, and compact evidence are durable. |
| M3 — leases and billing closure | **Complete** | The automated five-phase matrix and the bounded paid RunPod cancellation/restart campaign pass with provider-confirmed closure and zero final resources. |
| M4 — fast trusted restore | **Complete** | Signed trust receipts, complete first-seen verification, foreground and background corruption handling, safe fallback, and the paid 8.601% hot-to-cold preparation result are durable. |
| M5 — workflow capsules | **Complete** | Canonical capsules, free blockers, signed runtime bundles, exact runtime identity, first-rent publication, paid fresh-Pod restore, result transport, and compact redacted evidence are durable. |
| M6 — regional replication | **Complete** | Six positive-byte paid observations, 3-of-3 shadow validation, budgeted automatic copy, repeat safety, prepared and cold placement, loss recovery, TTL expiry, and source-preserving cleanup have accepted evidence. |
| M7 — production release gate | **In progress** | M0 through M6 are complete. Thirty consecutive full canary matrices are the active gate. |

### Milestone 0 — Measurement contract and production scorecard

- Define and persist `JobEventV2`.
- Add replay, snapshot, and support-bundle APIs.
- Build [spend-capped benchmark automation](production-benchmark.md).
- Run alternating fresh-Pod cold/hot jobs.
- Inject cancellation, provider, storage, corruption, and restart failures.
- Record startup, preparation, execution, closure, and cost distributions.

Exit:

- one job has an explainable critical path;
- reload reconstructs authoritative state;
- duplicate and reordered events do not regress it;
- cancellation, provider, storage, corruption, and restart canaries all have
  direct accepted production evidence within explicit spend/runtime ceilings;
- a compact redacted projection makes cold, hot, and failure results comparable
  without committing prompts, workflows, private paths, hook details, or
  secrets; and
- no validation run leaves an orphaned Pod or synthetic storage state.

### Milestone 1 — Preflight, GPU recommendation, and rental confirmation

- Implement `PreflightReportV1` and `POST /api/preflight`.
- Produce deterministic blockers, warnings, unknowns, preparation plan, and
  volatile estimates.
- Rank viable GPUs under `balanced`, `cheapest`, `fastest`, and `manual` policy.
- Show provider, GPU, region, hourly price, total-cost range, timing range, cache
  coverage, and recommendation rationale.
- Add the default ten-second auto-start confirmation.
- Add Start now, Cancel, Choose another GPU, and Don't show again.
- Persist confirmation policy and related settings.
- Revalidate quote, capacity, region, and storage immediately before launch.
- Restart confirmation after a material recommendation change.
- Preserve hard spend and policy limits even when normal confirmation is hidden.

Exit:

- deterministic blockers never create a Pod;
- the reference workflow produces a stable manifest digest;
- recommendation decisions are explainable;
- no Pod is created before countdown completion or explicit Start now;
- revalidation catches material price, storage, or capacity changes; and
- ordinary healthy preflight adds little friction.

### Milestone 2 — Persistent visibility

- Ship the Cloud Jobs drawer.
- Reconstruct status after reload or reconnect.
- Add bytes, throughput, ETA range, spend, Pod, and billing state.
- Normalize phase-aware overall progress and terminal 100%.
- Show cold fallback and provider failures explicitly.

Exit:

- state returns within two seconds after reload;
- no active job appears frozen at a static early percentage;
- large downloads report measurable progress when the source exposes size; and
- canvas state is no longer the only source of truth.

### Milestone 3 — Revocable leases and billing closure

- Persist `JobLease` and renewal state.
- Add cooperative worker cancellation boundaries.
- Add independent idempotent provider termination.
- Record provider termination receipts.
- Reconcile non-closed leases on startup.
- Add runtime and dollar circuit breakers.

Completion status: backend PR #34 and extension PR #8 deliver every listed
mechanism. The automated matrix passes with 593 backend tests, 82 extension
Python tests with 3 skips, and 68 extension JavaScript tests. The bounded paid
RunPod matrix passed worker-boot cancellation and coordinator restart after
provider creation. Both exact Pods became provider-absent, both leases received
provider closure confirmation, no manual cleanup was required, and final RunPod
inventory was empty.

Exit:

- cancellation during every tested phase removes the exact Pod;
- a job cannot claim billing stopped without provider acknowledgement;
- worker or coordinator death creates no orphan beyond the reconciliation SLO;
- late callbacks cannot reopen terminal state; and
- cancelled work cannot publish unverified shared cache state.

### Milestone 4 — Fast trusted restore

- Add cache trust receipts.
- Skip full hot-path reads only for recently attested immutable objects.
- Add sampled background scrubbing and scheduled complete audits.
- Quarantine mismatches and preserve safe cold fallback.

Implementation status: complete. The signed trust-receipt and metadata/sample
fast path binds the exact manifest signature, artifact, volume, compatibility
contract, object generation, expiry, and audit policy. Private and sensitive
artifacts stay on full verification. Invalid, changed, expired, or audit-due
receipts also return to a complete digest read. A second signed sample runs while
materialization proceeds; failure removes the target before return, quarantines
the object through the worker fallback path, and degrades the volume. Paid
fresh-Pod evidence puts median hot preparation at 8.601% of median cold
preparation.

Exit:

- trusted hot restore performs no complete artifact read;
- corruption canaries are detected and quarantined;
- first-seen integrity guarantees are unchanged; and
- median hot preparation is no more than 25% of median cold preparation.

### Milestone 5 — Prepared workflow capsules

- Define canonical `PreparedStateManifest` and capsule schema.
- Add optional custom-node readiness manifests.
- Build reproducible signed custom-node and environment bundles.
- Promote capsules using measured reuse and cold-start cost.

Implementation status: complete. The canonical `comfy.workflow.capsule.v1`
closure, stable capsule digest, full-workflow preflight, confirmed submission,
artifact input and result transport, environment readiness identities, and
cooperative whole-workflow cancellation are implemented. First-rent workers now
build reproducible custom-node and Python environment bundles. The coordinator
binds them to exact profile and runtime identities before it signs the prepared
manifest. Later Pods restore both before ComfyUI starts. Paid fresh-Pod proof
passed on the exact pinned image. The accepted population created both bundles.
The accepted restore used a different fresh Pod, verified both complete digests
before job claim, emitted two cache hits and no new population event, returned a
digested workflow result, and left zero provider resources. A separate bounded
image-start incident is retained in the evidence with its cost and clean provider
closure. The redacted evidence is in
`docs/evidence/m5-workflow-capsule-evidence-2026-07-30.json`.

Exit:

- missing credentials, artifacts, disk, and incompatible nodes fail free;
- fresh Pods restore models and custom-node state;
- identical closures produce identical capsule digests; and
- undeclared dynamic behavior remains visible as uncertainty.

### Milestone 6 — Demand-weighted regional replication

- Collect regional demand and measured benefit.
- Produce shadow recommendations.
- Add budgets, TTLs, eviction, and single-flight copies.
- Replicate through provider object APIs without a GPU.
- Enable automation only after shadow accuracy is demonstrated.

Implementation status: complete. Confirmed paid placements record safe demand
by profile fingerprint, provider, and region. The read-only shadow advisor uses
that demand and the signed manifest projection to report a source, target region,
bytes, expected hits, expected saved GPU time and cost, copy cost, incremental
monthly storage cost, expiry, budget effect, and decision reasons. It suppresses
a recommendation when the target already has a compatible manifest. Reports keep
cold fallback explicit and make no provider mutation. A durable copy controller
now checks the exact current recommendation, approved source and target, known
copy cost, finite monthly budget, concurrency limit, and expiry. Shadow mode
requires confirmation. Automatic mode stays locked until mature unique shadow
recommendations reach the configured precision gate. Copy uses provider object
APIs and no GPU. Repeated requests return the same action. Due replica manifests
and unshared target objects can be removed without deleting source state.
Automatic target-volume creation, scheduled controller runs, full replica-aware
placement evidence, and regional-loss recovery are now implemented. Automatic
target creation has a separate regional single-flight lock and checks both the
replication budget and the complete storage budget. The dispatcher starts a
non-blocking authenticated controller cycle. It recovers stale copy claims,
checks provider truth, records the shadow report, expires replicas, removes an
empty automatic target after provider confirmation, and starts at most one copy.
Lost targets leave prepared placement, and preflight keeps cold fallback visible
while it can use compatible replicas in multiple regions. The accepted M6
evidence records six positive-byte paid observations across three regions,
3-of-3 validated shadow recommendations at 100% precision, two complete S3
copies, repeat-cycle suppression, prepared and cold choices together,
provider-confirmed regional loss, controlled one-day TTL expiry, source
preservation, restored configuration, zero active GPUs, and no test volume.

Exit:

- every automatic replica has a reason, budget, and expiry;
- monthly storage spend cannot exceed policy;
- duplicate copies are suppressed;
- scheduling uses compatible replicas without hiding cold fallback; and
- regional loss has a rehearsed recovery path.

All exits pass. The compact redacted evidence is in
`docs/evidence/m6-regional-replication-evidence-2026-07-30.json`.

### Milestone 7 — Production release gate

The release scope for this campaign is RunPod Japan (`AP-JP-1`) on the
H100 SXM 80 GB (`NVIDIA H100 80GB HBM3`), as agreed on 2026-09-08. Only this
region and GPU are release targets; other regions do not require paid canaries
for M7. Placement must remain in Japan, including cold fallback. If Japan has
no eligible capacity, execution must wait or fail with an explanation rather
than allocate in another region. Regional failure handling remains covered by
contract tests without requiring another regional deployment.

Cloud Offload graduates from beta only after:

- thirty consecutive full canary matrices pass across supported images and
  regions;
- there are zero orphaned Pods;
- cancellation and provider-acknowledgement SLOs pass;
- reload, reconnect, and event-ordering tests pass;
- deterministic preflight false-readiness is zero in the validation matrix;
- hot/cold acceleration meets target;
- corrupt cache and stale-manifest recovery pass;
- regional failure and cold fallback pass;
- every failure can be explained from a redacted support bundle; and
- GPU and storage spending remain within configured budgets.

The M7 controller treats one full matrix as the complete cold, hot,
cancellation, provider, storage, corruption, restart, stale-manifest, and
regional-fallback canary set for one declared image-region case. Cases rotate in
a stable order. The trailing 30-pass window must cover every declared public
worker profile, pinned image, region, and profile-region case. A single failed
matrix resets the consecutive count. Raw plans and detailed scorecards remain
under `.runlogs/`; only the finite redacted ledger is eligible for durable
release evidence.

## Success metrics and SLO candidates

These are product targets to validate and tighten with baseline data:

| Area | Target |
| --- | --- |
| Preflight | No known deterministic blocker reaches Pod creation. |
| Recommendation | Every choice has a persisted explanation and cost estimate. |
| Confirmation | No launch before Start now or countdown completion; mandatory safety overrides always win. |
| Reload | Current state reconstructs within two seconds on a healthy local coordinator. |
| Progress | Monotonic; successful terminal state is 100%; ETA carries confidence. |
| Cancellation | Exact Pod reaches provider-confirmed termination within the provider-specific SLO. |
| Orphans | Zero Pods remain beyond two reconciliation intervals. |
| Hot restore | Median preparation is at most 25% of alternating cold baseline. |
| Integrity | Corrupt or incompatible prepared state never reaches ComfyUI silently. |
| Replication | Automated storage spend never exceeds configured budget. |
| Supportability | A redacted journal explains every failed canary without raw server logs. |

## Non-goals and rejected shortcuts

- Replacing GPU rental with a preflight or storage product. Rental and execution
  remain the core promise.
- Guaranteeing volatile provider capacity, price, boot time, or bandwidth.
- Making confirmation a mandatory modal on every run after the user deliberately
  disables it within safety limits.
- Treating hourly price as total job cost.
- Relying on worker cooperation alone to terminate paid resources.
- Claiming billing stopped from local state without provider evidence.
- Racing byte ranges across multiple sources before basic byte telemetry and
  integrity semantics are mature.
- Executing partial graphs while dependencies arrive if that requires a
  distributed scheduler rewrite.
- Presenting probabilistic cache verification as full verification.
- Standardizing the entire ComfyUI custom-node ecosystem before Cloud Offload can
  reproducibly bundle its own supported profiles.
- Automatically replicating state before demand, benefit, retention, and budget
  policies are proven in shadow mode.
- Persisting live VRAM, Python process objects, allocator state, or CUDA graphs
  across unrelated workers.

## Decisions recorded

- GPU rental and execution remain the central product promise.
- Readiness, visibility, closure, and acceleration are additive trust promises.
- Preflight runs before paid provider mutation.
- Healthy preflight flows automatically into a short rental confirmation.
- Balanced GPU recommendation is the proposed default.
- Default confirmation auto-starts after approximately ten seconds.
- The user can start immediately, cancel, choose another GPU, or disable future
  ordinary confirmations.
- The same confirmation behavior is controlled through persistent settings.
- Hard spend and policy limits remain active when confirmation is disabled.
- Material recommendation changes force a fresh decision when required by
  safety policy.
- Total estimated job cost matters more than hourly price alone.
- One timing observation cannot change ranking. Two through four matched
  observations give medium confidence; five or more give high confidence.
- The coordinator event journal is the authoritative lifecycle record.
- Billing closure requires provider acknowledgement.
- Cache fast paths require explicit trust evidence and background integrity work.
- Prepared capsules begin as a Cloud Offload-owned format.
- Regional replication begins in shadow mode and is budget constrained.
- Production readiness is earned through continuous failure rehearsal, not one
  successful end-to-end run.

## Open questions

1. Which exact RunPod state and API response constitute authoritative billing
   closure, and what uncertainty must remain visible?
2. What price and estimated-cost deltas count as a material recommendation
   change?
3. Should `material_changes` rather than `always` become the long-term default
   confirmation setting after a user establishes history?
4. How should per-job storage and transfer cost be attributed when a replica
   serves many future jobs?
5. Which custom nodes can provide complete readiness declarations, and how should
   undeclared runtime downloads be sandboxed or detected?
6. What cryptographic and object-generation evidence is sufficient to skip a
   complete hot-path read for each supported storage backend?
7. What provider-specific cancellation and orphan-reconciliation SLOs are
   realistic?
8. When should a frequently used preflight manifest be promoted into a capsule?
9. What measured avoided-GPU-idle threshold justifies a regional replica?

## Delivery shape

This plan should ship as narrow, reversible changes behind versioned schemas and
feature flags:

1. job journal and benchmark harness;
2. read-only preflight;
3. recommendation and confirmation UI;
4. persistent Cloud Jobs UI and transfer telemetry;
5. leases, termination receipts, and reconciliation;
6. cache trust receipts and scrubber;
7. custom-node bundles and prepared capsules;
8. replication shadow mode; and
9. budgeted automatic replication and the production release gate.

Each step preserves the existing execution path and stateless fallback. No phase
requires waiting for the complete roadmap before delivering user value.
