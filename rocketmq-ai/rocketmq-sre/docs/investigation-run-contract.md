# Investigation run contracts and persistence (D13–D14)

D13 defines serializable contracts and pure validation for bounded, read-only
investigations. A run is independent of the existing `Investigation` and
`Incident` lifecycles. Their enums and existing HTTP behavior remain unchanged.

D13 introduced the contracts and pure validation. D14 registers authenticated
creation, snapshot, command and durable-event HTTP operations, backed by
PostgreSQL transactions and fenced worker ownership. Managed recovery accounts
for interrupted work without dispatching it again. New runs remain `queued`:
the D15 diagnostic dispatcher and D16 UI controls are not delivered here.
The existing workspace continues to display saved manual investigations.

## Published components

| Rust root / OpenAPI component | Schema artifact | Purpose |
| --- | --- | --- |
| `InvestigationRun` | `investigation-run.schema.json` | Run identity, scope, lifecycle and bounded progress |
| `CreateInvestigationRunRequest` | `create-investigation-run-request.schema.json` | Typed creation input |
| `CreateInvestigationRunResponse` | `create-investigation-run-response.schema.json` | Creation result |
| `InvestigationCommandRequest` | `investigation-command-request.schema.json` | Typed command, concurrency and idempotency input |
| `InvestigationCommandReceipt` | `investigation-command-receipt.schema.json` | Command acceptance or replay result |
| `InvestigationStep` | `investigation-step.schema.json` | One bounded read or assessment step |
| `InvestigationStepResult` | `investigation-step-result.schema.json` | Attempt result and resource accounting |
| `HypothesisAssessment` | `hypothesis-assessment.schema.json` | Evidence-bound hypothesis assessment |
| `InvestigationRunFailure` | `investigation-run-failure.schema.json` | Closed, sanitized failures |
| `InvestigationRunEvent` | `investigation-run-event.schema.json` | Scoped append-only event with an independent decimal sequence |
| `InvestigationRunEventPage` | `investigation-run-event-page.schema.json` | Bounded contiguous replay or explicit snapshot recovery |

`rocketmq-sre-contracts` owns all fields. JSON Schema describes serialization;
cross-field, scope, budget and lifecycle rules also require the pure Rust
validation functions. Deserializing a request does not authorize work.
Neither a run nor an assessment grants execution eligibility. Target mutation
continues through the existing supervised approval and execution boundary.

Run `revision`, `control_epoch` and command `expected_revision` use checked
`u64` counters encoded as decimal strings. They are independent run counters,
not `DiagnosisRevisionId` values. Clients must keep them as strings so values
above JavaScript's exact integer range survive transport without rounding:

```typescript
import type { InvestigationCommandRequest, InvestigationRun } from "./api/types";

const revision: InvestigationRun["revision"] = "9007199254740993";
const concurrency = {
  expected_revision: revision,
} satisfies Pick<InvestigationCommandRequest, "expected_revision">;
```

An exact, currently authorized receipt replay may be returned after the run
deadline without reapplying its effect. New resume, steering and step admission
must reject expired deadlines. Pause and cancel remain available after the
deadline to stop outstanding work; their drain states do not claim that
already admitted reads have disappeared.

## Control, drain and accounting

Queued work starts in `running`. Pause enters `pausing` while admitted attempts
remain and reaches `paused` only after settlement. Cancel advances the control
epoch, enters `cancelling` when needed, and reaches `cancelled` after settlement.
No new attempt is admitted in either drain state. A cancellation acknowledgement
means the worker has accounted for its owned read; it does not assert that a
remote RocketMQ RPC was withdrawn.

Resume accepts only `paused` work. A `needs_evidence` run can receive a valid
steering refinement, become `paused`, and then explicitly resume. Steering may
narrow the existing subject/time scope, remove or reprioritize existing packs,
and supply authorized saved Evidence. It cannot add authority, extend the
deadline or reset the budget. Scope resources identify investigation subjects;
they are not canonical Evidence paths or target URLs.

Every admission reserves from one shared run budget. Retries count toward
`max_steps` as additional admitted attempts and also obey the per-step attempt
limit. The first version permits at most 12 tool-query admissions, including
retries, and an independent model-call limit of at most 8. A budget of 12 reads
plus one explanation is valid; the two limits are not summed into a smaller
shared call count. `budget.max_query_retries` is a run-wide tool-query retry
limit from 0 to 2. Every `collect_evidence` attempt after its logical step's first
attempt consumes this allowance, across all packs. For example, one retry on
each of two collection steps consumes both retries; a third collection retry
is rejected even when the 12-read budget still has capacity. The per-step
attempt limit also applies. Model-call retries use their independent call
budget and the per-step attempt limit.

Settled read/model calls remain conservatively charged by
admission even when a failed worker reports zero actual calls. Actual worker
costs remain available separately; exact replay does not charge another
admission. Evidence and output reservations settle to their actual costs,
including costs of superseded results. Evidence accounting counts distinct observed
references, including supplied references, once. D14 calculates that
distinct-reference delta inside the same locked persistence decision as result
acceptance, so concurrent results sharing an ID can both settle without a
double charge. The original submitted result is retained in a separate
append-only receipt; replay compares that original report, including its costs
and timestamps, while the run stores the canonical distinct-reference charge.
An exact duplicate does not charge again; a conflicting duplicate is rejected.

Logical step identity includes its scope, exact pack/version and closed work
kind. It excludes attempt IDs, reservation sizes and control epochs, so changing
those fields cannot rerun already successful work. Rule evaluation and
explanation results may reference only their declared Evidence inputs.
Collection results require service-verified Evidence scope bindings.

A worker whose ownership lease has expired cannot settle or admit work with
its old fence. Recovery first closes admission, then records a typed
`worker_lease_expired` attempt interruption. This is neither a measured result
nor a zero-cost failure: no `actual_cost` or invented Evidence is attached.
The unknown attempt's reservation continues to occupy the shared budget. An
explicit retry consumes additional quota. Interrupted attempts no longer keep
pause/cancel draining, and a late result from that attempt is rejected.

Trusted authorization and Evidence contexts are not serializable request DTOs.
D14 must construct them from current identity claims, compiled pack/requirement
admission, actual Evidence metadata and authorized related-resource provenance.
An assessment must also bind the saved diagnosis revision and current run
scope/epoch. A root-cause conclusion requires a non-partial saved `fault`
revision; a healthy conclusion requires a non-partial saved `healthy` revision.
Partial, unsupported or inconclusive revisions cannot be promoted into a
conclusive `completed` run merely because a referenced Evidence ID is visible.
An explicit inconclusive assessment may still record missing evidence or no
root cause. Any future cross-pack conclusive reassessment needs additional
typed service proof of evidence sufficiency before relaxing this gate.
Hydrated snapshots retain historical assessment references only from the run's
observed Evidence pool. The current terminal assessment must reference the
current active pool; steering cannot silently reactivate an old observation.
Supporting, counter and missing references remain traceable; the assessment
remains advisory, does not establish causality from timestamps, and grants no
execution permission.

## Registered D14 transport

The public Control Plane router registers these operations. They change only
the persisted read-only investigation lifecycle. They do not expose worker
claim, arbitrary collection, shell, raw request or target execution endpoints.
The fixed read-only SDK and existing UI receive no new mutation methods or
control buttons.

| Operation | Contract | Success mapping |
| --- | --- | --- |
| `POST /v1/investigations/{id}/runs` | `CreateInvestigationRunRequest` → `CreateInvestigationRunResponse` | `201` for a newly recorded queued run; `200` for exact authorized creation replay |
| `GET /v1/investigation-runs/{id}` | `InvestigationRun` | `200` for the authorized current snapshot |
| `POST /v1/investigation-runs/{id}/commands` | `InvestigationCommandRequest` → `InvestigationCommandReceipt` | `202` when a newly accepted pause/cancel must drain; `200` for immediate application or exact authorized receipt replay |
| `GET /v1/investigation-runs/{id}/events?after=0&limit=50` | `InvestigationRunEventPage` | `200` for contiguous durable events or an explicit snapshot reset |

A replay returns the previously accepted receipt's semantic result without
reapplying the command. Its `200` means that an existing receipt was read; it
does not mean that a new command was accepted or that all admitted work has
finished. A client must not replace a newer run snapshot with an older receipt's
status or applied revision. Path/body identities must match. The original
creation response and command receipt are persisted atomically with effects.

Every public request authenticates the current identity. Tenant and cluster
claims are checked against the saved run and investigation. Creation and
commands also require an existing diagnose/operator role; another actor cannot
replay the original actor's receipt. Saved subject strings are parsed into
exact resource identities, including all key components, rather than matched
by substring. An unrecognized constrained subject cannot become a cluster-wide
run. Steering only narrows the initial scope and requires real saved revision
references and same-tenant/cluster Evidence metadata. Related resources need a
saved revision on the already bound investigation subject; narrowing a broad
parent to a new subject does not supply that relationship.

Evidence `time_range` must fit within the requested observation scope.
`observed_at` is collection time and may legitimately follow the historical
window's end; it is not a collection deadline. Metadata authorization does not
convert freshness or retention into permission, or claim that stale Evidence
is sufficient for a new conclusion. Diagnostic qualification remains separate.

Event sequences are lossless decimal strings independent of run revision and
control epoch. `after` is an exclusive cursor and `limit` is between 1 and 100.
Expired, ahead or retained-gap cursors return `snapshot_required=true`, a typed
`recovery_reason`, no events, and the current authorized snapshot. Resume after
`next_sequence`; never silently skip a gap. Event payloads carry bounded IDs
and lifecycle facts, not source bodies, prompts or worker lease tokens.
PostgreSQL event and worker-fence sequences use checked nonnegative `BIGINT`
counters. Storage exhaustion rejects the whole transaction with
`counter_exhausted`; snapshots and receipts cannot partially advance. Run
revisions and control epochs retain their independent full `u64` string counters.

The following mapping applies to the dedicated `InvestigationRunFailure` codes;
it does not change the existing public error enum or envelope:

| Status | Dedicated failure codes |
| --- | --- |
| `400` | `invalid_input`, `invalid_assessment` |
| `403` | `unauthorized`, `scope_mismatch`, `pack_not_allowed` |
| `404` | `step_not_found`; missing run/resource lookup remains a D14 transport concern |
| `409` | `invalid_state_transition`, `revision_conflict`, `idempotency_conflict`, `budget_exceeded`, `deadline_exceeded`, `step_identity_conflict`, `step_already_completed`, `step_in_flight`, `attempt_mismatch`, `result_conflict`, `counter_exhausted` |

An invalid deadline in a new creation request is invalid input (`400`). A
validly created run whose deadline or remaining budget now forbids admission
has an admission conflict (`409`). Missing or invalid HTTP authentication is
handled by transport authentication (`401`), before the trusted pure
context is constructed. Error text stays sanitized and does not echo directives,
credentials, source content or model output.

## Generation and compatibility

The generation chain is:

```text
Rust JsonSchema + serde
  → eval schema-export
  → committed schemas/*.schema.json
  → scripts/openapi/investigation_run.mjs
  → phase05 OpenAPI components
  → ui/src/api/generated.d.ts
  → ui/src/api/types.ts aliases
```

The OpenAPI helper embeds each exported root and namespaces its `$defs` under
`<Root>__<Definition>`. It rewrites local references, removes obsolete generated
definitions for that root and leaves existing components and paths intact.
It does not duplicate Rust DTO fields in JavaScript. Existing `Investigation`,
`Incident`, Evidence and supervised-execution components keep their names.

Both `scripts/generate_phase5_openapi.mjs` and the UI's `generate:api` pipeline
include the component helper and D14 transport registration. The UI runs them as a separate generation step after the
conversation extender, including when that extender detects an existing
conversation contract and exits early. `check:api` remains a read-only check
against the committed OpenAPI document.

Run the following in the repository's Docker development environment from the
standalone SRE workspace; keep temporary export output outside the checkout:

```bash
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- /tmp/sre-schemas
cargo test --locked -p rocketmq-sre-eval committed_schema_fixtures_match_public_contracts
node --test scripts/openapi/investigation_run.test.mjs
cd ui
npm run generate:api
npm run check:api
npm run build
```

Copy only deliberately changed schema artifacts from the temporary export.
The eval fixture test compares parsed JSON with the Rust-derived schema and
requires an artifact for every exported root. Generator tests cover reference
closure, stale-definition removal and repeatable component-only integration.
No dependency or lockfile changes are required.

## Ownership and remaining runtime work

The service binds authenticated scope before accepting operations, and the
repository serializes the pure decision, expected revision, receipt, snapshot
and event writes in one transaction. Worker transitions require current leased
ownership and fencing. The recovery scan runs with a bounded batch under the
existing `ChildServiceContext` scheduled-task owner; process shutdown cancels
and awaits that owner's work. Recovery never dispatches queries, grants user
authority or reports unknown consumption as measured cost.

D15 must implement registered read-only collection and assessment dispatch,
including actual metadata/quality proof, cancellation settlement and recovery
coordination at the source boundary. D16 may add UI controls against these
versioned operations. Persistence and an accepted control receipt alone do not
claim that a diagnostic worker has run or that a conclusion was established.
