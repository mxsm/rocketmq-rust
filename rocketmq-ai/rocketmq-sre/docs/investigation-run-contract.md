# Investigation run contracts (D13)

D13 defines serializable contracts and pure validation for bounded, read-only
investigations. A run is independent of the existing `Investigation` and
`Incident` lifecycles. Their enums and existing HTTP behavior remain unchanged.

This is a contract delivery. There are no registered run HTTP routes, database
migrations, durable coordinator, recovery worker, model or tool dispatch, or UI
commands in this change. D14 must implement the durable runtime and transport
before create, pause, resume, cancel, or steering operations can be called.
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
references, including supplied references, once. D14 must calculate that
distinct-reference delta inside the same serialized persistence decision as
result acceptance. An exact duplicate result does not charge again; a
conflicting duplicate is rejected.

Logical step identity includes its scope, exact pack/version and closed work
kind. It excludes attempt IDs, reservation sizes and control epochs, so changing
those fields cannot rerun already successful work. Rule evaluation and
explanation results may reference only their declared Evidence inputs.
Collection results require service-verified Evidence scope bindings.

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

## Proposed D14 transport (not registered)

These routes and status mappings describe the intended integration. D13 adds
none of them to `paths`, the HTTP client or the UI.

| Proposed operation | Contract | Proposed success mapping |
| --- | --- | --- |
| `POST /v1/investigations/{investigation_id}/runs` | `CreateInvestigationRunRequest` → `CreateInvestigationRunResponse` | `201` for a newly recorded queued run; `200` for exact authorized creation replay |
| `GET /v1/investigation-runs/{run_id}` | `InvestigationRun` | `200` for the authorized current snapshot |
| `POST /v1/investigation-runs/{run_id}/commands` | `InvestigationCommandRequest` → `InvestigationCommandReceipt` | `202` when a newly accepted pause/cancel must drain; `200` for immediate application or exact authorized receipt replay |

A replay returns the previously accepted receipt's semantic result without
reapplying the command. Its `200` means that an existing receipt was read; it
does not mean that a new command was accepted or that all admitted work has
finished. A client must not replace a newer run snapshot with an older receipt's
status or applied revision. D14 must validate path/body identities and preserve
the original creation response and command receipt atomically with their effects.

The following mapping applies to the dedicated `InvestigationRunFailure` codes;
it does not change the existing public error enum or envelope:

| Proposed status | Dedicated failure codes |
| --- | --- |
| `400` | `invalid_input`, `invalid_assessment` |
| `403` | `unauthorized`, `scope_mismatch`, `pack_not_allowed` |
| `404` | `step_not_found`; missing run/resource lookup remains a D14 transport concern |
| `409` | `invalid_state_transition`, `revision_conflict`, `idempotency_conflict`, `budget_exceeded`, `deadline_exceeded`, `step_identity_conflict`, `step_already_completed`, `step_in_flight`, `attempt_mismatch`, `result_conflict`, `counter_exhausted` |

An invalid deadline in a new creation request is invalid input (`400`). A
validly created run whose deadline or remaining budget now forbids admission
has an admission conflict (`409`). Missing or invalid HTTP authentication is
handled by D14's transport authentication (`401`), before the trusted pure
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
include the helper. The UI runs it as a separate generation step after the
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

## D14 implementation obligations

D14 must bind authenticated tenant and cluster claims before accepting a run or
command, persist idempotency decisions atomically with their effects, enforce
expected revisions, and retain pending pause/cancel work until admitted steps
drain. It must coordinate attempts, duplicate results, evidence ownership and
resource accounting across restart. The pure contracts describe those
decisions; they do not supply storage atomicity, request authorization or a
running scheduler. The proposed transport and status mappings above remain
design contracts until D14 implements and registers them.
