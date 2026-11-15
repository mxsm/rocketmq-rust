# Execution observations and recovery boundaries

An accepted request, an Agent effect result, a completed verification window,
and business recovery are four separate facts. A recorded `succeeded` execution
does not establish current business recovery. The current implementation has no
independent business recovery source and reports that fact as Unknown with
`source=not_observed`.

## Fixed read operations

| Operation | Result |
| --- | --- |
| `GET /v1/executions/{id}/observation` | Exact accepted identity, retained attempts, typed verification provenance, recorded timeline and handoff facts |
| `POST /v1/executions/lookup` | Read-only lookup of the current requester's original `{plan_id, plan_hash, precondition_hash, idempotency_key}` |
| `GET /v1/plans/{id}/executions?plan_hash=...` | Bounded accepted receipt discovery for one authorized immutable Plan/hash |

These reads recheck tenant, cluster and current authenticated identity. They do
not call an Executor or Agent, retry a submission, mint authority, acquire a
lease, change a fence, append an execution result or clear a quarantine. A
`not_observed` lookup is not proof that no request was accepted or no effect
occurred. A response loss must retain the original selector; generating another
key is not recovery. The UI persists only that selector under the authenticated
owner and scope, and uses read-only discovery after navigation or reload.

The safe projection omits signed requests, Grants, nonce, raw parameters, Agent
payloads and arbitrary stored reason text. Limits are 16 Plan steps, 64 attempts,
64 verifications, 256 timeline events and 20 receipt references. Each journal
JSON record is limited to 512 KiB, with an 8 MiB transferred-payload budget for
each of the three journal tables. Truncation or unavailable retained payload is
reported explicitly; it cannot turn a missing result into a successful step.
The five-second validity interval applies to the read snapshot, never to the
age or validity of the underlying source evidence.

## Verification configuration and source facts

The Executor optionally reads a private file from
`ROCKETMQ_SRE_EXECUTOR_VERIFICATION_PROFILES_FILE`. Its closed schema is
`rocketmq-sre.execution-verification-profiles.v1`, with at most 128 rules and
128 KiB. A rule selects an exact tenant, cluster, action, descriptor version and
target, and supplies `declared_target_version`, `policy_revision`,
`max_source_age_seconds` and `max_sample_gap_seconds` (each limit 1–300 seconds).
The stability window and total wait limit remain the immutable recognized
descriptor's `VerificationSpec`. A declaration is not a physical target version
observation, identity attestation, action qualification or execution authority.
An absent profile is Unknown and never defaults to a zero-duration healthy
window.

New proofs retain both resource and exact technical-SLI data clocks, collection
time, actual monotonic elapsed time, source age, sample gaps, condition IDs and
selected policy digest. Source age includes the captured age plus elapsed age;
a fresh collection/evaluation timestamp or a replacement Evidence ID cannot
renew old data. Missing, partial, future, repeated, out-of-order, delayed or
out-of-window observations cannot satisfy the window. Reads share one bounded
run deadline; cancellation and late responses cannot extend it. Existing
cluster health indicators are not aliases for unrelated target SLI conditions.
The current production adapter does not expose an independently observed target
software version or exact target SLI source, so those facts remain Unknown.

`window_proof=None` is old recorded history and keeps its old serialization and
hash. Present malformed, modified or unavailable proof is not reclassified as
legacy missing proof. A valid historical engineering proof carries
`provenance=synthetic_fixture`; it does not set physical target-version
verification or production qualification.

## Cancellation, compensation and deployment

Only a qualified known condition failure may enter the existing authorized
typed compensation path. Missing profile/version/source or canceled observation
is not false: before an effect it stops dispatch, and after a recorded effect it
requires manual coordination/quarantine without another automatic target write.
A compensation response loss or failure does not establish rollback success.
Recovery retains the original accepted identity and uses the existing fenced
read-only reconciliation rules; it does not create a new execution intent.

Admitted HTTP execution and startup reconciliation are tracked by the process
TaskGroup. Losing the HTTP response waiter does not drop the accepted operation.
Owner cancellation stops reads and new dispatch, while entered journal writes
drain. A sent mutation is awaited only until the earlier original mutation
deadline or owner deadline with settlement time reserved. Local timeout does not
mean remote cancellation. Storage failure during shutdown remains an observable
service failure; a hard shutdown budget cannot guarantee a database commit.
The workload-only Executor status exposes `shutdown_storage_failures_total`;
task destruction and confirmed durable settlement remain separate facts.

Upgrade the Executor before the Control Plane, or upgrade the pair during a
controlled stop. Old Executor binaries treat inconclusive verification as a
compensation trigger; a new Control Plane's truthful missing-source result must
not be interpreted by that old logic. Persisted legacy results and accepted
receipt readability are separate from mixed-version runtime compatibility.
Once new `Some(window_proof)` records have been written, keep a reader that
supports that schema. Older binaries use strict unknown-field rejection and may
not decode those new journal records. Backward readability of old `None` bytes
does not promise forward readability by an older binary. Never remove or strip
append-only proof to enable a downgrade, or treat invalid present proof as legacy
missing proof. Falling back to an older UI route is a separate decision from
rolling back a persisted-journal reader.

Normal read-only acceptance fixtures are explicitly terminal Synthetic history.
They contain no current preparation proof, approval/Grant, runnable accepted
work or real target effect. Real IdP revocation, physical target qualification,
independent business recovery and long-running native lifecycle qualification
are not established by those fixtures.
