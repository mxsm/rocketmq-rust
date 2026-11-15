# Action readiness and exact preparation

The Incident workspace and immutable Plan view use a Control Plane projection
to explain why an action can advance or remains blocked. The projection reads
stored facts; opening or refreshing it does not call an Execution Agent,
collect Evidence, create a Plan, approve a Plan, or dispatch work.

## Operations and evidence

| Operation | Meaning |
| --- | --- |
| Confirm diagnosis | Append confirmation of the current complete, model-assisted diagnosis basis. This is separate from action approval. |
| Prepare readonly preconditions | Explicitly collect one bounded observation through the existing Executor and Execution Agent read path. |
| Create draft | Create an immutable Plan bound to the exact current diagnosis, parameters and Evidence. |
| Approve | An authorized person other than the Plan creator reviews the immutable Plan and obtains an expiring service-issued grant. |
| Submit | Request first execution admission; the server rechecks current readiness, policy, approval and execution authority. |

The first three affordances come from the server's operation-specific facts.
The execution summary and capability coverage matrix grant no authority. A
missing independent execution qualification can prevent submission while still
allowing an authorized readonly preparation, draft, or human review.

The nine fixed stages are:

| Stage | Source and limit |
| --- | --- |
| `descriptor` | Exact action catalog entry, descriptor version and validated parameters. |
| `handler` | A valid bound observation from the configured Agent read path; unobserved registration stays unknown. |
| `enabled` | Known local transport configuration and observed handler availability. |
| `target` | Exact operator-authorized target scope and actual target observations; a scope declaration alone does not establish execution support. |
| `evidence` | Exact linked Evidence IDs and content hashes, checked age, preparation origin and candidate binding. |
| `policy` | Server policy, diagnosis basis, quarantine, active changes and maintenance window. |
| `approval` | Applicable immutable Plan and current service-issued approval grant. |
| `qualification` | Independent qualification for the action, target and software version. No source is currently wired, so this stays unknown. |
| `compensation` | Observed compensation support. A descriptor's declared method alone is insufficient, so unobserved support stays unknown. |

States are `satisfied`, `blocked`, `unknown`, and `not_applicable`, with closed
reason codes and applicable source references and times. The logger handler's
readonly result can report `ready=true`, `logger_level_applied=false`, and
`ttl_restore_scheduled=false`: the latter two describe current effects, not
failed preparation conditions. They also do not prove write or rollback support.

## Typed forms and immutable review

The UI models descriptor version `1.0.0` of the nine existing wired handlers:
Logger TTL, allowlisted Broker/Topic/Group configuration, Proxy scale-one,
restart-one and image-canary-one, telemetry restart and credential overlap.
Every field is closed and bounded; patches require explicit selection and a
credential input accepts only a managed reference. The UI adds no handler,
permission, qualification source or arbitrary JSON/command input.

Plan review shows the saved target, safe requested fields, CAS assumptions,
server risk, verification policy and compensation declaration. It does not
invent missing before-values or interpret a declaration as observed support.
Sensitive references are masked. Older versions or unrecognized parameters
retain their metadata and deep links as read-only history, without raw JSON.
An unexpired human approval record is labelled separately from execution
authority; actual Plan/record deadlines update the display and close local
dispatch controls without requesting a replacement grant. The server still
enforces actor separation, grant verification and final admission.

## API

```text
POST /v1/incidents/{incident_id}/action-readiness
POST /v1/incidents/{incident_id}/execution-preconditions
```

The readiness request identifies `cluster_id`, `diagnosis_revision_id`,
`action_id`, `descriptor_version`, exact `resource`, and structured `parameters`.
An optional `precondition_evidence_id` selects one immutable preparation. An
existing Plan additionally requires both `plan_id` and `plan_step_id`; candidate
fields must match that step. The body is bounded to 128 KiB and parameters to
32 KiB. Unknown request fields are rejected.

An authorized in-scope blocked or unobserved candidate returns a projection,
including for a Viewer who has no privileged operation affordances. Invalid
selectors and bounds are rejected; foreign tenant, cluster, Incident, Plan or
Evidence access follows the existing safe authorization/not-found behavior.
Unvalidated parameters are not echoed.

The existing preparation request and required success fields remain available.
Its response adds optional `readiness`. It still requires an explicit authorized
request and creates actual immutable Evidence; it is not a background refresh.
The new Contracts types, exported schemas and generated OpenAPI/TypeScript
definitions are the source of the wire shape.

## Authorize the exact physical target

Configure `ROCKETMQ_SRE_ACTION_TARGET_SCOPE_JSON` privately on the Control Plane.
An omitted configuration contains no rules and denies new automatic preparation
before any Agent call. Each rule binds the authenticated tenant and cluster,
exact action and descriptor version, logical Incident resource, and exact
execution-target literal. For example, these identifiers and addresses are
illustrative deployment inputs:

```json
{
  "schema_version": "rocketmq-sre.action-target-scope.v1",
  "rules": [
    {
      "tenant_id": "11111111-1111-4111-8111-111111111111",
      "cluster_id": "22222222-2222-4222-8222-222222222222",
      "action_id": "observability.logger_level_ttl.v1",
      "descriptor_version": "1.0.0",
      "incident_resource": "broker/broker-a",
      "target": "broker/broker-a.internal:10911",
      "lookup": {
        "kind": "logger",
        "component": "broker",
        "logger": "rocketmq_broker::processor"
      },
      "security_revision": "1"
    }
  ]
}
```

The document is limited to 128 KiB and 128 rules. Unknown fields, duplicate
matching keys, wildcards, URL targets, and noncanonical revisions are rejected.
`security_revision` is a canonical positive `u64` decimal string. Resource and
target literals are limited to 512 bytes. Parsing errors and Debug output do
not reveal the private rule list.

The required closed `lookup` declares the actual selectors used by that action's
readonly driver. These selectors must match the validated candidate parameters
exactly before an Agent request, and again when consuming a prepared proof.
Matching the target label alone is insufficient when a driver reads by a Broker,
Topic, group, namespace, Pod, workload, container, pipeline or credential-set
parameter. The action-specific variants are defined in
[target scope](../crates/rocketmq-sre-control-plane/src/supervised_execution/target_scope.rs).
Missing, incompatible or mismatched lookup declarations cannot enable preparation.
No default namespace or physical address is inferred from an old target alias.
Multiple explicitly declared lookups can share a target; a repeated full matching
key is rejected even if only its security revision differs.

| Existing action | Lookup kind | Required exact selector fields |
| --- | --- | --- |
| Logger level with TTL | `logger` | `component` (`broker`), `logger`; the Broker address remains the exact target literal. |
| Broker configuration patch | `broker` | `broker`, the actual address used by the driver. |
| Topic configuration patch | `topic` | `topic`. |
| Subscription Group patch | `subscription_group` | `group`. |
| Proxy scale-out | `workload` | `namespace`, `workload`. |
| Proxy restart | `pod` | `namespace`, `pod`. |
| Proxy image canary | `canary` | `namespace`, `workload`, `container`. |
| Telemetry collector restart | `telemetry` | `namespace`, `pod`, `pipeline`. |
| Credential rotation | `credential_set` | `credential_set`. |

Lookup string fields are bounded to 512 bytes. The Topic and group drivers still
resolve routes through the Agent's privately configured NameServers; credential
validation probes use the Agent's private target configuration. This declaration
does not independently attest those deployment associations or grant their
production qualification. Request parameters cannot replace that private probe
configuration.

Configuration is read at startup; restart the Control Plane after changing it.
Only the matched rule, including its lookup and intentional security revision, enters the binding
digest. Reordering rules or adding unrelated rules does not invalidate a
preparation. Removing or changing the matching rule, including its revision,
invalidates the old binding on new progress. Operators should advance the
revision when intentionally replacing a security-sensitive target association.
The declaration authorizes this read association; independent network identity,
software-version, execution and rollback qualification remain separate.

## Preparation integrity and lifetime

New preparation Evidence uses the CP-owned content schema
`rocketmq-sre.action-preparation.v1`. It contains the unchanged original
`AgentReadResult` plus the exact Evidence ID and candidate binding: authenticated
scope, Incident and diagnosis revision, action and descriptor, target, canonical
parameters digest, diagnostic Evidence IDs/hashes, policy version, original
preparer, target-rule digest, preparation time and deadline.

The existing stable `ROCKETMQ_SRE_GRANT_SIGNING_KEY` signs a separate fixed
preparation-origin purpose. This origin seal authenticates the CP collection
path; it is not an Agent signature, approval or dispatch grant. Generic Evidence
ingress cannot create this CP-owned content namespace. Consumers verify origin
and exact binding before using a preparation. The UI and model summaries omit
the origin seal and raw Agent conditions. Keep the signing key stable across
restarts; a different key cannot validate preparations signed by the old key.

The original preparer's binding remains the Plan creator's basis when another
person approves the Plan. The current requester's authorization is checked
separately, preserving both non-self approval and the exact original evidence.

Effective Evidence age is recorded source age plus elapsed time, checked without
overflow. The Agent observation limit is the smaller of policy age and 120
seconds; the current diagnostic policy limit is 900 seconds. Future, incomplete,
invalid or stale evidence cannot advance a new automatic candidate. Selecting a
new same-resource Evidence record never renews the original bound proof.

CP-generated preparation and deadline timestamps use PostgreSQL microsecond
precision while the original Agent observation retains its nanoseconds inside
the sealed content. Deadlines are conservatively rounded down. A readiness view
lasts at most 30 seconds and is further bounded by evidence, policy and applicable
Plan/approval deadlines. Refreshing a view does not extend those deadlines.
Expired, late, principal-changed or edited-candidate responses cannot enable UI
operations. The server also rechecks current revision and a fresh clock after
acquiring the Incident row lock, so a lock wait cannot extend eligibility.

## Upgrade and execution admission

New automatic preparations, Plans, approval progression, Critic promotion and
first execution admission require the current confirmed complete model-assisted
diagnosis and the applicable exact fresh preparation. Starting a new diagnosis
revision invalidates progression of older unfinished automatic candidates.
Previously approved Plans that have not yet been submitted also receive the
new first-admission readiness check.

Independent action/target qualification and observed compensation are currently
unknown. New first submission therefore fails closed with
`action_readiness_blocked`, including after human approval. Completing the
separate qualification source and target-version validation is required before
that state can advance; enabling a handler or changing UI state cannot supply
those facts.

Existing plain Agent Evidence remains readable history; it cannot stand in for
a new bound preparation. Immutable Plan strings and hashes are not rewritten.
Already accepted execution retries retain their existing reconciliation path,
and finished receipts remain immutable history. They do not re-enter first
admission. Manual-only Runbooks retain their existing non-executable response
and do not require an automatic Agent preparation.

See [Plan, policy and approval](phase03-plan-policy-approval.md),
[Executor and Agent fencing](phase03-executor-agent-fencing.md), and
[execution contracts](phase03-execution-contracts.md) for the remaining execution
boundaries.
