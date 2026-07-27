# Phase 02 Alert Correlation and Notification

P2-04 turns authenticated alert-like inputs into durable, tenant-scoped
Incidents. The implementation remains an operations metadata plane: it can
record alerts, notes, ownership, topology and notifications, but it cannot
change RocketMQ resources.

## Ingestion surface

The public Control Plane exposes these authenticated endpoints:

| Endpoint | Purpose | Request bound |
| --- | --- | ---: |
| `POST /v1/integrations/alertmanager/events` | Alertmanager webhook v4 | 256 KiB, 1–128 alerts |
| `POST /v1/integrations/events` | Kubernetes, health, operator, inspection, deployment and synthetic events | 64 KiB |
| `POST /v1/integrations/webhook/test` | Queue one existing notification target test | 16 KiB |
| `GET /v1/incidents/{id}/timeline` | Read the append-only incident timeline | repository bounded |
| `POST /v1/incidents/{id}/notes` | Add a bounded, sanitized operator note | default JSON bound and 2,048 characters |
| `GET /v1/incidents/{id}/topology` | Read the bounded impact graph | 128 nodes, 256 edges |
| `GET /v1/clusters/{id}/health` | Read active incident health | one cluster |

The normal JWT tenant and cluster authorization runs before ingestion. The
Alertmanager payload must use webhook version `4`; labels, annotations,
identifiers, timestamps and collection sizes are validated before conversion
to `AlertEvent`. Credential-like label names, message bodies, control
characters and unbounded text are rejected. Arbitrary Alertmanager annotation
bodies are not copied into notifications or Incident summaries.

The provider-neutral endpoint accepts every non-Alertmanager `AlertSource`:
`kubernetes_event`, `health_probe`, `operator_query`, `inspection`,
`deployment`, and `synthetic_probe`. All sources are normalized through the
same canonical event builder.

## Deterministic correlation

Each accepted event is sealed with a deterministic source-event identifier,
fingerprint and five-minute window. The first-pass key is:

```text
tenant + cluster + resource kind/key + symptom family + bounded window
```

PostgreSQL advisory transaction locks serialize that key. A duplicate source
event reuses its deterministic occurrence identifier, so webhook or worker
retries cannot inflate `occurrence_count`.

If no exact active Incident exists, the second pass evaluates the current
bounded topology graph. It covers the required RocketMQ paths:

```text
Topic → Queue → Broker → Store
Broker → Controller → Node → Pod
ConsumerGroup → Connection → Broker
```

Candidate selection is stable: exact non-terminal match, nearest topology
match, latest update, then stable Incident identifier. Tenant and cluster are
always part of repository predicates and can never be crossed by topology
correlation.

Severity is monotonic for an active Incident and `last_alert_at` only moves
forward, so an out-of-order event cannot downgrade current state. Deployment,
configuration, certificate and traffic-change Evidence from the preceding
30 minutes is copied into the append-only timeline using deterministic event
identifiers.

Phase 00 terminal-state protection remains intact. If the same fault recurs
after an Incident becomes `resolved` or `escalated`, P2-04 creates a new
Incident and records an immutable `recurrence` relation. It never transitions
the terminal aggregate back to an active state.

## Owner routing

Owner resolution uses this precedence:

1. bounded `owner`, `team`, or `on_call` event label;
2. an active `on_call_owners` exact resource selector;
3. resource-kind wildcard, such as `broker:*`;
4. cluster wildcard `cluster:*`;
5. `unassigned`.

Selectors can be tenant-wide or cluster-specific. `target_ids` associate a
route with configured notification targets. If no selector exists, enabled
tenant/cluster notification targets are used while the Incident remains in
the unassigned queue.

The schema deliberately supports a future CMDB adapter without making the
runtime depend on a CMDB client: an adapter only needs to maintain versioned
`on_call_owners` rows.

## Transactional notification outbox

Incident creation/correlation and notification enqueueing share one database
transaction. The unique delivery key is derived from Incident, target,
severity and alert status. A repeated alert therefore cannot enqueue a second
equivalent delivery.

Supported channels are:

- `signed_webhook`: sends a bounded JSON document containing only delivery
  identifier, Incident identifier, sanitized summary and UI deep link;
- `email`: deterministic delivery mock for Phase 02;
- `pager`: deterministic delivery mock for Phase 02.

Signed webhooks require HTTPS except for loopback development endpoints, do
not follow redirects, and reject URL credentials. Secrets are referenced as
`env:VARIABLE_NAME`; the database never stores secret material. The worker
claims at most 16 rows, recovers stale claims, retries a delivery at most four
times with a bounded schedule, and records the result in the Incident
timeline. Delivery failure does not roll back or delete the Incident.

`NotificationOutboxWorker` runs in the Control Plane `ScheduledTaskGroup`.
Shutdown cancellation and task joining are therefore owned by the same
`ServiceContext` as the HTTP service; no detached Tokio tasks are introduced.

## Streaming and restart behavior

Every correlation, operator note and notification result is appended to the
durable workflow/event tables. Incident changes are also published through
the existing authenticated `/v1/events/stream` SSE surface. SSE is a progress
hint, not the source of truth: reconnecting clients reload the Incident and
timeline from PostgreSQL.

The following identities make retries and restarts safe:

- canonical alert ID: deterministic from tenant, source and source event;
- occurrence timeline ID: deterministic from alert and Incident;
- change timeline ID: deterministic from Incident and Evidence/change event;
- notification delivery key: unique per tenant and delivery semantics;
- recurrence relation: unique per tenant, cluster and Incident pair.

## Verification

Run all commands with Cargo directories outside the system drive:

```powershell
$env:CARGO_TARGET_DIR='G:\rocketmq-sre-phase2-cargo-target'
$env:CARGO_HOME='G:\rocketmq-sre-phase1-cargo-home'
$env:TEMP='G:\rocketmq-sre-phase2-temp'
$env:TMP='G:\rocketmq-sre-phase2-temp'

cargo test --manifest-path rocketmq-sre/Cargo.toml --locked `
  -p rocketmq-sre-core correlation
cargo test --manifest-path rocketmq-sre/Cargo.toml --locked `
  -p rocketmq-sre-control-plane --lib

$env:ROCKETMQ_SRE_TEST_DATABASE_URL = `
  'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre'
cargo test --manifest-path rocketmq-sre/Cargo.toml --locked `
  -p rocketmq-sre-control-plane 'alerting::repository::tests::postgres_' `
  -- --ignored --nocapture
```

The PostgreSQL tests prove duplicate idempotency, required cross-component
topology correlation, out-of-order monotonicity, recurrence without terminal
state reversal, tenant/cluster isolation, durable operator notes, bounded
outbox retry and Incident survival after delivery failure.
