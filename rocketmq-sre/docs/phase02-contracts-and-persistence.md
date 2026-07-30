# Phase 02 contracts and persistence

P2-01 extends the read-only AI SRE domain without changing any RocketMQ
resource. The public contract remains `rocketmq-sre.api.v1`; compatible Phase
2 DTOs are additive and are published through the checked-in OpenAPI document.

## Domain contracts

The contracts crate provides strongly typed DTOs for:

- alerts, scoped resources, symptoms, deterministic correlation keys and
  incident relations;
- immutable topology snapshots;
- capacity forecasts, backlog ETA, anomaly baselines and change points;
- read-only what-if simulations and upgrade/DR readiness reports;
- notification targets, on-call ownership and idempotent delivery records;
- postmortem heads, immutable revisions and independently tracked action
  items.

DTOs contain tenant and cluster scope wherever a cross-cluster mix-up would be
unsafe. They do not contain model reasoning, credentials, message bodies,
complete ACL/TLS material or executable actions.

## PostgreSQL migrations

Phase 01 already owns migrations `0001` through `0017`, so Phase 02 begins at
`0018`:

- `0018_alert_correlation_notification.sql`
- `0019_forecast_simulation_readiness.sql`
- `0020_postmortem_action_items.sql`

The migrations are forward-only and additive. Existing `sre_incidents`,
`incident_timeline`, `evidence_snapshots`, `knowledge_items` and model records
are not rebuilt or renumbered. Postmortem content is append-only in
`postmortem_revisions`; the `postmortems` table only holds lifecycle state and
the current revision pointer.

`notification_outbox.delivery_key` is tenant-unique. Repeated delivery
requests therefore remain idempotent. Action items may enter `completed` only
when verification text or Evidence exists.

## Repository boundary

`Phase2Repository` is implemented by `PostgresRepository`. It stores typed
domain records, advances postmortem revisions under a row lock, rejects skipped
revision numbers, retains earlier revisions, and never calls a RocketMQ or
Kubernetes mutation API.

## Generated contracts

Regenerate the JSON Schema, Phase 02 OpenAPI document and UI client in this
order:

```powershell
$env:CARGO_TARGET_DIR = 'D:\BuildCache\rocketmq-sre-target'
$env:TEMP = 'D:\BuildCache\rocketmq-sre-temp'
$env:TMP = 'D:\BuildCache\rocketmq-sre-temp'
cargo +1.95.0 run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
node scripts/export_phase2_openapi.mjs
npm --prefix ui run generate:api
npm --prefix ui run check:api
```

The OpenAPI generator embeds Schemars definitions with component-scoped
references and preserves every Phase 01 public path. The live endpoint
`GET /v1/capabilities/phase2-contract` returns the effective access profile,
supported Phase 2 operations and `cluster_mutation_supported=false`.

## Compatibility acceptance

The migration acceptance sequence uses Docker PostgreSQL:

1. apply migrations `0001` through `0017`;
2. insert a legacy Incident and canonical Evidence row;
3. apply migrations `0018` through `0020`;
4. read the legacy rows and resolve the new postmortem, notification and
   forecast tables;
5. remove only the isolated temporary database.

The accepted result is one legacy Incident, one legacy Evidence row and all
new tables present.
