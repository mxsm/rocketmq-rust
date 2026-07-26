# rocketmq-sre-control-plane

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-control-plane` is the Phase 00 composition root for cluster
onboarding and capability visibility. It serves the read-only HTTP API, stores
cluster state in PostgreSQL, and publishes the capability and telemetry
coverage views consumed by the separate AI SRE UI.

## Responsibilities

- Apply SQLx migrations and persist clusters, append-only capability snapshots,
  and append-only onboarding events.
- Drive the `Pending` through read-only ready, degraded, rejected, and
  offboarded lifecycle.
- Make repeated onboarding and handshakes idempotent.
- Offboard through a tombstone and identity revocation while retaining history.

## Phase 00 boundary

Effective cluster access is always `read_only`. The service contains no
RocketMQ Admin mutation client, approval workflow, Executor integration, or
automatic remediation path. `/readyz` requires successful database setup;
`/healthz` reports process liveness only.

## Validation

Run from `rocketmq-sre/` with PostgreSQL supplied by the development Compose
stack when exercising persistence:

```powershell
cargo check --locked -p rocketmq-sre-control-plane
cargo test --locked -p rocketmq-sre-control-plane
```

The tests cover API mapping, onboarding idempotency, capability aggregation,
degraded handshakes, and offboarding behavior.
