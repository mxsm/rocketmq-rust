# Phase 02 DiagnosticPack catalog

Phase 02 extends the eight Wave A packs with 18 Wave B packs and six Wave C
packs. All 32 packs execute deterministic, read-only Rust rules. YAML files
contain thresholds and time windows only; they cannot define executable rules
or enable mutations.

## Runtime behavior

- `full_registry()` is the production registry used by Orchestrator,
  Inspection, and Coverage.
- Every finding includes a stable pack ID/version, reason code, candidate root
  cause, supporting Evidence, optional counter-evidence, missing Evidence, and
  bounded follow-up queries.
- Confidence is calculated by the shared deterministic engine. A pack cannot
  provide a model-generated confidence percentage.
- Evidence older than the pack freshness bound is treated as missing.
- `partial` Evidence lowers confidence. Missing required Evidence returns
  `inconclusive`; `not_production_verified` required Evidence returns
  `unsupported`.
- Connector routes for a pack without a completed stable projection fail
  closed with an explicit `not_production_verified` reason. They never fall
  through to a similarly named legacy query.
- Message-related packs consume pseudonymized metadata only. Message bodies,
  tokens, secrets, private keys, and complete ACL/TLS material are forbidden in
  saved fixtures and diagnostic output.

## Wave B

| Pack | Primary diagnosis |
| --- | --- |
| `store-pressure.v1` | Disk, flush/dispatch, CommitLog/CQ, RocksDB, and tiered WAL pressure |
| `store-integrity.v1` | CommitLog/CQ/Index/Checkpoint consistency and recovery |
| `rocksdb-health.v1` | Cache, amplification, maintenance, checkpoint, and disk |
| `tiered-store.v1` | Provider, dispatch, transfer, fallback, read-ahead, and metadata |
| `broker-ha.v1` | Replica offsets, lag, SyncStateSet, and replica health |
| `controller-ha.v1` | Leader, quorum, commit/apply, and Broker heartbeat |
| `namesrv-route.v1` | Route agreement, registration freshness, reachability, and permission |
| `send-latency.v1` | Client, Proxy, Remoting, Broker, and Store segment latency |
| `proxy-connectivity.v1` | gRPC/remoting, session, TLS/Auth, forwarding, and backend route |
| `retry-dlq.v1` | Retry/DLQ growth, failure category, poison metadata, and downstream health |
| `transaction-message.v1` | Half backlog, finalization, checkback, and producer reachability |
| `pop-revive.v1` | Inflight, checkpoint, revive, receipt handle, and invisibility |
| `timer-backlog.v1` | Timer enqueue/dequeue, snapshot, clock, and Store pressure |
| `queue-hotspot.v1` | Queue traffic/storage/latency skew and expansion demand |
| `static-topic-route.v1` | Logical queue mapping, epoch, route, and expansion preflight |
| `topic-subscription-config.v1` | Topic/Group permission, filter, order, retry, and mode drift |
| `auth-failure.v1` | Scope, certificate, rotation, clock skew, replay, and deny category |
| `runtime-saturation.v1` | TaskGroup, BlockingExecutor, schedule, admission, and shutdown |

## Wave C

| Pack | Primary diagnosis |
| --- | --- |
| `upgrade-readiness.v1` | Compatibility, PDB, capacity, quorum, recovery, canary, and rollback |
| `capacity-runway.v1` | Traffic, connection, PVC/Store/tiered, and backlog runway |
| `cold-data-flow.v1` | Hot/cold hit rate, provider, cost, fallback, and local retention |
| `dr-readiness.v1` | Backup/restore, snapshots, metadata, RTO/RPO, and zone dependencies |
| `security-posture.v1` | Least privilege, credential expiry, configuration, and approval drift |
| `change-regression.v1` | Before/after image/config/action, SLO, and impact regression |

## Configuration and replay assets

- `config/diagnostics/wave-b/packs.v1.yaml`
- `config/diagnostics/wave-c/packs.v1.yaml`
- `tests/fixtures/diagnostics/wave-b/catalog.v1.json`
- `tests/fixtures/diagnostics/wave-c/catalog.v1.json`

The two fixture catalogs contain normal, fault, and missing-Evidence scenarios
for every new pack (72 scenarios total). Regenerate them with:

```powershell
node rocketmq-sre/scripts/generate_phase2_diagnostic_assets.mjs
```

The replay test verifies deterministic results, catalog coverage, and the
absence of sensitive fixture keys.
