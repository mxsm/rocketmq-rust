# Architecture production-readiness runbook

This runbook covers the M11-12/R24 release-candidate objectives. It complements the
dynamic Kind/K3d fault evidence; dashboards and alerts cannot replace a
`dynamic_execution=true`, non-fixture evidence package for the exact candidate commit.

## Release invariants

- CommitLog remains the authoritative WAL. Never delete or rewrite WAL, PVCs, queue
  offsets, or message payloads during rollback.
- A production candidate needs at least six hours of soak evidence sampled every
  minute. Missing samples must not exceed one percent.
- The evidence package must bind the candidate commit, candidate image digests,
  M11-11 fault run, SLO policy, dashboard, alerts, runbook, and every generated
  artifact by SHA-256.
- Any failed objective, unresolved alert, unresolved fault, mismatched commit, or
  incomplete rollback blocks promotion.

## Message availability

Alert: `RocketMQMessageDeliveryRatioBurn`

1. Confirm the alert is not caused by an idle input stream. The online objective
   compares delivery and ingress rates; M11-11 separately requires explicit
   acknowledged send/query probes.
2. Check Broker readiness, NameServer route visibility, Controller quorum, Proxy
   readiness, and the latest acknowledged-message recovery artifact.
3. Stop promotion if the delivery ratio is below `0.999`.
4. Roll back all five workloads to their recorded baseline digests. Preserve the
   candidate evidence and verify the acknowledged message, queue offset, CommitLog
   offset, and PVC UID set again.

## Send latency

Alert: `RocketMQSendMessageP99High`

1. Compare the functional-probe p99 with
   `rocketmq_send_message_latency`; the release objective is at most `1000 ms`.
2. Inspect flush/dispatch lag, HA replication lag, disk pressure, CPU throttling,
   and collector outage timing before attributing the regression.
3. Do not raise the threshold or shorten the soak window to make a candidate pass.
4. Roll back the candidate images when the objective remains exceeded for ten
   minutes, then repeat the same probe against the baseline.

## Consumer lag

Alert: `RocketMQConsumerLagHigh`

1. Identify the affected topic and consumer group without exporting message bodies.
2. Check `rocketmq_consumer_lag_messages`, inflight work, route freshness, consumer
   connectivity, and dispatch-behind bytes.
3. Promotion is blocked when the six-hour maximum exceeds `10000` messages.
4. If the lag began with the candidate, restore baseline images and confirm that the
   lag is decreasing before closing the incident.

## Store flush

Alert: `RocketMQStoreFlushBehindHigh`

1. Check `rocketmq_storage_flush_behind_bytes`,
   `rocketmq_storage_dispatch_behind_bytes`, disk pressure, flush latency, and
   storage errors.
2. Promotion is blocked when flush-behind exceeds `67108864` bytes.
3. Keep the Broker available for reads when safe, but do not acknowledge new writes
   if the configured durability contract cannot be met.
4. Roll back executable images and chart revision only. Do not delete CommitLog,
   ConsumeQueue, Index, RocksDB state, or PVCs.

## HA replication

Alert: `RocketMQHaReplicationLagHigh`

1. Check `rocketmq_store_ha_replication_lag_bytes`, replica connectivity,
   Controller quorum, confirm offset, and disk/flush health.
2. Promotion is blocked when replication lag exceeds `67108864` bytes.
3. Do not force a role change that can lose acknowledged data.
4. Restore baseline images and verify the same message ID and offsets after quorum
   recovery.

## Collector outage budget

The M11-11 `collector_outage` fault scenario must demonstrate a successful
send/query round trip in under 30 seconds while the collector is unavailable. The
telemetry queue remains bounded and the data plane must not wait for export.

If the budget is exceeded, restore the collector, preserve exporter logs and queue
measurements, stop promotion, and fix the owning telemetry path. Never disable the
bounded queue or privacy/cardinality guard as a workaround.

## Drain and rollback health

The candidate must report zero `FailedPreStopHook` events and restore:

- all five baseline image digests;
- the baseline Helm chart revision;
- the collector and Controller quorum;
- the original PVC UID set and acknowledged message;
- queue and CommitLog offsets;
- an empty unresolved-fault and unresolved-alert list.

Rollback is complete only after the baseline workloads are ready and the
acknowledged-message query succeeds. Preserve both failed and successful evidence
directories. A failed run must not contain a production `run.json` that can be
mistaken for a pass.

## Evidence verification

From the repository root, validate policy and fixtures:

```powershell
python scripts/architecture_slo_guard.py --policy-only
python scripts/architecture_slo_guard.py `
  --evidence scripts/tests/fixtures/m11-slo/pass `
  --allow-fixture
python -m unittest scripts.tests.test_architecture_slo_guard -v
```

For production evidence, omit `--allow-fixture`. The guard then requires the
candidate commit to equal the checked-out commit, verifies the embedded M11-11
dynamic fault evidence, compares every objective with the committed threshold, and
checks every SHA-256 entry.
