# Architecture production-readiness runbook

This runbook covers the M11-12/R24 release-candidate objectives. It complements the
dynamic Kind/K3d fault evidence; dashboards and alerts cannot replace a
`dynamic_execution=true`, non-fixture evidence package for the exact candidate commit.

The versioned source of truth is
`distribution/config/architecture-production-readiness-policy.json`. Metric names,
units, owners, query labels, thresholds, dashboard panels, alert routes, readiness
dependencies, and collector-outage limits must change together with that contract.

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
- Release identity uses only `service`, `release_commit`, and `release_nonce`.
  Credentials, configuration objects, message bodies, topic names, consumer groups,
  hostnames, and request identifiers are not release-identity labels.
- `/readyz` is the canonical service-readiness endpoint. It returns success only
  after the service-specific security, storage or control-plane dependencies and
  release identity have completed.

## Release identity

Alert: `RocketMQReleaseIdentityConflict`

1. **Diagnose:** Compare `rocketmq_release_info` by `service`,
   `release_commit`, and `release_nonce` with the active `ReleaseState`. Confirm
   that the conflict is not a stale Prometheus target.
2. **Contain:** Stop promotion and prevent another rollout until every workload
   references one commit, nonce, config digest, Secret version, and storage
   generation.
3. **Recover:** Remove only the stale workload or scrape target, wait for its
   series to age out, and verify all five `/readyz` endpoints against the intended
   release identity.
4. **Escalate:** Page the release-engineering and owning service teams when more
   than one identity remains active for five minutes or the running identity does
   not match the complete `ReleaseState`.

## Service readiness

Readiness is service-specific but governed by one policy:

- Broker requires validated security, a writable MessageStore, started request
  processors and listeners, NameServer registration, and release identity.
- NameServer requires validated security, route processing, the optional embedded
  Controller when configured, and release identity.
- Controller requires validated security, recovered storage and Raft state,
  cluster recovery, and release identity.
- Proxy requires validated security, a healthy metadata route in cluster mode,
  every configured listener, and release identity.
- MCP requires validated security, initialized application state, the selected
  transport listener, and release identity.

A bound socket alone is never readiness. When `/readyz` returns 503, stop the
rollout, inspect the failed dependency, restore that dependency, and wait for a
fresh 200 response. Escalate to the owning service team if the dependency is
healthy but readiness remains unpublished.

## Message availability

Alert: `RocketMQMessageDeliveryRatioBurn`

1. **Diagnose:** Confirm the alert is not caused by an idle input stream, then
   check Broker readiness, NameServer route visibility, Controller quorum, Proxy
   readiness, and the acknowledged send/query probe.
2. **Contain:** Stop promotion when the delivery ratio is below `0.999`; stop new
   writes if the configured acknowledgement contract cannot be preserved.
3. **Recover:** Roll back all five workloads to their recorded baseline digests
   and verify the acknowledged message, queue offset, CommitLog offset, and PVC
   UID set again.
4. **Escalate:** Page Broker, storage, and control-plane owners if the baseline
   still misses the objective or the acknowledged message cannot be recovered.

## Send latency

Alert: `RocketMQSendMessageP99High`

1. **Diagnose:** Compare the functional-probe p99 with
   `rocketmq_send_message_latency`; the release objective is at most `1000 ms`.
   Inspect flush/dispatch lag, HA replication lag, disk pressure, CPU throttling,
   and collector-outage timing before attributing the regression.
2. **Contain:** Stop promotion; do not raise the threshold or shorten the soak
   window to make a candidate pass.
3. **Recover:** Roll back the candidate images when the objective remains exceeded
   for ten minutes, then repeat the same probe against the baseline.
4. **Escalate:** Page Broker and storage owners when the baseline remains above
   the objective or durable acknowledgements are delayed.

## Consumer lag

Alert: `RocketMQConsumerLagHigh`

1. **Diagnose:** Identify the affected topic and consumer group without exporting
   message bodies. Check `rocketmq_consumer_lag_messages`, in-flight work, route
   freshness, consumer connectivity, and dispatch-behind bytes.
2. **Contain:** Block promotion when the six-hour maximum exceeds `10000`
   messages and avoid destructive offset changes.
3. **Recover:** If the lag began with the candidate, restore baseline images and
   confirm that the lag is decreasing before closing the incident.
4. **Escalate:** Page Broker and consumer owners if lag does not decrease after
   baseline recovery or route and storage health disagree.

## Store flush

Alert: `RocketMQStoreFlushBehindHigh`

1. **Diagnose:** Check `rocketmq_storage_flush_behind_bytes`,
   `rocketmq_storage_dispatch_behind_bytes`, disk pressure, flush latency, and
   storage errors.
2. **Contain:** Block promotion when flush-behind exceeds `67108864` bytes. Keep
   the Broker available for reads when safe, but do not acknowledge new writes if
   the configured durability contract cannot be met.
3. **Recover:** Roll back executable images and chart revision only. Do not delete CommitLog,
   ConsumeQueue, Index, RocksDB state, or PVCs.
4. **Escalate:** Page the storage owner for persistent backlog, disk errors, or
   any mismatch between reported and durable offsets.

## HA replication

Alert: `RocketMQHaReplicationLagHigh`

1. **Diagnose:** Check `rocketmq_store_ha_replication_lag_bytes`, replica
   connectivity, Controller quorum, confirm offset, and disk/flush health.
2. **Contain:** Block promotion when replication lag exceeds `67108864` bytes. Do
   not force a role change that can lose acknowledged data.
3. **Recover:** Restore baseline images and verify the same message ID and offsets
   after quorum recovery.
4. **Escalate:** Page storage and Controller owners if quorum cannot recover or
   replica and leader offsets remain inconsistent.

## Collector outage budget

The M11-11 `collector_outage` fault scenario must demonstrate a successful
send/query round trip in under 30 seconds while the collector is unavailable. The
telemetry queue remains bounded and the data plane must not wait for export.

The production boundary admits at most 2048 records, 8 MiB in total, and 64 KiB
per record. Admission uses `try_enqueue`, rejects the newest record when full, and
reports drops through `rocketmq.exporter.drop`; shutdown reports final queue and
drop totals through `rocketmq.exporter.shutdown`.

1. **Diagnose:** Inspect accepted, drained, dropped-by-reason, queued-item, and
   queued-byte measurements together with the send/query probe.
2. **Contain:** Stop promotion if the 30-second budget is exceeded; do not enlarge
   the queue until resource usage and outage duration are understood.
3. **Recover:** Restore the collector, allow the bounded queue to drain, and repeat
   the same functional probe.
4. **Escalate:** Page the observability owner if the data plane blocks, queue
   limits are exceeded, drops are not reported, or recovery remains above 30
   seconds.

Never disable the bounded queue or privacy/cardinality guard as a workaround.

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

## Evidence execution boundary

The six-hour sampler runs only on the dedicated
`self-hosted,linux,x64,rocketmq-architecture-evidence` runner. Pull requests execute
the static contract job only. The dynamic workflow generates cryptographically
random, run-scoped test credentials, authenticates and preloads immutable image
digests, and never uploads those credential manifests.

The retained fault cluster is promoted to the candidate digests before the soak.
`run-architecture-slo-cluster.ps1` then deploys a digest-pinned private Prometheus
that scrapes all five metrics Services and a bounded message send/consume probe.
Prometheus is reached only through a loopback `kubectl port-forward`. The wrapper
keeps the port-forward owned for the full sampler lifetime and terminates it during
cleanup. Production credentials and an externally reachable metrics endpoint are
not inputs to this isolated evidence environment.

## Evidence verification

From the repository root, validate policy and fixtures:

```powershell
python scripts/architecture_slo_guard.py --policy-only
python scripts/architecture_slo_guard.py `
  --evidence scripts/tests/fixtures/m11-slo/pass `
  --allow-fixture
python -m unittest scripts.tests.test_architecture_slo_guard -v
python -m unittest scripts.tests.test_m11_dynamic_evidence -v
cargo test -p rocketmq-observability --test production_readiness_contract
```

For production evidence, omit `--allow-fixture`. The guard then requires the
candidate commit to equal the checked-out commit, verifies the embedded M11-11
dynamic fault evidence, compares every objective with the committed threshold, and
checks every SHA-256 entry.
