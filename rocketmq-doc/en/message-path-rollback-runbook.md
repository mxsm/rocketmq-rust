# Message-path release and rollback runbook

This runbook covers the Rust producer, Proxy, Broker, store, consumer, and Controller message paths. It is the operational handoff for a release candidate that has already passed focused tests.

The compatibility contract is externally visible business behavior: the Rust implementation must produce the same message semantics and results expected by Java clients, but its scheduling, ownership, batching, backpressure, storage leases, and high-availability internals remain Rust-native. The project has one optimal implementation path; it does not use legacy or Java compatibility profiles.

DLedger is intentionally outside the Rust product scope. Do not add a DLedger migration, deployment, disk-format, performance, or rollback gate to this runbook. A DLedger configuration must fail fast with the existing typed unsupported result.

## Non-negotiable rollback invariants

- Do not delete, truncate, convert, or copy back CommitLog, ConsumeQueue, Index, Timer, RocksDB, or tiered-store data as part of rollback.
- Do not rewrite consumer offsets, receipt handles, transaction state, confirm offset, or Controller epochs.
- Do not enable unclean election to make a rollback appear successful.
- Do not weaken the configured flush or replica-ack contract during rollback.
- Roll back binaries and explicit configuration together. There is no compatibility-profile switch.
- Keep request codes, headers, response codes, message order, retry/DLQ behavior, and persisted layouts unchanged.

## Release inputs

Record these values before any deployment:

- candidate and rollback Git SHAs, immutable artifact or image digests, and build metadata;
- the exact NameServer endpoint, topic, consumer group, and target-environment owner;
- the effective Broker, store, Proxy, client, and Controller configuration;
- the durability contract, including flush mode, required replica acknowledgements, ISR requirements, and clean-election policy;
- the candidate `qualification-report.json`, same-environment performance comparison, fault-matrix directory, and six-hour soak report;
- the dashboard or query links for send failures, latency, replication lag, queue age, renewal lateness, resource budgets, process CPU, RSS, and task count.

The candidate is release-qualified only when `qualification-report.json` contains both `status: "pass"` and `release_qualified: true`. A passing local smoke report is useful functional evidence, but is not release approval.

## Preflight

1. Validate the committed policy and inspect the exact workload commands without connecting to a Broker:

   ```powershell
   python scripts/message_path_qualification.py validate-policy
   python scripts/message_path_qualification.py plan `
     --mode release `
     --namesrv 10.0.0.15:9876 `
     --confirm-target 10.0.0.15:9876 `
     --topic ReleaseQualification `
     --durability-contract sync-flush-required-replica-acks
   ```

2. Verify that the release worktree is clean and that the report SHA matches the candidate artifact.
3. Confirm Controller quorum, Broker roles, ISR membership, confirm offset, replication lag, disk headroom, and clean-election policy.
4. Export effective configuration and routing metadata. Keep the previous immutable artifact and its matching configuration immediately available.
5. Confirm that dashboards and alerts can distinguish client queueing, Proxy admission, Broker processing, store/flush, HA wait, ACK/renew, and consumer lag.
6. Stop if any required evidence is absent, belongs to different hardware or durability settings, or contains an unknown schema version.

## Staged rollout

### Stage 1: evidence freeze

- Archive the qualification report and all referenced raw artifacts by content hash.
- Record the operator, start time, candidate SHA, rollback SHA, and approval decision.
- Freeze unrelated configuration and topic changes until rollout completes.

### Stage 2: client and Proxy canary

- Route a bounded producer and consumer cohort through one candidate Proxy or client deployment.
- Exercise sync, async, batch, POP/LitePull, ACK/change-invisible-time, retry, DLQ, FIFO, delay, transaction, Priority, LiteTopic, Identity, and GZIP behavior that the deployment uses.
- Hold the canary long enough to cross message invisibility, retry, rebalance, and route-refresh windows.
- Compare success count, duplicate count, throughput, p99 latency, queue age, renew lateness, response-byte budget, CPU, and RSS with the frozen baseline.

### Stage 3: Broker and store canary

- Upgrade one eligible replica at a time while maintaining quorum and required ISR.
- Wait for role, route, confirm-offset, replication-lag, and store-health convergence before promoting the next replica.
- Run bounded write/read verification and confirm that every acknowledged message can be consumed once the business retry policy is applied.
- Never remove the rollback binary until the canary has completed recovery and restart checks.

### Stage 4: Controller and availability-zone waves

- Change one Controller member at a time and preserve quorum.
- Verify linearizable metadata reads, Broker authority, clean election, route convergence, and segmented T0-T5 failover evidence.
- Expand by availability zone or another independently reversible wave. Re-run the short qualification workload after every wave.

### Stage 5: full rollout and observation

- Promote only after every prior stage remains inside the approved gates.
- Continue observation through the six-hour resource-growth window.
- Archive the final report, configuration, dashboards, and operator decision.

## Abort triggers

Stop promotion and start rollback when any of these conditions is confirmed:

- a message acknowledged as `PutOk` is missing, corrupted, assigned an invalid offset, or violates the configured ordering contract;
- unexpected duplicate delivery exceeds the business retry/visibility contract;
- send, receive, ACK, renewal, offset, retry, DLQ, transaction, delay, FIFO, Priority, or LiteTopic behavior differs from the accepted contract;
- same-environment throughput regresses by more than 10% or p99 latency regresses by more than 15%;
- ACK/change-invisible-time is starved by long polling, renewal occurs after its safety deadline, or a local long poll expires before its requested wait;
- count, byte, age, cache, write-buffer, response-stream, or task ownership limits are exceeded or leak after cancellation;
- confirm offset regresses or exceeds a legal in-sync durable acknowledgement, clean election fails, or the accepted RPO/RTO evidence is violated;
- the six-hour run detects monotonic RSS, task, file-descriptor, queue-age, cache, or pending-request growth;
- operators cannot explain a schema mismatch, dirty worktree, target mismatch, or missing raw artifact.

## Rollback procedure

1. Stop promotion immediately and record the trigger, first bad timestamp, affected wave, and evidence links.
2. Stop routing new producer traffic to the affected canary. Allow bounded in-flight requests to complete or reach their original absolute deadline; do not extend deadlines during rollback.
3. Drain or pause affected consumers and Proxies so ACK, renewal, and offset operations are not abandoned midway. Keep healthy control lanes available.
4. Restore the previous immutable client or Proxy binary and its matching explicit configuration. Re-enable traffic gradually after route and session checks pass.
5. For Broker/store rollback, verify quorum and ISR first, then replace one replica at a time. Wait for replication and confirm-offset convergence between replicas. Do not roll all replicas simultaneously.
6. For Controller rollback, maintain a majority throughout, verify the active authority epoch after every member, and keep clean election enabled.
7. Reuse the existing data directories unchanged. If the previous binary cannot read them, stop and escalate; do not attempt an ad hoc data conversion.
8. Re-run the bounded managed smoke or an equivalent isolated target-environment smoke against the restored version.
9. Verify routes, Broker role, Controller quorum, ISR, confirm offset, consumer offsets, retry/DLQ, ACK/renew, and all `PutOk` message IDs captured during the affected interval.
10. Keep the rollout closed until the incident owner signs off the message audit and resource usage has returned to a stable baseline.

## Component rollback matrix

| Component | Safe rollback unit | Preserve | Mandatory verification |
|---|---|---|---|
| Producer/client | One deployment cohort | unique IDs, timeout budget, retry policy | sync/async/batch results, callback once, no missing acknowledged IDs |
| gRPC Proxy | One instance or traffic slice | session/receipt contract, absolute deadlines | send/receive streams, ACK/renew control lane, response budgets |
| Broker/store | One replica | all data files, offsets, confirm state | write/read, recovery, CQ visibility, flush and replica ACK contract |
| Controller | One quorum member | Raft state, authority epoch, clean election | quorum, linearizable read, Broker authority, route convergence |
| Qualification tooling | One pinned Git SHA | policy and report schema | policy validation, artifact hashes, target confirmation |

## Post-rollback verification

- [ ] The restored artifact and configuration digests match the recorded rollback set.
- [ ] NameServer routes, Controller quorum, Broker roles, ISR, and confirm offset are healthy.
- [ ] All acknowledged audit message IDs are readable; missing and unexpected duplicate counts are zero.
- [ ] Sync, async, batch, POP/LitePull, ACK, retry, DLQ, FIFO, delay, and transaction smoke checks pass.
- [ ] Production-used Priority, LiteTopic, and GZIP paths pass their contract checks.
- [ ] Queue, byte, cache, write-buffer, response-stream, and task usage return below configured limits.
- [ ] No data directory or consumer offset was deleted, rewritten, or migrated.
- [ ] Incident evidence, owner, timestamps, root cause, and the next release decision are archived.

## Sign-off record

| Field | Value |
|---|---|
| Candidate SHA / artifact | |
| Rollback SHA / artifact | |
| Durability contract | |
| Confirmed target | |
| Qualification report hash | |
| Performance comparison hash | |
| Fault-matrix evidence hash | |
| Soak report hash | |
| Release or rollback operator | |
| Decision and timestamp | |
