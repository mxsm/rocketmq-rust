---
title: "Maintain a running deployment"
---

Plan maintenance around surviving capacity and owned work. The goal of a restart is to replace one process while preserving the chosen write contract, recoverable state, and application completion behavior. Follow [HA deployment](../deployment/high-availability.md) and [Kubernetes operations](../deployment/kubernetes.md) for topology-specific steps.

## Routine observations

| Area | Observe | Respond when |
| --- | --- | --- |
| Routes and identities | Expected Broker groups/replicas, reachable advertised addresses, current Controller leader | Registration disappears, identity changes unexpectedly, or route data disagrees with the intended topology |
| Message progress | Send results, per-queue consumer movement, retry/DLQ/POP activity | Completion stalls or backlog approaches retained-data limits |
| Storage | Free bytes, segment growth, disk latency, cleanup activity, external roots, volume health | Headroom shrinks or durability/recovery work falls behind |
| Replication | In-sync membership, current write authority, durable progress | The chosen write requirement loses capacity or catch-up does not advance |
| Lifecycle and telemetry | Probe state, task/shutdown errors, export health, active TLS/ACL configuration | Process health differs from the observed application path |

Use [Admin](admin.md) and [monitoring](monitoring.md), comparing observations over time. Store logs and metrics within their own retention budget so diagnostics do not consume the message store's emergency space.

## Change retention deliberately

The local store uses `fileReservedTime` in hours and `deleteWhen` for scheduled cleanup. Disk pressure and `cleanFileForciblyEnable` can also influence deletion. The relevant disk ratios are normalized by the local-backend configuration; a raw value is not always the effective threshold.

Cleanup operates on eligible files/segments, not on one application's completed business records. Do not interpret a configured retention period as a guarantee that all unconsumed messages remain available under disk pressure. Before reducing retention, compare the oldest data needed for consumption, replay, backup, and delayed/transaction processing.

Observe actual minimum queue offsets and disk usage after a change. Restoring the old configuration does not restore deleted data. Never remove individual live CommitLog, index, timer, or RocksDB files as a space-saving substitute for supported cleanup.

## Drain a node

1. Identify the exact process/Pod, identity, current role, data roots, and replacement command/configuration.
2. Verify surviving Controller majority and Broker replication/write capacity. If the selected topology cannot maintain writes during the restart, plan a write pause instead of assuming availability.
3. Stop routing new application work to the affected path where the application/deployment supports it. Let admitted operations and business work settle; account for uncertain sends, POP receipts, prepared transactions, and retained response streams.
4. Request normal service shutdown through its process supervisor or deployment lifecycle. The core chart invokes `/drainz` before termination and budgets service shutdown inside Pod grace time.
5. Inspect shutdown results and confirm the old owner has stopped before reopening the same store. Cancellation and timeout are not proof that a blocking operation has finished.

Stopping a consumer can cause another consumer to receive unfinished work. Idempotent business handling and the mode's offset/ACK completion remain necessary during maintenance. Draining a Proxy does not by itself drain every application connected directly to Brokers.

## Restart in a topology-aware order

| Component | Procedure boundary |
| --- | --- |
| NameServer | Replace one at a time; ensure clients/Brokers can use other configured NameServers and observe registration convergence |
| Default HA replica | Preserve identity/store; restart and observe catch-up before any next change |
| Default HA primary | This mode does not acquire Controller-managed automatic promotion merely because a replica exists; plan the write interruption or the separately supported role-change procedure |
| Controller-mode Broker | Follow current Controller authority, in-sync membership, and catch-up; do not force an old primary writable |
| Controller | Use current membership and fresh replication observations; preserve quorum and stable node identity; chart deployments use the packaged rollout helper |
| Proxy | Remove/drain the instance, replace it, check ingress and downstream behavior, then restore traffic |

A two-replica group requiring both replicas has no spare synchronous write capacity while one is unavailable. Its chart PDB therefore has no voluntary disruption allowance. Do not bypass that fact by directly deleting the Pod. Adjust the maintenance plan or supported topology deliberately.

StatefulSet `OnDelete` updates need an explicit controlled replacement. A configuration edit is not a Controller membership change. Avoid overlapping planned restarts, and re-evaluate the current leader and replica health each time; a successful earlier observation does not reserve future availability.

## Rotate credentials and certificates

Use [security rotation](../deployment/security.md): update receiver material, transition callers where overlap is supported, apply the actual reload/restart path, then verify new access and old-access rejection. The core chart documents restarts after rotation; Proxy TLS also supports its own validated file-generation reload. An updated Secret or a watcher log is not evidence that all callers now use new material.

Protect local auth snapshots and backups alongside ACL files. Do not change credentials and unrelated topology/storage settings in the same maintenance step when separate observations can isolate failure causes.

## Return to service

Confirm the expected identity and role, reachable route, actual message completion, consumer progress, replication catch-up, and telemetry export. For a restarted replica, Ready is not a substitute for the durable progress required by the current write policy.

If the replacement cannot recover or the expected path fails, stop advancing the rollout. Preserve logs and original state, then use [troubleshooting](troubleshooting.md) or [upgrade/rollback](upgrade-rollback.md). Do not repeatedly restart every peer or delete identity/state to make startup appear clean.

These steps describe operational behavior; no multi-node maintenance exercise or availability guarantee is claimed by this page.

## Source map

[Local cleanup](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/message_store/local_file_message_store/health.rs), [normalized cleanup settings](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/config/message_store_config.rs), [core chart lifecycle and rollout](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/helm/rocketmq-rust-core/README.md), [runtime ownership](../architecture/runtime.md).
