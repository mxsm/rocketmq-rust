<!--
  Copyright 2023 The RocketMQ Rust Authors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# HA, RPO, and RTO Support Matrix

This document records the guarantees that the current implementation and automated tests actually establish. It does not turn a process-level smoke test into a production RPO or RTO commitment.

## Canonical acknowledgement contract

A primary append can report `Replicated` only when the canonical HA decision proves all of the following:

1. The requested `WriteAuthority` exactly matches the authority installed by the Controller.
2. The local durable watermark covers the complete appended range.
3. The configured `AckPolicy` is satisfied by unique, current sync-state-set members whose durable offsets cover the appended range.

An older epoch is rejected as stale. A future epoch or a different master at the installed epoch is rejected until that exact authority is installed. Missing, slow, duplicate, and removed replica acknowledgements never silently downgrade a replicated request to local durability.

## Support matrix

| Backend / topology | ACK policy | Code-level guarantee | Deterministic faults covered | Current RPO/RTO statement | Follow-up validation |
|---|---|---|---|---|---|
| Local Store / single Broker | `LocalDurable` | Acknowledgement requires the local durable watermark to cover the append. A local-only decision cannot construct `Replicated`. | Local fsync watermark behind append; invalid/truncated tail; reopen from a complete-record watermark. | No HA claim. RPO and RTO are not quantified by this PR. | Measure storage-device failure and restart recovery on supported production filesystems. |
| Local Store / default master-replica HA | `ReplicaCount(n)` | The leader and `n-1` unique current members must cover the append after local durability. | Replica stops before ACK; slow ACK; duplicate ACK; removed-member ACK; bounded group-transfer queue and deadline cleanup. | Process-level ACK safety is verified. No zero-loss or time-bound recovery claim is made. | Multi-process network partition, reconnect, sustained load, and 6-hour soak. |
| Local Store / Controller HA | `ReplicaCount(n)` or `AllInSyncSet` | Positive typed epochs, exact write authority, monotonic role transition, and current ISR membership are enforced before replicated acknowledgement. | Stale master; same-epoch conflicting master; unavailable Controller cannot pre-authorize a future leader; missing ISR progress; Controller durable-state write failures. | Fencing and process-level recovery smoke are verified. Cross-node RPO/RTO remain unquantified. | Full multi-node partition matrix, Controller quorum loss/recovery, node recreation, and complete DR exercise. |
| RocksDB message store | Delegates primary-log HA to the Local Store path | The implementation delegates HA runtime and primary-log replication to its Local Store component. | Generic contract tests apply; no RocksDB-specific HA fault matrix was added by this PR. | Not production-qualified by this PR for a distinct RPO/RTO claim. | Run the RocksDB feature matrix with multi-process HA faults before claiming parity. |
| Tiered / derived storage | Not a primary append ACK source | Derived or tiered progress cannot satisfy or upgrade primary append durability. | Contract tests prevent derived progress and local append state from constructing replicated durability. | Unsupported as an independent HA durability or RPO/RTO guarantee. | Define and test an explicit restore/read-continuity contract before making a tiered recovery claim. |

## Verified smoke inventory

The fast deterministic suite covers seven required categories without retry loops or arbitrary sleeps:

1. Replica stops before acknowledgement.
2. Stale master continues to request writes after a newer epoch is installed.
3. Controller authority is unavailable and competing future leaders are proposed.
4. Slow, duplicate, and removed replica acknowledgements are observed.
5. Local fsync progress remains behind the append.
6. A segment contains a complete record followed by an invalid tail.
7. A recovered complete-record watermark accepts the next complete record.

Existing Store restart tests additionally exercise dirty-tail truncation, reopen, produce/read continuity, and Local/RocksDB double-write compatibility. Existing Controller storage-fault tests verify that failed state, vote, and log persistence do not publish candidate in-memory state.

## Read-only SRE projection

Broker diagnostics expose only bounded, non-sensitive HA fields: role, master epoch, sync-state-set epoch and size, maximum replica lag, ACK policy, required ACK count, and a stable decision code. Missing observations remain absent or `not_observed`; connection addresses, credentials, message bodies, and full configuration are not included.

## Explicitly deferred

The following items are useful follow-up evidence but do not block this code-focused stage:

- Six-hour soak or chaos execution.
- Complete disaster-recovery and regional-loss exercises.
- Docker or Kubernetes image/deployment validation.
- Rolling upgrade and N-1 compatibility validation.
- Quantitative production RPO and RTO certification.
