# Acknowledgement and failover contract ADR

- Status: Accepted
- Decision date: 2026-07-30
- Owners: Broker, Store, Controller, Release Engineering
- Target: Current 1.x implementation and every later release candidate

## Context

A successful send response is meaningful only when it is tied to the storage
and replication state reached before the response. Flush mode, Broker role, and
Controller-managed promotion are independent dimensions; treating every
`PUT_OK` as a zero-loss guarantee would hide real failure boundaries.

## Decision

`rocketmq-store-api::AppendReceipt` is the canonical internal projection:

| Receipt durability | Required state at response | What it does not guarantee |
|---|---|---|
| `Memory` | The primary CommitLog accepted the complete byte range | Local fsync, replica acknowledgement, survival of process/node loss |
| `Local` | The durable local watermark covers the complete appended range | Survival of local disk/PVC loss or promotion of a lagging replica |
| `Replicated` | Local durability and the configured eligible-replica acknowledgement condition cover the range | Survival of failures outside the measured topology or unsafe manual promotion |

The operational profiles are:

| Profile | Flush/replication configuration | Promotion rule | RPO gate |
|---|---|---|---|
| `memory-accepted` | Asynchronous local flush; asynchronous replication or no eligible replica acknowledgement | Never advertise zero loss; recovery reports the exact acknowledged-message delta | Measured value is reported, but no finite zero-loss guarantee is claimed |
| `local-durable` | Synchronous local flush; replica acknowledgement not required | Recover only from the same preserved WAL/PVC | Zero acknowledged-message loss for a process restart or node reschedule that preserves storage |
| `replicated-durable` | Synchronous local flush plus successful eligible-replica acknowledgement, including Controller mode when enabled | Promote only when the candidate replica covers the recorded CommitLog and queue positions | Zero acknowledged-message loss for one approved Broker/replica failure |

`FlushDiskTimeout`, `FlushReplicaTimeout`, `ReplicaUnavailable`, and
`InsufficientReplicas` cannot be rewritten as a fully durable success by an
adapter. A message may have been appended despite a timeout, so retry behavior
must use message identity and duplicate-safe semantics.

## Measured evidence

The source of truth for a run is
`distribution/config/ack-failover-evidence-schema.json`. Each result records:

- candidate commit and immutable service image digests;
- flush, Broker role, Controller mode, acknowledgement profile, and topology;
- acknowledged message IDs and CommitLog, ConsumeQueue, and Controller/Raft
  positions before injection;
- the same values after recovery;
- observed lost/duplicate message counts and RPO;
- injection-to-ready and injection-to-message-visible RTO;
- promotion decision, cleanup status, leaked task count, and detached task
  count.

A reported RPO/RTO value is valid only for the exact candidate and fault
scenario in that artifact. The accepted policy threshold is not evidence that a
candidate met it.

## Failure scenarios

- Leader loss: election/promotion is blocked until the recorded positions are
  covered.
- Controller quorum loss: existing data-plane behavior is observed, but no new
  unsafe leadership decision is permitted.
- Network partition or half-open connection: the response classification and
  duplicate-safe replay are verified.
- Slow disk, full disk, or fsync jitter: a durability timeout remains a timeout;
  it cannot advance the durable watermark.
- Snapshot installation interruption: recovery must preserve membership,
  last-applied position, checksum, and the acknowledged-message ledger.

## Consequences

This decision keeps wire response codes unchanged while making the internal
durability meaning explicit. It does not promise zero RPO for asynchronous
acknowledgements and does not authorize promotion when quorum or position
evidence is missing.
