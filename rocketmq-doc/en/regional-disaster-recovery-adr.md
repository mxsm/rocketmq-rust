# Regional disaster recovery boundary ADR

- Status: Accepted
- Decision date: 2026-07-30
- Owners: Architecture, Store, Controller, Release Engineering
- Target: Explicit out-of-scope boundary until a replacement ADR is accepted

## Decision

The repository does not currently implement a cross-region replication plane,
global Controller quorum, regional traffic director, or automated regional
promotion protocol. Therefore it does not claim built-in regional disaster
recovery.

Multiple independent RocketMQ-Rust clusters can be deployed in different
regions. Topology placement, traffic switching, cross-region message
replication, offset reconciliation, encryption-key coordination, and regional
promotion remain deployment-orchestration responsibilities outside the
project's current control plane.

## Supported boundary

Within one tested cluster and its storage domain, the release machinery can:

- deploy digest-pinned workloads;
- preserve WAL, CommitLog, ConsumeQueue, Index, RocksDB state, offsets, and PVC
  identities during an executable rollback;
- verify Controller quorum and approved Broker promotion;
- measure acknowledgement-profile RPO/RTO for committed fault scenarios;
- retain commit-bound fault, soak, performance, and rollback artifacts.

These capabilities do not establish cross-region durability.

## Requirements for a future built-in capability

A future proposal must define and implement all of the following before the
documentation can claim regional disaster recovery:

- replication ownership and ordering across regions;
- conflict, duplicate, transaction, and offset reconciliation semantics;
- regional fencing and leadership authority;
- key and secret rotation across failure domains;
- measurable per-acknowledgement RPO/RTO;
- interruption-safe bootstrap, catch-up, failover, failback, and rollback;
- dynamic evidence against the exact candidate commit and immutable digests.

Until those conditions are met, regional recovery claims belong to the
external deployment design and must identify that external system explicitly.
