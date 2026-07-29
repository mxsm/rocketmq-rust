# Controller Typed Persistence ADR

- Status: Accepted
- Date: 2026-07-29
- Owner: Controller maintainers
- Decision scope: OpenRaft log, vote, committed position, state-machine, membership, and snapshot persistence

## Context

The Controller storage SPI already provides atomic `write_batch` and explicit
`sync`. The architecture defect was above that SPI: OpenRaft consumers carried
raw string keys, compact-JSON knowledge, record grouping, and backend calls in
the same modules as consensus and state-machine behavior.

The first typed boundary must not silently rewrite existing Controller data.
Existing keys and serialized bytes are compatibility surfaces even though the
Rust modules that produce them are internal.

## Decision

`openraft::persistence` owns the complete V1 persistence vocabulary:

- `RaftRecordKey` renders and parses the existing key namespace;
- `encode_v1` and `decode_v1` own compact JSON encoding;
- `RaftLogRepository` owns vote, committed position, log append, truncate, and
  purge records;
- `RaftStateRepository` owns replica state, last-applied position, membership,
  snapshot metadata, and snapshot payload records.

`LogStore` depends only on the log repository. `StateMachine` depends only on
the state repository. Neither consumer contains a raw persistence key,
backend batch/sync call, or persistence decoder.

Repository updates use one backend batch for each domain commit and call
`sync` before publishing the corresponding in-memory state. Partial
state-machine triples and partial snapshot pairs fail closed. Persisted log
keys are strictly parsed, entries remain contiguous, entry indices must match
their keys, committed positions cannot exceed durable positions, and snapshot
metadata must match the checksummed payload.

Errors identify only the operation and record class. They do not echo raw keys
or payload bytes.

## V1 format decision

Every current key and representative vote, log ID, log entry, membership,
snapshot metadata, and snapshot payload byte sequence has a golden fixture.
V1 continues to use the same strings and compact Serde JSON. Replica manager
state and snapshot payload placement are unchanged.

No V2 format is introduced because there is no business or recovery reason to
change the on-disk representation. Consequently no mixed-version,
interrupted-migration, dual-write, or rollback protocol is applicable to this
change. A future V2 requires a separate accepted ADR, a separate namespace,
cluster version fencing, and explicit interruption and rollback evidence. It
must not overwrite V1 bytes in place.

## Compatibility

Rust source-level helpers, constants, and direct backend access are deleted
without compatibility wrappers. Existing Controller key strings, JSON bytes,
Raft positions, membership, checksums, snapshot determinism, and recovery
behavior remain unchanged. Opening V1 storage does not rewrite it.

## Evidence

- `rocketmq-controller/src/openraft/persistence/key.rs`
- `rocketmq-controller/src/openraft/persistence/codec.rs`
- `rocketmq-controller/src/openraft/persistence/log_repository.rs`
- `rocketmq-controller/src/openraft/persistence/state_repository.rs`
- `rocketmq-controller/tests/controller_storage_faults.rs`
- `rocketmq-controller/tests/controller_snapshot_restore.rs`
- `rocketmq-controller/tests/controller_linearizability.rs`
