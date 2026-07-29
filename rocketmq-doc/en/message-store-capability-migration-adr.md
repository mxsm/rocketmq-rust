# MessageStore Capability Migration ADR

- Status: Accepted
- Date: 2026-07-29
- Owner: Store and Broker maintainers
- Decision scope: the aggregate Store facade, narrow capabilities, backend conformance, and Broker dependency burn-down

## Context

`MessageStore` predates the capability-oriented Store API and combines
lifecycle, append, read, offset/index, checkpoint, replication, health,
administration, and backend-internal operations. A token-derived inventory
finds 126 real trait methods. The earlier approximate count of 131 included
five declarations inside block comments.

Broker production source currently calls 62 of those methods, below the first
release threshold of 80, but 64 production files still name the wide facade.
Deleting the facade in one mechanical rewrite would combine unrelated
behavioral changes. Leaving it ungoverned would allow new behavior to enter the
same boundary.

## Decision

`MessageStore` is a frozen migration facade, not the owner of new Store
behavior. New behavior belongs in a narrow `rocketmq-store-api` capability or
an implementation-private backend contract. Capabilities remain independent;
there is no replacement supertrait that forces every backend to implement
unrelated operations.

The repository-owned guard derives the trait body from balanced, comment- and
literal-masked source tokens. It enforces:

- no new `MessageStore` method;
- a non-increasing method count;
- no new Broker production file naming the wide facade;
- a non-increasing wide-facade identifier count;
- no more than 80 facade methods called by Broker production source in this
  release;
- an owner, reason, and deletion condition for every remaining Broker path;
- deterministic method/caller and migration-board evidence.

Reductions are allowed and expected. Obsolete internal source signatures do
not receive compatibility wrappers merely to preserve their names. The guard
baseline must be reviewed in the same change when a dependency is removed.

## Capability and backend contract

The canonical capability groups are `MessageAppender`, `MessageReader`,
`OffsetIndex`, `ReleaseCheckpointStore`, `ReplicationControl`, `StoreHealth`,
`StoreLifecycle`, and `AdminStore`. Request, result, durability, error,
deadline, and cancellation semantics are owned by the narrow contract.

Local and RocksDB use the same lifecycle and result projection conformance
suite. Tiered implements the backend-neutral lifecycle it actually supports
and retains provider-specific read/write contracts below that boundary.
Unsupported optional behavior must be explicit; a panic or default no-op does
not satisfy a capability.

Broker send and pull paths already use capability-only test seams. Remaining
migration order is transaction/schedule/pop, failover/replication/pre-online,
administration/control plane, and finally composition-only ownership.

## Compatibility

This decision does not change RocketMQ request codes, headers, response codes,
message bytes, offsets, persisted records, recovery, or backend semantics.
The aggregate Rust source facade remains only while real consumers exist; it
is not a compatibility promise. Internal method and module removal is allowed
when consumers move to the corresponding capability.

The two approved aggregate package edges remain:

- Broker composition to `rocketmq-store`;
- Store inspection composition to `rocketmq-store`.

They may shrink but may not expand. Their deletion is evaluated independently
after each consumer has a canonical capability or backend inspection port.

## Evidence

- `scripts/message_store_capability_guard.py`
- `scripts/message-store-capability-baseline.json`
- `rocketmq-doc/en/message-store-capability-migration.md`
- `rocketmq-store-api/tests/capability_contracts.rs`
- `rocketmq-store/tests/capability_conformance_tests.rs`
- `rocketmq-store/tests/public_api_contract.rs`
- `rocketmq-tieredstore/src/store.rs`
