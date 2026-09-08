# MessageStore Capability Migration ADR

- Status: Accepted
- Date: 2026-07-29
- Updated: 2026-09-08 (migration gate retired after the capability cutover)
- Owner: Store and Broker maintainers
- Decision scope: the aggregate Store facade, narrow capabilities, backend conformance, and Broker dependency burn-down

## Context

`MessageStore` predated the capability-oriented Store API and combined
lifecycle, append, read, offset/index, checkpoint, replication, health,
administration, and backend-internal operations. A token-derived inventory
found 126 real trait methods. The earlier approximate count of 131 included
five declarations inside block comments.

At the time of the original decision, Broker production source called 62 of
those methods, below the first release threshold of 80, but 64 production files
still named the wide facade.
Deleting the facade in one mechanical rewrite would combine unrelated
behavioral changes. Leaving it ungoverned would allow new behavior to enter the
same boundary.

## Decision

`MessageStore` was a temporary migration facade. It has since been removed.
New behavior belongs in a narrow `rocketmq-store-api` capability or
an implementation-private backend contract. Capabilities remain independent;
there is no replacement supertrait that forces every backend to implement
unrelated operations.

The temporary gate counted methods, callers, and source identities during the
migration. That gate, its baseline, and its dedicated Python tests are retired.
Further capability changes use compiler checks and the maintained Rust behavior
and backend conformance tests below. Internal module and method names do not
need a migration-baseline update.

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

Broker send and pull paths use capability-only test seams. Composition and
backend selection remain separate from individual subsystem capabilities.

## Compatibility

This decision does not change RocketMQ request codes, headers, response codes,
message bytes, offsets, persisted records, recovery, or backend semantics.
The retired Rust source facade was not a compatibility promise. Internal method
and module removal is allowed when consumers move to the corresponding capability.

The two approved aggregate package edges remain:

- Broker composition to `rocketmq-store`;
- Store inspection composition to `rocketmq-store`.

They may shrink but may not expand. Their deletion is evaluated independently
after each consumer has a canonical capability or backend inspection port.

## Evidence

- `rocketmq-doc/en/message-store-capability-migration.md`
- `rocketmq-store-api/tests/capability_contracts.rs`
- `rocketmq-store/tests/capability_conformance_tests.rs`
- `rocketmq-store/tests/public_api_contract.rs`
- `rocketmq-tieredstore/src/store.rs`
