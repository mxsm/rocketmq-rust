# rocketmq-store-api

Storage capability contracts shared by RocketMQ storage implementations and their consumers. This package has no Tokio or RocksDB dependency and does not open a store.

## Contracts

| Surface | Responsibility |
| --- | --- |
| `MessageAppender`, `MessageReader`, `OffsetIndex`, `AdminStore` | Narrow append, read, offset-index, and administration capabilities. |
| `StoreLifecycle`, `StoreHealth`, `ReplicationControl` | Loading, startup/shutdown, health, and replication control. |
| `WalPort`, `DerivedRecordSink` | Primary-log access and derived-record application. |
| `AppendReceipt`, `AppendStatus`, `Durability` | Append acceptance and the durability reached by the primary log. |
| `DerivedCursor`, `DerivedCheckpoint`, `CursorAdvance` | Typed derived-state progress and checkpoint identity. |
| `WriteAuthority`, `WriteLeaseToken`, `AckPolicy`, `decide_replication` | Write authority and replica-acknowledgement decisions. |
| Timer and checkpoint types | Persisted timer identity/routes, snapshot manifests, release checkpoints, and restore verification. |

The capability traits expose asynchronous operations through returned futures without selecting an executor. Consumers use the capability they need instead of depending on a concrete backend.

An accepted append is not necessarily fully durable: some timeout/replica-unavailable statuses still mean the primary log accepted the bytes. `Durability::Memory`, `Local`, and `Replicated` describe distinct guarantees. Derived-index progress cannot upgrade primary-log durability. Inspect the receipt and acknowledgement policy before deciding whether a retry is safe.

`StoreContractViolation` represents invalid caller contracts; `StoreError` retains operation/component context and canonical operational error identity. See [`capability`](src/capability) and [`ha_contract.rs`](src/ha_contract.rs).

## Consumers and validation

[rocketmq-store](../rocketmq-store/README.md) assembles the broker-facing store. [rocketmq-store-local](../rocketmq-store-local/README.md) and [rocketmq-store-rocksdb](../rocketmq-store-rocksdb/README.md) own concrete backend components.

Default features are empty. Run from the root workspace:

```bash
cargo test -p rocketmq-store-api --test capability_contracts --test result_contracts --test ha_contracts
```

Other contract tests cover reads, derived cursors, and timer identity. Licensed under [Apache-2.0](../LICENSE-APACHE).
