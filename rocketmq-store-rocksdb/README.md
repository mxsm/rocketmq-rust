# rocketmq-store-rocksdb

RocksDB-backed storage foundation for RocketMQ. The package owns database configuration, column families, codecs, consume-queue/index/timer/transaction derived state, snapshots, and checkpoint operations. It is integrated into the broker-facing store by [rocketmq-store](../rocketmq-store/README.md).

## Opening and ownership

`RocksDbConfig`, `RocksDbOpenPlan`, `RocksDbResourceBudget`, and `RocksDbStore` are root exports. `message_store::RocksDbMessageStore` composes the derived-state databases and services.

`RocksDbOpenPlan::from_config` checks enabled/valid configuration without opening a database; it returns `None` for disabled or invalid configuration and does not inspect filesystem state. Opening planned state requires a `ChildServiceContext`. Runtime-backed maintenance and rebuild work remains owned by that context. Database opening and native I/O can still fail after plan validation.

The derived-state integration accepts a `WalPort` for primary-log access. Do not infer that selecting the RocksDB backend replaces every commitlog operation or that an index update establishes primary-log durability.

## Data and maintenance surfaces

See [`column_family`](src/column_family.rs), [`codec`](src/codec.rs), [`consume_queue`](src/consume_queue.rs), and [`message_store`](src/message_store.rs) for layout and composition. [`read_only`](src/read_only.rs) and [`release_checkpoint`](src/release_checkpoint.rs) serve inspection/checkpoint use cases with separate contracts from ordinary writes. Persisted keys, profile markers, and checkpoint formats are compatibility surfaces.

Default crate features are empty, but the native RocksDB dependency is unconditional. Enabling `rocksdb_store` on `rocketmq-broker` or `rocketmq-store` selects integration support there; the backend still needs runtime configuration. Building this package requires the native toolchain used by its `rocksdb` dependency.

## Validation

Run from the root workspace:

```bash
cargo test -p rocketmq-store-rocksdb --test foundation
cargo test -p rocketmq-store-rocksdb --test release_checkpoint
```

Use isolated temporary database directories for tests. Licensed under [Apache-2.0](../LICENSE-APACHE).
