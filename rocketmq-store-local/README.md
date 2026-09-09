# rocketmq-store-local

Local storage implementation components used by [rocketmq-store](../rocketmq-store/README.md). This is a library of commitlog, mapped-file, recovery, consume-queue, index, timer, HA, and checkpoint primitives; the broker-facing store is composed by `rocketmq-store`.

## Implementation boundaries

- [`commit_log`](src/commit_log) owns append framing, parsing, loading, and normal/abnormal recovery components.
- [`mapped_file`](src/mapped_file), [`flush`](src/flush), and [`transfer`](src/transfer) provide file lifetime, durability, and transfer mechanics.
- [`consume_queue`](src/consume_queue), [`index`](src/index), and [`derived`](src/derived) maintain derived state and progress.
- [`timer`](src/timer), [`ha`](src/ha), and [`services`](src/services) provide components integrated under an existing runtime/service owner.
- [`release_checkpoint`](src/release_checkpoint.rs) provides release-checkpoint support.

Operational results use `StoreResult<T>` with `rocketmq_store_api::StoreError`. File leases, append receipts, and progress/checkpoint types must retain their lifetime and durability meanings when crossing capability boundaries. Runtime-backed work uses supplied service contexts; dropping an arbitrary handle is not a complete shutdown protocol.

## Features

Default features are empty; the load policy allows parallel loading unless `safe-load` alone is enabled. `fast-load` takes precedence when both load features are enabled. A safe-load environment override can still force the sequential path; see [`load_orchestration.rs`](src/commit_log/load_orchestration.rs). `observability` enables metrics integration. `io_uring` adds an optional Linux backend and still requires platform/runtime capability checks; compiling the feature is not proof that the host can use it.

Selecting the broker's storage backend is a separate operation performed by `rocketmq-store` configuration. This package does not provide a broker binary or a standalone deployment configuration.

## Validation

Run from the root workspace. The [`tests`](tests) directory covers storage primitives, recovery, resource ownership, and lifecycle contracts. A declared benchmark is:

```bash
cargo bench -p rocketmq-store-local --bench commitlog_micro_batch
```

Keep storage layout, feature selection, and platform consistent when comparing measurements. Licensed under [Apache-2.0](../LICENSE-APACHE).
