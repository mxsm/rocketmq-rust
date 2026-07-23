# rocketmq-store

[![Crates.io](https://img.shields.io/crates/v/rocketmq-store.svg)](https://crates.io/crates/rocketmq-store)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE-APACHE)

`rocketmq-store` is the storage layer for the
[rocketmq-rust](https://github.com/mxsm/rocketmq-rust) workspace. It provides
the broker-facing `MessageStore` boundary, local file based CommitLog and
ConsumeQueue storage, RocksDB-backed metadata storage, index building, timer
message support, POP checkpoint models, HA replication services, store
statistics, and optional tiered storage and observability integrations.

The crate is designed for RocketMQ broker internals and for contributors who
need to work on persistence, recovery, dispatch, replication, or storage
performance in the Rust implementation.

[中文文档](README-zh_cn.md)

## Architecture

![rocketmq-store architecture](../resources/store-architecture.svg)

The store is organized around a few stable boundaries:

- **`MessageStore` trait**: the broker-facing API for lifecycle, message writes,
  reads, offset lookup, key query, commit-log access, HA metadata, cleanup, and
  runtime information.
- **`GenericMessageStore`**: feature-gated enum wrapper that delegates to
  `LocalFileMessageStore` or `RocksDBMessageStore`.
- **Local file store**: the default implementation built around CommitLog,
  mapped files, ConsumeQueue, index files, checkpoints, flush services, and
  recovery.
- **RocksDB store**: an optional implementation boundary that keeps the local
  file CommitLog path while storing consume queue, index, timer, and transaction
  metadata through RocksDB services.
- **Dispatch pipeline**: CommitLog append produces dispatch requests that build
  derived indexes such as ConsumeQueue, key index, compaction metadata,
  RocksDB metadata, and optional tiered-store dispatch.
- **Background services**: allocation, flush, recovery, cleanup, HA replication,
  timer message processing, statistics, and optional observability metrics.

## Capabilities

- Append single and batched broker messages to CommitLog.
- Read messages by topic, queue, logical offset, size limit, or physical
  CommitLog offset.
- Query messages by key through index services.
- Maintain queue offsets, logical ConsumeQueue files, batch consume queues, and
  consume queue extension metadata.
- Recover CommitLog and ConsumeQueue state after restart, including optimized
  parallel CommitLog loading through the default `fast-load` feature.
- Support sync and async flush policies through `FlushDiskType`.
- Support HA replication semantics for sync master, async master, slave, and
  controller-mode role transitions.
- Maintain broker and store statistics through `StoreStatsService` and
  `BrokerStatsManager`.
- Provide timer message data structures and timer RocksDB dispatch support.
- Provide POP acknowledgement and checkpoint models.
- Optionally store consume queue, index, timer, and transaction metadata in
  RocksDB.
- Optionally dispatch message data to `rocketmq-tieredstore`.
- Optionally expose OpenTelemetry-compatible metrics through
  `rocketmq-observability`.

## Quick Start

Build the default local-file store implementation from the workspace root:

```bash
cargo build -p rocketmq-store
```

Use it as a workspace dependency:

```toml
[dependencies]
rocketmq-store = { path = "../rocketmq-store" }
```

Create and start a local file message store:

```rust
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;

async fn start_store() -> Result<(), rocketmq_store::store_error::StoreError> {
    let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
    let mut store = LocalFileMessageStore::new(
        Arc::new(MessageStoreConfig::default()),
        Arc::new(BrokerConfig::default()),
        topic_table,
        None,
        false,
    );
    store.wire_owned_root_dependencies()?;

    store.init().await?;
    if store.load().await {
        store.start().await?;
    }

    store.shutdown().await;
    Ok(())
}
```

Enable RocksDB-backed metadata storage when building or testing RocksDB paths:

```bash
cargo build -p rocketmq-store --features rocksdb_store
```

## Feature Flags

| Feature | Default | Description |
| --- | --- | --- |
| `local_file_store` | yes | Enables the default local file message store. |
| `fast-load` | yes | Enables optimized parallel CommitLog loading. |
| `safe-load` | no | Keeps the safe sequential loading path available for fallback scenarios. |
| `rocksdb_store` | no | Enables RocksDB modules and `RocksDBMessageStore`. |
| `rocksdb-store` | no | Compatibility alias for `rocksdb_store`. |
| `data_store` | no | Compatibility feature that enables `local_file_store`. |
| `io_uring` | no | Enables Linux-only `tokio-uring` mapped-file backend experiments. |
| `tieredstore` | no | Enables integration with `rocketmq-tieredstore`. |
| `observability` | no | Enables OpenTelemetry metric emission through `rocketmq-observability`. |
| `observability-traces` | no | Enables observability crate integration without the metrics feature set. |

The default feature set is `["local_file_store", "fast-load"]`.

## Core API Surface

| Area | Important Types |
| --- | --- |
| Store boundary | `MessageStore`, `GenericMessageStore`, `LocalFileMessageStore`, `RocksDBMessageStore` |
| Configuration | `MessageStoreConfig`, `FlushDiskType`, `StoreType`, store path helpers |
| Write path | `CommitLog`, `DefaultAppendMessageCallback`, `PutMessageResult`, `AppendMessageResult` |
| Read path | `GetMessageResult`, `SelectMappedBufferResult`, `QueryMessageResult` |
| Dispatch | `CommitLogDispatcher`, `DispatchRequest`, ConsumeQueue and index dispatchers |
| Queues | `ConsumeQueueStore`, `ConsumeQueue`, `BatchConsumeQueue`, `ConsumeQueueExt` |
| Mapped files | `MappedFile`, `DefaultMappedFile`, `MappedFileQueue`, `TransientStorePool` |
| Index | `IndexService`, `IndexFile`, `IndexHeader`, `QueryOffsetResult` |
| RocksDB | `RocksDbStore`, `RocksDbConfig`, RocksDB consume queue, index, timer, and transaction services |
| HA | `GeneralHAService`, `DefaultHAService`, `AutoSwitchHAService`, HA clients and connections |
| Timer and POP | `TimerMessageStore`, timer log/wheel/checkpoint types, `AckMsg`, `BatchAckMsg`, `PopCheckPoint` |
| Stats and hooks | `StoreStatsService`, `BrokerStatsManager`, put-message and send-message-back hooks |

## Storage Model

### Local File Store

The default store persists message bodies in CommitLog mapped files and builds
logical indexes for broker reads:

1. `put_message` or `put_messages` encodes broker messages and appends them to
   CommitLog.
2. CommitLog produces `DispatchRequest` values.
3. Dispatchers update ConsumeQueue, index, compaction metadata, timer state, and
   optional tiered or RocksDB metadata.
4. `get_message` uses topic/queue logical offsets to locate CommitLog physical
   offsets and returns selected message buffers.
5. Recovery uses checkpoints, CommitLog scanning, and ConsumeQueue rebuilding to
   restore store state.

### RocksDB Store

When the `rocksdb_store` feature is enabled and `MessageStoreConfig.store_type`
is `StoreType::RocksDB`, `RocksDBMessageStore` is available. It currently keeps
CommitLog storage on the local file path and moves metadata ownership into
RocksDB-backed services:

- consume queue entries and queue offsets
- key index records
- optional timer records
- optional transaction records
- RocksDB maintenance operations such as flush, compaction, checkpoint, and
  backup scheduling

This gives the broker a concrete RocksDB boundary without changing the shared
`MessageStore` API.

## Reliability and Recovery

- CommitLog load supports the default `fast-load` path and a sequential fallback
  path.
- Recovery tests cover CommitLog loading, file truncation, recovery positions,
  and dirty data handling.
- HA tests cover sync-master slave timeouts, `wait_store_msg_ok` behavior, and
  controller-mode role transitions.
- RocksDB foundation and store semantics tests cover Java-compatible defaults,
  column family layout, range scans, index records, timer records, transaction
  records, and generic store delegation.
- Timer recovery integration tests cover delayed-message restart behavior.

## Crate Layout

```text
rocketmq-store/
  src/base/                 MessageStore trait, results, dispatch, stats, checkpoints
  src/config/               MessageStoreConfig, flush policy, store path helpers
  src/log_file/             CommitLog, mapped files, flush manager, recovery, fast load
  src/message_store/        LocalFileMessageStore and optional RocksDBMessageStore
  src/queue/                ConsumeQueue implementations and queue stores
  src/index/                key index file and service
  src/ha/                   replication clients, connections, and services
  src/rocksdb/              optional RocksDB store, codecs, CFs, maintenance, indexes
  src/timer/                timer message store, timer log, wheel, checkpoint, metrics
  src/pop/                  POP ack and checkpoint models
  src/stats/                broker and store statistics
  src/tieredstore.rs        optional tiered-store dispatcher adapter
  tests/                    recovery, HA, RocksDB, timer, and performance tests
  benches/                  CommitLog, mapped buffer, delivery, zero-copy, RocksDB benches
```

## Development

Useful checks while working on this crate:

```bash
cargo test -p rocketmq-store --lib
cargo test -p rocketmq-store --test commitlog_recovery_tests
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_foundation_tests
cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings
```

Benchmark targets are available for focused storage performance work:

```bash
cargo bench -p rocketmq-store --bench commit_log_performance
cargo bench -p rocketmq-store --bench mapped_buffer_bench
cargo bench -p rocketmq-store --features rocksdb_store --bench rocksdb_store
```

Run broader workspace validation when store API, recovery behavior,
feature-gated storage paths, or broker-facing semantics change.

## Design Boundaries

- The crate is a broker storage component, not a standalone broker process.
- The local file store is the default production path in the workspace.
- RocksDB support is feature-gated and models metadata storage while still
  reusing the local file CommitLog path.
- Timer, tiered storage, RocksDB, and observability paths are intentionally
  feature-gated so the default store remains focused.
- Many APIs are broker-internal and optimized for RocketMQ semantics rather than
  a general-purpose embedded database interface.

## License

Licensed under the Apache License, Version 2.0. See
[`LICENSE-APACHE`](../LICENSE-APACHE) for details.
