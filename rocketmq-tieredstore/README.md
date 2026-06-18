# rocketmq-tieredstore

[![Crates.io](https://img.shields.io/crates/v/rocketmq-tieredstore.svg)](https://crates.io/crates/rocketmq-tieredstore)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE-APACHE)

`rocketmq-tieredstore` is the tiered storage layer for the
[rocketmq-rust](https://github.com/mxsm/rocketmq-rust) workspace. It provides a
broker-facing dispatch and fetch model for moving older or policy-selected
messages into a secondary storage layer while preserving RocketMQ queue-offset,
timestamp, and key-query semantics.

The crate includes a `TieredStore` composition root, async dispatch pipeline,
logical message fetcher, flat-file segment model, JSON metadata store, POSIX and
in-memory providers, recovery and cleanup services, and an integration boundary
used by `rocketmq-store`.

[中文文档](README-zh_cn.md)

## Architecture

![rocketmq-tieredstore architecture](../resources/tieredstore-architecture.svg)

The tiered store is organized around six main areas:

- **`TieredStore`**: the composition root that wires configuration, metadata,
  flat files, dispatcher, fetcher, recovery, cleanup, and shutdown tokens.
- **Dispatcher path**: accepts `TieredDispatchRequest` values, validates message
  body size, appends message bytes to CommitLog segments, writes ConsumeQueue
  units, commits metadata, and builds key/unique-key indexes.
- **Fetcher path**: reads messages by topic, queue, and logical queue offset;
  resolves offsets by store timestamp; and queries messages by key or unique
  key.
- **Flat-file model**: manages topic/queue flat files backed by CommitLog,
  ConsumeQueue, and Index file segments.
- **Metadata model**: persists topics, queues, file segments, and index entries
  through `JsonMetadataStore`.
- **Provider model**: stores segment bytes through `TieredStoreProvider`
  implementations. The bundled providers are POSIX files and in-memory storage.

## Capabilities

- Dispatch message bodies and queue metadata into tiered CommitLog and
  ConsumeQueue segments.
- Preserve RocketMQ logical queue offsets through fixed-size 20-byte
  ConsumeQueue units.
- Query tiered messages by queue offset, store timestamp boundary, key, or
  unique key.
- Persist and recover metadata for topics, queues, file segments, and index
  entries.
- Recover flat files after restart and reconcile half-committed segment sizes
  against the provider's real segment size.
- Clean expired segments and index metadata according to retention settings.
- Choose tiered storage policy with `TieredStorageLevel`.
- Use POSIX file storage for durable local tiered data.
- Use the in-memory provider for tests, examples, and custom embedding.
- Integrate with `rocketmq-store` through the tiered CommitLog dispatcher
  adapter.

## Quick Start

Build the crate from the workspace root:

```bash
cargo build -p rocketmq-tieredstore
```

Use it as a workspace dependency:

```toml
[dependencies]
rocketmq-tieredstore = { path = "../rocketmq-tieredstore" }
```

Create a store with the in-memory provider, dispatch one message, drain the
dispatcher on shutdown, and fetch the message by queue offset:

```rust
use bytes::Bytes;
use rocketmq_error::RocketMQError;
use rocketmq_tieredstore::{
    TieredDispatchRequest, TieredDispatcher, TieredLifecycle, TieredMessageFetcher,
    TieredStorageLevel, TieredStore, TieredStoreConfig,
};

async fn example() -> Result<(), RocketMQError> {
    let store = TieredStore::new(TieredStoreConfig {
        storage_level: TieredStorageLevel::Force,
        backend_provider: "memory".to_owned(),
        max_pending_tasks: 16,
        ..TieredStoreConfig::default()
    })?;

    store.load().await?;
    store.start().await?;

    let body = Bytes::from_static(b"hello-tieredstore");
    store
        .dispatcher()
        .dispatch(TieredDispatchRequest {
            topic: "ExampleTopic".to_owned(),
            queue_id: 0,
            queue_offset: 0,
            commit_log_offset: 0,
            message_size: body.len() as i32,
            tags_code: 0,
            store_timestamp: 1_700_000_000,
            keys: Some("example-key".to_owned()),
            uniq_key: Some("example-uniq".to_owned()),
            offset_id: None,
            sys_flag: 0,
            body: Some(body),
        })
        .await?;

    store.shutdown().await?;

    let fetched = store
        .fetcher()
        .get_message("ExampleTopic".to_owned(), 0, 0, 1)
        .await?;

    assert_eq!(fetched.messages.len(), 1);
    Ok(())
}
```

Run the bundled example:

```bash
cargo run -p rocketmq-tieredstore --example basic_memory_tieredstore
```

Use the default POSIX provider for local durable tiered data:

```rust
use rocketmq_tieredstore::{TieredStorageLevel, TieredStore, TieredStoreConfig};

let store = TieredStore::new(TieredStoreConfig {
    storage_level: TieredStorageLevel::Force,
    backend_provider: "posix".to_owned(),
    store_path_root_dir: "./store/tieredstore".into(),
    ..TieredStoreConfig::default()
})?;
# Ok::<_, rocketmq_error::RocketMQError>(())
```

## Feature Flags

| Feature | Default | Description |
| --- | --- | --- |
| `posix-provider` | yes | Enables the POSIX file provider. |
| `memory-provider` | yes | Enables the in-memory provider used by tests and examples. |
| `serde` | yes | Enables JSON metadata serialization and deserialization. |
| `rocketmq-store-integration` | no | Enables integration surface used by `rocketmq-store`. |

The default feature set is `["posix-provider", "memory-provider", "serde"]`.

## Storage Levels

| Level | Meaning |
| --- | --- |
| `Disable` | Tiered storage dispatch is disabled. |
| `NotInDisk` | Tiered storage is enabled for data no longer expected in disk-level local storage. |
| `NotInMem` | Tiered storage is enabled for data no longer expected in memory-level local storage. |
| `Force` | Tiered storage dispatch is forced for eligible requests. |

`TieredStorageLevel::check` compares levels by policy strength, and
`TieredStorageLevel::enabled` determines whether dispatch accepts valid
requests.

## Core API Surface

| Area | Important Types |
| --- | --- |
| Composition | `TieredStore`, `TieredStoreConfig`, `TieredStorageLevel`, `TieredLifecycle` |
| Dispatch | `TieredDispatcher`, `DefaultTieredDispatcher`, `TieredDispatchRequest` |
| Fetch | `TieredMessageFetcher`, `DefaultTieredMessageFetcher`, `TieredGetMessageResult`, `TieredQueryResult` |
| Flat files | `TieredFlatFileStore`, `TieredFlatFile`, `TieredFileSegment`, `FileSegmentType`, `FileSegmentStatus` |
| Segments | `CommitLogSegment`, `ConsumeQueueSegment`, `IndexFileSegment`, `ConsumeQueueUnit`, `TieredIndexEntry` |
| Metadata | `TieredMetadataStore`, `JsonMetadataStore`, `TopicMetadata`, `TopicQueueMetadata`, `FileSegmentMetadata` |
| Providers | `TieredStoreProvider`, `ProviderKind`, `PosixProvider`, `MemoryProvider` |
| Services | `CommitLogRecoverService`, `TieredRecoverResult`, cleanup service set |

## Data Model

### Write Path

1. `rocketmq-store` or an embedding component converts CommitLog dispatch data
   into `TieredDispatchRequest`.
2. `DefaultTieredDispatcher` accepts the request through a bounded Tokio channel.
3. The dispatcher validates topic, queue id, queue offset, message size, and
   body length.
4. `TieredFlatFile` appends message bytes into a CommitLog segment and writes a
   ConsumeQueue unit that points back to the tiered CommitLog offset.
5. `TieredFlatFileStore` writes key and unique-key entries when indexing is
   enabled.
6. `JsonMetadataStore` persists queue and segment metadata.

### Read Path

`DefaultTieredMessageFetcher` resolves reads through the flat-file store:

- `get_message` reads ConsumeQueue units and then CommitLog ranges.
- `get_message_timestamp` extracts store timestamp data from message bytes.
- `get_offset_by_time` and `get_offset_by_time_with_boundary` resolve lower or
  upper queue-offset boundaries by timestamp.
- `query_message` uses tiered index entries to fetch messages by normal key or
  unique key.

### Segment Layout

Segment paths follow the topic/queue/segment-type/base-offset model:

```text
<topic>/<queue-id>/commitlog/<base-offset>
<topic>/<queue-id>/consumequeue/<base-offset>
```

Metadata is persisted at:

```text
<store_path_root_dir>/config/tieredStoreMetadata.json
```

## Integration With `rocketmq-store`

The `rocketmq-store` crate owns broker-local CommitLog dispatch. Its optional
tieredstore adapter converts store `DispatchRequest` values into
`TieredDispatchRequest` values and forwards them to `DefaultTieredDispatcher`.
This keeps the tiered storage crate independent while preserving a clear broker
integration point.

## Reliability and Recovery

- `TieredLifecycle::load` runs `CommitLogRecoverService`, loads JSON metadata,
  reconstructs flat files, and recovers index entries.
- Recovery corrects segment metadata when the metadata size differs from the
  provider's real segment size.
- `TieredLifecycle::start` starts the dispatcher and cleanup service when
  deletion is enabled.
- `TieredLifecycle::shutdown` cancels background work, drains queued dispatch
  requests, and shuts down flat-file state.
- POSIX persistence tests cover restart recovery, queue-offset fetch, timestamp
  lookup, key query, and unique-key query.

## Crate Layout

```text
rocketmq-tieredstore/
  src/config.rs                 storage policy, provider selection, retention, read-ahead settings
  src/store.rs                  TieredStore composition root and lifecycle wiring
  src/dispatcher/               dispatch request model and async dispatcher
  src/fetcher/                  queue-offset, timestamp, and key query fetcher
  src/file/                     flat files, CommitLog, ConsumeQueue, Index segments
  src/metadata/                 JSON metadata store and metadata records
  src/provider/                 POSIX and memory segment providers
  src/service/                  recovery and cleanup background services
  examples/                     runnable in-memory tiered store example
  tests/                        POSIX persistence and recovery tests
  benches/                      tieredstore benchmark target
```

## Development

Useful checks while working on this crate:

```bash
cargo test -p rocketmq-tieredstore --lib
cargo test -p rocketmq-tieredstore --test posix_persistence_tests
cargo clippy -p rocketmq-tieredstore --all-targets --all-features -- -D warnings
```

Run the benchmark target when working on segment IO, dispatch throughput, or
fetch performance:

```bash
cargo bench -p rocketmq-tieredstore --bench tieredstore_bench
```

Run broader workspace validation when changing provider traits, dispatch
semantics, or `rocketmq-store` integration behavior.

## Design Boundaries

- This crate implements the tiered storage layer, not the broker process.
- Bundled providers are POSIX files and in-memory storage. Remote object-store
  providers can be added behind `TieredStoreProvider`.
- Metadata currently uses `JsonMetadataStore` when the `serde` feature is
  enabled.
- Dispatch is asynchronous and bounded by `max_pending_tasks`; shutdown drains
  queued requests before returning.
- `TieredDispatchRequest` requires a message body whose length matches
  `message_size`.
- The `rocketmq-store` integration is deliberately an adapter boundary so the
  tiered store remains independently testable.

## License

Licensed under the Apache License, Version 2.0. See
[`LICENSE-APACHE`](../LICENSE-APACHE) for details.
