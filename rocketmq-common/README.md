# rocketmq-common

[English](README.md) | [简体中文](README-zh_cn.md)

Shared message models, protocol constants, configuration types, and utility infrastructure for
[RocketMQ-Rust](../README.md).

`rocketmq-common` is the foundational crate used across the RocketMQ-Rust workspace. It contains Java-compatible
RocketMQ data structures, message helpers, broker and NameServer configuration models, compression and serialization
utilities, consistent hashing, statistics primitives, and runtime support code shared by the broker, client, NameServer,
store, proxy, and tools crates.

This crate is intentionally not a standalone RocketMQ service. It provides the common building blocks that higher-level
crates use to implement RocketMQ-compatible behavior.

## Capabilities

| Area | What it provides |
|------|------------------|
| Message model | `Message`, `MessageExt`, broker-inner messages, batch messages, queue metadata, message accessors, message properties, delay metadata, recall handles, and message ID helpers. |
| Configuration | Broker, controller, NameServer, server, TLS, plain-access, and common config models with Java-style field naming where needed. |
| Protocol constants | Permissions, system flags, action/resource models, consumer and producer constants, topic attributes, queue types, MQ version data, and RocketMQ protocol keys. |
| Routing and topology | `MessageQueue`, topic metadata, key builders, namespace helpers, lite topic helpers, and consistent-hash routing with virtual nodes. |
| Serialization and compression | Serde JSON helpers, optional SIMD JSON utilities, LZ4/Zlib/Zstd compression support, CRC, Base64, MD5, and byte conversion helpers. |
| Runtime utilities | File, environment, time, network, HTTP, string, and config-file helpers, plus thread pool and executor service abstractions. |
| Observability and statistics | Metrics exporter enums, no-op metric wrappers, statistics items, snapshots, scheduled printers, and running stats models. |

## Crate Layout

| Path | Purpose |
|------|---------|
| [`src/lib.rs`](src/lib.rs) | Public crate exports used by the rest of the workspace. |
| [`src/common.rs`](src/common.rs) | Aggregates RocketMQ domain modules such as message, broker, controller, topic, stats, metrics, and compression. |
| [`src/common/message`](src/common/message) | Message model, builder, properties, decoder, batch formats, queue models, and broker-side message variants. |
| [`src/common/broker`](src/common/broker) | Broker configuration and role models shared by broker runtime code. |
| [`src/common/controller`](src/common/controller) | Controller configuration primitives. |
| [`src/common/namesrv`](src/common/namesrv) | NameServer addressing, configuration, and update callback helpers. |
| [`src/common/compression`](src/common/compression) | Compression trait, type dispatch, and LZ4/Zlib/Zstd implementations. |
| [`src/common/consistenthash`](src/common/consistenthash) | Consistent-hash router, node trait, hash function abstraction, and virtual-node support. |
| [`src/common/lite`](src/common/lite) | Lite topic/subscription DTOs and offset helpers. |
| [`src/common/metrics`](src/common/metrics) | Metrics exporter types and no-op metric instruments. |
| [`src/common/statistics`](src/common/statistics) | Statistics manager, item state, formatting, interception, and scheduled output primitives. |
| [`src/common/stats`](src/common/stats) | Java-compatible stats item sets, snapshots, and call snapshots. |
| [`src/utils.rs`](src/utils.rs) | Utility module exports for files, JSON, CRC, time, networking, config parsing, and general helpers. |
| [`src/thread_pool.rs`](src/thread_pool.rs) | Thread pool and executor service abstractions. |
| [`src/log.rs`](src/log.rs) | Logging initialization helpers used by examples and service crates. |

## Requirements

- Rust `1.85.0` or newer is the workspace minimum.
- Build this crate with the repository toolchain from [`../rust-toolchain.toml`](../rust-toolchain.toml). The crate
  currently enables a nightly Rust feature.
- No RocketMQ broker or NameServer is required for local examples, tests, or utility usage in this crate.

## Installation

Inside this workspace:

```toml
[dependencies]
rocketmq-common = { path = "../rocketmq-common" }
```

For external consumers:

```toml
[dependencies]
rocketmq-common = "1.0.0"
```

Optional features:

```toml
[dependencies]
rocketmq-common = { version = "1.0.0", features = ["async_fs"] }
```

```toml
[dependencies]
rocketmq-common = { version = "1.0.0", features = ["simd"] }
```

## Quick Start

Create a RocketMQ message with the builder API:

```rust
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::RocketMQResult;

fn main() -> RocketMQResult<()> {
    let message = Message::builder()
        .topic("TopicTest")
        .tags("TagA")
        .key("order-1001")
        .body_slice(b"Hello RocketMQ")
        .build()?;

    println!("topic: {}", message.topic());
    println!("tags: {:?}", message.tags());
    println!("keys: {:?}", message.keys());

    Ok(())
}
```

Route keys with the consistent-hash router:

```rust
use rocketmq_common::common::consistenthash::{ConsistentHashRouter, Node};

#[derive(Clone)]
struct BrokerNode {
    address: String,
}

impl Node for BrokerNode {
    fn get_key(&self) -> &str {
        &self.address
    }
}

fn main() {
    let nodes = vec![
        BrokerNode {
            address: "broker-a:10911".to_string(),
        },
        BrokerNode {
            address: "broker-b:10911".to_string(),
        },
    ];

    let router = ConsistentHashRouter::new(nodes, 150);
    let node = router.route_node("order-1001").expect("router has nodes");
    println!("selected broker: {}", node.get_key());
}
```

Build and decode a recall-message handle:

```rust
use rocketmq_common::{RecallMessageHandle, RecallMessageHandleV1};

fn main() -> rocketmq_common::RocketMQResult<()> {
    let handle = RecallMessageHandleV1::build_handle(
        "TopicTest",
        "broker-a",
        "1707111111111",
        "message-id-1001",
    );

    let decoded = RecallMessageHandle::decode_handle(&handle)?;
    println!("recall topic: {}", decoded.topic());

    Ok(())
}
```

## Feature Flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `async_fs` | No | Enables Tokio-backed async file helpers in `utils::file_utils`. |
| `simd` | No | Enables `utils::simd_json_utils` through `simd-json` for high-throughput JSON parsing and serialization paths. |

## Examples

Run examples from the workspace root:

```bash
cargo run -p rocketmq-common --example message_builder_examples
cargo run -p rocketmq-common --example consistent_hash_router_example
cargo run -p rocketmq-common --example recall_message_handle_example
cargo run -p rocketmq-common --example json_comparison
cargo run -p rocketmq-common --features simd --example json_comparison
```

## Validation

Focused checks for this crate:

```bash
cargo test -p rocketmq-common --lib
cargo test -p rocketmq-common --features async_fs --lib
cargo test -p rocketmq-common --examples --no-run
cargo test -p rocketmq-common --features simd --examples --no-run
```

Workspace-level Rust validation is handled from the repository root when Rust code changes:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmarks

Criterion benchmarks cover selected utility hot paths:

```bash
cargo bench -p rocketmq-common --bench delete_property
cargo bench -p rocketmq-common --bench perm_name
cargo bench -p rocketmq-common --bench json_utils_comparison
```

## License

Licensed under the [Apache License, Version 2.0](../LICENSE-APACHE).
