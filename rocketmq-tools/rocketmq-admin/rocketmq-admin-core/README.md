# rocketmq-admin-core

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-admin-core` is the presentation-independent administration layer for
RocketMQ Rust tooling. It owns the reusable request models, validation rules,
service orchestration, admin client lifecycle helpers, and structured results
used by `rocketmq-admin-cli`, `rocketmq-admin-tui`, tests, and future admin
adapters.

This crate is intentionally not a command-line parser, terminal renderer, or
TUI state layer. Adapters translate user input into core DTOs, call a core
service, and render the returned data in their own format.

[中文文档](README-zh_cn.md)

## Architecture

![rocketmq-admin-core architecture](../../../resources/admin-core-architecture.svg)

The stable integration path is:

```text
adapter input -> core request DTO -> core service -> admin client/remoting -> structured result -> adapter renderer
```

This keeps RocketMQ administration behavior reusable across interfaces while
allowing each adapter to own its presentation concerns.

## What This Crate Provides

- Presentation-independent request and result DTOs for RocketMQ admin domains.
- Domain services for topic, broker, consumer, message, auth, export, NameServer,
  controller, cluster, queue, offset, HA, lite, producer, connection, container,
  static topic, and stats operations.
- `AdminBuilder`, `AdminGuard`, `create_admin`, and `create_admin_with_guard`
  helpers for admin client startup, timeout configuration, RPC hook injection,
  and resource cleanup.
- `DefaultMQAdminExt` integration over RocketMQ client/remoting APIs.
- Shared validation and resolver logic for broker, cluster, topic, and
  NameServer targeting.
- A common error surface via `RocketMQResult`, `RocketMQError`, and `ToolsError`.
- Optional local RocksDB metadata export support behind the `rocksdb-export`
  feature.

## Quick Start

Use the crate from another workspace member:

```toml
[dependencies]
rocketmq-admin-core = { path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core" }
```

Query topic clusters through the full core request lifecycle:

```rust
use rocketmq_admin_core::core::RocketMQResult;
use rocketmq_admin_core::core::topic::{TopicClusterQueryRequest, TopicService};

async fn list_topic_clusters() -> RocketMQResult<()> {
    let request = TopicClusterQueryRequest::try_new("TestTopic")?
        .with_optional_namesrv_addr(Some("127.0.0.1:9876".to_string()));

    let result = TopicService::query_topic_clusters(request).await?;
    println!("clusters: {:?}", result.clusters);

    Ok(())
}
```

Manage an admin client directly when multiple operations should share one
connection lifecycle:

```rust
use rocketmq_admin_core::core::RocketMQResult;
use rocketmq_admin_core::core::admin::AdminBuilder;
use rocketmq_admin_core::core::topic::TopicService;

async fn query_with_shared_admin() -> RocketMQResult<()> {
    let mut admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .instance_name("admin-core-example")
        .timeout_millis(5000)
        .build_with_guard()
        .await?;

    let clusters = TopicService::get_topic_cluster_list(&mut admin, "TestTopic").await?;
    println!("clusters: {:?}", clusters.clusters);

    Ok(())
}
```

Run the package tests:

```bash
cargo test -p rocketmq-admin-core
```

## Core Domains

| Module | Purpose |
|---|---|
| `core::admin` | Admin client builder, RAII guard, RPC hook support, and lifecycle helpers. |
| `core::topic` | Topic route, status, list, create/update, delete, order config, static-topic support points, and topic permissions. |
| `core::broker` | Broker configuration, runtime status, consume stats, epoch, cold-data-flow control, commitlog read-ahead, and broker maintenance operations. |
| `core::consumer` | Subscription group, consume mode, consumer progress, monitoring, and consumer configuration operations. |
| `core::message` | Message query, trace, send, direct consume, decode, and compaction-log related operations. |
| `core::auth` | User and ACL create, update, delete, list, get, and copy operations. |
| `core::export_data` | Metadata, metrics, POP records, config export, and optional local RocksDB metadata export. |
| `core::namesrv` | NameServer config, KV config, and broker write permission operations. |
| `core::controller` | Controller metadata, config, master election, and broker metadata cleanup. |
| `core::cluster`, `core::queue`, `core::offset`, `core::producer`, `core::connection`, `core::container`, `core::ha`, `core::lite`, `core::stats` | Supporting RocketMQ administration domains used by CLI and TUI adapters. |

## Feature Flags

| Feature | Default | Purpose |
|---|---:|---|
| `rocksdb-export` | No | Enables direct local RocksDB metadata export in `core::export_data`. The default build keeps RocksDB out of consumers that only need RPC-backed administration. |

Use the feature only for tools that need to inspect local RocksDB metadata:

```toml
rocketmq-admin-core = {
    path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core",
    features = ["rocksdb-export"],
}
```

## Boundary Contract

`rocketmq-admin-core` keeps administration behavior reusable by excluding
presentation dependencies:

- No `clap` or `clap_complete`; command parsing belongs to `rocketmq-admin-cli`.
- No `colored`, `tabled`, `indicatif`, or `dialoguer`; terminal rendering and
  prompts belong to adapter crates.
- No `ratatui`; TUI state and layout belong to `rocketmq-admin-tui`.
- No dependency on `rocketmq-admin-cli`.

The `tests/no_cli_ui_boundary.rs` test protects this boundary and also verifies
that local RocksDB support remains feature-gated.

## Crate Layout

```text
rocketmq-admin-core/
├── src/
│   ├── lib.rs                         # Public crate surface
│   ├── admin/                         # DefaultMQAdminExt and admin utilities
│   └── core/                          # Presentation-independent admin domains
│       ├── admin.rs                   # AdminBuilder and AdminGuard
│       ├── broker/                    # Broker service and DTOs
│       ├── namesrv/                   # NameServer service and DTOs
│       ├── topic/                     # Topic service and DTOs
│       └── *.rs                       # Other domain services
├── examples/
│   └── admin_builder_pattern.rs       # AdminBuilder and RAII usage examples
└── tests/                             # Core model and boundary tests
```

## Adding an Admin Operation

1. Add a request DTO that trims and validates its own inputs.
2. Add a result DTO that contains adapter-neutral data, not table rows or
   terminal strings.
3. Implement the operation in the relevant `*Service` type.
4. Keep the admin lifecycle explicit: accept an existing admin client for
   shared lifecycles, or provide a request-based helper that creates and shuts
   down the admin client.
5. Add focused model/service tests in `rocketmq-admin-core`.
6. Map CLI/TUI parameters to the new core DTO in adapter crates.

## Validation

For documentation-only changes, local Markdown/SVG checks are usually enough.
For Rust changes in this crate, run:

```bash
cargo test -p rocketmq-admin-core
cargo test -p rocketmq-admin-core --features rocksdb-export
```

For root workspace Rust changes, also run the repository-required checks from
the workspace root:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Related Crates

- [`rocketmq-admin-cli`](../rocketmq-admin-cli) - command-line adapter for core admin services.
- [`rocketmq-admin-tui`](../rocketmq-admin-tui) - terminal UI adapter for core admin services.
- [`rocketmq-remoting`](../../../rocketmq-remoting) - RocketMQ remoting protocol and RPC types.
- [`rocketmq-client`](../../../rocketmq-client) - RocketMQ client APIs used by the admin client implementation.
- [`rocketmq-common`](../../../rocketmq-common) - shared RocketMQ data structures and utilities.
- [`rocketmq-error`](../../../rocketmq-error) - shared error and result types.

## License

Licensed under the [Apache License, Version 2.0](../../../LICENSE-APACHE).
