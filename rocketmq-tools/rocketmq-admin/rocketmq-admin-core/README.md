# rocketmq-admin-core

> Runtime ownership: `client_runtime` in the examples is an application-owned `Arc<ClientRuntime>` created from a `RuntimeOwner` child scope and shut down at the process boundary.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-admin-core` is the presentation-independent administration boundary
for RocketMQ Rust. It owns admin request/result types, validation, typed errors,
security inputs, capability traits, and session configuration. CLI, TUI, MCP,
Dashboard, and examples consume this boundary instead of importing an admin
facade from the Client SDK.

[中文文档](README-zh_cn.md)

## Architecture

```text
CLI / TUI / MCP / Dashboard / Example
                |
                v
        admin-owned core contract
                |
                v
      optional Client adapter + session
                |
                v
          RocketMQ Client SDK
```

The always-available `core` module does not import RocketMQ Client, Common, or
Remoting. SDK types and protocol mapping are confined to `client_adapter`,
which is enabled explicitly with the `client-adapter` feature.

## Features

| Feature | Default | Purpose |
|---|---:|---|
| `client-adapter` | No | Enables the RocketMQ Client-backed `AdminSession` and adapter implementations. |
| `rocksdb-export` | No | Enables direct local RocksDB metadata export for the admin tools that need it. |

Contract-only consumers can use the default build:

```toml
rocketmq-admin-core = { path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core" }
```

Runtime consumers enable the adapter explicitly:

```toml
rocketmq-admin-core = {
    path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core",
    features = ["client-adapter"],
}
```

## Explicit Session Lifecycle

An `AdminSession` owns its Client SDK handle. Callers must close it explicitly;
dropping a session never starts detached cleanup work.

```rust
use rocketmq_admin_core::core::AdminResult;
use rocketmq_admin_core::client_adapter::AdminBuilder;
use rocketmq_admin_core::core::topic::ListTopicsRequest;
use rocketmq_admin_core::core::topic::TopicAdmin;

async fn list_topics() -> AdminResult<()> {
    let mut session = AdminBuilder::new(client_runtime.clone())
        .namesrv_addr("127.0.0.1:9876")
        .instance_name("admin-core-example")
        .build_and_start()
        .await?;

    let result = session.list_topics(&ListTopicsRequest::default()).await;
    session.shutdown().await;

    for topic in result?.topics {
        println!("{}", topic.topic);
    }
    Ok(())
}
```

Use `AdminCredentials` for authenticated operations. Its debug output is
redacted, and conversion to a Client RPC hook happens only inside the adapter.

## Source Layout

```text
rocketmq-admin-core/
├── src/
│   ├── lib.rs
│   ├── core/                 # Admin-owned contracts; always available
│   └── client_adapter/       # Client SDK integration; feature-gated
│       ├── lifecycle.rs      # AdminSession ownership
│       └── services/         # CLI/TUI command adapters
└── tests/                    # Contract, model, and boundary tests
```

The former `admin/`, `client_adapter/legacy/`, self aliases, and
`legacy-common-compat` feature have been removed. The project has not released
this API, so no compatibility facade is retained.

## Boundary Rules

- Keep request/result models and validation in `core`.
- Keep Client/Common/Remoting imports in `client_adapter`.
- Do not expose `DefaultMQAdminExt`, raw RPC hooks, or Client runtime types to
  consumers.
- Keep command parsing and rendering in CLI/TUI crates.
- Close every started `AdminSession` on both success and failure paths.

`tests/boundary_source_guard.rs` protects the source and feature boundaries.

## Local Validation

```bash
cargo fmt --all -- --check
cargo test -p rocketmq-admin-core --features client-adapter
cargo clippy -p rocketmq-admin-core --all-targets --all-features -- -D warnings
```

Validation is local. Generated logs, reports, and one-off validation scripts
belong under ignored local output such as `target/`, not in the repository.

## Related Crates

- [`rocketmq-admin-cli`](../rocketmq-admin-cli)
- [`rocketmq-admin-tui`](../rocketmq-admin-tui)
- [`rocketmq-mcp`](../../rocketmq-mcp)
- [`rocketmq-client`](../../../rocketmq-client)

## License

Licensed under the [Apache License, Version 2.0](../../../LICENSE-APACHE).
