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

The default build exposes admin-owned contracts with shared model, protocol, error and security types,
without the optional Client/Transport adapter dependencies. SDK integration is feature-gated.
The full `client-adapter` provides the session/services used by CLI and TUI; narrower read and
mutation adapters expose their own capability boundaries.

## Features

| Feature | Default | Purpose |
|---|---:|---|
| `read-client-adapter` | No | Read-only SDK adapter capabilities. |
| `mutation-client-adapter` | No | Mutation SDK adapter capabilities. |
| `client-adapter` | No | Enables the RocketMQ Client-backed `AdminSession` and adapter implementations. |
| `tls` | No | Enables certificate-verified TLS transport for SDK adapter sessions. |
| `rocksdb-export` | No | Enables direct local RocksDB metadata export for the admin tools that need it. |

Contract-only consumers can use the default build:

```toml
[dependencies]
rocketmq-admin-core = { path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core" }
```

Runtime consumers enable the adapter explicitly:

```toml
[dependencies.rocketmq-admin-core]
path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core"
features = ["client-adapter"]
```

Add `tls` when sessions may enable TLS. Certificate verification uses the transport's
configured trust roots; enabling a session's TLS option alone does not enable a
Cargo feature. The Tauri desktop explicitly selects both `client-adapter` and `tls`.

## Explicit Session Lifecycle

An `AdminSession` owns its Client SDK handle. Callers must close it explicitly;
dropping a session never starts detached cleanup work. The example requires `client-adapter`.
Pass an application-owned `Arc<ClientRuntime>`; after all sessions close, shut down that shared
runtime and its `RuntimeOwner`. See the [client lifecycle example](../../../rocketmq-client/README.md).

```rust,no_run
use rocketmq_admin_core::core::AdminResult;
use rocketmq_admin_core::client_adapter::AdminBuilder;
use rocketmq_admin_core::core::topic::ListTopicsRequest;
use rocketmq_admin_core::core::topic::TopicAdmin;

async fn list_topics(
    client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
) -> AdminResult<()> {
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
- Keep SDK and transport integration behind the adapter feature boundaries.
- Keep SDK facades and raw hooks out of core request/result contracts. The full adapter
  intentionally re-exports `ClientRuntime` types for application lifecycle injection.
- Keep command parsing and rendering in CLI/TUI crates.
- Close every started `AdminSession` on both success and failure paths.

`tests/boundary_source_guard.rs` protects the source and feature boundaries.

## Local Validation

```bash
cargo fmt -p rocketmq-admin-core -- --check
cargo test -p rocketmq-admin-core --features client-adapter
cargo check -p rocketmq-admin-core --no-default-features
cargo check -p rocketmq-admin-core --no-default-features --features read-client-adapter
```

Validation is local. Generated logs, reports, and one-off validation scripts
belong under ignored local output such as `target/`, not in the repository.

## Related Crates

- [`rocketmq-admin-cli`](../rocketmq-admin-cli)
- [`rocketmq-admin-tui`](../rocketmq-admin-tui)
- [`rocketmq-mcp`](../../../rocketmq-ai/rocketmq-mcp)
- [`rocketmq-client`](../../../rocketmq-client)

## License

Licensed under the [Apache License, Version 2.0](../../../LICENSE-APACHE).
