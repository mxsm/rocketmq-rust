---
title: "Rust API entry points"
---

Use this page to locate the public API for a task and generate documentation from the source you build. The website's Next documentation, a registry package and a source checkout can describe different revisions. Read the package manifest and selected Cargo features together with Rustdoc; an item visible in another build is not necessarily available in yours.

## Select an API by responsibility

| Task | Cargo package / Rust crate | Public starting point and contract |
| --- | --- | --- |
| Send, consume or request a reply | `rocketmq-client-rust` / `rocketmq_client_rust` | Crate-root producer/consumer builders, request types and `ClientRuntime`; [client configuration](../configuration/client-config.md) explains construction and lifecycle |
| Represent a message, queue or result | `rocketmq-model` / `rocketmq_model` | Canonical domain types; [message model](../architecture/message-model.md) explains identity and offset units |
| Own process and service work | `rocketmq-runtime` / `rocketmq_runtime` | `RuntimeOwner`, `RuntimeOwnerPlan`, `RootServiceContext`, `ChildServiceContext`, `TaskGroup`, `BlockingExecutor` and `ShutdownReport` |
| Extend storage through a capability | `rocketmq-store-api` / `rocketmq_store_api` | `MessageAppender`, `MessageReader`, `OffsetIndex`, `ReplicationControl`, `StoreHealth` and `StoreLifecycle`; use the coherent ports supplied by the store composition |
| Decode a wire request | `rocketmq-protocol` / `rocketmq_protocol` | Request/response codes, headers and serialization; combine with [protocol and transport](../architecture/protocol-transport.md) |
| Exchange frames and manage sessions | `rocketmq-transport` / `rocketmq_transport` | Transport interfaces and configuration; a successful local write does not prove remote processing |
| Classify a failure | `rocketmq-error` / `rocketmq_error` | Catalog identities and typed context; [errors](./errors.md) explains preserving identity across boundaries |
| Configure telemetry | `rocketmq-observability` / `rocketmq_observability` | Configuration, telemetry ownership and handles; [observability design](../architecture/errors-observability.md) explains shutdown and export |

This is an entry-point index, not a promise that every public item in these crates is an equally stable extension point. The Client and Runtime maintain deliberate exports in `src/public_api.rs`. Prefer those documented root exports over reaching into implementation modules. Existing compatibility re-exports can remain public while being deprecated; follow their replacement notes in [Rust API migration](../migration/rust-api.md).

## Generate and open local Rustdoc

Run from the repository root with the selected Rust toolchain:

```bash
cargo doc -p rocketmq-client-rust --no-deps --open
cargo doc -p rocketmq-runtime -p rocketmq-store-api --no-deps
```

With the default Cargo target directory, the first command opens `target/doc/rocketmq_client_rust/index.html`; the other crate indexes are `target/doc/rocketmq_runtime/index.html` and `target/doc/rocketmq_store_api/index.html`. `CARGO_TARGET_DIR` or a configured target directory changes the location. Use Rustdoc's item search and source links to navigate types, trait implementations and feature annotations.

`--no-deps` limits the generated documentation; dependencies still need to be processed by Cargo. Native prerequisites can therefore matter. A missing Clang, C++ toolchain or `protoc` belongs to the selected dependency graph, not the prose renderer. Consult [features and platforms](./features-platforms.md) before enabling storage or Proxy features.

For a read-only client administration integration, generate exactly that surface:

```bash
cargo doc -p rocketmq-client-rust --no-default-features --features admin-read --no-deps --open
cargo tree -p rocketmq-client-rust --no-default-features --features admin-read -e features
```

The client defaults to `admin-full`, which enables both `admin-read` and `admin-mutation`. `MQAdminReadExt` is conditional on `admin-read`, while `MQAdminMutationExt` is conditional on `admin-mutation`. Cargo features select compiled capabilities; they do not authorize a caller against a live Broker. Credentials, policies and service-specific restrictions still apply.

Generate independent applications from their own manifests. For example, the read-only MCP project is outside the main Cargo workspace:

```bash
cargo doc --manifest-path rocketmq-ai/rocketmq-mcp/Cargo.toml --no-deps
```

A binary-only package can document internal items for its own executable. That output does not turn the application into a supported library dependency. Use its protocol/configuration documentation for external integrations.

## Read a signature as an operational contract

Before implementing an integration, answer four questions:

1. **Who owns it?** A clonable handle can share access without owning process shutdown. Keep the `RuntimeOwner` and service shutdown sequence explicit; an `Arc` alone does not join background work.
2. **What does completion mean?** Enqueuing work, writing a frame, appending a record, crossing a durable watermark and completing business work are distinct observations. Read the result type and [message lifecycle](../architecture/message-lifecycle.md).
3. **What happens after failure or cancellation?** Retain the error identity and operation context. A timeout can leave a remote result unknown. Read `Errors`, `Panics` and `Safety` sections where present; cancellation is not rollback.
4. **What enables the item?** Check the feature, platform and runtime conditions. Type visibility alone does not establish that an endpoint, storage backend or credentials have been configured.

For a concrete producer or consumer implementation, start with the [owned first-message example](../getting-started/quick-start.md) and the [producer](../producer/overview.md) or [consumer](../consumer/overview.md) guide. Use Rustdoc for exact signatures instead of copying incomplete initialization fragments from an implementation module.

## Keep application documentation aligned

Record the dependency version or source reference used by your application and its feature selection in the normal project manifest. When upgrading, regenerate the relevant local Rustdoc and review deprecation, default, wire and persistence changes separately. Do not substitute a `latest` API page for the version your application uses. There is no fingerprint or fixed-checkout requirement for writing documentation.

## Source references

- [Client manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml), [crate exports](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/lib.rs) and [deliberate public API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/public_api.rs).
- [Runtime public API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-runtime/src/public_api.rs) and [Store API exports](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-api/src/lib.rs).
- [Workspace manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml) and [MCP manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp/Cargo.toml).
