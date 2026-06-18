# rocketmq-auth

[English](README.md) | [简体中文](README-zh_cn.md)

Authentication, authorization, ACL migration, and auth runtime support for [RocketMQ-Rust](../README.md).

`rocketmq-auth` provides the auth layer used by RocketMQ-Rust services and clients. It focuses on RocketMQ-compatible
access-key authentication, ACL-based authorization, Java-style ACL file compatibility, local metadata providers,
stateful cache support, and remoting request integration.

## Capabilities

| Area | What it provides |
|------|------------------|
| Authentication | Access-key based authentication, remoting signature parsing, request timestamp validation, and pluggable authentication strategies. |
| Authorization | Subject/resource/action based ACL authorization, allow/deny policy evaluation, super-user bypass, and default-deny behavior. |
| ACL compatibility | Java-style `plain_acl.yml` loading, recursive ACL file discovery, global/account white remote address support, and legacy ACL migration. |
| Runtime integration | `AuthRuntime` for broker/proxy integration, remoting command checks, metadata seeding, ACL reloads, and cache invalidation. |
| Metadata | Local authentication and authorization metadata providers with persisted snapshots under `authConfigPath`. |
| Observability | Lightweight counters for ACL reloads, cache hits/misses, signatures, whitelist checks, authentication, and authorization results. |

## Crate Layout

| Module | Purpose |
|--------|---------|
| [`authentication`](src/authentication.rs) | Authentication contexts, providers, strategies, ACL signing, and client RPC hook support. |
| [`authorization`](src/authorization.rs) | Authorization contexts, providers, handlers, strategies, ACL models, resources, and policies. |
| [`acl`](src/acl.rs) | Java ACL file loading, validation, storage helpers, and white remote address matching. |
| [`runtime`](src/runtime.rs) | Service-facing auth runtime that wires providers, ACL files, reloads, caches, and remoting checks. |
| [`config`](src/config.rs) | `AuthConfig` with serde-compatible camelCase fields and redacted debug output for secrets. |
| [`migration`](src/migration.rs) | Legacy ACL model compatibility used when migrating from RocketMQ v1 ACL files. |
| [`observability`](src/observability.rs) | Auth metrics snapshots and stable metric sample names. |
| [`permission`](src/permission.rs) | Permission conversion helpers used by ACL migration and policy generation. |

## Installation

Inside this workspace, use the package directly:

```toml
[dependencies]
rocketmq-auth = { path = "../rocketmq-auth" }
```

For external consumers, use the published workspace version:

```toml
[dependencies]
rocketmq-auth = "1.0.0"
```

Optional gRPC metadata parsing support is available through the `grpc` feature:

```toml
[dependencies]
rocketmq-auth = { version = "1.0.0", features = ["grpc"] }
```

## Quick Start

Create an auth runtime from `AuthConfig` and point it at a RocketMQ-style ACL file:

```rust
use cheetah_string::CheetahString;
use rocketmq_auth::config::AuthConfig;
use rocketmq_auth::AuthRuntimeBuilder;

#[tokio::main]
async fn main() -> rocketmq_error::RocketMQResult<()> {
    let config = AuthConfig {
        auth_config_path: CheetahString::from_static_str("store/auth"),
        acl_file: CheetahString::from_static_str("conf/plain_acl.yml"),
        authentication_enabled: true,
        authorization_enabled: true,
        acl_file_watch_enabled: true,
        ..AuthConfig::default()
    };

    let runtime = AuthRuntimeBuilder::new(config).build().await?;

    // Broker/proxy integrations call check_remoting(...) with a channel context
    // and RemotingCommand before serving protected requests.
    let metrics = runtime.metrics_snapshot();
    println!("auth reload attempts: {}", metrics.acl_reload_attempts);

    runtime.shutdown().await?;
    Ok(())
}
```

A minimal `plain_acl.yml`:

```yaml
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: alice-secret
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: DENY
    topicPerms:
      - TopicA=PUB
    groupPerms:
      - GroupA=SUB
```

The runtime loads the file into local authentication and authorization metadata. When `aclFileWatchEnabled` is enabled,
file changes are reloaded in the background; failed reloads preserve the previous working snapshot.

## Configuration

`AuthConfig` is serde-compatible with camelCase field names. Important fields include:

| Field | Purpose |
|-------|---------|
| `authConfigPath` | Directory/file root used by local metadata providers for persisted users and ACL snapshots. |
| `aclFile` | Java-style ACL YAML file or directory to load. Directories are searched recursively for `.yml` and `.yaml` files. |
| `aclFileWatchEnabled` | Enables background ACL file reloads. |
| `aclFileWatchIntervalMillis` | Polling interval for ACL file reloads. Defaults to `5000`. |
| `authenticationEnabled` | Enables authentication checks in `AuthenticationService`. |
| `authorizationEnabled` | Enables authorization checks in `AuthorizationService`. |
| `authenticationWhitelist` | Comma-separated remoting request codes that bypass authentication. |
| `authorizationWhitelist` | Comma-separated remoting request codes that bypass authorization. |
| `signatureAlgorithm` | Java-compatible signature algorithm: `HmacSHA1`, `HmacSHA256`, or `HmacMD5`. |
| `requestTimestampExpiredMillis` | Optional replay-protection window. `0` keeps Java-compatible no-expiry behavior. |
| `migrateAuthFromV1Enabled` | Enables legacy ACL migration through the v1 plain permission manager. |

## Examples

Run the focused examples from the workspace root:

```bash
cargo run -p rocketmq-auth --example authentication_strategy_usage
cargo run -p rocketmq-auth --example authentication_manager_usage
cargo run -p rocketmq-auth --example authorization_evaluator_usage
cargo run -p rocketmq-auth --example acl_authorization_handler_usage
cargo run -p rocketmq-auth --example metadata_provider_example
```

## Validation

For this crate:

```bash
cargo test -p rocketmq-auth
cargo test -p rocketmq-auth --test java_alignment
cargo test -p rocketmq-auth --examples --no-run
```

The `java_alignment` integration test covers Java-visible behavior such as remoting signature content, legacy
`plain_acl.yml` semantics, deny precedence, super-user bypass, safe ACL reloads, factory caching, and request context
model fields.

Workspace-level validation is run from the repository root:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmarks

Run focused authentication and authorization hot-path benchmarks with:

```bash
cargo bench -p rocketmq-auth --bench auth_hot_path_bench
```

The benchmark covers remoting signature calculation, white remote address matching, ACL policy matching, and stateful
authentication/authorization cache hits.

For CI or local regression gating, first verify the benchmark target compiles:

```bash
cargo test -p rocketmq-auth --benches --no-run
```

When collecting a baseline, run the benchmark on an otherwise idle machine and keep the generated Criterion report under
`target/criterion/auth_*`. Compare future changes against the same toolchain and hardware; auth hot-path regressions should be investigated before merging changes that touch signing, whitelist matching, ACL policy matching, or stateful cache keys.

## License

RocketMQ-Rust is licensed under the Apache License 2.0. See [../LICENSE-APACHE](../LICENSE-APACHE).
