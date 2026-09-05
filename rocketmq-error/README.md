# rocketmq-error

[English](README.md) | [简体中文](README-zh_cn.md)

[![Crates.io](https://img.shields.io/crates/v/rocketmq-error.svg)](https://crates.io/crates/rocketmq-error)
[![Documentation](https://docs.rs/rocketmq-error/badge.svg)](https://docs.rs/rocketmq-error)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE-APACHE)

`rocketmq-error` is the shared error kernel for the RocketMQ Rust workspace. It
provides typed causes, stable descriptor identity, explicit protocol
projections, bounded context, and redaction-safe boundary views.

## What This Crate Owns

- The opaque canonical `Error`, `Result<T>`, and `SharedError` types.
- `RocketMQError`, `RocketMQResult<T>`, and the retained domain error enums
  used throughout the workspace.
- `ErrorDescriptor` and the single `ALL_DESCRIPTORS` catalog.
- Stable descriptor metadata: code, class, condition, fault attribution,
  component, fixed public message, severity, recovery hint, backtrace policy,
  exposure, four explicit boundary projections, and ordered field schemas.
- `ErrorContext`, `PublicErrorView`, `DiagnosticView`,
  `BoundaryErrorView`, and `CliErrorView`.

The crate intentionally does not depend on transport implementations or
generated protobuf bindings. Its remoting, gRPC, HTTP, and CLI projection types
are dependency-light values consumed by boundary adapters.

## Quick Start

```rust
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

fn validate_broker_addr(addr: &str) -> RocketMQResult<()> {
    if addr.is_empty() {
        return Err(RocketMQError::Network(
            rocketmq_error::NetworkError::InvalidAddress {
                addr: addr.to_owned(),
            },
        ));
    }

    Ok(())
}
```

Nested domain errors such as `NetworkError`, `SerializationError`,
`ProtocolError`, `RpcClientError`, `AuthError`, `ControllerError`,
`ToolsError`, `FilterError`, `ObservabilityError`, and
`UnifiedServiceError` convert to `RocketMQError` through `From`.

## Canonical Descriptors

Every retained error leaf associates with exactly one immutable descriptor.
Code that needs stable behavior reads the descriptor rather than deriving policy
from an enum, display string, or caller override.

```rust
use rocketmq_error::RocketMQError;

let error = RocketMQError::route_not_found("TopicA");
let descriptor = error.descriptor();

assert_eq!(descriptor.code().as_str(), "route.topic.not_found");
assert_eq!(descriptor.public_message(), "Topic route was not found");
assert_eq!(
    descriptor.recovery_hint(),
    rocketmq_error::RecoveryHint::RefreshRoute
);
```

`ALL_DESCRIPTORS` is the sole catalog. `descriptor_by_code` performs exact
lookup of canonical lowercase dotted codes. `ErrorKind` remains a structural
typed discriminator for local exhaustive matching; it does not own a second
metadata or policy table.

A descriptor explicitly owns all four projections:

- `RemotingSpec` for RocketMQ response codes.
- `GrpcSpec` for payload and transport status.
- `HttpSpec` for HTTP status.
- `CliSpec` for process exit status.

## Boundary Views and Redaction

Use `boundary_view()` for remoting, gRPC, HTTP, CLI, dashboard, or other public
adapters. The view reads identity and projections from the descriptor and
enforces its exposure policy.

```rust
use rocketmq_error::RocketMQError;

let error = RocketMQError::storage_read_failed(
    "/var/lib/rocketmq/commitlog/00000000000000000000",
    "permission denied",
);
let view = error.boundary_view();

assert_eq!(view.code().as_str(), "storage.read.failed");
assert_eq!(view.message(), "Storage read failed");
assert!(view.context().is_empty());
```

For `Exposure::Generic`, a boundary view exposes the fixed message and no
dynamic public fields. For `Exposure::Public`, it exposes only fields whose
descriptor schema declares `ContextVisibility::Public`.

Original diagnostic context remains available on the typed error and through
`DiagnosticView`. Secret-bearing values are never stored in `ErrorContext`;
only bounded, value-free presence markers are recorded. Typed source text,
locations, and backtraces are not rendered by safe public views.

```rust
use rocketmq_error::fields;
use rocketmq_error::ErrorContext;

let context = ErrorContext::new()
    .with_text(fields::TOPIC, "TopicA")
    .with_secret_presence(fields::CREDENTIALS_PRESENT);

assert_eq!(context.to_string(), "topic=TopicA, credentials_present=<redacted>");
```

## Recovery and Severity

`RecoveryHint` is catalog-owned advice, not a complete retry decision.
Operation owners combine it with idempotency, progress, deadline, and retry
budget.

Current recovery hints are `Never`, `Backoff`, `RefreshRoute`,
`RefreshLeader`, `SwitchBroker`, `RefreshCredentials`, and
`OperatorAction`. Current severities are `Debug`, `Info`, `Warn`,
`Error`, and `Critical`.

```rust
use rocketmq_error::ErrorSeverity;
use rocketmq_error::RocketMQError;

let error = RocketMQError::ControllerNotLeader { leader_id: None };
assert_eq!(
    error.descriptor().recovery_hint(),
    rocketmq_error::RecoveryHint::RefreshLeader
);
assert_eq!(error.descriptor().severity(), ErrorSeverity::Warn);
```

## Typed Sources

Use source-preserving constructors when a lower-level operation failed.
`std::error::Error::source()` retains the original typed cause; boundary views
never stringify it.

```rust
use std::error::Error as _;
use rocketmq_error::RocketMQError;

let error = RocketMQError::request_header_source(
    "decode header",
    std::io::Error::other("private detail"),
);

assert!(error
    .source()
    .and_then(|source| source.downcast_ref::<std::io::Error>())
    .is_some());
assert!(error.boundary_view().context().is_empty());
```

## Public API Notes

- Stable integrations use descriptor codes and projections, not `Display`.
- Descriptor and projection construction is private; catalog constants are
  read-only public values.
- Deleted legacy `ErrorSpec`, recovery/observability policy tables, and
  category/scope metadata are not compatibility aliases.
- The six obsolete `ProtocolError` leaves, four conflicting
  `ControllerError` leaves, and the unused required-property leaf are removed.
  Retained callers use canonical `RocketMQError` variants and descriptors.

## Tests

Run from the workspace root:

```bash
cargo test -p rocketmq-error
cargo fmt -p rocketmq-error -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Focused catalog and association suites:

```bash
cargo test -p rocketmq-error --test error_descriptor_catalog
cargo test -p rocketmq-error --test legacy_descriptor_associations
cargo test -p rocketmq-error --test error_context_redaction
```

## License

Licensed under [Apache License, Version 2.0](../LICENSE-APACHE).

## Contributing

Contributions are welcome. Read the workspace
[Contributing Guide](../CONTRIBUTING.md) before submitting changes.
