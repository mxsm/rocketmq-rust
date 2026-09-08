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
- `ErrorDescriptor` and the single `ALL_DESCRIPTORS` catalog.
- Stable descriptor metadata: code, class, condition, fault attribution,
  component, fixed public message, severity, recovery hint, backtrace policy,
  exposure, four explicit boundary projections, and ordered field schemas.
- `ErrorContext`, `PublicErrorView`, `DiagnosticView`, and `CliErrorView`.

The crate intentionally does not depend on transport implementations or
generated protobuf bindings. Its remoting, gRPC, HTTP, and CLI projection types
are dependency-light values consumed by boundary adapters.

## Quick Start

```rust
use std::sync::Arc;

use rocketmq_error::{Error, Result, SharedError, TRANSPORT_ENDPOINT_INVALID};

fn validate_transport_endpoint(addr: &str) -> Result<()> {
    if addr.is_empty() {
        return Err(Error::new(&TRANSPORT_ENDPOINT_INVALID));
    }

    Ok(())
}

let error = validate_transport_endpoint("").expect_err("empty endpoint must fail");
let shared: SharedError = Arc::new(error);
assert_eq!(shared.code().as_str(), "transport.endpoint.invalid");
```

`Error` is the sole canonical envelope. It is intentionally not cloneable;
`SharedError` is `Arc<Error>` and preserves the same descriptor, context, and
typed source when an error needs multiple owners. Crate-specific facades should
carry `Error` or `SharedError` without reconstructing them from display text.

## Canonical Descriptors

Every canonical error selects exactly one immutable descriptor. Code that needs
stable behavior reads the descriptor rather than deriving policy from a display
string or caller override.

```rust
use rocketmq_error::{fields, Error, ErrorContext, ROUTE_TOPIC_NOT_FOUND};

let error = Error::new(&ROUTE_TOPIC_NOT_FOUND)
    .with_context(ErrorContext::new().with_text(fields::TOPIC, "TopicA"));
let descriptor = error.descriptor();

assert_eq!(descriptor.code().as_str(), "route.topic.not_found");
assert_eq!(descriptor.public_message(), "Topic route was not found");
assert_eq!(
    descriptor.recovery_hint(),
    rocketmq_error::RecoveryHint::RefreshRoute
);
```

`ALL_DESCRIPTORS` is the sole catalog. `descriptor_by_code` performs exact
lookup of canonical lowercase dotted codes. Stable behavior is selected
directly from descriptors; there is no central structural error kind or reverse
descriptor-to-kind mapping.

A descriptor explicitly owns all four projections:

- `RemotingSpec` for RocketMQ response codes.
- `GrpcSpec` for payload and transport status.
- `HttpSpec` for HTTP status.
- `CliSpec` for process exit status.

## Boundary Views and Redaction

Use `PublicErrorView` for approved public context fields at remoting, gRPC,
HTTP, dashboard, or other public adapters. Read protocol mappings directly
from the descriptor-owned projection. `CliErrorView` provides the corresponding
CLI projection.

```rust
use rocketmq_error::{fields, Error, ErrorContext, STORAGE_READ_FAILED};

let error = Error::new(&STORAGE_READ_FAILED).with_context(
    ErrorContext::new()
        .with_text(fields::STORE_OPERATION, "read")
        .with_text(fields::STORE_COMPONENT, "commitlog")
        .with_secret_presence(fields::STORE_DETAIL_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT),
);
let view = error.public_view().unwrap();

assert_eq!(view.code().as_str(), "storage.read.failed");
assert_eq!(view.message(), "Storage read failed");
assert_eq!(view.fields().count(), 0);
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
use rocketmq_error::Error;
use rocketmq_error::ErrorSeverity;
use rocketmq_error::CONTROLLER_LEADERSHIP_NOT_LEADER;

let error = Error::new(&CONTROLLER_LEADERSHIP_NOT_LEADER);
assert_eq!(
    error.descriptor().recovery_hint(),
    rocketmq_error::RecoveryHint::RefreshLeader
);
assert_eq!(error.descriptor().severity(), ErrorSeverity::Warn);
```

## Typed Sources

Use source-preserving constructors when a lower-level operation failed.
`std::error::Error::source()` retains the original typed cause; safe views
never stringify it.

```rust
use std::error::Error as _;
use rocketmq_error::{fields, Error, ErrorContext, PROTOCOL_BODY_INVALID};

let error = Error::caused_by(
    &PROTOCOL_BODY_INVALID,
    std::io::Error::other("private detail"),
)
.with_context(
    ErrorContext::new()
        .with_text(fields::OPERATION_DIAGNOSTIC, "decode header")
        .with_secret_presence(fields::INVALID_VALUE_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT),
);

assert!(error
    .source()
    .and_then(|source| source.downcast_ref::<std::io::Error>())
    .is_some());
let public = error.public_view().unwrap();
assert_eq!(public.fields().count(), 0);
```

## Public API Notes

- Stable integrations use descriptor codes and projections, not `Display`.
- Descriptor and projection construction is private; catalog constants are
  read-only public values.
- The crate maintains one current error model and does not provide a versioned
  compatibility facade.
- Component-specific adapters may expose narrow facades, but canonical identity,
  context, typed sources, and boundary projections remain descriptor-owned.

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
cargo test -p rocketmq-error --test typed_error_public_api
cargo test -p rocketmq-error --test shared_error_contract
cargo test -p rocketmq-error --test error_context_redaction
```

## License

Licensed under [Apache License, Version 2.0](../LICENSE-APACHE).

## Contributing

Contributions are welcome. Read the workspace
[Contributing Guide](../CONTRIBUTING.md) before submitting changes.
