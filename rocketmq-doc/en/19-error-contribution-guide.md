---
title: "Error Architecture Contribution Guide"
permalink: /docs/error-contribution-guide/
excerpt: "How to add, map, test, and review typed RocketMQ Rust errors."
last_modified_at: 2026-07-04T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Contribution Guide

This guide is the contribution contract for RocketMQ Rust error changes. The
current direction is no-compatibility typed errors: new code must use the
central `RocketMQError` architecture instead of adding legacy aliases or local
string-only mappings.

## Core Rules

1. Use `rocketmq_error::RocketMQError` and `RocketMQResult<T>` for library
   error contracts.
2. Do not reintroduce `RocketmqError`, `RocketMqError`,
   `LegacyRocketMQResult`, `LegacyResult`, or `rocketmq_error::Result`.
3. Keep `anyhow` at application, binary, test, and one-shot tool boundaries.
   Core library crates such as `rocketmq-error` and `rocketmq-common` must not
   expose public `anyhow::Result` contracts.
4. Add or reuse an `ErrorKind` and `ErrorSpec` entry before exposing a new
   cross-boundary error.
5. Boundary crates must map from `ErrorSpec` primitives instead of parsing
   display text.
6. Sensitive fields such as secret keys, tokens, signatures, and passwords must
   be redacted in `Display`, `Debug`, logs, and exported diagnostics.

## Where Changes Belong

| Change type | Owner |
| --- | --- |
| New stable kind, code, severity, retry class, or redaction contract | `rocketmq-error` |
| Remoting response code and remark conversion | `rocketmq-remoting::error_response` |
| gRPC payload/status conversion | `rocketmq-proxy::status` |
| Broker or NameServer external response conversion | Processor code using `rocketmq_remoting::error_response` |
| Dashboard or HTTP conversion | Dashboard boundary error wrapper |
| CLI display and exit behavior | CLI/tool boundary |
| Domain-local storage, auth, controller, or client source preservation | Owning crate, then convert to `RocketMQError` at the boundary |

## Adding A New Error

Use this sequence for any new cross-boundary error:

1. Add or reuse an `ErrorKind`.
2. Add a stable `ErrorCode` and `ErrorSpec` entry.
3. Define retry, severity, observability, remoting, gRPC, HTTP, and CLI
   primitive metadata.
4. Add a typed `RocketMQError` variant or domain error conversion.
5. Update the relevant boundary adapter if the default metadata is not enough.
6. Add focused tests for the new kind and the affected boundary.
7. Run the error guard and the relevant Cargo validation.

```powershell
python scripts/error_architecture_guard.py
cargo fmt --all
cargo test -p rocketmq-error
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Boundary Mapping Pattern

Boundary code should look up the spec and convert primitive metadata into local
wire types.

```rust
use rocketmq_error::RocketMQError;
use rocketmq_remoting::error_response;

fn to_response(error: &RocketMQError, opaque: i32) -> RemotingCommand {
    error_response::command_from_error_with_opaque(error, opaque)
}
```

Do not add new mapping tables beside the boundary adapters unless the adapter
itself is being extended.

## Redaction Pattern

Prefer `Sensitive<T>` or a small local redaction helper. Never format secret
material directly.

```rust
use rocketmq_error::Sensitive;

let secret = Sensitive::new(secret_key);
tracing::warn!(secret = %secret, "authentication failed");
```

For custom `Debug` implementations, sensitive fields must render as
`<redacted>` or route through an equivalent helper.

## Review Checklist

- Does the change add a typed error instead of a string-only internal error?
- Does the new error have a stable `ErrorKind` and `ErrorSpec`?
- Do remoting, gRPC, HTTP, CLI, retry, severity, and observability mappings
  come from the central spec?
- Are source errors preserved where they help debugging?
- Are credentials, tokens, signatures, passwords, and authorization values
  redacted?
- Does the change avoid public `anyhow` in library APIs?
- Did `python scripts/error_architecture_guard.py` pass?
- Did the affected crate tests and required clippy command pass?

## Useful References

- [Error Architecture Redesign ADR](07-error-architecture-adr.md)
- [Error Architecture Inventory](07-error-inventory.md)
- [Error Architecture Runbooks](19-error-runbooks.md)
