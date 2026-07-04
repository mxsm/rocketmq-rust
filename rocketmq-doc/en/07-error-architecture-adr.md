---
title: "Error Architecture Redesign ADR"
permalink: /docs/error-architecture-adr/
excerpt: "Accepted direction for the RocketMQ Rust error architecture redesign."
last_modified_at: 2026-07-04T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Redesign ADR

## Status

Accepted.

## Context

RocketMQ Rust already has a central `rocketmq-error` crate and a widely used
`RocketMQError` type. The current design is still transitional:

- `RocketmqError`, `LegacyRocketMQResult`, `LegacyResult`, and legacy macros are
  still public.
- `rocketmq-error::Result<T>` is an alias for `anyhow::Result<T>`.
- Remoting, proxy gRPC, dashboard HTTP, admin tools, broker processors, and
  NameServer processors map errors locally.
- Some domain errors still collapse into string-only variants such as
  `Internal(String)` or `General(String)`.
- Client callbacks and request futures still expose dynamic error values.
- Credential formatting previously printed secret material in
  `SessionCredentials::Display`.

Compatibility with the legacy error API is not a goal for this redesign. The
project should converge on the latest error architecture instead of preserving
old aliases or adapters.

## Decision

Use a single error architecture:

**Typed Error Kernel + ErrorSpec Registry + Boundary Adapter +
Redaction/Observability Contract**.

The central contract is:

- `rocketmq-error` owns the public error kernel:
  `RocketMQError`, `RocketMQResult<T>`, `ErrorKind`, `ErrorCode`,
  `ErrorCategory`, `ErrorScope`, `RetryClass`, `ErrorSeverity`,
  `ErrorSpec`, `ErrorContext`, and `Sensitive<T>`.
- Public library APIs return typed errors. Application entry points, tests,
  and one-shot tools may use `anyhow` only at their outermost boundary.
- Every cross-boundary error has stable metadata for protocol mapping, retry,
  severity, redaction, metrics, logs, traces, and runbook routing.
- External exits use boundary adapters. `rocketmq-error` can define primitive
  specs, but it must not depend on remoting, proxy, dashboard, or frontend
  crates.
- Sensitive fields are redacted by default in `Display`, `Debug`, structured
  logs, and exported error reports.
- Legacy error types, legacy result aliases, and legacy macro output are removed
  after callers are migrated.

## Dependency Direction

`rocketmq-error` stays below all protocol and application crates.

```text
rocketmq-error
  -> rocketmq-common
  -> rocketmq-remoting
  -> rocketmq-client
  -> rocketmq-broker / rocketmq-namesrv / rocketmq-controller / rocketmq-store
  -> rocketmq-proxy / dashboard backends / tools
```

Boundary crates convert primitive specs into their local protocol types:

| Boundary | Local adapter output |
| --- | --- |
| Remoting | `ResponseCode` and response remark |
| Proxy gRPC | `v2::Code` and `tonic::Code` |
| Dashboard HTTP | HTTP status, API code, and public message |
| CLI/tools | exit category and concise user message |
| Observability | low-cardinality labels and redacted fields |

## Delivery Sequence

Each PR must keep the affected Cargo project buildable. The legacy API is
deleted only after macro, remoting, client, and public alias callers are moved.

| PR | Scope | Goal |
| --- | --- | --- |
| 01 | docs | Accept the no-compatibility error redesign ADR |
| 02 | docs + scan evidence | Add error inventory and ownership matrix |
| 03 | `rocketmq-client` | Redact credential formatting |
| 04 | `rocketmq-error` | Introduce `ErrorKind`, `ErrorCode`, and `ErrorScope` |
| 05 | `rocketmq-error` | Introduce the `ErrorSpec` registry |
| 06 | `rocketmq-error` | Add redacted `ErrorContext` and `Sensitive<T>` |
| 07 | `rocketmq-error` | Add protocol primitive specs |
| 08 | `rocketmq-error` | Add recovery and observability policies |
| 09 | broker, controller, namesrv, examples | Replace `rocketmq_error::Result` callers |
| 10 | `rocketmq-error` | Remove public `anyhow` aliases |
| 11 | `rocketmq-macros` | Generate typed `RocketMQError` values |
| 12 | `rocketmq-remoting` | Migrate codec and serialization errors |
| 13 | `rocketmq-remoting` | Add the remoting boundary adapter |
| 14 | broker and namesrv | Route processor failures through the adapter |
| 15 | `rocketmq-client` | Replace callback dynamic error downcasts |
| 16 | `rocketmq-client` | Centralize retry decisions |
| 17 | `rocketmq-store` | Replace `StoreError::General` with typed variants |
| 18 | `rocketmq-controller` | Preserve raft, storage, and serde sources |
| 19 | `rocketmq-auth` | Split authentication, authorization, config, and storage errors |
| 20 | common and tools | Remove public `anyhow` and unclassified internals |
| 21 | `rocketmq-proxy` | Use the gRPC boundary adapter |
| 22 | dashboard web backend | Use a typed `RocketMQError` wrapper |
| 23 | standalone projects | Align examples and dashboard apps with typed errors |
| 24 | `rocketmq-error` | Delete the legacy `RocketmqError` API |
| 25 | scripts/CI | Add legacy, public-anyhow, mapping, and redaction guards |
| 26 | docs | Publish contribution guide and runbooks |

## Acceptance Gates

Root workspace Rust changes must finish with:

```powershell
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Focused tests should also run for the modified crate, for example:

```powershell
cargo test -p rocketmq-client-rust --lib session_credentials_display
```

Static checks must eventually reject unapproved occurrences of:

```powershell
rg -n "RocketmqError|LegacyRocketMQResult|LegacyResult" --glob "*.rs"
rg -n "type Result<T> = anyhow::Result|pub type Result<T> = anyhow::Result" --glob "*.rs"
rg -n "Internal\\(String\\)|General\\(String\\)|to_string\\(\\)" --glob "*.rs"
rg -n "secretKey|SecurityToken|signature" rocketmq-client rocketmq-auth --glob "*.rs"
```

## Consequences

This is a breaking redesign. The short-term cost is a coordinated migration
across the error kernel, macros, remoting, client, store, controller, auth,
proxy, dashboard, tools, and standalone projects.

The long-term result is a single machine-readable error contract for protocol
mapping, retry behavior, operations, security redaction, and debugging.
