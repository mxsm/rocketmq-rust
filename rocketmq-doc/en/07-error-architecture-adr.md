---
title: "Error Architecture Redesign ADR"
permalink: /docs/error-architecture-adr/
excerpt: "Accepted direction for the RocketMQ Rust error architecture redesign."
last_modified_at: 2026-09-05T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Redesign ADR

## Status

Accepted and implemented. The workspace uses one opaque canonical error model;
there is no versioned compatibility facade or parallel central error API.

## Context and current state

`rocketmq-error` exposes a one-pointer, non-`Clone` `Error`, the canonical
`Result<T>` alias, `SharedError = Arc<Error>`, typed `ErrorContext`, safe views,
and an exact 136-entry declarative descriptor catalog. Each error retains one
direct typed source, the first canonical promotion location, and optional
catalog-controlled backtrace state.

Owning crates use the canonical value directly or expose a narrow facade.
`StoreError` owns one canonical `Error`; cloneable facades such as
`ClientError`, `RuntimeError`, and `TransportError` retain `SharedError`. Their
standard source chains lead through the canonical value to the original typed
leaf. Facades cannot override descriptor identity or boundary policy.

## Decision

Use an opaque canonical `Error` contract with five layers. The layers are
ordered by responsibility; `ErrorCatalog`, boundary adapters, and presentation
are supporting mechanisms, not additional error layers.

### Five-layer model

1. **Outcome/Decision/Rejection.** Public control-flow outcomes express a
   successful outcome, a decision, or a rejection without exposing leaf error
   representation. Retry is not inferred from a rejection's rendered text.
2. **ContractViolation.** Caller, protocol, and invariant violations have a
   distinct contract-violation shape. They are not mixed into operational leaf
   errors merely because both cross a boundary.
3. **Private leaf `Error`.** Domain implementation errors retain typed source
   chains for I/O, serde, storage, raft, transport, and runtime failures. Leaf
   representation and matching remain private to the owning implementation.
4. **Domain operational facade.** A domain such as storage exposes a narrow
   operational facade (for example, the existing opaque `StoreError` facade)
   instead of leaking its private leaf set. The facade preserves source and
   policy information needed by the canonical conversion.
5. **Opaque canonical `Error` with safe projections.** `rocketmq-error` owns
   one canonical value with private representation. Consumers use safe
   projections such as `PublicErrorView` or `DiagnosticView`; they do not match
   public error representation to classify failures.

The declarative `ErrorCatalog` is a supporting mechanism and the sole owner of
stable dotted codes, class, `CanonicalCondition`, fault attribution, component,
fixed public messages, severity, `RecoveryHint`, backtrace policy, exposure,
and projection metadata. Boundary adapters project catalog metadata into
remoting, gRPC, HTTP, CLI, and other local primitives.
Presentation and observability render the approved views with redaction; they
do not create a second semantic catalog.

### Canonical metadata and views

Each catalog entry owns a stable dotted code and its class,
`CanonicalCondition`, fault attribution, component, fixed public message,
severity, `RecoveryHint`, backtrace policy, exposure, projection, and ordered
field schema. A free-form remark or a source display string is not an entry
key, policy, or compatibility value.

Catalog codes use lowercase dotted stable domain semantics, for example
`storage.commit_log.corrupt_record`. A code must never contain a dynamic topic,
group, path, broker address, or other runtime value. `CanonicalCondition` is
protocol-independent. Domain facades and private leaf errors cannot override
the catalog code, condition, severity, `RecoveryHint`, or projection metadata.

Context has an explicit visibility class:

- `Public`: safe for the public error view and external protocol response;
- `Diagnostic`: available only to controlled diagnostics and operational
  telemetry; and
- `SecretPresenceOnly`: records only that a secret-bearing value was present,
  never the value itself.

`PublicErrorView` contains only catalog-approved identity, fixed public message,
safe public context, and boundary-safe projection fields. A descriptor with
`Exposure::Generic` exposes no dynamic public context. `DiagnosticView` adds
only descriptor-declared, bounded diagnostic values and value-free redaction
markers; it never renders or exposes the source, caller location, or backtrace.
Typed causes remain available through `std::error::Error::source()`.

`RetryDecision` is a separate decision, not a catalog field copied into an
error. It considers operation idempotency, operation stage, and remaining
budget together with the catalog's `RecoveryHint`; a response message or
source string cannot decide retry on its own.

### Public-surface rules

Three accidental contracts remain prohibited:

- **Public leaf errors:** do not add consumer-facing enum variants,
  constructors, or exhaustive matching surfaces merely to represent a new
  boundary condition. Keep leaf representation private behind the domain
  facade and canonical conversion.
- **Arbitrary remarks:** remarks are bounded presentation text only. They are
  not stable codes, public messages, retry signals, or catalog entries. A
  boundary adapter must use the catalog and safe context rather than treating
  caller-supplied text as authoritative.
- **Source stringification:** `Display`, `Debug`, and
  `source().to_string()` output is diagnostic presentation, not stable data.
  Preserve typed sources where useful, but never classify, map, retry, persist,
  or compare an error by rendered source text.

## Current architecture

| Concern | Effective design |
| --- | --- |
| Public canonical value | Opaque `Error`, `Result<T>`, and `SharedError` |
| Outcome/control flow | Operation-specific outcomes and rejections remain distinct from failures |
| Contract failures | Caller, protocol, and invariant violations have explicit owning shapes |
| Leaf errors | Private typed leaves remain behind an owning facade or canonical conversion |
| Facades | Narrow owner facades preserve canonical identity, context, and typed sources |
| Catalog | One 136-entry declarative catalog owns all stable metadata and projections |
| Context/views | Typed visibility fields feed `PublicErrorView` and `DiagnosticView` |
| Retry | Decisions use idempotency, operation stage, remaining budget, and `RecoveryHint` |
| Boundaries | Adapters project descriptor metadata and safe views without parsing display text |
| Sensitive data | Public, diagnostic, and secret-presence-only visibility enforce redaction |

## Compatibility and dependency direction

The canonical Rust API has no public or private compatibility shim,
compatibility feature, or long-lived dual path. New code uses the current API
directly.

Externally observable protocol and storage contracts remain compatibility
surfaces: remoting numeric response codes and headers, gRPC and HTTP external
contracts, wire semantics, and persisted record layouts. Each affected change
supplies focused regression evidence; preserving a wire contract does not make
rendered error text a stable API.

`rocketmq-error` stays below all protocol and application crates:

```text
leaf Error / ContractViolation / domain facade
  -> rocketmq-error (opaque canonical Error + declarative ErrorCatalog)
  -> remoting / client / broker / namesrv / controller / store
  -> proxy / dashboard / tools boundary adapters
```

The central crate may define catalog primitives, canonical views, and redaction
rules, but it must not depend on remoting, proxy, dashboard, or frontend
crates.

| Boundary | Projection |
| --- | --- |
| Remoting | Preserved numeric `ResponseCode`, headers, and safe response remark |
| Proxy gRPC | Preserved external payload/status contract and local gRPC codes |
| Dashboard HTTP | Preserved HTTP/API contract and public message |
| CLI/tools | Exit category and concise public message |
| Observability | Redacted `DiagnosticView` fields and low-cardinality labels |

## Dependency-driven delivery

An error change is complete only when its producer and every affected consumer
use the same descriptor-owned contract. The delivery includes focused source,
redaction, and boundary tests plus compatibility evidence for any remoting,
gRPC, HTTP, wire, or persistence surface it touches.

## Lightweight governance and non-goals

Governance is intentionally lightweight: each implementation change gets
focused tests, the current error guard when applicable, targeted `rg` scans,
`git diff --check`, and relative Markdown link checks. Rust changes follow the
repository profile of package-scoped `cargo fmt -p <package> -- --check` plus
workspace Clippy; documentation-only changes do not require Cargo validation.

This architecture does not require content fingerprints, file hashes, complex
AST gates, custom Clippy policy, or a heavy platform.

## Acceptance gates

Every change must show that affected producers and consumers use opaque
canonical identity, the declarative catalog, and safe projections; that retry
remains a separate decision; and that public leaf matching, arbitrary remarks,
and source stringification are not semantic contracts.

## Consequences

Stable semantic meaning lives in the declarative catalog, typed causes remain
available through the standard source chain, and safe views prevent accidental
leakage or coupling to error representation. Compatibility remains explicit
for remoting, gRPC, HTTP, wire, and persistence contracts.
