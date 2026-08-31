---
title: "Error Architecture Redesign ADR"
permalink: /docs/error-architecture-adr/
excerpt: "Accepted direction for the RocketMQ Rust error architecture redesign."
last_modified_at: 2026-08-31T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Redesign ADR

## Status

Accepted. This decision supersedes the previously accepted transitional
public `RocketMQError` enum/`ErrorKind`/`ErrorSpec` architecture. The target
implementation remains pending; this document must not be read as evidence
that every target type or classification already exists.

## Context and current state

At the pinned inventory baseline,
`e016a878703767924529911240808c649ed4a801`, the repository has one large
public `RocketMQError` enum and a partially centralized error vocabulary. The
current implementation includes:

- `ErrorKind`, `ErrorSpec`, and `ALL_ERROR_SPECS` in `rocketmq-error`;
- a `DomainError` override path, free-string `ErrorContext`,
  `BoundaryErrorView`, and shared `Sensitive` support;
- typed client callback errors and an opaque `StoreError` facade;
- arbitrary boundary remarks and source `Display` output that are still
  observable but are not stable semantic contracts; and
- `anyhow`, configuration, and `serde_json` dependencies in `rocketmq-error`.

The baseline has no `RocketmqError` or `Legacy*` aliases, no public
`anyhow::Result` alias or callers, no dynamic client callback error, and no
`StoreError::General`. `AdminErrorCode::RocketMqError` in dashboard code is a
dashboard response-code name, not evidence of a legacy `rocketmq-error` type.

This distinction matters: the current mega-enum and existing support types are
effective repository APIs today, while the opaque canonical contract below is
the migration target.

## Decision

Adopt an opaque canonical `Error` contract with five layers. The layers are
ordered by responsibility; `ErrorCatalog`, boundary adapters, and presentation
are supporting mechanisms, not additional error layers.

This accepted decision replaces the transitional public enum architecture
described in the current-state section; migration is still required before the
target implementation is effective in each consumer.

### Target five-layer model

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
   a public mega-enum to classify failures.

The declarative `ErrorCatalog` is a supporting mechanism and the sole owner of
stable dotted codes, `CanonicalCondition`, fixed public messages, severity,
`RecoveryHint`, and projection metadata. Boundary adapters project catalog
metadata into remoting, gRPC, HTTP, CLI, and other local primitives.
Presentation and observability render the approved views with redaction; they
do not create a second semantic catalog.

### Canonical metadata and views

Each catalog entry owns a stable dotted code and its
`CanonicalCondition`, fixed public message, severity, `RecoveryHint`, and
projection metadata. A free-form remark or a source display string is not an
entry key, policy, or compatibility value.

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
safe public context, and boundary-safe projection fields. `DiagnosticView` may
add redacted diagnostic context and typed source information subject to its
visibility policy. Neither view treats arbitrary remarks or source
stringification as stable input.

`RetryDecision` is a separate decision, not a catalog field copied into an
error. It considers operation idempotency, operation stage, and remaining
budget together with the catalog's `RecoveryHint`; a response message or
source string cannot decide retry on its own.

### Public-surface freezes

The migration freezes three accidental contracts now:

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

## Current versus target

| Concern | Effective current repository | Target after migration |
| --- | --- | --- |
| Public error value | Large public `RocketMQError` enum | Opaque canonical `Error` with safe projections |
| Outcome/control flow | Existing APIs mix error and operation-specific outcomes | `Outcome`/`Decision`/`Rejection` are explicit public control-flow concepts |
| Contract failures | Not uniformly separated from operational errors | `ContractViolation` is a distinct layer |
| Leaf errors | Domain-specific leaves plus a domain override path | Private leaf `Error` retained behind each domain facade |
| Domain facades | Opaque `StoreError` exists; other domains vary | Narrow operational facades preserve source and policy information |
| Catalog | `ErrorKind`, `ErrorSpec`, and `ALL_ERROR_SPECS` exist, but the target declarative ownership is incomplete | `ErrorCatalog` solely owns dotted code, condition, fixed public message, severity, recovery hint, and projections |
| Context/views | Free-string `ErrorContext` and `BoundaryErrorView` exist | Visibility-tagged context feeds `PublicErrorView` and `DiagnosticView` |
| Retry | Decisions remain distributed | Separate `RetryDecision` uses idempotency, stage, and budget |
| Boundaries | Local adapters can emit arbitrary remarks and inspect display text | Adapters project catalog metadata and safe views |
| Dependencies | `rocketmq-error` still depends on anyhow/config/serde_json | Dependencies are reduced or isolated without widening the public contract |
| Sensitive data | Shared `Sensitive` support exists | Public, diagnostic, and secret-presence-only views enforce default redaction |

## Compatibility and dependency direction

The Rust API is allowed to break as this redesign lands. There will be no V1 or
V2 public compatibility layer, deprecated compatibility shim, compatibility
feature, or long-lived dual API. A private migration bridge may exist only
inside the wave that needs it and must be removed in that wave after its
producer and every consumer have migrated.

The redesign nevertheless preserves externally observable compatibility where
it is a protocol or storage contract: remoting numeric response codes and
headers, gRPC and HTTP external contracts, wire semantics, and persisted record
layouts. Each affected migration supplies focused regression evidence for
those surfaces; preserving a wire contract does not require preserving a Rust
error enum or rendered message.

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

| Boundary | Target projection |
| --- | --- |
| Remoting | Preserved numeric `ResponseCode`, headers, and safe response remark |
| Proxy gRPC | Preserved external payload/status contract and local gRPC codes |
| Dashboard HTTP | Preserved HTTP/API contract and public message |
| CLI/tools | Exit category and concise public message |
| Observability | Redacted `DiagnosticView` fields and low-cardinality labels |

## Dependency-driven delivery waves

The migration follows producer and consumer dependencies, not a fixed list of
PR numbers. A wave closes only after its producer changes and every known
consumer is migrated, focused tests and compatibility regression evidence pass,
and any private bridge introduced for that wave is removed. Bridges never
become public, deprecated, feature-gated, or long-lived APIs.

| Wave | Producer/consumer scope | Bridge removal condition |
| --- | --- | --- |
| 0 | Pin the inventory baseline; freeze leaf, remark, and source-string contracts; record protocol and persistence invariants | No bridge; baseline and evidence rules are established |
| 0B | Revalidate every inventory row and every current consumer against the pinned lexical baseline before changing ownership or status | No bridge; row-level and consumer-level evidence is required |
| 1 | Define Outcome/Decision/Rejection, ContractViolation, context visibility, views, and declarative catalog primitives | Remove temporary representations once all primitive producers and readers use the new schema |
| 2 | Convert domain leaf producers and operational facades, beginning with storage and other high-fan-out domains | Remove each private facade bridge after its leaf producers and all domain consumers migrate |
| 3 | Wrap migrated leaves and facades in opaque canonical `Error`; update central constructors and conversions | Remove enum-to-canonical bridges after all error producers and canonical consumers migrate |
| 4 | Migrate remoting, gRPC, HTTP, CLI, and observability consumers to safe projections | Remove each boundary bridge after its producer path and every boundary consumer have regression evidence |
| 5 | Migrate client retry decisions and remaining broker, namesrv, controller, auth, common, proxy, dashboard, tools, and standalone consumers | Remove per-domain private bridges only after producer plus consumer coverage is complete |
| 6 | Remove obsolete internal aliases, local semantic tables, display-text mapping, and transitional adapters | Remove the private bridge in the same wave; no compatibility shim survives as public API |
| 7 | Run focused tests, applicable guards, targeted scans, link checks, and protocol/persistence regression evidence | No bridge remains; inventory status changes only with evidence |

## Lightweight governance and non-goals

Governance is intentionally lightweight: each implementation change gets
focused tests, the current error guard when applicable, targeted `rg` scans,
`git diff --check`, and relative Markdown link checks. Rust changes follow the
repository profile of package-scoped `cargo fmt -p <package> -- --check` plus
workspace Clippy; documentation-only changes do not require Cargo validation.

This effort does not add content fingerprints, file hashes, complex AST gates,
custom Clippy policy, or a heavy platform. The pinned Git commit identifies the
inventory snapshot; it is not a content-fingerprint mechanism.

## Acceptance gates

The target is reached only when implementation and evidence show that each
migrated producer and consumer uses the opaque canonical identity, declarative
catalog, and safe projections; that retry remains a separate decision; and
that public leaf matching, arbitrary remarks, and source stringification are
not semantic contracts. The inventory records pending classification and
evidence rather than asserting completion.

## Consequences

This is a deliberate Rust API break with a narrow compatibility promise for
remoting, gRPC, HTTP, wire, and persistence contracts. Stable semantic meaning
lives in the declarative catalog, typed causes remain available to controlled
diagnostics, and safe public views prevent accidental leakage or coupling to
the mega-enum.
