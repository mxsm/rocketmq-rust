---
title: "Error Architecture Contribution Guide"
permalink: /docs/error-contribution-guide/
excerpt: "How to add, map, test, and review typed RocketMQ Rust errors."
last_modified_at: 2026-09-05T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Contribution Guide

This guide separates the canonical core that is effective now from the
remaining migration target. The [Error Architecture Redesign
ADR](07-error-architecture-adr.md) defines both, and the [Error Architecture
Inventory](07-error-inventory.md) pins the baseline and records per-owner
migration evidence.

## Effective-now rules

The effective central API now includes the one-pointer, non-`Clone` canonical
`Error`, `Result<T>`, `SharedError = Arc<Error>`, typed `ErrorContext`, safe
views, and the complete policy fields on the exact 98-entry declarative
catalog. `StoreError` owns one canonical `Error`; `RuntimeError` and
`TransportError` clone through `SharedError`. The public `RocketMQError` enum
and structural `ErrorKind` remain typed domain surfaces, but the legacy
`ErrorSpec`, scope/category metadata, and parallel recovery/projection tables
have been removed.

1. Preserve typed sources for I/O, serde, storage, raft, transport, and runtime
   failures. Do not replace a source with rendered text.
2. Keep public leaf error shapes frozen. Do not add consumer-facing enum
   variants, constructors, or exhaustive matching surfaces merely to represent
   a boundary condition.
3. Treat arbitrary boundary remarks as bounded presentation text only. They
   are not stable codes, public messages, retry signals, or catalog entries;
   caller-provided strings must not become authoritative protocol meaning.
4. Treat `Display`, `Debug`, and `source().to_string()` output as diagnostic
   presentation, never as data for classification, mapping, retry, persistence,
   or comparison.
5. Use the existing typed error contracts and local boundary mappings without
   widening public `anyhow` contracts. The baseline has no legacy
   `RocketmqError`/`Legacy*` aliases and no public anyhow `Result` alias or
   callers; do not reintroduce either.
6. Redact secret keys, tokens, signatures, passwords, authorization values,
   and equivalent sensitive data in public output, diagnostics, logs, traces,
   and exported reports. Use the repository's existing shared redaction
   support where available.

These rules apply now, including while a private migration bridge is present.
A bridge must remain private and temporary; it must not become a public,
deprecated, feature-gated, or long-lived dual API.

## Five-layer migration contract

When an owning migration lands, it follows these five layers exactly:

1. **Outcome/Decision/Rejection:** public control-flow outcomes without leaf
   representation; retry is not inferred from rendered text.
2. **ContractViolation:** caller, protocol, and invariant violations kept
   distinct from operational failures.
3. **Private leaf `Error`:** domain implementation errors with typed source
   chains and private representation.
4. **Domain operational facade:** a narrow domain facade that hides private
   leaves while retaining source and policy information.
5. **Opaque canonical `Error` with safe projections:** one canonical value with
   private representation, projected through `PublicErrorView` and
   `DiagnosticView`.

`ErrorCatalog`, boundary adapters, and presentation/observability are
supporting mechanisms, not additional layers. The declarative catalog is the
sole owner of stable dotted code, class, `CanonicalCondition`, fault
attribution, component, fixed public message, severity, `RecoveryHint`,
backtrace policy, exposure, projection, and ordered field schemas.

Catalog codes use lowercase dotted stable domain semantics, for example
`storage.commit_log.corrupt_record`. Never include a dynamic topic, group, path,
broker address, or other runtime value in a code. `CanonicalCondition` is
protocol-independent. Domain facades and private leaf errors cannot override
the catalog code, condition, severity, `RecoveryHint`, or projection metadata.

Context visibility is explicit: `Public` is safe for the public view,
`Diagnostic` is restricted to controlled diagnostics and operational telemetry,
and `SecretPresenceOnly` records only that secret-bearing data was present.
`PublicErrorView` contains only catalog-approved identity, fixed public message,
safe public context, and boundary-safe projection fields; Generic exposure
suppresses all dynamic public fields. `DiagnosticView` adds only declared,
bounded diagnostic values and value-free redaction markers. Neither view
exposes the source, caller location, or backtrace; typed causes are inspected
through `std::error::Error::source()`.

`RetryDecision` remains separate from canonical error identity. It uses
operation idempotency, operation stage, and remaining budget together with the
catalog's `RecoveryHint`; it never uses a response message or source string as
the decision.

The canonical core types are current imports from `rocketmq-error`. Other
target types become usable only in their owning migration, with inventory
evidence and focused tests; do not invent a compatibility alias or parallel
catalog in an unmigrated crate.

## Where changes belong

| Change type | Owner |
| --- | --- |
| Current leaf/source type and source preservation | Owning domain crate |
| Future canonical identity, catalog policy, redaction, or recovery contract | `rocketmq-error` |
| Remoting response code and safe remark conversion | Remoting boundary adapter |
| gRPC payload/status conversion | Proxy gRPC boundary adapter |
| Broker or NameServer external response conversion | Processor code using the remoting adapter |
| Dashboard or HTTP conversion | Dashboard boundary error wrapper |
| CLI display and exit behavior | CLI/tool boundary |
| Domain-local storage, auth, controller, or client source preservation | Owning crate, then canonical conversion during migration |

Do not create a second semantic catalog in a boundary crate. During migration,
keep an existing local mapper buildable while removing new display-text and
arbitrary-remark decisions. Preserve remoting numeric codes and headers,
gRPC/HTTP external contracts, wire semantics, and persisted layouts with
focused regression evidence when affected.

## Adding a new error

For a current API change:

1. Reuse the owning crate's existing typed shape where possible.
2. Add or reuse one canonical descriptor and associate every retained leaf.
3. Preserve the typed source and record deliberate redaction with only fields
   allowed by the descriptor schema.
4. Keep protocol conversion in the owning adapter, sourced from the
   descriptor's explicit projection; do not add another semantic mapping table.
5. Add focused tests for association, source preservation, redaction, and
   boundary output, then update the inventory evidence.

For a target migration, the owning change assigns the catalog's stable dotted
code, `CanonicalCondition`, fixed public message, severity, `RecoveryHint`,
projection metadata, context visibility, and source policy. It also migrates
both producers and consumers before removing its private bridge. These are
implementation requirements for that wave, not current API imports.

## Boundary and redaction rules

Boundary adapters project catalog metadata into local protocol primitives and
safe views. Preserve any still-local operation decision without parsing display
text or making an arbitrary remark authoritative.

Public output uses only `PublicErrorView`; controlled diagnostics may use
`DiagnosticView` with visibility checks and redaction. `SecretPresenceOnly`
never carries a secret value. Typed source chains may be retained for
diagnostics, but their rendered strings are not copied into stable fields.

## Dependency-driven wave rule

The work is organized by producer/consumer dependency, not by a fixed PR list.
A wave closes only after its producer and every known consumer migrate, focused
tests and compatibility regression evidence pass, and the private bridge for
that wave is removed. No bridge may become a V1/V2 API, deprecated shim,
compatibility feature, or long-lived dual path.

The dependency order is: baseline and freezes; primitive layers, views, and
catalog; domain leaf producers and facades; opaque canonical conversion;
boundary projections and client retry decisions; remaining application
consumers; bridge and local-table removal; then evidence and inventory update.
Wave 0B specifically revalidates every inventory row and every current consumer
before a row's ownership or status changes.

## Lightweight governance and validation

For implementation changes, run focused tests, the current error guard when
applicable, targeted `rg` scans, `git diff --check`, relative Markdown link
checks, and protocol/persistence regression checks when those contracts are
affected. Rust changes use the root repository profile: package-scoped
`cargo fmt -p <package> -- --check` and workspace Clippy with warnings denied.
Documentation-only changes do not require Cargo validation.

This guide intentionally does not require content fingerprints, file hashes,
complex AST gates, custom Clippy policy, or a heavy platform. The inventory's
pinned Git commit identifies its source snapshot; it is not a content
fingerprint requirement.

## Review checklist

- Does every retained typed leaf associate with the single canonical descriptor
  catalog without a parallel spec or policy table?
- Are Outcome/Decision/Rejection, ContractViolation, private leaf `Error`,
  domain facade, and opaque canonical `Error` kept distinct where migrated?
- Are public leaf errors, arbitrary remarks, and source stringification kept
  out of stable semantic contracts?
- Does the declarative catalog remain the sole owner of dotted code,
  `CanonicalCondition`, fixed public message, severity, `RecoveryHint`, and
  projection metadata?
- Do `PublicErrorView`, `DiagnosticView`, and context visibility prevent
  sensitive data from crossing the wrong boundary?
- Is `RetryDecision` based on idempotency, stage, and budget separately from
  error identity?
- Are remoting, gRPC, HTTP, wire, and persistence contracts backed by
  regression evidence when affected?
- Are producer and consumer migrations complete before removing the private
  bridge?
- Were focused tests, applicable guard, targeted scans, diff checks, and link
  checks run according to the change scope?

## Useful references

- [Error Architecture Redesign ADR](07-error-architecture-adr.md)
- [Error Architecture Inventory](07-error-inventory.md)
- [Error Architecture Runbooks](19-error-runbooks.md)
