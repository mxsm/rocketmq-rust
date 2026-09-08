---
title: "Canonical Error Migration Report"
permalink: /docs/error-migration-report/
excerpt: "Completion record for the direct migration to the canonical RocketMQ Rust error architecture."
last_modified_at: 2026-09-08T00:00:00+08:00
toc: true
classes: wide
---

# Canonical Error Migration Report

## Status and scope

The E9 migration replaces the repository-wide legacy error tracks with the
canonical `rocketmq-error` model. This was a direct cutover: there is no V1/V2
feature switch, compatibility crate, deprecated type alias, or legacy adapter.
Rust callers that depended on the removed public types must migrate to the
canonical API in the same release.

The completed architecture has one opaque `rocketmq_error::Error` value,
`rocketmq_error::Result<T>`, `SharedError` for shared ownership, 136 declarative
descriptors, typed causal chains, schema-governed context, and separate public
and diagnostic views. Domain crates may retain narrow owner facades or private
leaf enums, but not an independent error identity, classification, redaction,
or boundary-mapping system.

This report records the migration outcome and its review evidence. It does not
replace the final workspace, standalone-project, or CI validation gates.

## Compatibility decision

### Direct Rust API cutover

- The former `RocketMQError`, `RocketMQResult`, `DomainError`,
  `BoundaryErrorView`, `LegacyAdapter`, and domain compatibility-kind surfaces
  were removed rather than deprecated in parallel.
- Call sites now construct canonical errors or owner facades and return the
  canonical result type directly. Classification uses descriptor metadata or a
  typed owner enum, never rendered strings.
- The deliberate source break prevents a long-lived dual track. It also means
  downstream Rust users must update imports, constructors, pattern matches, and
  result signatures when adopting this release.

### Protocol and process boundaries

| Boundary | Migration rule |
| --- | --- |
| RocketMQ remoting | Existing numeric response codes and response headers are frozen compatibility surfaces. The cutover may change internal construction, but must preserve the established number, correlation data, flags, body, and extension headers for the same protocol outcome. |
| gRPC | Payload codes and transport statuses come from the canonical descriptor. A mapping may be corrected when semantically wrong, with an explicit compatibility review and focused golden coverage. |
| HTTP | Status codes come from the canonical descriptor and may be corrected when the old projection was inconsistent. Public bodies use the safe view, not a source string. |
| CLI | Exit codes are explicit canonical projections and may be corrected to distinguish operation, usage, authentication, configuration, and internal failures. Terminal text remains a safe presentation. |

The ability to correct HTTP, gRPC, and CLI mappings is not permission to alter
RocketMQ remoting codes. Remoting compatibility remains independently frozen.

## Migration results by area

| Area | Result |
| --- | --- |
| `rocketmq-error` | Replaced the central public mega-enum and duplicate domain/boundary tracks with the opaque canonical error value. Added descriptor lookup, typed sources, `SharedError`, bounded context schemas, redaction, safe views, boundary projections, and the exact 136-entry catalog. |
| `rocketmq-model`, `rocketmq-protocol`, and `rocketmq-transport` | Removed legacy re-exports and signatures, converted model/codec and transport stages to canonical construction, and retained protocol-specific remoting response and header behavior at the wire boundary. Transport failures preserve typed causes and structured stage context. |
| `rocketmq-macros` | Updated generated request-header codec paths to the protocol crate's canonical error signatures while retaining the existing legacy-v1/v2 header encoding formats; those names describe wire formats, not error compatibility facades. |
| `rocketmq-store-api`, `rocketmq-store`, and `rocketmq-store-local` | Migrated public results and owner errors to the canonical model while retaining private typed leaves where they express store semantics. Large `Err` payloads were boxed at owner/result boundaries; store-local namespace-transition proof results retain narrowly scoped, reasoned `result_large_err` allowances where the typed payload is intentional. |
| `rocketmq-auth` and `rocketmq-security-api` | Replaced parallel public error kinds with closed owner outcomes backed by canonical errors. Provider and credential failures preserve typed sources; secret-bearing material is represented only by schema-approved or presence-only context. |
| `rocketmq-controller`, `rocketmq-namesrv`, and `rocketmq-broker` | Migrated startup, lifecycle, processing, persistence, deferred response, and control-plane paths to canonical errors or narrow facades. Boundary assembly preserves existing remoting numbers and headers and renders only safe public content. |
| `rocketmq-client` | Replaced legacy client result/error tracks with `ClientError` backed by `SharedError`. Retry, callback, broker-response, and transport flows classify by typed outcome or descriptor metadata and preserve downcastable sources. |
| `rocketmq-proxy-core`, `rocketmq-proxy-cluster`, `rocketmq-proxy-local`, and `rocketmq-proxy` | Removed `ProxyErrorKind` and parallel status policy. Local and cluster proxy paths use canonical descriptors for gRPC/HTTP/remoting projections, retain typed causes, and expose only safe views. |
| `rocketmq-filter` | Removed the legacy `Filter::compile`/adapter string path. Structured compilation returns typed `FilterCompileErrorKind` outcomes converted into the canonical error model. |
| `rocketmq-observability` | Removed duplicate error-kind classification. Metrics and tracing labels are derived from low-cardinality descriptor metadata; arbitrary source strings are excluded. |
| `rocketmq-dashboard-common`, `rocketmq-dashboard-gpui`, `rocketmq-dashboard-tauri`, and `rocketmq-dashboard-web` | Migrated dashboard error facades to canonical errors. HTTP and process boundaries use safe projections and retain typed causes internally. |
| `rocketmq-admin-core`, `rocketmq-admin-cli`, `rocketmq-admin-tui`, and `rocketmq-store-inspect` | Migrated operation results to closed owner outcomes backed by canonical errors. CLI exits are explicit, safe, and descriptor-aligned rather than inferred from display text. |
| `rocketmq-mcp`, `rocketmq-mcp-control`, `rocketmq-sre-probe`, and `rocketmq-example` | Updated standalone consumers, probes, and scenarios to the canonical public API and removed legacy facade dependencies. Diagnostic interfaces keep typed chains internally and sanitize externally visible output. |

## Typed causes and safe presentation

Canonical construction accepts typed causes through `Error::caused_by` and
preserves them through `std::error::Error::source()`. Shared asynchronous or
facade paths use `SharedError`; owner wrappers expose the canonical error as
their source. Error classification and recovery policy do not inspect
`Display`, `Debug`, or `to_string()` output.

Boundary presentation is intentionally separate:

- `PublicErrorView` contains the stable catalog code, fixed public message, and
  only schema fields marked `Public`. A descriptor with `Generic` exposure
  suppresses all dynamic fields.
- `DiagnosticView` may contain bounded diagnostic context and redaction
  markers, but never renders the typed source chain, backtrace, caller location,
  credential material, token values, or message bodies.
- `SecretPresenceOnly` fields record presence without preserving or displaying
  the secret value.

These rules keep operational diagnostics useful without turning a source chain
or free-form context into an external data channel.

## `result_large_err` resolution

The migration initially exposed large public `Result` error payloads in owner
enums and facades. The repaired shape boxes large canonical or transition
payloads at the owning boundary, removes crate-wide suppressions, and keeps
only narrow store-local allowances for namespace transition results whose typed
proof and disposition data are intentionally returned together. Each retained
allowance is local and reasoned; it is not a compatibility mechanism or a
blanket exemption for the migrated crates.

## Verification evidence

| Evidence | Contract covered |
| --- | --- |
| `rocketmq-error/tests/error_descriptor_catalog.rs` | Exact ordered snapshot of all 136 descriptors, including field schema and remoting/gRPC/HTTP/CLI projections. |
| `rocketmq-error/tests/error_catalog_primitives.rs` and `error_core.rs` | Descriptor lookup, identity, construction, ownership, and core typed-source behavior. |
| `rocketmq-error/tests/error_context_visibility.rs`, `error_context_redaction.rs`, and `error_safe_views.rs` | Public/diagnostic visibility, bounded values, secret-presence handling, and safe rendering. |
| `rocketmq-error/tests/boundary_mapping_golden.rs` and `proxy_broker_response_catalog.rs` | Canonical boundary projections and proxy/broker response selection. |
| `rocketmq-error/tests/shared_error_contract.rs` and `typed_error_public_api.rs` | Shared ownership, source traversal, and the intended public API. |
| Client typed-error, callback, broker-response, and retry tests | Client facade/source preservation and policy classification without string matching. |
| Protocol codec, transport typed-boundary, and broker wire tests | Remoting response-code/header preservation and transport source propagation. |
| Filter compile contract tests | Structured compiler outcomes and removal of the legacy compile adapter. |
| Admin CLI `operation_exit_codes` tests | Stable explicit process-exit projection. |
| `scripts/error_architecture_guard.py` / `scripts/check-error-hygiene.ps1` | Repository-wide architecture, forbidden-symbol, redaction, and boundary hygiene checks. |

A static Rust-source scan at report generation found no uses of the removed
`RocketMQError`, `RocketMQResult`, `DomainError`, `BoundaryErrorView`,
`LegacyAdapter`, `ProxyErrorKind`, `DashboardStorageErrorKind`, `McpErrorKind`,
or `NameServerRouteErrorKind` symbols.

## Remaining risks and final gates

- The change crosses the root workspace and several standalone Cargo projects;
  feature-only and path-dependent consumers remain the main compilation-risk
  surface until every routed profile completes on the final tree.
- Remoting compatibility has a higher bar than other projections. Final review
  must treat any changed numeric code, response header, body ownership, or
  correlation behavior as a release blocker even when a descriptor mapping
  looks semantically cleaner.
- The 136-entry catalog and its snapshot must move together. Any future catalog
  edit requires focused boundary review rather than a mechanical snapshot
  refresh.
- Public API/structural baselines, package format checks, workspace Clippy,
  focused crate tests, standalone-project profiles, the error hygiene guard,
  and applicable runtime audit remain required final evidence. This
  documentation-only E9-2 step intentionally performs static Markdown and
  catalog-consistency checks only.

## Maintenance references

- [Canonical Error Architecture ADR](07-error-architecture-adr.md)
- [Canonical Error Catalog](07-error-catalog.md)
- [Error Contribution Guide](19-error-contribution-guide.md)
