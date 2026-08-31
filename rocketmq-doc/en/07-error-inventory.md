---
title: "Error Architecture Inventory"
permalink: /docs/error-inventory/
excerpt: "Current RocketMQ Rust error ownership and migration inventory."
last_modified_at: 2026-08-31T00:00:00+08:00
toc: true
classes: wide
---

# Error Architecture Inventory

Snapshot date: 2026-08-31.

Inventory baseline commit: `e016a878703767924529911240808c649ed4a801`.

This is a pinned current-state inventory, not a completion claim. At this
baseline, `rocketmq-error` has a public mega `RocketMQError` enum together with
`ErrorKind`, `ErrorSpec`, and `ALL_ERROR_SPECS`. It also has a `DomainError`
override path, free-string `ErrorContext`, `BoundaryErrorView`, and shared
`Sensitive` support. Client callback errors are typed, and `StoreError` is an
opaque domain facade.

The baseline has no `RocketmqError` or `Legacy*` aliases, no public `anyhow`
`Result` alias or callers, no dynamic client callback error, and no
`StoreError::General`. Dashboard `AdminErrorCode::RocketMqError` is an
unrelated dashboard response-code name. The `rocketmq-error` crate still has
`anyhow`, configuration, and `serde_json` dependencies. Boundary remarks remain
arbitrary, and source `Display` output remains observable; neither is stable
semantic data.

The target vocabulary is the exact five-layer model in the [Error Architecture
Redesign ADR](07-error-architecture-adr.md). Itemized classification is pending
follow-up; the presence of a target schema or a migration wave does not assert
that any row is complete.

## Counting scope and methodology

The baseline counts below are lexical candidates from the implementation-plan
scan, not Rustdoc reachability, semantic ownership, or completion results. Run
the following read-only commands from the repository root in PowerShell. The
all-tree scope excludes `rocketmq-error`, `target`, and tests using the exact
globs shown. The explicit standalone scopes are `rocketmq-ai/rocketmq-mcp`,
`rocketmq-ai/rocketmq-sre`,
`rocketmq-dashboard/rocketmq-dashboard-gpui`,
`rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri`, and
`rocketmq-dashboard/rocketmq-dashboard-web/backend`. Root-workspace counts
mean Cargo metadata workspace members excluding `rocketmq-error`; equivalently,
at this baseline they are the all-tree counts minus those explicit standalone
scope counts.

```powershell
$errorPattern = '^\s*(?:pub(?:\([^)]*\))?\s+)?(?:enum|struct)\s+\w*Error\b'
$plainErrorPattern = '^\s*pub\s+(?:enum|struct)\s+\w*Error\b'
$errorKindPattern = '^\s*(?:pub(?:\([^)]*\))?\s+)?(?:enum|struct)\s+\w*ErrorKind\b'
$plainErrorKindPattern = '^\s*pub\s+(?:enum|struct)\s+\w*ErrorKind\b'
$standaloneScopes = @(
  'rocketmq-ai/rocketmq-mcp',
  'rocketmq-ai/rocketmq-sre',
  'rocketmq-dashboard/rocketmq-dashboard-gpui',
  'rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri',
  'rocketmq-dashboard/rocketmq-dashboard-web/backend'
)

$all = @(rg -n --glob '*.rs' --glob '!target/**' --glob '!**/tests/**' --glob '!rocketmq-error/**' $errorPattern .)
$plainAll = @(rg -n --glob '*.rs' --glob '!target/**' --glob '!**/tests/**' --glob '!rocketmq-error/**' $plainErrorPattern .)
$kindAll = @(rg -n --glob '*.rs' --glob '!target/**' --glob '!**/tests/**' --glob '!rocketmq-error/**' $errorKindPattern .)
$plainKindAll = @(rg -n --glob '*.rs' --glob '!target/**' --glob '!**/tests/**' --glob '!rocketmq-error/**' $plainErrorKindPattern .)
$standalone = @($standaloneScopes | ForEach-Object { rg -n --glob '*.rs' --glob '!target/**' --glob '!**/tests/**' $errorPattern $_ })
$plainStandalone = @($standaloneScopes | ForEach-Object { rg -n --glob '*.rs' --glob '!target/**' --glob '!**/tests/**' $plainErrorPattern $_ })

[pscustomobject]@{
  All = $all.Count
  RootWorkspace = $all.Count - $standalone.Count
  Standalone = $standalone.Count
  PlainPubAll = $plainAll.Count
  PlainPubRootWorkspace = $plainAll.Count - $plainStandalone.Count
  PlainPubStandalone = $plainStandalone.Count
  ErrorKind = $kindAll.Count
  PlainPubErrorKind = $plainKindAll.Count
}
```

This produces 301 enum/struct names ending in `Error`: 238 in the root
workspace (129 plain `pub`) and 63 in standalone projects (49 plain `pub`). It
also produces 47 `ErrorKind` declarations, of which 25 are plain `pub`. These
figures are repeatable lexical candidates for triage, not proof that a name is
reachable in Rustdoc or that a migration is complete. Wave 0B must revalidate
every inventory row and every current consumer before its status or ownership
is changed.

## Baseline signals

| Current signal | Baseline truth |
| --- | --- |
| Public central error | A large public `RocketMQError` enum is the central error value |
| Existing metadata | `ErrorKind`, `ErrorSpec`, and `ALL_ERROR_SPECS` exist, but declarative sole ownership is not yet established |
| Domain override/context | `DomainError` override and free-string `ErrorContext` exist |
| Boundary view | `BoundaryErrorView` exists; boundary remarks can still be arbitrary |
| Client callback | Callback error paths are typed rather than dynamic |
| Storage facade | `StoreError` is opaque; `StoreError::General` is absent |
| Redaction | Shared `Sensitive` support exists |
| Source presentation | Source `Display` output remains observable and is not a stable contract |
| `rocketmq-error` dependencies | `anyhow`, configuration, and `serde_json` dependencies remain |
| Legacy aliases | No `RocketmqError` or `Legacy*` aliases are present at the baseline |

## Exact target five-layer model

| Layer | Target responsibility | Current position |
| --- | --- | --- |
| Outcome/Decision/Rejection | Public control-flow outcomes; retry is not inferred from rendered text | Not yet a uniformly separated public model |
| ContractViolation | Caller, protocol, and invariant violations distinct from operational failures | Not yet uniformly separated |
| Private leaf `Error` | Domain implementation errors retain typed sources behind private representation | Current domain errors vary; public mega-enum remains central |
| Domain operational facade | Narrow domain facade hides private leaves while retaining source and policy information | Opaque `StoreError` exists; other domains vary |
| Opaque canonical `Error` with safe projections | One canonical value with private representation projected through `PublicErrorView` and `DiagnosticView` | Target migration from public mega-enum is pending |

`ErrorCatalog`, boundary adapters, and presentation/observability are supporting
mechanisms for these five layers, not extra layers. The declarative catalog is
the sole owner of stable dotted code, `CanonicalCondition`, fixed public
message, severity, `RecoveryHint`, and projection metadata.

## Catalog, context, and decision contract

Every catalog entry must declaratively own:

- one stable dotted code;
- one `CanonicalCondition`;
- one fixed public message;
- severity and `RecoveryHint`; and
- projection metadata for remoting, gRPC, HTTP, CLI, and observability.

Catalog codes use lowercase dotted stable domain semantics, for example
`storage.commit_log.corrupt_record`. A code must never contain a dynamic topic,
group, path, broker address, or other runtime value. `CanonicalCondition` is
protocol-independent. Domain facades and private leaf errors cannot override
the catalog code, condition, severity, `RecoveryHint`, or projection metadata.

Context visibility is explicit:

- `Public` may appear in `PublicErrorView` and external responses;
- `Diagnostic` is restricted to controlled diagnostics and operational
  telemetry; and
- `SecretPresenceOnly` records only the presence of secret-bearing data, never
  its value.

`PublicErrorView` contains catalog-approved identity, fixed public message,
safe public context, and boundary-safe projection fields. `DiagnosticView` may
add redacted diagnostic context and typed source information according to
visibility policy. Free-form remarks and source `Display` text are not fields
that define either view.

`RetryDecision` is separate from catalog identity and error rendering. It uses
operation idempotency, operation stage, and remaining budget together with the
catalog's `RecoveryHint`; no response message or source string decides retry by
itself.

## Per-type inventory schema

Itemized classification is a pending follow-up. When rows are added, the
inventory must use these exact fields (extra evidence columns are allowed):

| Type | Crate/Path | Visibility | Current Consumers | Category | Target Type | Catalog Code | Source Preserved | Owner | Wave | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| _pending follow-up_ | _pending follow-up_ | _pending follow-up_ | _pending follow-up_ | _one of Outcome/Contract/PrivateLeaf/Facade/Delete_ | _pending follow-up_ | _pending follow-up_ | _pending follow-up_ | _pending follow-up_ | _pending follow-up_ | _pending; no completion implied_ |

`Category` is deliberately limited to `Outcome`, `Contract`, `PrivateLeaf`,
`Facade`, or `Delete`. `Source Preserved` records the typed source-chain
decision and rationale; it must never be inferred from source stringification.
`Status` needs implementation and evidence before it can be changed from
pending.

## Compatibility constraints

The Rust API is allowed to break. There will be no V1/V2 public compatibility
API, deprecated compatibility shim, compatibility feature, or long-lived dual
API. A private migration bridge may exist only inside its dependency-driven
wave and must be removed in that wave after its producer and every consumer
have migrated.

The inventory preserves external contracts separately from Rust API shape:
remoting numeric response codes and headers, gRPC and HTTP external contracts,
wire semantics, and persisted record layouts require regression evidence when
affected. Preserving those contracts does not preserve an enum, arbitrary
remark, or source `Display` string.

## Dependency-driven waves

| Wave | Producer and consumer scope | Required evidence and bridge rule |
| --- | --- | --- |
| 0 | Pin baseline; freeze public leaves, arbitrary remarks, and source stringification; record wire/persistence invariants | Establish evidence; no bridge |
| 0B | Revalidate every inventory row and every current consumer against the pinned lexical baseline | Row-level and consumer-level evidence is required before changing ownership or status; no bridge |
| 1 | Define Outcome/Decision/Rejection, ContractViolation, context visibility, views, and catalog primitives | Primitive producer and all initial readers migrate before temporary representation is removed |
| 2 | Migrate domain leaf producers and operational facades | Remove each private facade bridge after its producer set and every domain consumer migrate |
| 3 | Wrap migrated leaves/facades in opaque canonical `Error` | Remove enum-to-canonical bridge after all affected producers and canonical consumers migrate |
| 4 | Migrate boundary adapters and safe projections for remoting, gRPC, HTTP, CLI, and observability | Remove each boundary bridge only after producer paths and all consumers have regression evidence |
| 5 | Migrate client retry decisions and remaining broker, namesrv, controller, auth, common, proxy, dashboard, tool, and standalone consumers | Remove each domain bridge after producer plus consumer coverage is complete |
| 6 | Remove local semantic tables, display-text mapping, and transitional internal adapters | Private bridge is removed in the same wave; nothing becomes a public/deprecated API |
| 7 | Run focused tests, applicable guards, targeted scans, link checks, and protocol/persistence regression evidence | No bridge remains; inventory status changes only with evidence |

## Target acceptance criteria

These are future acceptance criteria, not statements that the pinned baseline
passes:

- The five layers are separated, with `ErrorCatalog` as the sole owner of
  stable semantic metadata and adapters/presentation as supporting mechanisms.
- Public boundaries use opaque canonical `Error` and safe projections rather
  than exhaustive mega-enum matching.
- `PublicErrorView` and `DiagnosticView` enforce context visibility and default
  redaction; source chains remain typed where useful.
- `RetryDecision` uses idempotency, stage, and budget separately from error
  identity and rendered text.
- Remoting numeric codes/headers, gRPC/HTTP contracts, wire semantics, and
  persisted layouts retain regression evidence.
- Legacy aliases, public anyhow contracts, and private bridges are absent when
  their owning waves complete; no compatibility shim survives as public API.

No itemized row should be marked complete solely because this target design is
documented. The pinned commit and follow-up evidence are the audit trail.
