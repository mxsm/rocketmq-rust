# Error Hygiene Allowlist

This document explains the allowlist classes enforced by `scripts/error_architecture_guard.py`.
The Python guard is the source of truth and is wired into `.github/workflows/rocketmq-rust-ci.yaml`.
The PowerShell entry point `scripts/check-error-hygiene.ps1` runs the same guard locally on Windows.

## Review Rules

- Each new allowlist entry must name a repository-relative path or path prefix.
- Each new entry must explain why the exception is safe, temporary, or externally required.
- Boundary paths must prefer `BoundaryErrorView`, `ErrorSpec`, and redacted context over raw `Display`.
- Library code must use `RocketMQResult` or a domain result unless the path is a process, CLI, build, example, or compatibility boundary.
- `RocketMQError::Internal` is allowed only for audited invariants, compatibility surfaces, or presentation-layer diagnostics.

## Guard Categories

| Category | Enforced rule | Cleanup policy |
| --- | --- | --- |
| Core public surface | `rocketmq-error` and `rocketmq-common` must not expose legacy aliases or public `anyhow` contracts. | No allowlist. New hits are regressions. |
| Processor boundary mappings | Unsupported request-code paths must use central remoting helpers. | No allowlist for new unsupported-code responses. |
| Generic processor response codes | Java-compatible protocol branches may keep fixed `ResponseCode` values. | New business failures should return typed errors and convert at the boundary. |
| Required mapping adapters | Remoting, gRPC, HTTP, CLI, and admin views must keep tokens that prove `BoundaryErrorView` or `ErrorSpec` usage. | Missing tokens are regressions. |
| Source stringification | Domain layers must not stringify source errors unless the file has an audited compatibility reason. | Prefer typed wrappers with `#[source]`; shrink entries as domain errors mature. |
| Internal errors | `RocketMQError::Internal` must be in an audited path prefix or test context. | Replace common business failures with typed variants. |
| `anyhow` results | `anyhow::Result` and `anyhow::Error` are limited to process, CLI, build, dashboard, example, and explicit app boundaries. | Library/public APIs should use typed results. |
| Redaction guards | Sensitive fields must use `Sensitive<T>`, `REDACTED`, or a manual redacted `Debug` implementation. | New sensitive fields need tests. |

## Current Path Allowlist

The guard keeps path-level entries inline so that CI can fail atomically when a path is removed or renamed.
The current audited classes are:

| Path or prefix | Reason |
| --- | --- |
| `rocketmq-broker/src/processor/admin_broker_processor/` | Admin remoting APIs retain Java-compatible local response codes pending typed handler migration. |
| `rocketmq-broker/src/processor/*` protocol handlers | Pop, pull, send, query, reply, recall, transaction, notification, and lite-management protocols keep Java-compatible broker response codes where the branch is protocol-defined. |
| `rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands/` | CLI presentation commands may format local diagnostics for terminal output while admin core keeps typed views. |
| `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/` | Admin core still has audited compatibility adapters; new business failures should use domain errors. |
| `rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/` | TUI process and terminal runtime boundaries may use `anyhow` or local display diagnostics. |
| `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/lib.rs` and `main.rs` | Web backend process boundary. |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/**` dashboard service files | Standalone Tauri boundary pending dashboard alignment. |
| `rocketmq-dashboard/rocketmq-dashboard-gpui/build.rs` and other `build.rs` files | Build script boundary. |
| `rocketmq-auth/src/**` selected loader, provider, strategy, migration, and runtime bridge files | Auth compatibility paths convert provider or legacy ACL details into stable public auth errors while provider internals are still being split. |
| `rocketmq-store/src/ha/**` selected service files | HA service traits and async runtime joins still expose service diagnostics at compatibility boundaries. |
| `rocketmq-store/src/message_store/local_file_message_store.rs` | Recovery progress and HA/storage diagnostics are preserved as display text until all local recovery errors have typed wrappers. |
| `rocketmq-store/src/rocksdb/**` selected files | RocksDB integration keeps task failure text or wraps external errors through storage variants pending richer external source slots. |
| `rocketmq-store/src/utils/ffi.rs` | FFI helpers expose OS error reasons across a C-compatible boundary. |
| `rocketmq-controller/src/openraft/**` selected files | OpenRaft traits require `std::io::Error` or network error values at the storage/network boundary. |
| `rocketmq-controller/src/processor/controller_request_processor.rs` | Controller config remoting endpoint maps UTF-8 parser details into request validation text. |

## Local Verification

```powershell
.\scripts\check-error-hygiene.ps1
```

The command should end with `ERROR_ARCHITECTURE_GUARD_OK all`. Any `ERROR_ARCHITECTURE_GUARD_FAIL` output must be fixed or explicitly justified with a new allowlist entry and tests.
