# Error Hygiene Allowlist

## Purpose

RocketMQ Rust uses typed errors at crate, process, protocol, HTTP, gRPC, CLI, and storage boundaries. The error
architecture guard rejects early source stringification, public `anyhow::Result`, generic protocol responses, sensitive
debug output, and unregistered internal-error escapes. An allowlist is a temporary compatibility or framework-boundary
decision, not permission to introduce new untyped error handling.

Run the authoritative guard with:

```powershell
.\scripts\check-error-hygiene.ps1
```

or directly with:

```shell
python scripts/error_architecture_guard.py
```

A fully governed repository ends with `ERROR_ARCHITECTURE_GUARD_OK all`.

## Current Path Allowlist

The executable source of truth is the exact-path dictionaries in `scripts/error_architecture_guard.py`. Every entry has
an owner-specific reason; directory-wide additions are rejected unless the checked boundary itself is a deliberately
versioned protocol family.

| Category | Current approved boundary | Exit rule |
|---|---|---|
| `ANYHOW_RESULT_ALLOWLIST` | GPUI build script; standalone Tauri/Web process and persistence boundaries; Admin TUI process/runtime boundary | replace with the owning standalone project's typed process/persistence error when that project is migrated |
| `ANYHOW_RESULT_ALLOWLIST` | MCP R0 compatibility wrappers in `app.rs`, `transport/stdio.rs`, and `transport/streamable_http.rs` | new code uses `McpError`; remove deprecated `anyhow` wrappers only in the separately approved next-major window |
| `SOURCE_STRINGIFICATION_ALLOWLIST` | legacy Auth/config/storage, protocol-remark, OpenRaft trait, HA diagnostic, and external RocksDB/Store adapter boundaries | preserve a typed `#[source]` when the upstream contract permits it; otherwise retain the exact public compatibility reason |
| `INTERNAL_ERROR_ALLOWLIST` | audited internal invariants that cannot yet express a narrower stable kind | introduce/reuse `ErrorKind` and `ErrorSpec`, then delete the exact entry |
| processor response allowlists | Java-compatible remoting handlers whose wire response code is part of the public protocol | migrate through a typed handler mapping without changing the wire code |

The MCP compatibility paths now forward to these canonical typed APIs:

- `McpApp::bootstrap_typed` instead of `McpApp::bootstrap`;
- `init_tracing_typed` instead of `init_tracing`;
- `transport::stdio::serve_typed` instead of `transport::stdio::serve`;
- `transport::streamable_http::{serve_typed, build_router_typed}` instead of the `anyhow` wrappers.

The process binary and all new internal callers use the typed functions. The wrappers remain only to honor the R0 public
API snapshot and are deprecated with an explicit canonical replacement.

## Change policy

An allowlist change must include all of the following in one reviewable change:

1. exact repository-relative path and a boundary-specific reason;
2. evidence that the error kind, retry/severity/redaction metadata, wire/HTTP/CLI mapping, and source chain are preserved;
3. an owner and removal window, or an explicit long-term framework/protocol compatibility decision;
4. focused positive coverage plus a guard fixture proving a renamed, moved, or expanded escape still fails closed;
5. updates to this document when the governed category or canonical replacement changes.

Do not add an entry merely to make CI green. New library or public business APIs must return `RocketMQResult`, a
domain-specific result, or another typed error. Error text must not contain credentials, tokens, ACL/TLS material,
message bodies, or whole request/config objects.

## Review checklist

- [ ] The path is exact and already exists.
- [ ] A typed alternative was attempted and the remaining compatibility constraint is documented.
- [ ] Public/wire/storage behavior is unchanged or separately versioned.
- [ ] Source and sensitive-field handling are tested.
- [ ] The owner and exit window are explicit.
- [ ] `scripts/check-error-hygiene.ps1` and the focused package tests pass.
