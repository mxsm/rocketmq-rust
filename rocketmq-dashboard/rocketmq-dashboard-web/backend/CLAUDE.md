# RocketMQ Dashboard Web Backend - Claude Code Instructions

## Scope
These instructions apply to `rocketmq-dashboard/rocketmq-dashboard-web/backend/`.

## Project role
This directory is the standalone Rust 2024 + Axum HTTP API for RocketMQ Dashboard Web.

The backend should:
- Load Dashboard configuration.
- Expose `/api/*` REST endpoints.
- Use `rocketmq-dashboard-common` for reusable Dashboard and admin behavior where practical.
- Keep HTTP handlers thin and move orchestration into service modules.
- Avoid duplicating RocketMQ protocol logic already available from shared crates.

## Boundaries
- Do not modify GPUI or Tauri Dashboard projects unless explicitly requested.
- Do not add this backend to the root Cargo workspace without explicit approval.
- If a change requires `rocketmq-dashboard-common`, follow the root repository validation requirements too.

## Rust rules
- Use Rust 2024, Tokio, Axum, Serde, Tracing, `thiserror`/`anyhow`, and `tower-http`.
- New `.rs` files must include the RocketMQ Rust Apache 2.0 copyright header.
- Do not use `mod.rs`.
- Use snake_case module names.
- Separate DTOs from internal models.
- Avoid `async-trait` unless there is no reasonable alternative.

## Validation
Run from this directory after Rust changes:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

For compile and CI parity checks:

```bash
cargo build --all-targets --all-features
```

Documentation-only changes do not require Cargo validation unless they affect build instructions or generated Rust.

## Working principles
- Inspect before editing.
- Preserve unrelated uncommitted changes.
- Keep diffs small and scoped.
- Prefer existing local module patterns over new abstractions.
