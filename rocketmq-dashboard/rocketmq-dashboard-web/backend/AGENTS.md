# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-web/backend/`.

## Project role
- This directory is the standalone Rust backend for RocketMQ Dashboard Web.
- It uses Rust 2024, Tokio, Axum, Serde, Tracing, `thiserror`/`anyhow`, and `tower-http`.

## Rust style
- Do not use `mod.rs`.
- Use snake_case module names.
- Keep API DTOs separate from internal models.
- Keep Axum handlers thin; put orchestration in services and reusable logic in common.
- Prefer explicit error mapping through the local dashboard error and API response model.

## Validation
Run from this directory before PR submission or final handoff for Rust code changes:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

For compile-scope backend changes, also run:

```bash
cargo build --all-targets --all-features
```
