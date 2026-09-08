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

## Development validation

From this directory, use `cargo fmt --all -- --check` and `cargo check` for the affected code;
run a focused `cargo test <test_name>` for behavior changes. Tests that compile the changed target
can replace the separate check.

Use `cargo clippy --no-deps -- -D warnings` and additional features/targets when relevant.
Full-target/all-feature builds belong to backend integration or CI, not every local handoff.
