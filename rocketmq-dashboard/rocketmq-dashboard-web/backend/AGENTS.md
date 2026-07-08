# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-web/backend/`.

It overrides the Web Dashboard root instructions for backend files.

## Project role
- This directory is the standalone Rust backend for RocketMQ Dashboard Web.
- It uses Rust 2024, Tokio, Axum, Serde, Tracing, `thiserror`/`anyhow`, and `tower-http`.
- It is not part of the root Cargo workspace.
- It should expose HTTP APIs and keep RocketMQ admin/business reuse in `rocketmq-dashboard-common` when practical.

## Repository boundaries
- Do not modify `rocketmq-dashboard-gpui/` or `rocketmq-dashboard-tauri/` while working here unless the user explicitly asks.
- Do not add this backend to the root Cargo workspace without an explicit architecture decision.
- If shared functionality is needed, prefer a small change in `rocketmq-dashboard-common/` over duplicating protocol/admin logic here.
- If `rocketmq-dashboard-common/` is modified, follow the root repository validation rules as well.

## Codex workflow
- Check `git status --short` before editing.
- Treat existing uncommitted changes as user work.
- Keep edits scoped to the backend unless a shared change is required.
- Use `rg` or `rg --files` for searching.
- Use patch-style edits for manual changes.
- Do not create commits, branches, or pull requests unless the user asks.

## Rust style
- New Rust source files must include the RocketMQ Rust Apache 2.0 copyright header.
- Do not use `mod.rs`.
- Use snake_case module names.
- Keep API DTOs separate from internal models.
- Keep Axum handlers thin; put orchestration in services and reusable logic in common.
- Avoid `async-trait` unless there is no reasonable alternative.
- Prefer explicit error mapping through the local dashboard error and API response model.

## Validation
Run from this directory before PR submission or final handoff for Rust code changes:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

For compile-scope backend changes, also run:

```bash
cargo build --all-targets --all-features
```

Documentation-only changes do not require Cargo validation unless they change build instructions or generated Rust.

## Final response expectations
- Summarize backend files changed and why.
- List validation commands and results.
- If validation was skipped because the change is documentation-only, say so explicitly.
