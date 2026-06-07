# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-web/`.

## Project shape
- `backend/` is a standalone Rust 2024 Cargo project for the Axum HTTP API.
- `frontend/` is a standalone React + TypeScript + Vite project.
- This Web Dashboard is not part of the root Cargo workspace.
- Do not change `rocketmq-dashboard-gpui/` or `rocketmq-dashboard-tauri/` when working here unless the user explicitly asks.
- Prefer shared logic from `rocketmq-dashboard-common/` instead of duplicating admin or model code in the Web backend.

## Codex workflow
- Check `git status --short` before editing.
- Treat existing uncommitted changes as user work.
- Keep changes scoped to Web Dashboard files unless shared code is required.
- Use patch-style edits for manual file changes.
- Do not create commits, branches, or pull requests unless the user asks.

## Backend rules
- Use Rust 2024, Tokio, Axum, Serde, Tracing, `thiserror`/`anyhow`, and `tower-http`.
- Do not use `mod.rs`.
- Keep module names in snake_case.
- Keep DTOs and internal models separated.
- Keep the HTTP backend thin; put reusable behavior in `rocketmq-dashboard-common` when it is useful for GPUI/Tauri/Web.
- Avoid `async-trait` unless there is no reasonable alternative.

## Backend validation
Run from `rocketmq-dashboard/rocketmq-dashboard-web/backend/` after every Rust code change:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

For compile-scope backend changes, also run:

```bash
cargo build --all-targets --all-features
```

## Frontend rules
- Use React, TypeScript, and Vite.
- Keep UI aligned with Java Dashboard behavior, but do not copy the old Java UI styling.
- Use shared app components, design tokens, and existing page patterns before adding new UI primitives.
- All table-heavy pages should support loading, error, empty, search, pagination, refresh, and clear operation states.
- Dangerous operations must use confirmation dialogs.
- Message and detail inspection should use drawers or focused dialogs, not full page rewrites unless routing requires it.

## Frontend validation
Run from `rocketmq-dashboard/rocketmq-dashboard-web/frontend/` after frontend changes:

```bash
npm ci
npm run build
```

When dependencies are already installed locally, `npm run build` is enough for iteration, but CI uses `npm ci`.

## Cross-project validation
- If `rocketmq-dashboard-common/` is modified, also follow the root `AGENTS.md` validation rules for the root workspace.
- If common changes can affect GPUI or Tauri, validate the affected standalone project as well.

## GitHub Actions
- Web CI lives in `.github/workflows/dashboard-web-ci.yml`.
- Keep Web-only validation in the Web CI workflow.
- Do not add `rocketmq-dashboard-web/backend` to the root Cargo workspace without an explicit design decision.

## Final response expectations
- Summarize changed Web files and intent.
- List validation commands that were run and whether they passed.
- If validation is skipped for documentation-only changes, say so explicitly.
