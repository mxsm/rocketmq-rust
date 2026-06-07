# RocketMQ Dashboard Web - Claude Code Instructions

This directory contains the browser-based RocketMQ Dashboard implementation.

## Scope
These instructions apply to `rocketmq-dashboard/rocketmq-dashboard-web/`.

## Project layout
- `backend/` - Rust 2024 + Axum HTTP API.
- `frontend/` - React + TypeScript + Vite UI.
- `README.md` - Web Dashboard usage and migration notes.

The backend is a standalone Cargo project. The frontend is a standalone Node/Vite project.

## Relationship to other Dashboard modules
- `../rocketmq-dashboard-common/` contains shared Dashboard models, configuration helpers, and reusable admin contracts.
- `../rocketmq-dashboard-gpui/` is the GPUI desktop Dashboard.
- `../rocketmq-dashboard-tauri/` is the Tauri desktop Dashboard.
- Do not modify GPUI or Tauri while working on Web Dashboard unless explicitly requested.

## Backend development rules
- Use Rust 2024, Tokio, Axum, Serde, Tracing, `thiserror`/`anyhow`, and `tower-http`.
- Keep HTTP handlers thin and move reusable behavior into `rocketmq-dashboard-common` when appropriate.
- Avoid `mod.rs`; use explicit module files.
- Use snake_case module names.
- Keep DTOs separate from internal models.

## Backend validation
Run from `backend/` after Rust changes:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

For compile and CI parity checks:

```bash
cargo build --all-targets --all-features
```

## Frontend development rules
- Use React, TypeScript, Vite, the existing design tokens, and shared components.
- Align behavior with the Java Dashboard feature flow.
- Do not copy the old Java Dashboard visual style directly.
- Tables should include loading, error, empty, search, pagination, and refresh behavior.
- Dangerous actions require confirmation.
- Detail views should use drawers or focused dialogs where practical.

## Frontend validation
Run from `frontend/` after frontend changes:

```bash
npm ci
npm run build
```

For local iteration with dependencies already installed:

```bash
npm run build
```

## CI
Web Dashboard CI is defined in:

```text
.github/workflows/dashboard-web-ci.yml
```

It validates the frontend build and backend Rust build independently from the root workspace CI.

## Working principles
- Inspect before editing.
- Keep diffs scoped.
- Preserve existing uncommitted user changes.
- Prefer existing local patterns over new abstractions.
- Do not add the Web backend to the root workspace unless the user explicitly approves that architecture change.
