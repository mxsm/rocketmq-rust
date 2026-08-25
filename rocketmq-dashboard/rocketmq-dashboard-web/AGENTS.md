# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-web/`.

## Project shape
- `backend/` is a standalone Rust 2024 Cargo project for the Axum HTTP API.
- `frontend/` is a standalone React + TypeScript + Vite project.
- This Web Dashboard is not part of the root Cargo workspace.
- Do not change `rocketmq-dashboard-gpui/` or `rocketmq-dashboard-tauri/` when working here unless the user explicitly asks.
- Prefer shared logic from `rocketmq-dashboard-common/` instead of duplicating admin or model code in the Web backend.

## GitHub Actions
- Web CI lives in `.github/workflows/dashboard-web-ci.yml`.
- Keep Web-only validation in the Web CI workflow.
- Do not add `rocketmq-dashboard-web/backend` to the root Cargo workspace without an explicit design decision.
