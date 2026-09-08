# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-tauri/`.

## Structure
- This directory is the Tauri app root.
- Frontend code is validated here.
- Rust backend code is validated in `src-tauri/`.

## Frontend validation

From this directory, run `npm run build` for frontend code or build changes.
Reuse installed dependencies; run `npm ci` only when dependencies are missing or the lockfile changes.
Instruction-only edits do not require frontend or Rust builds.

## Frontend test policy
- Run only the affected frontend validation by default.
- Use broader validation only when shared frontend code or build configuration changes.

## Rust backend
- Do not run Rust validation from this directory.
- Use `src-tauri/AGENTS.md` for Rust backend changes.
