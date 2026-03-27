## Project Memory

This repository contains:
- one root Cargo workspace
- several standalone Rust projects

Root workspace validation does not cover standalone projects.

Keep validation scoped to:
- the modified project
- the modified area
- the smallest effective test scope

## Validation Priorities

- For Rust changes, formatting and clippy are mandatory for the affected Cargo project.
- Tests should be targeted by default.
- Broader validation is only needed when the change affects shared infrastructure, shared crates, build configuration, or multiple projects.

## Repository Layout

- The repository root is the main Cargo workspace.
- `rocketmq-example` is a standalone Cargo project.
- `rocketmq-dashboard/rocketmq-dashboard-gpui` is a standalone Cargo project.
- `rocketmq-dashboard/rocketmq-dashboard-tauri` is the Tauri app root, not a Cargo workspace root.
- The Rust backend for Tauri is in `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri`.

## Working Style

- Keep changes scoped to the task.
- Prefer minimal diffs.
- Add or update tests when behavior changes.
- Do not broaden validation unnecessarily.
