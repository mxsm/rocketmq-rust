# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-web/frontend/`.

It overrides the Web Dashboard root instructions for frontend files.

## Project role
- This directory is the standalone React + TypeScript + Vite frontend for RocketMQ Dashboard Web.
- It talks to the Rust backend through the unified API client under `src/api/`.
- UI behavior should align with the Java Dashboard, while visual design should use the new Web Dashboard design system.

## Repository boundaries
- Do not modify backend Rust files unless the requested UI work requires an API contract change.
- Do not modify `rocketmq-dashboard-gpui/` or `rocketmq-dashboard-tauri/` unless explicitly requested.
- Do not commit generated build output or logs.
- Do not introduce unrelated dependency updates.

## Codex workflow
- Check `git status --short` before editing.
- Treat existing uncommitted changes as user work.
- Keep edits scoped to frontend files unless an API contract change is required.
- Use `rg` or `rg --files` for searching.
- Use patch-style edits for manual changes.
- Do not create commits, branches, or pull requests unless the user asks.

## Frontend style
- Use React, TypeScript, Vite, React Router, existing design tokens, and shared components.
- Prefer existing app components before adding new primitives.
- Keep layouts dense and operational; avoid marketing-style sections.
- Use lucide icons for icon buttons when an appropriate icon exists.
- Tables should support loading, error, empty, search, pagination, refresh, and clear operation states.
- Dangerous operations must use confirmation dialogs.
- Message and broker details should use drawers or focused dialogs where practical.
- Keep select, dialog, drawer, and table styling consistent in light and dark themes.
- Do not show internal migration/API parity hints in the product UI.

## Java Dashboard parity
- When matching behavior, inspect the local Java frontend at:

```text
D:\Github\Java\rocketmq-dashboard\frontend-new
```

- Preserve Java Dashboard operational flows where practical: Topic, Consumer, Producer, Broker/Cluster, Message, DLQMessage, MessageTrace, Proxy, OPS, and ACL.
- Do not copy the old Java visual style directly.

## Validation
Run from this directory after frontend changes:

```bash
npm ci
npm run build
```

For iteration when dependencies are already installed:

```bash
npm run build
```

After significant UI changes, inspect the local app in the in-app browser when a dev server is available.

Documentation-only changes do not require npm validation unless they change build instructions.

## Final response expectations
- Summarize frontend files changed and why.
- List validation commands and results.
- If validation was skipped because the change is documentation-only, say so explicitly.
