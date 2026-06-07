# RocketMQ Dashboard Web Frontend - Claude Code Instructions

## Scope
These instructions apply to `rocketmq-dashboard/rocketmq-dashboard-web/frontend/`.

## Project role
This directory is the standalone React + TypeScript + Vite UI for RocketMQ Dashboard Web.

The frontend should:
- Use the unified API client in `src/api/`.
- Align behavior with the Java Dashboard.
- Use the redesigned Web Dashboard visual system rather than copying the old Java UI.
- Keep operational pages dense, readable, and suitable for RocketMQ administration.

## Boundaries
- Do not modify backend Rust code unless a UI change requires an API contract change.
- Do not modify GPUI or Tauri Dashboard projects unless explicitly requested.
- Do not commit generated build output, logs, or unrelated dependency changes.

## Frontend rules
- Use React, TypeScript, Vite, React Router, existing design tokens, and shared components.
- Use lucide icons for buttons when suitable.
- Tables should include loading, error, empty, search, pagination, and refresh behavior.
- Dangerous actions require confirmation.
- Detail views should use drawers or focused dialogs where practical.
- Select boxes, dialogs, drawers, buttons, and tables must be visually consistent in light and dark themes.
- Avoid visible internal implementation notes such as Java-to-Rust API parity hints.

## Java Dashboard parity
Use the local Java frontend as the behavior reference:

```text
D:\Github\Java\rocketmq-dashboard\frontend-new
```

Match the operational flow for Topic, Consumer, Producer, Broker/Cluster, Message, DLQMessage, MessageTrace, Proxy, OPS, and ACL where practical.

## Validation
Run from this directory after frontend changes:

```bash
npm ci
npm run build
```

For local iteration with dependencies already installed:

```bash
npm run build
```

After significant UI work, inspect the running app in the in-app browser when available.

Documentation-only changes do not require npm validation unless they affect build instructions.

## Working principles
- Inspect before editing.
- Preserve unrelated uncommitted changes.
- Keep diffs scoped.
- Prefer existing components and tokens over new styling systems.
