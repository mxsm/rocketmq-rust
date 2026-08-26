# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-web/frontend/`.

## Project role
- This directory is the standalone React + TypeScript + Vite frontend for RocketMQ Dashboard Web.
- It talks to the Rust backend through the unified API client under `src/api/`.
- Do not modify backend Rust files unless the requested UI work requires an API contract change.

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
- When matching behavior, inspect the upstream [RocketMQ Dashboard frontend](https://github.com/apache/rocketmq-dashboard/tree/master/frontend-new).

- Preserve Java Dashboard operational flows where practical: Topic, Consumer, Producer, Broker/Cluster, Message, DLQMessage, MessageTrace, Proxy, OPS, and ACL.
- Do not copy the old Java visual style directly.

## Validation
Run from this directory before PR submission or final handoff for frontend changes:

```bash
npm ci
npm run build
```

For iteration when dependencies are already installed:

```bash
npm run build
```

After significant UI changes, inspect the local app in the in-app browser when a dev server is available.
