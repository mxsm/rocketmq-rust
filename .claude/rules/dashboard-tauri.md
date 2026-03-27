---
paths:
  - "rocketmq-dashboard/rocketmq-dashboard-tauri/**/*"
---

# Dashboard Tauri Rules

- `rocketmq-dashboard/rocketmq-dashboard-tauri` is the Tauri app root.
- Frontend validation happens in `rocketmq-dashboard/rocketmq-dashboard-tauri`.
- Rust backend validation happens in `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri`.
- Do not treat `rocketmq-dashboard/rocketmq-dashboard-tauri` itself as a Cargo workspace root.

## Frontend validation

When changing frontend code, run from `rocketmq-dashboard/rocketmq-dashboard-tauri/`:

```bash
npm ci
npm run build
```

## Frontend test policy

- Run only the affected frontend validation by default.
- Use broader frontend validation only when shared frontend code, routing, or build configuration changes.
