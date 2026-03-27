## Claude Code

This directory is the Tauri app root.

- Frontend code is validated here.
- Rust backend code is validated in `src-tauri/`.
- Use the more specific `src-tauri/CLAUDE.md` for Rust backend changes.

## Frontend validation

When changing frontend code, run from this directory:

```bash
npm ci
npm run build
```

## Frontend test policy

- Run only the affected frontend validation by default.
- Use broader validation only when shared frontend code or build configuration changes.
