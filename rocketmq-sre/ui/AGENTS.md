# AGENTS.md

## Scope

This file applies to `rocketmq-sre/ui/`.

## Project role

- This is the standalone React, TypeScript, and Vite UI for RocketMQ-Rust AI SRE.
- It talks only to the versioned RocketMQ SRE Control Plane API.
- It must not call RocketMQ Dashboard mutation APIs or share Dashboard sessions.

## Working agreement

- Keep the interface dense, desktop-first, and operational.
- Read and diagnosis surfaces remain read-only. Phase 3 approval and execution
  controls must call only the versioned Control Plane API and must never expose
  shell, raw request, arbitrary patch, or target credential input.
- Keep API DTOs under `src/api/` and page orchestration under `src/pages/`.
- Never display credentials, tokens, message bodies, ACL/TLS material, or whole configurations.
- Do not commit `dist/`, logs, or local environment files.

## Validation

Run from this directory:

```bash
npm ci
npm run check:api
npm run lint
npm run test -- --run
npm run build
```
