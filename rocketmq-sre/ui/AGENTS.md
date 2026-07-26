# AGENTS.md

## Scope

This file applies to `rocketmq-sre/ui/`.

## Project role

- This is the standalone React, TypeScript, and Vite UI for RocketMQ-Rust AI SRE.
- It talks only to the versioned RocketMQ SRE Control Plane API.
- It must not call RocketMQ Dashboard mutation APIs or share Dashboard sessions.

## Working agreement

- Keep the interface dense, operational, and read-only during Phase 00.
- Do not add approval, execution, mutation, or autonomy controls.
- Keep API DTOs under `src/api/` and page orchestration under `src/pages/`.
- Never display credentials, tokens, message bodies, ACL/TLS material, or whole configurations.
- Do not commit `dist/`, logs, or local environment files.

## Validation

Run from this directory:

```bash
npm ci
npm run test -- --run
npm run build
```
