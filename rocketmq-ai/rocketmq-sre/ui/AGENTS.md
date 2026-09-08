# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-sre/ui/`.

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

## Development validation

Reuse installed dependencies; run `npm ci` only when missing or when the lockfile changes.
Select checks for the actual change from this directory:

- `npm run check:api` for OpenAPI or API-client changes.
- `npm run lint` for changed frontend code.
- `npm run test -- --run` with the relevant existing test filter for behavior changes.
- `npm run build` for TypeScript, routes, shared UI, or build configuration changes.

The full list belongs to broad frontend integration/CI, not every small edit. Instruction-only changes
need only document checks. Preserve the Control Plane and sensitive-data boundaries above.
