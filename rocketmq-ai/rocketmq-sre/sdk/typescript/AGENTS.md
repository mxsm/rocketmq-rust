# RocketMQ AI SRE TypeScript SDK working agreement

## Scope

This file applies to the standalone package in this directory. The repository
root and `rocketmq-ai/rocketmq-sre/AGENTS.md` instructions also apply unless this file is
more specific.

## Boundary

- Keep the exported network surface read-only and fixed to status, cluster,
  incident, inspection, plan, and OpenAPI queries.
- Do not export a generic request method, raw Admin or shell escape hatch,
  approval, execution, or target mutation API.
- Plan drafts are local-only typed values. Creating a draft must not perform
  network I/O or imply approval or execution authority.
- Bearer tokens must come from a string or provider callback, must not appear
  in errors, and must never be redirected to another origin.
- Keep strict TypeScript enabled and bound every response before JSON decode.

## Validation

From this directory, run `npm test` for SDK behavior/type changes. Reuse installed dependencies;
run `npm ci` only when dependencies are missing or the lockfile changes. Instruction-only edits
do not need a Node build. This package does not inherit the parent SRE Cargo validation profile.

Build output under `dist/` and dependencies under `node_modules/` are local
artifacts and must not be committed.
