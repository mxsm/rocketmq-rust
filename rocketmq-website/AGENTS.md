# AGENTS.md

## Scope
This file applies to `rocketmq-website/`.

## Project role
- This directory is the standalone Docusaurus website for RocketMQ Rust.
- The website is a Node project outside the root Rust workspace.
- `examples/first-message/` is a standalone Cargo tutorial; follow its
  [local guide](examples/first-message/AGENTS.md) for Rust validation.
- Root Cargo validation does not validate this website.

## Repository boundaries
- Keep website changes scoped to `rocketmq-website/` unless the user explicitly asks to update source docs, workflows, or shared repository instructions.
- Do not commit generated Docusaurus build output.

## Development validation

Reuse installed dependencies. Run `npm ci` only when dependencies are missing or the lockfile changes.
For changes to rendered content, TypeScript, routes, or build configuration, run from this directory:

```bash
npm run build
```

Select existing focused tests for behavior changes. Instruction-only edits do not require a Node build.
Documentation-only changes outside this directory do not require website validation. Website content uses
Docusaurus routing and MDX, so validate relevant rendered pages and build output when they change.
