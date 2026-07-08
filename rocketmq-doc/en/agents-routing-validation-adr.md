# ADR: AGENTS Routing Validation

## Status
Accepted

## Context
The RocketMQ Rust repository is broader than the root Cargo workspace. The root `Cargo.toml` owns the main workspace members, while several projects are intentionally standalone:

- `rocketmq-example/`
- `rocketmq-dashboard/rocketmq-dashboard-gpui/`
- `rocketmq-dashboard/rocketmq-dashboard-tauri/`
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/`
- `rocketmq-dashboard/rocketmq-dashboard-web/`
- `rocketmq-dashboard/rocketmq-dashboard-web/backend/`
- `rocketmq-dashboard/rocketmq-dashboard-web/frontend/`
- `rocketmq-website/`

Root workspace commands such as `cargo fmt --all` and `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` do not validate all standalone Rust, Node/Vite, or Docusaurus projects. The repository also has specialized quality gates for runtime ownership, typed error architecture, observability feature combinations, and RocksDB store behavior.

Without an explicit routing model, agents can incorrectly treat the root Cargo workspace as the whole repository or skip project-specific validation after touching shared crates.

## Decision
Use root `AGENTS.md` as the repository-level validation router.

The root file must:

- Identify the root workspace source of truth as the root `Cargo.toml`.
- Route standalone Cargo, Node/Vite, Web Dashboard, Tauri, and Docusaurus work to the nearest project `AGENTS.md`.
- List the local validation commands for root workspace changes and the routing rules for standalone projects.
- Define when to run specialized guards such as `scripts/runtime-audit.ps1`, `scripts/check-error-hygiene.ps1`, `scripts/error_architecture_guard.py`, observability feature checks, and RocksDB feature checks.
- Require `scripts/check-agents-routing.ps1` on Windows or `scripts/check-agents-routing.sh` on Unix when project boundaries, manifests, workflow path filters, or `AGENTS.md` files change.

Add `rocketmq-website/AGENTS.md` so the Docusaurus website has a local validation contract.

Add `scripts/check-agents-routing.ps1` and `scripts/check-agents-routing.sh` as lightweight drift checks. The scripts validate that:

- Root `AGENTS.md` mentions all required standalone routes.
- Every standalone Cargo project with its own `[workspace]` has a same-directory `AGENTS.md`.
- Every discovered `package.json` project has a same-directory `AGENTS.md`.
- Key workflow routes are represented in root `AGENTS.md`.
- The shared-code list and specialized guard commands remain discoverable.

## Alternatives
### Only Expand Root AGENTS.md
This is simpler, but it leaves no automated signal when a new standalone project, `package.json`, workflow path, or shared validation route is added.

### Rely Only on GitHub Actions
CI validates pull requests but does not help agents choose the right local command before finishing work. It also does not explain routing intent or standalone project boundaries.

### Duplicate Full Instructions Everywhere
Copying root validation details into every subproject increases drift risk. The chosen model keeps root routing centralized and leaves project-specific details in nearest `AGENTS.md` files.

## Consequences
Benefits:

- Agents can determine validation scope from path ownership instead of guessing.
- Standalone project coverage becomes visible and checkable.
- New project boundaries require an explicit AGENTS update.
- Runtime, typed error, observability, and RocksDB guardrails are discoverable from the root workflow.

Costs:

- `scripts/check-agents-routing.ps1` and `scripts/check-agents-routing.sh` must be updated together when intentional validation topology changes.
- The check is structural. It cannot prove every command is sufficient for every code change.
- CI remains the final cross-platform authority for Linux, macOS, Windows, and Node version behavior.

## Validation
Run from the repository root before PR submission or final handoff when changes touch `AGENTS.md`, nested `AGENTS.md` files, `Cargo.toml`, `package.json`, or `.github/workflows/**`:

```powershell
.\scripts\check-agents-routing.ps1
git diff --check
```

```bash
bash ./scripts/check-agents-routing.sh
git diff --check
```

Rust or Node validation is still required when the changed files affect Rust code, generated Rust, build configuration, examples, frontend behavior, website behavior, or documented commands that need verification.
