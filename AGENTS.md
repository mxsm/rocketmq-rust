# AGENTS.md

## Scope and precedence

- This file applies to the whole repository unless a deeper `AGENTS.md` overrides it.
- Follow the nearest `AGENTS.md` for files under standalone projects and dashboard subdirectories.
- Direct user instructions in the current conversation take precedence over this file.
- If repository instructions conflict with each other, choose the more specific instruction and mention the conflict in the final response.

## Repository routing model

- The repository root is the main Cargo workspace. Its source of truth is the root `Cargo.toml` `[workspace].members` list.
- `rocketmq-dashboard/rocketmq-dashboard-common/` is a root workspace member and shared dashboard crate.
- `rocketmq-example/` is a standalone Cargo project.
- `rocketmq-dashboard/rocketmq-dashboard-gpui/` is a standalone Cargo project.
- `rocketmq-dashboard/rocketmq-dashboard-tauri/` is the Tauri app root and a standalone Node/Vite frontend project.
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/` is the standalone Rust backend for the Tauri app.
- `rocketmq-dashboard/rocketmq-dashboard-web/` is the Web Dashboard root, not a Cargo workspace root.
- `rocketmq-dashboard/rocketmq-dashboard-web/backend/` is the standalone Rust backend for the Web Dashboard.
- `rocketmq-dashboard/rocketmq-dashboard-web/frontend/` is the standalone React + TypeScript + Vite frontend for the Web Dashboard.
- `rocketmq-website/` is the standalone Docusaurus website project.
- Root workspace validation does not validate standalone Cargo projects or Node/Vite/Docusaurus projects.

## Workflow

- Before editing, inspect the relevant files and check the worktree state with `git status --short`.
- Treat existing uncommitted changes as user work. Do not overwrite, revert, or reformat unrelated changes.
- Keep changes tightly scoped to the request. Avoid unrelated refactors, dependency updates, and configuration churn.
- Prefer `rg` or `rg --files` for search. If unavailable, use the fastest local equivalent.
- If a required local validation command is missing, use the repository's documented installation or CI-equivalent path when one exists; do not change project
  dependencies or configuration just to work around local tool availability.
- Follow existing module structure, naming, error handling, and test style before introducing new patterns.
- Use patch-style edits for manual changes when tool support is available. Generated files and formatter output may be produced by their normal tools.
- For behavior changes, add or update focused tests that would fail without the change.
- Do not create commits, branches, or pull requests unless the user asks.

## Rust toolchain and style

- Use the repository Rust toolchain. Do not change `rust-toolchain.toml` unless explicitly required.
- Respect `rustfmt.toml` and `.clippy.toml`.
- Do not relax lint, formatting, feature, or CI configuration to make a change pass unless the task specifically requires it.
- Prefer clear, idiomatic Rust over clever abstractions.
- Be mindful of allocations, blocking calls, and lock scope in hot paths and async code.
- New Rust source files should keep the RocketMQ Rust Apache 2.0 copyright header style already used in the repository.
- Do not normalize editions across projects. The root workspace is mostly Rust 2021, while several tools and standalone dashboard projects are Rust 2024.

## Rust API and implementation guardrails

- When touching Rust code, prefer Clippy-friendly forms such as collapsed `if`/`if let`, method references over redundant closures, and inline named variables
  in `format!`, logging, and assertion macros when it reads naturally.
- When code is complex, add concise comments or examples that explain invariants, ordering, error handling, or concurrency assumptions. Avoid comments that only
  restate what the code already says.
- Avoid introducing public APIs with positional `bool`, ambiguous `Option`, or numeric mode arguments that make callsites hard to read. Prefer enums, named
  methods, builders, or small newtypes when they keep the callsite self-documenting.
- When an existing positional API must be called with an opaque `None`, boolean, or numeric literal, consider an exact `/*param_name*/` comment before the
  literal. Do not add these comments for string or char literals unless they add real clarity.
- Prefer exhaustive `match` arms for project-owned enums and public behavior. Use wildcard arms only when grouping future or irrelevant cases is intentional and
  clear from context.
- Keep modules private by default and expose intentional public API through narrow visibility or explicit re-exports.
- Newly added public traits should include doc comments that explain the trait role and what implementations are expected to provide.
- Avoid adding small helper methods that are referenced only once unless they name a non-trivial invariant, isolate unsafe or locking behavior, or match an
  established local pattern.
- For tracing async work, prefer instrumenting the function or method definition with `#[tracing::instrument(...)]` after checking whether the callee or
  immediate delegate is already instrumented. Use call-site `.instrument(...)` when intentionally binding an explicit observation/request span to a future.
- Avoid growing large, high-touch modules. Prefer adding focused modules for new functionality; target Rust modules under roughly 500 lines excluding tests, and
  avoid extending files above roughly 800 lines unless there is a strong local reason.
- When extracting code from a large module, move the related tests and module/type docs toward the new implementation so invariants stay close to the code that
  owns them.

## Validation router

| Area changed                                                                      | Work directory                                           | Required final / pre-PR validation                                                                                      | Escalate when                                                                                                                           |
|-----------------------------------------------------------------------------------|----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| Root workspace Rust crates and `rocketmq-dashboard/rocketmq-dashboard-common/`    | repository root                                          | `cargo fmt --all`; `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`                     | Cross-crate API, feature flags, build config, runtime ownership, typed errors, observability, RocksDB, or shared dashboard code changes |
| `rocketmq-example/`                                                               | `rocketmq-example/`                                      | Follow `rocketmq-example/AGENTS.md`                                                                                     | Shared client/common/remoting/error/observability/admin dependencies changed                                                            |
| `rocketmq-dashboard/rocketmq-dashboard-gpui/`                                     | `rocketmq-dashboard/rocketmq-dashboard-gpui/`            | Follow its `AGENTS.md`                                                                                                  | `rocketmq-dashboard-common/` or dashboard shared behavior changed                                                                       |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/` frontend                           | `rocketmq-dashboard/rocketmq-dashboard-tauri/`           | Follow its `AGENTS.md`; CI uses `npm ci` and `npm run build`                                                            | Shared frontend config, Tauri shell behavior, or package metadata changed                                                               |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/`                          | `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/` | Follow its `AGENTS.md`                                                                                                  | Shared root crates used by the Tauri backend changed                                                                                    |
| `rocketmq-dashboard/rocketmq-dashboard-web/backend/`                              | `rocketmq-dashboard/rocketmq-dashboard-web/backend/`     | Follow its `AGENTS.md`                                                                                                  | `rocketmq-dashboard-common/`, admin core, client/common/remoting/error, or API contracts changed                                        |
| `rocketmq-dashboard/rocketmq-dashboard-web/frontend/`                             | `rocketmq-dashboard/rocketmq-dashboard-web/frontend/`    | Follow its `AGENTS.md`; CI uses `npm ci` and `npm run build`                                                            | API contract, routing, shared UI patterns, or package metadata changed                                                                  |
| `rocketmq-website/`                                                               | `rocketmq-website/`                                      | Follow `rocketmq-website/AGENTS.md`; CI uses `npm ci` and `npm run build`                                               | Docusaurus config, package metadata, generated docs commands, or navigation changed                                                     |
| `AGENTS.md`, `**/AGENTS.md`, `Cargo.toml`, `package.json`, `.github/workflows/**` | repository root                                          | `.\scripts\check-agents-routing.ps1` on Windows or `bash ./scripts/check-agents-routing.sh` on Unix; `git diff --check` | Any project boundary, validation command, workflow path filter, or standalone project changes                                           |

## Mandatory Rust validation

Before submitting a PR or handing off completed Rust code work, finish with a successful format pass and clippy pass for each affected Cargo project. These full
validation commands are not required after every small edit while iterating.

Do not finish with unformatted Rust code.
Do not finish PR-ready or final Rust code work without a relevant clippy pass.
Do not claim validation passed unless the command completed successfully.
Rust validation can be slow because of workspace locks and native dependencies; let relevant commands complete unless they clearly fail or exceed the chosen
timeout, and do not kill unrelated Cargo or rustc processes.

Documentation-only changes do not require Cargo validation unless they affect generated Rust, build configuration, examples, documented commands that need
verification, or routing/validation instructions.

## Root workspace validation

For Rust changes inside the root workspace, run from the repository root before PR submission or final handoff:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Package-level clippy may be useful while iterating, but the final required pass for root workspace Rust changes is the workspace command above.

## Standalone project validation

Root workspace commands do not cover standalone projects. If changes affect one of these projects, validate it before PR submission or final handoff in its own
directory and follow any deeper `AGENTS.md` there:

- `rocketmq-example/`
- `rocketmq-dashboard/rocketmq-dashboard-gpui/`
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/`
- `rocketmq-dashboard/rocketmq-dashboard-web/backend/`

When no deeper instruction gives a different command, use:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

For `rocketmq-dashboard/rocketmq-dashboard-tauri/` frontend changes, follow the dashboard `AGENTS.md` in that directory.
For `rocketmq-dashboard/rocketmq-dashboard-web/` changes, follow the nearest Web Dashboard `AGENTS.md`; backend validation runs from `backend/`, and frontend
validation runs from `frontend/`.
For `rocketmq-website/` changes, follow `rocketmq-website/AGENTS.md`.

## Specialized guardrails

- Runtime ownership and blocking: if production changes touch `tokio::spawn`, `JoinSet`, `std::thread`, runtime creation, `block_on`, `spawn_blocking`,
  scheduler loops, shutdown paths, `TaskGroup`, `ScheduledTaskGroup`, `RuntimeOwner`, or `ServiceContext`, run:

```powershell
.\scripts\runtime-audit.ps1 -SkipBaseline
```

- Runtime baseline changes: if the accepted runtime boundary baseline changes, run:

```powershell
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
```

- Typed error architecture: if changes touch `rocketmq-error`, public error mapping, remoting/gRPC/HTTP/CLI error exits, retry/severity/redaction/observability
  error metadata, or sensitive debug fields, run one of:

```powershell
.\scripts\check-error-hygiene.ps1
```

```bash
python scripts/error_architecture_guard.py
```

- Observability feature matrix: if changes touch `rocketmq-observability`, telemetry feature flags, broker observability wiring, or `Cargo.toml` feature
  definitions, run the relevant local subset of the CI matrix from `.github/workflows/rocketmq-rust-ci.yaml`, including `cargo check`, `cargo clippy`, and
  `cargo test -p rocketmq-observability` for affected features such as `observability`, `otlp-metrics`, `otel-metrics,prometheus`, `otlp-traces`, `otlp-logs`,
  and combined OTLP/prometheus features.
- RocksDB store feature: if changes touch `rocksdb_store` behavior in `rocketmq-store` or `rocketmq-broker`, run the relevant CI-equivalent checks:

```bash
cargo clippy -p rocketmq-store --features rocksdb_store --all-targets -- -D warnings
cargo clippy -p rocketmq-broker --features rocksdb_store --all-targets -- -D warnings
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_foundation_tests
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_store_semantics_tests
cargo test -p rocketmq-broker --features rocksdb_store rocksdb
cargo test -p rocketmq-broker --features rocksdb_store pop_consumer
```

## Testing policy

- Run tests only for the modified area by default.
- Prefer the smallest effective scope: named test, module test, package test, then broader integration tests.
- Run broader tests only when the change affects shared infrastructure, feature flags, public cross-crate APIs, or multiple crates.
- For bug fixes, add or update a regression test when practical.
- For new behavior, add or update tests that cover the externally visible behavior.
- Prefer comparing whole values or DTOs when they implement meaningful equality instead of asserting field-by-field copies of the same object.
- Do not add tests that only restate statically defined constants, and do not add negative tests for behavior that was removed.

Examples:

```bash
cargo test -p rocketmq-common
cargo test -p rocketmq-client-rust --lib
cargo test -p rocketmq-remoting some_test_name
```

## Shared code rule

If a shared crate is modified and that change can affect standalone projects, validate the affected standalone projects as well. Check the standalone
`Cargo.toml` files for actual path dependencies before deciding the final scope.

Common shared crates and paths include:

- `rocketmq`
- `rocketmq-common`
- `rocketmq-runtime`
- `rocketmq-client`
- `rocketmq-remoting`
- `rocketmq-macros`
- `rocketmq-error`
- `rocketmq-observability`
- `rocketmq-dashboard/rocketmq-dashboard-common`
- `rocketmq-tools/rocketmq-admin/rocketmq-admin-core`

## Documentation, website, and generated artifacts

- Documentation-only changes outside executable docs do not require Cargo validation.
- Changes under `rocketmq-website/` are website changes, not plain Markdown-only changes; run the website validation from `rocketmq-website/AGENTS.md`.
- Do not commit generated build output, local logs, runtime audit artifacts under `target/`, or Node build directories.
- For paired Markdown/HTML reports, verify both outputs stay aligned with the source data or script that generated them.

## AGENTS routing drift control

Run the routing drift check when changing project boundaries, validation commands, workflow path filters, package manifests, or any `AGENTS.md`:

Windows PowerShell:

```powershell
.\scripts\check-agents-routing.ps1
```

Unix Bash:

```bash
bash ./scripts/check-agents-routing.sh
```

The PowerShell and Bash scripts are equivalent checks and must be kept in sync. They check that root routing mentions all standalone Cargo projects,
Node/Docusaurus projects, critical workflow routes, and specialized guard commands. The architecture decision is documented in
`rocketmq-doc/en/agents-routing-validation-adr.md`.

## Project-local agent assets

- Use `.agents/skills/rocketmq-rust-issue-generator/` when drafting or publishing GitHub issues for this repository.
- Use `.agents/skills/rocketmq-rust-pr-submitter/` when preparing or publishing PR titles, commit messages, and PR bodies for this repository.
- Use `.agents/skills/rust-doc-comment-generator/` for substantial Rustdoc/comment generation.
- Use `.agents/skills/translate-it-doc-en-zh/` for English-to-Chinese technical documentation translation.
- Keep `.agents/skills/**` and `.claude/skills/**` copies aligned when changing duplicated project-local skills.

## Final response expectations

- Summarize the files changed and the intent of the change.
- List validation commands run and their result.
- If required validation was skipped or could not run, explain why and identify the remaining risk.
- Mention any pre-existing unrelated worktree changes only if they matter to the task.
