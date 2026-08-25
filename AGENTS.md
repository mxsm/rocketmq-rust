# AGENTS.md

## Scope and precedence

- This file applies to the whole repository unless a deeper `AGENTS.md` overrides it.
- Follow the nearest `AGENTS.md` for files under standalone projects and dashboard subdirectories.
- Direct user instructions in the current conversation take precedence over repository instructions.
- If repository instructions conflict, follow the more specific instruction and report the conflict in the final response.

## Repository boundaries

- The repository root is the main Cargo workspace. The source of truth is the root `Cargo.toml` `[workspace].members` list.
- Root workspace validation does not cover standalone Cargo projects or Node/Vite/Docusaurus projects.

| Path | Role | Instruction owner |
|---|---|---|
| `rocketmq-dashboard/rocketmq-dashboard-common/` | Root workspace member and shared dashboard crate | This file |
| `rocketmq-example/` | Standalone Cargo project | `rocketmq-example/AGENTS.md` |
| `rocketmq-macros/tests/fixtures/renamed-consumer/` | Standalone compile fixture for renamed codec dependencies | Its local `AGENTS.md` |
| `rocketmq-tools/rocketmq-mcp/` | Standalone Rust MCP server | Its local `AGENTS.md` |
| `rocketmq-sre/` | Standalone AI SRE Rust workspace and UI | Its local `AGENTS.md` |
| `rocketmq-sre/ui/` | Standalone React/TypeScript/Vite AI SRE frontend | Its local `AGENTS.md` |
| `rocketmq-sre/sdk/typescript/` | Standalone read-only AI SRE TypeScript SDK | Its local `AGENTS.md` |
| `rocketmq-dashboard/rocketmq-dashboard-gpui/` | Standalone Cargo project | Its local `AGENTS.md` |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/` | Standalone Node/Vite/Tauri frontend | Its local `AGENTS.md` |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/` | Standalone Rust backend | Its local `AGENTS.md` |
| `rocketmq-dashboard/rocketmq-dashboard-web/` | Web Dashboard container, not a Cargo workspace root | Its local `AGENTS.md` |
| `rocketmq-dashboard/rocketmq-dashboard-web/backend/` | Standalone Rust backend | Its local `AGENTS.md` |
| `rocketmq-dashboard/rocketmq-dashboard-web/frontend/` | Standalone React/TypeScript/Vite frontend | Its local `AGENTS.md` |
| `rocketmq-website/` | Standalone Docusaurus site | `rocketmq-website/AGENTS.md` |

## Working agreement

- Before editing, inspect the relevant files, the nearest `AGENTS.md`, and `git status --short`.
- Treat existing uncommitted changes as user work. Do not overwrite, revert, or reformat unrelated changes.
- Keep the change scoped to the request; avoid unrelated refactors, dependency updates, and configuration churn.
- Prefer `rg` and `rg --files` for search. If unavailable, use the fastest local equivalent.
- Follow existing module structure, naming, error handling, and test style before introducing a new pattern.
- Use patch-style edits for manual changes. Generated files and formatter output may be produced by their normal tools.
- If a required local command is unavailable, use the documented installation or CI-equivalent path when one exists.
  Do not change project dependencies or configuration merely to bypass missing tooling.
- Add or update a focused test for behavior changes when practical; the test should fail without the change.
- Do not create commits, branches, pull requests, releases, or remote changes unless the user asks.

## Rust engineering guardrails

### Toolchain, compatibility, and API design

- Use the repository toolchain and respect `rustfmt.toml` and `.clippy.toml`. Do not relax lint, formatting,
  feature, or CI configuration merely to make a change pass.
- Prefer clear, idiomatic Rust over clever abstractions. New Rust source files must keep the repository's Apache 2.0 copyright-header style.
- Treat `rust-version` as the MSRV and `rust-toolchain.toml` as the repository toolchain selection. Changes
  to either must keep root and standalone manifests, `.clippy.toml`, and CI aligned.
- Do not normalize editions across projects. The root workspace defaults to Rust 2021;
  `rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/` explicitly uses Rust 2024. Follow each of the five
  standalone Cargo projects' local edition and toolchain rules; `fuzz/` intentionally remains Rust 2021.
- Keep Cargo features additive and explicit. Preserve documented default behavior, gate optional dependencies
  with their owning feature, and validate the exact changed feature combinations.
- Treat public crate APIs, request/response codes and headers, Serde field names/defaults, and persisted record
  layouts as compatibility surfaces. Do not remove, renumber, or change their semantics without an explicit
  compatibility decision and migration plan.
- Prefer enums, request/config structs, builders, and newtypes over positional booleans, ambiguous `Option`
  values, numeric modes, or long public parameter lists. Preserve unavoidable legacy signatures through narrow
  wrappers.
- Keep modules private by default and expose intentional public API through narrow visibility or explicit re-exports.
- Public APIs must explain non-obvious invariants. Add `# Errors`, `# Panics`, and `# Safety` sections when
  applicable, and add examples when they clarify why or how the API is used.
- Prefer exhaustive matches for project-owned enums and compatibility-sensitive behavior. Use wildcard arms
  only when intentionally grouping future or irrelevant cases.
- Any new `#[allow(...)]` must be scoped to the narrowest item and include a reason. Do not add crate- or
  module-wide `allow(warnings)` or broad Clippy-group suppression.
- Prefer native `async fn` methods in traits. `#[allow(async_fn_in_trait)]` is permitted when required by the
  lint for an intentional public async trait API; do not add `#[async_trait]`.
- Treat roughly 500 lines of code as a module review signal and avoid extending high-touch modules beyond roughly
  800 lines of code without a strong local reason. For this guideline, exclude comments, attributes/annotations,
  blank lines, and `use` imports. Do not split unrelated legacy modules solely to meet a number.
- Keep comments focused on invariants, ordering, error handling, protocol compatibility, or concurrency assumptions; do not restate the code.

### Errors, unsafe code, async ownership, and tracing

- Do not add `todo!`, `unimplemented!`, or panic/unwrap/expect for recoverable input, I/O, network, storage,
  protocol, or runtime-lifecycle failures in production paths. Return the project's typed error instead.
- In production paths, permit panic/unwrap/expect only for a documented invariant or an existing compatibility
  facade backed by a fallible API. Document public panic conditions under `# Panics`. Tests may use these
  constructs when they make failures clearer.
- Keep every new or modified `unsafe` region minimal. Place a `// SAFETY:` comment immediately before each
  `unsafe` block or `unsafe impl`, documenting the invariants that make it sound.
- A safe wrapper around unsafe operations must establish the required invariants itself. If callers must uphold
  them, expose an `unsafe fn` or unsafe trait and document its `# Safety` contract.
- New production background work must be owned by an injected `ServiceContext`, parent `TaskGroup`, or another
  established lifecycle owner. Shutdown must cancel and await owned work.
- Route blocking work through the established `BlockingExecutor` or an existing audit-approved top-level
  boundary. Do not add detached `tokio::spawn`, ad hoc runtimes, nested `block_on`, or raw `spawn_blocking`.
  A new exception is a runtime-baseline architecture change, not a local code comment.
- Do not hold synchronous mutex or RwLock guards across `.await`; keep lock scope and hot-path allocations small.
- Prefer `#[tracing::instrument(skip_all, ...)]` with explicit low-cardinality, non-sensitive fields. Use
  call-site `.instrument(...)` only when intentionally binding an existing observation/request span to a future.
- Never record credentials, ACL/TLS material, message bodies, tokens, or whole request/config objects. Avoid
  per-message hot-loop spans unless sampling and overhead are intentional.

## Validation policy

- Validation routes are cumulative: run every profile, project rule, and specialized gate whose trigger matches.
- During iteration, use the smallest useful formatter, check, and test scope. Full workspace validation commands
  are final/pre-PR gates, not requirements after every small edit.
- Before PR submission or final handoff of Rust code, complete a successful format check and applicable Clippy pass for every affected Cargo project.
- Do not run a mutating workspace-wide formatter while unrelated dirty Rust files are present. Format only
  intended files while iterating, then use the non-mutating final checks below.
- A command counts as passed only if it completed with exit code zero. If a required baseline already fails,
  report the exact pre-existing findings, prove the change introduced no new findings, and do not describe the
  passed.
- Rust validation can be slow because of workspace locks and native dependencies. Let relevant commands finish
  unless they clearly fail or exceed the chosen timeout, and never kill unrelated Cargo or rustc processes.
- Documentation-only changes do not require Cargo validation unless they affect generated Rust, build
  configuration, executable examples, or documented Rust commands that need verification.

### Root workspace Rust profile

Run from the repository root. Repeat the format check for every affected root-workspace package, replacing
`<package>` with its Cargo package name:

```bash
cargo fmt -p <package> -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Do not use `cargo fmt --all -- --check` for root-workspace changes. Package-scoped format checks for every affected
package, together with the workspace Clippy command, form the final root-workspace profile.

### Standalone Cargo fallback profile

Follow the nearest local `AGENTS.md`. When it does not define a different profile, run from that standalone Cargo root:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Validation router

| Changed area | Run from | Required final validation | Additional validation or scope |
|---|---|---|---|
| Root workspace Rust crates and `rocketmq-dashboard/rocketmq-dashboard-common/` | Repository root | Root workspace Rust profile plus applicable focused tests for behavior changes | Apply every matching specialized gate below |
| `rocketmq-example/` | `rocketmq-example/` | Follow `rocketmq-example/AGENTS.md` | Revalidate when any repository path dependency in its `Cargo.toml` changes, especially client, model, protocol, transport, runtime, error, observability, or admin-core |
| `rocketmq-sre/` | Its standalone workspace root | Follow `rocketmq-sre/AGENTS.md` | Include every SRE Cargo member and the execution dependency boundary |
| `rocketmq-sre/ui/` | Its project root | Follow its local `AGENTS.md` | Include OpenAPI, routes, shared UI, and package metadata changes |
| `rocketmq-sre/sdk/typescript/` | Its project root | Follow its local `AGENTS.md` | Preserve the fixed read-only SDK and local-only draft boundary |
| `rocketmq-dashboard/rocketmq-dashboard-gpui/` | Its project root | Follow its `AGENTS.md` | Revalidate for `rocketmq-dashboard-common/` or shared dashboard behavior changes |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/` frontend | Its project root | Follow its `AGENTS.md` | Include shared frontend config, shell behavior, and package metadata changes |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/` | Its Cargo root | Follow its `AGENTS.md` | Revalidate when a root path dependency used by the backend changes |
| `rocketmq-dashboard/rocketmq-dashboard-web/backend/` | Its Cargo root | Follow its `AGENTS.md` | Include dashboard-common, admin-core, client/model/protocol/transport/error, and API contract changes |
| `rocketmq-dashboard/rocketmq-dashboard-web/frontend/` | Its project root | Follow its `AGENTS.md` | Include API contract, routing, shared UI, and package metadata changes |
| `rocketmq-website/` | Its project root | Follow `rocketmq-website/AGENTS.md` | Include Docusaurus config, navigation, generated-doc commands, and package metadata changes |
| `AGENTS.md`, `**/AGENTS.md`, `**/Cargo.toml`, `**/package.json`, `.github/workflows/**` | Repository root | AGENTS routing drift control below plus `git diff --check` | This row is additive; also run the owning project profile when build configuration or behavior changes |

## Specialized gates

### Runtime ownership and blocking

If production changes touch `tokio::spawn`, `JoinSet`, `std::thread`, runtime creation, `block_on`,
`spawn_blocking`, scheduler loops, shutdown paths, `TaskGroup`, `ScheduledTaskGroup`, `RuntimeOwner`,
`ServiceContext`, or `BlockingExecutor`, run:

```powershell
.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline
```

On Unix, run the same PowerShell script with PowerShell 7 (`pwsh`). `scripts/runtime-audit.sh` is reporting-only
and is not an equivalent enforcing gate.

Treat changes to `scripts/runtime-audit-baseline.json` as architecture changes: explain the ownership decision,
keep the baseline update scoped, and rerun the enforcing command.

### Typed error architecture

If changes touch `rocketmq-error`, public error mapping, remoting/gRPC/HTTP/CLI exits,
retry/severity/redaction/observability metadata, or sensitive debug fields, run the platform-appropriate guard:

```powershell
.\scripts\check-error-hygiene.ps1
```

```bash
python scripts/error_architecture_guard.py
```

### Observability feature matrix

If changes touch `rocketmq-observability`, telemetry feature flags, broker observability wiring, or relevant
`Cargo.toml` feature definitions, run the affected subset of `.github/workflows/rocketmq-rust-ci.yaml`. Cover the
applicable `cargo check`, Clippy, and `cargo test -p rocketmq-observability` combinations for `observability`,
`otlp-metrics`, `otel-metrics,prometheus`, `otlp-traces`, `otlp-logs`, and combined OTLP/Prometheus features.

### RocksDB store feature

If changes touch `rocksdb_store` behavior in `rocketmq-store` or `rocketmq-broker`, run:

```bash
cargo clippy -p rocketmq-store --features rocksdb_store --all-targets -- -D warnings
cargo clippy -p rocketmq-broker --features rocksdb_store --all-targets -- -D warnings
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_foundation_tests
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_store_semantics_tests
cargo test -p rocketmq-broker --features rocksdb_store rocksdb
cargo test -p rocketmq-broker --features rocksdb_store pop_consumer
```

### RocketMQ MCP

If changes touch `rocketmq-tools/rocketmq-mcp/`, its feature definitions, or a shared public API it consumes,
run the mandatory profile in `rocketmq-tools/rocketmq-mcp/AGENTS.md` from the MCP project root.

Preserve the MCP deny-by-default boundary: default tools remain read-only/diagnostic; `dangerous-tools` requires
compile-time and runtime opt-in, confirmation, and audit; Streamable HTTP remains authenticated by default;
stdio writes protocol frames only to stdout; and sensitive output remains sanitized.

## Testing policy

- Prefer the smallest effective scope: named test, module test, package test, then broader integration tests.
- Broaden tests when a change affects shared infrastructure, feature flags, public cross-crate APIs, wire/storage compatibility, or multiple crates.
- Add regression coverage for bug fixes and externally visible coverage for new behavior.
- Prefer whole-value or DTO equality when it has meaningful semantics instead of copying field-by-field assertions.
- Do not add tests that only restate constants or negative tests for behavior that no longer exists.
- Keep async/concurrency tests deterministic: prefer explicit synchronization or Tokio virtual time over
  arbitrary sleeps, and avoid fixed ports or external network dependencies when practical.
- For feature-gated behavior, test the exact changed feature set; `--all-features` alone may not exercise the same conditional compilation path.

Examples:

```bash
cargo test -p rocketmq-model
cargo test -p rocketmq-client-rust --lib
cargo test -p rocketmq-transport some_test_name
```

## Shared code and cross-project validation

If a shared crate changes, inspect standalone `Cargo.toml` path dependencies and validate every affected consumer. Common shared paths include:

- `rocketmq-model`
- `rocketmq-protocol`
- `rocketmq-runtime`
- `rocketmq-client`
- `rocketmq-transport`
- `rocketmq-macros`
- `rocketmq-error`
- `rocketmq-observability`
- `rocketmq-dashboard/rocketmq-dashboard-common`
- `rocketmq-tools/rocketmq-admin/rocketmq-admin-core`

Changes to `rocketmq-broker`, `rocketmq-controller`, `rocketmq-protocol`, or `rocketmq-store-local` must also
revalidate the standalone `fuzz/` project because those crates are direct path dependencies.

Do not infer consumer scope from directory names alone; use the current manifests.

## Documentation and generated artifacts

- Changes under `rocketmq-website/` are website changes, not plain Markdown-only changes; follow `rocketmq-website/AGENTS.md`.
- For public API or Rustdoc changes, run relevant doctests or `cargo doc` for the affected package when examples, links, or feature-gated items could break.
- Do not commit build output, local logs, runtime audit artifacts under `target/`, coverage output, or Node build directories.
- For paired Markdown/HTML reports, verify both outputs remain aligned with their source data or generator.

## AGENTS routing drift control

Run when changing project boundaries, validation commands, workflow routes, package manifests, routing scripts, the routing ADR, or any `AGENTS.md`.

Windows PowerShell:

```powershell
.\scripts\check-agents-routing.ps1
```

Unix Bash:

```bash
bash ./scripts/check-agents-routing.sh
```

The PowerShell and Bash scripts are equivalent and must stay aligned. They verify required standalone routes,
local instruction files, critical workflow presence, Node/Docusaurus projects, shared-code paths, and specialized
guard commands. The design is documented in `rocketmq-doc/en/agents-routing-validation-adr.md`.

## Project-local agent assets

- Use `.agents/skills/rocketmq-rust-issue-generator/` when drafting or publishing GitHub issues.
- Use `.agents/skills/rocketmq-rust-pr-submitter/` when preparing or publishing PR titles, commit messages, and PR bodies.
- Use `.agents/skills/rust-doc-comment-generator/` for substantial Rustdoc/comment generation.
- Use `.agents/skills/translate-it-doc-en-zh/` for English-to-Chinese technical documentation translation.
- Keep duplicated `.agents/skills/**` and `.claude/skills/**` copies aligned.

## Final response expectations

- Summarize changed files and intent.
- List every validation command run and its result.
- If required validation was skipped or failed, explain why and identify the remaining risk.
- Mention pre-existing unrelated worktree changes only when they matter to the task.
