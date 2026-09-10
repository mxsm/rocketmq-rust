# AGENTS.md

## Scope and precedence

- Direct user instructions take precedence over repository and skill guidance.
- Root engineering rules apply throughout the repository. The nearest `AGENTS.md` selects local commands
  and exceptions; its validation profile replaces the root fallback rather than accumulating full profiles.
- Apply only instructions relevant to the files and behavior being changed. Resolve routine choices within
  the authorized scope and continue; ask only when missing information materially changes the outcome.
- If an instruction actually blocks authorized work, identify its file and explain the specific conflict.

## Repository map

The root `Cargo.toml` `[workspace].members` list owns the main Cargo workspace. Standalone Cargo and
Node projects require their own commands. Consult the matching local guide when working in these paths:

| Path | Role / local guide |
| --- | --- |
| `rocketmq-dashboard/rocketmq-dashboard-common/` | Root workspace member; this guide |
| `fuzz/` | [Fuzz targets](fuzz/AGENTS.md) |
| `rocketmq-example/` | [Standalone examples](rocketmq-example/AGENTS.md) |
| `rocketmq-macros/tests/fixtures/renamed-consumer/` | [Renamed dependency fixture](rocketmq-macros/tests/fixtures/renamed-consumer/AGENTS.md) |
| `rocketmq-ai/rocketmq-mcp/` | [Read-only MCP](rocketmq-ai/rocketmq-mcp/AGENTS.md) |
| `rocketmq-ai/rocketmq-mcp-control/` | [Isolated mutation control](rocketmq-ai/rocketmq-mcp-control/AGENTS.md) |
| `rocketmq-ai/rocketmq-sre/` | [Standalone SRE workspace](rocketmq-ai/rocketmq-sre/AGENTS.md) |
| `rocketmq-ai/rocketmq-sre/ui/` | [SRE frontend](rocketmq-ai/rocketmq-sre/ui/AGENTS.md) |
| `rocketmq-ai/rocketmq-sre/sdk/typescript/` | [Read-only TypeScript SDK](rocketmq-ai/rocketmq-sre/sdk/typescript/AGENTS.md) |
| `rocketmq-dashboard/rocketmq-dashboard-gpui/` | [Standalone native dashboard](rocketmq-dashboard/rocketmq-dashboard-gpui/AGENTS.md) |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/` | [Tauri frontend](rocketmq-dashboard/rocketmq-dashboard-tauri/AGENTS.md) |
| `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/` | [Tauri backend](rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/AGENTS.md) |
| `rocketmq-dashboard/rocketmq-dashboard-web/` | [Web dashboard boundary](rocketmq-dashboard/rocketmq-dashboard-web/AGENTS.md) |
| `rocketmq-dashboard/rocketmq-dashboard-web/backend/` | [Web backend](rocketmq-dashboard/rocketmq-dashboard-web/backend/AGENTS.md) |
| `rocketmq-dashboard/rocketmq-dashboard-web/frontend/` | [Web frontend](rocketmq-dashboard/rocketmq-dashboard-web/frontend/AGENTS.md) |
| `rocketmq-website/` | [Docusaurus site](rocketmq-website/AGENTS.md) |
| `rocketmq-website/examples/first-message/` | [Standalone first-message tutorial](rocketmq-website/examples/first-message/AGENTS.md) |

## Working agreement

- Before editing, inspect relevant files, the nearest guide, and `git status --short`.
- Preserve existing user changes. Do not overwrite, revert, or reformat unrelated work.
- Keep changes scoped. Follow existing modules, naming, error handling, and test style; avoid incidental
  refactors, dependency upgrades, and configuration churn.
- Prefer `rg` / `rg --files` and patch-style manual edits. Use normal generators and formatters for their outputs.
- Complete authorized implementation and necessary verification without repeated permission requests.
  Do not create commits, branches, PRs, releases, or remote changes unless the user asks.
- Missing optional tools or unrelated historical failures should not block independent work. Use a documented
  setup path when needed; do not change dependencies or lint/CI settings just to bypass a failure.

## Rust engineering

- Respect the selected toolchain, MSRV, `rustfmt.toml`, and `.clippy.toml`. Toolchain changes must keep affected
  manifests and CI aligned. Preserve each project's edition: the root defaults to Rust 2021, while
  `rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/` uses Rust 2024; `fuzz/` remains Rust 2021.
- Keep the repository's Apache 2.0 header on new Rust files.
- Keep features additive and optional dependencies explicitly gated. Preserve default behavior and test
  the changed feature combinations; `--all-features` does not replace feature-absence tests.
- Treat public APIs, request/response codes, headers, Serde fields/defaults, and persisted layouts as
  compatibility surfaces. Semantic changes need an explicit decision and migration approach.
- Prefer enums, config/request structs, builders, and newtypes over ambiguous booleans, numeric modes,
  or long positional APIs. Keep modules private and exports intentional; preserve needed compatibility
  through narrow wrappers.
- Document non-obvious public invariants and applicable `# Errors`, `# Panics`, and `# Safety` contracts.
  Use exhaustive matches for project-owned, compatibility-sensitive enums.
- Scope new lint allowances narrowly and give a reason. No broad warning suppression.
  Prefer native async trait methods; do not add `#[async_trait]`.
- Roughly 500/800 effective lines are module review signals, not size gates. Split by behavior when useful;
  do not split unrelated code just to satisfy a count. Comments should explain invariants and decisions.
- Use typed errors for recoverable input, I/O, network, storage, protocol, and lifecycle failures.
  Do not add `todo!`, `unimplemented!`, or recoverable-path panic/unwrap/expect. Production panic facades
  require a documented invariant or a fallible compatibility API; tests may use assertions and unwraps.
- Keep unsafe regions minimal with an immediately preceding `// SAFETY:` explanation.
  Safe wrappers establish their own invariants; caller obligations require an unsafe API and safety contract.
- Own background work through `ServiceContext`, `TaskGroup`, or an established lifecycle owner.
  Shutdown cancels and awaits owned work. Route blocking work through `BlockingExecutor` or an established
  top-level boundary; do not add detached tasks, ad hoc runtimes, nested `block_on`, or raw `spawn_blocking`.
  Explain any new ownership boundary in the change, without requiring a fingerprint/baseline ceremony.
- Never hold synchronous lock guards across `.await`. Keep lock scopes and hot-path allocations small.
- Prefer `#[tracing::instrument(skip_all, ...)]` with explicit, low-cardinality fields; attach existing spans
  at call sites only intentionally. Never log credentials, ACL/TLS material, tokens, message bodies, or
  entire request/config objects; avoid unsampled per-message spans.
- Preserve the security and product boundaries defined in local MCP, MCP-control, SRE, and dashboard guides.
  Lighter development checks do not change runtime authorization, audit, or protocol behavior.

## Development validation

- Default completion means the affected code compiles, relevant behavior is verified, and intended Rust
  files pass a package-scoped format check. For behavior changes, reuse or add focused regression coverage.
- Select the smallest useful target and actual feature set. A test or Clippy run that compiles the affected
  target can satisfy the compilation check; do not repeat `cargo check` solely to complete a checklist.
- After relevant checks pass, finish the task. Broaden or repeat only for new edits, failures, or a concrete
  unresolved risk. Do not add tests that merely restate constants or mirror low-impact implementation details.
- Keep async tests deterministic with synchronization or virtual time, avoiding sleeps, fixed ports, and
  external services where practical.
- Do not run a mutating workspace-wide formatter while unrelated Rust files are dirty.
- No SHA, file hash, fingerprint, fixed checkout, clean-worktree, historical-baseline, or score requirement
  is part of routine development. Do not demand all historical findings be cleared before delivering a fix.
- Report actual results. Address regressions caused by this change; briefly record unrelated failures,
  missing optional tooling, and untested scope without claiming a pass or expanding into unrelated repairs.
- Do not kill unrelated Cargo/rustc processes. Allow relevant work to finish within a reasonable task timeout.
- Pure documentation/instruction changes need only relevant document/script checks. Run Rustdoc or example
  checks when executable examples or public documentation links could break; website content uses its guide.

Root-workspace commands to select from (replace placeholders; these are not an all-targets checklist):

```bash
cargo fmt -p <package> -- --check
cargo check -p <package>
cargo test -p <package> <test_name>
```

When useful for the change, run `cargo clippy -p <package> --no-deps -- -D warnings` with the affected
features/targets. A routine final response or PR preparation does not automatically require full-workspace
Clippy, all features, or every specialized suite. Existing CI runs retain their actual requirements.

For a standalone Cargo project without more specific commands, use `cargo fmt --all -- --check`,
`cargo check`, and a focused `cargo test <test_name>` from its root, selecting only relevant work as above.

## Shared changes and integration

- Use current manifests to identify consumers when APIs, features, wire/storage contracts, or shared behavior
  change. Validate the directly affected consumers and scenarios; a local internal edit does not automatically
  trigger all standalone projects or fuzz targets.
- Runtime ownership changes need relevant cancellation/shutdown/resource tests. Error mapping changes need
  relevant error/redaction tests. RocksDB and telemetry changes need their affected feature combinations.
- Full-workspace Clippy, feature matrices, metadata consumer audits, long fuzzing, interoperability,
  performance, and deployment/fault tests belong to the corresponding integration, CI, or release task.
  Select them when the task requires that evidence.
- See [validation reference](rocketmq-doc/en/agent-validation-reference.md) only when choosing specialist or
  integration checks; its command lists are not cumulative local gates.
- For changes to project layout, AGENTS routing, or the routing scripts, run the lightweight
  `.\scripts\check-agents-routing.ps1` or `bash ./scripts/check-agents-routing.sh`, plus `git diff --check`.
  The checker covers paths and instruction/workflow existence, not command wording or source identity.
  See the [routing ADR](rocketmq-doc/en/agents-routing-validation-adr.md).
- Keep paired Markdown/HTML artifacts aligned. Do not commit build output, audit artifacts, logs, coverage,
  Node build directories, or other temporary validation output.

## Project skills and reporting

- Use the relevant project skill for issue generation, PR preparation, substantial Rustdoc, or English-to-Chinese
  translation under `.agents/skills/`. Read only the skill needed for the task; keep edited duplicate
  `.agents/skills/**` and `.claude/skills/**` copies aligned.
- Keep the final response concise: what changed, relevant validation results, and any material limitation.
  Include command details needed to reproduce a failure; summarize routine successful checks.
  Mention unrelated worktree changes only when they matter.
