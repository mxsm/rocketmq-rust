# Agent validation reference

Read this file when a change needs specialist or integration evidence. These are selectable tools,
not cumulative development gates. Routine checks and the stopping rule live in the root
[AGENTS.md](../../AGENTS.md); standalone project commands live in their nearest local guides.

Choose checks for the actual behavior, targets, and features being changed. Reuse a passing test or
Clippy build as compilation evidence. Do not rerun equivalent checks or sweep unrelated historical
findings solely because a task reaches final handoff or PR preparation.

## Root workspace integration

For broad workspace or feature integration, run from the repository root:

```bash
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Format-check only the affected packages with `cargo fmt -p <package> -- --check`.
Select relevant package tests; all features do not replace default/no-feature coverage.

Standalone projects remain outside this command. Inspect their manifests and validate consumers whose
API, feature, wire/storage contract, or shared behavior is affected. Common shared crates include model,
protocol, runtime, client, transport, macros, error, observability, dashboard-common, and admin-core.
Dependency membership alone does not require every consumer or fuzz harness to rebuild for an internal edit.

## Runtime ownership and blocking

For lifecycle changes, start with focused cancellation, shutdown, resource cleanup, and blocking-boundary
tests. If a repository-wide runtime inventory helps the task, use the reporting mode:

```powershell
.\scripts\runtime-audit.ps1 -SkipBaseline
```

PowerShell 7 can run the same script on Unix. `scripts/runtime-audit.sh` is also reporting-only.
Historical boundary baselines are optional audit inputs, not local completion gates.
Explain new ownership boundaries and keep production tasks owned and awaited.

## Typed errors and sensitive output

For changes to public error mapping, retry/severity metadata, redaction, or cross-boundary errors,
run focused behavior tests. Use the architecture guard when the change spans error-policy boundaries:

```powershell
.\scripts\check-error-hygiene.ps1
```

```bash
python scripts/error_architecture_guard.py
```

Choose the platform-appropriate command. Report unrelated findings without turning a local fix into
a repository-wide error cleanup.

## Observability features

Select the affected `cargo check`, Clippy, and `cargo test -p rocketmq-observability` combinations
from `.github/workflows/rocketmq-rust-ci.yaml`. Relevant combinations include `observability`,
`otlp-metrics`, `otel-metrics,prometheus`, `otlp-traces`, `otlp-logs`, and combined OTLP/Prometheus.
Run the complete matrix for feature-wide integration, not an unrelated internal edit.

## RocksDB store

For `rocksdb_store` behavior, select the relevant store/broker commands:

```bash
cargo clippy -p rocketmq-store --features rocksdb_store --all-targets -- -D warnings
cargo clippy -p rocketmq-broker --features rocksdb_store --all-targets -- -D warnings
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_foundation_tests
cargo test -p rocketmq-store --features rocksdb_store --test rocksdb_store_semantics_tests
cargo test -p rocketmq-broker --features rocksdb_store rocksdb
cargo test -p rocketmq-broker --features rocksdb_store pop_consumer
```

Run the full set when the change spans those behaviors or the task calls for store integration.

## Standalone boundaries and CI routing

Consult local AGENTS files for MCP read-only/authentication behavior, MCP-control mutation/audit policy,
SRE execution boundaries, dashboard projects, examples, and fuzz harnesses. Focused boundary checks stay
appropriate for changes to those contracts; full suites are integration choices.

For a workflow dependency-trigger review, the separate metadata guard is available:

```bash
python scripts/standalone_workspace_trigger_guard.py --repo-root .
```

This guard invokes Cargo metadata and can require the standalone projects' toolchains and dependency
resolution. It is deliberately not called by the lightweight AGENTS routing scripts.
Use the existing workflow definitions as the source for full CI commands and platform dependencies.

## Documentation and evidence

Run doctests or `cargo doc -p <package> --no-deps` when changed executable examples, links, or
feature-gated public items need validation. Website content follows the Docusaurus project's guide.
Keep paired Markdown/HTML reports aligned. Record the commands and meaningful results without requiring
SHA/hash/fingerprint matching, clean-worktree checks, or a separate evidence package for routine work.
