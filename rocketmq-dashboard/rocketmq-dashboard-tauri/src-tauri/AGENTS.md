# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/`.

## Rules
- This is a standalone Rust Cargo project.
- Do not rely on root workspace validation for this directory.

## Development validation

From this standalone Cargo root, use `cargo fmt --all -- --check` and `cargo check`,
plus focused tests for changed behavior. A relevant test build can replace the separate compile check.
Select `cargo clippy --no-deps -- -D warnings` when useful; add only affected features/targets.
Full-suite validation belongs to the corresponding integration or CI task.

## Test policy
- Run only the affected tests by default.
- Do not run the full test suite unless the change is broad or shared.

Examples:

```bash
cargo test some_test_name
cargo test --lib
cargo test
```

## Cross-project rule
Validate shared crates and this consumer when their API, feature, or behavior changes affect it;
local internal changes do not automatically add other standalone profiles.
