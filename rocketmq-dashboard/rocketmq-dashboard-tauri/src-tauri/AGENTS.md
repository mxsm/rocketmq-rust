# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/`.

## Rules
- This is a standalone Rust Cargo project.
- Do not rely on root workspace validation for this directory.

## Mandatory validation
Run from `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/` after every Rust code change:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

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
If shared Rust crates referenced by this project are modified, validate those shared crates in the repository root as needed.
