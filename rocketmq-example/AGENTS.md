# AGENTS.md

## Scope
This file applies to `rocketmq-example/`.

## Rules
- This is a standalone Cargo project.
- Do not rely on root workspace validation for this directory.

## Mandatory validation
Run from `rocketmq-example/` before PR submission or final handoff for Rust code changes:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

## Test policy
- Run only the affected tests or examples by default.
- Do not run all examples by default unless the change is broad.

Examples:

```bash
cargo build --example example_name
cargo test
cargo test some_test_name
```
