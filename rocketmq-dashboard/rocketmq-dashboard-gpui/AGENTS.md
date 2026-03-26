# AGENTS.md

## Scope
This file applies to `rocketmq-dashboard/rocketmq-dashboard-gpui/`.

## Rules
- This is a standalone Cargo project.
- Do not rely on root workspace validation for this directory.

## Mandatory validation
Run from `rocketmq-dashboard/rocketmq-dashboard-gpui/` after every Rust code change:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

## Test policy
- Run only the affected validation by default.
- Prefer targeted tests or `cargo check` for compile-scope changes.
- Run full `cargo test` only when the change has wider impact.

Examples:

```bash
cargo check
cargo test some_test_name
cargo test
```
