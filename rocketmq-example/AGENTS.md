# AGENTS.md

## Scope
This file applies to `rocketmq-example/`.

## Rules
- This is a standalone Cargo project.
- Do not rely on root workspace validation for this directory.

## Development validation

From this standalone root, format intended files and compile/test only the affected example:

```bash
cargo fmt --all -- --check
cargo check --example example_name
```

Use `cargo test some_test_name` for changed testable behavior. Add package Clippy when useful;
all-target builds and running every example are integration choices, not routine handoff requirements.

## Test policy
- Run only the affected tests or examples by default.
- Do not run all examples by default unless the change is broad.

Examples:

```bash
cargo build --example example_name
cargo test
cargo test some_test_name
```
