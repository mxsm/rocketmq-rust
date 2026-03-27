## Claude Code

This is a standalone Cargo project.

Do not rely on root workspace validation for this directory.

## Mandatory validation

Run from this directory after every Rust code change:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## Test policy

- Run only the affected tests or examples by default.
- Do not run all examples by default unless the change is broad.

## Examples

```bash
cargo build --example example_name
cargo test
cargo test some_test_name
```
