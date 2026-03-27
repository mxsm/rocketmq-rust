# Rust Validation

- After every Rust code change, always run `cargo fmt` and `cargo clippy` for the affected Cargo project.
- Do not finish with unformatted Rust code.
- Do not finish without a clippy pass.

## Root workspace

For changes inside the root workspace, run from the repository root:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Standalone project reminder

- Root workspace validation does not cover standalone Cargo projects.
- Standalone projects must be validated in their own directories.
