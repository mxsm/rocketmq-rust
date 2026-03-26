# AGENTS.md

## Scope
This file applies to the whole repository unless a deeper `AGENTS.md` overrides it.

## Toolchain
- Use the repository Rust toolchain.
- Respect `rustfmt.toml` and `.clippy.toml`.
- Do not change toolchain or lint configuration unless explicitly required.

## Mandatory validation
After every Rust code change, always run the following for the affected Cargo project:
1. `cargo fmt`
2. `cargo clippy`

Do not finish with unformatted code.
Do not finish without a clippy pass.

## Test policy
- Tests only need to cover the modified area.
- Do not run full-repository or full-workspace tests by default.
- Prefer the smallest effective scope:
  - package-level tests
  - module-related tests
  - named tests
  - integration tests related to the change
- Run broader tests only when the change affects shared infrastructure or multiple crates/projects.

## Repository structure
- The repository root is the main Cargo workspace.
- `rocketmq-example` is a standalone Cargo project and must be validated separately.
- `rocketmq-dashboard/rocketmq-dashboard-gpui` is a standalone Cargo project and must be validated separately.
- `rocketmq-dashboard/rocketmq-dashboard-tauri` is not a Cargo workspace root.
- The Rust backend for Tauri is in `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri` and must be validated there separately.

## Workspace changes
For changes inside the root workspace, run from the repository root:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

Tests should remain targeted. Example:

```bash
cargo test -p rocketmq-common
cargo test -p rocketmq-client
cargo test -p rocketmq-remoting some_test_name
```

## Standalone project rule
Root workspace validation does not cover standalone projects.

If changes affect any of the following, validate them in their own directories:
- `rocketmq-example`
- `rocketmq-dashboard/rocketmq-dashboard-gpui`
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri`

## Shared code rule
If a shared crate is modified and that change can affect a standalone project, also validate the affected standalone project.

## Change hygiene
- Keep changes scoped to the task.
- Add or update tests when behavior changes.
- Prefer minimal targeted validation over broad validation.
