# AGENTS.md

## Scope and boundary

- This crate is a repository-root workspace member for UI- and HTTP-independent native Dashboard persistence.
- Preserve the existing GPUI file layouts, migration protection, audit acceptance/finish invariants, and file
  directory's single-writer ownership. SQL uses its own `native_dashboard_*` tables; Web migrations are unrelated.
- Keep SQL drivers optional and additive. Default builds must not compile SQLx or require a database.
- Keep connection credentials out of Debug, errors, persisted selection, history, and audit.
- Accept an owned `ChildServiceContext` for file I/O. Do not create runtimes or detached tasks. The caller owns
  closing the store before its runtime stops.

## Validation

Run root `cargo fmt -p rocketmq-dashboard-storage -- --check` and focused
`cargo test -p rocketmq-dashboard-storage` with default features and each affected driver feature. SQL service
tests are opt-in and use injected connection URLs; do not claim skipped tests as database verification.
Validate GPUI separately when changing its actual persistence wiring.
