# AGENTS.md

## Scope

This file applies to `rocketmq-tools/rocketmq-mcp/`.

## Boundary

- This directory is a standalone Rust 2021 Cargo workspace.
- The default server is read-only and diagnostic.
- Depend on `rocketmq-admin-core` only through `read-client-adapter`.
- Do not enable `client-adapter`, `mutation-client-adapter`, `admin-full`, or `admin-mutation`.
- Streamable HTTP is authenticated by default. Stdio is local-development only and writes protocol frames only to stdout.
- Tool and Resource output must use the shared authorization, audit, correlation, sanitization, row, and byte policy.
- Prefer native async fn methods in traits. #[allow(async_fn_in_trait)] is permitted when required by the lint for an intentional public async trait API; do not add #[async_trait].

## Mandatory validation

Run from this directory:

```bash
cargo fmt --all -- --check
cargo check --locked
python scripts/check_read_only_boundary.py
cargo test --locked
cargo test --locked --all-features
cargo clippy --locked --all-targets --features streamable-http -- -D warnings
cargo doc --locked --no-deps
```
