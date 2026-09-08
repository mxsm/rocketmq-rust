# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-mcp/`.

## Boundary

- This directory is a standalone Rust 2021 Cargo workspace.
- The default server is read-only and diagnostic.
- Depend on `rocketmq-admin-core` only through `read-client-adapter`.
- Do not enable `client-adapter`, `mutation-client-adapter`, `admin-full`, or `admin-mutation`.
- Streamable HTTP is authenticated by default. Stdio is local-development only and writes protocol frames only to stdout.
- Tool and Resource output must use the shared authorization, audit, correlation, sanitization, row, and byte policy.
- Prefer native async fn methods in traits. #[allow(async_fn_in_trait)] is permitted when required by the lint for an intentional public async trait API; do not add #[async_trait].

## Development validation

From this directory, use `cargo fmt --all -- --check`, `cargo check --locked`,
and focused `cargo test --locked <test_name>` cases for the changed behavior.
A test build may replace the separate compile check.

Run `python scripts/check_read_only_boundary.py` when tools, resources, authorization, adapter
dependencies, or feature boundaries change. Test the affected transport/feature combination;
changes spanning default and HTTP behavior need coverage of both.

For MCP integration, select broader tests, `cargo clippy --locked --all-targets --features streamable-http -- -D warnings`,
or `cargo doc --locked --no-deps` as relevant. Do not automatically stack root workspace checks or
run all features for an internal fix.
