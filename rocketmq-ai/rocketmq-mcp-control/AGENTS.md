# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-mcp-control/`.

## Boundary

- This directory is a standalone Rust 2021 Cargo workspace excluded from the root workspace.
- HTTPS Streamable MCP with RS256 OAuth/JWKS is the only transport and authentication mode.
- The default feature set must not depend on RocketMQ Admin or client mutation adapters.
- `write-tools` may enable only `rocketmq-admin-core/mutation-client-adapter`; it must never enable a read or full adapter.
- The reviewed production tools are `rocketmq_upsert_topic`,
  `rocketmq_upsert_consumer_group`, `rocketmq_reset_consumer_offset`,
  `rocketmq_patch_broker_config`, and `rocketmq_set_consumer_request_mode`.
  Keep delete, skip, resend, and any free-form mutation outside this project
  until separately reviewed.
- OAuth and closed operation/cluster authorization must complete before mutation argument parsing. A durable
  `started` audit record must then complete before session creation or RPC.
- Do not add stdio, CLI, shell, subprocess, free-form RPC, arbitrary Admin commands, or stdout protocol output.
- Audit and public data must exclude credentials, tokens, network addresses, raw backend errors, message bodies, and client identities.
- Each upsert accepts only 1--64 explicit broker names. All operations validate the full selected-cluster topology
  before target state RPC, keep plans sealed to one Admin session, and preserve exact-target post-read verification.
- Targeted Topic upserts must treat the complete order-Topic KV as a sealed no-write guard. Never merge, put,
  or delete that global KV from the targeted path; reject any selected entry that is not already exact.
- Request-key reuse is bounded and process-local; every invocation, including followers and cache hits, keeps
  its own durable audit pair.
- Prefer native async trait methods where object safety is not required; do not add `async_trait`.

## Mandatory validation

Run from this directory with `INSTA_UPDATE=no` and `RUST_MIN_STACK` unset:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo check --locked --features write-tools
python scripts/check_control_boundary.py
cargo test --locked
cargo test --locked --features write-tools
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo doc --locked --no-deps --all-features
```

Also run the repository AGENTS routing guard, the query MCP read-only boundary and contract snapshot checks, and
the root strict Clippy profile required by the root `AGENTS.md` before final handoff.
