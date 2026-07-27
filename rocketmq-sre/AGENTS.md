# RocketMQ AI SRE working agreement

## Scope

This file applies to the standalone `rocketmq-sre` Cargo workspace. The root
repository instructions also apply unless this file is more specific.

## Architecture boundaries

- This standalone workspace uses Rust 2024 and the modern module layout.
  Represent a module as `foo.rs` with child modules under `foo/`; do not add
  `foo/mod.rs`.
- `rocketmq-sre-contracts` stays independent of networking, async runtimes,
  databases, model SDKs, and RocketMQ implementation crates.
- `rocketmq-sre-core` depends only on `rocketmq-sre-contracts`.
- The connector communicates with RocketMQ MCP over its public wire protocol;
  it must not import the MCP server crate or its Rust DTOs.
- The probe may use producer and consumer APIs only. It must never enable an
  admin or mutation feature.
- Plan, policy, approval, and audit code must not import target mutation
  drivers. The Executor and Execution Agent remain mutation-disabled until
  P3-05 enables the isolated Agent boundary with leases and fencing.
- Do not expose credentials, message bodies, access tokens, TLS material, or
  full configuration values through logs, evidence, diagnostics, or errors.

## Validation

Run from this directory:

```powershell
cargo fmt -p rocketmq-sre-contracts -p rocketmq-sre-core -p rocketmq-sre-model-gateway -p rocketmq-sre-control-plane -p rocketmq-sre-connector -p rocketmq-sre-executor -p rocketmq-sre-execution-agent -p rocketmq-sre-probe -p rocketmq-sre-eval -- --check
cargo check --locked --workspace
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo doc --locked --workspace --no-deps
python scripts/check_source_layout.py
```

Schema artifacts are generated deliberately, not as part of a normal build:

```powershell
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
```
