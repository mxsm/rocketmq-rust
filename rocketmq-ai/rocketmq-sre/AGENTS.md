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
- `rocketmq-sre-client` and `rocketmq-sre-cli` expose fixed read operations
  only. They must not add a public arbitrary-request method, raw Admin or shell
  escape hatch, approval, execution, or target mutation API. Local Plan and
  Runbook drafts grant no server-side authority.
- The probe may use producer and consumer APIs only. It must never enable an
  admin or mutation feature.
- Plan, policy, approval, and audit code must not import target mutation
  drivers. The Executor and Execution Agent remain mutation-disabled until
  P3-05 enables the isolated Agent boundary with leases and fencing.
- Do not expose credentials, message bodies, access tokens, TLS material, or
  full configuration values through logs, evidence, diagnostics, or errors.

## Development validation

Choose the changed member(s) from this standalone workspace rather than running the entire workspace:

```powershell
cargo fmt -p <package> -- --check
cargo check --locked -p <package>
cargo test --locked -p <package> <test_name>
```

A focused test build may replace the compile check. Select package Clippy and affected features when useful.
Run `python scripts/check_source_layout.py` for module-layout changes, and
`python scripts/check_execution_dependency_boundary.py` when execution boundaries or dependencies change.

Shared contract/feature changes need their affected members or direct consumers. Whole-workspace
tests, all features, and Rustdoc are integration/CI choices, not a default for every local handoff.
The UI and TypeScript SDK use their own local guides without accumulating this Cargo profile.

Schema artifacts are generated deliberately, not as part of a normal build:

```powershell
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
```
