# rocketmq-sre-execution-agent

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-execution-agent` reserves the future process boundary for
policy-controlled change execution. In Phase 00 it is a library-only marker
that reports the execution agent as disabled.

## Responsibilities

- Expose the explicit disabled execution-agent state.
- Keep future execution wiring separate from diagnostics and evidence
  collection.

## Phase 00 boundary

There is no binary, scheduler, target credential, mutation driver, queue
consumer, approval workflow, or connection to `rocketmq-sre-executor`.
Importing this crate cannot start or authorize an operation.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo check --locked -p rocketmq-sre-execution-agent
cargo test --locked -p rocketmq-sre-execution-agent
```

Validation confirms the public state remains disabled and the dependency graph
contains only the shared contracts boundary.
