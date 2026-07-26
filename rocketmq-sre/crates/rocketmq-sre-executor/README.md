# rocketmq-sre-executor

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-executor` defines the explicit Phase 00 execution boundary. It
advertises that execution is unavailable and deterministically rejects every
execution attempt.

## Responsibilities

- Provide a machine-readable disabled availability response.
- Return a typed rejection when a caller requests execution.
- Preserve a narrow location for a later, separately reviewed execution
  design.

## Phase 00 boundary

The crate has no binary, RocketMQ Admin dependency, mutation driver, target
credential, approval workflow, background task, or network client. It cannot
apply, delete, update, reset, clean, or truncate cluster state.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo check --locked -p rocketmq-sre-executor
cargo test --locked -p rocketmq-sre-executor
```

Validation confirms availability remains disabled and all execution requests
are rejected.
