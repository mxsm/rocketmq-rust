# rocketmq-sre-core

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-core` contains implementation-independent coordination rules for
the AI SRE workspace. It builds on the contracts crate to manage incident
transitions and the lifecycle of registered extension descriptors.

## Responsibilities

- Enforce valid incident-state transitions and terminal-state protection.
- Register versioned evidence sources, diagnostic packs, actions, model
  providers, and integrations.
- Support descriptor upgrade, disable, deprecate, and rollback operations.
- Fail closed for unknown schema majors and unsupported required capabilities.

## Phase 00 boundary

The crate depends only on `rocketmq-sre-contracts` at runtime. It has no
network, database, RocketMQ client, model provider, or service-runtime behavior,
and it does not execute actions.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo test --locked -p rocketmq-sre-core
```

The tests cover incident transitions, duplicate registration, compatible
upgrades, unsupported features, disabling, deprecation, and rollback.
