# rocketmq-model

Runtime-neutral RocketMQ domain types and value-level utilities. This crate has no Tokio dependency and does not create clients, listeners, or background services.

## Public surface

| Module | Responsibility |
| --- | --- |
| [`message`](src/message.rs) and [`common::message`](src/common/message) | Queue identity, message representations, properties, flags, and message conversion helpers. |
| [`codec`](src/codec.rs) | Property-string and byte/hex conversion utilities. |
| [`topic`](src/topic.rs), [`lite`](src/lite.rs), and [`allocation`](src/allocation.rs) | Topic, lite-subscription, and allocation contracts. |
| [`common`](src/common), [`time`](src/time.rs), and [`version`](src/version.rs) | Shared constants, attributes, time utilities, and protocol-version identity. |
| [`boundary_type`](src/boundary_type.rs) and [`result`](src/result.rs) | Shared value types for boundary and result handling. |

`ModelContractViolation` describes invalid model contracts. Shared operational errors use `rocketmq-error`.
Imports use `rocketmq_model`; the package name is `rocketmq-model`. Prefer these types over defining duplicate queue/message identities in consumers. Serialization names and enum values are compatibility surfaces.

## Features and integration

Default features are empty. `simd` enables the optional `simd-json` dependency and the model paths guarded by that feature. It does not add network or runtime ownership.

Wire requests and responses belong to [rocketmq-protocol](../rocketmq-protocol/README.md); runtime capabilities belong to [rocketmq-runtime](../rocketmq-runtime/README.md).

## Validation

Run from the root workspace with its pinned toolchain:

```bash
cargo test -p rocketmq-model --test model_contracts --test type_identity
cargo test -p rocketmq-model --test message_redaction
```

See [`Cargo.toml`](Cargo.toml) for dependencies and feature definitions. Licensed under [Apache-2.0](../LICENSE-APACHE).
