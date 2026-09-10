---
title: "Reference entry points"
---

Use a reference when you already know the operation and need an exact option, type or compatibility rule. Start with [quick start](../getting-started/quick-start.md) for a procedure, or [architecture overview](../architecture/overview.md) for responsibilities.

## Find the owner of a question

| Question | Authoritative entry |
| --- | --- |
| Which packages and minimum Rust version belong to this source? | [Root manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml) and [toolchain](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml) |
| Which files/settings start the Broker? | [Broker configuration](../configuration/broker-config.md) and [deployment overview](../deployment/overview.md) |
| Which NameServer flags and merge rules apply? | [Service configuration](service-configuration.md#nameserver) |
| How do Controller/Proxy modes start? | [Service configuration](service-configuration.md) and the corresponding [deployment guide](../deployment/overview.md) |
| Which client builders and Cargo features exist? | [Client configuration](../configuration/client-config.md) and [manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml) |
| What does a send result mean? | [Producer result table](../producer/overview.md) and [canonical result types](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs) |
| What does LitePull commit mean? | [Polling and commit semantics](../consumer/pull-consumer.md) |
| Which administration command accepts this option? | [Admin CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/README.md) and that subcommand's `--help` |
| Which error identity should an integration retain? | [Error guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/README.md) and [catalog](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/src/catalog.rs) |
| Which request/response codes or header types are used? | [Protocol guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/README.md) and [source](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-protocol/src) |
| Which limits and TLS capabilities belong to networking? | [Transport guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/README.md) |
| Which storage durability and progress types apply? | [Storage design](../architecture/storage.md) and [Store API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-api/README.md) |
| How are metrics, logs and traces configured? | [Observability guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/README.md) and [configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/config.rs) |
| Which product-specific configuration applies? | [Ecosystem overview](../ecosystem/overview.md) and the selected standalone product |

These source references describe the current checkout family. A published release can have different options and APIs; use its source/tag and artifacts together.

## Read a configuration field completely

For each field, identify the external key, owning section, type, default, unit, constraints, precedence and reload behavior. Do not infer those properties solely from a Rust field name.

For example, Broker `listenPort` belongs in `[broker]`, while its nested server configuration supplies binding details. The source uses separate Broker metadata and message-store roots. Copying a flat legacy file or moving a field to a similarly named section can change parsing or be rejected.

An option may require a build feature and a runtime setting. The Rust client has no standalone `tls` Cargo feature; the transport implementation and actual endpoint configuration control that capability. Likewise, a metrics feature does not automatically select an exporter endpoint.

## Inspect the command you will run

From the repository root:

```bash
cargo run -p rocketmq-admin-cli -- --help
cargo run -p rocketmq-admin-cli -- topic updateTopic --help
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup --help
```

CLI options belong to their declared level. Current `clusterList` and `updateSubGroup` use `NAMESRV_ADDR` rather than accepting `-n`; Topic commands accept their own `-n`. A command name copied from another RocketMQ tool is not proof that all flags are interchangeable.

Before running a command, distinguish reads from metadata changes, offset changes or data operations. [First diagnosis](../operations/first-diagnosis.md) provides a small read-only sequence and explains state effects.

## Generate Rust API documentation for your source

To inspect the current client API locally:

```bash
cargo doc -p rocketmq-client-rust --no-deps --open
```

This generates documentation for the selected package and feature graph. Add the features your application actually uses, and generate standalone products from their own manifest. Published API documentation may describe a different release from 1.0.0 development.

Import curated public types from the crate root or its documented `api`/`prelude`. Files under implementation modules are not automatically public integration contracts.

## Compatibility has several dimensions

Rust source API, serialized fields, request/response codes, persisted layouts, Controller internals and operational behavior are separate compatibility surfaces. Matching one does not establish the others.

The [capability matrix](../overview/capability-matrix.md) records relevant mode/feature conditions. The [module map](../architecture/module-map.md) identifies owners when a shared contract changes. Keep exact versions and observed scenarios with compatibility claims rather than using a blanket “fully compatible” label.

## Reference and migration pages

- [Features and platforms](./features-platforms.md): package defaults, native prerequisites and runtime conditions.
- [Errors and status](./errors.md): stable identities, boundary mappings and retry decisions.
- [Protocol compatibility](./protocol-compatibility.md): endpoints, scope, evidence and storage boundaries.
- [Admin CLI](./admin-cli.md): command catalog, parameters, effects and exit behavior.
- [Java migration](../migration/java-to-rust.md): client, cluster and data migration procedures.
- [Rust API migration](../migration/rust-api.md): public imports, owned runtimes and Classic/LitePull changes.

## API and terminology

- [Rust API entry points](./rust-api.md)
- [Bilingual glossary](./glossary.md)
