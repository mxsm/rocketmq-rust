---
title: "Introduction to RocketMQ-Rust"
---

RocketMQ-Rust implements message services and client APIs in Rust. Applications publish messages to Brokers, discover routes through NameServers, and consume messages using a model suited to their processing needs. The repository also contains Proxy and Controller services, storage implementations, administration tools, dashboards, and AI operations products.

This documentation is maintained for the RocketMQ-Rust project. The repository's distribution identity is an independent community distribution, not an official Apache Software Foundation release. Compatibility with Apache RocketMQ is described at particular protocol and behavior boundaries, rather than as a claim that every upstream component is interchangeable.

## Start with one complete message path

For a first local system, use one NameServer, one Broker with local file storage, and a Rust Producer/LitePull Consumer. Controller, Proxy, dashboards, and AI services are optional additions. They are not prerequisites for sending your first message.

1. Read [installation](getting-started/installation.md) to select the source toolchain and build targets.
2. Follow [local source setup](getting-started/local-source.md) to start the Rust services with explicit addresses and data directories.
3. Complete [the first-message tutorial](getting-started/quick-start.md) to create a Topic and Consumer Group, send, poll, and commit consumption progress.
4. Read [delivery and retry](guides/delivery-and-retry.md) before treating a successful send or commit as a business transaction guarantee.

## What the components do

| Component | Responsibility | When you need it |
| --- | --- | --- |
| NameServer | Broker registration, liveness, and topic-route lookup | The ordinary client discovery path |
| Broker | Request processing, topic/group metadata, message storage and delivery | Every message path |
| Rust client | Producer, Push/LitePull/POP consumption, and optional administration | Rust applications accessing the cluster |
| Proxy | Protocol ingress with cluster or embedded-local backend adaptation | A deployment that needs those ingress modes |
| Controller | Rust controller coordination for a selected HA topology | Controller-managed replication |
| Admin CLI and storage inspection | Cluster administration and explicit offline operations | Development, diagnosis, and maintenance |
| Dashboards | Web or native interfaces around cluster operations | Interactive administration |
| MCP, MCP Control, SRE | Read-only diagnostics, separate controlled changes, and SRE workflows | Optional operations products with their own boundaries |

The system is modular at the library level as well. Message domain types belong to Model, wire types to Protocol, networking to Transport, background-work ownership to Runtime, and storage contracts to Store API. A crate is not necessarily a separate process. See [architecture overview](architecture/overview.md) for the relationship between modules and running components.

## Version and capability scope

These **1.0.0 development** pages describe the current source tree. The root package version is 1.0.0 and its toolchain is Rust 1.95.0. Neither value proves that an identically numbered downloadable artifact has been published. Use [GitHub releases](https://github.com/mxsm/rocketmq-rust/releases) for release-specific artifacts, and keep the corresponding release's configuration and APIs together.

The [capability matrix](overview/capability-matrix.md) separates implementation from build features, runtime configuration, and deployment limits. An available request handler does not establish that every storage backend or topology supports the same behavior.

## Choose a consumption model

Start with [LitePull](consumer/pull-consumer.md) when your application wants an explicit polling loop and progress commits. Use [Push consumption](consumer/push-consumer.md) when you want the client to dispatch messages to application listeners. Push describes the application-facing callback model; its implementation includes client-side pulling and long polling. POP uses receipts and acknowledgement semantics that must be understood separately from offset-based consumption.

For an existing Classic Pull application, consult the current API's compatibility behavior before migrating. The compatibility facade is deprecated, but a runtime-backed builder path exists; detached construction does not provide an initialized running consumer.

## Source references

- [Workspace manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml) and [distribution identity](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/release-identity.json).
- [Client API and runtime ownership](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md).
