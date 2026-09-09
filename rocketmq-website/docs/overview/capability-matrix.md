---
title: "Capabilities and implementation boundaries"
---

Use this page to decide which component and configuration belong in your system. It describes the current source, not a blanket production certification or a complete feature-parity statement for Apache RocketMQ.

## Read a capability as a set of conditions

“Implemented” means a concrete source path exists. “Enabled” additionally requires the right build features, runtime settings, credentials, and backend. “Exercised” describes a particular test or deployment scenario. “Released” means a published artifact contains it. These are different claims.

The repository's `v1-capability-manifest.json` records a defined 1.0 core scope, with profile-specific implementation and evidence fields. Its recorded `component` or `interop` state is not a claim that this documentation task reran those scenarios, and the manifest is not the complete capability catalog for Dashboard and AI products.

## Core services and clients

| Capability | Current source surface | Conditions and limits |
| --- | --- | --- |
| Topic routing and broker discovery | NameServer registration, expiry, route snapshots and queries | A live Broker must register useful topic metadata; a listening NameServer alone does not create routes |
| Ordinary message storage and delivery | Broker processors and LocalFile storage | Role, permissions, topic/group configuration and disk state matter |
| Producer sending | Sync-result, callback, one-way, batch and queue-selection APIs | Result guarantees differ; one-way has no broker acknowledgement to inspect |
| Push consumption | Client listeners, rebalance and pull/long-poll scheduling | Application callbacks do not mean the Broker opens unsolicited delivery connections |
| LitePull consumption | Explicit polling, subscription/assignment and offset management | Application processing and progress commits are separate operations |
| Classic Pull compatibility | Deprecated facade with runtime-backed compatibility implementation | Detached constructors cannot run initialized operations; new applications should prefer LitePull |
| POP | Request-mode, receipt, invisible-time and ACK handling | Requires the selected Topic/Group request mode and matching broker behavior |
| Transactions, ordering, filtering, delay and recall | Client APIs plus corresponding broker processors | Each feature has its own topic, group, filter or timer conditions; an API symbol alone is insufficient |
| Admin APIs | Client and Admin Core read/mutation surfaces | Cargo feature availability does not grant runtime permissions |

The first-message tutorial deliberately selects ordinary messages, a LocalFile master, and LitePull. Add an advanced message model only after its processing, retry, and acknowledgement semantics are clear.

## Storage, replication and ingress

| Area | Selection | Boundary |
| --- | --- | --- |
| Local storage | Broker/Store defaults select local file storage | Main-log durability and derived visibility are separate |
| RocksDB | Broker `rocksdb_store` and the compatible store configuration | Follow the actual backend's log/derived-structure and recovery contracts; it is not an arbitrary online replacement |
| Tiered storage | Optional integration with tiered storage components | Remote/derived progress cannot strengthen the primary write acknowledgement |
| Default HA | A configured master/replica deployment | Replica acknowledgement policy and the actual failure model determine the guarantee |
| Controller HA | Rust Controller coordination and compatible broker configuration | Do not assume mixed Java Controller/JRaft/DLedger membership or internal protocol compatibility |
| Proxy Cluster | Cluster backend adaptation with remote services | Configure downstream discovery, identity, protocol and resource limits |
| Proxy Local | In-process backend adaptation | Embedded ownership and shutdown differ from a remote cluster connection |
| NameServer embedded Controller | `embedded-controller` build feature plus `enableControllerInNamesrv` | Default NameServer builds exclude the Controller dependency; the setting alone cannot enable it |
| Transport security | Appropriate transport feature and endpoint configuration | Auth bootstrap checks and actual TLS listener/client wiring are separate |

The client crate has no `tls` feature of its own. Client applications requiring TLS must enable the transport TLS implementation in their dependency graph and configure the connection appropriately. Similarly, selecting an observability feature does not by itself configure an exporter.

## Operations products

| Product | Purpose | Separate boundary |
| --- | --- | --- |
| Web Dashboard | Browser UI and server backend | Frontend/backend build, authentication and deployment |
| GPUI / Tauri Dashboard | Native desktop administration | Native dependencies, platform support and application packaging |
| MCP | Read-only cluster diagnostics | stdio/HTTP transport, identity and allowed read tools |
| MCP Control | Independently configured controlled mutation tools | Opt-in implementation, registered operations, authorization and audit |
| AI SRE | Contracts, connectors, model interaction and SRE workflows | Product stage and registered execution paths; a plan is not authority to mutate a cluster |

These products have their own manifests and guides. In particular, do not translate broad SRE roadmap language into a statement that every executor is currently enabled.

## What to record when choosing a deployment

Record the source/release you use, product, build feature set, storage and ingress mode, authentication settings, and the failure behavior your application depends on. Then follow the matching guide. A runtime change in any of those conditions can invalidate an earlier operational assumption even when the public method name stays unchanged.

## Sources and next steps

- [Core capability manifest](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/v1-capability-manifest.json) and [core release scope](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json).
- [Broker](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/README.md), [Client](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md), [Controller](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/README.md), [Proxy](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/README.md).
- [Local setup](../getting-started/local-source.md) and [delivery and retry](../guides/delivery-and-retry.md).
