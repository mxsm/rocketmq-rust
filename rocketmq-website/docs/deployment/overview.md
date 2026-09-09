---
title: "Choose a deployment topology"
---

Choose the deployment by the failure model, client ingress and operational responsibility it must support. The smallest learning system and an HA production system share concepts, but their addresses, storage, identities and recovery procedures differ.

## Start with the required components

Ordinary Rust client access needs NameServer discovery and a Broker with usable storage and Topic/Consumer Group configuration. The client must reach both discovery and advertised Broker endpoints. Controller, Proxy, dashboards and AI products are additional choices.

| Topology | Required additions | What it provides | Boundary to evaluate |
| --- | --- | --- | --- |
| Local single Broker | One NameServer, LocalFile Broker and clients | A complete learning and development path | No replica or automatic failover |
| Multiple independent Brokers | Separate Broker identities and stores | More placement and queue capacity | Additional Brokers are not automatically replicas |
| Default master/replica HA | Matching master/replica configuration and HA connectivity | Replication under the chosen role/acknowledgement policy | Actual replica progress and tolerated failure window |
| Controller-managed HA | Rust Controller quorum and Controller-aware Brokers | Coordinated role and replica-state management | Quorum, write authority, leases and persisted membership |
| Proxy Cluster | Proxy plus an existing cluster | gRPC and optional remoting ingress | Backend reachability, security and bounded session state |
| Proxy Local | Proxy owning an embedded Broker-backed composition | Combined ingress and local message service | Shared process failure and lifecycle |
| Kubernetes | Images, storage, identities, placement and orchestration | Repeatable process placement and lifecycle control | Orchestration does not create message durability by itself |

For the first row, follow [local source setup](../getting-started/local-source.md) and [quick start](../getting-started/quick-start.md). Those commands have a matched Topic, Group and client application. Use the remaining rows only after their failure and ownership boundaries are understood.

## Addressing is part of the topology

Keep binding, advertisement and discovery separate. A listener can bind successfully while advertising an address unreachable by its clients. Host loopback, container loopback and Pod addresses are different network locations.

Inventory the NameServer remoting endpoint, Broker normal/fast remoting and HA endpoints, Controller remoting/Raft endpoints, and optional Proxy, probe or telemetry listeners. Configure the listeners actually used by the selected composition; opening only the NameServer port is insufficient.

In a replicated deployment, each member needs its own data path and stable identity. Replicas on one machine can demonstrate protocol behavior but do not tolerate losing that machine. A backup policy also needs a defined recovery boundary; replica availability is not a replacement for backup and restore.

## Source and deployment assets

| Need | Current source entry |
| --- | --- |
| Single-machine Rust setup | [Local tutorial configurations](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/first-message) |
| Master/replica configuration examples | [Broker distribution configs](https://github.com/mxsm/rocketmq-rust/tree/main/distribution/config/broker) |
| Controller peers, storage and startup | [Controller guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/README.md) |
| Proxy Local/Cluster and ingress | [Proxy guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/README.md) |
| Core service container assembly | [Core service Dockerfile](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/container/core-service.Dockerfile) |
| Core Helm deployment profiles | [rocketmq-rust-core chart](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/helm/rocketmq-rust-core/README.md) |
| Broader Kubernetes integration assets | [Kubernetes asset guide](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/kubernetes/README.md) |

The core service Dockerfile copies a supplied binary/configuration into an image; it is not a universal source build command. A source version or Dockerfile does not prove that a matching public image tag exists. Select artifacts from the release actually being deployed.

The repository contains two distinct Helm paths. `rocketmq-rust-core` provides core-service profiles including development, default HA, Controller HA and Proxy TLS. The broader `rocketmq-rust` integration chart and Kubernetes base also include MCP and release-state procedures. Their development profiles, security inputs and image assumptions differ. Do not combine values between them by name alone.

The committed Kubernetes base uses local image fixtures and is documented as non-deployable until its intended image/release inputs are supplied. Rendering YAML is evidence about rendered configuration, not evidence that Pods, PVC recovery or failover succeeded.

## Security and configuration ownership

The local tutorial's insecure development profile requires loopback listeners. A shared-network deployment needs the selected services' real authentication, authorization and transport configuration.

Keep process bootstrap requirements distinct from TLS wiring and resource permissions. Compiling TLS code does not configure a listener or client trust. Enabling authentication without configuring internal client identities can prevent services from communicating.

Use each product's configuration loader and deployment guide. Secret references belong in deployment configuration; secret values belong in the chosen secret store or mounted input. Where credentials or certificates are loaded only at startup, rotation requires the corresponding restart procedure.

## Readiness and shutdown

Readiness should describe the dependencies needed to serve that component's traffic. A Broker process can be alive before storage recovery, registration or role acquisition is complete. A Controller can listen before it has useful quorum/applied state.

`ServiceLifecycle` coordinates readiness/liveness and one shutdown deadline. Give component cleanup enough time within the orchestrator's termination budget, and preserve state directories across normal restarts. See [runtime design](../architecture/runtime.md) for what a shutdown report does and does not prove.

For the core Helm chart, NameServer/Broker/Controller StatefulSets use `OnDelete` updates, while Proxy has its own deployment strategy. Updating a configuration checksum does not automatically restart every stateful service. Changing a Controller peer list also does not perform a live Raft membership change.

## Extend the system deliberately

Add observability after the core message path is usable, then choose the required administration or AI product from [ecosystem overview](../ecosystem/overview.md). Keep each product's storage, identity and ingress distinct.

The [capability matrix](../overview/capability-matrix.md) identifies feature/mode conditions. [Storage design](../architecture/storage.md) explains durability, and [first diagnosis](../operations/first-diagnosis.md) provides the initial running-system checks.

This overview selects paths and explains conditions. It does not claim that the HA, Kubernetes or production-security scenarios were exercised by the local single-Broker tutorial.
