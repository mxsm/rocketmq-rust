---
title: "Architecture"
permalink: /docs/architecture/
excerpt: "Rocketmq rust Architecture"
last_modified_at: 2025-01-02T08:48:05-04:00
redirect_from:
  - /theme-setup/
toc: false
classes: wide
---

![Architecture](/assets/images/architecture.png)

## 1. System Architecture Composition

Apache RocketMQ-Rust adopts a distributed architecture. Its core components include **Producer (message producer)**, **Consumer (message consumer)**, **Name Server (routing service)**, and **Broker cluster (message storage and forwarding node)**. Through efficient network communication and a coordination mechanism, it achieves low-latency and highly reliable message processing.

## 2. Technical Details of Core Components

### 1. Producer (Message Producer)

- **Functional Positioning**: Encapsulate business data into messages and push them to the RocketMQ cluster.
- Technical Implementation:
  - Use the Tokio-based remoting transport to query Name Server routing metadata (Topic queue mapping and Broker addresses).
  - Support multiple load - balancing strategies (such as `RandomQueue` for random queue selection and `RoundRobin` for round - robin), ensuring uniform message distribution.
  - Provide synchronous, asynchronous, and one - way sending modes to adapt to different reliability and performance requirements.

### 2. Name Server (Routing Service)

- **Technical Positioning**: A stateless distributed routing center with a decentralized design to avoid single - point failures.
- Core Mechanisms:
  - **Routing Registration**: After the Broker starts, it registers metadata (Broker address, cluster, Topic queue) with the Name Server through regular heartbeats (default 30 seconds).
  - **Routing Discovery**: Producers and Consumers periodically pull the routing table (default 30 seconds) to perceive dynamic changes in the Broker.
  - **Data Storage**: Store routing information based on an in - memory `ConcurrentMap` to ensure query and update performance.

### 3. Broker Cluster (Message Storage and Forwarding)

- Architectural Design:
  - Master - slave replication architecture. The Master supports writes and reads while replicas provide backup and eligible reads. The implemented HA paths support synchronous/asynchronous replication, and controller mode uses the pinned OpenRaft stack for leadership decisions.
  - Multi-cluster deployment supports horizontal scaling. It does not by itself
    provide cross-region replication or regional disaster recovery; those
    boundaries are defined by the
    [regional disaster recovery ADR](regional-disaster-recovery-adr.md).
- Core Functions:
  - **Message Storage**: Based on memory - mapped files (MappedFile), unified storage in CommitLog, and ConsumeQueue index queues to improve read and write performance.
  - **Message Forwarding**: Provide a write interface for Producers and long - polling pull (Pull) or active push (Push) for Consumers.
  - **High - Availability Guarantee**: The master - slave switching mechanism ensures that the Slave can quickly take over read services when the Master fails.

### 4. Consumer (Message Consumer)

- Consumption Modes:
  - **Push Mode**: Encapsulate the Pull operation, and the client controls the pull frequency to achieve real - time consumption.
  - **Pull Mode**: Actively pull messages, suitable for scenarios where fine - grained control of the consumption rate is required.
- Technical Implementation:
  - Based on the Topic subscription relationship, obtain the queue distribution from the Name Server and allocate consumption queues through a load - balancing algorithm (such as average distribution).
  - Support ordered consumption (queue orderliness) and concurrent consumption (multithreaded processing of queues), and ensure message processing reliability through consumption Offset.

## 3. Technical Implementation of Core Interactions

1. Broker Registration and Routing Discovery:
   - The Broker registers with regular heartbeats. If the Name Server does not receive a heartbeat for more than 120 seconds, it determines that the Broker has failed.
   - Producers and Consumers periodically pull the routing table to update local Broker addresses and Topic routing.
2. Message Sending Link:
   - The Producer selects a Broker queue from the routing table and sends through the Tokio transport. The Broker writes to the CommitLog, applies the configured replication policy, and returns the result.
3. Message Consumption Link:
   - The Consumer long - polls messages from the Broker, updates the consumption Offset after processing, and ensures processing reliability.

## 4. Technical Advantages of the Architecture

- **High - Performance Storage**: Memory-mapped files, bounded transfer strategies, and batched indexing reduce unnecessary copies. Measured results belong to the versioned architecture performance evidence rather than this overview.
- **High-availability mechanisms**: Broker replication, NameServer routing, and
  optional Controller mode provide failover building blocks. The measurable
  RPO/RTO depends on the acknowledgement profile and the exact candidate
  evidence described by the
  [acknowledgement and failover ADR](acknowledgement-failover-contract-adr.md).
- **Flexible Scalability**: Multi - Broker cluster deployment horizontally scales message processing capabilities.
- **Rich Functions**: Based on the architecture, features such as transactional messages (two - phase commit), message filtering (SQL/Tag filtering), and message backtracking are implemented to adapt to scenarios such as finance and e - commerce.

## 5. Summary of Rust Architecture Advantages

| Dimension | Rust implementation |
|---|---|
| **Memory Safety** | Ownership and borrowing constrain aliasing; audited unsafe regions document their invariants. |
| **Asynchronous I/O** | Tokio tasks are owned by lifecycle-aware task groups and shut down with explicit deadlines. |
| **Concurrency Control** | Channels, sharded concurrent maps, atomics, and locks are selected according to the data contract. |
| **Memory Efficiency** | `bytes::Bytes`, bounded queues, and transfer capabilities make ownership and copying decisions explicit. |
| **Error Handling** | Typed errors preserve category, retry, severity, redaction, and source information across boundaries. |

These mechanisms define the implementation boundaries. Performance,
reliability, and promotion claims apply only to a candidate whose current
fault, soak, performance, acknowledgement, and rollback evidence passes the
[production-readiness runbook](architecture-production-readiness-runbook.md).
