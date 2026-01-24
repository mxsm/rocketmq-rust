---
sidebar_position: 1
title: Architecture Overview
---

# Architecture Overview

RocketMQ-Rust inherits the proven architecture of Apache RocketMQ while leveraging Rust's performance and safety features.

## System Architecture

```mermaid
graph TB
    subgraph Clients["Client Applications"]
        P1[Producer 1]
        P2[Producer 2]
        C1[Consumer 1]
        C2[Consumer 2]
    end

    subgraph Namesrv["Name Server Cluster"]
        NS1[Name Server 1]
        NS2[Name Server 2]
    end

    subgraph Brokers["Broker Cluster"]
        B1[Broker Master 1]
        B2[Broker Master 2]
        BS1[Broker Slave 1]
        BS2[Broker Slave 2]
    end

    P1 --> B1
    P1 --> B2
    P2 --> B1
    P2 --> B2

    C1 --> B1
    C1 --> B2
    C2 --> B1
    C2 --> B2

    B1 -.-> NS1
    B1 -.-> NS2
    B2 -.-> NS1
    B2 -.-> NS2

    P1 -.-> NS1
    P2 -.-> NS1
    C1 -.-> NS1
    C2 -.-> NS1

    B1 --> BS1
    B2 --> BS2

    style Brokers fill:#f9f,stroke:#333,stroke-width:2px
    style Namesrv fill:#bbf,stroke:#333,stroke-width:2px
```

## Components

### 1. Name Server

The Name Server is a lightweight registry service that provides:

**Functions:**
- Broker registration and discovery
- Route information management
- Heartbeat detection

**Characteristics:**
- Stateless design
- Cluster deployment for high availability
- No data synchronization needed between nodes

**Key Operations:**
```rust
// Client queries name server for broker addresses
let broker_addrs = name_server.lookup_broker("TopicTest").await?;
```

### 2. Broker

Brokers are the core message handling component:

**Responsibilities:**
- Message reception and storage
- Message querying and delivery
- Consumer offset management
- HA (High Availability) replication

**Broker Components:**

```mermaid
graph TB
    subgraph Broker["Broker Instance"]
        Remoting[Remoting Module]
        Store[Storage Module]
        Offset[Offset Manager]
        HA[HA Service]
    end

    Remoting --> Store
    Remoting --> Offset
    Store --> HA

    style Broker fill:#f96,stroke:#333,stroke-width:2px
```

**Message Storage:**
- CommitLog: Sequential storage for all messages
- ConsumeQueue: Index structure for fast consumption
- IndexFile: Hash index for message key queries

### 3. Producer

Producers send messages to brokers:

**Features:**
- Asynchronous sending
- Automatic retry on failure
- Load balancing across brokers
- Transactional message support

**Sending Flow:**

```mermaid
sequenceDiagram
    participant P as Producer
    participant NS as Name Server
    participant B as Broker

    P->>NS: Query route info for topic
    NS-->>P: Return broker list
    P->>B: Send message
    B-->>P: Send result
```

### 4. Consumer

Consumers receive and process messages:

**Types:**
- **Push Consumer**: Event-driven, broker pushes messages
- **Pull Consumer**: Polling-based, consumer pulls messages

**Consumption Models:**
- **Clustering**: Load balancing across consumers
- **Broadcasting**: Each consumer receives all messages

**Consumption Flow:**

```mermaid
sequenceDiagram
    participant C as Consumer
    participant NS as Name Server
    participant B as Broker

    C->>NS: Query route info for topic
    NS-->>C: Return broker list
    C->>B: Pull messages
    B-->>C: Return messages
    C->>C: Process messages
    C->>B: Update offset
```

## Message Flow

### Message Sending

```
1. Producer queries Name Server for topic route info
2. Name Server returns broker list with queue information
3. Producer selects a queue (load balancing)
4. Producer sends message to broker
5. Broker stores message in CommitLog
6. Broker updates ConsumeQueue index
7. Broker returns send result to producer
```

### Message Consumption

```
1. Consumer queries Name Server for topic route info
2. Name Server returns broker list with queue information
3. Consumer rebalances and assigns queues
4. Consumer pulls messages from assigned queues
5. Consumer processes messages
6. Consumer commits offset updates
7. Consumer pulls next batch of messages
```

## High Availability

### Broker HA

Brokers support master-slave replication:

- **Master**: Handles read/write operations
- **Slave**: Replicates master data, handles read operations
- **Synchronization**: Synchronous or async replication

```mermaid
graph TB
    Master[Broker Master]
    Slave[Broker Slave]

    Master -->|Replicate| Slave

    Clients[Clients]
    Clients -->|Write| Master
    Clients -->|Read| Master
    Clients -->|Read| Slave
```

### Name Server HA

Name Servers form a cluster:
- Each broker registers with all name servers
- Clients can query any name server
- No single point of failure

## Rust-Specific Design

### Memory Safety

RocketMQ-Rust leverages Rust's ownership model:
- No manual memory management
- No null pointer dereferences
- No data races at compile time

### Async Architecture

Built on Tokio runtime:
- Non-blocking I/O operations
- Efficient resource utilization
- High concurrency with minimal overhead

### Zero-Cost Abstractions

- Generics for type-safe APIs
- Compile-time optimizations
- No runtime penalty for abstractions

## Next Steps

- [Message Model](../architecture/message-model) - Deep dive into message storage
- [Storage](../architecture/storage) - Learn about persistence mechanisms
- [Configuration](../category/configuration) - Configure your deployment
