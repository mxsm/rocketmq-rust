# RocketMQ Controller

RocketMQ Controller Module - High Availability Controller based on Raft

## Introduction

RocketMQ Controller is the core management component of RocketMQ cluster, responsible for:

- **Cluster Metadata Management**: Broker registration, Topic configuration, cluster configuration, etc.
- **High Availability**: Master-slave failover based on Raft consensus algorithm
- **Leader Election**: Automatic leader node election and failover
- **Data Consistency**: Ensures strong data consistency through Raft log replication

## Architecture

```
┌──────────────────────────────────────────┐
│         Controller Manager               │
├──────────────────────────────────────────┤
│                                          │
│  ┌────────────┐  ┌────────────────────┐ │
│  │   Raft     │  │   Metadata Store   │ │
│  │ Controller │  │                    │ │
│  │            │  │  - Broker Manager  │ │
│  │ - Election │  │  - Topic Manager   │ │
│  │ - Replica  │  │  - Config Manager  │ │
│  └────────────┘  └────────────────────┘ │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │     Processor Manager              │  │
│  │                                    │  │
│  │  - Register Broker                │  │
│  │  - Heartbeat                      │  │
│  │  - Create/Update Topic            │  │
│  │  - Query Metadata                 │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
```

## Features

### ✅ Implemented
- Basic project structure
- Configuration management (ControllerConfig)
- Error handling (ControllerError)
- Raft controller framework
- Metadata storage (Broker, Topic, Config)
- Processor manager framework

### 🚧 In Progress
- Complete Raft node implementation
- Network communication layer
- RPC processor implementation

### 📋 Planned
- Persistent storage (RocksDB/custom logging)
- Snapshot management
- Complete integration tests
- Performance benchmarks
- Monitoring metrics

## Quick Start

### Basic Usage

```rust
use rocketmq_controller::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create configuration
    let config = ControllerConfig::new(
        1,  // node_id
        "127.0.0.1:9876".parse().unwrap()
    )
    .with_raft_peers(vec![
        RaftPeer { id: 1, addr: "127.0.0.1:9876".parse().unwrap() },
        RaftPeer { id: 2, addr: "127.0.0.1:9877".parse().unwrap() },
        RaftPeer { id: 3, addr: "127.0.0.1:9878".parse().unwrap() },
    ])
    .with_storage_path("/data/controller".into());

    // Create and start Controller
    let manager = ControllerManager::new(config).await?;
    manager.start().await?;

    // Wait...
    
    // Graceful shutdown
    manager.shutdown().await?;
    Ok(())
}
```

## Dependencies

Main dependencies:

- `raft-rs` - Raft consensus algorithm implementation
- `tokio` - Async runtime
- `dashmap` - Concurrent hash map
- `serde` - Serialization/deserialization
- `tracing` - Logging and tracing

## Development

### Build

```bash
cargo build -p rocketmq-controller
```

### 测试

```bash
cargo test -p rocketmq-controller
```

### Benchmark

```bash
cargo bench -p rocketmq-controller
```

## Comparison with Java Version

| Feature | Java (DLedger) | Rust (raft-rs) |
|---------|---------------|----------------|
| Consensus Algorithm | DLedger | raft-rs |
| Async Model | Netty | Tokio |
| Concurrency Control | ConcurrentHashMap | DashMap |
| Error Handling | Exceptions | Result<T, E> |
| Type Safety | Runtime | Compile-time |

## Performance Goals

- Leader election latency: < 500ms
- Heartbeat throughput: > 10,000 ops/s
- Metadata write latency: < 10ms (p99)
- Metadata read latency: < 1ms (p99)

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../CONTRIBUTING.md).

## License

Licensed under Apache License 2.0 or MIT license, at your option.
