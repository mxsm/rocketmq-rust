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

## Quick Start

### Installation

Build from source:

```bash
cargo build --release --bin rocketmq-controller-rust
```

The binary will be located at `target/release/rocketmq-controller-rust`.

### Running the Controller

#### 1. Using Default Configuration

Start the controller with default settings:

```bash
# Set ROCKETMQ_HOME environment variable
export ROCKETMQ_HOME=/opt/rocketmq

# Run the controller
./target/release/rocketmq-controller-rust
```

#### 2. Using Configuration File (TOML)

Create a configuration file `controller.toml`:

```toml
# Basic Configuration
rocketmq_home = "/opt/rocketmq"
controller_type = "Raft"

# Node Configuration
node_id = 1
listen_addr = "127.0.0.1:9878"

# Controller Behavior
scan_not_active_broker_interval = 3000
controller_thread_pool_nums = 16

# Raft Configuration
election_timeout_ms = 1000
heartbeat_interval_ms = 500
storage_path = "/opt/rocketmq/controller/storage"

# Raft Peers (3-node cluster example)
[[raft_peers]]
id = 1
addr = "127.0.0.1:9878"

[[raft_peers]]
id = 2
addr = "127.0.0.1:9879"

[[raft_peers]]
id = 3
addr = "127.0.0.1:9880"
```

Start with configuration file:

```bash
./target/release/rocketmq-controller-rust -c controller.toml
```

Or using the long form:

```bash
./target/release/rocketmq-controller-rust --config-file /path/to/controller.toml
```

#### 3. Print Configuration

View the current configuration without starting:

```bash
./target/release/rocketmq-controller-rust -c controller.toml -p
```

Output example:
```
========== Controller Configuration ==========
RocketMQ Home:           /opt/rocketmq
Config Store Path:       "/opt/rocketmq/controller/controller.toml"
Controller Type:         Raft
Scan Interval:           3000 ms
Thread Pool Nums:        16

========== Node Configuration ==========
Node ID:                 1
Listen Address:          127.0.0.1:9878

========== Raft Configuration ==========
Election Timeout:        1000 ms
Heartbeat Interval:      500 ms
Raft Peers:              3 peers
  - Node 1: 127.0.0.1:9878
  - Node 2: 127.0.0.1:9879
  - Node 3: 127.0.0.1:9880

========== Storage Configuration ==========
Storage Path:            /opt/rocketmq/controller/storage
Storage Backend:         RocksDB
Mapped File Size:        1073741824 bytes
```

#### 4. View Help

```bash
./target/release/rocketmq-controller-rust --help
```

Output:
```
RocketMQ Controller Server (Rust)

Usage: rocketmq-controller-rust [OPTIONS]

Options:
  -c, --config-file <FILE>  Controller config file (TOML/JSON/YAML)
  -p, --print-config-item   Print all config items
  -m, --print-important-config  Print important config items
  -h, --help               Print help
  -V, --version            Print version
```

### Running a 3-Node Cluster

For a production setup with 3 controller nodes:

**Node 1** (controller.toml):
```bash
export ROCKETMQ_HOME=/opt/rocketmq
./rocketmq-controller-rust -c controller-node1.toml
```

**Node 2** (controller-node2.toml):
```toml
node_id = 2
listen_addr = "192.168.1.102:9878"
# ... same raft_peers configuration
```

```bash
export ROCKETMQ_HOME=/opt/rocketmq
./rocketmq-controller-rust -c controller-node2.toml
```

**Node 3** (controller-node3.toml):
```toml
node_id = 3
listen_addr = "192.168.1.103:9878"
# ... same raft_peers configuration
```

```bash
export ROCKETMQ_HOME=/opt/rocketmq
./rocketmq-controller-rust -c controller-node3.toml
```

### Configuration Formats

The controller supports multiple configuration formats:

- **TOML** (`.toml`) - Recommended, Rust-native format
- **JSON** (`.json`) - Standard JSON format
- **YAML** (`.yaml`, `.yml`) - YAML format

Example JSON configuration:

```json
{
  "rocketmq_home": "/opt/rocketmq",
  "node_id": 1,
  "listen_addr": "127.0.0.1:9878",
  "election_timeout_ms": 1000,
  "raft_peers": [
    {"id": 1, "addr": "127.0.0.1:9878"},
    {"id": 2, "addr": "127.0.0.1:9879"},
    {"id": 3, "addr": "127.0.0.1:9880"}
  ]
}
```

### Stopping the Controller

Press `Ctrl+C` to gracefully shutdown the controller:

```
Controller is running. Press Ctrl+C to stop.
^C
Received shutdown signal, shutting down controller...
Controller shutdown completed.
```

## Development

See [CLI_README.md](CLI_README.md) for detailed CLI usage and [CLI_MAPPING.md](CLI_MAPPING.md) for Java-Rust parameter mapping.

