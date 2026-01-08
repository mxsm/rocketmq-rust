# The Rust Implementation of Apache RocketMQ Broker

## Overview

This module is mainly the implementation of the [Apache RocketMQ](https://github.com/apache/rocketmq) Broker, containing all the functionalities of the Java version Broker.

## Getting Started

### Requirements

1. rust toolchain MSRV is 1.75.(stable,nightly)

### Run Broker

**Run the following command to see usage：**

```shell
cargo run --bin rocketmq-broker-rust -- --help
```

**Output:**

```
Apache RocketMQ Broker Server implemented in Rust
For more information: https://github.com/mxsm/rocketmq-rust

Usage: rocketmq-broker-rust [OPTIONS]

Options:
  -c, --configFile <FILE>
          Broker config properties file path

          If not specified, will try to load from:

          1. $ROCKETMQ_HOME/conf/broker.toml (if ROCKETMQ_HOME is set)

          2. Default configuration values

  -p, --printConfigItem
          Print all configuration items and exit

          Prints all broker configuration properties including:

          - Broker configuration

          - Netty server configuration

          - Netty client configuration

          - Message store configuration

          - Authentication configuration

  -m, --printImportantConfig
          Print important configuration items and exit

          Prints only the important configuration items that are most

          commonly used for broker setup and troubleshooting.

  -n, --namesrvAddr <ADDR>
          Name server address list

          Format: '192.168.0.1:9876' or '192.168.0.1:9876;192.168.0.2:9876'

          Multiple addresses should be separated by semicolon (;)

          Can also be set via NAMESRV_ADDR environment variable

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

**Run the following command to start the broker:**

```shell
cargo run --bin rocketmq-broker-rust
```

### Usage Examples

#### 1. Start with default configuration

```shell
# Set ROCKETMQ_HOME environment variable first
# Windows
set ROCKETMQ_HOME=D:\rocketmq

# Linux/macOS
export ROCKETMQ_HOME=/opt/rocketmq

# Then start broker
cargo run --bin rocketmq-broker-rust
```

#### 2. Start with custom configuration file

```shell
cargo run --bin rocketmq-broker-rust -- -c ./conf/broker.toml
```

#### 3. Start with custom name server address

```shell
# Single name server
cargo run --bin rocketmq-broker-rust -- -n 192.168.1.100:9876

# Multiple name servers
cargo run --bin rocketmq-broker-rust -- -n "192.168.1.100:9876;192.168.1.101:9876"
```

#### 4. Print all configuration items

```shell
cargo run --bin rocketmq-broker-rust -- -p
```

#### 5. Print important configuration items only

```shell
cargo run --bin rocketmq-broker-rust -- -m
```

#### 6. Using environment variables

```shell
# Set name server via environment variable
# Windows
set NAMESRV_ADDR=192.168.1.100:9876

# Linux/macOS
export NAMESRV_ADDR=192.168.1.100:9876

# Then start broker (will use the environment variable)
cargo run --bin rocketmq-broker-rust
```

### Configuration

#### Configuration Priority

The broker uses the following configuration priority (highest to lowest):

1. **Command-line arguments** (`-c`, `-n`)
2. **Environment variables** (`NAMESRV_ADDR`)
3. **Configuration file** (`broker.toml`)
4. **Default values**

#### Environment Variables

- `ROCKETMQ_HOME`: RocketMQ installation directory (required)
- `NAMESRV_ADDR`: Name server address (optional), format: `127.0.0.1:9876` or `192.168.0.1:9876;192.168.0.2:9876`

#### Configuration File

If no configuration file is specified via `-c`, the broker will try to load from:
- `$ROCKETMQ_HOME/conf/broker.toml`

Example `broker.toml`:

```toml
[broker_identity]
broker_name = "broker-a"
broker_cluster_name = "DefaultCluster"
broker_id = 0

namesrv_addr = "127.0.0.1:9876"
broker_ip1 = "127.0.0.1"
listen_port = 10911
store_path_root_dir = "./store"
enable_controller_mode = false
```

#### Exit Codes

- **0**: Normal exit (when using `-p` or `-m` flags)
- **-1**: Invalid command-line arguments
- **-2**: `ROCKETMQ_HOME` environment variable not set
- **-3**: Failed to parse configuration file
- **-4**: Invalid broker configuration

## Feature

**Feature list**:

- **Not support**: 💔 ❌
- **Base support**: ❤️ ✅
- **Perfect support**: 💖 ✅

| Feature                      | request code       | Support | remark                                  |
|------------------------------|--------------------|---------|-----------------------------------------|
| topic config load            | :heavy_minus_sign: | 💔 ❌    | TopicConfigManager class function       |
| topic queue mapping load     | :heavy_minus_sign: | 💔 ❌    | TopicQueueMappingManager class function |
| consume offset load          | :heavy_minus_sign: | 💔 ❌    | ConsumerOffsetManager class function    |
| subscription group load      | :heavy_minus_sign: | 💔 ❌    | SubscriptionGroupManager class function |
| consumer filter load         | :heavy_minus_sign: | 💔 ❌    | ConsumerFilterManager class function    |
| consumer order info load     | :heavy_minus_sign: | 💔 ❌    | ConsumerOrderInfoManager class function |
| message store load           | :heavy_minus_sign: | 💔 ❌    |                                         |
| timer message store load     | :heavy_minus_sign: | 💔 ❌    |                                         |
| schedule message store load  | :heavy_minus_sign: | 💔 ❌    |                                         |
| send message hook            | :heavy_minus_sign: | 💔 ❌    |                                         |
| consume message hook         | :heavy_minus_sign: | 💔 ❌    |                                         |
| send message                 | 10                 | ❤️ ✅    |                                         |
| send message v2              | 310                | ❤️ ✅    |                                         |
| send batch message           | 320                | ❤️ ✅    |                                         |
| consume send message back    | 36                 | ❤️ ✅    |                                         |
| pull message                 | 11                 | ❤️ ✅    |                                         |
| lite pull message            | 361                | ❤️ ✅    |                                         |
| peek message                 | 200052             | 💔 ❌    |                                         |
| pop message                  | 200050             | ❤️ ✅    |                                         |
| ack message                  | 200051             | ❤️ ✅    |                                         |
| batch ack message            | 200151             | ❤️ ✅    |                                         |
| change message invisibletime | 200053             | ❤️ ✅    |                                         |
| notification                 | 200054             | 💔 ❌    |                                         |
| polling info                 | 200055             | 💔 ❌    |                                         |
| send reply message           | 324                | ❤️ ✅    |                                         |
| send reply message v2        | 325                | ❤️ ✅    |                                         |
| query message                | 12                 | ❤️ ✅    |                                         |
| view message by id           | 33                 | ❤️ ✅    |                                         |
| heart beat                   | 34                 | ❤️ ✅    |                                         |
| unregister client            | 35                 | ❤️ ✅    |                                         |
| check client config          | 46                 | 💔 ❌    |                                         |
| get consumer list by group   | 38                 | ❤️ ✅    |                                         |
| update consumer offset       | 15                 | ❤️ ✅    |                                         |
| query consumer offset        | 14                 | ❤️ ✅    |                                         |
| query assignment             | 400                | ❤️ ✅    |                                         |
| set message request mode     | 401                | ❤️ ✅    |                                         |
| end transacation             | 37                 | ❤️ ✅    |                                         |
| default processor            | :heavy_minus_sign: | 💔 ❌    | AdminBrokerProcessor class function     |







