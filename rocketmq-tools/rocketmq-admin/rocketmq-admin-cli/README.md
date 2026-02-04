# RocketMQ-Rust Admin CLI

A powerful command-line interface for managing both RocketMQ-Rust and Apache RocketMQ clusters, implemented in Rust. This is the Rust implementation
of [Apache RocketMQ Tools](https://github.com/apache/rocketmq/tree/develop/tools), providing a fast, safe, and ergonomic CLI experience with full compatibility
for both platforms.

## 📖 Overview

`rocketmq-admin-cli` is a comprehensive admin tool that provides full management capabilities for RocketMQ clusters, including:

- **Topic Management**: Create, update, delete, and query topics
- **Consumer Management**: Manage consumer groups and monitor consumption
- **NameServer Operations**: Query and manage NameServer metadata
- **Controller Operations**: Interact with RocketMQ controllers
- **ACL/Auth Management**: Manage access control and authentication

This CLI tool is built on top of [`rocketmq-admin-core`](../rocketmq-admin-core), which provides the core business logic.

## ✨ Features

- 🚀 **High Performance**: Built with Rust for blazing-fast execution
- 🛡️ **Type Safety**: Leverages Rust's type system to prevent errors
- 📝 **Rich Output Formats**: Support for Table, JSON, YAML output
- 🔧 **Auto-completion**: Shell completion for Bash, Zsh, Fish
- 💡 **Intuitive Commands**: Well-organized command hierarchy
- 🔄 **Compatible**: Implements all Apache RocketMQ Tools commands
- ⚡ **Async Runtime**: Built on Tokio for efficient I/O operations

## 📦 Installation

### Prerequisites

- Rust 1.75 or higher
- A running RocketMQ cluster (NameServer + Broker)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust

# Build the CLI tool
cargo build --release -p rocketmq-admin-cli

# The binary will be available at:
# target/release/rocketmq-admin-cli
```

### Install via Cargo

```bash
cargo install --path rocketmq-tools/rocketmq-admin/rocketmq-admin-cli
```

## 🚀 Quick Start

### Basic Usage

```bash
# Show help information
rocketmq-admin-cli --help

# Show all available command categories
rocketmq-admin-cli show

# Topic commands help
rocketmq-admin-cli topic --help
```

### Common Operations

#### Topic Management

```bash
# List all topics
rocketmq-admin-cli topic topicList -n 127.0.0.1:9876

# Get topic cluster list
rocketmq-admin-cli topic topicClusterList -t MyTopic -n 127.0.0.1:9876

# Get topic route information
rocketmq-admin-cli topic topicRoute -t MyTopic -n 127.0.0.1:9876

# Create/Update topic
rocketmq-admin-cli topic updateTopic \
    -t MyTopic \
    -c DefaultCluster \
    -r 8 \
    -w 8 \
    -n 127.0.0.1:9876

# Delete topic
rocketmq-admin-cli topic deleteTopic -t MyTopic -c DefaultCluster -n 127.0.0.1:9876
```

#### Consumer Management

```bash
# Query consumer group information
rocketmq-admin-cli consumer consumerProgress -g MyConsumerGroup -n 127.0.0.1:9876

# Get consumer connection
rocketmq-admin-cli consumer consumerConnection -g MyConsumerGroup -n 127.0.0.1:9876

# Get consumer status
rocketmq-admin-cli consumer consumerStatus -g MyConsumerGroup -n 127.0.0.1:9876

# Update subscription group
rocketmq-admin-cli consumer updateSubGroup \
    -g MyConsumerGroup \
    -c DefaultCluster \
    -n 127.0.0.1:9876

# Delete subscription group
rocketmq-admin-cli consumer deleteSubGroup -g MyConsumerGroup -c DefaultCluster -n 127.0.0.1:9876
```

#### NameServer Operations

```bash
# Get KV config
rocketmq-admin-cli nameserver getKVConfig -s namespace -k key -n 127.0.0.1:9876

# Update KV config
rocketmq-admin-cli nameserver updateKVConfig -s namespace -k key -v value -n 127.0.0.1:9876

# Delete KV config
rocketmq-admin-cli nameserver deleteKVConfig -s namespace -k key -n 127.0.0.1:9876

# Wipe write permission
rocketmq-admin-cli nameserver wipeWritePerm -b BrokerName -n 127.0.0.1:9876

# Add write permission
rocketmq-admin-cli nameserver addWritePerm -b BrokerName -n 127.0.0.1:9876
```

#### Controller Operations

```bash
# Get controller configuration
rocketmq-admin-cli controller getControllerConfig -a 127.0.0.1:9878

# Update controller configuration
rocketmq-admin-cli controller updateControllerConfig -k key -v value -a 127.0.0.1:9878

# Get controller metadata
rocketmq-admin-cli controller getControllerMetadata -a 127.0.0.1:9878
```

#### ACL/Auth Management

```bash
# Get all ACL configuration
rocketmq-admin-cli auth getAcl -n 127.0.0.1:9876

# Get user ACL
rocketmq-admin-cli auth getUserAcl -u username -n 127.0.0.1:9876

# Update ACL
rocketmq-admin-cli auth updateAcl \
    -u username \
    -p password \
    -t topic \
    --perm PUB|SUB \
    -n 127.0.0.1:9876

# Delete ACL
rocketmq-admin-cli auth deleteAcl -u username -t topic -n 127.0.0.1:9876
```

## 📋 Command Categories

The CLI organizes commands into logical categories:

| Category       | Description                 | Example Commands                                                             |
|----------------|-----------------------------|------------------------------------------------------------------------------|
| **topic**      | Topic management operations | `topicList`, `updateTopic`, `deleteTopic`, `topicRoute`, `topicStatus`       |
| **consumer**   | Consumer group management   | `consumerProgress`, `consumerConnection`, `updateSubGroup`, `deleteSubGroup` |
| **nameserver** | NameServer operations       | `getKVConfig`, `updateKVConfig`, `deleteKVConfig`, `wipeWritePerm`           |
| **controller** | Controller management       | `getControllerConfig`, `updateControllerConfig`, `getControllerMetadata`     |
| **auth**       | ACL and authentication      | `getAcl`, `getUserAcl`, `updateAcl`, `deleteAcl`, `copyAcl`                  |

### View All Command Categories

```bash
# Show categorized command table
rocketmq-admin-cli show
```

## 🔧 Advanced Features

### Shell Completion

Generate shell completion scripts for better command-line experience:

```bash
# Bash
rocketmq-admin-cli --generate-completion bash > /etc/bash_completion.d/rocketmq-admin-cli

# Zsh
rocketmq-admin-cli --generate-completion zsh > ~/.zsh/completion/_rocketmq-admin-cli

# Fish
rocketmq-admin-cli --generate-completion fish > ~/.config/fish/completions/rocketmq-admin-cli.fish
```

### Environment Variables

You can set the NameServer address via environment variable:

```bash
# Set default NameServer address
export ROCKETMQ_NAMESRV_ADDR="127.0.0.1:9876;127.0.0.1:9877"

# Now you can omit -n flag
rocketmq-admin-cli topic topicList
```

### Skip Confirmation Prompts

For dangerous operations (like delete), use `-y` or `--yes` to skip confirmation:

```bash
# Delete topic without confirmation
rocketmq-admin-cli topic deleteTopic -t MyTopic -c DefaultCluster -n 127.0.0.1:9876 -y
```

### Output Formatting

Control output format with appropriate flags (implementation may vary by command):

```bash
# JSON output (if supported)
rocketmq-admin-cli topic topicList -n 127.0.0.1:9876 --output json

# YAML output (if supported)
rocketmq-admin-cli topic topicList -n 127.0.0.1:9876 --output yaml

# Table output (default)
rocketmq-admin-cli topic topicList -n 127.0.0.1:9876
```

## 🏗️ Architecture

```text
┌─────────────────────────────────────────┐
│       rocketmq-admin-cli (binary)       │
│  - CLI entry point (main.rs)            │
│  - Command-line argument parsing        │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│     rocketmq-admin-core (library)       │
│  - Core business logic                  │
│  - Command implementations              │
│  - Output formatters                    │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│      rocketmq-client (library)          │
│  - DefaultMQAdminExt                    │
│  - MQAdminExt trait                     │
│  - Admin API implementations            │
└─────────────────────────────────────────┘
```

## 📚 Documentation

- [RocketMQ Rust Documentation](https://mxsm.github.io/rocketmq-rust/)
- [Apache RocketMQ Documentation](https://rocketmq.apache.org/docs/)
- [RocketMQ Admin Core README](../rocketmq-admin-core/README.md)
- [API Guidelines](https://github.com/mxsm/rocketmq-rust/blob/main/CONTRIBUTING.md)

## 🤝 Contributing

Contributions are welcome! Please see our [Contributing Guide](../../../CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli

# Run in development mode
cargo run -- topic topicList -n 127.0.0.1:9876

# Run tests
cargo test

# Build release version
cargo build --release
```

### Adding New Commands

1. Add command definition in `rocketmq-admin-core/src/commands/`
2. Implement the `CommandExecute` trait
3. Add command to appropriate category enum
4. Update documentation and tests

Example:

```rust
// In rocketmq-admin-core/src/commands/topic_commands/my_command.rs
use crate::commands::CommandExecute;

#[derive(clap::Args)]
pub struct MyTopicCommand {
    #[arg(short, long)]
    topic_name: String,
}

impl CommandExecute for MyTopicCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // Implementation here
        Ok(())
    }
}
```

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../../LICENSE-APACHE) file for details.

## 🔗 Related Projects

- [rocketmq-rust](https://github.com/mxsm/rocketmq-rust) - The main RocketMQ Rust implementation
- [Apache RocketMQ](https://github.com/apache/rocketmq) - Original Java implementation
- [rocketmq-admin-core](../rocketmq-admin-core) - Core admin library
- [rocketmq-admin-tui](../rocketmq-admin-tui) - Terminal UI for admin operations

## 🙋 Support

- GitHub Issues: [Report bugs or request features](https://github.com/mxsm/rocketmq-rust/issues)
- Discussions: [Ask questions and share ideas](https://github.com/mxsm/rocketmq-rust/discussions)
- Apache RocketMQ Community: [Join the community](https://rocketmq.apache.org/community/)

## 📈 Roadmap

- [x] Basic topic management commands
- [x] Consumer management commands
- [x] NameServer operations
- [x] Controller operations
- [x] ACL/Auth management
- [ ] Broker management commands
- [ ] Message query and resend commands
- [ ] Cluster monitoring commands
- [ ] Performance testing tools
- [ ] Interactive mode (REPL)
- [ ] Configuration file support
- [ ] Batch operations support

## 🌟 Acknowledgments

This project is inspired by and aims to be compatible with:

- [Apache RocketMQ Tools](https://github.com/apache/rocketmq/tree/develop/tools)
- [RocketMQ Console](https://github.com/apache/rocketmq-dashboard)

Special thanks to all contributors who have helped make this project better!

---

Made with ❤️ by the RocketMQ Rust Community
