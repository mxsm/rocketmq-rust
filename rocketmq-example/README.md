# RocketMQ-Rust Examples

Comprehensive examples demonstrating how to use RocketMQ-Rust client APIs and features.

## 📋 Overview

This is a **standalone project** containing practical examples for RocketMQ-Rust. It showcases various usage patterns including:

- **Consumer Examples**: Push consumer, pop consumer, message listeners
- **Producer Examples**: *(Coming soon)*
- **Admin Operations**: *(Coming soon)*

## 🚀 Quick Start

### Prerequisites

- Rust 1.85.0 or later
- Running RocketMQ nameserver and broker (default: `127.0.0.1:9876`)

### Running Examples

```bash
# Navigate to the examples directory
cd rocketmq-example

# Run the pop consumer example
cargo run --example pop-consumer

# List all available examples
cargo run --example <TAB>
```

## 📚 Available Examples

### Consumer Examples

#### Pop Consumer
Demonstrates how to use the pop consumption model with client-side load balancing disabled.

**File**: [examples/consumer/pop_consumer.rs](examples/consumer/pop_consumer.rs)

**Run**:
```bash
cargo run --example pop-consumer
```

**Features**:
- Pop-based message consumption
- Concurrent message processing
- Automatic topic/consumer group creation
- Message request mode configuration

## 🔧 Configuration

Most examples use default configuration:

```rust
const NAMESRV_ADDR: &str = "127.0.0.1:9876";
const TOPIC: &str = "TopicTest";
const CONSUMER_GROUP: &str = "please_rename_unique_group_name_4";
```

Modify these constants in the example source files to match your RocketMQ cluster setup.

## 📖 Development Guide

### Adding New Examples

1. Create a new file in the appropriate category folder (e.g., `examples/consumer/my_example.rs`)
2. Add the example configuration to `Cargo.toml`:

```toml
[[example]]
name = "my-example"
path = "examples/consumer/my_example.rs"
```

3. Run the example:
```bash
cargo run --example my-example
```

### Dependencies

This project references local RocketMQ-Rust crates:

- `rocketmq-client-rust` - Client APIs
- `rocketmq-common` - Common utilities
- `rocketmq-rust` - Core runtime
- `rocketmq-tools` - Admin tools

## 🏗️ Project Structure

```
rocketmq-example/
├── examples/
│   └── consumer/
│       └── pop_consumer.rs
├── Cargo.toml
└── README.md
```

## 📝 Notes

- This is a **standalone project**, not part of the main RocketMQ-Rust workspace
- Examples use relative path dependencies to reference parent crates
- Each example is self-contained and runnable independently

## 🤝 Contributing

Contributions of new examples are welcome! Please ensure:

1. Examples are well-documented with inline comments
2. Configuration is clearly specified
3. Examples follow Rust best practices
4. Each example demonstrates a single, clear use case

## 📄 License

This project inherits the dual-license from RocketMQ-Rust:
- Apache License 2.0
- MIT License