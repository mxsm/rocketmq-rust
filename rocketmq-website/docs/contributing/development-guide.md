---
sidebar_position: 2
title: Development Guide
---

# Development Guide

Detailed guide for developing RocketMQ-Rust.

## Development Environment

### Prerequisites

- **Rust**: 1.70.0 or later
- **Git**: For version control
- **Docker**: For running RocketMQ broker in development
- **IDE**: VS Code, IntelliJ IDEA, or similar

### IDE Setup

**VS Code**:

Install extensions:
- rust-analyzer
- CodeLLDB (debugger)
- Even Better TOML
- Error Lens

**IntelliJ IDEA**:

Install the Rust plugin for full IDE support.

### Building from Source

```bash
# Clone repository
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust

# Build in debug mode
cargo build

# Build in release mode
cargo build --release

# Run tests
cargo test --all
```

## Project Structure

```
rocketmq-rust/
├── rocketmq/           # Main library
│   ├── src/
│   │   ├── client/     # Client code
│   │   ├── producer/   # Producer implementation
│   │   ├── consumer/   # Consumer implementation
│   │   ├── model/      # Data models
│   │   ├── protocol/   # Protocol implementation
│   │   └── error/      # Error types
│   └── Cargo.toml
├── rocketmq-remoting/  # Remoting module
├── examples/           # Example code
└── docs/              # Documentation
```

## Running Integration Tests

### Start Test Broker

```bash
# Using Docker
docker run -d -p 9876:9876 --name rmqnamesrv apache/rocketmq:nameserver
docker run -d -p 10911:10911 -p 10909:10909 --name rmqbroker \
  -e "NAMESRV_ADDR=rmqnamesrv:9876" \
  --link rmqnamesrv:rmqnamesrv \
  apache/rocketmq:broker
```

### Run Integration Tests

```bash
# Run integration tests
cargo test --test '*'

# Run specific test
cargo test --test integration test_send_message
```

## Debugging

### Using VS Code Debugger

Create `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "lldb",
      "request": "launch",
      "name": "Debug example",
      "cargo": {
        "args": [
          "build",
          "--example=producer_example"
        ]
      },
      "cwd": "${workspaceFolder}",
      "args": []
    }
  ]
}
```

### Logging

Enable debug logging:

```bash
# Set log level
RUST_LOG=debug cargo run

# Or in code
use log::debug;
env_logger::init();
```

## Code Organization

### Module Structure

```rust
// src/producer/mod.rs
pub mod producer;
pub mod producer_impl;
pub mod transaction_producer;

pub use producer::Producer;
pub use transaction_producer::TransactionProducer;
```

### Error Handling

```rust
// Define error types
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Broker not available: {0}")]
    BrokerNotFound(String),

    #[error("Timeout: {0}ms")]
    Timeout(u64),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// Result type alias
pub type Result<T> = std::result::Result<T, Error>;
```

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::new("Test".to_string(), vec![1, 2, 3]);
        assert_eq!(msg.get_topic(), "Test");
        assert_eq!(msg.get_body(), &vec![1, 2, 3]);
    }
}
```

### Integration Tests

```rust
// tests/integration_test.rs
#[tokio::test]
async fn test_producer_send() {
    let producer = Producer::new();
    producer.start().await.unwrap();

    let message = Message::new("TestTopic".to_string(), b"Test".to_vec());
    let result = producer.send(message).await;

    assert!(result.is_ok());
}
```

### Property-Based Testing

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_message_roundtrip(topic in "[a-zA-Z0-9]+") {
        let msg = Message::new(topic.clone(), vec![1, 2, 3]);
        assert_eq!(msg.get_topic(), topic);
    }
}
```

## Performance Testing

### Benchmarking

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_send_message(c: &mut Criterion) {
    let producer = Producer::new();
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("send_message", |b| {
        b.iter(|| {
            let message = Message::new("Test".to_string(), vec![0; 1024]);
            rt.block_on(producer.send(message)).unwrap();
        });
    });
}

criterion_group!(benches, bench_send_message);
criterion_main!(benches);
```

## Documentation

### Code Documentation

```rust
/// Sends a message to the broker.
///
/// This method sends a message to the RocketMQ broker and returns
/// the send result including message ID and queue information.
///
/// # Arguments
///
/// * `message` - The message to send
///
/// # Returns
///
/// Returns a `Result<SendResult>` containing the send result or an error.
///
/// # Examples
///
/// ```no_run
/// use rocketmq::producer::Producer;
/// use rocketmq::model::Message;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let producer = Producer::new();
/// let message = Message::new("TestTopic".to_string(), b"Hello".to_vec());
/// let result = producer.send(message).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// This function will return an error if:
/// - The broker is not available
/// - The message size exceeds the maximum allowed
/// - Network timeout occurs
pub async fn send(&self, message: Message) -> Result<SendResult> {
    // Implementation
}
```

## Continuous Integration

### GitHub Actions

`.github/workflows/ci.yml`:

```yaml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - uses: actions-rs/cargo@v1
        with:
          command: test
          args: --all
```

## Best Practices

1. **Write tests first**: TDD approach
2. **Keep functions small**: Single responsibility
3. **Use meaningful names**: Self-documenting code
4. **Document public APIs**: Comprehensive docs
5. **Handle errors properly**: Use Result types
6. **Avoid unwraps**: Use proper error handling
7. **Use clippy**: Catch common mistakes
8. **Format code**: Use rustfmt

## Next Steps

- [Coding Standards](./coding-standards) - Code style guidelines
- [Overview](./overview) - Contributing overview
- [Report Issues](https://github.com/mxsm/rocketmq-rust/issues) - File issues
