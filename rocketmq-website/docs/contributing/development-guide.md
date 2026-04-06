---
sidebar_position: 2
title: Development Guide
---

# Development Guide

Detailed guide for developing RocketMQ-Rust.

## Development Environment

### Prerequisites

- **Rust**: nightly toolchain
- **Git**: For version control
- **IDE**: VS Code, RustRover, or similar

### IDE Setup

**VS Code**:

Install extensions:

- rust-analyzer
- CodeLLDB (debugger)
- Even Better TOML
- Error Lens

**RustRover**:

RustRover comes with built-in Rust support. No additional plugins required.

### Installing Rust Nightly

```bash
# Install rustup if you haven't already
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install nightly toolchain
rustup toolchain install nightly

# Set nightly as default (optional)
rustup default nightly

# Or use nightly for this project only
rustup override set nightly
```

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

RocketMQ-Rust is a workspace-based project with multiple crates. Here's the high-level structure:

```tree
rocketmq-rust/
├── rocketmq/              # Core library (utilities, scheduling, concurrency)
├── rocketmq-auth/         # Authentication and authorization
├── rocketmq-broker/       # Broker implementation
├── rocketmq-cli/          # Command-line interface tools
├── rocketmq-client/       # Client library (producer & consumer)
│   ├── src/
│   │   ├── admin/         # Admin tools
│   │   ├── base/          # Base client functionality
│   │   ├── common/        # Common utilities
│   │   ├── consumer/      # Consumer implementation
│   │   ├── producer/      # Producer implementation
│   │   ├── factory/       # Client factory
│   │   ├── implementation/ # Implementation details
│   │   ├── latency/       # Latency tracking
│   │   ├── hook/          # Hooks and interceptors
│   │   ├── trace/         # Message tracing
│   │   └── utils/         # Utility functions
├── rocketmq-common/       # Common data structures and utilities
├── rocketmq-controller/   # Controller component
├── rocketmq-doc/          # Documentation resources
├── rocketmq-error/        # Error types and handling
├── rocketmq-example/      # Example code
├── rocketmq-filter/       # Message filtering
├── rocketmq-macros/       # Procedural macros
├── rocketmq-namesrv/      # Name server implementation
├── rocketmq-proxy/        # Proxy server
├── rocketmq-remoting/     # Remoting/communication layer
├── rocketmq-runtime/      # Runtime utilities
├── rocketmq-store/        # Message storage
├── rocketmq-tools/        # Development tools
├── rocketmq-tui/          # Terminal user interface
├── rocketmq-website/      # Documentation website
├── Cargo.toml             # Workspace configuration
├── Cargo.lock             # Lock file
├── CHANGELOG.md           # Change log
├── CONTRIBUTING.md        # Contributing guidelines
├── README.md              # Project README
└── resources/             # Additional resources
```

## Running Tests

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
pub use transaction_producer::TransactionMQProducer;
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
    let msg = Message::builder()
      .topic("Test")
      .body(vec![1, 2, 3])
      .build()
      .unwrap();
    assert_eq!(msg.topic().as_str(), "Test");
    assert_eq!(msg.body().unwrap().to_vec(), vec![1, 2, 3]);
    }
}
```

### Integration Tests

```rust
// tests/integration_test.rs
#[tokio::test]
async fn test_producer_send() {
  let mut producer = DefaultMQProducer::builder()
    .producer_group("example_group")
    .name_server_addr("localhost:9876")
    .build();
    producer.start().await.unwrap();

  let message = Message::builder()
    .topic("TestTopic")
    .body("Test")
    .build()
    .unwrap();
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
    let msg = Message::builder()
      .topic(topic.clone())
      .body(vec![1, 2, 3])
      .build()
      .unwrap();
    assert_eq!(msg.topic().as_str(), topic);
    }
}
```

## Performance Testing

### Benchmarking

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_send_message(c: &mut Criterion) {
  let mut producer = DefaultMQProducer::builder()
    .producer_group("bench_group")
    .name_server_addr("localhost:9876")
    .build();
    let rt = tokio::runtime::Runtime::new().unwrap();
  rt.block_on(producer.start()).unwrap();

    c.bench_function("send_message", |b| {
        b.iter(|| {
        let message = Message::builder()
          .topic("Test")
          .body(vec![0; 1024])
          .build()
          .unwrap();
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
/// use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
/// use rocketmq_common::common::message::message_single::Message;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut producer = DefaultMQProducer::builder()
///     .producer_group("example_group")
///     .name_server_addr("localhost:9876")
///     .build();
/// let message = Message::builder()
///     .topic("TestTopic")
///     .body("Hello")
///     .build()?;
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
          toolchain: nightly
          override: true
      - uses: actions-rs/cargo@v1
        with:
          command: test
          args: --all
```

## Contributing Workflow

### Reporting Issues

- Before submitting an issue, please go through a comprehensive search to make sure the problem cannot be solved just by searching.
- Check the [Issue List](https://github.com/mxsm/rocketmq-rust/issues) to make sure the problem is not repeated.
- Create a new issue and choose the type of issue.
- Define the issue with a clear and descriptive title.
- Fill in necessary information according to the template.
- Please pay attention to your issue, you may need to provide more information during discussion.

### How to Contribute

#### 1. Prepare Repository

Go to [RocketMQ Rust GitHub Repo](https://github.com/mxsm/rocketmq-rust) and fork the repository to your account.

Clone the repository to your local machine:

```bash
git clone https://github.com/(your-username)/rocketmq-rust.git
cd rocketmq-rust
```

Add the upstream **`rocketmq-rust`** remote repository:

```bash
git remote add mxsm https://github.com/mxsm/rocketmq-rust.git
git remote -v
git fetch mxsm
```

#### 2. Choose Issue

Please choose the issue to be worked on. If it is a new issue discovered or a new feature enhancement to offer, please create an issue and set the appropriate label for it.

#### 3. Create Branch

```bash
git checkout main
git fetch mxsm
git rebase mxsm/main
git checkout -b feature-issueNo
```

**Note:** We will merge PR using squash, commit log will be different with upstream if you use old branch.

#### 4. Development Workflow

After the development is completed, it is necessary to perform code formatting, compilation, and format checking.

**Format the code in the project:**

```bash
cargo fmt --all
```

**Build:**

```bash
cargo build
```

**Run Clippy:**

```bash
cargo clippy --all-targets --all-features --workspace
```

**Run all tests:**

```bash
cargo test --all-features --workspace
```

**Push code to your fork repo:**

```bash
git add modified-file-names
git commit -m 'commit log'
git push origin feature-issueNo
```

#### 5. Submit Pull Request

- Send a pull request to the main branch
- Maintainers will do code review and discuss details (including design, implementation, and performance) with you
- The request will be merged into the current development branch after the review is complete
- Congratulations on becoming a contributor to rocketmq-rust!

**Note:** 🚨 The code review suggestions from CodeRabbit are to be used as a reference only. The PR submitter can decide whether to make changes based on their own judgment. Ultimately, the project maintainers will conduct the final code review.

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
