---
sidebar_position: 3
title: Coding Standards
---

# Coding Standards

Welcome to RocketMQ-Rust's coding standards! 📝

This guide will help you write clean, idiomatic Rust code that follows our project conventions. These standards ensure code consistency, maintainability, and quality across the entire codebase.

## Why Coding Standards Matter

Consistent code is:
- **Easier to read** and understand
- **Easier to maintain** and debug
- **Easier to review** in pull requests
- **More reliable** with fewer bugs

## Rust Conventions

### Naming

```rust
// Modules: snake_case
mod message_queue;

// Types: PascalCase
struct MessageQueue;
enum ConsumeResult;

// Functions: snake_case
fn send_message() {}

// Constants: SCREAMING_SNAKE_CASE
const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

// Static: SCREAMING_SNAKE_CASE
static DEFAULT_TIMEOUT: u64 = 3000;
```

### Code Organization

```rust
// Imports (std, external crates, internal modules)
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::model::Message;
use crate::error::{Error, Result};

// Type aliases
type MessageQueueRef = Arc<MessageQueue>;

// Constants
const MAX_RETRY: u32 = 3;

// Structs
pub struct Producer {
    // Private fields
    client: Arc<Client>,
    options: ProducerOptions,
}

// Impl blocks
impl Producer {
    // Associated functions ( constructors)
    pub fn new() -> Self { }

    // Methods
    pub async fn send(&self, msg: Message) -> Result<SendResult> { }

    // Private methods
    async fn do_send(&self, msg: Message) -> Result<SendResult> { }
}

// Trait impls
impl Default for Producer {
    fn default() -> Self { }
}
```

### Error Handling

RocketMQ-Rust uses the `thiserror` crate for error definitions. Always use `Result` types for operations that can fail.

```rust
// Use Result for fallible operations
use crate::error::Result;

pub async fn send_message(&self, msg: Message) -> Result<SendResult> {
    // Use ? for error propagation - clean and idiomatic
    let broker = self.find_broker(&msg.topic)?;

    // ⚠️ Avoid unwrap() in library code - it can panic!
    // ✅ Instead, handle errors explicitly
    match broker.send(msg).await {
        Ok(result) => Ok(result),
        Err(e) => Err(Error::SendFailed(e.to_string())),
    }
}

// Custom error types using thiserror
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Broker not found: {0}")]
    BrokerNotFound(String),

    #[error("Timeout after {0}ms")]
    Timeout(u64),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// ✅ Good: Explicit error handling
pub fn parse_config(data: &str) -> Result<Config> {
    serde_json::from_str(data)
        .map_err(|e| Error::ConfigParse(e.to_string()))
}

// ❌ Bad: Using unwrap() - can panic!
pub fn parse_config_bad(data: &str) -> Config {
    serde_json::from_str(data).unwrap() // Don't do this!
}
```

### Async/Await

RocketMQ-Rust uses `tokio` as the async runtime. All I/O operations should be async.

```rust
// ✅ Use async/await for async operations
pub async fn send(&self, msg: Message) -> Result<SendResult> {
    let broker = self.get_broker().await?;
    broker.send(msg).await
}

// ✅ Spawn tasks for concurrent operations
pub async fn send_batch(&self, msgs: Vec<Message>) -> Result<Vec<SendResult>> {
    let tasks: Vec<_> = msgs
        .into_iter()
        .map(|msg| {
            let self_clone = self.clone();
            tokio::spawn(async move { self_clone.send(msg).await })
        })
        .collect();

    let results = futures::future::try_join_all(tasks).await?;
    results.into_iter().collect::<Result<Vec<_>>>()
}

// ❌ Bad: Blocking operations in async context
pub async fn send_bad(&self, msg: Message) -> Result<SendResult> {
    // Don't do this - blocks the async runtime!
    std::thread::sleep(std::time::Duration::from_secs(1));
    self.send_impl(msg).await
}

// ✅ Good: Use tokio's async sleep
pub async fn send_good(&self, msg: Message) -> Result<SendResult> {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    self.send_impl(msg).await
}
```

## Documentation

### Public APIs

```rust
/// A producer for sending messages to RocketMQ brokers.
///
/// The producer handles message routing, load balancing, and
/// automatic retry on failure.
///
/// # Examples
///
/// ```rust
/// use rocketmq::producer::Producer;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let producer = Producer::new();
/// producer.start().await?;
/// # Ok(())
/// # }
/// ```
///
/// # See Also
///
/// - [`Consumer`] for consuming messages
/// - [`Message`] for message structure
pub struct Producer { }
```

### Module Documentation

```rust
//! Producer module.
//!
//! This module provides the [`Producer`] type for sending messages
//! to RocketMQ brokers.
//!
//! # Features
//!
//! - Asynchronous message sending
//! - Automatic retry on failure
//! - Load balancing across brokers
//! - Transactional message support
//!
//! # Examples
//!
//! ```rust
//! use rocketmq::producer::Producer;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let producer = Producer::new();
//! let message = Message::new("TopicTest".to_string(), b"Hello".to_vec());
//! producer.send(message).await?;
//! # Ok(())
//! # }
//! ```
```

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::new("Test".to_string(), vec![1, 2, 3]);
        assert_eq!(msg.get_topic(), "Test");
    }

    #[tokio::test]
    async fn test_async_operation() {
        let result = async_operation().await;
        assert!(result.is_ok());
    }
}
```

### Integration Tests

```rust
// tests/integration_test.rs
#[tokio::test]
async fn test_producer_consumer() {
    let producer = Producer::new();
    producer.start().await.unwrap();

    let consumer = PushConsumer::new();
    consumer.subscribe("TestTopic", "*").await.unwrap();

    // Test logic
}
```

## Code Style

### Automated Formatting

We use `rustfmt` to ensure consistent code formatting across the project. **Always format your code before committing!**

```bash
# Format all code in the workspace
cargo fmt --all

# Check if code is formatted (used in CI)
cargo fmt --all --check
```

**Pro tip**: Configure your IDE to format on save:
- **VS Code**: Set `"editor.formatOnSave": true` with rust-analyzer
- **RustRover**: Enable "Reformat code" in Settings → Tools → Actions on Save

### Linting with Clippy

We use `clippy` to catch common mistakes and non-idiomatic code. All clippy warnings must be fixed before merging.

```bash
# Run clippy on all targets and features
cargo clippy --all-targets --all-features --workspace -- -D warnings

# Auto-fix clippy suggestions (when possible)
cargo clippy --fix --all-targets --all-features --workspace
```

**Note**: Some clippy suggestions are auto-fixable, but always review the changes before committing.

### Common Patterns

**Builder Pattern**:
```rust
pub struct ProducerOptions {
    name_server_addr: String,
    group_name: String,
    timeout: u64,
}

impl ProducerOptions {
    pub fn new() -> Self {
        Self {
            name_server_addr: "localhost:9876".to_string(),
            group_name: "DEFAULT_PRODUCER".to_string(),
            timeout: 3000,
        }
    }

    pub fn name_server_addr(mut self, addr: impl Into<String>) -> Self {
        self.name_server_addr = addr.into();
        self
    }

    pub fn timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }
}
```

**Newtype Pattern**:
```rust
/// Wrapper for message IDs with validation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId(String);

impl MessageId {
    pub fn new(id: String) -> Result<Self> {
        if id.is_empty() {
            return Err(Error::InvalidMessageId);
        }
        Ok(Self(id))
    }
}

impl AsRef<str> for MessageId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
```

## Performance Guidelines

RocketMQ-Rust is designed for high performance. Follow these guidelines to maintain optimal performance.

### Memory Management

**Prefer borrowing over cloning** - avoid unnecessary allocations:

```rust
// ✅ Good: Use references to avoid copies
pub fn process_message(msg: &Message) -> Result<()> {
    let body = msg.get_body();  // Borrows, no copy
    // Process body without cloning
    Ok(())
}

// ❌ Bad: Unnecessary cloning
pub fn process_message_bad(msg: Message) -> Result<()> {
    let body = msg.get_body().clone();  // Extra allocation!
    Ok(())
}

// ✅ Use Cow for conditional ownership
use std::borrow::Cow;

pub fn get_topic<'a>(msg: &'a Message, default: &'a str) -> Cow<'a, str> {
    match msg.get_topic() {
        "" => Cow::Borrowed(default),  // No allocation
        topic => Cow::Borrowed(topic), // No allocation
    }
}
```

### Concurrency

```rust
// Use Arc for shared ownership
use std::sync::Arc;

let client = Arc::new(Client::new());

// Use Mutex/RwLock for interior mutability
use tokio::sync::Mutex;

let state = Arc::new(Mutex::new(State::new()));

// Use channels for communication
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(1000);
```

## Common Mistakes to Avoid ⚠️

Learn from these common pitfalls:

### 1. Using `unwrap()` or `panic!()` in Library Code

❌ **Bad**:
```rust
pub fn get_broker(&self) -> Broker {
    self.brokers.get(0).unwrap()  // Can panic!
}
```

✅ **Good**:
```rust
pub fn get_broker(&self) -> Result<&Broker> {
    self.brokers.get(0).ok_or(Error::NoBrokerAvailable)
}
```

### 2. Ignoring Errors

❌ **Bad**:
```rust
let _ = self.send(msg).await;  // Error silently ignored!
```

✅ **Good**:
```rust
if let Err(e) = self.send(msg).await {
    log::error!("Failed to send message: {}", e);
    return Err(e);
}
```

### 3. Blocking in Async Code

❌ **Bad**:
```rust
pub async fn send(&self) -> Result<()> {
    std::thread::sleep(Duration::from_secs(1));  // Blocks executor!
}
```

✅ **Good**:
```rust
pub async fn send(&self) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

### 4. Unnecessary Clones

❌ **Bad**:
```rust
pub fn process(&self, data: String) -> Result<()> {
    let copy = data.clone();  // Unnecessary!
    self.process_impl(&copy)
}
```

✅ **Good**:
```rust
pub fn process(&self, data: &str) -> Result<()> {
    self.process_impl(data)
}
```

### 5. Memory Leaks with Reference Cycles

Be careful with `Rc`/`Arc` cycles. Use `Weak` references when needed.

### 6. Overusing `unsafe`

Only use `unsafe` when absolutely necessary and always document why it's safe.

### 7. Not Handling All Enum Variants

Avoid using `_` in match arms - be explicit to catch future enum additions.

## Learning Resources 📚

Want to write better Rust code? Check out these resources:

### Official Rust Resources
- [The Rust Book](https://doc.rust-lang.org/book/) - Comprehensive Rust guide
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) - API design best practices
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/) - Learn by examples
- [Clippy Lint List](https://rust-lang.github.io/rust-clippy/master/index.html) - All clippy lints explained

### Advanced Topics
- [Async Book](https://rust-lang.github.io/async-book/) - Deep dive into async Rust
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial) - Async runtime guide
- [The Rustonomicon](https://doc.rust-lang.org/nomicon/) - Unsafe Rust (advanced)

### RocketMQ-Rust Specific
- [Architecture Overview](/docs/architecture/overview) - Understand the codebase structure
- [Development Guide](./development-guide) - Set up your dev environment
- [Contributing Overview](./overview) - Start contributing today!

## Summary

Remember:
- ✅ Write idiomatic Rust code
- ✅ Handle errors properly with `Result`
- ✅ Use async/await for I/O operations
- ✅ Format code with `cargo fmt`
- ✅ Fix clippy warnings before committing
- ✅ Write tests for your code
- ✅ Document public APIs

Happy coding! 🚀

## Next Steps

- [Development Guide](./development-guide) - Set up your environment
- [Overview](./overview) - Start contributing
- [Report Issues](https://github.com/mxsm/rocketmq-rust/issues) - Found a bug?
