---
sidebar_position: 3
title: Coding Standards
---

# Coding Standards

Follow these standards when contributing to RocketMQ-Rust.

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

```rust
// Use Result for fallible operations
use crate::error::Result;

pub async fn send_message(&self, msg: Message) -> Result<SendResult> {
    // Use ? for error propagation
    let broker = self.find_broker(&msg.topic)?;

    // Avoid unwrap() in library code
    match broker.send(msg).await {
        Ok(result) => Ok(result),
        Err(e) => Err(Error::SendFailed(e.to_string())),
    }
}

// Custom error types
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Broker not found: {0}")]
    BrokerNotFound(String),

    #[error("Timeout after {0}ms")]
    Timeout(u64),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

### Async/Await

```rust
// Use async/await for async operations
pub async fn send(&self, msg: Message) -> Result<SendResult> {
    let broker = self.get_broker().await?;
    broker.send(msg).await
}

// Spawn tasks for concurrent operations
pub async fn send_batch(&self, msgs: Vec<Message>) -> Result<Vec<SendResult>> {
    let tasks: Vec<_> = msgs
        .into_iter()
        .map(|msg| tokio::spawn(self.send(msg)))
        .collect();

    let results = futures::future::try_join_all(tasks).await?;
    Ok(results.into_iter().map(|r| r.unwrap()).collect())
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

### Formatting

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt --check
```

### Linting

```bash
# Run clippy
cargo clippy -- -D warnings

# Fix clippy suggestions
cargo clippy --fix
```

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

### Memory Management

```rust
// Use references to avoid copies
pub fn process_message(msg: &Message) -> Result<()> {
    let body = msg.get_body();
    // Process body without cloning
}

// Use Cow for conditional ownership
use std::borrow::Cow;

pub fn get_topic<'a>(msg: &'a Message, default: &'a str) -> Cow<'a, str> {
    match msg.get_topic() {
        "" => Cow::Borrowed(default),
        topic => Cow::Borrowed(topic),
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

## Common Mistakes to Avoid

1. **Don't use `unwrap()`** in library code
2. **Don't use `expect()`** unless absolutely necessary
3. **Don't ignore errors**: Always handle `Result` properly
4. **Don't use blocking operations** in async code
5. **Don't leak memory**: Be careful with reference cycles
6. **Don't overuse `unsafe`**: Only when necessary
7. **Don't make unnecessary clones**: Use references

## Resources

- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [Rust Style Guide](https://rust-lang.github.io/rust-clippy/master/index.html)
- [Effective Rust](https://doc.rust-lang.org/book/title-page.html)

## Next Steps

- [Development Guide](./development-guide) - Development information
- [Overview](./overview) - Contributing overview
- [Report Issues](https://github.com/mxsm/rocketmq-rust/issues) - File issues
