---
sidebar_position: 3
title: 编码标准
---

# 编码标准

欢迎来到 RocketMQ-Rust 的编码标准！📝

本指南将帮助你编写整洁、地道的 Rust 代码，遵循项目约定。这些标准确保整个代码库的一致性、可维护性和质量。

## 为什么编码标准很重要

一致的代码：

- **更易于阅读**和理解
- **更易于维护**和调试
- **更易于审查**拉取请求
- **更可靠**，bug 更少

## Rust 约定

### 命名

```rust
// 模块：snake_case
mod message_queue;

// 类型：PascalCase
struct MessageQueue;
enum ConsumeResult;

// 函数：snake_case
fn send_message() {}

// 常量：SCREAMING_SNAKE_CASE
const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

// 静态变量：SCREAMING_SNAKE_CASE
static DEFAULT_TIMEOUT: u64 = 3000;
```

### 代码组织

```rust
// 导入（std、外部 crate、内部模块）
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::model::Message;
use crate::error::{Error, Result};

// 类型别名
type MessageQueueRef = Arc<MessageQueue>;

// 常量
const MAX_RETRY: u32 = 3;

// 结构体
pub struct Producer {
    // 私有字段
    client: Arc<Client>,
    options: ProducerOptions,
}

// Impl 块
impl Producer {
    // 关联函数（构造函数）
    pub fn new() -> Self { }

    // 方法
    pub async fn send(&self, msg: Message) -> Result<SendResult> { }

    // 私有方法
    async fn do_send(&self, msg: Message) -> Result<SendResult> { }
}

// Trait 实现
impl Default for Producer {
    fn default() -> Self { }
}
```

### 错误处理

RocketMQ-Rust 使用 `thiserror` crate 定义错误。对于可能失败的操作，始终使用 `Result` 类型。

```rust
// 对可能失败的操作使用 Result
use crate::error::Result;

pub async fn send_message(&self, msg: Message) -> Result<SendResult> {
    // 使用 ? 进行错误传播 - 简洁且地道
    let broker = resolve_broker(&msg.topic)?;

    // ⚠️ 在库代码中避免使用 unwrap() - 它可能导致 panic！
    // ✅ 相反，应显式处理错误
    match broker.send(msg).await {
        Ok(result) => Ok(result),
        Err(e) => Err(Error::SendFailed(e.to_string())),
    }
}

// 使用 thiserror 定义自定义错误类型
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Broker not found: {0}")]
    BrokerNotFound(String),

    #[error("Timeout after {0}ms")]
    Timeout(u64),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// ✅ 好的做法：显式错误处理
pub fn parse_config(data: &str) -> Result<Config> {
    serde_json::from_str(data)
        .map_err(|e| Error::ConfigParse(e.to_string()))
}

// ❌ 坏的做法：使用 unwrap() - 可能 panic！
pub fn parse_config_bad(data: &str) -> Config {
    serde_json::from_str(data).unwrap() // 不要这样做！
}
```

### Async/Await

RocketMQ-Rust 使用 `tokio` 作为异步运行时。所有 I/O 操作都应该是异步的。

```rust
// ✅ 对异步操作使用 async/await
pub async fn send(&self, msg: Message) -> Result<SendResult> {
    let broker = resolve_broker_async().await?;
    broker.send(msg).await
}

// ✅ 为并发操作生成任务
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

// ❌ 坏的做法：异步上下文中的阻塞操作
pub async fn send_bad(&self, msg: Message) -> Result<SendResult> {
    // 不要这样做 - 会阻塞异步运行时！
    std::thread::sleep(std::time::Duration::from_secs(1));
    self.send(msg).await
}

// ✅ 好的做法：使用 tokio 的异步 sleep
pub async fn send_good(&self, msg: Message) -> Result<SendResult> {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    self.send(msg).await
}
```

## 文档

### 公共 API

```rust
/// 向 RocketMQ broker 发送消息的生产者。
///
/// 生产者处理消息路由、负载均衡和失败时的自动重试。
///
/// # 示例
///
/// ```rust
/// use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut producer = DefaultMQProducer::builder()
///     .producer_group("example_group")
///     .name_server_addr("localhost:9876")
///     .build();
/// producer.start().await?;
/// # Ok(())
/// # }
/// ```
///
/// # 另请参阅
///
/// - [`Consumer`] 用于消费消息
/// - [`Message`] 用于消息结构
pub struct Producer { }
```

### 模块文档

```rust
//! 生产者模块。
//!
//! 此模块提供 [`Producer`] 类型用于向 RocketMQ broker 发送消息。
//!
//! # 特性
//!
//! - 异步消息发送
//! - 失败时自动重试
//! - 跨 broker 的负载均衡
//! - 事务消息支持
//!
//! # 示例
//!
//! ```rust
//! use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut producer = DefaultMQProducer::builder()
//!     .producer_group("example_group")
//!     .name_server_addr("localhost:9876")
//!     .build();
//! let message = Message::builder()
//!     .topic("TopicTest")
//!     .body("Hello")
//!     .build()?;
//! producer.send(message).await?;
//! # Ok(())
//! # }
//! //! ```
```

## 测试

### 单元测试

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
    }

    #[tokio::test]
    async fn test_async_operation() {
        let result = async_operation().await;
        assert!(result.is_ok());
    }
}
```

### 集成测试

```rust
// tests/integration_test.rs
#[tokio::test]
async fn test_producer_consumer() {
    let mut producer = DefaultMQProducer::builder()
        .producer_group("example_group")
        .name_server_addr("localhost:9876")
        .build();
    producer.start().await.unwrap();

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("example_group")
        .name_server_addr("localhost:9876")
        .build();
    consumer.subscribe("TestTopic", "*").await.unwrap();

    // 测试逻辑
}
```

## 代码风格

### 自动格式化

我们使用 `rustfmt` 确保整个项目的代码格式一致。**提交前务必格式化代码！**

```bash
# 格式化 workspace 中的所有代码
cargo fmt --all

# 检查代码是否已格式化（在 CI 中使用）
cargo fmt --all --check
```

**专业提示**：配置 IDE 在保存时自动格式化：

- **VS Code**：使用 rust-analyzer 设置 `"editor.formatOnSave": true`
- **RustRover**：在设置 → 工具 → 保存时操作中启用"重新格式化代码"

### 使用 Clippy 进行 Linting

我们使用 `clippy` 来捕获常见错误和非地道代码。所有 clippy 警告必须在合并前修复。

```bash
# 对所有目标和功能运行 clippy
cargo clippy --all-targets --all-features --workspace -- -D warnings

# 自动修复 clippy 建议（如果可能）
cargo clippy --fix --all-targets --all-features --workspace
```

**注意**：一些 clippy 建议可以自动修复，但在提交前请始终审查更改。

### 常见模式

**构建器模式**：

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

**Newtype 模式**：

```rust
/// 带验证的消息 ID 包装器
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

## 性能指南

RocketMQ-Rust 专为高性能设计。遵循这些指南以保持最佳性能。

### 内存管理

**优先使用借用而不是克隆** - 避免不必要的分配：

```rust
// ✅ 好的做法：使用引用避免复制
pub fn process_message(msg: &Message) -> Result<()> {
    let body = msg.get_body();  // 借用，不复制
    // 在不克隆的情况下处理 body
    Ok(())
}

// ❌ 坏的做法：不必要的克隆
pub fn process_message_bad(msg: Message) -> Result<()> {
    let body = msg.get_body().clone();  // 额外分配！
    Ok(())
}

// ✅ 使用 Cow 进行条件所有权
use std::borrow::Cow;

pub fn get_topic<'a>(msg: &'a Message, default: &'a str) -> Cow<'a, str> {
    match msg.get_topic() {
        "" => Cow::Borrowed(default),  // 无分配
        topic => Cow::Borrowed(topic), // 无分配
    }
}
```

### 并发

```rust
// 使用 Arc 进行共享所有权
use std::sync::Arc;

let client = Arc::new(Client::new());

// 使用 Mutex/RwLock 进行内部可变性
use tokio::sync::Mutex;

let state = Arc::new(Mutex::new(State::new()));

// 使用 channel 进行通信
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(1000);
```

## 需要避免的常见错误 ⚠️

从这些常见陷阱中学习：

### 1. 在库代码中使用 `unwrap()` 或 `panic!()`

❌ **坏的做法**：

```rust
pub fn get_broker(&self) -> Broker {
    self.brokers.get(0).unwrap()  // 可能 panic！
}
```

✅ **好的做法**：

```rust
pub fn get_broker(&self) -> Result<&Broker> {
    self.brokers.get(0).ok_or(Error::NoBrokerAvailable)
}
```

### 2. 忽略错误

❌ **坏的做法**：

```rust
let _ = self.send(msg).await;  // 错误被静默忽略！
```

✅ **好的做法**：

```rust
if let Err(e) = self.send(msg).await {
    log::error!("Failed to send message: {}", e);
    return Err(e);
}
```

### 3. 在异步代码中阻塞

❌ **坏的做法**：

```rust
pub async fn send(&self) -> Result<()> {
    std::thread::sleep(Duration::from_secs(1));  // 阻塞执行器！
}
```

✅ **好的做法**：

```rust
pub async fn send(&self) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

### 4. 不必要的克隆

❌ **坏的做法**：

```rust
pub fn process(&self, data: String) -> Result<()> {
    let copy = data.clone();  // 不必要！
    process_data(&copy)
}
```

✅ **好的做法**：

```rust
pub fn process(&self, data: &str) -> Result<()> {
    process_data(data)
}
```

### 5. 引用循环导致的内存泄漏

使用 `Rc`/`Arc` 循环时要小心。需要时使用 `Weak` 引用。

### 6. 过度使用 `unsafe`

只在绝对必要时使用 `unsafe`，并始终记录为什么它是安全的。

### 7. 未处理所有枚举变体

避免在 match 分支中使用 `_` - 要明确以捕获未来的枚举添加。

## 学习资源 📚

想编写更好的 Rust 代码？查看这些资源：

### 官方 Rust 资源

- [The Rust Book](https://doc.rust-lang.org/book/) - 全面的 Rust 指南
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) - API 设计最佳实践
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/) - 通过示例学习
- [Clippy Lint List](https://rust-lang.github.io/rust-clippy/master/index.html) - 所有 clippy lints 解释

### 高级主题

- [Async Book](https://rust-lang.github.io/async-book/) - 深入异步 Rust
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial) - 异步运行时指南
- [The Rustonomicon](https://doc.rust-lang.org/nomicon/) - Unsafe Rust（高级）

### RocketMQ-Rust 专用

- [架构概述](/docs/zh-CN/architecture/overview) - 了解代码库结构
- [开发指南](./development-guide) - 设置开发环境
- [贡献概述](./overview) - 立即开始贡献！

## 总结

请记住：

- ✅ 编写地道的 Rust 代码
- ✅ 使用 `Result` 正确处理错误
- ✅ 对 I/O 操作使用 async/await
- ✅ 使用 `cargo fmt` 格式化代码
- ✅ 提交前修复 clippy 警告
- ✅ 为代码编写测试
- ✅ 记录公共 API

祝你编码愉快！🚀

## 后续步骤

- [开发指南](./development-guide) - 设置环境
- [概述](./overview) - 开始贡献
- [报告问题](https://github.com/mxsm/rocketmq-rust/issues) - 发现 bug？
