---
sidebar_position: 2
title: 开发指南
---

# 开发指南

RocketMQ-Rust 详细开发指南。

## 开发环境

### 前置要求

- **Rust**：nightly 工具链
- **Git**：用于版本控制
- **IDE**：VS Code、RustRover 或类似工具

### IDE 设置

**VS Code**：

安装扩展：

- rust-analyzer
- CodeLLDB（调试器）
- Even Better TOML
- Error Lens

**RustRover**：

RustRover 内置 Rust 支持。无需额外插件。

### 安装 Rust Nightly

```bash
# 如果尚未安装 rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 安装 nightly 工具链
rustup toolchain install nightly

# 设置 nightly 为默认（可选）
rustup default nightly

# 或仅为该项目使用 nightly
rustup override set nightly
```

### 从源代码构建

```bash
# 克隆仓库
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust

# Debug 模式构建
cargo build

# Release 模式构建
cargo build --release

# 运行测试
cargo test --all
```

## 项目结构

RocketMQ-Rust 是基于 workspace 的项目，包含多个 crate。以下是高层结构：

```tree
rocketmq-rust/
├── rocketmq/              # 核心库（工具、调度、并发）
├── rocketmq-auth/         # 认证和授权
├── rocketmq-broker/       # Broker 实现
├── rocketmq-cli/          # 命令行工具
├── rocketmq-client/       # 客户端库（生产者和消费者）
│   ├── src/
│   │   ├── admin/         # 管理工具
│   │   ├── base/          # 基础客户端功能
│   │   ├── common/        # 通用工具
│   │   ├── consumer/      # 消费者实现
│   │   ├── producer/      # 生产者实现
│   │   ├── factory/       # 客户端工厂
│   │   ├── implementation/ # 实现细节
│   │   ├── latency/       # 延迟跟踪
│   │   ├── hook/          # 钩子和拦截器
│   │   ├── trace/         # 消息跟踪
│   │   └── utils/         # 工具函数
├── rocketmq-common/       # 通用数据结构和工具
├── rocketmq-controller/   # 控制器组件
├── rocketmq-doc/          # 文档资源
├── rocketmq-error/        # 错误类型和处理
├── rocketmq-example/      # 示例代码
├── rocketmq-filter/       # 消息过滤
├── rocketmq-macros/       # 过程宏
├── rocketmq-namesrv/      # 名称服务器实现
├── rocketmq-proxy/        # 代理服务器
├── rocketmq-remoting/     # 远程通信层
├── rocketmq-runtime/      # 运行时工具
├── rocketmq-store/        # 消息存储
├── rocketmq-tools/        # 开发工具
├── rocketmq-tui/          # 终端用户界面
├── rocketmq-website/      # 文档网站
├── Cargo.toml             # Workspace 配置
├── Cargo.lock             # 锁文件
├── CHANGELOG.md           # 变更日志
├── CONTRIBUTING.md        # 贡献指南
├── README.md              # 项目 README
└── resources/             # 额外资源
```

## 运行测试

### 运行集成测试

```bash
# 运行集成测试
cargo test --test '*'

# 运行特定测试
cargo test --test integration test_send_message
```

## 调试

### 使用 VS Code 调试器

创建 `.vscode/launch.json`：

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

### 日志记录

启用调试日志：

```bash
# 设置日志级别
RUST_LOG=debug cargo run

# 或在代码中
use log::debug;
env_logger::init();
```

## 代码组织

### 模块结构

```rust
// src/producer/mod.rs
pub mod producer;
pub mod producer_impl;
pub mod transaction_producer;

pub use producer::Producer;
pub use transaction_producer::TransactionMQProducer;
```

### 错误处理

```rust
// 定义错误类型
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Broker not available: {0}")]
    BrokerNotFound(String),

    #[error("Timeout: {0}ms")]
    Timeout(u64),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// Result 类型别名
pub type Result<T> = std::result::Result<T, Error>;
```

## 测试策略

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
    assert_eq!(msg.body().unwrap().to_vec(), vec![1, 2, 3]);
    }
}
```

### 集成测试

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

### 基于属性的测试

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

## 性能测试

### 基准测试

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

## 文档

### 代码文档

```rust
/// 向 broker 发送消息。
///
/// 此方法向 RocketMQ broker 发送消息，并返回包含消息 ID 和队列信息的发送结果。
///
/// # 参数
///
/// * `message` - 要发送的消息
///
/// # 返回
///
/// 返回包含发送结果或错误的 `Result<SendResult>`。
///
/// # 示例
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
/// # 错误
///
/// 此函数将在以下情况返回错误：
/// - broker 不可用
/// - 消息大小超过最大允许值
/// - 发生网络超时
pub async fn send(&self, message: Message) -> Result<SendResult> {
    // 实现
}
```

## 持续集成

### GitHub Actions

`.github/workflows/ci.yml`：

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

## 贡献工作流

### 报告问题

- 在提交问题之前，请进行全面搜索，确保问题不能仅通过搜索解决。
- 查看[问题列表](https://github.com/mxsm/rocketmq-rust/issues)以确保问题不重复。
- 创建新问题并选择问题类型。
- 使用清晰且描述性的标题定义问题。
- 根据模板填写必要信息。
- 请关注你的问题，在讨论期间可能需要提供更多信息。

### 如何贡献

#### 1. 准备仓库

前往 [RocketMQ Rust GitHub 仓库](https://github.com/mxsm/rocketmq-rust) 并将仓库 fork 到你的账户。

将仓库克隆到本地机器：

```bash
git clone https://github.com/(your-username)/rocketmq-rust.git
cd rocketmq-rust
```

添加 upstream **`rocketmq-rust`** 远程仓库：

```bash
git remote add mxsm https://github.com/mxsm/rocketmq-rust.git
git remote -v
git fetch mxsm
```

#### 2. 选择问题

请选择要处理的问题。如果是发现的新问题或提供的新功能增强，请创建问题并为其设置适当的标签。

#### 3. 创建分支

```bash
git checkout main
git fetch mxsm
git rebase mxsm/main
git checkout -b feature-issueNo
```

**注意：** 我们将使用 squash 合并 PR，如果使用旧分支，提交日志将与 upstream 不同。

#### 4. 开发工作流

开发完成后，需要进行代码格式化、编译和格式检查。

**格式化项目中的代码：**

```bash
cargo fmt --all
```

**构建：**

```bash
cargo build
```

**运行 Clippy：**

```bash
cargo clippy --all-targets --all-features --workspace
```

**运行所有测试：**

```bash
cargo test --all-features --workspace
```

**推送代码到你的 fork 仓库：**

```bash
git add modified-file-names
git commit -m 'commit log'
git push origin feature-issueNo
```

#### 5. 提交拉取请求

- 向 main 分支发送拉取请求
- 维护者将进行代码审查并与你讨论细节（包括设计、实现和性能）
- 审查完成后，请求将合并到当前开发分支
- 恭喜你成为 rocketmq-rust 的贡献者！

**注意：** 🚨 CodeRabbit 的代码审查建议仅供参考。PR 提交者可以根据自己的判断决定是否进行更改。最终，项目维护者将进行最终的代码审查。

## 最佳实践

1. **测试先行**：TDD 方法
2. **保持函数简短**：单一职责
3. **使用有意义的名称**：自文档化代码
4. **记录公共 API**：全面的文档
5. **正确处理错误**：使用 Result 类型
6. **避免 unwrap**：使用适当的错误处理
7. **使用 clippy**：捕获常见错误
8. **格式化代码**：使用 rustfmt

## 后续步骤

- [编码标准](./coding-standards) - 代码风格指南
- [概述](./overview) - 贡献概述
- [报告问题](https://github.com/mxsm/rocketmq-rust/issues) - 提交问题
