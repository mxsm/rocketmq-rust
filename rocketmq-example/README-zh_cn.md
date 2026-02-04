# RocketMQ-Rust 示例

展示如何使用 RocketMQ-Rust 客户端 API 和功能的全面示例集合。

## 📋 概述

这是一个**独立项目**，包含 RocketMQ-Rust 的实用示例。展示了各种使用模式，包括：

- **消费者示例**：Push 消费者、Pop 消费者、消息监听器
- **生产者示例**：*（即将推出）*
- **管理操作**：*（即将推出）*

## 🚀 快速开始

### 前置要求

- Rust 1.85.0 或更高版本
- 运行中的 RocketMQ nameserver 和 broker（默认：`127.0.0.1:9876`）

### 运行示例

```bash
# 进入示例目录
cd rocketmq-example

# 运行 pop 消费者示例
cargo run --example pop-consumer

# 列出所有可用示例
cargo run --example <TAB>
```

## 📚 可用示例

### 消费者示例

#### Pop 消费者
演示如何使用 pop 消费模式并禁用客户端侧负载均衡。

**文件**：[examples/consumer/pop_consumer.rs](examples/consumer/pop_consumer.rs)

**运行**：
```bash
cargo run --example pop-consumer
```

**功能特性**：
- 基于 Pop 的消息消费
- 并发消息处理
- 自动创建主题/消费者组
- 消息请求模式配置

## 🔧 配置

大多数示例使用默认配置：

```rust
const NAMESRV_ADDR: &str = "127.0.0.1:9876";
const TOPIC: &str = "TopicTest";
const CONSUMER_GROUP: &str = "please_rename_unique_group_name_4";
```

修改示例源文件中的这些常量以匹配您的 RocketMQ 集群设置。

## 📖 开发指南

### 添加新示例

1. 在适当的类别文件夹中创建新文件（例如：`examples/consumer/my_example.rs`）
2. 将示例配置添加到 `Cargo.toml`：

```toml
[[example]]
name = "my-example"
path = "examples/consumer/my_example.rs"
```

3. 运行示例：
```bash
cargo run --example my-example
```

### 依赖项

该项目引用本地 RocketMQ-Rust crate：

- `rocketmq-client-rust` - 客户端 API
- `rocketmq-common` - 通用工具
- `rocketmq-rust` - 核心运行时
- `rocketmq-tools` - 管理工具

## 🏗️ 项目结构

```
rocketmq-example/
├── examples/
│   └── consumer/
│       └── pop_consumer.rs
├── Cargo.toml
└── README.md
```

## 📝 注意事项

- 这是一个**独立项目**，不属于主 RocketMQ-Rust workspace
- 示例使用相对路径依赖来引用父 crate
- 每个示例都是独立的，可以单独运行

## 🤝 贡献

欢迎贡献新示例！请确保：

1. 示例有良好的内联注释文档
2. 配置明确指定
3. 示例遵循 Rust 最佳实践
4. 每个示例演示单一、清晰的用例

## 📄 许可证

该项目继承 RocketMQ-Rust 的双重许可证：
- Apache License 2.0
- MIT License
