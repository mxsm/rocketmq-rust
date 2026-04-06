---
sidebar_position: 1
title: 概述
---

# 贡献指南概述

欢迎来到 RocketMQ-Rust 社区！🎉

感谢你对 RocketMQ-Rust 项目的贡献兴趣。无论是修复 bug、添加新功能还是改进文档，每一个贡献都很重要。本指南将帮助你开始贡献之旅。

## 贡献方式

有很多种方式可以为 RocketMQ-Rust 做出贡献：

### 代码贡献

- **Bug 修复**：修复已报告的问题
- **新功能**：添加新功能
- **性能改进**：优化现有代码
- **文档**：改进代码文档
- **测试**：添加单元测试和集成测试

### 非代码贡献

- **Bug 报告**：报告 bug 和问题
- **功能请求**：建议新功能
- **文档**：改进用户文档
- **代码审查**：审查拉取请求
- **社区支持**：帮助其他用户

## 开始贡献

### 1. Fork 和克隆

```bash
# 在 GitHub 上 Fork 仓库
# 克隆你的 fork
git clone https://github.com/YOUR_USERNAME/rocketmq-rust.git
cd rocketmq-rust

# 添加 upstream 远程仓库
git remote add upstream https://github.com/mxsm/rocketmq-rust.git
```

### 2. 设置开发环境

```bash
# 安装 Rust nightly 工具链
rustup toolchain install nightly
rustup default nightly

# 安装开发工具
rustup component add rustfmt clippy

# 构建项目
cargo build
```

### 3. 创建分支

```bash
# 与 upstream 同步
git fetch upstream
git checkout main
git merge upstream/main

# 创建功能分支
git checkout -b feature/your-feature-name
```

## 开发工作流

### 进行更改

1. **编写代码**：遵循编码标准
2. **添加测试**：确保测试覆盖率
3. **格式化代码**：使用 rustfmt

    ```bash
    cargo fmt
    ```

4. **运行 linter**：使用 clippy

    ```bash
    cargo clippy -- -D warnings
    ```

5. **运行测试**：确保所有测试通过

    ```bash
    cargo test --all
    ```

### 提交更改

使用清晰的提交信息：

```text
feat: 添加事务消息支持

- 实现 TransactionMQProducer
- 添加事务监听器 trait
- 为事务消息添加单元测试

Closes #123
```

提交信息格式：

- `feat:` 新功能
- `fix:` Bug 修复
- `docs:` 文档更改
- `test:` 测试更改
- `refactor:` 代码重构
- `perf:` 性能改进
- `chore:` 构建过程或工具

### 提交拉取请求

1. **推送到你的 fork**：

    ```bash
    git push origin feature/your-feature-name
    ```

2. **创建拉取请求**：在 GitHub 上

3. **填写 PR 模板**：
   - 描述你的更改
   - 引用相关问题
   - 如果适用，添加截图

4. **等待审查**：维护者将审查你的 PR

## 代码审查流程

### 预期情况

我们的审查流程旨在保持代码质量，同时对贡献者友好：

1. **自动检查**（1-5 分钟）：CI 运行测试、linting 和格式检查
2. **人工审查**（1-3 天）：维护者审查你的代码质量和设计
3. **反馈讨论**：协作讨论以改进代码
4. **批准和合并**：一旦批准，你的 PR 将被合并！🎉

### 处理反馈

代码审查是对话，而非批评。当你收到反馈时：

- 回应所有审查意见（即使是确认收到）
- 如果有不清楚的地方，请提问
- 做出请求的更改并推送到同一分支
- 准备好后请求重新审查
- 不要犹豫讨论替代方案

**注意**：CodeRabbit 的建议是有帮助的参考，但最终决定由维护者在代码审查期间做出。

## 编码标准

详见 [编码标准](./coding-standards) 获取详细指南。

### 核心原则

1. **遵循 Rust 惯用语**：编写地道的 Rust 代码
2. **错误处理**：正确使用 `Result` 类型
3. **文档**：为公共 API 添加文档注释
4. **测试**：编写全面的测试
5. **性能**：考虑性能影响

### 示例

```rust
//! 向 RocketMQ broker 发送消息的生产者模块。

use crate::error::{Error, Result};
use crate::model::Message;

/// 用于发送消息的 RocketMQ 生产者。
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
pub struct Producer {
    // 实现
}

impl Producer {
    /// 创建新的生产者实例。
    pub fn new() -> Self {
        Self { /* ... */ }
    }

    /// 向 broker 发送消息。
    ///
    /// # 错误
    ///
    /// 以下情况将返回错误：
    /// - broker 不可用
    /// - 消息超过最大大小
    /// - 发生网络超时
    pub async fn send(&self, message: Message) -> Result<SendResult> {
        // 实现
    }
}
```

## 开发指南

详见 [开发指南](./development-guide) 获取详细信息。

## 测试

### 运行测试

```bash
# 运行所有测试
cargo test --all

# 运行特定测试
cargo test test_send_message

# 带输出运行测试
cargo test -- --nocapture

# 并行运行测试
cargo test --all -- --test-threads=4
```

### 编写测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_message() {
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
}
```

## 文档

### 构建文档

```bash
# 构建文档
cargo doc --no-deps --open
```

### 编写文档

```rust
/// 简要说明（一句话）
///
/// 更详细的解释。
///
/// # 示例
///
/// ```
/// use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
/// use rocketmq_common::common::message::message_single::Message;
///
/// let mut producer = DefaultMQProducer::builder()
///     .producer_group("example_group")
///     .name_server_addr("localhost:9876")
///     .build();
/// let message = Message::builder().topic("TopicTest").body("hello").build().unwrap();
/// ```
///
/// # 错误
///
/// 以下情况此函数将返回错误...
///
/// # Panics
///
/// 以下情况此函数将 panic...
pub fn public_function() -> Result<()> {
    // 实现
}
```

## 获取帮助

我们随时提供帮助！请通过以下方式联系：

- **GitHub Issues**：[报告 bug 或请求功能](https://github.com/mxsm/rocketmq-rust/issues)
- **GitHub Discussions**：[提问和分享想法](https://github.com/mxsm/rocketmq-rust/discussions)
- **邮件**：[mxsm@apache.org](mailto:mxsm@apache.org)

## 许可证

RocketMQ-Rust 采用 [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) 许可。通过贡献，你同意你的贡献将使用相同的许可证。

## 行为准则

我们致力于为每个人提供一个友好和包容的环境。请：

- 在交流中保持尊重和体贴
- 欢迎新来者并帮助他们入门
- 专注于建设性反馈
- 假设良好的意图
- 开放协作并分享知识

我们都在这里一起构建优秀的软件！🚀

## 后续步骤

- [开发指南](./development-guide) - 详细的开发信息
- [编码标准](./coding-standards) - 代码风格指南
- [报告问题](https://github.com/mxsm/rocketmq-rust/issues) - 提交 bug 或功能请求
