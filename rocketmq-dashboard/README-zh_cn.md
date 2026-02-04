# RocketMQ-Rust 管理控制台

RocketMQ-Rust 的现代化、高性能管理控制台实现，兼容 Apache RocketMQ。采用前沿 UI 框架构建，提供多种部署选项。

## 🎯 概述

该工作空间使用不同的 UI 技术提供多种管理控制台实现，所有实现共享相同的核心业务逻辑。无论您偏好原生桌面性能还是基于 Web 的灵活性，我们都能满足您的需求。

**兼容性**：所有实现均兼容 Apache RocketMQ 集群，可无缝管理 RocketMQ-Rust 和 Apache RocketMQ 部署。

## 📦 结构

这是一个嵌套工作空间，包含：

- **[rocketmq-dashboard-common](./rocketmq-dashboard-common)**：共享代码、数据模型、API 客户端和业务逻辑
- **[rocketmq-dashboard-gpui](./rocketmq-dashboard-gpui)**：使用 [GPUI](https://www.gpui.rs/) 的原生桌面 UI - GPU 加速，纯 Rust 实现
- **[rocketmq-dashboard-tauri](./rocketmq-dashboard-tauri)**：使用 [Tauri](https://tauri.app/) 的跨平台桌面 UI - Rust 后端配合 Web 前端

## 🤔 为什么要提供多种实现？

不同的 UI 框架在不同场景下各有优势：

| 框架 | 优势 | 适用场景 |
|-----------|-----------|----------|
| **GPUI** | • 原生性能<br>• GPU 加速渲染<br>• 纯 Rust 实现（无 Web 技术栈）<br>• 低内存占用 | 高级用户、熟悉原生应用的开发者 |
| **Tauri** | • 熟悉的 Web 技术栈（React/TypeScript）<br>• 丰富的 UI 组件生态<br>• 易于定制和扩展<br>• 跨平台（Windows、macOS、Linux） | 具有 Web 开发经验的团队、快速原型开发 |

两种实现通过 `rocketmq-dashboard-common` 共享相同的核心功能。

## 🚀 快速开始

### 前置要求

- Rust 工具链 1.85.0 或更高版本
- Tauri 版本需要：Node.js 24.x 和 npm

### 开发模式

用于快速开发迭代，编译时间更短：

```bash
cd rocketmq-dashboard

# 以开发模式运行 GPUI 版本
cargo run -p rocketmq-dashboard-gpui

# 以开发模式运行 Tauri 版本
cd rocketmq-dashboard-tauri
cargo tauri dev
```

### 生产构建

用于优化构建，获得完整性能：

```bash
# 构建所有实现
cargo build --workspace --release

# 构建特定实现
cargo build -p rocketmq-dashboard-gpui --release
cargo build -p rocketmq-dashboard-tauri --release

# 以 release 模式运行
cargo run -p rocketmq-dashboard-gpui --release

# 对于 Tauri，构建可分发的安装包
cd rocketmq-dashboard-tauri
cargo tauri build
```

**⚡ 性能提示**：开发模式编译速度快约 10 倍，但运行速度较慢。开发时使用开发模式，性能测试和分发时使用 release 模式。

## 🛠️ 开发

### 代码质量检查

提交前运行全面检查：

```bash
# 检查编译
cargo check --workspace

# 格式化代码
cargo fmt --all

# 运行 Clippy（代码检查工具）- 所有实现
cargo clippy --workspace --all-targets --all-features -- -D warnings

# 对特定包运行 Clippy
cargo clippy -p rocketmq-dashboard-common --all-targets -- -D warnings
cargo clippy -p rocketmq-dashboard-gpui --all-targets -- -D warnings
cargo clippy -p rocketmq-dashboard-tauri --all-targets -- -D warnings

# 运行测试
cargo test --workspace
```

### 完整 CI 级别验证

运行与 CI 完全相同的所有检查：

```bash
# 格式检查（非修改性）
cargo fmt --all -- --check

# Clippy，包含所有特性和严格警告
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings

# 以 release 模式构建所有内容
cargo build --workspace --release

# 运行全面测试
cargo test --workspace --all-features
```

## 📚 文档

每个实现都有详细的文档：

- **[通用库](./rocketmq-dashboard-common/README.md)** - 共享 API、数据模型和业务逻辑
- **[GPUI 实现](./rocketmq-dashboard-gpui/README.md)** - GPU 加速的原生桌面 UI
- **[Tauri 实现](./rocketmq-dashboard-tauri/README.md)** - 基于 Web 的跨平台 UI

## 🔌 Apache RocketMQ 兼容性

所有管理控制台实现均完全兼容 Apache RocketMQ：

- **协议兼容性**：使用标准 RocketMQ 管理 API
- **集群管理**：管理 Apache RocketMQ broker 和 name server
- **混合部署**：在同一个管理控制台中监控 RocketMQ-Rust 和 Apache RocketMQ
- **功能对等**：支持所有标准管理控制台操作（主题管理、消费者组、消息查询等）

## 🤝 贡献

欢迎贡献！无论您是想改进共享的通用库还是增强特定的 UI 实现，请：

1. 检查现有 issue 或创建新的 issue
2. Fork 仓库
3. 创建功能分支
4. 进行更改并附带适当的测试
5. 运行完整的 CI 检查套件
6. 提交 pull request

## 📄 许可证

该项目继承 RocketMQ-Rust 父项目的双重许可证：
- Apache License 2.0
- MIT License
