<p align="center">
    <img src="resources/logo.png" width="30%" height="auto"/>
</p>

<div align="center">

[![GitHub last commit](https://img.shields.io/github/last-commit/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/commits/main)
[![Crates.io](https://img.shields.io/crates/v/rocketmq-rust.svg)](https://crates.io/crates/rocketmq-rust)
[![Docs.rs](https://docs.rs/rocketmq-rust/badge.svg)](https://docs.rs/rocketmq-rust)
[![CI](https://github.com/mxsm/rocketmq-rust/workflows/CI/badge.svg)](https://github.com/mxsm/rocketmq-rust/actions)
[![CodeCov][codecov-image]][codecov-url] [![GitHub contributors](https://img.shields.io/github/contributors/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/graphs/contributors) [![Crates.io License](https://img.shields.io/crates/l/rocketmq-rust)](#license)
<br/>
![GitHub repo size](https://img.shields.io/github/repo-size/mxsm/rocketmq-rust)
![Static Badge](https://img.shields.io/badge/MSRV-1.85.0%2B-25b373)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/mxsm/rocketmq-rust)

</div>

<div align="center">
  <a href="https://trendshift.io/repositories/12176" target="_blank"><img src="https://trendshift.io/api/badge/repositories/12176" alt="mxsm%2Frocketmq-rust | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
  <a href="https://trendshift.io/developers/3818" target="_blank"><img src="https://trendshift.io/api/badge/developers/3818" alt="mxsm | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>

# RocketMQ-Rust

🚀 一个高性能、可靠且功能丰富的 [Apache RocketMQ](https://github.com/apache/rocketmq) **非官方 Rust 实现**，旨在将企业级消息中间件引入 Rust 生态系统。

<div align="center">

[![概述](https://img.shields.io/badge/📖_概述-4A90E2?style=flat-square&labelColor=2C5F9E&color=4A90E2)](#-概述)
[![快速开始](https://img.shields.io/badge/🚀_快速开始-50C878?style=flat-square&labelColor=2D7A4F&color=50C878)](#-快速开始)
[![文档](https://img.shields.io/badge/📚_文档-FF8C42?style=flat-square&labelColor=CC6A2F&color=FF8C42)](#-文档)
[![组件](https://img.shields.io/badge/📦_组件-9B59B6?style=flat-square&labelColor=6C3483&color=9B59B6)](#-组件--crate)
<br/>
[![贡献](https://img.shields.io/badge/🤝_贡献-F39C12?style=flat-square&labelColor=B9770E&color=F39C12)](#-贡献)
[![社区](https://img.shields.io/badge/👥_社区-8E44AD?style=flat-square&labelColor=633974&color=8E44AD)](#社区--支持)

</div>

---

## ✨ 概述

**RocketMQ-Rust** 是 Apache RocketMQ 的完整 Rust 重新实现，利用 Rust 在内存安全、零成本抽象和无畏并发方面的独特优势。该项目旨在为 Rust 开发者提供一个生产就绪的分布式消息队列系统，在保持与 RocketMQ 协议完全兼容的同时，提供卓越的性能。

### 🎯 为什么选择 RocketMQ-Rust？

- **🦀 内存安全**：基于 Rust 的所有权模型，在编译时消除空指针解引用、缓冲区溢出和数据竞争等整类错误
- **⚡ 高性能**：零成本抽象和高效的异步运行时，以最小的资源开销提供卓越的吞吐量
- **🔒 线程安全**：无畏并发支持安全并行处理，没有竞争条件的风险
- **🌐 跨平台**：在 Linux、Windows 和 macOS 上提供一流支持，每个平台都有原生性能
- **🔌 生态系统集成**：与 Rust 丰富的生态系统无缝集成，包括 Tokio、Serde 和其他现代库
- **📦 生产就绪**：经过实战验证的架构，具有全面的错误处理和可观察性

## 🏗️ 架构

<p align="center">
  <img src="resources/architecture.png" alt="RocketMQ-Rust 架构" width="80%"/>
</p>

RocketMQ-Rust 实现了分布式架构，包含以下核心组件：

- **Name Server**：轻量级服务发现和路由协调
- **Broker**：消息存储和传递引擎，支持主题、队列和消费者组
- **Producer Client**：高性能消息发布，支持多种发送模式
- **Consumer Client**：灵活的消息消费，支持推送和拉取模式
- **Store**：高效的本地存储引擎，针对顺序写入进行了优化
- **Controller**：高级高可用性和故障转移能力

## 📚 文档

- **📖 官方文档**：[rocketmqrust.com](https://rocketmqrust.com) - 综合指南、API 参考和最佳实践
- **🤖 AI 驱动文档**：[DeepWiki](https://deepwiki.com/mxsm/rocketmq-rust) - 带有智能搜索的交互式文档
- **📝 API 文档**：[docs.rs/rocketmq-rust](https://docs.rs/rocketmq-rust) - 完整的 API 文档
- **📋 示例**：[rocketmq-client/examples](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/examples) - 可运行的代码示例

## 🚀 快速开始

### 前置要求

- Rust 工具链 1.85.0 或更高版本
- 可用的 shell 和 `cargo`
- 为 NameServer、Broker 和客户端示例准备独立终端

### 1. 构建工作区

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
cargo build --workspace
```

如果只想在自己的应用中使用客户端 SDK，请在 `Cargo.toml` 中添加当前版本：

```toml
[dependencies]
rocketmq-client-rust = "1.0.0"
rocketmq-common = "1.0.0"
```

### 2. 启动 NameServer

```bash
cargo run --bin rocketmq-namesrv-rust
```

默认 NameServer 地址为 `127.0.0.1:9876`。如需显式指定绑定地址：

```bash
cargo run --bin rocketmq-namesrv-rust -- --ip 127.0.0.1 --port 9876
```

### 3. 启动 Broker

Broker 需要设置 `ROCKETMQ_HOME`。可以指向已有 RocketMQ 目录，也可以为本地快速测试创建一个运行目录。

Linux/macOS：

```bash
export ROCKETMQ_HOME="$(pwd)/.rocketmq"
mkdir -p "$ROCKETMQ_HOME/conf"
cargo run --bin rocketmq-broker-rust -- -n 127.0.0.1:9876
```

Windows PowerShell：

```powershell
$env:ROCKETMQ_HOME = "$PWD\.rocketmq"
New-Item -ItemType Directory -Force "$env:ROCKETMQ_HOME\conf" | Out-Null
cargo run --bin rocketmq-broker-rust -- -n 127.0.0.1:9876
```

使用 `cargo run --bin rocketmq-broker-rust -- --help` 查看 `--configFile`、`--namesrvAddr` 和配置打印等参数。

### 4. 收发消息

先启动消费者示例：

```bash
cargo run -p rocketmq-client-rust --example consumer
```

然后在另一个终端发送消息：

```bash
cargo run -p rocketmq-client-rust --example producer
```

快速开始示例默认使用 `127.0.0.1:9876` 和 `TopicTest`。更多消息模式请查看：

- [发送单条消息](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#send-a-single-message)
- [批量发送消息](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#send-batch-messages)
- [RPC 消息](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#send-rpc-messages)
- [所有示例](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/examples)

## 📦 组件 & Crate

RocketMQ-Rust 按可部署服务、可复用协议/运行时 crate 和运维应用组织。下表关注职责和集成边界，不再按单个 crate 标注成熟度。

### 核心运行时服务

| Crate | 职责 |
|-------|------|
| [rocketmq](./rocketmq) | 公共基础 crate 和共享运行时入口。 |
| [rocketmq-namesrv](./rocketmq-namesrv) | NameServer 实现，负责 broker 注册、主题路由和服务发现。 |
| [rocketmq-broker](./rocketmq-broker) | Broker 实现，负责消息存储、分发、投递和消费协调。 |
| [rocketmq-controller](./rocketmq-controller) | Controller 服务，负责 broker 协调和高可用工作流。 |
| [rocketmq-proxy](./rocketmq-proxy) | Proxy 层，提供网关式客户端访问和协议集成。 |

### 客户端、协议与共享库

| Crate | 职责 |
|-------|------|
| [rocketmq-client](./rocketmq-client) | 面向应用集成的异步 producer、consumer 和 admin SDK。 |
| [rocketmq-remoting](./rocketmq-remoting) | RocketMQ remoting 协议、命令编解码和网络集成。 |
| [rocketmq-common](./rocketmq-common) | 共享消息模型、配置类型、常量和工具代码。 |
| [rocketmq-auth](./rocketmq-auth) | 认证、授权、ACL 判断和请求上下文支持。 |
| [rocketmq-filter](./rocketmq-filter) | 消息过滤支持，包括 tag 和表达式过滤。 |

### 存储、运行时与可观测性

| Crate | 职责 |
|-------|------|
| [rocketmq-store](./rocketmq-store) | 持久化本地存储引擎，覆盖 commit log、consume queue 和消息索引。 |
| [rocketmq-tieredstore](./rocketmq-tieredstore) | 分层存储抽象，用于将消息数据扩展到本地磁盘之外。 |
| [rocketmq-runtime](./rocketmq-runtime) | 异步运行时抽象和适配异步运行时的协调工具。 |
| [rocketmq-error](./rocketmq-error) | 工作区 crate 共享的错误类型和结果约定。 |
| [rocketmq-macros](./rocketmq-macros) | RocketMQ-Rust crate 和示例使用的过程宏。 |
| [rocketmq-observability](./rocketmq-observability) | 服务端和客户端埋点使用的 metrics 与 tracing 集成。 |

### 工具、示例与 Dashboard

| Project | 职责 |
|---------|------|
| [rocketmq-example](./rocketmq-example) | 独立示例，覆盖 producer、consumer、请求/响应、顺序、延迟和事务流程。 |
| [rocketmq-tools](./rocketmq-tools) | 命令行工具和运维实用程序。 |
| [rocketmq-admin-cli](./rocketmq-tools/rocketmq-admin/rocketmq-admin-cli) | 用于集群和 broker 运维操作的命令行管理接口。 |
| [rocketmq-admin-core](./rocketmq-tools/rocketmq-admin/rocketmq-admin-core) | CLI 和终端界面复用的管理核心能力。 |
| [rocketmq-admin-tui](./rocketmq-tools/rocketmq-admin/rocketmq-admin-tui) | 面向交互式运维流程的终端 UI。 |
| [rocketmq-store-inspect](./rocketmq-tools/rocketmq-store-inspect) | broker 数据文件的存储检查工具。 |
| [rocketmq-dashboard](./rocketmq-dashboard) | Dashboard 工作区，覆盖桌面、Web 和共享管理 UI 组件。 |
| [rocketmq-dashboard-common](./rocketmq-dashboard/rocketmq-dashboard-common) | 共享 dashboard 模型和可复用 dashboard 基础设施。 |
| [rocketmq-dashboard-gpui](./rocketmq-dashboard/rocketmq-dashboard-gpui) | 基于 GPUI 的桌面 dashboard。 |
| [rocketmq-dashboard-tauri](./rocketmq-dashboard/rocketmq-dashboard-tauri) | 基于 Tauri 的跨平台 dashboard shell 和后端。 |
| [rocketmq-dashboard-web](./rocketmq-dashboard/rocketmq-dashboard-web) | Web dashboard 前端和后端项目。 |

## 💡 能力边界

RocketMQ-Rust 聚焦 RocketMQ 兼容的消息服务和 Rust 原生集成能力。

| 领域 | 提供能力 |
|------|----------|
| 消息服务 | NameServer、Broker、Controller 和 Proxy 服务，覆盖路由、存储、投递、协调和网关访问。 |
| 客户端集成 | 异步 producer、consumer、admin、请求/响应、批量、顺序、延迟和事务消息 API。 |
| 协议兼容 | RocketMQ remoting 命令模型、header、序列化、路由发现以及 client/broker 互操作。 |
| 存储引擎 | durable commit log、consume queue、index、checkpoint 和分层存储构建块。 |
| 安全与治理 | 认证、授权、ACL 判断、请求上下文以及 broker/client 侧集成点。 |
| 运维能力 | metrics、tracing、admin 工具、存储检查工具和用于集群可视化的 dashboard 项目。 |

## 🧪 构建与校验

快速开始章节覆盖首次本地运行。日常开发和代码审查请使用以下根工作区命令。

| 任务 | 命令 |
|------|------|
| 构建工作区 | `cargo build --workspace` |
| 运行工作区测试 | `cargo test --workspace` |
| 运行指定 crate 测试 | `cargo test -p rocketmq-client` |
| 格式化 Rust 代码 | `cargo fmt --all` |
| 按工作区 feature 运行 clippy | `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` |
| 构建本地 API 文档 | `cargo doc --workspace --no-deps` |

`rocketmq-example/` 和 `rocketmq-dashboard/` 下的独立项目需要在各自项目根目录中单独校验。

## 🤝 贡献

我们欢迎社区贡献！无论是修复错误、添加功能、改进文档还是分享想法，您的输入都很有价值。

### 如何贡献

1. **Fork** 仓库
2. **创建** 功能分支（`git checkout -b feature/amazing-feature`）
3. **提交** 您的更改（`git commit -m 'Add amazing feature'`）
4. **推送** 到分支（`git push origin feature/amazing-feature`）
5. **打开** Pull Request

### 贡献指南

- 遵循 Rust 最佳实践和惯用模式
- 为新功能添加测试
- 根据需要更新文档
- 在提交 PR 之前确保 CI 通过
- 使用有意义的提交消息

详细指南，请阅读我们的[贡献指南](https://rocketmqrust.com/docs/contribute-guide/)。

### 仓库活动

![Repository Activity](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

## ❓ 常见问题

<details>
<summary><b>RocketMQ-Rust 是否生产就绪？</b></summary>

是的。核心服务和客户端 SDK 面向生产部署设计，并保持积极维护。
</details>

<details>
<summary><b>是否与 Apache RocketMQ 兼容？</b></summary>

是的，RocketMQ-Rust 实现了 RocketMQ 协议，可以与 Apache RocketMQ Java 客户端和服务器互操作。
</details>

<details>
<summary><b>最低支持的 Rust 版本（MSRV）是什么？</b></summary>

最低支持的 Rust 版本是 1.85.0（stable 或 nightly）。
</details>

<details>
<summary><b>性能与 Java RocketMQ 相比如何？</b></summary>

RocketMQ-Rust 利用 Rust 的零成本抽象和高效的异步运行时，以较低的内存占用提供相当或更好的性能。基准测试可在各个组件文档中找到。
</details>

<details>
<summary><b>可以与现有的 RocketMQ 部署一起使用吗？</b></summary>

可以，您可以将 RocketMQ-Rust 组件与 Java RocketMQ 一起部署。例如，您可以在 Java broker 上使用 Rust 客户端，反之亦然。
</details>

<details>
<summary><b>如何从 Java RocketMQ 迁移到 RocketMQ-Rust？</b></summary>

迁移可以增量完成：
1. 首先在现有 Java broker 上使用 Rust 客户端 SDK
2. 逐步用 Rust 实现替换 broker
3. 迁移期间两种实现可以共存

有关详细步骤，请参阅我们的[迁移指南](https://rocketmqrust.com)。
</details>

## 👥 社区 & 支持

- **💬 讨论**：[GitHub Discussions](https://github.com/mxsm/rocketmq-rust/discussions) - 提问和分享想法
- **🐛 问题**：[GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues) - 报告错误或请求功能
- **📧 联系**：联系 [mxsm@apache.org](mailto:mxsm@apache.org)

### 贡献者

感谢所有贡献者！🙏

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" />
</a>

### Star 历史

[![Star History Chart](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)](https://star-history.com/#mxsm/rocketmq-rust&Date)

## 📄 许可证

RocketMQ-Rust 采用 **Apache License 2.0** 许可证。

请参阅 [LICENSE-APACHE](LICENSE-APACHE) 或 http://www.apache.org/licenses/LICENSE-2.0。

## 🙏 致谢

- **Apache RocketMQ 社区** 提供原始 Java 实现和设计
- **Rust 社区** 提供优秀的工具和库
- **所有贡献者** 帮助改进这个项目

---

<p align="center">
  <sub>由 RocketMQ-Rust 社区用 ❤️ 构建</sub>
</p>

[codecov-image]: https://codecov.io/gh/mxsm/rocketmq-rust/branch/main/graph/badge.svg
[codecov-url]: https://codecov.io/gh/mxsm/rocketmq-rust
