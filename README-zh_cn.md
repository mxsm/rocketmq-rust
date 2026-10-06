<p align="center">
    <img src="resources/RocketMQ-Rust.png" width="30%" height="auto" alt="RocketMQ-Rust 标志"/>
    <img src="resources/logo.png" width="30%" height="auto" alt="RocketMQ-Rust 字标"/>
</p>

<div align="center">

[![GitHub last commit](https://img.shields.io/github/last-commit/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/commits/main)
[![Crates.io](https://img.shields.io/crates/v/rocketmq-client-rust.svg)](https://crates.io/crates/rocketmq-client-rust)
[![Docs.rs](https://docs.rs/rocketmq-client-rust/badge.svg)](https://docs.rs/rocketmq-client-rust)
[![Docker](https://img.shields.io/docker/v/mxsm/rocketmq-rust-broker?label=docker)](https://hub.docker.com/u/mxsm)
[![CI](https://github.com/mxsm/rocketmq-rust/actions/workflows/rocketmq-rust-ci.yaml/badge.svg?branch=main)](https://github.com/mxsm/rocketmq-rust/actions/workflows/rocketmq-rust-ci.yaml)
[![Website Deploy](https://github.com/mxsm/rocketmq-rust/actions/workflows/deploy.yml/badge.svg)](https://github.com/mxsm/rocketmq-rust/actions/workflows/deploy.yml)
[![Website Check](https://github.com/mxsm/rocketmq-rust/actions/workflows/website-check.yml/badge.svg)](https://github.com/mxsm/rocketmq-rust/actions/workflows/website-check.yml)
[![CodeCov][codecov-image]][codecov-url] [![GitHub contributors](https://img.shields.io/github/contributors/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/graphs/contributors) [![License](https://img.shields.io/crates/l/rocketmq-client-rust)](#-许可证)
<br/>
![GitHub repo size](https://img.shields.io/github/repo-size/mxsm/rocketmq-rust)
![MSRV](https://img.shields.io/badge/MSRV-1.95.0%2B-25b373)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/mxsm/rocketmq-rust)

</div>

<div align="center">
  <a href="https://trendshift.io/repositories/12176" target="_blank"><img src="https://trendshift.io/api/badge/repositories/12176" alt="mxsm%2Frocketmq-rust | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
  <a href="https://trendshift.io/developers/3818" target="_blank"><img src="https://trendshift.io/api/badge/developers/3818" alt="mxsm | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>

# RocketMQ-Rust

[English](README.md) | [简体中文](README-zh_cn.md)

🚀 一个高性能、可靠且功能丰富的 [Apache RocketMQ](https://github.com/apache/rocketmq) **非官方 Rust 实现**，旨在将企业级消息中间件引入 Rust 生态系统。

<div align="center">

[![概述](https://img.shields.io/badge/📖_概述-4A90E2?style=flat-square&labelColor=2C5F9E&color=4A90E2)](#-概述)
[![快速开始](https://img.shields.io/badge/🚀_快速开始-50C878?style=flat-square&labelColor=2D7A4F&color=50C878)](#-快速开始)
[![文档](https://img.shields.io/badge/📚_文档-FF8C42?style=flat-square&labelColor=CC6A2F&color=FF8C42)](#-文档)
[![组件](https://img.shields.io/badge/📦_组件-9B59B6?style=flat-square&labelColor=6C3483&color=9B59B6)](#-组件--crate)
<br/>
[![部署](https://img.shields.io/badge/🚢_部署-1ABC9C?style=flat-square&labelColor=117A65&color=1ABC9C)](#-部署)
[![贡献](https://img.shields.io/badge/🤝_贡献-F39C12?style=flat-square&labelColor=B9770E&color=F39C12)](#-贡献)
[![常见问题](https://img.shields.io/badge/❓_常见问题-E74C3C?style=flat-square&labelColor=A93226&color=E74C3C)](#-常见问题)
[![社区](https://img.shields.io/badge/👥_社区-8E44AD?style=flat-square&labelColor=633974&color=8E44AD)](#-社区--支持)

</div>

---

## ✨ 概述

**RocketMQ-Rust** 使用 Rust 重新实现了 Apache RocketMQ，包括 NameServer、Broker、Controller 和 Proxy 服务、异步客户端 SDK 以及配套的运维工具。它沿用 RocketMQ 的主题、队列、消费者组和 NameServer 路由模型，支持 RocketMQ remoting 协议，并通过 Proxy 提供 gRPC `MessagingService`；同时借助 Rust 的所有权模型保证内存安全，基于 Tokio 实现异步 I/O。

> **状态：** 当前稳定版本为 `1.0.0`。工作区的 28 个 crate 已全部发布到 [crates.io](https://crates.io/crates/rocketmq-client-rust)，13 个容器镜像已发布到 [Docker Hub](https://hub.docker.com/u/mxsm) 和 [GHCR](https://github.com/mxsm?tab=packages&repo_name=rocketmq-rust)。RocketMQ-Rust 是独立的社区发行，不是 Apache 软件基金会的官方发行。

### 🎯 为什么选择 RocketMQ-Rust？

- **🦀 内存安全**：Rust 的所有权模型在编译期消除释放后使用、数据竞争等整类错误，并且没有垃圾回收停顿
- **⚡ 原生异步**：服务端和客户端运行在 Tokio 之上，具备明确的运行时所有权、有界队列与准入控制，优雅停机时会报告已停止的任务
- **🔁 兼容 RocketMQ**：实现 remoting 协议和 gRPC `MessagingService`，可转换已有的 Java Broker `.properties` 配置文件；以 Apache RocketMQ 5.5.0 作为对比基线
- **💾 可插拔存储**：默认使用本地 CommitLog 和 ConsumeQueue 文件，可选 RocksDB 和分层存储后端
- **🔒 安全且可观测**：ACL 认证与授权、TLS 传输，以及带 OTLP 和 Prometheus 导出器的 OpenTelemetry 指标、链路追踪和日志
- **🛠️ 自带运维能力**：Admin CLI 与终端 UI、Web 与桌面 Dashboard、容器镜像、Helm chart 和 Kubernetes 清单
- **🤖 面向 AI 的运维**：用于诊断的只读 MCP 服务、用于受控变更且独立管控的 MCP 服务，以及 AI SRE 平台
- **🌐 跨平台**：在 CI 中于 Linux、Windows 和 macOS 上构建并测试

## 🏗️ 架构

<p align="center">
  <img src="resources/rocketmq-rust-architecture.svg" alt="RocketMQ-Rust 架构" width="100%"/>
</p>

RocketMQ-Rust 由以下组件构成。最小集群只需要 NameServer、Broker 和客户端，其余组件均为可选。

- **NameServer**：Broker 注册、存活跟踪和主题路由查询
- **Broker**：请求处理、主题与消费者组元数据、消息存储与投递
- **Store**：Broker 内部的存储引擎，包含 CommitLog、ConsumeQueue 和索引文件，并可选 RocksDB 与分层存储后端
- **客户端 SDK**：面向 Rust 应用的生产者，以及 Push、Lite Pull 和 POP 消费者
- **Proxy**：gRPC 及可选的 remoting 接入层，可部署在集群前端，也可内嵌 Broker 运行
- **Controller**：基于 OpenRaft 的主节点选举与副本协调，用于 Controller 模式的高可用
- **运维工具**：Admin CLI 与 TUI、Dashboard 和 MCP 服务，均不在消息链路上

[架构总览](https://rocketmqrust.com/zh-CN/docs/architecture/overview)介绍了消息生命周期、存储和高可用设计。

## 📚 文档

- **📖 官方文档**：[rocketmqrust.com](https://rocketmqrust.com/zh-CN/) - 入门、架构、配置、部署和运维指南，提供中英文版本
- **📝 API 文档**：[docs.rs/rocketmq-client-rust](https://docs.rs/rocketmq-client-rust) - 客户端 SDK 的 API 参考
- **📋 示例**：[rocketmq-example](./rocketmq-example) 和 [rocketmq-client/examples](./rocketmq-client/examples) - 可直接运行的生产者和消费者示例
- **🧭 设计文档**：[rocketmq-doc](./rocketmq-doc) - 架构决策记录、协议说明和迁移指南
- **📦 版本发布**：[GitHub Releases](https://github.com/mxsm/rocketmq-rust/releases) 和 [CHANGELOG.md](CHANGELOG.md) - 发布说明和变更历史
- **🤖 AI 驱动文档**：[DeepWiki](https://deepwiki.com/mxsm/rocketmq-rust) - 带有智能搜索的交互式文档

## 🚀 快速开始

从源码运行单节点集群并收发消息。所有监听端口都绑定在 `127.0.0.1`，Broker 的数据保存在已被 git 忽略的 `.rocketmq/` 目录中。

### 前置要求

- Git，以及通过 [rustup](https://rustup.rs) 安装的 Rust 工具链；仓库在 `rust-toolchain.toml` 中固定使用 Rust 1.95.0
- 用于编译原生依赖的 C/C++ 构建工具链：Windows 上的 MSVC Build Tools、macOS 上的 Xcode Command Line Tools，或 Linux 上的 GCC/Clang
- 空闲的 TCP 端口 `9876`、`10909`、`10911` 和 `10912`，以及约 1.5 GB 磁盘空间，用于 Broker 预分配的存储文件

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
```

以下命令均在仓库根目录执行，每个服务使用一个独立终端。首次构建需要几分钟。

### 1. 启动 NameServer

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- --rocketmqHome .rocketmq --bindAddress 127.0.0.1
```

NameServer 监听 `127.0.0.1:9876`。它需要一个主目录：请传入 `--rocketmqHome` 或设置 `ROCKETMQ_HOME`，否则会在启动阶段退出。

### 2. 启动 Broker

创建 `.rocketmq` 目录，并将以下配置保存为 `.rocketmq/broker.toml`：

```toml
[broker]
namesrvAddr = "127.0.0.1:9876"
brokerIp1 = "127.0.0.1"
storePathRootDir = "./.rocketmq/broker"

[broker.brokerServerConfig]
bindAddress = "127.0.0.1"

[store]
storePathRootDir = "./.rocketmq/store"
haListenAddress = "127.0.0.1"
```

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c .rocketmq/broker.toml
```

等待日志中出现 `started successfully`。此时 Broker 已注册到 NameServer，并监听 `127.0.0.1:10911`。如果不指定配置文件，Broker 会绑定所有网卡、对外通告自动探测到的地址，并将数据保存在 `~/store` 下。

### 3. 收发消息

```bash
cargo run -p rocketmq-client-rust --example simple-producer
```

生产者向 `TopicTest` 发送十条消息后退出，该主题由 Broker 在首次使用时自动创建。然后启动消费者：

```bash
cargo run -p rocketmq-client-rust --example consumer
```

消费者会打印这十条消息并继续等待新消息，按 `Ctrl+C` 停止。

### 4. 停止与清理

在 Broker 终端按 `Ctrl+C`，等待其停机完成，再以同样方式停止 NameServer。删除 `.rocketmq/` 即可清除数据。

> 本教程未启用认证和 TLS，因此所有监听端口都只绑定回环地址。将服务暴露到网络之前，请先阅读[部署安全指南](https://rocketmqrust.com/zh-CN/docs/deployment/security)。

### 在应用中使用客户端 SDK

添加客户端以及与之配合使用的三个 crate：

```toml
[dependencies]
rocketmq-client-rust = "1.0.0"
rocketmq-model = "1.0.0"
rocketmq-runtime = "1.0.0"
rocketmq-observability = "1.0.0"
```

运行时由应用持有，并传递给每个客户端。一个最小的生产者示例：

```rust
use rocketmq_client_rust::{ClientRuntime, ClientRuntimeConfig, DefaultMQProducer};
use rocketmq_model::common::message::message_single::Message;
use rocketmq_observability::TelemetryRuntimeGuard;
use rocketmq_runtime::RuntimeOwner;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 运行时由应用持有，客户端不会隐式创建运行时。
    let owner = RuntimeOwner::new()?;
    let telemetry = TelemetryRuntimeGuard::noop();
    let client_runtime = ClientRuntime::try_new(
        owner.root_context().component("example-client"),
        ClientRuntimeConfig::default(),
        telemetry.handle(),
    )?;

    let (sent, client_report) = owner.block_on(async {
        let mut producer = DefaultMQProducer::builder(client_runtime.clone())
            .producer_group("example_producer_group")
            .name_server_addr("127.0.0.1:9876")
            .build();
        let sent = async {
            producer.start().await?;
            let message = Message::builder()
                .topic("TopicTest")
                .body_slice(b"Hello RocketMQ")
                .build()?;
            producer.send_with_timeout(message, 3000).await
        }
        .await;
        // 按顺序停机：生产者、共享的客户端运行时，最后是运行时所有者。
        producer.shutdown().await;
        (sent, client_runtime.shutdown().await)
    });
    let runtime_report = owner.shutdown_runtime_blocking()?;

    println!("send result: {:?}", sent?);
    if !client_report.is_healthy() || !runtime_report.is_healthy() {
        return Err("client shutdown did not complete cleanly".into());
    }
    Ok(())
}
```

消费者遵循相同的所有权模式。后续可以阅读：

- [客户端指南](./rocketmq-client/README-zh_cn.md) - Push 与 Lite Pull 消费者示例、Cargo feature 和 TLS
- [第一条消息教程](https://rocketmqrust.com/zh-CN/docs/getting-started/quick-start) - 相同的流程，但显式创建主题和消费者组
- [rocketmq-example](./rocketmq-example) - 顺序、延迟、事务、批量和请求/应答消息
- [Rust API 迁移指南](https://rocketmqrust.com/zh-CN/docs/migration/rust-api) - 从 0.x 升级时的源码变更

## 📦 组件 & Crate

根 Cargo 工作区包含 28 个 crate，均已按工作区版本发布到 crates.io。目录名与包名一致，唯一的例外是 `rocketmq-client` 目录对应 `rocketmq-client-rust` 包。Dashboard、AI 运维产品、示例和网站是拥有各自清单文件的独立项目。

### 核心运行时服务

| Crate | 职责 |
|-------|------|
| [rocketmq-namesrv](./rocketmq-namesrv) | NameServer，负责 Broker 注册、存活跟踪、主题路由和 KV 配置。二进制：`rocketmq-namesrv-rust`。 |
| [rocketmq-broker](./rocketmq-broker) | Broker，负责请求处理、主题与消费者组元数据、消息存储、投递和 HA 集成。二进制：`rocketmq-broker-rust`。 |
| [rocketmq-controller](./rocketmq-controller) | Controller，负责 OpenRaft 共识、主节点选举和副本元数据。二进制：`rocketmq-controller-rust`。 |
| [rocketmq-proxy](./rocketmq-proxy) | Proxy 运行时，提供 gRPC `MessagingService` 和可选的 remoting 接入。二进制：`rocketmq-proxy-rust`。 |
| [rocketmq-proxy-core](./rocketmq-proxy-core) | 与后端无关的 Proxy 契约、protobuf 绑定以及接入层与会话状态。 |
| [rocketmq-proxy-cluster](./rocketmq-proxy-cluster) | Cluster 模式的 Proxy 适配器，通过客户端 SDK 访问 NameServer 和 Broker。 |
| [rocketmq-proxy-local](./rocketmq-proxy-local) | Local 模式的 Proxy 适配器，由内嵌 Broker 提供支撑。 |

### 客户端、协议与共享库

| Crate | 职责 |
|-------|------|
| [rocketmq-client](./rocketmq-client) | 异步客户端 SDK，提供生产者、事务生产者、Push 与 Lite Pull 消费者以及可选的管理 API。 |
| [rocketmq-protocol](./rocketmq-protocol) | RocketMQ 协议契约：请求与响应码、remoting 命令帧、类型化的 header 和 body。 |
| [rocketmq-transport](./rocketmq-transport) | 有界的 TCP/TLS 传输层，提供连接准入、请求分发和请求截止时间。 |
| [rocketmq-model](./rocketmq-model) | 与运行时无关的领域类型，涵盖消息、队列、主题和结果。 |
| [rocketmq-auth](./rocketmq-auth) | 认证与 ACL 授权：access key 签名、ACL 文件、用户和策略。 |
| [rocketmq-security-api](./rocketmq-security-api) | 共享安全契约，涵盖主体、请求策略、密钥提供者和启动检查。 |
| [rocketmq-filter](./rocketmq-filter) | Tag 与 SQL92 消息过滤、表达式求值以及布隆过滤器工具。 |

### 存储、运行时与可观测性

| Crate | 职责 |
|-------|------|
| [rocketmq-store](./rocketmq-store) | 面向 Broker 的存储层，涵盖 CommitLog、ConsumeQueue、索引、定时消息、POP 检查点和 HA 复制。 |
| [rocketmq-store-api](./rocketmq-store-api) | 与后端无关的存储契约，涵盖追加、读取、生命周期、复制、检查点和健康能力。 |
| [rocketmq-store-local](./rocketmq-store-local) | 本地文件基础组件：映射文件、刷盘、恢复、消费队列和索引。 |
| [rocketmq-store-rocksdb](./rocketmq-store-rocksdb) | 可选的 RocksDB 派生状态存储，用于消费队列、索引、定时器和事务。 |
| [rocketmq-tieredstore](./rocketmq-tieredstore) | 可选的分层存储，将较早的消息分发到二级存储并按需取回。 |
| [rocketmq-runtime](./rocketmq-runtime) | 基于 Tokio 的运行时底座：运行时所有权、任务跟踪、调度、有界阻塞执行和停机报告。 |
| [rocketmq-error](./rocketmq-error) | 共享错误内核，提供类型化的错误原因、稳定的描述符和可安全脱敏的视图。 |
| [rocketmq-macros](./rocketmq-macros) | 用于协议类型和请求 header 的过程宏。 |
| [rocketmq-observability](./rocketmq-observability) | 日志、指标和链路追踪，可选 OpenTelemetry、OTLP 和 Prometheus 导出器。 |

### 运维工具

| Crate | 职责 |
|-------|------|
| [rocketmq-admin-cli](./rocketmq-tools/rocketmq-admin/rocketmq-admin-cli) | 命令行管理工具，命令体系与 Java `mqadmin` 兼容。 |
| [rocketmq-admin-tui](./rocketmq-tools/rocketmq-admin/rocketmq-admin-tui) | 提供同一组管理命令的交互式终端 UI。 |
| [rocketmq-admin-core](./rocketmq-tools/rocketmq-admin/rocketmq-admin-core) | 与展示层无关的管理服务，供 CLI、TUI、Dashboard 和 MCP 复用。 |
| [rocketmq-store-inspect](./rocketmq-tools/rocketmq-store-inspect) | 离线 CommitLog 检查、降级预检和多路径合并。二进制：`rocketmq-cli-rust`。 |
| [rocketmq-dashboard-common](./rocketmq-dashboard/rocketmq-dashboard-common) | Dashboard 共享的模型与服务。 |

<p align="center">
  <img src="resources/rocketmq-admin-tui.png" alt="rocketmq-admin-tui 展示消费进度" width="85%"/>
</p>

### 独立项目

以下项目位于本仓库中，但不属于根工作区，需要在各自目录中构建。

| 项目 | 职责 |
|------|------|
| [rocketmq-example](./rocketmq-example) | 客户端示例，覆盖各种发送方式、顺序、延迟和事务消息、请求/应答，以及 Push、Lite Pull 和 POP 消费者。 |
| [rocketmq-dashboard-web](./rocketmq-dashboard/rocketmq-dashboard-web) | Web Dashboard，由 Rust 后端和 React 前端组成。 |
| [rocketmq-dashboard-gpui](./rocketmq-dashboard/rocketmq-dashboard-gpui) | 基于 GPUI 的原生桌面 Dashboard。 |
| [rocketmq-dashboard-tauri](./rocketmq-dashboard/rocketmq-dashboard-tauri) | 基于 Tauri 和 React 的跨平台桌面 Dashboard。 |
| [rocketmq-mcp](./rocketmq-ai/rocketmq-mcp) | 用于集群诊断的只读 Model Context Protocol 服务。 |
| [rocketmq-mcp-control](./rocketmq-ai/rocketmq-mcp-control) | 独立隔离、默认拒绝的 MCP 服务，用于受控的集群变更。 |
| [rocketmq-sre](./rocketmq-ai/rocketmq-sre) | AI SRE 平台，用于证据采集、诊断、规划和受控自动化。 |
| [rocketmq-website](./rocketmq-website) | [rocketmqrust.com](https://rocketmqrust.com/zh-CN/) 的 Docusaurus 源码。 |
| [fuzz](./fuzz) | 针对协议、配置、Controller 快照和存储恢复输入的 `cargo-fuzz` 目标。 |

## 💡 能力边界

| 领域 | 提供能力 |
|------|----------|
| 消息服务 | NameServer、Broker、Controller 和 Proxy，支持 TOML 配置、健康探针和优雅停机。Broker 还可以加载 Java `.properties` 文件。 |
| 消息类型 | 普通、顺序、延迟与定时、事务、批量和请求/应答消息，并支持消息撤回。 |
| 消息发送 | 同步、回调和单向发送，批量发送以及自定义队列选择。 |
| 消息消费 | 支持并发或顺序监听器的 Push 消费者，Lite Pull 与 POP 消费，集群与广播模式，Tag 与 SQL92 过滤。 |
| 协议 | RocketMQ remoting 命令、header 和序列化，以及通过 Proxy 提供的 gRPC `MessagingService`。 |
| 存储 | 默认使用本地 CommitLog、ConsumeQueue 和索引文件，可选 RocksDB 和分层存储。 |
| 高可用 | 支持同步或异步确认的主副本复制，以及基于 OpenRaft 的 Controller 模式故障转移。 |
| 安全 | access key 认证、ACL 授权、TLS 传输和安全启动 profile。 |
| 可观测性 | 结构化日志，OpenTelemetry 指标、链路追踪和日志，OTLP 与 Prometheus 导出器。 |
| 运维 | Admin CLI 与 TUI、离线存储检查、Web 与桌面 Dashboard、MCP 服务和 AI SRE。 |

具体能力是否可用取决于 Cargo feature、运行时配置以及所选的存储后端或拓扑。[能力矩阵](https://rocketmqrust.com/zh-CN/docs/overview/capability-matrix)列出了每项能力的前提条件。

## 🚢 部署

| 方式 | 说明 | 入口 |
|------|------|------|
| 源码构建 | 在 Linux、Windows 或 macOS 上使用 Cargo 构建服务二进制。 | [安装](https://rocketmqrust.com/zh-CN/docs/getting-started/installation) |
| 容器镜像 | 每个版本发布 13 个 `linux/amd64` 镜像，Docker Hub 上的镜像带有 Cosign 签名和 CycloneDX SBOM 证明。 | [容器](https://rocketmqrust.com/zh-CN/docs/deployment/containers) |
| Kubernetes | [distribution](./distribution) 目录下的 Helm chart 和 Kustomize 清单。 | [Kubernetes](https://rocketmqrust.com/zh-CN/docs/deployment/kubernetes) |
| 高可用 | 主副本或由 Controller 管理的 Broker 组。 | [高可用](https://rocketmqrust.com/zh-CN/docs/deployment/high-availability) |

发布镜像在两个镜像仓库中使用相同的组件名：

```text
docker.io/mxsm/rocketmq-rust-<component>:<version>
ghcr.io/mxsm/rocketmq-rust/<component>:<version>
```

组件包括 `namesrv`、`broker`、`controller`、`proxy`、`mcp`、`dashboard-web-backend`、`dashboard-web-frontend`、`sre-control-plane`、`sre-connector`、`sre-executor`、`sre-execution-agent`、`sre-probe` 和 `sre-ui`。镜像不包含部署配置：核心服务读取 `/etc/rocketmq/<component>.toml`，并将数据保存在 `/var/lib/rocketmq` 下，两者都需要挂载。详见[发布与镜像指南](rocketmq-doc/en/releasing.md)。

## 🧪 构建与校验

Cargo 按包名选择包，因此 `rocketmq-client` 目录对应的包名是 `rocketmq-client-rust`。

| 任务 | 命令 |
|------|------|
| 检查单个 crate | `cargo check -p rocketmq-broker` |
| 测试单个 crate | `cargo test -p rocketmq-client-rust` |
| 运行单个测试 | `cargo test -p rocketmq-namesrv <test_name>` |
| 检查单个 crate 的格式 | `cargo fmt -p rocketmq-broker -- --check` |
| 对单个 crate 运行 lint | `cargo clippy -p rocketmq-broker --no-deps -- -D warnings` |
| 构建工作区 | `cargo build --workspace` |
| 测试工作区 | `cargo test --workspace` |
| 构建本地 API 文档 | `cargo doc --workspace --no-deps` |

完整的工作区构建还会编译 RocksDB 和 Proxy 的 protobuf 绑定，因此额外需要 Clang（libclang）和 `protoc`，后者可以位于 `PATH` 中，也可以通过 `PROTOC` 环境变量指定。`rocketmq-example/`、`rocketmq-dashboard/`、`rocketmq-ai/`、`rocketmq-website/` 和 `fuzz/` 下的独立项目需要在各自目录中构建和校验。

## 🤝 贡献

我们欢迎社区贡献！无论是修复错误、添加功能、改进文档还是分享想法，您的输入都很有价值。

### 如何贡献

1. **选择**一个[未关闭的 issue](https://github.com/mxsm/rocketmq-rust/issues)，或通过 [issue 模板](https://github.com/mxsm/rocketmq-rust/issues/new/choose)新建一个；带有 [good first issue](https://github.com/mxsm/rocketmq-rust/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) 标签的 issue 适合新贡献者
2. **Fork** 仓库，并基于 `main` 创建分支
3. **修改**时一次只解决一个问题，补充或更新测试，并对涉及的 crate 运行[校验命令](#-构建与校验)
4. **记录**用户可感知的变更：在 [CHANGELOG.md](CHANGELOG.md) 的 **Unreleased** 小节中添加条目
5. **提交**指向 `main` 的 Pull Request 并关联 issue，标题形如 `[ISSUE #1234]📝Clarify consumer offset completion`

### 贡献指南

- 遵循 Rust 最佳实践以及 [AGENTS.md](AGENTS.md) 中的工程规则，这些规则同时适用于贡献者和 AI 编码代理
- 为新功能添加测试
- 根据需要更新文档
- 保持 Pull Request 聚焦，不改动无关代码
- 遵守[行为准则](CODE_OF_CONDUCT.md)

详细指南请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 和我们的[贡献指南](https://rocketmqrust.com/zh-CN/docs/contributing/overview)。

### 仓库活动

![Repository Activity](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

## ❓ 常见问题

<details>
<summary><b>RocketMQ-Rust 是否生产就绪？</b></summary>

`1.0.0` 是第一个稳定版本。各服务提供健康探针、优雅停机、安全 profile 和可观测性，但是否生产就绪还取决于您的工作负载、拓扑和故障模型。上线前请验证业务依赖的消息链路，并逐项核对[生产部署自查清单](https://rocketmqrust.com/zh-CN/docs/deployment/production-checklist)。
</details>

<details>
<summary><b>是否与 Apache RocketMQ 兼容？</b></summary>

RocketMQ-Rust 实现了 RocketMQ remoting 协议，并通过 Proxy 实现了 gRPC `MessagingService`，以 Apache RocketMQ 5.5.0 作为对比基线。兼容性按具体接口面定义，而不是笼统的保证：Controller 使用自身的 OpenRaft 协议，不能加入 Java Controller 的仲裁组；不支持 DLedger；Java Broker 的数据目录也不能直接当作存储使用。详见[协议与兼容性参考](https://rocketmqrust.com/zh-CN/docs/reference/protocol-compatibility)。
</details>

<details>
<summary><b>最低支持的 Rust 版本（MSRV）是什么？</b></summary>

Rust 1.95.0。工作区中的每个清单都将其声明为 `rust-version`，`rust-toolchain.toml` 也固定使用同一 stable 版本进行构建。
</details>

<details>
<summary><b>性能与 Java RocketMQ 相比如何？</b></summary>

项目没有发布与 Java 版本的直接对比数据，因为吞吐量取决于硬件、存储、消息大小以及刷盘和复制策略。各 crate 中的 Criterion 基准测试（`cargo bench -p <package>`）用于跟踪单个组件的性能回退。容量规划请以自己的工作负载实测为准，并参考[容量规划与性能分析](https://rocketmqrust.com/zh-CN/docs/operations/capacity-performance)。
</details>

<details>
<summary><b>可以与现有的 RocketMQ 部署一起使用吗？</b></summary>

可以，边界在客户端一侧。Rust 应用可以使用 `rocketmq-client-rust` 通过 remoting 协议访问现有的 NameServer 和 Broker，gRPC 客户端可以通过 Proxy 接入。请针对实际使用的服务端版本、ACL 和 TLS 配置验证所依赖的操作。Rust 服务与 Java 服务不能共用 Controller 仲裁组或数据目录。
</details>

<details>
<summary><b>如何从 Java RocketMQ 迁移到 RocketMQ-Rust？</b></summary>

请先确定迁移边界，因为每种边界对应不同的操作：

1. 保留现有集群，将应用迁移到 Rust 客户端 SDK
2. 部署独立的 Rust 集群，并逐步切换流量
3. 通过显式的回放或桥接迁移历史数据，而不是复用 Java 数据目录

Broker 可以加载已有的 Java `broker.properties` 文件，并报告每个配置项的转换结果。详细步骤请参阅[迁移指南](https://rocketmqrust.com/zh-CN/docs/migration/java-to-rust)。
</details>

## 👥 社区 & 支持

- **💬 讨论**：[GitHub Discussions](https://github.com/mxsm/rocketmq-rust/discussions) - 提问和分享想法
- **🐛 问题**：[GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues) - 报告错误或请求功能
- **📧 联系**：联系 [mxsm@apache.org](mailto:mxsm@apache.org)

### 贡献者

感谢所有贡献者！🙏

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" alt="RocketMQ-Rust 贡献者"/>
</a>

## 📄 许可证

RocketMQ-Rust 采用 **Apache License 2.0** 许可证。

请参阅 [LICENSE-APACHE](LICENSE-APACHE) 和 [NOTICE](NOTICE)，或访问 <http://www.apache.org/licenses/LICENSE-2.0>。

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
