---
title: "RocketMQ-Rust 简介"
---

RocketMQ-Rust 使用 Rust 实现消息服务和客户端 API。应用将消息发送给 Broker，通过 NameServer 查询路由，再按适合业务处理的消费模型读取消息。仓库还包含 Proxy、Controller、存储实现、管理工具、Dashboard 和 AI 运维产品。

本文档由 RocketMQ-Rust 项目维护。仓库中的发行身份是独立社区发行，不是 Apache 软件基金会的官方发行。与 Apache RocketMQ 的兼容性按具体协议和行为边界说明，不代表所有上游组件都可以直接互换。

## 从一条完整消息链路开始

第一次本地搭建使用一个 NameServer、一个采用本地文件存储的 Broker，以及 Rust 生产者/LitePull 消费者。Controller、Proxy、Dashboard 和 AI 服务可以按需增加，不是首次收发的前置条件。

1. 阅读[安装说明](getting-started/installation.md)，确定源码工具链和构建目标。
2. 按[本地源码搭建](getting-started/local-source.md)启动 Rust 服务，明确地址和数据目录。
3. 完成[第一条消息教程](getting-started/quick-start.md)，创建 主题 和 消费者组，发送、轮询并提交消费进度。
4. 阅读[投递与重试](guides/delivery-and-retry.md)，理解发送成功或偏移量提交与业务事务保证之间的关系。

## 各组件负责什么

| 组件 | 职责 | 何时需要 |
| --- | --- | --- |
| NameServer | Broker 注册、存活状态和 主题 路由查询 | 常规客户端发现路径 |
| Broker | 请求处理、主题/组元数据、消息存储与投递 | 每条消息链路 |
| Rust Client | 生产者、Push/LitePull/POP 消费和可选管理能力 | Rust 应用访问集群 |
| Proxy | 协议接入以及集群或进程内 Local 后端适配 | 需要对应接入模式的部署 |
| Controller | 所选 HA 拓扑中的 Rust Controller 协调 | Controller 管理的复制 |
| Admin CLI 与存储检查工具 | 集群管理和明确的离线操作 | 开发、诊断和维护 |
| Dashboard | 集群操作的 Web 或原生界面 | 交互式管理 |
| MCP、MCP Control、SRE | 只读诊断、独立受控变更和 SRE 流程 | 具有各自边界的可选运维产品 |

系统还在库层面拆分职责：Model 管理消息领域类型，Protocol 管理协议类型，Transport 负责网络，Runtime 管理后台工作所有权，Store API 定义存储契约。crate 不一定对应独立进程。[架构总览](architecture/overview.md)解释模块与运行组件之间的关系。

## 版本与能力范围

这些 **1.0.0 开发版** 页面描述当前源码。根包版本为 1.0.0，工具链为 Rust 1.95.0；任一数值都不能证明已经发布同版本下载包。发行物以 [GitHub releases](https://github.com/mxsm/rocketmq-rust/releases) 为准，同一发行版的配置和 API 应配套使用。

[能力矩阵](overview/capability-matrix.md)分别说明实现、编译 feature、运行配置和部署限制。存在请求处理器，不代表每种存储后端或拓扑都具有相同语义。

## 选择消费模型

应用需要显式轮询和提交进度时，从 [LitePull](consumer/pull-consumer.md) 开始；希望客户端将消息分派给业务监听器时，使用 [Push 消费](consumer/push-consumer.md)。Push 描述应用侧回调体验，其内部包含客户端拉取和长轮询。POP 使用 receipt 与 ACK，需要与基于偏移量的消费分别理解。

迁移已有 Classic Pull 应用前，先核对当前兼容 API 的行为。该 facade 已弃用，但仍存在由 runtime builder 创建的可运行路径；detached 构造并不提供已经初始化的运行消费者。

## 源码参考

- [Workspace 清单](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml)与[发行身份](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/release-identity.json)。
- [客户端 API 与运行时所有权](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md)。
