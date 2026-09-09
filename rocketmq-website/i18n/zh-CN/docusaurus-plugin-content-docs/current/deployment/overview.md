---
title: "选择部署拓扑"
---

应按需要支持的故障模型、客户端入口和运维责任选择部署方式。最小学习系统与生产 HA 系统共享概念，但地址、存储、身份和恢复流程不同。

## 从必需组件开始

普通 Rust 客户端访问需要 NameServer 发现，以及具有可用存储和主题/消费者组配置的 Broker。客户端必须同时访问发现端点和 Broker 公布端点。Controller、Proxy、Dashboard 和 AI 产品属于额外选择。

| 拓扑 | 需要增加的内容 | 提供的能力 | 应评估的边界 |
| --- | --- | --- | --- |
| 本地单 Broker | 一个 NameServer、LocalFile Broker 和客户端 | 完整学习与开发链路 | 无副本或自动故障转移 |
| 多个独立 Broker | 独立 Broker 身份与存储 | 更多消息放置与队列容量 | 增加 Broker 不会自动形成副本 |
| 默认主副本 HA | 匹配的主副本配置与 HA 连通性 | 按所选角色/确认策略复制 | 实际副本进度与可容忍故障窗口 |
| Controller 管理 HA | Rust Controller quorum 与相应 Broker | 协调角色与副本状态管理 | quorum、写入权、租约和持久成员关系 |
| Proxy Cluster | Proxy 加已有集群 | gRPC 及可选 remoting 入口 | 后端可达性、安全和有界会话状态 |
| Proxy Local | Proxy 持有嵌入式 Broker 组合 | 合并协议入口与本地消息服务 | 共享进程故障与生命周期 |
| Kubernetes | 镜像、存储、身份、放置和编排 | 可重复的进程放置与生命周期控制 | 编排本身不会创造消息持久性 |

第一种拓扑参见[本地源码搭建](../getting-started/local-source.md)和[快速开始](../getting-started/quick-start.md)，其中主题、组和客户端应用已经配套。其他拓扑需要先理解其故障与所有权边界。

## 地址属于拓扑设计

分别管理监听、公布和发现。监听器可以成功绑定，却公布客户端不可达的地址。宿主机回环、容器回环和 Pod 地址属于不同网络位置。

列出 NameServer remoting、Broker 普通/fast remoting 与 HA、Controller remoting/Raft，以及可选 Proxy、探针或遥测端点。为所选组合配置实际使用的监听器，仅开放 NameServer 端口并不足够。

复制部署中的每个成员需要独立数据路径和稳定身份。同一机器上的副本可以演示协议行为，但不能容忍整机丢失。备份策略还需要定义恢复边界，副本可用性不能替代备份恢复。

## 源码与部署资产

| 需求 | 当前源码入口 |
| --- | --- |
| 单机 Rust 搭建 | [本地教程配置](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/first-message) |
| 主副本配置示例 | [Broker 发行配置](https://github.com/mxsm/rocketmq-rust/tree/main/distribution/config/broker) |
| Controller 成员、存储与启动 | [Controller 指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/README.md) |
| Proxy Local/Cluster 与接入 | [Proxy 指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/README.md) |
| 核心服务容器组装 | [核心服务 Dockerfile](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/container/core-service.Dockerfile) |
| 核心 Helm 部署 profile | [rocketmq-rust-core chart](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/helm/rocketmq-rust-core/README.md) |
| 更广的 Kubernetes 集成资产 | [Kubernetes 资产指南](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/kubernetes/README.md) |

核心服务 Dockerfile 将指定二进制和配置复制到镜像中，不是通用源码构建命令。源码版本或 Dockerfile 不能证明存在匹配的公开镜像标签，应选择实际部署发行版提供的资产。

仓库包含两条不同的 Helm 路径。`rocketmq-rust-core` 提供开发、默认 HA、Controller HA 和 Proxy TLS 等核心服务 profile。范围更广的 `rocketmq-rust` 集成 chart 与 Kubernetes base 还包含 MCP 和发行状态流程。它们的开发 profile、安全输入和镜像假设不同，不能仅按名称混用 values。

已提交 Kubernetes base 使用本地镜像 fixture，文档明确要求提供预期镜像/发行输入后才能部署。渲染 YAML 只能证明渲染配置的情况，不能证明 Pod、PVC 恢复或故障转移成功。

## 安全与配置所有权

本地教程的不安全开发 profile 要求回环监听。共享网络部署需要配置所选服务的真实认证、授权和传输。

进程 bootstrap 要求、TLS 接线和资源权限应分别理解。编译 TLS 代码不会配置监听器或客户端信任；启用认证却未配置内部客户端身份，可能使服务间通信失败。

使用各产品自己的配置加载器和部署指南。部署配置存储 secret 引用，secret 值存储在所选密钥系统或挂载输入中。若凭证或证书仅在启动时加载，轮换需要执行相应重启流程。

## 就绪与关闭

就绪应描述组件提供流量服务所需的依赖。Broker 进程可能已存活，但存储恢复、注册或角色获取尚未完成；Controller 也可能已经监听，但尚无可用 quorum 或已应用状态。

`ServiceLifecycle` 协调就绪/存活和统一关闭截止时间。应在编排终止预算内为组件清理留足时间，并在正常重启时保留状态目录。[运行时设计](../architecture/runtime.md)解释关闭报告能证明和不能证明的内容。

核心 Helm chart 中，NameServer/Broker/Controller StatefulSet 使用 `OnDelete` 更新，Proxy 使用自己的 Deployment 策略。更新配置校验字段，不会自动重启所有有状态服务。修改 Controller peer 列表，也不等于执行在线 Raft 成员变更。

## 有明确目的地扩展系统

核心消息链路可用后再接入可观测性，然后通过[生态总览](../ecosystem/overview.md)选择所需管理或 AI 产品。各产品保持独立存储、身份和入口。

[能力矩阵](../overview/capability-matrix.md)列出 feature/模式条件，[存储设计](../architecture/storage.md)解释持久性，[首次诊断](../operations/first-diagnosis.md)提供初次运行检查。

本总览用于选择路径和说明条件，不表示本地单 Broker 教程已经验证 HA、Kubernetes 或生产安全场景。
