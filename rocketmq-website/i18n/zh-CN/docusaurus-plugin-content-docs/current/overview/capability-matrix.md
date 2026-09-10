---
title: "能力与实现边界"
---

本页帮助你选择系统需要的组件和配置。内容描述当前源码，不是对生产适用性的统一认证，也不代表与 Apache RocketMQ 完全功能对等。

## 把能力理解为一组条件

“已实现”表示存在具体源码路径；“已启用”还需要正确的编译 feature、运行设置、凭据和后端；“已演练”针对某个测试或部署场景；“已发布”表示发行物中包含该能力。这些结论不能互相替代。

仓库 `v1-capability-manifest.json` 记录 1.0.0 核心范围，包含按 profile 划分的实现和证据字段。`component` 或 `interop` 状态适用于其引用的场景与 profile，不能证明其他部署具有相同行为。该文件也不是 Dashboard 和 AI 产品的完整能力目录。

## 核心服务与客户端

| 能力 | 当前源码入口 | 条件与限制 |
| --- | --- | --- |
| 主题 路由与 Broker 发现 | NameServer 注册、过期清理、路由快照和查询 | 需要存活 Broker 注册有效 主题 元数据；NameServer 监听成功不会自动生成路由 |
| 普通消息存储与投递 | Broker 处理器和 LocalFile 存储 | 受角色、权限、主题/组配置及磁盘状态影响 |
| 生产者 发送 | 等待结果、回调、单向、批量、选队列 API | 返回保证不同，单向发送没有可检查的 Broker 确认 |
| Push 消费 | 客户端监听器、重平衡与拉取/长轮询调度 | 业务回调不意味着 Broker 主动建立未请求的投递连接 |
| LitePull 消费 | 显式轮询、订阅/分配和偏移量管理 | 业务处理与提交进度是两个操作 |
| Classic Pull 兼容 | 已弃用 facade，保留 runtime 支持的兼容实现 | detached 构造不能执行已初始化运行操作；新应用优先 LitePull |
| POP | 请求模式、receipt、不可见时间和 ACK | 需要对应 主题/Group 请求模式与匹配的 Broker 行为 |
| 事务、顺序、过滤、延迟与召回 | 客户端 API 及对应 Broker 处理器 | 各有 主题、组、过滤或定时条件，不能仅由 API 符号推断可用 |
| Admin API | Client 与 Admin Core 的查询/变更入口 | Cargo feature 提供接口不等于获得运行权限 |

第一条消息教程有意选择普通消息、LocalFile 主节点和 LitePull。增加高级消息模型前，先明确它的处理、重试和确认语义。

## 存储、复制与接入

| 领域 | 选择 | 边界 |
| --- | --- | --- |
| 本地存储 | Broker/Store 默认选择本地文件 | 主日志持久性与派生状态可见性分开 |
| RocksDB | Broker `rocksdb_store` 和匹配的存储配置 | 遵循具体后端的日志/派生结构及恢复契约，不是可任意在线替换的引擎 |
| 分层存储 | 可选 Tiered 集成 | 远端或派生进度不能提升主写入确认保证 |
| 默认 HA | 配置的主副本部署 | 副本确认策略与实际故障模型决定保证 |
| Controller HA | Rust Controller 协调和兼容 Broker 配置 | 不假定支持 Java Controller/JRaft/DLedger 混合成员或内部协议 |
| Proxy Cluster | 面向远端服务的集群后端适配 | 配置下游发现、身份、协议及资源限制 |
| Proxy Local | 进程内后端适配 | 嵌入式所有权和关闭区别于远端连接 |
| NameServer 内嵌 Controller | `embedded-controller` feature 加 `enableControllerInNamesrv` | 默认 NameServer 构建不含 Controller 依赖，仅设置运行参数无法启用 |
| 传输安全 | 对应 Transport feature 与端点配置 | 安全 bootstrap 检查与实际 TLS 监听/客户端接线不同 |

Client crate 自身没有 `tls` feature。需要 TLS 的应用必须在依赖图中启用传输层 TLS 实现，并配置对应连接。同样，启用观测 feature 不等于已经配置 exporter。

## 运维产品

| 产品 | 用途 | 独立边界 |
| --- | --- | --- |
| Web Dashboard | 浏览器界面与服务端后端 | 前后端构建、认证和部署 |
| GPUI / Tauri Dashboard | 原生桌面管理 | 原生依赖、平台范围和打包 |
| MCP | 只读集群诊断 | stdio/HTTP、身份和允许的查询工具 |
| MCP Control | 独立配置的受控变更工具 | 显式启用实现、操作注册、授权和审计 |
| AI SRE | 契约、连接器、模型交互和 SRE 流程 | 产品阶段和已注册执行路径；计划本身不授予集群变更权限 |

这些产品具有独立 manifest 和指南。尤其不能将 SRE 路线图中的宽泛描述直接翻译为所有执行器当前都已启用。

## 选择部署时记录什么

记录所用源码/发行版、产品、编译 feature、存储与接入模式、认证设置，以及业务依赖的故障行为，再选择匹配指南。即使公开方法名不变，上述任一条件变化也可能使原来的运维假设失效。

## 来源与下一步

- [核心能力清单](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/v1-capability-manifest.json)与[核心发行范围](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json)。
- [Broker](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/README.md)、[Client](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md)、[Controller](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/README.md)、[Proxy](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/README.md)。
- [本地搭建](../getting-started/local-source.md)与[投递和重试](../guides/delivery-and-retry.md)。
