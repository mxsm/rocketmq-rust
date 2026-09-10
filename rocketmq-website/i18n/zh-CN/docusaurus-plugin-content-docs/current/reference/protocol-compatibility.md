---
title: "协议与兼容性参考"
---

# 协议与兼容性参考

兼容性描述两个端点、版本、配置和操作之间的关系。请求码相同或普通消息发送成功，不能证明所有客户端 API、管理命令、共识协议或磁盘格式均兼容。本页用于在选择部署或迁移路径前，明确应用实际依赖的接口范围。

## 选择正确端点

| 端点 | 协议接口 | 典型操作 | 边界 |
| --- | --- | --- | --- |
| NameServer remoting 监听器，通常为 `9876` | RocketMQ remoting | Broker 注册及路由/KV 查询 | 提供路由服务，不是 Proxy 的 gRPC `MessagingService`。 |
| Broker remoting 监听器，通常为 `10911`；快速端口通常为 `10909` | 由已注册 Broker 处理器处理的 RocketMQ remoting | 发送、拉取/POP、偏移量、元数据和管理操作 | 请求枚举已有定义，不代表所选处理器/模式一定处理该请求。 |
| Broker HA 监听器，通常为 `10912` | 所选模式的 HA 复制路径 | 主日志复制与进度 | 不是客户端发送端口，也不是可任意互换的 Java/Rust HA 契约。 |
| Proxy gRPC 监听器，通常为 `8081` | `apache.rocketmq.v2.MessagingService` | 路由、发送、分配、接收/ACK、拉取/偏移量、事务及遥测方法 | 要求 API 模型、客户端元数据、所选后端及操作支持相匹配。 |
| 可选 Proxy remoting 监听器，通常为 `8080` | Proxy remoting 适配器 | 经 Proxy 执行受支持的 remoting 操作 | 默认禁用，与 gRPC 及直接连接 Broker 不同。 |
| Controller remoting 监听器 | 面向 Broker 的控制与管理接口 | Controller 元数据、角色/选举及同步状态操作 | 与 Controller 内部 Raft 传输独立。 |
| Controller Raft 端点 | Rust OpenRaft gRPC | 已配置 Rust Controller 节点之间的共识 | 不提供 Java JRaft/DLedger 成员关系或持久化状态兼容性。 |

这些是常用默认值，不是服务发现说明；应检查实际配置与公布地址。Controller 示例有意显式配置独立的 remoting 和 Raft 端口。具体见[服务配置](service-configuration.md)与[部署总览](../deployment/overview.md)。

## remoting 线协议契约

帧包含总长度、序列化类型与头部长度组合字段、头部字节及可选消息体。组合字段的高 8 位标识序列化类型，低 24 位保存头部长度。JSON 与 RocketMQ 二进制头部编码不同。数值请求/响应码、关联字段、标志、扩展键名、字段宽度及消息体编解码，都是兼容性接口。

`RemotingCommandFactory` 持有不可变的版本和序列化默认值。初始化应用默认值不会追溯修改已构造的工厂。运行时语言标记 `RUST` 是元数据，不代表另一套分帧协议。消息压缩、CRC 处理、v1/v2 主题编码及消息体/属性解码也必须与所选路径匹配，仅头部兼容并不足够。

具体见[请求码](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/src/code/request_code.rs)、[响应码](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/src/code/response_code.rs)与[传输层设计](../architecture/protocol-transport.md)。不支持的请求和无效编码应产生对应的明确结果；无限重连不会使不支持的协议变为受支持。

## gRPC 契约与业务状态

仓库的[服务模型](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/proto/service.proto)定义 `QueryRoute`、`SendMessage`、`QueryAssignment`、流式 `ReceiveMessage`/`PullMessage`、确认和偏移量 API、事务完成、客户端遥测、召回及 Lite 订阅方法。模型定义消息与 RPC 签名，具体请求是否可执行则由 Proxy 后端与所选 Broker 能力决定。

即使 gRPC 传输调用成功，仍应检查响应中的 RocketMQ 状态及逐项状态。特别是，`SendMessageResponse` 同时包含总体状态与重复结果条目。流式方法既可以传递状态记录，也可以传递消息；若将每个收到的条目都当作业务消息，会丢失失败信息。

回执句柄与不可见时间属于 POP 式投递，不能与经典拉取的队列偏移量互换。结束事务请求被成功接受，不能证明全部事务恢复/检查行为已完成。客户端身份元数据、认证、TLS、消息体限制、批量及 gzip 接纳条件均是独立请求约束。应结合具体 [Proxy 示例](../deployment/proxy.md)与[错误参考](errors.md)理解。

## 按接口范围判断兼容性

| 接口范围 | 保留的可识别概念 | 需要独立核对的内容 |
| --- | --- | --- |
| Rust 源码 API | 生产者、消费者、消息、路由和结果等公共概念 | 当前 crate 导入、feature 标志、方法签名及运行时注入。线协议兼容不能使私有 Rust 模块导入继续编译。 |
| 普通客户端 remoting | 已实现路径中的 RocketMQ 分帧、强类型头部、响应码和消息语义 | 精确客户端/服务端版本、序列化、压缩、TLS/ACL、主题类型及操作。 |
| 高级消息语义 | 事务、顺序、过滤、延迟/召回、POP 与 Lite 概念 | Broker 配置、当前操作支持、消费者组/请求模式、重试与提交行为。 |
| 管理 API | 熟悉的主题、消费者组、配置和偏移量操作 | 实际 CLI 层级、参数、已注册命令、权限、部分结果处理，以及运行时变更与持久化变更的区别。 |
| 配置 | 部分 Java 兼容字段名及显式 Broker properties 转换 | 规范 TOML 配置段、类型、默认值、被拒绝字段和转换报告。复制的 Java 文件不会自动成为有效 Rust 配置。 |
| 主存储与派生存储 | CommitLog、队列和索引概念 | 段大小、记录版本、派生后端、源纪元、定时/POP/压缩/分层元数据及恢复所有权。 |
| 默认 HA | 主从副本概念 | 精确复制实现与确认/故障场景，不能仅凭概念推断混合实现已完成验证。 |
| Controller HA | 主节点选举与同步状态的功能结果 | Rust 写权限/租约契约、OpenRaft 成员关系、内部传输与持久化。Java 内部协议与混合法定多数派兼容性不在范围内。 |
| 运维产品 | 集群和资源词汇 | Dashboard、MCP、SRE 的 API/认证/状态模型拥有独立版本，不属于核心消息发行范围。 |

SQL 过滤同时依赖客户端选择器与 Broker 属性过滤支持。LitePull 的本地偏移量提交、远端提交、Broker 持久化及业务副作用完成，是不同阶段。复制中的请求接纳、本地持久化进度与副本确认也是不同阶段。即使两种实现接受相同字节，这些仍属于独立行为条件。

## 已记录的核心范围与明确排除项

[1.0 能力清单](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/v1-capability-manifest.json)将 Apache RocketMQ `5.5.0` 记录为 Java 对照基线。每项能力有独立的 profile、兼容模式、实现状态、证据状态及测试引用。该声明不代表与所有 Java 版本或部署全面等价。

清单明确排除 OpenMessaging、BrokerContainer 运行时/管理操作、DLedger CommitLog 及 Java Controller 内部协议。特别是，Controller 等价性通过纯 Rust 功能结果描述，而非 Java DLedger、JRaft、AutoSwitch 线协议或混合法定多数派兼容性。Broker 会拒绝 DLedger 配置，不会静默改选其他 HA 实现。

[核心发行范围](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json)对包分类并列出核心服务。它将 Dashboard、MCP、SRE 产品排除在核心发行范围之外；这些产品仍然存在，并需要独立文档与验证。标记为延期且没有证据的长期安全/并发及容量验证项，不能因为存在短期组件测试就被描述为已通过。

## 按实际覆盖范围理解证据

| 仓库证据 | 能够说明什么 | 单独不能说明什么 |
| --- | --- | --- |
| [头部兼容测试](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/tests/request_header_java_compatibility.rs) | 该测试断言的头部场景与固定样例 | 所有已注册请求与真实 Java 服务端的互操作。 |
| [remoting 业务兼容测试](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/tests/remoting_command_java_business_compatibility.rs) | 特定扩展字段、语言码和键长度行为 | 完整端到端消息持久性或 HA 故障切换。 |
| [消息编解码测试](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/tests/message_codec_compatibility.rs) | 固定帧解码、截断、CRC/压缩及 v2 主题场景 | 任意历史存储目录或完整数据迁移。 |
| [客户端批量管理 API 测试](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/tests/batch_admin_delete.rs) | 在对应 feature 选择下公共强类型 API 可用 | 真实远端服务已经接受批次，或保持了授权语义。 |
| 已记录的 `component` 或 `interop` 能力状态 | 引用证据所描述的场景 | 文档变更期间重新执行了相同场景，或覆盖未列出的 profile。 |

评估自身兼容需求时，应记录两端版本、端点与序列化器、构建 features、后端/模式、主题/消费者组配置、凭据策略及观察到的操作结果。需求依赖故障或重试行为时，还应覆盖这些行为。例如，“在已配置的 Rust LocalFile 部署上完成普通发送与 LitePull”比“兼容所有 RocketMQ 客户端”范围更清晰，也更有用。

## 存储与回退决策

不要让不同后端或旧二进制直接打开正在使用的数据目录来试探兼容性。应先停止拥有该目录的服务，保留可恢复副本，再使用对应[升级与回退](../operations/upgrade-rollback.md)流程。离线降级检查仅报告其实际检查的持久化格式，不能证明 Controller 成员兼容性或业务状态正确性。

修改 `storeType` 不等于迁移。RocksDB 派生结构仍关联主日志及其记录进度，分层状态属于独立辅助路径。Controller 内部状态目录不是 Java 共识快照导入格式。需要旧布局的回退，可能要求兼容备份或受支持的迁移，不能仅替换可执行文件。

这些决策背后的所有权与恢复契约见[存储后端](../architecture/storage-backends.md)、[HA 与 Controller](../architecture/ha-controller.md)和[备份与恢复](../operations/backup-recovery.md)。
