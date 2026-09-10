---
title: "从 Java RocketMQ 迁移"
---

# 从 Java RocketMQ 迁移

变更部署前，先确定迁移边界。替换应用客户端、引入 Rust Broker 集群、搬迁存储数据是不同操作。本文为已有 RocketMQ 工作负载提供实施流程，不宣称存在通用的 Java 进程或数据目录原地转换方案。

当前能力清单以 Java RocketMQ 5.5.0 为比较基准，并列出明确排除项。结合实际版本和工作负载阅读[协议兼容性](../reference/protocol-compatibility.md)。基准声明不能证明所有 Java SDK 版本、Controller 配置或高级消息模式均可互换。

## 选择迁移路径

| 路径 | 初始保留 | 首先改变 | 扩大流量前需要的依据 |
| --- | --- | --- | --- |
| Java 应用改为 Rust 应用 | 现有服务端点、主题、消费者组和业务契约 | SDK 集成、运行时所有权和结果处理 | 精确客户端/服务端组合的操作、序列化、ACL/TLS、重试和关闭行为 |
| Java 服务迁往独立 Rust 集群 | 应用负载模式和业务标识 | 新 NameServer/Broker 部署及运维工具 | 目标配置组合的拓扑、配置、功能语义、持久性、恢复和容量 |
| 历史数据或进度迁移 | 可恢复的源环境及已记录业务位置 | 明确的重放/桥接方案，或另行确认兼容的离线流程 | 所选方法的消息覆盖、重复、顺序、偏移量与恢复行为 |

评估期间分开源、目标集群的 NameServer 地址和存储根目录。使用已有 Broker 身份注册新实现可能改变在线客户端的路由。第二个进程不得与旧进程共享正在使用的存储目录。

## 1. 盘点应用契约

为每个应用记录下列具体输入，作为迁移工作表，无需引入文档审批流程。

| 领域 | 记录内容 |
| --- | --- |
| 端点 | Java SDK 版本、remoting 或 Proxy gRPC、NameServer 列表、Broker 广播地址、TLS/ACL 和命名空间行为 |
| 消息 | 主题和消息类型、负载编码/模式、键/标签/属性、大小分布和压缩 |
| 生产者 | 同步/异步/单向调用、超时预算、重试策略、队列选择、事务回查、延迟/召回 |
| 消费者 | 消费者组、集群/广播模式、Push/Pull/POP/Lite 模式、选择器、队列分配、重试/死信和初始位置规则 |
| 业务完成 | 幂等键、持久业务事务边界、偏移量/ACK 顺序、允许的重复和顺序范围 |
| 运维 | 峰值流量、保留时间、恢复目标、监控信号、管理脚本和维护责任 |

把故障行为纳入契约。发送后超时可能意味着结果未知，重放不能建立在“消息肯定未存储”的假设上。消息 ID 和队列偏移量是有用的传输标识，但不能替代应用定义的幂等键。

## 2. 将概念映射到实际 Rust 接口

| Java 侧概念 | Rust 迁移决策 |
| --- | --- |
| JVM 进程参数、堆和 GC 调优 | 选择 Rust 构建配置和原生依赖，按归属组件配置运行时、线程、准入及存储。JVM 参数没有直接对应的 Rust 含义。 |
| 扁平 Broker properties | 使用规范 TOML，或显式 Broker properties 转换器并检查报告。保留期望数值，不保留扁平布局假设。 |
| 生产者生命周期 | 向 builder 注入应用持有的 `Arc<ClientRuntime>`，启动门面、检查发送结果，再显式关闭门面和共享运行时。 |
| Push 监听器 | 选择并发或顺序处理，保持成功/重试语义、分配行为和业务事务边界。 |
| Classic Pull | 用运行时驱动的兼容 builder 保留显式队列/偏移量所有权，或明确改用 LitePull 的分配和轮询模型。 |
| 消费偏移量 | 区分下一读取位置、本地偏移量存储状态、远程提交和 Broker 持久化。 |
| SQL 选择器 | 除客户端选择器外，还需要启用 Broker 属性过滤。 |
| DLedger / Java Controller | 不复用 Java 共识成员、快照或内部协议。Rust Controller 使用自己的 OpenRaft 和 HA 写权限契约。 |
| mqadmin 脚本 | 将每次调用改为 Rust CLI 的领域和末级命令，核对参数、凭据、输出和部分失败行为。 |

可执行示例见 [Rust API 迁移](./rust-api.md)、[客户端配置](../configuration/client-config.md)及相应[生产者](../producer/overview.md)/[消费者](../consumer/overview.md)文章。类名相似不代表构造函数或回调签名相同。

## 3. 搭建隔离目标环境

先执行[本地源码搭建](../getting-started/local-source.md)和[第一条消息流程](../getting-started/quick-start.md)，再按目标拓扑选择[多节点部署](../deployment/multi-node.md)或 [HA 部署](../deployment/high-availability.md)。使用独立的 Broker 身份和数据路径。

逐段迁移配置：Broker 身份与监听、NameServer 发现、Broker 元数据根目录、消息存储根目录、保留/刷盘策略、主题/消费者组设置、安全和可观测性。规范 TOML 的监听配置不是可以随意嵌套的 Java 属性；[Broker 配置](../configuration/broker-config.md)定义了可接受段及派生字段。

测试已有 Java properties 文件时，可显式使用转换入口：

```bash
cargo run -p rocketmq-broker -- -c /path/to/broker.properties --config-format properties -p
```

将路径替换为单独的工作副本。转换会写出报告，配置打印在绑定服务端口前退出。检查被拒绝或转换的设置和打印出的有效值；解析成功不能证明部署健康。本文编写期间没有针对 Java 配置执行这一示意命令。

不要带入 DLedger 配置并期待自动回退到另一种 HA 模式，Broker 会拒绝这些设置。不要将 Java Controller 状态目录作为 Rust Controller 存储根目录。gRPC 应用应部署和配置 [Proxy](../deployment/proxy.md)，Broker remoting 端口不是 gRPC 端点。

## 4. 验证有代表性的消息路径

| 场景 | 观察内容 |
| --- | --- |
| 普通发送与消费 | 负载和属性、发送状态、分配队列、消费内容及业务完成 |
| 超时与重试 | 整体请求预算、未知结果、重复抑制和重试负载 |
| 再平衡与重启 | 撤销队列停止处理，新分配从预期进度恢复，有界关闭 |
| 过滤与顺序 | 重新分配情况下实际选择器和队列/顺序范围，而不只测试单生产者成功路径 |
| 使用中的事务/延迟/POP | 事务回查恢复、定时与召回竞争、不可见时间、ACK 和再次投递 |
| 需要保障的服务故障 | 所选刷盘/复制策略、故障切换写权限、恢复时间和重放范围 |

只运行与应用相关的场景，但不要将基本收发结果推广到未经测试的高级模式。消费者组积压为零时，外部业务处理仍可能失败；HTTP 或传输成功时，操作状态仍可能失败。

网站第一条消息示例已在本地 Rust NameServer/Broker 上完成五条发送和五条接收。这不构成 Java/Rust 互操作试验或数据迁移试验，仍需针对实际源、目标组合执行相应验证。

## 5. 有计划地迁移流量和进度

1. 选择有界的应用、租户或分区批次及回退路由，在目标端准备主题、消费者组、权限和可观测性。
2. 同一集群替换消费者实现时，记录已完成业务进度，停止旧所有者，再以预期消费者组和模式启动新所有者。已有消费者组偏移量可能优先于初始位置设置。
3. 迁往独立目标集群时，明确历史消息如何到达：业务源重放、应用桥接或其他明确支持的方法。在所需保留窗口内维持源端可读。
4. 将偏移量视为集群和队列专属位置，不要向另一队列复制一个数字就假设它标识同一消息。通过所选重放方法和业务标识映射进度。
5. 观察目标结果、积压和错误行为后扩大流量。若两个生产者或桥接程序并发写入，应明确重复和顺序处理；双写不是跨集群原子提交。
6. 满足应用重放/恢复窗口和运维义务后，再下线源端。

使用新消费者组进行对照会创建独立进度历史，并可能重放旧数据，应将业务效果导向隔离目标或实现幂等。同一组同时运行旧、新实现可能在它们之间分配队列，而不是提供完全相同的影子消息流。

## 6. 保留可操作的回退路径

客户端回退可以恢复旧可执行文件和端点配置，但不会撤销已经完成的业务效果或偏移量推进。集群回退需要处理只被新集群接受的写入，在丢弃目标或切回路由前完成对账或重放。

保留源配置、可恢复数据和上一版应用制品。若流程改变持久化布局，应遵循[升级与回退](../operations/upgrade-rollback.md)和[备份与恢复](../operations/backup-recovery.md)。仅替换二进制或修改 `storeType` 不构成存储转换。

切换后保留正常运维记录：精确版本、所选 feature/模式、配置差异、测试场景、观察到的限制和未完成迁移工作的责任人，使后续故障可诊断，同时不宣称超出依据的兼容性。

来源：[能力范围](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/v1-capability-manifest.json)、[Broker 入口](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/bin/broker_bootstrap_server.rs)、[客户端公共 API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/public_api.rs)、[HA 契约](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-api/src/ha_contract.rs)。
