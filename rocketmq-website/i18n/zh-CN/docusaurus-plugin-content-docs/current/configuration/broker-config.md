---
title: "Broker 配置参考"
---

# Broker 配置参考

Broker 启动时读取强类型配置，统一各配置段之间的字段归属，完成校验后再构造运行时。本页说明规范 TOML 键名及受支持的在线更新范围。可运行的本地集群见[本地源码搭建](../getting-started/local-source.md)，副本配置见[高可用部署](../deployment/high-availability.md)。

## 文件选择与优先级

1. `-c` / `--configFile` 指定配置文件。未指定时，如果 `$ROCKETMQ_HOME/conf/broker.toml` 存在则加载该文件，否则使用默认配置。为保证安装可复现，应明确设置 `ROCKETMQ_HOME`。
2. `--config-format toml|properties` 指定输入格式。未指定时，`.toml` 使用规范 TOML，`.conf` 和 `.properties` 使用 Java properties 转换器。扩展名无法确定格式时，需要显式指定。
3. NameServer 地址的优先级依次为显式 `-n` / `--namesrvAddr`、`NAMESRV_ADDR`、配置文件、默认值 `127.0.0.1:9876`。环境变量 `NAMESRV_ADDR` 即使为空，只要存在也会进入覆盖路径并解析为默认地址；若需保留文件中的值，应取消设置该变量。
4. 配置校验先于服务启动。`-p` / `--printConfigItem` 和 `-m` / `--printImportantConfig` 打印解析后的配置并退出，不启动监听器。

打印的属性表使用扁平的管理接口名称，用于检查配置，不能原样复制为规范 TOML。Java properties 在内存中转换，同时写出转换报告；应检查报告中被拒绝或转换的字段。`--conversion-report` 指定报告路径，否则将输入扩展名替换为 `.conversion.json`。报告写入失败会导致加载失败。

例如，构建 Broker 后，在仓库根目录执行：

```powershell
$env:ROCKETMQ_HOME = (Get-Location).Path
& .\target\debug\rocketmq-broker-rust.exe -c .\rocketmq-website\examples\first-message\broker.toml -p
```

## 规范配置段结构

```toml
[broker]
namesrvAddr = "127.0.0.1:9876"
brokerIp1 = "127.0.0.1"
listenPort = 10911
storePathRootDir = ".rocketmq-reference/broker"
autoCreateTopicEnable = false
autoCreateSubscriptionGroup = false

[broker.brokerIdentity]
brokerClusterName = "DocsCluster"
brokerName = "docs-reference-broker"
brokerId = 0

[broker.topicQueueConfig]
defaultTopicQueueNums = 4

[store]
storePathRootDir = ".rocketmq-reference/store"
brokerRole = "ASYNC_MASTER"
flushDiskType = "ASYNC_FLUSH"
mappedFileSizeCommitLog = 1073741824
mappedFileSizeConsumeQueue = 6000000
fileReservedTime = 72
deleteWhen = "04"
```

这是本地配置示例，不构成高可用拓扑。相对路径以进程工作目录为基准。`broker.storePathRootDir` 保存 Broker 元数据，`store.storePathRootDir` 保存消息存储数据。即使两者都默认指向用户主目录下的 `store`，它们仍是独立配置项；备份和恢复时应同时保留。

根配置接受 `broker`、`store`、`logging`、旧式 `logFilter` 和 `observability`。根配置、Broker 和 store 配置模型均拒绝未知字段。因此，将 Java 风格的 `brokerName` 放在 TOML 根层级是无效的。主监听端口应通过 `broker.listenPort` 配置：`broker.brokerServerConfig.listenPort` 由其派生，不能独立输入。同样，`store.enableControllerMode` 和 `store.duplicationEnable` 由 Broker 中的对应字段派生，不接受独立设置。

## Broker 身份、路由与请求接纳

表中路径均相对于 TOML 根层级。默认值指显式覆盖之前的启动配置。除在线更新一节另有说明外，应通过受控重启应用变更。

| 外部键名 | 类型 / 默认值 | 含义与约束 |
| --- | --- | --- |
| `broker.brokerIdentity.brokerClusterName` | string / `DefaultCluster` | 非空集群名称；同一集群的成员使用相同名称。 |
| `broker.brokerIdentity.brokerName` | string / 依次取 `HOSTNAME`、`COMPUTERNAME`、`DEFAULT_BROKER` | 非空 Broker 组名。同组副本共用名称，独立主节点使用不同名称。 |
| `broker.brokerIdentity.brokerId` | u64 / `0` | 非 Controller 模式下，主节点必须为 `0`，从节点必须为非零值。Controller 模式遵循其身份分配和纪元规则。 |
| `broker.namesrvAddr` | optional string / 通常为 `127.0.0.1:9876` | 以分号分隔的有效端点；受上述 CLI 和环境变量优先级影响。 |
| `broker.brokerIp1` | string / 检测本机地址，失败时回退到环回地址 | 向客户端公布的地址，应确保客户端网络可达。它与绑定地址不同。 |
| `broker.listenPort` | u32 / `10911` | 有效 TCP 端口，且 `listenPort - 2` 必须是有效快速端口；有效范围为 `3..=65535`。 |
| `broker.storePathRootDir` | string / 用户主目录 + `store` | 非空元数据根目录。修改此值不会迁移已有元数据。 |
| `broker.autoCreateTopicEnable` | bool / `true` | 启用自动创建主题；共享环境中显式创建主题更便于控制。 |
| `broker.autoCreateSubscriptionGroup` | bool / `true` | 启用自动创建订阅组。 |
| `broker.brokerPermission` | u32 / `6` | 读写权限位；在线更新要求至少包含读或写权限位。 |
| `broker.topicQueueConfig.defaultTopicQueueNums` | u32 / `8` | 创建主题时使用的默认队列数，不会自动调整已有主题的队列划分。 |
| `broker.enablePropertyFilter` | bool / `false` | 启用 Broker 端 SQL/属性过滤，仅配置客户端选择器并不足够。 |
| `broker.flushConsumerOffsetInterval` | u64 / `5000` ms | 消费偏移量持久化间隔，与客户端提交偏移量的间隔不同。 |
| `broker.registerNameServerPeriod` | u64 / `30000` ms | 注册周期。不同 NameServer 上的注册可见性可能不同。 |
| `broker.registerBrokerTimeoutMills` | i32 / `24000` ms | Broker 注册请求超时，外部键名保留 `Mills` 拼写。 |
| `broker.brokerFastFailurePendingMaxCount` | usize / `4096` | 待处理请求数量上限，必须为正数。 |
| `broker.brokerFastFailurePendingMaxBytes` | usize / `67108864` bytes | 待处理请求字节数上限，必须为正数；与单条消息大小限制不同。 |
| `broker.enableControllerMode` | bool / `false` | 启用基于 Controller 的写入权限管理；要求 `broker.controllerAddr` 有效且非空。 |
| `broker.controllerAddr` | string / 空 | Controller 端点，应使用部署中的 remoting 地址，而不是 Raft 地址。 |

兼容字段不代表性能能力。例如，`asyncSendEnable` 为兼容 Java 配置而保留，`sendRequestExecutorDetachedEnable` 会被忽略并产生启动警告。不能将 Java 线程池参数复制到文件中，再据此推算 Rust 的执行容量。

## 存储、磁盘与复制

| `store` 下的外部键名 | 类型 / 默认值 | 单位与运维含义 |
| --- | --- | --- |
| `storeType` | enum / `LocalFile` | 存储后端选择；可选后端需要相应构建 feature。修改选择项不等于迁移数据。 |
| `storePathRootDir` | string / 用户主目录 + `store` | 非空消息存储根目录。 |
| `storePathCommitLog` | optional string / 未设置 | 默认使用存储根目录下的 CommitLog 位置。自定义路径必须纳入备份与恢复。 |
| `mappedFileSizeCommitLog` | usize / `1073741824` | 每个 CommitLog 段的字节数，必须为正数。在已有存储上变更时，应按存储布局操作处理。 |
| `mappedFileSizeConsumeQueue` | usize / `6000000` | 每个 ConsumeQueue 文件的字节数，为 `300000 × 20`，并非 30 MB。必须为正数。 |
| `maxMessageSize` | i32 / `4194304` | 消息大小限制，单位为字节。客户端与 Broker 限制应协调；批量发送和协议开销还需要单独预留空间。 |
| `flushDiskType` | enum / `ASYNC_FLUSH` | 可选 `ASYNC_FLUSH` 或 `SYNC_FLUSH`。本地刷盘策略不能证明远端复制完成或业务处理完成。 |
| `flushIntervalCommitLog` | i32 / `500` | 周期性尝试刷写 CommitLog 的间隔，单位为毫秒。 |
| `flushCommitLogLeastPages` | i32 / `4` | 刷盘页数阈值。异步模式拒绝零值。 |
| `syncFlushTimeout` | u64 / `5000` | 等待同步刷盘条件的毫秒数。超时表示发送结果不确定，不能证明消息不存在。 |
| `fileReservedTime` | usize / `72` | 保留时长，单位为小时。磁盘压力与清理策略同样影响删除行为。 |
| `deleteWhen` | string / `"04"` | 定时清理小时表达式，在 TOML 中必须加引号。 |
| `diskMaxUsedSpaceRatio` | usize / `75` | 百分比阈值，必须低于强制清理阈值。 |
| `diskSpaceCleanForciblyRatio` | usize / `85` | 百分比阈值，必须低于警告阈值。 |
| `diskSpaceWarningLevelRatio` | usize / `90` | 磁盘警告百分比阈值。运行时还会将磁盘比例限制在支持的范围内。 |
| `cleanFileForciblyEnable` | bool / `true` | 允许强制清理；保留小时数本身不能保证最短保留时间。 |
| `messageIndexEnable` | bool / `true` | 启用消息索引。运行时修改不承诺重建历史索引。 |
| `brokerRole` | enum / `ASYNC_MASTER` | 可选 `ASYNC_MASTER`、`SYNC_MASTER`、`SLAVE`；应结合 Broker 身份与预期 HA 模式配置。 |
| `haListenAddress` | IP address / `0.0.0.0` | HA 绑定地址。 |
| `haListenPort` | usize / `10912` | 非零有效 TCP 端口，且不能与主 remoting 端口和快速端口冲突。 |
| `totalReplicas` | usize / `1` | 预期副本数量。 |
| `inSyncReplicas` | i32 / `1` | 同步副本数量配置。 |
| `minInSyncReplicas` | usize / `1` | 最小同步副本数；校验要求 `1 <= min <= inSync <= total`。 |
| `slaveTimeout` | usize / `3000` | 单位为毫秒；同步复制或更强副本要求使用该值时，必须为正数。 |
| `haMaxTimeSlaveNotCatchup` | usize / `15000` | 单位为毫秒；Controller 模式或自动 ISR 管理要求其为正数。 |

默认副本数并不构成复制服务。应使用完整的[高可用配置](../deployment/high-availability.md)、独立存储根目录、可达的 HA 地址及文档中说明的写入权限模型。DLedger 配置会被拒绝，包括兼容拼写 `enableDledgerCommitLog` 和 `enableDlegerCommitLog`；不能通过这些字段间接启用 Controller HA。

## 在线更新与持久化

普通 Broker 配置事务仅接受以下六个扁平属性名：

| 管理接口属性 | 运行时接受的值 | 规范 TOML 路径 |
| --- | --- | --- |
| `autoCreateTopicEnable` | `true` 或 `false` | `broker.autoCreateTopicEnable` |
| `autoCreateSubscriptionGroup` | `true` 或 `false` | `broker.autoCreateSubscriptionGroup` |
| `brokerPermission` | 含读写权限的规范整数 `2..=7` | `broker.brokerPermission` |
| `defaultTopicQueueNums` | 规范整数 `1..=128` | `broker.topicQueueConfig.defaultTopicQueueNums` |
| `messageIndexEnable` | `true` 或 `false` | `store.messageIndexEnable` |
| `traceTopicEnable` | `true` 或 `false` | `broker.traceTopicEnable` |

布尔值必须小写；`08` 等非规范整数会被拒绝。事务会校验候选配置，并移除没有变化的值。索引变更还要求消息存储提供运行时投影；缺少该能力时请求失败，不会仅修改一个无效标志。

成功响应报告 `applied=true` 和 **`persisted=false`**。若需重启后保留设置，必须另行更新部署管理的启动文件。运行时更新后的主题协调过程不会持久化 Broker 配置文件。此范围之外的已知字段需要重启，未知字段不受支持。CAS 请求还会检查正数 `expectedGeneration`，并在冲突时返回当前 generation。该值标识运行时配置快照，不是存储纪元。

日志过滤器更新使用独立的、受授权约束的路径，不属于上述六字段事务。启动日志配置依次取 CLI、环境输入、`logging.filter`、旧式 `logFilter`、回退值。运行时过滤器更新要求启用重载路径，并满足其认证与授权条件。安全材料与遥测配置分别见[安全部署](../deployment/security.md)和[可观测性配置](./observability.md)。

## 变更行为之前先检查配置

启动前先打印并检查候选文件。应将公布地址、元数据和消息存储根目录、监听与 HA 端口、角色与 ID、副本要求作为整体核对。对于运行中的服务，应区分实际生效的运行时配置和部署工具管理的文件。变更后检查受影响的行为：主题权限对应路由注册，索引设置对应索引查询，持久性设置对应实际发送与复制结果。

配置定义来源包括[原始配置模型](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/raw.rs)、[校验逻辑](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/sections.rs)、[Broker 默认值](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/broker_config.rs)、[存储默认值](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/config/message_store_config.rs)和[运行时事务](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/transaction.rs)。本参考列出主要运维字段；定时、压缩及后端专用配置应继续查阅对应配置模型。
