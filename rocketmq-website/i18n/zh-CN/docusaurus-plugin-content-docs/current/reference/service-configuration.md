---
title: "NameServer、Controller 与 Proxy 配置"
---

# NameServer、Controller 与 Proxy 配置

这些服务采用不同的文件布局、CLI 拼写及运行时更新路径。字段名称相近，并不表示加载优先级或持久化行为相同。Broker 和存储字段见 [Broker 配置](../configuration/broker-config.md)，应用 builder 见[客户端配置](../configuration/client-config.md)。

## 选择并检查配置文件

| 服务二进制 | 文件选项 | 打印后退出选项 | 未显式指定文件 |
| --- | --- | --- | --- |
| `rocketmq-namesrv-rust` | `-c` / `--configFile` | `-p` / `--printConfigItem` | 使用 NameServer 默认值，再加载 `configStorePath` 下可能存在的持久化期望配置 |
| `rocketmq-controller-rust` | `-c` / `--config-file` | `-p` / `--print-config-item` | 使用 Controller 默认值 |
| `rocketmq-proxy-rust` | `-c` / `--config` | `--printConfig` | 使用与已编译模式对应的 Proxy 默认值 |

应使用网站[部署示例](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples)中的 TOML。显式指定的文件缺失或格式错误时，加载失败。这些服务不会继承 Broker 自动选择 `ROCKETMQ_HOME/conf/broker.toml` 或转换 Java properties 的行为。打印命令用于检查配置，不会验证端口绑定、证书握手、远端依赖或 Controller 法定多数派。

例如，构建对应二进制后，在仓库根目录执行：

```powershell
& .\target\debug\rocketmq-namesrv-rust.exe -c .\rocketmq-website\examples\first-message\namesrv.toml -p
& .\target\debug\rocketmq-controller-rust.exe -c .\rocketmq-website\examples\ha\controller-1.toml -p
& .\target\debug\rocketmq-proxy-rust.exe -c .\rocketmq-website\examples\proxy\cluster.toml --printConfig
```

## NameServer

### 启动顺序

NameServer 配置与传输层覆盖项使用根层级 camelCase 键名。启动时先加载所选文件或默认值，应用 `--rocketmqHome` 与 `--kvConfigPath`，加载文件中的传输层和 TLS 设置，然后读取 `configStorePath` 下可能存在的持久化期望 properties 文件。该持久化快照可以覆盖启动文件中的对应值。最后，`--listenPort` 和 `--bindAddress` 再覆盖已解析的监听设置。

因此，在曾经执行管理接口更新后，仅修改 TOML 可能看起来没有生效。应同时检查两个输入文件与实际配置。持久化 properties 文件和 `kvConfigPath` 不同：前者保存配置意图，后者保存 NameServer KV 数据。两者都不是完整实时路由表的副本。

```toml
rocketmqHome = ".rocketmq-reference"
kvConfigPath = ".rocketmq-reference/namesrv/kvConfig.json"
configStorePath = ".rocketmq-reference/namesrv/namesrv.properties"
listenPort = 9876
bindAddress = "127.0.0.1"
scanNotActiveBrokerInterval = 5000
needWaitForService = false
```

| 外部键名 | 类型 / 默认值 | 作用范围与单位 |
| --- | --- | --- |
| `rocketmqHome` | string / 依次取 `rocketmq.home.dir`、`ROCKETMQ_HOME`，否则为空 | 服务启动要求非空；打印模式会提前返回。 |
| `kvConfigPath` | string / 用户主目录 + `rocketmq-namesrv/kvConfig.json` | KV 元数据文件，每个进程应使用独立路径。 |
| `configStorePath` | string / 用户主目录 + `rocketmq-namesrv/rocketmq-namesrv.properties` | 启动时加载的持久化期望配置。 |
| `listenPort` | u32 / `9876` | TCP 监听端口，文件传输层覆盖项范围为 `1..=65535`。 |
| `bindAddress` | string / `0.0.0.0` | 监听绑定地址，本地示例使用环回地址。安全引导流程独立控制是否允许该监听器。 |
| `scanNotActiveBrokerInterval` | u64 / `5000` | 单位为毫秒，领域校验范围为 `1..=3600000`；管理接口变更需要重启。 |
| `needWaitForService` | bool / `false` | 启用配置的启动等待行为，需要重启。 |
| `waitSecondsForService` | i32 / `45` | 单位为秒，范围 `0..=3600`，与毫秒级扫描间隔不同。 |
| `namesrvRouteResponseCacheEnable` | bool / `false` | 可在运行时切换的路由响应缓存行为；缓存容量与分片数为独立字段。 |
| `enableRegistrationDelta` | bool / `false` | 注册增量设置，不能仅凭启用该字段推断线协议兼容性。 |
| `enableControllerInNamesrv` | bool / `false` | 要求 `embedded-controller` feature；未编译该能力时启动失败。它与普通 NameServer 路由服务不同。 |

### 管理接口更新行为

NameServer 配置路径将属性分为即时生效、重启生效和不支持三类。它先校验期望快照并完成持久化元数据写入，再发布即时生效的变更。响应区分 desired、durable 和 effective generation，并报告已应用与需要重启的键。需要重启的值即使已持久化，也尚未改变当前监听器或运行中的服务配置。

可即时生效的示例包括 `orderMessageEnable`、`namesrvRouteResponseCacheEnable`、`enableAllTopicList`、`enableTopicList` 和 `notifyMinBrokerIdChanged`。`kvConfigPath`、`configStorePath` 等路径不是在线更新目标。应以实现中的可变性分类为准；打印属性表包含某个字段，并不能证明其支持即时更新。路由可见性与 KV 持久化边界见 [NameServer 设计](../architecture/nameserver.md)。

## Controller

### 文件模型与身份

Controller 使用根层级 camelCase 字段及 Serde 默认值。应使用 `nodeId`、`listenAddr`、`raftListenAddr`、`raftPeers`、`storageBackend`，不要直接复制 Rust 的 snake_case 字段名。文件值覆盖默认值；节点身份与存储没有通用 CLI 覆盖项。`--log-filter` 是独立的启动日志覆盖项。

| 外部键名 | 类型 / 默认值 | 含义 |
| --- | --- | --- |
| `controllerType` | string / `Raft` | 受支持的 Controller 类型，使用 OpenRaft 实现。 |
| `nodeId` | u64 / `1` | 稳定的 Controller 节点身份，与 Broker ID 不同。不能复用其他节点的数据目录。 |
| `listenAddr` | SocketAddr / `127.0.0.1:60109` | Broker 与管理请求使用的 Controller remoting 监听地址。 |
| `raftListenAddr` | optional SocketAddr / 未设置 | 显式 Raft 绑定地址，否则根据已配置的对等节点端点解析。remoting 与 Raft 监听器应相互独立。 |
| `raftPeers` | `{ id, addr }` 数组 / 空 | Raft 成员端点输入。`controllerPeers` 是独立的 remoting 对等节点列表；配置模型还提供强类型端点替代形式。 |
| `electionTimeoutMs` | u64 / `1000` ms | Raft 选举时间输入，应结合心跳时序与网络条件配置。 |
| `heartbeatIntervalMs` | u64 / `300` ms | Raft 心跳时间输入，不是 Broker 心跳租约超时。 |
| `storageBackend` | enum / `RocksDB` | 序列化值为 `RocksDB`、`File`、`Memory`。Memory 用于测试，不提供重启持久性。 |
| `storagePath` | string / 空 | 首选存储根目录。为空时使用 `controllerStorePath`；两者均为空时使用 `rocketmqHome/controller/node-<nodeId>`。 |
| `snapshotLogsSinceLast` | u64 / `5000` | 触发快照的日志条目阈值，必须为正数。 |
| `snapshotMaxLogEntriesToKeep` | u64 / `1000` | 与快照相关的保留日志设置，必须为正数。 |
| `enableElectUncleanMaster` | bool / `false` | 是否允许选举同步状态集合之外的节点，需要明确数据丢失方面的取舍。 |
| `enableElectUncleanMasterLocal` | bool / `false` | 独立的本地非干净选举设置，不能假定由另一标志隐式启用。 |
| `authenticationEnabled` / `authorizationEnabled` | bool / `false` / `false` | 授权要求认证已启用。特权维护还要求专用策略与存储配置。 |

应使用完整的[三节点 Controller 示例](../deployment/high-availability.md)，其中分配了独立的 remoting/Raft 端口及持久化根目录。`ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true` 显式启用初始集群引导；对于新集群，由已配置的最小节点 ID 执行初始化。它不是替换已有成员关系的命令。Raft 控制平面健康，本身并不表示 Broker 消息体已经复制。

### 快照更新不等于拓扑操作

Controller 管理接口的更新路径检查黑名单、解析已知属性、校验候选配置，然后原子发布配置快照。未知或无效属性不会改变当前快照。该路径不会写入部署管理的启动文件，也不会重新创建已有监听器、存储引擎或 Raft 实例。即使结构性属性更新成功，也不能据此认为节点身份、成员关系或后端迁移已经生效。应将预期启动设置保存到部署配置，并通过受支持的运维流程完成结构变更。

## Proxy

### 模式、文件值与覆盖项

根字段 `mode` 取 `"cluster"` 或 `"local"`。包含 `cluster-mode` 的构建默认使用 Cluster，仅包含 Local 的构建默认使用 Local。所选模式需要对应 feature 已编译。Cluster 通过客户端基础设施转发到外部 Broker，Local 则拥有嵌入式 Broker 路径；Local 配置不是完整的 Broker TOML 模型。

读取文件或默认值后，Proxy 应用显式 `--mode`、`--grpcListenAddr`、`--remotingListenAddr`、`--enableRemoting` 和 `-n` / `--namesrvAddr`。`--enableRemoting` 只能启用监听器，禁用需通过文件设置。`--namesrvAddr` 更新 Cluster 配置，并要求 `cluster-mode`。不存在将环境变量自动映射到所有字段的通用规则；应显式配置文件值，遥测与安全环境输入则遵循各自文档。

```toml
mode = "cluster"
[grpc]
listenAddr = "127.0.0.1:8081"
[remoting]
enabled = false
[cluster]
namesrvAddr = "127.0.0.1:9876"
brokerClusterName = "DocsCluster"
```

| 外部键名 | 类型 / 默认值 | 作用范围与单位 |
| --- | --- | --- |
| `grpc.listenAddr` | string / `0.0.0.0:8081` | gRPC 入口套接字地址。 |
| `grpc.maxDecodingMessageSize` / `grpc.maxEncodingMessageSize` | usize / 各 `8388608` | 编码后 gRPC 消息大小上限，单位为字节。 |
| `grpc.maxMessageBodySize` | usize / `4194304` | 单条消息未压缩消息体的字节数上限。 |
| `grpc.maxSendMessagesPerRequest` | usize / `1024` | 发送批次消息数量限制，与字节数限制独立。 |
| `grpc.maxDecompressedRequestBytes` | usize / `8388608` | 请求解压后的字节数预算。 |
| `grpc.gzipDecodeSlots` | usize / `2` | 常驻 gzip 解码容量，零值禁用 gzip。 |
| `grpc.concurrencyLimitPerConnection` | usize / `256` | 每连接请求并发限制。 |
| `grpc.timerMaxDelayMs` / `grpc.timerPrecisionMs` | u64 / `86400000` / `1000` ms | Proxy 延迟请求接纳时域与精度；Broker 仍需独立支持定时消息。 |
| `remoting.enabled` | bool / `false` | 启用可选 remoting 入口。 |
| `remoting.listenAddr` | string / `0.0.0.0:8080` | remoting 入口地址，与 gRPC 独立。 |
| `cluster.namesrvAddr` | optional string / 未设置 | Cluster 模式下选择外部 NameServer。 |
| `cluster.brokerClusterName` | string / `DefaultCluster` | 目标 Broker 集群。 |
| `cluster.mqClientApiTimeoutMs` / `cluster.sendMessageTimeoutMs` | u64 / 各 `3000` | Cluster 客户端 API 与发送超时，单位为毫秒。 |
| `cluster.commandQueueCapacity` / `cluster.commandQueueMaxBytes` | usize / `1024` / `67108864` | 独立的请求数量与字节数预算。 |
| `cluster.ioMaxInflight` / `cluster.longPollMaxInflight` | usize / `16` / `256` | 普通 I/O 与长轮询并发预算。 |
| `local.brokerName` | string / `rocketmq-proxy-local` | Local 模式下的嵌入式 Broker 名称。 |
| `local.brokerListenPort` | u16 / `10911` | 嵌入式 Broker 端口，与 Proxy 入口独立。 |
| `local.storeRootDir` | string / `store/proxy/local-broker` | Local 存储根目录；非绝对路径时相对于工作目录解析。 |

普通模式、监听器、后端与接纳限制变更应作为启动配置处理。解析成功不代表所有限制已组成有效的运行时配置，启动阶段与所属子系统还会继续校验。路由、发送和拉取示例及其当前执行覆盖范围见 [Proxy 部署](../deployment/proxy.md)。

### 可重载的 gRPC TLS 材料

`grpc.tls.enabled` 默认为 `false`。启用时，`certificatePath` 和 `privateKeyPath` 必填。`clientAuth` 默认为 `none`，也可以取 `optional` 或 `require`；后两种验证客户端证书的模式均要求 `clientCaPath`。`reloadIntervalMs` 默认为 `5000`，启用 TLS 时必须为正数。

TLS 工作任务检测文件元数据变化，校验完整的替代材料版本，并将其应用于新连接。替代材料无效时保留最后一个可用版本，并记录诊断信息。已有连接不会重新握手。没有变化的无效文件不会被持续当作新版本重试。此机制不会重载整个 Proxy TOML，也不会轮换上游 RocketMQ 凭据。入口、上游与 ACL 的独立边界见[安全部署](../deployment/security.md)。

## 公共日志与可观测性

启动日志过滤器依次取 `--log-filter`、`RUST_LOG`、`logging.filter`、旧式 `logFilter`、回退值。规范遥测配置位于 `observability` 下，并与受支持且实际存在的环境覆盖项组合。导出器是否可用仍取决于编译的 feature 集合。具体见[可观测性配置](../configuration/observability.md)和[监控](../operations/monitoring.md)；不能假定某个服务导出的指标在所有服务中都存在。

来源：[NameServer 加载器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs)、[NameServer 更新路径](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/src/bootstrap/config_apply.rs)、[Controller 配置模型](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/src/config/controller_config.rs)、[Controller 快照发布](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/src/config.rs)、[Proxy CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/bin/rocketmq-proxy-rust.rs)和 [Proxy 入口配置模型](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/src/config.rs)。

Controller 后端选择还取决于构建 feature：`RocksDB` 需要 `storage-rocksdb`，运行时 `File` 路径需要 `dev-single`。参见 [features 与平台](./features-platforms.md)。
