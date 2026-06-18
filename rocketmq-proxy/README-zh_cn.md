# rocketmq-proxy

[![Crates.io](https://img.shields.io/crates/v/rocketmq-proxy.svg)](https://crates.io/crates/rocketmq-proxy)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE-APACHE)

`rocketmq-proxy` 是
[rocketmq-rust](https://github.com/mxsm/rocketmq-rust) 工作空间中的
RocketMQ Proxy 运行时。它暴露 Apache RocketMQ v2 gRPC
`MessagingService`，提供可选的 remoting 兼容入口，并可以将代理请求路由
到已有 RocketMQ 集群或内嵌的本地 Broker 运行时。

该 crate 适用于需要 Rust 原生 RocketMQ Proxy 进程的使用者，也适用于维护
proxy、auth、client、broker、remoting 集成面的贡献者。

[English](README.md)

## 能力概览

- 面向 Apache RocketMQ v2 `MessagingService` 协议的 gRPC Proxy 运行时。
- Cluster 模式通过 `rocketmq-client-rust` 和 `rocketmq-remoting` 访问
  NameServer 与 Broker 节点。
- Local 模式启动内嵌 Broker 支撑的服务管理器，适用于开发、集成测试和单
  进程部署。
- 可选 remoting 入口，用于兼容常见 RocketMQ 客户端请求码。
- 通过 `rocketmq-auth` 集成认证与授权，覆盖 gRPC metadata 认证、remoting
  ACL 签名、ACL 文件重载和集群元数据查询。
- 管理心跳、客户端设置、receipt handle、lite subscription、telemetry 链
  路、事务预备状态和客户端终止清理。
- 提供请求 hook 和内存指标快照，并可通过 `observability` feature 输出
  OpenTelemetry 指标。

## 架构

![rocketmq-proxy 架构](../resources/proxy-architecture.svg)

运行时由 `ProxyRuntimeBuilder` 组装：

- `ProxyRuntime` 负责进程生命周期并启动 gRPC server。
- `ProxyGrpcService` 实现生成的 v2 `MessagingService` server。
- `ProxyRemotingDispatcher` 在启用 remoting 入口时，将选定 request code
  适配到同一套 processor 模型。
- `DefaultMessagingProcessor` 校验请求，并将 route、metadata、assignment、
  message、consumer、transaction 操作委派给 `ServiceManager`。
- `ClusterServiceManager` 将操作转发到配置的 RocketMQ 集群。
- `LocalServiceManager` 基于内嵌 Broker facade 执行操作。

## 协议接口

### gRPC

生成的 gRPC service 来自 [`proto/service.proto`](proto/service.proto)，当前实现：

| RPC | 用途 |
| --- | --- |
| `QueryRoute` | 查询 topic 路由数据。 |
| `Heartbeat` | 注册并刷新客户端 session 状态。 |
| `SendMessage` | 发送单条或批量消息。 |
| `QueryAssignment` | 查询消费者队列分配。 |
| `ReceiveMessage` | 以流式方式返回 pop receive 响应。 |
| `AckMessage` | 确认已接收消息并清理跟踪的 handle。 |
| `ForwardMessageToDeadLetterQueue` | 将消息转发到死信队列。 |
| `PullMessage` | 以流式方式返回基于队列的 pull 响应。 |
| `UpdateOffset`, `GetOffset`, `QueryOffset` | 管理消费者进度。 |
| `EndTransaction` | 提交或回滚预备事务状态。 |
| `Telemetry` | 交换客户端设置和服务端命令。 |
| `NotifyClientTermination` | 清理客户端持有的 proxy 状态。 |
| `ChangeInvisibleDuration` | 续期或更新 receipt handle。 |
| `RecallMessage` | 在后端支持时召回延迟消息。 |
| `SyncLiteSubscription` | 跟踪 lite subscription 成员关系。 |

### Remoting 入口

Remoting 入口默认关闭。启用后，proxy 会监听配置的 remoting 地址，并处理常
见客户端请求码：

- 路由与分配：`GetRouteinfoByTopic`、`QueryAssignment`
- 生产者：`SendMessage`、`SendMessageV2`、`SendBatchMessage`
- 客户端生命周期：`HeartBeat`、`UnregisterClient`、`CheckClientConfig`
- 消费者成员与通知：`GetConsumerListByGroup`、`NotifyConsumerIdsChanged`、
  `NotifyUnsubscribeLite`
- 消费操作：`PullMessage`、`LitePullMessage`、`UpdateConsumerOffset`、
  `QueryConsumerOffset`、`GetMaxOffset`、`GetMinOffset`、
  `SearchOffsetByTimestamp`
- Lite 元数据：`GetBrokerLiteInfo`、`GetParentTopicInfo`、
  `GetLiteTopicInfo`、`GetLiteGroupInfo`
- Local 模式透传：`LockBatchMq`、`UnlockBatchMq`

Auth 管理类 request code 会被 proxy remoting 入口明确拒绝，应发送到
Broker 管理端点。

## 环境要求

- Rust `1.85.0` 或更高版本，与 workspace toolchain 保持一致。
- Cluster 模式需要可访问的 RocketMQ NameServer。
- Local 模式和基于文件的 auth metadata 需要可写本地存储。
- 可选：启用 `observability` feature 时需要 OpenTelemetry 指标管线。

## 安装

在 workspace 根目录执行：

```bash
cargo build -p rocketmq-proxy
```

二进制目标为 `rocketmq-proxy-rust`：

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust -- --help
```

## 快速开始

### Cluster 模式

启动一个在 `0.0.0.0:8081` 暴露 gRPC，并连接已有 NameServer 的 proxy：

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust -- \
  --mode cluster \
  --namesrvAddr 127.0.0.1:9876 \
  --grpcListenAddr 0.0.0.0:8081
```

只打印生效启动配置，不绑定端口：

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust -- \
  --namesrvAddr 127.0.0.1:9876 \
  --printConfig
```

### Local 模式

启动使用内嵌 Broker 服务管理器的 proxy：

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust -- \
  --mode local \
  --grpcListenAddr 127.0.0.1:8081
```

Local 模式适用于开发和集成场景，尤其是希望用单进程运行 proxy 与 broker
能力时。

### 启用 Remoting 入口

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust -- \
  --mode cluster \
  --namesrvAddr 127.0.0.1:9876 \
  --grpcListenAddr 0.0.0.0:8081 \
  --enableRemoting \
  --remotingListenAddr 0.0.0.0:8080
```

## 配置

Proxy 可以加载 TOML、YAML、JSON 或其他
[`config`](https://docs.rs/config/) crate 支持的格式：

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust -- -c proxy.toml
```

CLI 参数会覆盖配置文件中对应字段：

```text
rocketmq-proxy-rust [-c <proxy.toml>] [--mode cluster|local]
  [--grpcListenAddr <host:port>]
  [--enableRemoting] [--remotingListenAddr <host:port>]
  [--namesrvAddr <host:port>] [--printConfig]
```

TOML 示例：

```toml
mode = "cluster"
enableAclRpcHookForClusterMode = true

[grpc]
listenAddr = "0.0.0.0:8081"
maxDecodingMessageSize = 8388608
maxEncodingMessageSize = 8388608
concurrencyLimitPerConnection = 256
useEndpointPortFromRequest = false

[remoting]
enabled = true
listenAddr = "0.0.0.0:8080"

[cluster]
namesrvAddr = "127.0.0.1:9876"
brokerClusterName = "DefaultCluster"
instanceName = "rocketmq-proxy-cluster"
mqClientApiTimeoutMs = 3000
queryAssignmentStrategyName = "AVG"
producerGroupPrefix = "PROXY_SEND"
sendMessageTimeoutMs = 3000
routeCacheTtlMs = 5000
metadataCacheTtlMs = 5000

[local]
brokerClusterName = "DefaultCluster"
brokerName = "rocketmq-proxy-local"
brokerIp = "127.0.0.1"
brokerListenPort = 10911
storeRootDir = "store/proxy/local-broker"

[runtime]
routePermits = 512
producerPermits = 1024
consumerPermits = 1024
clientManagerPermits = 512

[session]
clientTtlMs = 60000
receiptHandleTtlMs = 300000
autoRenewEnabled = true
minLongPollingTimeoutMs = 5000
maxLongPollingTimeoutMs = 20000

[auth]
authenticationEnabled = false
authorizationEnabled = false
authConfigPath = "store/proxy/auth"
aclFile = ""
aclFileWatchEnabled = false
```

### 关键配置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `mode` | `cluster` | `cluster` 转发到 RocketMQ 集群；`local` 使用内嵌 Broker 服务管理器。 |
| `grpc.listenAddr` | `0.0.0.0:8081` | v2 `MessagingService` 的 gRPC 绑定地址。 |
| `remoting.enabled` | `false` | 启用 remoting 兼容入口。 |
| `remoting.listenAddr` | `0.0.0.0:8080` | remoting 启用时的绑定地址。 |
| `cluster.namesrvAddr` | 未设置 | Cluster 模式使用的 NameServer 地址。 |
| `cluster.routeCacheTtlMs` | `5000` | Cluster client 使用的路由缓存 TTL。 |
| `cluster.metadataCacheTtlMs` | `5000` | Cluster client 和 auth metadata 刷新使用的元数据缓存 TTL。 |
| `enableAclRpcHookForClusterMode` | `false` | 为 cluster 调用添加 proxy 内部凭证 ACL RPC hook。 |
| `runtime.*Permits` | 不同字段不同 | route、producer、consumer、client-manager RPC 的并发保护。 |
| `session.clientTtlMs` | `60000` | 客户端 session TTL。 |
| `session.receiptHandleTtlMs` | `300000` | Receipt handle 跟踪 TTL。 |
| `auth.authenticationEnabled` | `false` | 启用请求认证。 |
| `auth.authorizationEnabled` | `false` | 启用 ACL 授权。 |

## 认证

`ProxyAuthRuntime` 会将 proxy auth 配置映射到 `rocketmq-auth`，并被 gRPC
和 remoting 入口共享：

- gRPC 请求使用 `authorization`、`x-mq-date-time`、`channel-id`、
  `x-mq-client-id` 等 metadata。
- Remoting 请求使用 RocketMQ ACL 字段，例如 `AccessKey` 和 `Signature`。
- ACL 文件配置支持本地 metadata 和可选 watcher 重载。
- 在 Cluster 模式中，auth metadata 可以通过 Broker metadata API 刷新。

`ProxyAuthConfig` 的 debug 输出会隐藏敏感内嵌凭证。

## 作为库使用

运行时可以嵌入到测试、工具或更高层服务中，并按需定制：

```rust
use rocketmq_proxy::{ProxyConfig, ProxyRuntime};

#[tokio::main]
async fn main() -> rocketmq_proxy::ProxyResult<()> {
    let config = ProxyConfig::default();

    ProxyRuntime::builder(config)
        .build()
        .serve()
        .await
}
```

可以通过 `ProxyRuntime::builder` 注入自定义 `ServiceManager`、
`ClientSessionRegistry`、`ProxyAuthRuntime`、请求 hook、metrics 或 remoting
backend。

## Crate 结构

```text
rocketmq-proxy/
  proto/                 Apache RocketMQ v2 protobuf 定义
  src/bin/               rocketmq-proxy-rust 二进制入口
  src/bootstrap.rs       ProxyRuntime 与 ProxyRuntimeBuilder
  src/config.rs          runtime、cluster、local、session、remoting、auth 配置
  src/grpc/              gRPC server、service 实现、adapter、middleware
  src/remoting.rs        可选 remoting 入口和 request-code dispatcher
  src/cluster.rs         Cluster 模式 client bridge
  src/local.rs           内嵌 Broker 支撑的 Local 模式
  src/service.rs         Service traits 与默认/cluster/local managers
  src/session.rs         客户端 session、telemetry、receipt handle、lite subscription
  src/auth.rs            Proxy 认证与授权集成
  src/observability.rs   Hook、内存指标、可选 OTel 指标记录
  tests/                 gRPC 与 remoting 入口集成测试
```

## Feature Flags

| Feature | 说明 |
| --- | --- |
| `observability` | 启用 `rocketmq-observability` OpenTelemetry 指标集成。 |
| `tieredstore` | 将 tiered-store 支持传递给 broker 和 store 依赖。 |

默认 feature set 为空。

## 验证

该 crate 常用检查：

```bash
cargo test -p rocketmq-proxy --lib
cargo test -p rocketmq-proxy --test grpc_ingress --test remoting_ingress
cargo clippy -p rocketmq-proxy --all-targets --all-features -- -D warnings
```

如果修改了仓库范围 Rust 代码，需要在根目录运行 workspace 验证：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## License

基于 [Apache License, Version 2.0](../LICENSE-APACHE) 许可发布。
