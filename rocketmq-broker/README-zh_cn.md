# rocketmq-broker

[English](README.md) | [简体中文](README-zh_cn.md)

[RocketMQ-Rust](../README-zh_cn.md) 的 Broker 运行时、remoting 请求处理器、消息存储集成和服务编排 crate。

`rocketmq-broker` 是 RocketMQ-Rust workspace 中的服务端 broker crate。它提供 `rocketmq-broker-rust` 二进制入口，
负责串联 broker runtime、注册 remoting processor、初始化消息存储、集成认证与授权，并协调 offset、subscription、
topic metadata、long polling、pop 消费、事务检查、metrics 和优雅关闭等 broker 服务。

## 能力边界

| 领域 | 提供能力 |
|------|----------|
| Broker 启动 | `rocketmq-broker-rust` 二进制入口，支持 RocketMQ 风格 CLI 参数、配置加载、启动日志和优雅关闭。 |
| Remoting processor | send、pull、pop、ack、隐身时间变更、notification、polling info、reply、recall、query、client management、consumer management、lite、transaction 和 admin 请求路由。 |
| 消息存储 | 默认 local file store，可选 RocksDB store，集成 timer/schedule message、HA service，并包含 tieredstore 配置校验。 |
| 元数据管理 | Topic config、queue mapping、subscription group、consumer offset、consumer order、consumer filter 和 route metadata 管理器。 |
| Auth 集成 | Broker 级 `rocketmq-auth` runtime、ACL 文件加载/reload、auth admin 请求处理和 auth metrics 暴露。 |
| 运维能力 | Fast-failure 队列、client housekeeping、long polling、broker stats、定时服务、controller-mode hook 和优雅 shutdown。 |
| 可观测性 | 可选 OpenTelemetry metrics/traces/logs、Prometheus metrics exporter、broker metric labels 和 auth metric gauges。 |

## 架构

![rocketmq-broker architecture](../resources/broker-architecture.svg)

Broker 从 `rocketmq-broker-rust` 启动，先解析 CLI 和配置输入，再交由 `BrokerBootstrap` 管理生命周期。
`BrokerRuntime` 负责把 remoting 入口、请求处理器、auth runtime、元数据管理器、message store、运维服务和可观测性集成
串联成运行中的 broker 进程。

公开的 library API：

| API | 用途 |
|-----|------|
| `Builder` | 使用 `BrokerConfig` 和 `MessageStoreConfig` 构建 broker runtime。 |
| `BrokerBootstrap` | 管理二进制入口使用的生命周期：initialize、start、等待信号和 shutdown。 |
| `ProxyBrokerFacade` | 供 proxy 侧集成使用的 broker facade。 |

## Crate 结构

| 模块 | 职责 |
|------|------|
| [`src/bin/broker_bootstrap_server.rs`](src/bin/broker_bootstrap_server.rs) | 二进制入口、CLI 解析、配置加载、校验和启动流程。 |
| [`src/broker_bootstrap.rs`](src/broker_bootstrap.rs) | `BrokerRuntime` 的生命周期包装。 |
| [`src/broker_runtime.rs`](src/broker_runtime.rs) | 主服务图：storage、manager、processor、auth、observability、定时任务和 shutdown。 |
| [`src/processor.rs`](src/processor.rs) | 请求处理器注册表、auth 前置检查、fast-failure 分发和 processor variant。 |
| [`src/auth.rs`](src/auth.rs) | Broker auth admin service，以及 ACL/user 转换辅助能力。 |
| [`src/topic`](src/topic) | Topic config、route info 和 topic queue mapping 管理。 |
| [`src/subscription`](src/subscription) | Subscription group 管理和 lite subscription registry。 |
| [`src/offset`](src/offset) | Consumer offset、broadcast offset 和 consumer order info 管理。 |
| [`src/pop`](src/pop) | Pop 消费支持，以及 checkpoint/revive 服务。 |
| [`src/transaction`](src/transaction) | 事务消息服务、bridge、check service 和 transaction metrics flush。 |
| [`src/metrics`](src/metrics) | Broker metrics 常量、label 和基于 OpenTelemetry 的 metric manager。 |

## 环境要求

- Rust `1.85.0` 或更新版本。
- 启动 broker 前必须设置 `ROCKETMQ_HOME`。如果 `$ROCKETMQ_HOME/conf/broker.toml` 存在，会作为默认配置文件。
- 正常 broker 注册建议提供可访问的 NameServer。未提供地址时，默认使用 `127.0.0.1:9876`。

## 构建

在 workspace 根目录构建 broker 二进制：

```bash
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --release
```

按存储和可观测性场景启用 feature：

```bash
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --release --features rocksdb_store
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --release --features tieredstore
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --release --features prometheus
```

## 快速开始

使用本地 NameServer 和默认 broker 配置启动：

```bash
# Windows PowerShell
$env:ROCKETMQ_HOME = "D:\rocketmq"
$env:NAMESRV_ADDR = "127.0.0.1:9876"
cargo run -p rocketmq-broker --bin rocketmq-broker-rust
```

```bash
# Linux/macOS
export ROCKETMQ_HOME=/opt/rocketmq
export NAMESRV_ADDR=127.0.0.1:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust
```

使用显式配置文件启动：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c ./conf/broker.toml
```

指定一个或多个 NameServer 地址：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -n 192.168.1.100:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -n "192.168.1.100:9876;192.168.1.101:9876"
```

查看 CLI 参数：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- --help
```

## 命令行

| 参数 | 作用 |
|------|------|
| `-c, --configFile <FILE>` | 从 TOML 文件加载 broker 和 message-store 配置。 |
| `-p, --printConfigItem` | 打印全部 broker 和 message-store 配置项后退出。 |
| `-m, --printImportantConfig` | 打印关键运行配置项后退出。 |
| `-n, --namesrvAddr <ADDR>` | 覆盖 NameServer 地址列表。多个地址使用分号分隔。 |
| `-h, --help` | 打印 CLI 帮助。 |
| `-V, --version` | 打印二进制版本。 |

二进制使用的退出码：

| 退出码 | 含义 |
|--------|------|
| `0` | 正常退出，包括配置打印模式。 |
| `-1` | 命令行参数非法。 |
| `-2` | 未设置 `ROCKETMQ_HOME`。 |
| `-3` | 配置解析失败。 |
| `-4` | Broker 配置校验失败。 |

## 配置

配置分两层解析：

1. 配置文件来源：显式 `-c <FILE>`，然后是存在时的 `$ROCKETMQ_HOME/conf/broker.toml`，最后是默认值。
2. NameServer 覆盖顺序：`-n` 优先于 `NAMESRV_ADDR`，然后是配置文件值，最后是 `127.0.0.1:9876`。

最小 `broker.toml`：

```toml
namesrvAddr = "127.0.0.1:9876"
brokerIp1 = "127.0.0.1"
listenPort = 10911
storePathRootDir = "./store"
storePathCommitLog = "./store/commitlog"
enableControllerMode = false
storeType = "LocalFile"

[brokerServerConfig]
listenPort = 10911
bindAddress = "0.0.0.0"

[brokerIdentity]
brokerName = "broker-a"
brokerClusterName = "DefaultCluster"
brokerId = 0
```

常用运维字段：

| 字段 | 作用 |
|------|------|
| `namesrvAddr` | NameServer 地址列表，使用 `;` 分隔。 |
| `brokerIp1` / `listenPort` | Broker 对客户端和 NameServer 暴露的监听身份。 |
| `storePathRootDir` / `storePathCommitLog` | Message store 根目录和 commitlog 目录。 |
| `storeType` | Message store 后端。默认是 `LocalFile`；`RocksDB` 需要启用 `rocksdb_store` feature。 |
| `authConfigPath` / `aclFile` | Broker auth 元数据路径和 Java 风格 ACL 文件位置。 |
| `authenticationEnabled` / `authorizationEnabled` | 通过 `rocketmq-auth` 启用 broker 认证和授权检查。 |
| `metricsExporterType` | Metrics exporter：`disable`、`otlp_grpc`、`prom` 或 `log`。 |
| `traceExporterType` / `logExporterType` | Trace/log exporter：`disable`、`otlp_grpc` 或 `log`。 |

认证配置示例：

```toml
authConfigPath = "./store/auth"
aclFile = "./conf/plain_acl.yml"
aclFileWatchEnabled = true
authenticationEnabled = true
authorizationEnabled = true
signatureAlgorithm = "HmacSHA1"
```

Prometheus metrics 示例：

```toml
metricsExporterType = "prom"
metricsPromExporterHost = "127.0.0.1"
metricsPromExporterPort = 5557
metricsPromExporterPath = "/metrics"
```

启用 `tieredstore` feature 时，分层存储当前要求 `storeType = "LocalFile"`。

## Feature Flags

| Feature | 用途 |
|---------|------|
| `local_file_store` | 默认 feature，启用 local file message store 路径。 |
| `rocksdb_store` | 启用 RocksDB message store 和 broker metadata config manager。 |
| `rocksdb-store` | `rocksdb_store` 的兼容别名。 |
| `tieredstore` | 在 local file storage 之上启用 tieredstore 集成。 |
| `observability` | 启用 metrics 和 traces 可观测性栈。 |
| `otel-metrics` | 启用 OpenTelemetry metrics instrumentation。 |
| `otel-traces` | 启用 OpenTelemetry tracing instrumentation。 |
| `otel-logs` | 启用 OpenTelemetry log export 支持。 |
| `otlp-metrics` / `otlp-traces` / `otlp-logs` | 为对应 signal 启用 OTLP exporter。 |
| `prometheus` | 启用 Prometheus metrics endpoint。 |

## 请求处理面

Broker runtime 会为主要 remoting 请求族注册 processor：

| Processor family | 请求示例 |
|------------------|----------|
| Send | `SendMessage`、`SendMessageV2`、`SendBatchMessage`、`ConsumerSendMsgBack` |
| Pull | `PullMessage`、`LitePullMessage` |
| Pop and ack | `PopMessage`、`PopLiteMessage`、`AckMessage`、`BatchAckMessage`、`ChangeMessageInvisibleTime` |
| Long polling | `Notification`、`PollingInfo` |
| Reply and recall | `SendReplyMessage`、`SendReplyMessageV2`、`RecallMessage` |
| Query | `QueryMessage`、`ViewMessageById` |
| Client and consumer management | `HeartBeat`、`UnregisterClient`、`CheckClientConfig`、offset 和 consumer-list 请求 |
| Lite mode | Broker、topic、client、group、dispatch 和 lite subscription control 请求 |
| Transaction | `EndTransaction` |
| Admin default processor | Topic、subscription、broker config、runtime info、stats、auth user、auth ACL 和其他 admin 请求 |

启用 auth 后，认证和授权检查会在请求分发前执行。

## 校验

Broker 相关常用检查：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- --help
cargo test -p rocketmq-broker --lib
cargo test -p rocketmq-broker --features tieredstore --lib
cargo test -p rocketmq-broker --features rocksdb_store --lib
```

工作区级校验在仓库根目录运行：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmarks

Broker 提供 manager 和 service 热路径 benchmark：

```bash
cargo bench -p rocketmq-broker --bench consumer_manager_benchmark
cargo bench -p rocketmq-broker --bench consumer_filter_benchmark
cargo bench -p rocketmq-broker --bench subscription_group_manager_benchmark
cargo bench -p rocketmq-broker --bench schedule_message_service_performance
cargo bench -p rocketmq-broker --bench syncunsafecell_mut
```

对比 benchmark baseline 时，应保持相同 toolchain、feature set 和 storage backend。

## 许可证

RocketMQ-Rust 使用 Apache License 2.0。详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
