# rocketmq-controller

[English](README.md) | [简体中文](README-zh_cn.md)

[RocketMQ-Rust](../README-zh_cn.md) 的高可用 Controller、Broker 元数据协调器，以及基于 OpenRaft 的选主服务。

`rocketmq-controller` 提供 RocketMQ-Rust 集群使用的 Controller 运行时。它负责 Controller 启动、OpenRaft 共识、
Broker 心跳跟踪、Master 选举、副本元数据、Controller 请求处理、Controller 状态持久化，以及可选的 metrics 集成。

该 crate 同时提供库 API 和 `rocketmq-controller-rust` 二进制程序。

## 能力概览

| 领域 | 提供能力 |
|------|----------|
| Controller 启动 | `rocketmq-controller-rust` 二进制程序、CLI 解析、配置加载、启动日志、remoting 版本设置、单节点引导和优雅关闭。 |
| Raft 协调 | OpenRaft 集成、gRPC Raft 传输、log store、state machine、集群初始化、leader 检查和复制的 Controller 事件。 |
| Broker 协调 | Broker 注册、心跳、非活跃 Broker 扫描、Master 选举、sync-state-set 管理、Broker ID 分配、Broker 清理和角色变更通知。 |
| 元数据管理 | Broker 副本元数据、Topic/Config 元数据、Controller 元数据查询、sync-state 快照，以及 Java 兼容的 Controller 响应模型。 |
| 请求处理 | 面向 Broker 的 remoting 请求处理器，覆盖 elect-master、alter-sync-state-set、get-replica-info、broker heartbeat 和 register broker 等 Controller 请求码。 |
| 存储 | 默认启用的 RocksDB backend、可选 File backend，以及测试用 Memory backend。 |
| 可观测性 | Controller 请求、选举、心跳和延迟指标，并支持可选 OpenTelemetry、OTLP 和 Prometheus exporter。 |

## 架构

![rocketmq-controller architecture](../resources/controller-architecture.svg)

Controller 从 `rocketmq-controller-rust` 启动，加载 `ControllerConfig` 后构建 `ControllerManager`。
`ControllerManager` 负责 broker-facing remoting、请求处理、心跳跟踪、角色变更通知、基于 OpenRaft 的状态复制、
storage、snapshot，以及可选 metrics exporter。

二进制启动时要求配置加载后的 `rocketmqHome` 非空。常规运行时请设置 `ROCKETMQ_HOME`，或在 Controller 配置文件中提供
`rocketmqHome`。

## 目录结构

| 路径 | 说明 |
|------|------|
| [`src/bin/controller_bootstrap.rs`](src/bin/controller_bootstrap.rs) | 二进制入口，负责日志初始化、CLI/配置加载、manager 生命周期、单节点引导和关闭信号处理。 |
| [`src/cli.rs`](src/cli.rs) | 基于 Clap 的 CLI 模型，并通过 `rocketmq-common` 的配置解析器加载配置文件。 |
| [`src/config.rs`](src/config.rs) | 从 `rocketmq-common` 复用并导出 `ControllerConfig`、`RaftPeer` 和 `StorageBackendType`。 |
| [`src/controller`](src/controller) | Controller trait 实现、`ControllerManager`、OpenRaft controller wrapper、心跳管理器和 housekeeping service。 |
| [`src/openraft`](src/openraft) | OpenRaft node manager、gRPC network、log store、state machine、storage bridge 和生成的 raft service glue。 |
| [`src/processor`](src/processor) | Controller 请求处理器，以及 broker、topic、metadata 操作的领域处理器。 |
| [`src/manager`](src/manager) | 副本信息管理器、Broker 副本元数据和 sync-state 模型。 |
| [`src/metadata`](src/metadata) | Broker、Topic、Config 和 Replica metadata store。 |
| [`src/heartbeat`](src/heartbeat) | Broker identity、live-info 跟踪和默认心跳管理器。 |
| [`src/event`](src/event) | 复制的 Controller 事件模型和事件序列化。 |
| [`src/storage`](src/storage) | Storage backend 抽象、默认 RocksDB backend、可选 File backend 和测试用内存 backend。 |
| [`src/qualification.rs`](src/qualification.rs) | T0-T5 故障切换时间线、`PutOk` 消息恢复审计、confirm-offset 边界审计与机器可读资格报告。 |
| [`src/metrics`](src/metrics) | Controller metric 常量、请求/选举状态枚举和 metrics manager。 |
| [`proto`](proto) | Controller 和 OpenRaft RPC 使用的 gRPC protobuf 定义。 |
| [`examples`](examples) | 单节点 Raft、三节点 Raft、manager 使用、metrics 和 CLI 解析示例。 |
| [`tests`](tests) | Raft、snapshot、多节点、请求处理器契约和 metrics 的集成测试。 |

## 环境要求

- workspace 最低 Rust 版本和仓库构建工具链均为 stable Rust `1.95.0`。
- 请使用仓库根目录的 [`../rust-toolchain.toml`](../rust-toolchain.toml) 指定的固定 stable toolchain 构建。
- 启动 Controller 二进制前，必须设置 `ROCKETMQ_HOME` 或在配置文件中设置 `rocketmqHome`。
- 默认 feature 组合启用 RocksDB，默认配置使用 `storageBackend = "RocksDB"`。
- 仅在启用可选 `storage-file` feature 时使用 `storageBackend = "File"`。

## 构建

从 workspace 根目录构建 Controller 二进制：

```bash
cargo build -p rocketmq-controller --bin rocketmq-controller-rust --release
```

使用可选存储或 metrics features：

```bash
cargo build -p rocketmq-controller --bin rocketmq-controller-rust --release --features storage-rocksdb
cargo build -p rocketmq-controller --bin rocketmq-controller-rust --release --features metrics
cargo build -p rocketmq-controller --bin rocketmq-controller-rust --release --features metrics-otlp
cargo build -p rocketmq-controller --bin rocketmq-controller-rust --release --features metrics-prometheus
```

## 配置

CLI 可加载 TOML、JSON、YAML 以及 `config` crate 支持的其它格式。当前文件加载会直接反序列化为 `ControllerConfig`，
因此配置文件应提供完整必需字段，不应依赖部分字段覆盖默认值。

请使用 camelCase 字段名：

```toml
rocketmqHome = "/opt/rocketmq"
configStorePath = "/opt/rocketmq/controller/controller.properties"
controllerType = "Raft"
scanNotActiveBrokerInterval = 5000
controllerThreadPoolNums = 16
controllerRequestThreadPoolQueueCapacity = 50000
mappedFileSize = 1073741824
controllerStorePath = ""
electMasterMaxRetryCount = 3
enableElectUncleanMaster = false
isProcessReadEvent = false
notifyBrokerRoleChanged = true
scanInactiveMasterInterval = 5000
raftScanWaitTimeoutMs = 1000
configBlackList = "configBlackList;configStorePath"

nodeId = 1
listenAddr = "127.0.0.1:60109"
controllerPeers = []
electionTimeoutMs = 1000
heartbeatIntervalMs = 300
storagePath = "/opt/rocketmq/controller/node-1"
storageBackend = "RocksDB"
enableElectUncleanMasterLocal = false

[[raftPeers]]
id = 1
addr = "127.0.0.1:60110"

[observability.metrics]
exporter = "disable"

[observability.traces]
exporter = "disable"

[observability.logs]
exporter = "disable"

[observability.otlp]
endpoint = "http://127.0.0.1:4317"
protocol = "grpc"

[observability.prometheus]
host = "127.0.0.1"
port = 5557
path = "/metrics"
```

旧版扁平 telemetry 字段已经删除。Controller 只能通过结构化的 `[observability]` 配置段设置可观测性；实际存在的运行时环境变量会覆盖对应文件值。

多节点 Controller 集群中，每个节点应使用独立的 `nodeId`、`listenAddr` 和 `storagePath`，所有节点共享相同的
`raftPeers` 列表。`listenAddr` 是面向 Broker 的 remoting 端点；每个 `raftPeers.addr` 是对外通告的 OpenRaft
gRPC 端点，因此必须使用独立端口。

当通告的 Raft 地址无法直接绑定到 Pod 或主机时，设置
`ROCKETMQ_CONTROLLER_RAFT_BIND_ADDR=<本地 IP>:<Raft 端口>`（例如 `0.0.0.0:60110`）。对外通告地址仍取自
当前节点对应的 `raftPeers.addr`，该覆盖只改变本地 listener；无效 socket address 会使启动失败。

单成员配置保持自动 bootstrap。多成员 bootstrap 默认关闭，只有显式设置
`ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true`（或 `1`）才启用；启用后仅最小配置 node ID 初始化完整
membership，已有 committed state 永远不会被重新初始化。运维仍必须验证 leader 与 quorum 确实形成；配置和
replica 数量本身不是 quorum 证据。

## 快速开始

使用配置文件启动单节点 Controller：

```bash
# Linux/macOS
export ROCKETMQ_HOME=/opt/rocketmq
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c ./controller-node1.toml
```

```powershell
# Windows PowerShell
$env:ROCKETMQ_HOME = "C:\rocketmq"
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c .\controller-node1.toml
```

查看 CLI 选项：

```bash
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- --help
```

运行单节点 OpenRaft 示例：

```bash
cargo run -p rocketmq-controller --example single_node
```

在不同终端中运行三节点 OpenRaft 示例：

```bash
cargo run -p rocketmq-controller --example three_node_cluster -- --node-id 1 --init
cargo run -p rocketmq-controller --example three_node_cluster -- --node-id 2
cargo run -p rocketmq-controller --example three_node_cluster -- --node-id 3
```

## 库 API 使用

在 Rust 代码中直接创建和管理 Controller：

```rust
use rocketmq_controller::config::{ControllerConfig, RaftPeer};
use rocketmq_controller::manager::ControllerManager;
use rocketmq_error::Result;
use rocketmq_observability::TelemetryHandle;
use rocketmq_runtime::RuntimeContext;
use rocketmq_rust::ArcMut;

#[tokio::main]
async fn main() -> Result<()> {
    let listen_addr = "127.0.0.1:9878".parse().unwrap();
    let config = ControllerConfig::new_node(1, listen_addr)
        .with_raft_peers(vec![RaftPeer {
            id: 1,
            addr: listen_addr,
        }])
        .with_storage_path("/tmp/rocketmq-controller/node-1");

    let runtime = RuntimeContext::from_current("controller");
    let manager = ArcMut::new(
        ControllerManager::new(
            config,
            runtime.service_context("controller"),
            TelemetryHandle::noop(),
        )
        .await?,
    );
    if !manager.clone().initialize().await? {
        return Err(rocketmq_error::Error::new(&rocketmq_error::CONTROLLER_INTERNAL_FAILURE));
    }

    manager.clone().start().await?;
    manager.shutdown().await?;
    Ok(())
}
```

## Feature Flags

| Feature | 默认开启 | 用途 |
|---------|----------|------|
| `storage-file` | 否 | 启用 File backend，用于 `storageBackend = "File"`。 |
| `storage-rocksdb` | 是 | 启用默认 RocksDB backend，用于 `storageBackend = "RocksDB"`。 |
| `metrics` | 否 | 通过 `rocketmq-observability` 启用 Controller metrics 集成。 |
| `metrics-otlp` | 否 | 启用 OTLP metrics export 支持。 |
| `metrics-prometheus` | 否 | 启用 Prometheus metrics export 支持。 |
| `debug` | 否 | Controller 构建预留的 debug feature flag。 |

## Examples

```bash
cargo run -p rocketmq-controller --example single_node
cargo run -p rocketmq-controller --example three_node_cluster -- --node-id 1 --init
cargo run -p rocketmq-controller --example controller_manager_basic
cargo run -p rocketmq-controller --example controller_manager_cluster
cargo run -p rocketmq-controller --example controller_metrics_example
cargo run -p rocketmq-controller --example cli_usage -- -c ./controller-node1.toml -p
```

## 验证

本 crate 的聚焦检查：

```bash
cargo test -p rocketmq-controller --lib
cargo test -p rocketmq-controller --tests --no-run
cargo test -p rocketmq-controller --examples --no-run
cargo test -p rocketmq-controller --test controller_failover_slo
```

当修改 Rust 代码时，需要从仓库根目录执行 workspace 级验证：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## 故障切换资格验证

Controller 元数据共识和消息 payload 持久性是两个不同边界。仅完成元数据 leader 选举不能证明消息
RPO=0。只有测试链路同时采用同步本地刷盘、完成要求的副本 ACK、保持 clean election、恢复全部生产者已收到
`PutOk` 的消息，并保证 `confirmOffset` 单调且不超过合法 in-sync ACK 时，才接受严格 RPO=0 结论。

资格验证 API 按顺序记录一次故障切换：T0 注入故障、T1 Controller leader 选出、T2 broker master 选出、
T3 store 写权限生效、T4 NameServer 路由收敛、T5 producer 首次发送成功。

`FailoverQualificationReport` 输出带版本号的 JSON 证据，并明确拒绝不完整或不安全的运行结果。它不会把配置的
选举超时当成实测 RTO，也不会从单次运行推导百分位。发布 p50/p95/p99 或 SLO 前，必须通过端到端故障注入
harness 收集重复样本。当前固定的 OpenRaft alpha 依赖在发布前必须持续通过线性一致性、存储故障、多节点和故障
切换资格验证门禁。

## Benchmarks

```bash
cargo bench -p rocketmq-controller --bench controller_bench
```

该 benchmark 测量确定性的 Controller 热路径：复制心跳应用、Raft 请求编码和资格证据记录。Leader 选举与
端到端 RTO 属于集成/故障测试，不作为 Criterion 微基准。

## License

基于 [Apache License, Version 2.0](../LICENSE-APACHE) 开源。
