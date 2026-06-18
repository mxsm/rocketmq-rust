# rocketmq-namesrv

[English](README.md) | [简体中文](README-zh_cn.md)

[RocketMQ-Rust](../README-zh_cn.md) 的 RocketMQ NameServer 实现。

`rocketmq-namesrv` 为 RocketMQ broker、client 和 admin tool 提供轻量级服务发现与路由能力。它负责 broker 存活状态、
topic route metadata、broker member group、写权限、KV 配置、运行时配置以及可选的内嵌 controller 集成。默认路由管理器是
基于并发表和 segmented lock 的生产可用 V2 实现。

该 crate 既可以作为 `rocketmq-namesrv-rust` 二进制运行，也可以通过 `bootstrap::Builder` API 嵌入测试或上层服务。

## 能力边界

| 领域 | 提供能力 |
|------|----------|
| 服务发现 | Broker 注册、注销、heartbeat 跟踪、inactive broker 扫描和 channel destroy 清理。 |
| Topic 路由 | Topic route 查询、standard/legacy JSON route 编码、zone-aware route filtering、filter-server metadata 和 order-topic config 查询。 |
| 路由存储 | `RouteInfoManagerWrapper` 默认选择 V2 DashMap 并发表，同时保留可配置的 legacy V1 manager。 |
| Broker 与 topic 管理 | Cluster info、broker member group、topic 注册/删除、按 cluster 查询 topic list、unit-topic list 和写权限更新。 |
| KV 配置 | 通过 `KVConfigManager` 支持 KV namespace 的 put/get/delete/list 和磁盘持久化。 |
| 运行时配置 | `GetNamesrvConfig` 和 `UpdateNamesrvConfig` 支持 Java-properties payload，并对敏感路径和 home 设置保留固定黑名单。 |
| Cluster test 模式 | 本地无 route data 时，可通过 `DefaultMQAdminExtImpl` 回查 product environment。 |
| 内嵌 controller | 可通过 `enableControllerInNamesrv` 初始化并运行 `rocketmq-controller`，同时检查 controller listen 地址不能与 NameServer 冲突。 |
| 可观测性 | 可选 `observability` feature 记录 route request 数量/延迟、broker registration 和 active broker gauge。 |

## 架构

![rocketmq-namesrv 架构](../resources/namesrv-architecture.svg)

`KVConfigManager` 负责 namespace config 持久化，`BrokerHousekeepingService` 响应 channel event，定时任务扫描 inactive
broker；启用内嵌 controller 时，`ControllerManager` 会随 NameServer 生命周期启动和关闭。

## 协议覆盖

| 类别 | Request codes |
|------|---------------|
| Client route 查询 | `GetRouteinfoByTopic` `105` |
| Broker 生命周期 | `RegisterBroker` `103`、`UnregisterBroker` `104`、`BrokerHeartbeat` `904`、`GetBrokerMemberGroup` `901`、`QueryDataVersion` `322` |
| Cluster 与 broker 管理 | `GetBrokerClusterInfo` `106`、`WipeWritePermOfBroker` `205`、`AddWritePermOfBroker` `327` |
| Topic metadata | `GetAllTopicListFromNameserver` `206`、`DeleteTopicInNamesrv` `216`、`RegisterTopicInNamesrv` `217`、`GetTopicsByCluster` `224` |
| System 与 unit topic | `GetSystemTopicListFromNs` `304`、`GetUnitTopicList` `311`、`GetHasUnitSubTopicList` `312`、`GetHasUnitSubUnunitTopicList` `313` |
| KV config | `PutKvConfig` `100`、`GetKvConfig` `101`、`DeleteKvConfig` `102`、`GetKvlistByNamespace` `219` |
| Runtime config | `UpdateNamesrvConfig` `318`、`GetNamesrvConfig` `319` |

不支持的 request code 会由 default processor 返回 `RequestCodeNotSupported`。

## 环境要求

- Rust `1.85.0` 或更新版本。
- 使用仓库中的 [`../rust-toolchain.toml`](../rust-toolchain.toml) 工具链。
- 正常启动二进制时必须设置 `ROCKETMQ_HOME` 或传入 `--rocketmqHome`。
- 默认 NameServer 监听端口为 `9876`。
- 启动 NameServer 不要求 broker 已运行，但 client route 查询需要 broker 注册后才能返回 topic metadata。

## 安装

在当前 workspace 内使用：

```toml
[dependencies]
rocketmq-namesrv = { path = "../rocketmq-namesrv" }
```

外部项目使用：

```toml
[dependencies]
rocketmq-namesrv = "1.0.0"
```

启用 NameServer metrics 集成：

```toml
[dependencies]
rocketmq-namesrv = { version = "1.0.0", features = ["observability"] }
```

## 快速开始

查看 CLI 选项：

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- --help
```

通过命令行参数启动：

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- \
  --rocketmqHome /opt/rocketmq \
  --listenPort 9876 \
  --bindAddress 0.0.0.0
```

使用示例配置文件启动：

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- \
  -c rocketmq-namesrv/resource/namesrv-example.toml
```

打印合并后的配置并退出：

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- \
  -c rocketmq-namesrv/resource/namesrv-example.toml \
  -p
```

配置优先级：

1. 内置默认值。
2. `-c` / `--configFile` 指定的配置文件。
3. 命令行覆盖项，例如 `--listenPort`、`--bindAddress`、`--rocketmqHome` 和 `--kvConfigPath`。

## 配置

示例文件 [`resource/namesrv-example.toml`](resource/namesrv-example.toml) 记录了支持的配置项。配置模型支持 Java 风格
camelCase key；存在 serde alias 的字段也可使用 Rust 风格字段名。

| Key | 默认值 | 作用 |
|-----|--------|------|
| `rocketmqHome` | `ROCKETMQ_HOME` / `ROCKETMQ_HOME_PROPERTY` | Runtime 使用的 RocketMQ home 目录。 |
| `kvConfigPath` | `~/rocketmq-namesrv/kvConfig.json` | KV 配置持久化文件。 |
| `configStorePath` | `~/rocketmq-namesrv/rocketmq-namesrv.properties` | Runtime 配置持久化路径。 |
| `listenPort` | `9876` | Remoting server 端口，通过 `ServerConfig` 或 CLI 配置。 |
| `bindAddress` | `0.0.0.0` | Remoting server 绑定地址，通过 `ServerConfig` 或 CLI 配置。 |
| `scanNotActiveBrokerInterval` | `5000` | Broker inactive 扫描间隔，单位毫秒。 |
| `useRouteInfoManagerV2` | `true` | 启用 DashMap-based route manager。 |
| `enableControllerInNamesrv` | `false` | 在 NameServer 内运行 embedded controller。 |
| `clusterTest` | `false` | 启用 product-environment route fallback。 |
| `orderMessageEnable` | `false` | 在 route response 中附加 `ORDER_TOPIC_CONFIG`。 |
| `configBlackList` | `configBlackList;configStorePath;kvConfigPath` | 远程运行时配置更新时禁止修改的额外 key。 |

`UpdateNamesrvConfig` 还会拒绝固定保护集合，包括 `rocketmqHome`、`kvConfigPath`、`configStorePath` 和
`configBlackList`。

## 嵌入式使用

在测试或上层服务中可以使用 `bootstrap::Builder` 嵌入 NameServer：

```rust
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_namesrv::bootstrap::Builder;

async fn run_namesrv() -> rocketmq_error::RocketMQResult<()> {
    let namesrv_config = NamesrvConfig {
        rocketmq_home: "/opt/rocketmq".to_string(),
        use_route_info_manager_v2: true,
        ..NamesrvConfig::default()
    };

    let server_config = ServerConfig {
        listen_port: 9876,
        bind_address: "0.0.0.0".to_string(),
        ..ServerConfig::default()
    };

    Builder::new()
        .set_name_server_config(namesrv_config)
        .set_server_config(server_config)
        .build()
        .boot()
        .await
}
```

确定性测试可以使用 `boot_with_shutdown(...)` 传入显式 shutdown future。

## Crate 结构

| 路径 | 职责 |
|------|------|
| [`src/bin/namesrv_bootstrap_server.rs`](src/bin/namesrv_bootstrap_server.rs) | CLI 入口、配置合并、embedded-controller 配置加载和启动校验。 |
| [`src/bootstrap.rs`](src/bootstrap.rs) | Runtime 生命周期、server 启动、processor 注册、定时 broker 扫描、remoting client 和优雅关闭。 |
| [`src/processor.rs`](src/processor.rs) | Request processor dispatcher 和 route request metrics hook。 |
| [`src/processor/default_request_processor.rs`](src/processor/default_request_processor.rs) | Broker、topic、KV、permission 和 runtime config 请求处理。 |
| [`src/processor/client_request_processor.rs`](src/processor/client_request_processor.rs) | Client topic-route 查询路径。 |
| [`src/processor/cluster_test_request_processor.rs`](src/processor/cluster_test_request_processor.rs) | 带 product environment fallback 的 cluster-test route 查询。 |
| [`src/route`](src/route) | Route manager、segmented lock、route table、unregister service 和 zone route hook。 |
| [`src/route/tables`](src/route/tables) | Topic queue、broker、cluster、live broker、filter server 和 topic queue mapping 的并发表。 |
| [`src/kvconfig`](src/kvconfig) | KV config manager 和持久化。 |
| [`src/observability_metrics.rs`](src/observability_metrics.rs) | `observability` feature 下的可选 metrics 记录。 |
| [`tests`](tests) | 网络级和 route-table integration 覆盖。 |
| [`benches`](benches) | Route manager、concurrency、lock 和 topic-table 热路径 benchmark。 |

## Feature Flags

| Feature | 作用 |
|---------|------|
| `observability` | 通过 `rocketmq-observability/otel-metrics` 启用 NameServer metrics。 |

## 验证

该 crate 的聚焦检查：

```bash
cargo test -p rocketmq-namesrv --lib
cargo test -p rocketmq-namesrv --test route_info_manager_integration
cargo test -p rocketmq-namesrv --test topic_table_index_equivalence
cargo test -p rocketmq-namesrv --benches --no-run
```

如果修改 Rust 代码，需要在仓库根目录执行 workspace 级验证：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmark

在 workspace 根目录运行聚焦 benchmark：

```bash
cargo bench -p rocketmq-namesrv --bench route_manager_benchmark
cargo bench -p rocketmq-namesrv --bench route_concurrency_bench
cargo bench -p rocketmq-namesrv --bench async_segmented_lock_bench
cargo bench -p rocketmq-namesrv --bench topic_table_hot_path_bench
```

对比 benchmark 时应保持相同工具链、硬件、route table size、broker count 和 topic distribution。

## License

RocketMQ-Rust 使用 Apache License 2.0。详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
