# rocketmq-observability

[English](README.md) | [简体中文](README-zh_cn.md)

为 RocketMQ-Rust 提供统一遥测配置、本地日志、OpenTelemetry 集成及导出器生命周期管理。
默认不启用可选的 OpenTelemetry 特性；控制台/文件日志、配置解析、统计及空操作遥测句柄仍然可用。

## 所有权与公开 API

```text
Application RuntimeOwner
  -> TelemetryRuntimeGuard (providers, logging guards, exporter shutdown)
     -> TelemetryHandle clones -> component metrics and trace policy
  -> owned exporter tasks (for example, Prometheus HTTP)
```

应用持有不可克隆的 `TelemetryRuntimeGuard`。业务组件接收 `TelemetryHandle` 的克隆，
通过 `from_handle` 创建对应角色的记录器。句柄不能关闭提供程序；守卫关闭后，
仍然存活的句柄停止记录。SDK 提供程序由守卫持有，初始化不会设置全局 OpenTelemetry
指标或追踪提供程序。tracing 订阅器及可选的文本映射传播器属于进程级状态。

配置、启动函数、句柄及上下文传播辅助函数从 crate 根路径导入。
`config`、`init`、`logging`、`propagation` 是私有实现模块。
公开的 `metrics`、`trace`、`logs`、`semantic`、`statistics` 和 `stats`
模块提供相应的埋点与统计 API。

## 快速开始

对于同级应用，根据实际位置调整路径：

```toml
[dependencies]
rocketmq-observability = { path = "../rocketmq-observability", features = ["otlp-metrics", "otlp-traces"] }
rocketmq-runtime = { path = "../rocketmq-runtime" }
```

以下生命周期示例需要上述 OTLP 特性及配置地址上的收集器：

```rust,no_run
use std::time::Duration;
use rocketmq_observability::{
    install_global_with_service_context, MetricsExporter, TelemetryBootstrapConfig, TraceExporter,
};
use rocketmq_runtime::RuntimeOwner;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let owner = RuntimeOwner::new()?;
    let result: Result<(), Box<dyn std::error::Error>> = owner.block_on(async {
        let context = owner.root_context().component("telemetry");
        let mut config = TelemetryBootstrapConfig::default();
        config.observability.enabled = true;
        config.observability.service_name = "example-service".to_string();
        config.observability.metrics.enabled = true;
        config.observability.metrics.exporter = MetricsExporter::OtlpGrpc;
        config.observability.traces.enabled = true;
        config.observability.traces.exporter = TraceExporter::OtlpGrpc;
        config.observability.otlp.endpoint = "http://127.0.0.1:4317".to_string();

        let guard = install_global_with_service_context(&config, &context).await?;
        let _handle = guard.handle();
        // Inject the handle into services, then stop those services before telemetry.
        let report = guard
            .shutdown_with_service_context(&context, Duration::from_secs(10))
            .await;
        if !report.is_healthy() {
            return Err(std::io::Error::other("telemetry shutdown was unhealthy").into());
        }
        Ok(())
    });
    let report = owner.shutdown_runtime_blocking()?;
    result?;
    if !report.is_healthy() {
        return Err(std::io::Error::other("runtime shutdown was unhealthy").into());
    }
    Ok(())
}
```

使用句柄启动应用服务，并在关闭遥测之前停止这些服务。
需要在同一个订阅器中组合控制台/文件日志与 OpenTelemetry 层的服务入口，
使用 `install_global_with_service_context`。
不需要运行时持有导出器任务的配置也可使用同步 `install_global`。
`init_observability` 和 `init_observability_with_service_context` 保留仅初始化遥测的兼容路径，
返回类型是 `TelemetryRuntimeGuard`，不存在独立的 `TelemetryGuard` 类型。

Prometheus 必须使用接收作用域的初始化 API，以及 `shutdown_with_service_context`，
以等待监听器和任务退出。关闭返回 `TelemetryShutdownReport`；
通过 `is_healthy()` 检查任务和提供程序的关闭失败。
丢弃守卫不能替代显式刷新和关闭。

## 配置与日志

- `TelemetryBootstrapConfig` 组合 `ObservabilityConfig` 和 `LoggingConfig`。
- `ObservabilityOverrides` 与 `resolve_telemetry_from_env` 供服务入口合并结构化文件设置
  和其声明的环境变量。
- `LogFilterResolver` 按运行时覆盖值、CLI、环境变量、配置、回退值的顺序选择日志过滤器。
  调用方显式传入这些输入；单独调用 `install_global` 不会读取 `RUST_LOG`。
- 控制台/文件输出、轮转、有界非阻塞日志及重载配置，与 OpenTelemetry 日志导出器分别管理。
- `SubscriberInstallPolicy::Required` 在无法安装所需订阅器时失败；
  `BestEffort` 记录安装状态。嵌入已持有订阅器的宿主时，应检查该状态。
- 运行时请求未编译的导出器会返回类型化的 `observability.feature.disabled` 错误。

服务配置参见[可观测性指南](../rocketmq-website/docs/configuration/observability.md)。

## 特性与导出器

默认特性集合为空。

| 特性 | 作用 |
| --- | --- |
| `observability` | 同时启用 `otel-metrics` 和 `otel-traces` 的便捷别名。 |
| `otel-metrics` | 指标工具及 SDK 提供程序。 |
| `otel-traces` | 追踪集成、Span 辅助 API 及消息上下文传播。 |
| `otel-logs` | OpenTelemetry 日志及 tracing 桥接。 |
| `otlp-grpc` | 共享 OTLP 传输依赖；还需选择具体信号特性。 |
| `otlp-metrics`、`otlp-traces`、`otlp-logs` | 相应信号及 OTLP gRPC 导出。 |
| `prometheus` | 指标读取器及 HTTP 抓取端点；必须注入服务上下文。 |
| `stdout` | 兼容特性；日志输出由运行时导出器设置控制。 |

运行时可选择 `MetricsExporter::{Log, OtlpGrpc, Prometheus}`、
`TraceExporter::{Log, OtlpGrpc}` 和 `LogsExporter::{Log, OtlpGrpc}`。
本地日志导出器仍需对应的 `otel-*` 特性。

## 埋点

角色记录器覆盖 Broker、客户端、传输、NameServer、Controller、Proxy、存储、分层存储、
运行时、Dashboard 和 SRE 组件。标签策略限制主题/消费组的基数。
使用注入句柄派生的记录器；对于提供相应 API 的模块，也可在启用特性后使用接收 meter 的 SDK 构造器。

启用 `otel-traces` 后，根路径导出的 `inject_current_context_with_handle`、
`extract_context_with_handle` 和 `set_span_parent_from_properties_with_handle`
通过消息属性映射和句柄中的追踪策略传播上下文。
共享常量 `TRACEPARENT` 和 `TRACESTATE` 定义线上属性名称。

`rocketmq-client-rust/observability` 启用客户端追踪，
`observability-metrics` 启用客户端指标。通过 OTLP 导出客户端指标还需直接依赖
`rocketmq-observability/otlp-metrics`；客户端没有同名特性。

## 源码与验证

参见[公开导出](src/lib.rs)、[启动与关闭](src/logging.rs)、
[提供程序创建](src/init.rs)、[能力句柄](src/handle.rs)、
[配置解析](src/resolver.rs)、[指标](src/metrics)及[导出器故障处理](src/exporter/outage.rs)。

按实际改动的特性选择检查：

```bash
cargo fmt -p rocketmq-observability -- --check
cargo test -p rocketmq-observability --lib
cargo test -p rocketmq-observability --lib --features otel-metrics,otel-traces
cargo bench -p rocketmq-observability --bench observability_hot_path
```

README 示例变更后应验证编译。导出器集成需要相应的收集器或抓取测试；
通过编译不能证明数据已经交付。

[Apache License 2.0](../LICENSE-APACHE)。
