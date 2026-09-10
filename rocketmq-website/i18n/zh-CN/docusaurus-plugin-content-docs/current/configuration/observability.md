---
sidebar_position: 4
title: 可观测性
---

# RocketMQ Rust 可观测性

Broker、NameServer、Controller、Proxy 和 RocketMQ MCP 使用统一的
`observability` 文件配置段。共享解析器按顺序合并服务默认值、文件值以及
实际存在的环境变量。

## 启用模型

每类信号只有在以下两个门槛同时满足时才会运行：

| 门槛 | 要求 |
| --- | --- |
| 构建时 | 使用对应信号及导出器的 feature 编译服务。 |
| 运行时 | 在 `observability` 中或通过受支持的环境变量覆盖选择非 `disable` 导出器。 |

不同服务的 feature 名称并不完全相同，必须以对应服务的 `Cargo.toml` 为准：

| 服务 | 便捷 feature | 信号及导出器 feature |
| --- | --- | --- |
| Broker | `observability` 启用 metrics 和 traces | `otel-metrics`、`otlp-metrics`、`prometheus`、`metrics-prometheus`、`otel-traces`、`otlp-traces`、`otel-logs`、`otlp-logs` |
| NameServer | `observability` 启用 metrics 和 traces | `otel-metrics`、`otlp-metrics`、`otel-traces`、`otlp-traces`、`otel-logs`、`otlp-logs` |
| Controller | 无 | `metrics`、`metrics-otlp`、`metrics-prometheus`、`otel-traces`、`otlp-traces`、`otel-logs`、`otlp-logs` |
| Proxy | `observability` 仅启用 metrics | `otlp-metrics`、`otel-traces`、`otlp-traces`、`otel-logs`、`otlp-logs` |
| MCP | `observability` 启用 metrics 和 traces | `otlp` 同时加入 OTLP metrics、traces 和 logs |

特别需要注意，Controller 没有定义 `observability` 便捷 feature。OTLP logs
仍然需要在构建时显式选择。例如：

```bash
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-metrics,otlp-traces,otlp-logs"
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features prometheus
```

仅启用 Cargo feature 不会自动启用导出；运行时选择导出器也无法加入编译时未包含的代码。

## 优先级

解析优先级从低到高固定如下：

| 优先级 | 来源 | 行为 |
| --- | --- | --- |
| 1 | 服务默认值 | 所有信号默认关闭，监听地址默认仅限本机。 |
| 2 | 文件 `[observability]` 配置段 | 只有文件中实际存在的字段会替换默认值。 |
| 3 | 实际存在的环境变量 | 只有已设置的环境变量会替换对应的已解析字段。 |

缺失的环境变量不会产生默认覆盖。例如，未设置
`ROCKETMQ_METRICS_ENABLED` 时，文件中的 metrics 导出器保持有效。

## TOML 观测配置段

所有文件字段都使用 camelCase。将以下根级配置段合并到已有的规范 Broker TOML 中，保留原有 `broker` 与 `store` 配置。这是观测配置节选，不是完整部署文件。空映射可避免将凭据写入共享示例。

```toml
[observability]
environment = "production"
serviceInstanceId = "broker-a-0"
resourceAttributes = { "deployment.zone" = "az-a", "deployment.rack" = "rack-1" }

[observability.metrics]
exporter = "otlp_grpc"
exportIntervalMillis = 5000
exportTimeoutMillis = 3000
cardinalityLimit = 10000
sampleRatio = 1.0
topicLabelEnabled = true
consumerGroupLabelEnabled = true

[observability.traces]
exporter = "otlp_grpc"
sampleRatio = 0.01
propagateContext = true
recordMessageId = false
recordMessageKeys = false
recordBodySize = true

[observability.logs]
exporter = "otlp_grpc"

[observability.otlp]
endpoint = "http://otel-collector.observability.svc.cluster.local:4317"
protocol = "grpc"
headers = {}
timeoutMillis = 3000

[observability.prometheus]
host = "127.0.0.1"
port = 5557
path = "/metrics"
```

## 配置文件格式

1.0.0 Broker 可执行程序通过 `--config-format toml|properties` 接受规范 TOML 或 Java properties，不加载 YAML。请将上述 TOML 配置段与 [Broker 配置](./broker-config.md)配套使用。共享 Rust 数据结构的 YAML 表达，包括旧 `broker_observability.yaml` 示例，不能直接传给当前 Broker 启动器。

Kubernetes values、Collector 和 Prometheus 配置各自使用对应工具的 YAML 格式，不能与 Broker 服务文件互换。完整本地集成流程见[监控指南](../operations/monitoring.md)。

## 导出器和文件字段

| 信号 | 导出器取值 |
| --- | --- |
| Metrics | `disable`、`otlp_grpc`、`prometheus`、`log` |
| Traces | `disable`、`otlp_grpc`、`log` |
| Logs | `disable`、`otlp_grpc`、`log` |

`otlp` 由所有 OTLP 信号共享。目前只实现了 OTLP gRPC，因此启用 OTLP
导出器时必须设置 `protocol = "grpc"`。`prometheus` 用于配置直接 metrics
监听器。

## 环境变量映射

| 环境变量 | 解析后的设置 |
| --- | --- |
| `ROCKETMQ_METRICS_ENABLED` | 启用或关闭最终的 metrics 选择。 |
| `ROCKETMQ_METRICS_EXPORTER` | `observability.metrics.exporter` |
| `ROCKETMQ_METRICS_BIND_ADDR` | `observability.prometheus.host` 和 `port` |
| `ROCKETMQ_METRICS_PATH` | `observability.prometheus.path` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `observability.otlp.endpoint`；非空值会把 metrics、traces 和 logs 切换到 OTLP gRPC。 |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | 标准 endpoint 环境变量非空时，该值必须严格为 `grpc`。 |
| `ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO` | Broker 的 `observability.traces.sampleRatio` |
| `ROCKETMQ_MCP_TRACE_SAMPLE_RATIO` | MCP 的 `observability.traces.sampleRatio` |

标准 OTLP endpoint 缺失或为空时，文件配置保持不变。endpoint 非空但未同时
设置 `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` 时，服务会启动失败。

`ROCKETMQ_RELEASE_COMMIT` 和 `ROCKETMQ_RELEASE_NONCE` 仍然属于构建及
进程身份输入，不是文件字段。部署可以保留 `OTEL_SERVICE_NAME`，但本解析器
会特意忽略它。每个服务的组合根负责设置 `service_name`、`service_namespace`、
`node_type` 和 `node_id`；文件及环境变量均无法修改这些字段。

## 从扁平服务字段迁移

Broker 和 Controller 已不再接受旧版扁平 telemetry 字段。请删除这些字段，
并将其值迁移到嵌套的 `[observability]` 配置段。不得同时配置两种形式：
结构化配置是唯一的文件配置接口。

Helm chart 默认把 `global.observability.environmentOverridesEnabled` 设为
`false`。此模式始终注入 release identity 环境变量，但不会注入
`ROCKETMQ_METRICS_*`、`OTEL_EXPORTER_OTLP_ENDPOINT` 或
`OTEL_EXPORTER_OTLP_PROTOCOL`，因此 ConfigMap 中的结构化文件选择可以真实
控制全部关闭、OTLP、日志及混合信号配置。

stock Helm chart 的 `global.observability.metricsExporter` 仅接受
`disable`、`otlp_grpc` 和 `log`，并默认关闭 metrics Service。当前生产镜像并未
为五类服务一致编译直接 Prometheus 导出器，因此 stock 全局 schema 刻意不提供
`prometheus` 选项。若服务使用对应 feature 自定义编译（目前为 Broker 或
Controller），仍可在该服务的文件配置中选择直接 Prometheus；此类部署需要在
stock 全局选择器之外自行配置并暴露对应工作负载。

只有需要保留旧版环境变量驱动的部署行为时，才应显式开启该选项：

```yaml
global:
  observability:
    environmentOverridesEnabled: true
```

兼容模式会注入 metrics 环境变量及解析后的 OTLP endpoint，并同时设置
`OTEL_EXPORTER_OTLP_PROTOCOL=grpc`。这些实际存在的环境变量优先级高于
ConfigMap 值。ConfigMap 与兼容环境变量使用同一套新旧 endpoint alias
解析结果。

## 校验和敏感信息处理

解析器对以下配置采用关闭式失败：无效采样率、值为零的间隔或限制、非规范
Prometheus 路径、无效监听地址、启用 OTLP 时 endpoint 为空，以及不受支持的
OTLP 协议。错误只包含字段名和约束，不包含 endpoint、header 或资源属性的值。
启动日志同样不会输出这些原始值。

不得在 Kubernetes ConfigMap 中保存 authorization header 或 token。公开示例
应保留 `headers = {}`，敏感配置应通过受限的 Secret 后端文件或 Collector
认证机制提供。不得把消息 ID、trace ID、偏移量、request ID 或事务 ID 用作
metrics 标签。

## 信号说明

- 主题和消费者组标签受 `cardinalityLimit` 限制；超出限制的值会归一化为
  `other`。
- Trace 的消息 ID 和 key 默认关闭，因为它们具有高基数。消息体大小只记录大小。
- 消息传播使用小写 W3C 属性键 `traceparent` 与 `tracestate`，Rust 常量 `TRACEPARENT` 和 `TRACESTATE` 的值对应这些键。内置传播器不会将任意 OpenTelemetry `baggage` 写入消息属性；`traceparent` 最多允许 128 字节，`tracestate` 最多允许 512 字节。
- 直接 Prometheus endpoint 默认为
  `http://127.0.0.1:5557/metrics`。

本地 Collector 示例位于 `distribution/config`：

```bash
otelcol-contrib --config distribution/config/otel-collector-observability.yaml
prometheus --config.file=distribution/config/prometheus-observability.yaml
```

## 运维阅读入口

本页保留配置参考。安装采集器、解释指标和调查信号缺失，请阅读[监控](../operations/monitoring.md)；所有权和关闭语义见[错误与可观测性设计](../architecture/errors-observability.md)。

来源：[Broker 文件加载](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/bin/broker_bootstrap_server.rs)、[共享配置解析器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/resolver.rs)、[消息上下文传播](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/propagation.rs)。
