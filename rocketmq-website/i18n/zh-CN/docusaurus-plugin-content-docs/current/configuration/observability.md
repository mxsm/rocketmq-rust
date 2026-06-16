---
sidebar_position: 4
title: 可观测性
---

# RocketMQ Rust 可观测性

RocketMQ Rust 的可观测性能力集中在 `rocketmq-observability` crate 中。
该 crate 默认关闭，每类信号都需要通过 Cargo feature 和 Broker 配置启用。

## 信号

| 信号 | Broker feature | 导出器 |
| --- | --- | --- |
| 指标 | `otel-metrics`, `otlp-metrics`, `prometheus` | disable, OTLP gRPC, Prometheus, log |
| 链路追踪 | `otel-traces`, `otlp-traces` | disable, OTLP gRPC, log |
| 日志 | `otel-logs`, `otlp-logs` | disable, OTLP gRPC, log |

便捷 feature `observability` 会启用指标和链路追踪。日志保持独立，
应用可以按需显式启用。

## Broker 配置

Broker 可观测性配置沿用现有 Broker 配置模型，字段使用 camelCase。
可复制的配置片段放在现有示例目录：
`rocketmq-example/examples/broker_observability.yaml`。

```yaml
metricsExporterType: otlp_grpc
metricsExportIntervalMillis: 5000
metricsCardinalityLimit: 10000
metricsTopicLabelEnabled: true
metricsConsumerGroupLabelEnabled: true
otlpExporterEndpoint: http://127.0.0.1:4317
otlpExporterHeaders: authorization:Bearer token,tenant:rocketmq
otlpExporterTimeoutMillis: 3000
traceExporterType: otlp_grpc
traceSampleRatio: 0.01
tracePropagateContext: true
traceRecordMessageId: false
traceRecordMessageKeys: false
traceRecordBodySize: true
logExporterType: otlp_grpc
observabilityEnvironment: dev
observabilityServiceInstanceId: broker-a-0
observabilityResourceAttributes: zone:az-a,rack:rack-1
```

有效的导出器取值：

| 字段 | 取值 |
| --- | --- |
| `metricsExporterType` | `disable`, `otlp_grpc`, `prom`, `log` |
| `traceExporterType` | `disable`, `otlp_grpc`, `log` |
| `logExporterType` | `disable`, `otlp_grpc`, `log` |

OTLP 设置由指标、链路追踪和日志共享。Header 和资源属性使用逗号分隔的
`key:value` 键值对。

## 指标

Broker 指标通过 `BrokerMetricsManager` 记录，在启用指标时委托给
`rocketmq-observability`。

| 指标 | 类型 | 单位 | 含义 |
| --- | --- | --- | --- |
| `rocketmq_messages_in_total` | counter | messages | Broker 接收的消息数 |
| `rocketmq_messages_out_total` | counter | messages | Broker 投递出的消息数 |
| `rocketmq_throughput_in_total` | counter | bytes | Broker 入站吞吐量 |
| `rocketmq_throughput_out_total` | counter | bytes | Broker 出站吞吐量 |
| `rocketmq_message_size` | histogram | bytes | 消息体大小分布 |
| `rocketmq_send_message_latency` | histogram | ms | 发送消息处理延迟 |
| `rocketmq_metrics_label_dropped_total` | counter | labels | 被基数保护归一化的标签数 |

安全的低基数标签包括 `cluster`、`node_type`、`node_id`、`topic`、
`consumer_group` 和 `invocation_status`。不要把消息 ID、trace ID、offset、
request ID 或事务 ID 加入指标标签。

Broker 标签基数通过以下配置控制：

```yaml
metricsCardinalityLimit: 10000
metricsTopicLabelEnabled: true
metricsConsumerGroupLabelEnabled: true
```

当 topic 或 consumer group 超过限制时，标签值会被归一化为 `other`，
同时 `rocketmq_metrics_label_dropped_total` 会带着低基数 `label_key`
属性递增。

## 本地 Collector

OpenTelemetry Collector 和 Prometheus 配置文件放在现有的 distribution
配置目录中：

```bash
otelcol-contrib --config distribution/config/otel-collector-observability.yaml
prometheus --config.file=distribution/config/prometheus-observability.yaml
```

本地 Collector 配置会在 `4317` 接收 OTLP gRPC，在 `4318` 接收 OTLP HTTP，
在 `9464` 暴露 Prometheus 指标，并把指标、链路追踪和日志写入 Collector
debug exporter。提供的 Prometheus 配置会抓取 `127.0.0.1:9464`。

## Prometheus

可以让 Broker 通过 OTLP 将指标发送到 OpenTelemetry Collector，再由
Prometheus 抓取 Collector 的 Prometheus exporter：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features otlp-metrics
```

Broker 也可以直接暴露 `/metrics`：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features prometheus
curl http://127.0.0.1:5557/metrics
```

直接导出器配置：

```yaml
metricsExporterType: prom
metricsPromExporterHost: 127.0.0.1
metricsPromExporterPort: 5557
metricsPromExporterPath: /metrics
```

从 `distribution/config/grafana-rocketmq-broker-dashboard.json` 导入 Grafana
dashboard，并选择 Prometheus 数据源。

## 链路追踪

链路追踪使用 `tracing`、`tracing-opentelemetry`，并通过 RocketMQ 消息属性
传播标准 W3C trace context。

```yaml
traceExporterType: otlp_grpc
traceSampleRatio: 0.01
tracePropagateContext: true
traceRecordMessageId: false
traceRecordMessageKeys: false
traceRecordBodySize: true
```

`traceRecordMessageId` 和 `traceRecordMessageKeys` 默认保持关闭，以避免记录
高基数字段。`traceRecordBodySize` 默认开启，因为它只记录 payload 大小。

| 配置 | Span 属性 |
| --- | --- |
| `traceRecordMessageId` | `messaging.message.id` |
| `traceRecordMessageKeys` | `messaging.rocketmq.message.keys` |
| `traceRecordBodySize` | `messaging.message.body.size` |

当前 span 名称：

| Span | 范围 |
| --- | --- |
| `RocketMQ PRODUCER SEND` | Producer 发送路径 |
| `RocketMQ BROKER RECEIVE_SEND` | Broker 发送消息请求处理 |
| `RocketMQ STORE APPEND` | Store 追加路径 |
| `RocketMQ CONSUMER PROCESS` | Consumer listener 执行 |

Consumer ACK、retry 和 DLQ 结果会记录为 span event。

Context propagation 使用以下消息属性：

| 属性 | 用途 |
| --- | --- |
| `traceparent` | W3C trace parent |
| `tracestate` | W3C trace state |
| `baggage` | W3C baggage |

`traceSampleRatio` 接受 `0.0` 到 `1.0` 之间的值。

## 日志

日志通过 `opentelemetry-appender-tracing` 从 `tracing` 桥接到 OpenTelemetry
logs。当 tracing 在同一个 subscriber 中启用时，该桥接可以把日志关联到当前
trace/span context。

```yaml
logExporterType: otlp_grpc
```

本地调试可以使用 `logExporterType: log`。使用 `disable` 可以让日志不进入
OpenTelemetry pipeline。

如果需要关联链路追踪和日志，请同时启用 trace 和 log feature 运行 Broker：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-traces,otlp-logs"
```

本地 Collector 配置包含日志 pipeline，会将日志写入 debug exporter。

## 示例命令

通过 OTLP 导出指标：

```bash
cargo run -p rocketmq-observability --example broker_metrics --features otlp-metrics
```

Broker 同时启用 OTLP 指标、链路追踪和日志：

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-metrics,otlp-traces,otlp-logs"
```
