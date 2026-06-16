---
sidebar_position: 4
title: Observability
---

# RocketMQ Rust Observability

RocketMQ Rust observability is centered in the `rocketmq-observability` crate.
The crate is disabled by default and each signal is enabled by Cargo feature
and broker configuration.

## Signals

| Signal | Broker feature | Exporters |
| --- | --- | --- |
| Metrics | `otel-metrics`, `otlp-metrics`, `prometheus` | disable, OTLP gRPC, Prometheus, log |
| Traces | `otel-traces`, `otlp-traces` | disable, OTLP gRPC, log |
| Logs | `otel-logs`, `otlp-logs` | disable, OTLP gRPC, log |

The convenience `observability` feature enables metrics and traces. Logs are
kept separate so applications can opt in explicitly.

## Broker Configuration

Broker observability uses the existing broker config model with camelCase
fields. A copyable fragment is kept with the existing examples at
`rocketmq-example/examples/broker_observability.yaml`.

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

Valid exporter values:

| Field | Values |
| --- | --- |
| `metricsExporterType` | `disable`, `otlp_grpc`, `prom`, `log` |
| `traceExporterType` | `disable`, `otlp_grpc`, `log` |
| `logExporterType` | `disable`, `otlp_grpc`, `log` |

OTLP settings are shared by metrics, traces, and logs. Header and resource
attribute values use comma-separated `key:value` pairs.

## Metrics

Broker metrics are recorded through `BrokerMetricsManager` and delegated to
`rocketmq-observability` when metrics are enabled.

| Metric | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `rocketmq_messages_in_total` | counter | messages | Messages accepted by the broker |
| `rocketmq_messages_out_total` | counter | messages | Messages delivered from the broker |
| `rocketmq_throughput_in_total` | counter | bytes | Incoming broker throughput |
| `rocketmq_throughput_out_total` | counter | bytes | Outgoing broker throughput |
| `rocketmq_message_size` | histogram | bytes | Message body size distribution |
| `rocketmq_send_message_latency` | histogram | ms | Send-message processing latency |
| `rocketmq_metrics_label_dropped_total` | counter | labels | Labels normalized by the cardinality guard |

Safe low-cardinality labels include `cluster`, `node_type`, `node_id`,
`topic`, `consumer_group`, and `invocation_status`. Do not add message IDs,
trace IDs, offsets, request IDs, or transaction IDs as metric labels.

Broker label cardinality is controlled through:

```yaml
metricsCardinalityLimit: 10000
metricsTopicLabelEnabled: true
metricsConsumerGroupLabelEnabled: true
```

When the topic or consumer group limit is exceeded, the label value is
normalized to `other`, and `rocketmq_metrics_label_dropped_total` is incremented
with the low-cardinality `label_key` attribute.

## Local Collector

OpenTelemetry Collector and Prometheus config files live in the existing
distribution config directory:

```bash
otelcol-contrib --config distribution/config/otel-collector-observability.yaml
prometheus --config.file=distribution/config/prometheus-observability.yaml
```

The local Collector config accepts OTLP gRPC on `4317`, OTLP HTTP on `4318`,
exports metrics to Prometheus on `9464`, and writes metrics, traces, and logs to
the Collector debug exporter. The provided Prometheus config scrapes
`127.0.0.1:9464`.

## Prometheus

Use OTLP metrics from the broker to the OpenTelemetry Collector, then scrape
the Collector Prometheus exporter:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features otlp-metrics
```

The broker can also expose `/metrics` directly:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features prometheus
curl http://127.0.0.1:5557/metrics
```

Direct exporter config:

```yaml
metricsExporterType: prom
metricsPromExporterHost: 127.0.0.1
metricsPromExporterPort: 5557
metricsPromExporterPath: /metrics
```

Import the Grafana dashboard from
`distribution/config/grafana-rocketmq-broker-dashboard.json` and select a
Prometheus data source.

## Tracing

Tracing uses `tracing`, `tracing-opentelemetry`, and standard W3C trace context
propagation through RocketMQ message properties.

```yaml
traceExporterType: otlp_grpc
traceSampleRatio: 0.01
tracePropagateContext: true
traceRecordMessageId: false
traceRecordMessageKeys: false
traceRecordBodySize: true
```

`traceRecordMessageId` and `traceRecordMessageKeys` stay disabled by default to
avoid recording high-cardinality fields. `traceRecordBodySize` is enabled by
default because it records only the payload size.

| Config | Span attribute |
| --- | --- |
| `traceRecordMessageId` | `messaging.message.id` |
| `traceRecordMessageKeys` | `messaging.rocketmq.message.keys` |
| `traceRecordBodySize` | `messaging.message.body.size` |

Current span names:

| Span | Scope |
| --- | --- |
| `RocketMQ PRODUCER SEND` | Producer send path |
| `RocketMQ BROKER RECEIVE_SEND` | Broker send-message request processing |
| `RocketMQ STORE APPEND` | Store append path |
| `RocketMQ CONSUMER PROCESS` | Consumer listener execution |

Consumer ACK, retry, and DLQ outcomes are recorded as span events.

Context propagation uses these message properties:

| Property | Purpose |
| --- | --- |
| `traceparent` | W3C trace parent |
| `tracestate` | W3C trace state |
| `baggage` | W3C baggage |

`traceSampleRatio` accepts values from `0.0` to `1.0`.

## Logs

Logs are bridged from `tracing` to OpenTelemetry logs with
`opentelemetry-appender-tracing`. The bridge can attach logs to the current
trace/span context when tracing is active in the same subscriber.

```yaml
logExporterType: otlp_grpc
```

Use `logExporterType: log` for local debugging. Use `disable` to leave logs out
of the OpenTelemetry pipeline.

For correlated traces and logs, run the broker with both trace and log features:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-traces,otlp-logs"
```

The local Collector config includes a logs pipeline that writes to the debug
exporter.

## Example Commands

Metrics through OTLP:

```bash
cargo run -p rocketmq-observability --example broker_metrics --features otlp-metrics
```

Broker with OTLP metrics, traces, and logs:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-metrics,otlp-traces,otlp-logs"
```
