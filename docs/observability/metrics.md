# Observability Metrics

Broker metrics are recorded through `BrokerMetricsManager` and delegated to
`rocketmq-observability` when metrics are enabled.

## Core Broker Metrics

| Metric | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `rocketmq_messages_in_total` | counter | messages | Messages accepted by the broker |
| `rocketmq_messages_out_total` | counter | messages | Messages delivered from the broker |
| `rocketmq_throughput_in_total` | counter | bytes | Incoming broker throughput |
| `rocketmq_throughput_out_total` | counter | bytes | Outgoing broker throughput |
| `rocketmq_message_size` | histogram | bytes | Message body size distribution |
| `rocketmq_send_message_latency` | histogram | ms | Send-message processing latency |
| `rocketmq_metrics_label_dropped_total` | counter | labels | Labels normalized by the cardinality guard |

Additional broker metric constants remain in `rocketmq-broker` and continue to
use the `rocketmq_` prefix.

## Labels

Low-cardinality labels are safe for metrics:

| Label | Meaning |
| --- | --- |
| `cluster` | RocketMQ cluster |
| `node_type` | Node type such as `broker` |
| `node_id` | Broker identity |
| `topic` | Topic, guarded by cardinality settings |
| `consumer_group` | Consumer group, guarded by cardinality settings |
| `invocation_status` | Operation result |

Do not add message IDs, trace IDs, offsets, request IDs, or transaction IDs as
metric labels.

Broker label cardinality is controlled through:

```yaml
metricsCardinalityLimit: 10000
metricsTopicLabelEnabled: true
metricsConsumerGroupLabelEnabled: true
```

When the topic or consumer group limit is exceeded, the label value is
normalized to `other`, and `rocketmq_metrics_label_dropped_total` is incremented
with the low-cardinality `label_key` attribute.

## Exporters

OTLP metrics:

```yaml
metricsExporterType: otlp_grpc
metricsExportIntervalMillis: 5000
otlpExporterEndpoint: http://127.0.0.1:4317
otlpExporterHeaders: authorization:Bearer token,tenant:rocketmq
otlpExporterTimeoutMillis: 3000
```

Prometheus direct exporter:

```yaml
metricsExporterType: prom
metricsPromExporterHost: 127.0.0.1
metricsPromExporterPort: 5557
metricsPromExporterPath: /metrics
```

Local debug exporter:

```yaml
metricsExporterType: log
```

Disabled:

```yaml
metricsExporterType: disable
```
