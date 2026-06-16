# RocketMQ Rust OpenTelemetry

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
fields:

```yaml
brokerIdentity:
  brokerName: broker-a
  brokerClusterName: DefaultCluster
  brokerId: 0

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

Valid exporter values are:

| Field | Values |
| --- | --- |
| `metricsExporterType` | `disable`, `otlp_grpc`, `prom`, `log` |
| `traceExporterType` | `disable`, `otlp_grpc`, `log` |
| `logExporterType` | `disable`, `otlp_grpc`, `log` |

OTLP settings are shared by metrics, traces, and logs. Header and resource
attribute values use comma-separated `key:value` pairs.

## Local Collector

Start an OpenTelemetry Collector with:

```bash
otelcol-contrib --config etc/observability/otel-collector-config.yaml
```

The local config accepts OTLP gRPC on `4317`, OTLP HTTP on `4318`, exports
metrics to Prometheus on `9464`, and writes metrics, traces, and logs to the
Collector debug exporter.

## Example Commands

Metrics through OTLP:

```bash
cargo run -p rocketmq-observability --example broker_metrics --features otlp-metrics
```

Broker with OTLP metrics, traces, and logs:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-metrics,otlp-traces,otlp-logs"
```

Prometheus direct exporter:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features prometheus
curl http://127.0.0.1:5557/metrics
```
