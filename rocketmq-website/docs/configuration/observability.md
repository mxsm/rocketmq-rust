---
sidebar_position: 4
title: Observability
---

# RocketMQ Rust Observability

Broker, NameServer, Controller, Proxy, and RocketMQ MCP use one canonical
`observability` file section. The shared resolver merges service defaults,
file values, and only environment variables that are actually present.

## Activation model

A signal runs only when both gates are open:

| Gate | Requirement |
| --- | --- |
| Build time | Compile the service with the feature for that signal and exporter. |
| Runtime | Select a non-`disable` exporter in `observability` or through a supported environment override. |

Feature names differ by service and must be selected from that service's
`Cargo.toml`:

| Service | Convenience feature | Signal and exporter features |
| --- | --- | --- |
| Broker | `observability` enables metrics and traces | `otel-metrics`, `otlp-metrics`, `prometheus`, `metrics-prometheus`, `otel-traces`, `otlp-traces`, `otel-logs`, `otlp-logs` |
| NameServer | `observability` enables metrics and traces | `otel-metrics`, `otlp-metrics`, `otel-traces`, `otlp-traces`, `otel-logs`, `otlp-logs` |
| Controller | None | `metrics`, `metrics-otlp`, `metrics-prometheus`, `otel-traces`, `otlp-traces`, `otel-logs`, `otlp-logs` |
| Proxy | `observability` enables metrics only | `otlp-metrics`, `otel-traces`, `otlp-traces`, `otel-logs`, `otlp-logs` |
| MCP | `observability` enables metrics and traces | `otlp` adds OTLP metrics, traces, and logs |

In particular, Controller does not define an `observability` convenience
feature. OTLP logs remain an explicit build-time choice. Examples:

```bash
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-metrics,otlp-traces,otlp-logs"
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features prometheus
```

Enabling a Cargo feature does not enable export by itself, and selecting an
exporter at runtime cannot add code that was not compiled.

## Precedence

Resolution is deterministic from lowest to highest precedence:

| Priority | Source | Behavior |
| --- | --- | --- |
| 1 | Service defaults | Signals are disabled and listeners are local-only. |
| 2 | File `[observability]` section | Only fields present in the file replace defaults. |
| 3 | Present environment variables | Only variables that exist replace the matching resolved fields. |

A missing environment variable never supplies a fallback override. For example,
if `ROCKETMQ_METRICS_ENABLED` is absent, the file's metrics exporter remains
effective.

## TOML observability section

All file keys use camelCase. Merge these root-level sections into an existing canonical Broker TOML, preserving its `broker` and `store` configuration. This is an observability excerpt, not a complete deployment file. Empty maps avoid putting credentials in a shared example.

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

## Configuration file formats

The 1.0.0 Broker executable accepts canonical TOML or Java properties through `--config-format toml|properties`; it does not load YAML. Use the TOML section above with [Broker configuration](./broker-config.md). A YAML representation of the shared Rust data structure, including the older `broker_observability.yaml` example, is not a file that can be passed to the current Broker launcher.

Kubernetes values and collector/Prometheus configuration use their own YAML formats. They configure those tools; they are not interchangeable with the Broker service file. See [monitoring](../operations/monitoring.md) for a complete local integration procedure.

## Exporters and file fields

| Signal | Exporter values |
| --- | --- |
| Metrics | `disable`, `otlp_grpc`, `prometheus`, `log` |
| Traces | `disable`, `otlp_grpc`, `log` |
| Logs | `disable`, `otlp_grpc`, `log` |

`otlp` is shared by all OTLP signals. Only OTLP gRPC is currently
implemented, so an active OTLP exporter requires `protocol = "grpc"`.
`prometheus` configures the direct metrics listener.

## Environment mapping

| Environment variable | Resolved setting |
| --- | --- |
| `ROCKETMQ_METRICS_ENABLED` | Enables or disables the final metrics selection. |
| `ROCKETMQ_METRICS_EXPORTER` | `observability.metrics.exporter` |
| `ROCKETMQ_METRICS_BIND_ADDR` | `observability.prometheus.host` and `port` |
| `ROCKETMQ_METRICS_PATH` | `observability.prometheus.path` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `observability.otlp.endpoint`; a non-empty value switches metrics, traces, and logs to OTLP gRPC. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Must be exactly `grpc` when the standard endpoint variable is non-empty. |
| `ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO` | Broker `observability.traces.sampleRatio` |
| `ROCKETMQ_MCP_TRACE_SAMPLE_RATIO` | MCP `observability.traces.sampleRatio` |

A missing or blank standard OTLP endpoint leaves the file configuration
unchanged. A non-empty endpoint without `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`
fails startup.

`ROCKETMQ_RELEASE_COMMIT` and `ROCKETMQ_RELEASE_NONCE` remain build/process
identity inputs and are not file fields. A deployment may retain
`OTEL_SERVICE_NAME`, but this resolver deliberately ignores it. Each service
composition root owns `service_name`, `service_namespace`, `node_type`, and
`node_id`; file and environment input cannot change them.

## Migration from flat service fields

Broker and Controller no longer accept their legacy flat telemetry fields.
Remove those fields and migrate their values into the nested
`[observability]` sections. Do not configure both forms: the structured
configuration is the only file interface.

The Helm chart defaults
`global.observability.environmentOverridesEnabled` to `false`. In this mode it
always injects release identity variables, but does not inject
`ROCKETMQ_METRICS_*`, `OTEL_EXPORTER_OTLP_ENDPOINT`, or
`OTEL_EXPORTER_OTLP_PROTOCOL`. The ConfigMap's structured file selection is
therefore effective for all-disabled, OTLP, log, and mixed-signal
configurations.

The stock chart accepts only `disable`, `otlp_grpc`, and `log` for
`global.observability.metricsExporter`, and metrics Services are disabled by
default. Its production images do not compile the direct Prometheus exporter
consistently across all five services, so `prometheus` is intentionally not a
stock global schema option. Direct Prometheus remains available through a
service's file configuration when that service is custom-built with the
corresponding feature (currently Broker or Controller); such a deployment must
configure and expose the custom workload outside the stock global selector.

Set the flag to `true` only while preserving the previous environment-driven
deployment behavior:

```yaml
global:
  observability:
    environmentOverridesEnabled: true
```

Compatibility mode injects the metrics variables and the resolved OTLP
endpoint with `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`. These present environment
variables take precedence over ConfigMap values. Both the ConfigMap and the
compatibility variables use the same structured/legacy endpoint alias
resolution.

## Validation and secret handling

The resolver fails closed for invalid sample ratios, zero intervals or limits,
non-canonical Prometheus paths, invalid listener addresses, blank active OTLP
endpoints, and unsupported OTLP protocols. Errors name the field and constraint
but do not include endpoint, header, or resource-attribute values. Startup logs
also avoid printing those raw values.

Do not place authorization headers or tokens in a Kubernetes ConfigMap. Keep
`headers = {}` in public examples and provide sensitive configuration through
a restricted secret-backed file or the collector's authentication mechanism.
Never use message IDs, trace IDs, offsets, request IDs, or transaction IDs as
metric labels.

## Signal notes

- Topic and consumer-group labels are bounded by `cardinalityLimit`; overflow
  values are normalized to `other`.
- Trace message IDs and keys are disabled by default because they are
  high-cardinality. Body-size recording stores only the size.
- Message propagation uses the lowercase W3C property keys `traceparent` and `tracestate`. The Rust constants `TRACEPARENT` and `TRACESTATE` resolve to those keys. The built-in propagator excludes arbitrary OpenTelemetry `baggage` from message properties and admits at most 128 bytes for `traceparent` and 512 bytes for `tracestate`.
- The direct Prometheus endpoint defaults to
  `http://127.0.0.1:5557/metrics`.

Local collector examples remain under `distribution/config`:

```bash
otelcol-contrib --config distribution/config/otel-collector-observability.yaml
prometheus --config.file=distribution/config/prometheus-observability.yaml
```

## Operational context

This page is the configuration reference. Use [monitoring](../operations/monitoring.md) to install collectors, interpret metrics and investigate missing signals; [error and observability design](../architecture/errors-observability.md) explains ownership and shutdown.

Sources: [Broker file loading](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/bin/broker_bootstrap_server.rs), [shared configuration resolver](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/resolver.rs), [message propagation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/propagation.rs).
