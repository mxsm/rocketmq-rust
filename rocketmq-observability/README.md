# rocketmq-observability

[中文](README-zh_cn.md)

`rocketmq-observability` is the shared observability foundation for the
rocketmq-rust workspace. It centralizes configuration, OpenTelemetry provider
lifecycle, resource metadata, metric names, trace propagation, logs bridging,
and exporter wiring for RocketMQ components.

The crate is feature-gated and disabled by default. Runtime components keep
their own business instrumentation points, then use this crate to publish
metrics, traces, and logs through a consistent OpenTelemetry pipeline.

## What It Provides

- A single `ObservabilityConfig` for metrics, traces, logs, OTLP, Prometheus,
  resource attributes, sampling, and trace propagation.
- `init_observability` and `TelemetryGuard` for process-level telemetry
  initialization and shutdown.
- Role-specific metrics APIs for broker, client, store, remoting, nameserver,
  controller, and proxy.
- Trace helpers for message attributes, span names, and producer to consumer
  context propagation through RocketMQ message properties.
- Exporters for local log output, OTLP gRPC, and Prometheus scraping.
- No-op paths when optional telemetry features are not enabled.

For broker-level configuration and dashboard usage, see
`../rocketmq-website/docs/configuration/observability.md`.

## Architecture

![rocketmq-observability global design architecture](docs/observability-architecture.svg)

At a high level, applications and RocketMQ components build an
`ObservabilityConfig`, call `init_observability`, and keep the returned
`TelemetryGuard` alive. Components then record metrics and traces through the
role-specific APIs. The configured OpenTelemetry providers export data to local
logs, an OTLP collector, or a Prometheus endpoint.

## Quick Start

Enable only the signals and exporters you need.

```toml
[dependencies]
rocketmq-observability = {
  path = "../rocketmq-observability",
  features = ["otlp-metrics", "otlp-traces"]
}
```

Initialize telemetry once during process startup:

```rust
use rocketmq_observability::config::{MetricsExporter, ObservabilityConfig, TraceExporter};

fn main() -> Result<(), rocketmq_observability::ObservabilityError> {
    let mut config = ObservabilityConfig {
        enabled: true,
        service_name: "rocketmq-broker".to_string(),
        service_namespace: "rocketmq".to_string(),
        cluster: "DefaultCluster".to_string(),
        node_type: "broker".to_string(),
        node_id: "broker-a".to_string(),
        ..ObservabilityConfig::default()
    };

    config.metrics.enabled = true;
    config.metrics.exporter = MetricsExporter::OtlpGrpc;
    config.metrics.export_interval_millis = 5_000;

    config.traces.enabled = true;
    config.traces.exporter = TraceExporter::OtlpGrpc;
    config.traces.sample_ratio = 1.0;

    config.otlp.endpoint = "http://127.0.0.1:4317".to_string();

    let telemetry_guard = rocketmq_observability::init_observability(&config)?;

    // Start RocketMQ components and record telemetry.

    telemetry_guard.shutdown()
}
```

For local inspection without a collector, use `MetricsExporter::Log` and
`TraceExporter::Log` with `otel-metrics` and `otel-traces`.

## Runtime Flow

1. The embedding application builds `ObservabilityConfig`.
2. `init_observability` validates the config and applies trace message-field
   recording flags.
3. Enabled signal providers are created:
   `SdkMeterProvider`, `SdkTracerProvider`, and `SdkLoggerProvider`.
4. Global OpenTelemetry meter and tracer providers are installed.
5. Trace context propagators are installed when trace propagation is enabled.
6. `tracing_subscriber` layers are installed for traces and logs when possible.
7. Components record telemetry through `metrics::*`, `trace::*`,
   `propagation::*`, and normal `tracing` events.
8. `TelemetryGuard::shutdown` flushes and shuts down providers and the
   Prometheus HTTP endpoint.

Initialize this crate once per process. OpenTelemetry global providers and
`tracing_subscriber` are process-level state, so producer, consumer, broker,
and proxy code should record telemetry rather than independently initialize the
runtime.

## Feature Matrix

The default feature set is empty.

| Feature | Purpose |
| --- | --- |
| `observability` | Convenience feature for `otel-metrics` and `otel-traces`. |
| `otel-metrics` | Enables OpenTelemetry metrics APIs, SDK types, providers, and real metric instruments. |
| `otel-traces` | Enables OpenTelemetry tracing, tracing-opentelemetry, span helpers, and message context propagation. |
| `otel-logs` | Enables OpenTelemetry logs and the `tracing` to OpenTelemetry logs bridge. |
| `otlp-metrics` | Exports metrics through OTLP gRPC. Implies `otel-metrics` and `otlp-grpc`. |
| `otlp-traces` | Exports traces through OTLP gRPC. Implies `otel-traces` and `otlp-grpc`. |
| `otlp-logs` | Exports logs through OTLP gRPC. Implies `otel-logs` and `otlp-grpc`. |
| `prometheus` | Exports metrics through a Prometheus reader and HTTP scrape endpoint. Implies `otel-metrics`. |
| `stdout` | Compatibility feature. Runtime log output is selected through `MetricsExporter::Log`, `TraceExporter::Log`, or `LogsExporter::Log`. |

If runtime config requests an exporter whose feature is not enabled,
`init_observability` returns `ObservabilityError::FeatureDisabled`.

## Module Map

| Module | Responsibility |
| --- | --- |
| `config` | Serializable config model for global enablement, resources, metrics, traces, logs, OTLP, and Prometheus. |
| `init` | Validation, provider construction, global runtime installation, and shutdown lifecycle. |
| `resource` | OpenTelemetry resource metadata such as service name, namespace, version, environment, cluster, node type, and node id. |
| `semantic` | Shared metric names, label names, trace headers, and trace attribute names. |
| `metrics` | Role-specific metric wrappers and global recorders. |
| `metrics::labels` | Cardinality guard for labels such as topic and consumer group. |
| `trace` | Span names, tracing layer construction, and message attribute recording. |
| `propagation` | W3C `traceparent`, `tracestate`, and `baggage` injection/extraction through RocketMQ message properties. |
| `logs` | Bridge from `tracing` events to OpenTelemetry logs. |
| `exporter` | Local log output, OTLP gRPC, and Prometheus exporter implementations. |
| `attributes` | Lightweight base metric attributes for RocketMQ components. |
| `noop` | No-op counter and histogram helpers for disabled instrumentation paths. |

## Metrics Ownership

Metrics are grouped by RocketMQ role:

| Module | Metrics |
| --- | --- |
| `metrics::broker` | Message in/out, throughput in/out, message size, send latency, dropped metric labels. |
| `metrics::client` | Client send count/latency, consume count/latency, rebalance events. |
| `metrics::store` | Append latency, flush latency, dispatch latency, disk usage. |
| `metrics::remoting` | Request count, request latency, network bytes. |
| `metrics::namesrv` | Route request count/latency, broker registrations, active brokers. |
| `metrics::controller` | Election count/latency, leader changes, active brokers. |
| `metrics::proxy` | gRPC request count/latency, forward latency, active connections. |

Most role modules expose two styles:

- `init_global(meter)` plus `record_*` functions for simple global recorders.
- A `*Metrics::new(meter)` struct for components that want to own instruments
  directly.

When `otel-metrics` is disabled, role modules provide no-op behavior where the
API supports it.

## Tracing And Propagation

Tracing uses `tracing` spans and an OpenTelemetry tracing layer. Common span
names live in `trace::span_names`, while role modules expose domain-specific
constants.

Message propagation follows the W3C trace context model:

1. Producers inject the current context into RocketMQ message properties through
   `propagation::inject_current_context_into_message`.
2. Brokers and consumers extract parent context through
   `propagation::set_current_span_parent_from_message` or
   `set_span_parent_from_message`.
3. Optional fields such as message id, keys, and body size are recorded
   according to `TracesConfig`.

This makes producer, broker, store, and consumer spans join the same trace when
the embedding component enables trace propagation.

## Exporter Paths

| Runtime exporter | Required feature | Destination |
| --- | --- | --- |
| `MetricsExporter::Log` | `otel-metrics` | Periodic metrics snapshots to local log output. |
| `MetricsExporter::OtlpGrpc` | `otlp-metrics` | OTLP gRPC collector endpoint. |
| `MetricsExporter::Prometheus` | `prometheus` | HTTP scrape endpoint configured by `PrometheusConfig`. |
| `TraceExporter::Log` | `otel-traces` | Spans to local log output. |
| `TraceExporter::OtlpGrpc` | `otlp-traces` | OTLP gRPC collector endpoint. |
| `LogsExporter::Log` | `otel-logs` | OpenTelemetry logs to local log output. |
| `LogsExporter::OtlpGrpc` | `otlp-logs` | OTLP gRPC collector endpoint. |

## Client Integration Note

`rocketmq-client-rust` has separate feature gates for client instrumentation:

- `observability` enables trace instrumentation.
- `observability-metrics` enables client metrics instrumentation.
- `otlp-traces` forwards trace export support.

For client metrics over OTLP, the application must also depend on
`rocketmq-observability` with `otlp-metrics`, because the client crate does not
currently forward an `otlp-metrics` feature.

```toml
[dependencies]
rocketmq-client-rust = {
  path = "../rocketmq-client",
  features = ["observability-metrics", "otlp-traces"]
}

rocketmq-observability = {
  path = "../rocketmq-observability",
  features = ["otlp-metrics", "otlp-traces"]
}
```

## Development Checks

For documentation-only changes, no Cargo validation is required. For Rust code
changes in this crate, run the root workspace checks required by the repository:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```
