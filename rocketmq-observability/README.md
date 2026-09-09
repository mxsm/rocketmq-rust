# rocketmq-observability

[English](README.md) | [简体中文](README-zh_cn.md)

Shared telemetry configuration, local logging, OpenTelemetry integration and exporter lifecycle
for RocketMQ-Rust. Optional OpenTelemetry features are off by default; console/file logging,
configuration resolution, statistics and no-op telemetry handles remain available.

## Ownership and public API

```text
Application RuntimeOwner
  -> TelemetryRuntimeGuard (providers, logging guards, exporter shutdown)
     -> TelemetryHandle clones -> component metrics and trace policy
  -> owned exporter tasks (for example, Prometheus HTTP)
```

The application retains the non-cloneable `TelemetryRuntimeGuard`. Business components receive
`TelemetryHandle` clones and construct role-specific recorders with `from_handle`.
A handle cannot shut down providers. Closing the guard makes surviving handles stop recording.
SDK providers are owned by the guard; initialization does not install global OpenTelemetry
meter or tracer providers. The tracing subscriber and optional text-map propagator are process-level state.

Import configuration, bootstrap functions, handles and propagation helpers from the crate root.
Modules such as `config`, `init`, `logging` and `propagation` are private implementation details.
The public `metrics`, `trace`, `logs`, `semantic`, `statistics` and `stats` modules expose their
respective instrumentation and statistics APIs.

## Quick start

For a sibling application, adjust these paths:

```toml
[dependencies]
rocketmq-observability = { path = "../rocketmq-observability", features = ["otlp-metrics", "otlp-traces"] }
rocketmq-runtime = { path = "../rocketmq-runtime" }
```

This lifecycle example requires the OTLP features above and a collector at the configured endpoint:

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

Start application services with the handle and stop them before shutting down telemetry.
Use `install_global_with_service_context` for service entrypoints that need console/file logging
and OpenTelemetry layers in one subscriber. The synchronous `install_global` is available for
configurations without runtime-owned exporter tasks. `init_observability` and
`init_observability_with_service_context` retain the telemetry-only compatibility initialization path;
they return `TelemetryRuntimeGuard`, not a separate `TelemetryGuard` type.

Prometheus needs the scoped initialization API and
`shutdown_with_service_context` so its listener and tasks are awaited. Shutdown returns
`TelemetryShutdownReport`; inspect `is_healthy()` to include task and provider failures.
Dropping a guard does not replace explicit flushing and shutdown.

## Configuration and logging

- `TelemetryBootstrapConfig` combines `ObservabilityConfig` and `LoggingConfig`.
- `ObservabilityOverrides` and `resolve_telemetry_from_env` let service entrypoints merge
  structured file settings with their declared environment variables.
- `LogFilterResolver` selects a runtime override, then CLI, environment, configuration and fallback.
  Callers supply those inputs explicitly; `install_global` alone does not read `RUST_LOG`.
- Console/file sinks, rotation, bounded nonblocking logging and reload configuration are
  separate from the OpenTelemetry logs exporter.
- `SubscriberInstallPolicy::Required` fails when a needed subscriber cannot be installed;
  `BestEffort` records the installation status. Inspect status when embedding in a host
  that already owns a subscriber.
- Runtime requests for an exporter whose feature was not compiled return a typed
  `observability.feature.disabled` error.

For service configuration, see the [observability guide](../rocketmq-website/docs/configuration/observability.md).

## Features and exporters

The default feature set is empty.

| Feature | Effect |
| --- | --- |
| `observability` | Convenience alias for `otel-metrics` and `otel-traces`. |
| `otel-metrics` | Metric instruments and SDK providers. |
| `otel-traces` | Tracing integration, span helpers and message context propagation. |
| `otel-logs` | OpenTelemetry logs and the tracing bridge. |
| `otlp-grpc` | Shared OTLP transport dependency; select a signal feature as well. |
| `otlp-metrics`, `otlp-traces`, `otlp-logs` | Corresponding signal plus OTLP gRPC export. |
| `prometheus` | Metrics reader and an HTTP scrape endpoint; requires an injected service context. |
| `stdout` | Compatibility flag; runtime exporter selection controls log output. |

Runtime selections include `MetricsExporter::{Log, OtlpGrpc, Prometheus}`,
`TraceExporter::{Log, OtlpGrpc}` and `LogsExporter::{Log, OtlpGrpc}`.
A local log exporter still needs its corresponding `otel-*` feature.

## Instrumentation

Role recorders cover broker, client, transport, NameServer, Controller, proxy, store, tiered
store, runtime, dashboard and SRE components. Label policies bound topic/group cardinality.
Use recorder instances derived from the injected handle; metric SDK constructors that take
a meter are available where the relevant feature and API expose them.

With `otel-traces`, root exports such as `inject_current_context_with_handle`,
`extract_context_with_handle` and `set_span_parent_from_properties_with_handle` propagate
context using message property maps and the handle's trace policy. The shared
`TRACEPARENT` and `TRACESTATE` constants identify the wire property names.

`rocketmq-client-rust/observability` enables client traces;
`observability-metrics` enables client metrics. Client metrics over OTLP also require
a direct `rocketmq-observability/otlp-metrics` dependency; the client has no feature with that name.

## Source and validation

See [exports](src/lib.rs), [bootstrap and shutdown](src/logging.rs),
[provider creation](src/init.rs), [capability handles](src/handle.rs),
[configuration resolution](src/resolver.rs), [metrics](src/metrics) and
[exporter outage handling](src/exporter/outage.rs).

Select checks for the actual features changed:

```bash
cargo fmt -p rocketmq-observability -- --check
cargo test -p rocketmq-observability --lib
cargo test -p rocketmq-observability --lib --features otel-metrics,otel-traces
cargo bench -p rocketmq-observability --bench observability_hot_path
```

README examples should be compiled when changed. Exporter integration needs its own
collector or scrape tests; a compile check alone does not prove delivery.

[Apache License 2.0](../LICENSE-APACHE).
