# Observability Logs

Logs are bridged from `tracing` to OpenTelemetry logs with
`opentelemetry-appender-tracing`. The bridge can attach logs to the current
trace/span context when tracing is active in the same subscriber.

## Broker Configuration

```yaml
logExporterType: otlp_grpc
```

Use `logExporterType: log` for local debugging. Use `disable` to leave logs
out of the OpenTelemetry pipeline.

## Feature Flags

| Feature | Purpose |
| --- | --- |
| `otel-logs` | Enable OpenTelemetry logs provider and tracing bridge |
| `otlp-logs` | Enable OTLP gRPC logs exporter |

For correlated traces and logs, run the broker with both trace and log
features:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features "otlp-traces,otlp-logs"
```

The local Collector config includes a logs pipeline that writes to the debug
exporter.
