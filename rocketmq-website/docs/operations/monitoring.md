---
title: "Monitor services and message progress"
---

Monitor the application's completion path and the services that support it. A healthy process can still have a full disk, insufficient synchronized replicas, a denied downstream request, or a consumer that is connected but makes no business progress. Use [Admin observations](admin.md) alongside logs, metrics, and traces.

## Choose the export path

Two independent choices activate a signal: the binary includes its Cargo feature, and runtime configuration selects an active exporter. A service convenience feature does not necessarily include every signal or exporter. Controller has no `observability` convenience feature; Proxy's convenience feature covers metrics only. The service-specific table and full parameter schema are in [observability configuration](../configuration/observability.md).

| Path | Use and limitation |
| --- | --- |
| Local console/file logging | Inspect startup, errors, lifecycle, and controlled diagnostics; separate from OTLP log export |
| OTLP gRPC to a collector | Centralize metrics/traces/logs compiled and selected for the service; verify each pipeline |
| Direct Prometheus | Use a service build supporting that exporter, currently Broker or Controller; expose the metrics listener deliberately |
| Log exporter | Inspect selected telemetry locally; not equivalent to a durable remote monitoring system |

Selecting an uncompiled exporter returns a typed feature-disabled error. A disabled signal can use no-op handles; the mere presence of metric names in source does not prove samples are exported.

## Exercise a local Broker-to-collector path

Use the local Broker/NameServer setup from [source startup](../getting-started/local-source.md). Stop the previous Broker before replacing its process. Build the desired signals:

```bash
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features otlp-metrics,otlp-traces,otlp-logs
```

Copy its Broker TOML into your local deployment workspace and add the following **root-level** sections, preserving the existing `broker` and `store` values:

```toml
[observability]
environment = "development"
serviceInstanceId = "docs-broker"

[observability.metrics]
exporter = "otlp_grpc"

[observability.traces]
exporter = "otlp_grpc"
sampleRatio = 1.0
recordMessageId = false
recordMessageKeys = false

[observability.logs]
exporter = "otlp_grpc"

[observability.otlp]
endpoint = "http://127.0.0.1:4317"
protocol = "grpc"
headers = {}
```

The full trace sample ratio is for a small local trial. Select an appropriate rate for production. Do not add another `[observability]` section if the file already has one; merge fields instead.

The repository includes `distribution/config/otel-collector-observability.yaml` and `prometheus-observability.yaml`. With compatible collector and Prometheus binaries installed, run from the repository root in separate terminals:

```bash
otelcol-contrib --config distribution/config/otel-collector-observability.yaml
prometheus --config.file=distribution/config/prometheus-observability.yaml
```

The collector example accepts OTLP gRPC on `4317` and exports metrics for Prometheus on `9464`. Its traces and logs go to the debug exporter; it does not configure a trace/log storage backend or dashboard. It also exposes an HTTP receiver on `4318`, but RocketMQ Rust's active OTLP path described here uses gRPC. The collector listens on all interfaces in this fixture: restrict it to the intended test environment and access policy.

Restart the Broker with the copied configuration and the same runtime/security environment as the local tutorial. Run the [first-message application](../getting-started/quick-start.md). Look for service-specific observations at the collector and a successful `otel-collector` scrape target in Prometheus. Examine actual series and units before creating queries; exporter translation can change metric spelling and suffixes.

This is a setup procedure, not a recorded collector run. The [configuration guide](../configuration/observability.md) remains the source for precedence and exporter parameters.

## Explain missing or unexpected telemetry

1. Confirm the exact service binary includes the requested signal/exporter features.
2. Read effective file selection and **present** environment overrides. Missing environment variables do not override file values.
3. Check `OTEL_EXPORTER_OTLP_ENDPOINT`: a nonempty value selects OTLP for metrics, traces, and logs and requires `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`. A file that requests metrics only can therefore behave differently under an inherited standard endpoint variable.
4. Check endpoint reachability from the process namespace, collector receiver/pipeline configuration, exporter errors, and scrape health. A container's `127.0.0.1` refers to that container/network namespace.
5. Generate a small known workload and compare timestamps, service identity, and instance identity. Idle instruments or disabled sampling can explain missing observations.

The global `global.observability` selectors in the existing configuration guide belong to `distribution/helm/rocketmq-rust`, the separate five-service integration chart. Do not paste them into `rocketmq-rust-core` and assume its templates render those settings. Inspect the selected chart's actual values/templates or configure a custom workload's service file.

## Observe symptoms across layers

| Symptom | Compare | Useful response |
| --- | --- | --- |
| Producer latency rises | Client deadlines/retries, Broker append/flush latency, HA ACK/lag, disk service time | Locate the delayed completion stage before increasing timeouts |
| Consumer lag grows | Per-queue ingress/commit movement, processing latency, retry/ACK failures, queue assignment | Identify a hot queue or downstream bottleneck; assess catch-up capacity |
| HA writes wait or fail | Current authority, in-sync set, durable replication progress, peer/network/disk health | Restore the missing condition; do not infer quorum from connection count |
| Proxy transport succeeds but application fails | gRPC transport status, payload status, downstream latency, admission/session state | Inspect the returned payload and the responsible hop |
| Service disappears from routes | Broker registration, NameServer observations, network and process lifecycle | Query each NameServer and the actual Broker rather than restarting all nodes |
| Memory or disk pressure grows | Resident memory, retained request/stream bytes, store/log growth, filesystem capacity, operating-system pressure | Bound work and restore headroom while preserving durability requirements |

Use the canonical metric definitions and the actually exported series to build dashboards. Store recorders distinguish append, flush, dispatch, transfer, and HA observations; Proxy records payload failures separately from transport failures. Do not combine counters, gauges, bytes, queue offsets, and latency units as though they represented the same quantity.

## Alerts and diagnostic data

Define alert windows from workload expectations: sustained lag growth, shrinking disk headroom, loss of write-capable replication, export failure, or elevated completion errors. Include the affected service/group/queue scope and the first diagnostic action. Missing telemetry needs a collection-health response, not interpretation as zero traffic or zero errors.

Keep labels bounded. Topic/group labels require cardinality limits; message IDs, offsets, request IDs, and transaction IDs do not belong in metric labels. Keep message bodies, credentials, ACL/TLS material, and complete request/config objects out of logs. Collect public stable error codes and controlled diagnostic fields instead.

Use a service's supported `--log-filter` option where available and narrowly select relevant modules. Do not assume an embedded application automatically reads `RUST_LOG` or can install a second process-wide tracing subscriber. See [errors and observability design](../architecture/errors-observability.md).

On shutdown, stop application work, drain services, then flush and shut down owned telemetry before the runtime exits. Inspect shutdown reports: dropping the telemetry guard or closing its network socket does not establish successful export.

## Source map

[Observability ownership and exporters](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/README.md), [metric semantics](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/semantic.rs), [store recorders](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/metrics/store.rs), [Proxy observations](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/metrics/proxy.rs), [collector example](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/config/otel-collector-observability.yaml).
