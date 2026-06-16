# Prometheus and Grafana

RocketMQ Rust supports two metrics paths for Prometheus.

## Collector Prometheus Exporter

Use OTLP metrics from the broker to the OpenTelemetry Collector, then scrape
the Collector Prometheus exporter.

```bash
otelcol-contrib --config etc/observability/otel-collector-config.yaml
prometheus --config.file=etc/observability/prometheus.yaml
```

The provided Prometheus config scrapes `127.0.0.1:9464`.

Broker feature:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features otlp-metrics
```

Broker config:

```yaml
metricsExporterType: otlp_grpc
metricsExportIntervalMillis: 5000
```

## Direct Broker Prometheus Exporter

The broker can expose `/metrics` directly.

Broker feature:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust --features prometheus
```

Broker config:

```yaml
metricsExporterType: prom
metricsPromExporterHost: 127.0.0.1
metricsPromExporterPort: 5557
metricsPromExporterPath: /metrics
```

Scrape target:

```yaml
scrape_configs:
  - job_name: rocketmq-broker
    static_configs:
      - targets: ["127.0.0.1:5557"]
```

## Grafana

Import `etc/grafana/dashboards/rocketmq-broker.json` and select a Prometheus
data source. The dashboard uses the current broker metric names and standard
Prometheus histogram bucket suffixes.
