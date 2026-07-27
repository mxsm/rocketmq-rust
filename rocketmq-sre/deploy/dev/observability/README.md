# Phase 1 AI SRE observability

This directory contains the desktop operations dashboard for the Phase 1 SRE services. It does not add a mobile layout or a production Grafana deployment.

## Import

1. Start the Phase 1 Compose observability stack.
2. Open an existing Grafana instance and add the Compose Prometheus endpoint as a data source.
3. Import `grafana/dashboards/rocketmq-sre-phase1.json`.
4. Select the Prometheus data source when Grafana asks for `DS_PROMETHEUS`.

The dashboard uses only bounded labels. Use the correlation ID in Tempo or Loki to investigate a single operation; do not add tenant, cluster, incident, evidence, Connector, model, prompt, or tool-argument labels to Prometheus.

Canonical queries and readiness semantics are documented in:

- `../../../config/observability/sre/prometheus-queries.md`
- `../../../config/observability/sre/health.v1.yaml`
