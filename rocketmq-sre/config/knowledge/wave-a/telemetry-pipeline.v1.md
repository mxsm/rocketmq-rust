# Telemetry pipeline diagnosis

Use this runbook when metrics, logs, traces, or runtime diagnostics are stale or missing.

## Required evidence

- Collector reachability and exporter state.
- Prometheus scrape freshness, Loki ingestion freshness, and Tempo trace availability.
- Export queue, drop, failure, and shutdown counters.
- Runtime TaskGroup and BlockingExecutor bounded summaries.

## Interpretation

Exporter failure or Collector outage supports an observability-path issue but must not be treated as RocketMQ data-plane failure. Missing values remain missing instead of zero. Continued send and consume success while the Collector is down is strong counter-evidence against a data-plane outage.

## Read-only recommendation

Identify the first stale hop and its last-success time. Do not block RocketMQ or MCP readiness solely because an optional telemetry backend is unavailable.
