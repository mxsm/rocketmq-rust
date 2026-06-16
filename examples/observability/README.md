# Observability Examples

This directory contains copyable configuration snippets for the broker
observability landing path.

Use `broker-observability.yaml` as a fragment inside a full broker
configuration file. The root Cargo example remains in
`rocketmq-observability/examples/broker_metrics.rs`:

```bash
cargo run -p rocketmq-observability --example broker_metrics --features otlp-metrics
```
