# rocketmq-observability

Shared observability foundation for the rocketmq-rust workspace.

This crate starts with configuration, lifecycle, resource metadata, metrics
labels, and propagation primitives. Exporter-specific wiring is feature gated
and intentionally disabled by default.
