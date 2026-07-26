# rocketmq-sre-probe

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-probe` provides the bounded synthetic producer/consumer workflow
used to verify that evidence collection observes real RocketMQ behavior. It
creates a plan with dedicated topic and group names and runs for a fixed amount
of work.

## Responsibilities

- Enforce the `SRE_PROBE_` topic and `SRE_PROBE_G_` group namespaces.
- Bound message count, payload size, and duration before any client work starts.
- Produce and consume synthetic messages to create and then drain observable
  consumer lag.
- Use a probe-specific identity that is separate from MCP and execution roles.

## Phase 00 boundary

The probe uses producer and consumer APIs with Admin features disabled. It
cannot create arbitrary business topics, reuse business groups, invoke
RocketMQ Admin operations, or carry business message content.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo check --locked -p rocketmq-sre-probe
cargo test --locked -p rocketmq-sre-probe
```

The tests cover prefix validation, hard limits, deterministic identities, and
rejection of unsafe probe configurations.
