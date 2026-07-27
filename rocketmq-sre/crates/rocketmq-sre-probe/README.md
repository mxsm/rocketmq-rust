# rocketmq-sre-probe

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-probe` provides bounded synthetic producer/consumer workflows
used to verify that evidence collection observes real RocketMQ behavior. It
creates a plan with dedicated Topic and Group names, enforces count, rate,
payload, and duration budgets, and emits canonical metadata-only Evidence.

## Responsibilities

- Enforce the `SRE_PROBE_` topic and `SRE_PROBE_G_` group namespaces.
- Bound message count, payload size, and duration before any client work starts.
- Produce and consume synthetic messages to create and then drain observable
  consumer lag.
- Use a probe-specific identity that is separate from MCP and execution roles.
- Run `send-consume-ack`, `proxy-path`, `transaction-commit`,
  `delayed-timer`, and `pop-ack` scenarios.
- Record trace ID, per-stage latency, sent/received/acknowledged counts, and
  bounded cleanup status without exporting the synthetic message bytes.

## Phase 00 boundary

The probe uses producer and consumer APIs with Admin features disabled. It
cannot create arbitrary business topics, reuse business groups, invoke
RocketMQ Admin operations, or carry business message content.

The POP scenario requires its dedicated Group to be configured for POP by an
external platform operator. The probe never invokes `setConsumerRequestMode`.
Topic/Group deletion is also external; probe cleanup only stops its producer,
consumer, runtime work, and local run metadata.

## Phase 2 scenarios

Pre-provision resources described in
`config/probe/phase2-probe-resources.v1.yaml`, then provide the selected
resource triple. All three values must be set together and must retain the
`SRE_PROBE_` / `SRE_PROBE_G_` namespaces.

```powershell
$env:ROCKETMQ_SRE_PROBE_TOPIC='SRE_PROBE_PHASE2'
$env:ROCKETMQ_SRE_PROBE_PRODUCER_GROUP='SRE_PROBE_G_P_PHASE2'
$env:ROCKETMQ_SRE_PROBE_CONSUMER_GROUP='SRE_PROBE_G_C_ACK'
$env:ROCKETMQ_SRE_PROBE_MAX_MESSAGES='5'
$env:ROCKETMQ_SRE_PROBE_MAX_MESSAGES_PER_SECOND='2'
$env:ROCKETMQ_SRE_PROBE_PAYLOAD_BYTES='64'
$env:ROCKETMQ_SRE_PROBE_DURATION_SECONDS='30'
cargo run --locked -p rocketmq-sre-probe -- run send-consume-ack
```

For `proxy-path`, set `ROCKETMQ_SRE_PROBE_PROXY_ADDR` to the dedicated
RocketMQ remoting-compatible Proxy endpoint. The command prints one canonical
Evidence envelope. A failed or timed-out probe still emits status and cleanup
metadata and never initiates a cluster change.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo check --locked -p rocketmq-sre-probe
cargo test --locked -p rocketmq-sre-probe
```

The tests cover all five scenario routes, prefix validation, count/rate/size/
duration limits, deterministic identities, cleanup boundaries, metadata-only
Evidence, and rejection of unsafe probe configurations.
