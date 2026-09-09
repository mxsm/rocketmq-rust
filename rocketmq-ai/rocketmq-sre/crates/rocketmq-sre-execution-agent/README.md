# rocketmq-sre-execution-agent

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../../LICENSE-APACHE)

`rocketmq-sre-execution-agent` is the only SRE workload allowed to hold
RocketMQ Admin mutation, Kubernetes, or configuration-system write
credentials. It exposes no generic command surface: all reads, writes,
reconciliation, and compensation pass through an explicitly registered typed
handler.

## Fence and effect invariants

- Only the exact `spiffe://rocketmq-sre/executor` workload identity plus its
  bearer token can call internal routes.
- Each dispatch verifies the fresh Lease Authority grant both before and after
  acquiring the PostgreSQL shared dispatch barrier.
- The Agent rejects any grant whose epoch is not its durable highest accepted
  cluster epoch.
- `Prepared` and then `Dispatched` are persisted before the driver is called.
  `Confirmed` is written only for a bounded, verifiable outcome.
- A duplicate idempotency key replays a prior `Confirmed` result and never
  invokes the driver twice. A non-terminal duplicate returns
  `unresolved_old_effects`.
- `AdvanceFence` takes the exclusive cluster barrier, waits for in-flight
  shared dispatches, rejects any old `Prepared`/`Dispatched`/`Unknown` effect,
  persists the new highest epoch, and returns a nonce-bound signed `FenceAck`.
- Lease Authority or PostgreSQL unavailability fails closed before a target
  write.

The three driver families are `AdminCoreDriver`, `KubernetesDriver`, and
`ConfigDriver`. Raw Admin request codes, shell execution, deletion/cleaning
operations, and arbitrary Kubernetes patches do not exist in the protocol or
registry.

The service listens on `8095` by default. `/healthz` is liveness only;
`/readyz` requires the PostgreSQL effect ledger. The production registry is
empty with all action settings disabled. Startup now registers reviewed
handlers when explicit enablement and required configuration are present;
compiling the crate does not enable them.

| Handler | Enable setting suffix (`ROCKETMQ_SRE_AGENT_`) |
| --- | --- |
| Allowlisted Broker configuration | `ENABLE_BROKER_CONFIG` |
| Allowlisted Topic configuration | `ENABLE_TOPIC_CONFIG` |
| Allowlisted Subscription Group configuration | `ENABLE_SUBSCRIPTION_GROUP_CONFIG` |
| Logger level with TTL | `ENABLE_LOGGER_TTL` |
| One-unit Proxy scale-out | `ENABLE_PROXY_SCALE_OUT` |
| Proxy image canary | `ENABLE_PROXY_IMAGE_CANARY` |
| Credential rotation with overlap | `ENABLE_CREDENTIAL_ROTATION` |
| One Proxy restart | `ENABLE_PROXY_RESTART` |
| One telemetry collector restart | `ENABLE_TELEMETRY_COLLECTOR_RESTART` |

All enable settings default to false. Depending on the handler, startup also
requires target allowlists, dedicated credentials, verification endpoints or
Kubernetes configuration. [config.rs](src/config.rs) validates these combinations;
[api.rs](src/api.rs) constructs drivers and registers the selected handlers.
Lease, fence, idempotency and effect-ledger checks apply to every dispatch.

The Axum listener is an internal HTTP endpoint behind an enforcing mTLS
identity proxy. Middleware checks `x-forwarded-client-cert` and the separate
Executor bearer token; the Agent listener itself does not terminate TLS.
Restrict listener access to the proxy and replace untrusted identity headers
there. The explicit development profile relaxes this identity check.

## Validation

Run from `rocketmq-ai/rocketmq-sre/` with a non-system target directory:

```powershell
cargo test --locked -p rocketmq-sre-execution-agent
cargo clippy --locked -p rocketmq-sre-execution-agent --all-targets -- -D warnings
```

The Executor integration tests exercise the Agent against Docker PostgreSQL,
including exact dispatch/fence interleavings and crash recovery.
