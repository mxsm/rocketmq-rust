# rocketmq-sre-executor

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../../LICENSE-APACHE)

`rocketmq-sre-executor` is the single-active state-machine boundary for
human-approved RocketMQ SRE changes. It accepts only a short-lived, signed
`ExecutionRequest` from Control Plane and can reach targets only through the
typed Execution Agent API.

## Security boundary

- The normal dependency graph contains no RocketMQ Admin mutation adapter,
  Kubernetes client, model gateway, MCP server, shell, raw request-code client,
  or arbitrary JSON Patch implementation.
- Control Plane is the only accepted caller. Production transport requires the
  exact `spiffe://rocketmq-sre/control-plane` identity plus a separate workload
  bearer token.
- Executor receives no Lease Authority signing key and no target credentials.
- Every step is locally revalidated against the embedded descriptor, live
  `precondition_hash`, persistent quarantine, resource lock, active lease
  epoch, and a fresh step-scoped fence grant.
- An uncertain Agent result is journaled as `Unknown` and reconciled by a
  read-only live-state call; it is never blindly dispatched again.
- Verification combines two independent read-only observations: typed resource
  conditions from Execution Agent and technical SLI conditions from Control
  Plane. Scope, schema, correlation ID, and the complete descriptor condition
  surface must match before canonical Evidence is accepted.

## Durable execution sequence

```text
verify signed request
  -> recover or create idempotent execution
  -> PendingFence + read-only reconcile
  -> Agent AdvanceFence + signed FenceAck
  -> Active lease
  -> descriptor/live-state precheck
  -> resource lock
  -> StepIntent + audit in one transaction
  -> fresh LeaseFenceGrant
  -> typed Agent dispatch
  -> StepResult + audit
  -> Agent resource observation + Control Plane SLI observation
  -> canonical Evidence + bounded stability-window verification
  -> success or verified compensation
```

The service listens on `8094` by default. `/healthz` is liveness only;
`/readyz` requires the PostgreSQL journal. Status and execution endpoints are
internal workload routes. `ROCKETMQ_SRE_EXECUTOR_VERIFICATION_POLL_SECONDS`
controls the production verification interval and is restricted to 1–60
seconds; the default is 5 seconds.

## Validation

Run from `rocketmq-ai/rocketmq-sre/` with a non-system target directory:

```powershell
cargo test --locked -p rocketmq-sre-executor
cargo clippy --locked -p rocketmq-sre-executor --all-targets -- -D warnings
cargo tree -p rocketmq-sre-executor --edges normal
```

The ignored `postgres_recovery` and `fencing_interleavings` tests require
`ROCKETMQ_SRE_TEST_DATABASE_URL`. They prove durable restart recovery,
idempotency, quarantine isolation, shared/exclusive Agent fencing, and the
`Dispatched`-effect takeover block.
