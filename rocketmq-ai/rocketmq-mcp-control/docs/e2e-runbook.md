# RocketMQ MCP Control real-cluster E2E runbook

This runbook operates the checked-in ignored real-cluster test. It validates the five reviewed
Control MCP mutations against an ephemeral local Rust NameServer and Broker. It is not a production
deployment procedure and it does not add a delete MCP Tool.

## Preconditions

- PowerShell 7 or a compatible PowerShell host is available.
- Cargo and the repository's locked Rust toolchain can build the repository NameServer and Broker binaries
  and the standalone Control project.
- Run from `rocketmq-ai/rocketmq-mcp-control` in a working tree with the repository source available above it.
- Ensure the host can reserve loopback ports and create temporary files. The harness chooses its own unique
  loopback ports and temporary root.

Docker is not a runner dependency. The test does not create a Docker container, image, network, Compose stack,
or volume. It may be run on a local development machine that also has Docker installed, but the runner itself
uses only the repository's Rust binaries and an owned process harness.

## Run the scenario

Read the runner usage first:

```powershell
.\scripts\run_real_cluster_e2e.ps1 -Help
```

Then start the scenario:

```powershell
.\scripts\run_real_cluster_e2e.ps1
```

The script builds `rocketmq-namesrv-rust` and `rocketmq-broker-rust` from the repository manifest, resolves
their output locations through Cargo metadata, sets the test-only opt-in environment, and runs exactly one
ignored test with `--test-threads=1`. The test remains ignored unless the runner explicitly sets
`ROCKETMQ_MCP_CONTROL_REAL_CLUSTER_E2E=1`; do not run it as part of ordinary unit tests.

## What the harness proves

The harness starts and owns an isolated Rust NameServer and Broker on dynamically reserved loopback ports. It
then starts the real TLS Streamable HTTP Control MCP server with the real Admin mutation factory and a
test-only RS256/JWK identity seam. It exercises all five reviewed tools over TLS MCP:

1. Topic upsert: dry-run, execute, and idempotent execution.
2. Consumer Group upsert: dry-run, execute, and idempotent execution.
3. Consumer offset reset: dry-run, execute, idempotent execution, and per-queue readback.
4. Broker configuration patch: dry-run, execute, idempotent execution, and exact restoration of all six
   reviewed Broker fields with a monotonic generation.
5. Consumer request mode: baseline dry-run/execute, changed dry-run/execute, idempotent execution, and exact
   baseline restoration.

The scenario makes 22 authenticated TLS MCP calls and verifies 44 durable
`rocketmq-mcp-control.audit.v2` records: one `started` and one terminal record per call. It checks closed
operation, mode, result/error-code relationship, safe cluster/operator/reason evidence, sequence ordering, and
terminal duration. It also exercises a Broker outage and restart, a Control restart with audit recovery, and a
post-restart successful dry-run.

## State restoration and failure handling

Before a mutable fixture step, the harness records the relevant original state. Cleanup restores all six
Broker configuration fields—`autoCreateTopicEnable`, `autoCreateSubscriptionGroup`, `brokerPermission`,
`defaultTopicQueueNums`, `messageIndexEnable`, and `traceTopicEnable`—and restores the Consumer request mode.
It conditionally removes only the uniquely named test Topic and Consumer Group that it created using the
underlying typed test fixture (the `McpE2eTopic_` and `McpE2eGroup_` prefixes); no MCP delete Tool is
registered or required.

The harness owns its NameServer, Broker, Control server, fixture state, and a unique temporary root created
with the `rocketmq-mcp-control-e2e-` prefix. It uses bounded readiness, liveness, shutdown, kill/wait, and
drop cleanup. On a successful run it asserts that owned children were reaped and the temporary root was
removed; cleanup is idempotent. On failure, inspect sanitized harness diagnostics, which redact filesystem
paths, and handle only resources whose harness ownership can be proved. Do not terminate unrelated RocketMQ
processes, delete a shared store, or remove an unknown temporary directory.

If restoration or cleanup reports a failure, keep the test failure evidence and let the harness-owned cleanup
handle resources it can prove it created. A fixed test prefix is not sufficient evidence by itself; never
delete an uncertain process, directory, Topic, Group, or Broker state. Reconcile the recorded Broker and
request-mode state through an authorized path before another run. Do not turn the test into a broad cleanup
command and do not add a delete operation to the Control MCP surface.

## Expected completion evidence

A passing runner exits zero and reports completion after the test verifies state restoration and reaping. The
expected proof is one serial ignored test pass together with its fixed 22 TLS MCP invocation and 44 durable v2
audit-record checks. The runner restores the environment variables that it temporarily owns before returning.
