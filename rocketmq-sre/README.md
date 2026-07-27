# RocketMQ Rust AI SRE

`rocketmq-sre` is the independent Rust workspace and UI for the AI-assisted,
read-only operations plane for RocketMQ Rust. Phase 00 provides the secure
foundation. Phase 01 adds durable operator workflows, eight deterministic
diagnostic packs, a bounded multi-provider Model Gateway, Evidence and
knowledge services, asset/topology inventory, inspections, recommendations,
Shadow evaluation, and the independent desktop AI SRE workspace.
Phase 02 is being delivered in bounded increments. P2-01 adds typed alert,
correlation, topology, forecast, simulation, readiness, notification,
postmortem, and action-item contracts together with forward-only PostgreSQL
migrations and a generated Phase 02 OpenAPI/UI contract.

## Workspace boundaries

The workspace deliberately does not share MCP server DTOs. The Connector will
consume MCP through Streamable HTTP and translate validated wire responses into
canonical Evidence contracts. The Executor and Execution Agent are compiled as
disabled libraries and cannot mutate a target cluster.

The nine crates are:

- `rocketmq-sre-contracts`: versioned wire and persistence contracts.
- `rocketmq-sre-core`: incident coordination and extension registry.
- `rocketmq-sre-model-gateway`: canonical model IR and provider descriptors.
- `rocketmq-sre-control-plane`: control-plane service composition root.
- `rocketmq-sre-connector`: MCP connector composition root.
- `rocketmq-sre-executor`: disabled Phase 00 execution boundary.
- `rocketmq-sre-execution-agent`: disabled Phase 00 execution agent.
- `rocketmq-sre-probe`: bounded synthetic probe identity and validation.
- `rocketmq-sre-eval`: schema and coverage validation utilities.

## Development

```powershell
python scripts/check_source_layout.py
cargo check --locked --workspace
cargo test --locked --workspace --all-features
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
node scripts/export_phase2_openapi.mjs
npm --prefix ui run check:api
```

The workspace uses Rust 2024's modern source layout: `foo.rs` owns child
modules stored under `foo/`. Legacy `foo/mod.rs` entry points are rejected by
the source-layout check.

## Run the Phase 00 stack

PostgreSQL is containerized; no host database installation is required.
Large Evidence payloads are stored in the private `evidence-objects` named
volume mounted only by the Control Plane, so a normal stack or Control Plane
restart preserves both the PostgreSQL reference and its content. The in-memory
object store is never selected by normal service startup.

```powershell
.\scripts\dev.ps1 -Action Up
.\scripts\phase00-smoke.ps1 -Target Compose
.\scripts\dev.ps1 -Action Down
```

The Compose smoke waits for the bounded probe to create positive Consumer Lag
and then verifies that consumption makes it decline. It performs real
Prometheus, Loki, and Tempo queries; validates the versioned MCP runtime and
observability resources through the Connector data-source report; restarts
PostgreSQL and the Control Plane to prove onboarding persistence; and confirms
that a Collector outage does not block the RocketMQ/MCP data path before
checking exporter recovery. The final step offboards the fixed development
cluster. Because `Down` preserves the PostgreSQL volume, remove the Phase 00
volumes as described in [the local stack guide](deploy/dev/README.md) before
repeating the full smoke from a clean state.

The Phase 01 reverse Connector channel is HTTPS-only outside loopback. Compose
and Kind terminate mTLS in a constrained Control Plane proxy, use separate
server/client trust roots, overwrite connector identity headers from the
verified certificate, and isolate direct Axum access from the Connector.
Validate the deployment contract with:

```powershell
.\scripts\verify-mtls-deployment.ps1 -CheckCertificates
```

The ordinary RocketMQ Dashboard and the AI SRE UI are deliberately separate.
The SRE UI only consumes read-only onboarding, capability, Evidence, coverage,
runtime, and observability APIs. It contains no execution or resource mutation
entry point. Phase 01 targets a full-screen desktop workspace at 1280×720,
1440×900, and 1920×1080. Narrow layouts remain non-breaking, but dedicated
mobile interaction design is intentionally deferred.

Run the Phase 01 live read-only path against the Compose stack before Phase 00
offboarding:

```powershell
.\scripts\phase01-smoke.ps1 -Target Compose -BootstrapProbe
```

This complements the deterministic eight-pack Shadow suite: the live smoke
proves the real Connector/MCP/RocketMQ Evidence path and durable product
workflow, while Shadow supplies normal/fault/missing coverage for every Wave A
pack.

See:

- [Project boundaries](docs/decisions/0001-project-boundaries.md)
- [Read-only MCP boundary](docs/decisions/0002-read-only-mcp-boundary.md)
- [Control Plane connector-channel mTLS](docs/control-plane-connector-mtls.md)
- [Connector transport ADR](docs/connector-control-plane-transport-adr.md)
- [Compatibility](docs/compatibility.md)
- [Phase 02 contracts and persistence](docs/phase02-contracts-and-persistence.md)
- [Phase 02 diagnostic evidence sources](docs/phase02-evidence-sources.md)
- [Local stack](deploy/dev/README.md)

## Kind acceptance

The Phase 00 Kind environment reuses the repository-pinned Kubernetes tool
versions, the canonical Helm `dev-single` profile, and locally loaded images.
It adds ephemeral in-cluster PostgreSQL, the SRE services, and the minimum
Prometheus/Loki/Tempo/OTel acceptance overlay. It is deliberately not a
production Helm distribution.

```powershell
.\scripts\kind.ps1 -Action Up
.\scripts\kind.ps1 -Action Status
.\scripts\kind.ps1 -Action Smoke
.\scripts\kind.ps1 -Action Down
```

See [Kind acceptance environment](deploy/kind/README.md) for prerequisites,
the pinned versions, and smoke coverage.

## Phase 01 offline Shadow evaluation

The Phase 01 evaluator runs eight Wave A diagnostic packs against normal,
fault, and missing-evidence fixtures. It supports a deterministic mock
Provider, rules-only operation, and Provider-outage fallback. The Compose
runner has no network, and the Kind Job additionally has no service-account
token or RBAC plus deny-all ingress/egress. Both report zero mutation and
Executor calls.

```powershell
.\scripts\phase01-shadow.ps1 -Target Offline -Provider Mock
.\scripts\phase01-shadow.ps1 -Target Offline -Provider RulesOnly
.\scripts\phase01-shadow.ps1 -Target Offline -Provider Outage
.\scripts\phase01-shadow.ps1 -Target Compose -Provider Mock
```

See [Phase 01 Shadow evaluation](docs/phase01-shadow-evaluation.md) and
[Phase 01 known issues and Phase 02 inputs](docs/phase01-known-issues-and-phase02-inputs.md).
