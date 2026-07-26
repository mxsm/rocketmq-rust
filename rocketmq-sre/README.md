# RocketMQ Rust AI SRE

`rocketmq-sre` is the independent Rust workspace and UI for the AI-assisted,
read-only operations plane for RocketMQ Rust. Phase 00 provides stable
contracts, PostgreSQL-backed cluster onboarding, an OAuth2/TLS MCP connector,
required telemetry, a capability registry, bounded synthetic probes, and a
repeatable local environment.

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
cargo check --locked --workspace
cargo test --locked --workspace --all-features
cargo run --locked -p rocketmq-sre-eval --bin schema-export -- schemas
```

## Run the Phase 00 stack

PostgreSQL is containerized; no host database installation is required.

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

The ordinary RocketMQ Dashboard and the AI SRE UI are deliberately separate.
The SRE UI only consumes read-only onboarding, capability, Evidence, coverage,
runtime, and observability APIs. It contains no execution or resource mutation
entry point.

See:

- [Project boundaries](docs/decisions/0001-project-boundaries.md)
- [Read-only MCP boundary](docs/decisions/0002-read-only-mcp-boundary.md)
- [Compatibility](docs/compatibility.md)
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
