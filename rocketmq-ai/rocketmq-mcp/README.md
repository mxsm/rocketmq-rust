# RocketMQ MCP

`rocketmq-mcp` is the feature-composed entrypoint for the RocketMQ MCP servers. It defaults to the
read-only query and diagnostics service and preserves the existing `rocketmq-mcp` binary, query arguments,
configuration, environment variables, tools, Resources, Prompts, and MCP protocol.

| Crate | Responsibility |
|---|---|
| [`rocketmq-mcp-query`](../rocketmq-mcp-query/README.md) | Query and diagnostics implementation; standalone query binary. |
| [`rocketmq-mcp-auth`](../rocketmq-mcp-auth/README.md) | Shared Bearer extraction, JWKS retrieval/cache, RS256 key selection, and protocol revision definitions. |
| [`rocketmq-mcp-control`](../rocketmq-mcp-control/README.md) | Separately authenticated HTTPS control service with reviewed mutations and durable audit. |

## Features

Defaults remain `read-only`, `diagnose`, and `stdio`. `read-only` is a compatibility alias for `query`.

| Feature | Effect |
|---|---|
| `query` | Includes the query implementation without implicitly selecting a transport. |
| `diagnose` | Includes query diagnosis tools. |
| `stdio` | Includes query stdio transport, for local development. |
| `auth` | Includes and re-exports the shared authentication library. |
| `streamable-http` | Includes query HTTP and its authentication support. |
| `control` | Includes the HTTPS control service and shared auth, without Admin dependencies. |
| `write-tools` | Includes control and forwards its reviewed mutation feature. |
| `observability` | Forwards query observability when query is included. |
| `otlp` | Forwards OTLP to included servers; query exports its existing signals, control exports metrics only. |
| `change-planning` | Preserves the deprecated query planning feature and its runtime policy. |
| `security-admin` | Preserves the optional query ACL-user listing feature and its separate scope. |

Telemetry features do not select a server. Compilation does not grant runtime mutation authority.
Control retains its OAuth, scope, cluster/operation authorization, mutation enablement, admission control,
durable audit-before-RPC, and emergency-stop policy.

## Starting a server

Run these commands from `rocketmq-ai/rocketmq-mcp`.

The process selects exactly one server with `ROCKETMQ_MCP_MODE`: `query` (the default) or `control`.
A selected server that was not compiled fails before its startup. Invalid modes fail without echoing their value.
The selected server owns its existing `RuntimeOwner` and graceful shutdown; the entrypoint adds no runtime or
subprocess. Server tool catalogs, middleware, configuration, and audit trails remain separate.

Default query stdio:

```bash
cargo run --locked
```

With no configuration argument or environment variable, the server uses built-in local-development settings:
stdio, the `local-dev` cluster at `127.0.0.1:9876`, read-only queries and diagnostics, output sanitization,
and audit records through stderr logging. Configuration and permissions are embedded, so the binary can
start from any working directory without configuration files. It waits for MCP JSON-RPC requests on stdin;
RocketMQ queries require a running local NameServer and Broker.

Use `--config` or `ROCKETMQ_MCP_CONFIG` for custom settings; the argument takes precedence. Explicit configuration
errors fail startup. For example:

```bash
cargo run --locked -- --config conf/mcp.example.toml
```

Query HTTP without stdio:

```bash
cargo run --locked --no-default-features --features streamable-http -- \
  --config conf/mcp.example.toml --transport streamable-http
```

See the [query configuration and deployment guide](../rocketmq-mcp-query/README.md) for authentication,
TLS, permissions, and output policy. Canonical query examples live in the query crate; this crate keeps
identical copies under `conf/` so existing configuration paths continue to work. Update both copies together
when changing the examples.

Control-only build and launch in PowerShell:

```powershell
cargo build --locked --no-default-features --features write-tools
$env:ROCKETMQ_MCP_MODE = 'control'
$env:ROCKETMQ_MCP_CONTROL_CONFIG = '../rocketmq-mcp-control/conf/mcp-control.example.toml'
.\target\debug\rocketmq-mcp.exe
```

Use a deployment-specific configuration prepared from the control example. The
[control operations runbook](../rocketmq-mcp-control/docs/operations-runbook.md) describes the required HTTPS,
OAuth, durable audit, closed allowlists, and runtime mutation settings. `control` alone starts the foundation
service; `write-tools` compiles the reviewed tools. A control-only binary still defaults to query mode, so set
the mode explicitly. Control reads its own configuration and does not parse query command-line arguments.

## Dependency and deployment boundaries

Default and query-only builds must have no mutation adapter or control dependency. Control-only foundation
builds must have no Admin dependency; control-only write builds select only the mutation adapter.

Enabling query and write tools together unifies the read and mutation features of their shared Admin dependency.
That combined binary preserves server authorization and tool separation, but its dependency closure contains
both capabilities. Deploy independent `rocketmq-mcp-query` and `rocketmq-mcp-control` binaries when separate
dependency closures are required. The standalone boundary checks continue to validate those builds.

## Rust API

The entrypoint exports `query`, `auth`, and `control` when their features are enabled. Under `query`, the original
query modules and `McpError` / `McpResult` remain available at the crate root, including `rocketmq_mcp::app::McpApp`.
`ServerMode` and `run_from_env` provide synchronous process startup with a bounded `LaunchError`.

## Development

```bash
cargo fmt --all -- --check
cargo test --locked
cargo test --locked --no-default-features
cargo test --locked --no-default-features --features auth
cargo test --locked --no-default-features --features query
cargo test --locked --no-default-features --features streamable-http
cargo test --locked --no-default-features --features control
cargo test --locked --no-default-features --features write-tools,otlp
cargo test --locked --features write-tools
```

Select the affected combinations rather than treating this list as an internal-change checklist. The integration
test starts query stdio without a live cluster and verifies that compiled control tools do not enter its catalog.
Implementation tests, protocol snapshots, and standalone dependency checks live in their owning crates.
