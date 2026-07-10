# RocketMQ MCP

`rocketmq-mcp` is the Model Context Protocol server for RocketMQ-Rust AI SRE and diagnostics workflows. It exposes read-only RocketMQ context, diagnostic tools, and runbook prompts to MCP clients such as Claude Desktop, Cursor, Codex, and MCP Inspector.

The frozen MCP 2025-11-25 contract is documented in `rocketmq-doc/en/07-rocketmq-mcp-contract-v2.md`.

## What It Is

- A standalone MCP server binary named `rocketmq-mcp`.
- A bridge from MCP clients to RocketMQ-Rust admin/query capabilities.
- A diagnostics surface for cluster overview, topics, brokers, consumer groups, consumer lag, and guided runbooks.
- A process boundary outside broker, namesrv, store, and dashboard runtimes.

## What It Is Not

- It is not part of the RocketMQ broker or namesrv runtime path.
- It is not a replacement for production access control, network policy, or operator review.
- It does not expose an Apply path. Optional planning Tools produce reviewable, non-mutating plans only.
- It does not hide all operational risk. Treat AI-generated recommendations as operator input, not an automatic execution plan.

## Capabilities

Default features are `read-only`, `diagnose`, and `stdio`.

Optional features:

- `streamable-http`: enables the Streamable HTTP transport.
- `observability`: reserves integration with the repository observability crate.
- `change-planning`: registers non-mutating change planning Tools and still requires runtime policy.

## Safety Boundary

The default profile is diagnostics-oriented and read-only:

- `security.profile = "diagnose"` allows read-only and diagnosis tools.
- `security.allow_change_planning = false` blocks planning Tools unless explicitly enabled.
- `security.sanitize_output = true` redacts configured sensitive output patterns.
- `audit.enabled = true` records tool decisions and HTTP rejections.
- `server.stdio.log_to_stderr = true` keeps stdout reserved for MCP protocol frames.

For HTTP deployments, keep `server.http.bind` on loopback unless there is a reviewed network boundary. When `server.http.require_auth = true`, the process requires `ROCKETMQ_MCP_HTTP_TOKEN` and clients must send `Authorization: Bearer <token>`.

When `change-planning` is compiled, planning Tools are still controlled by runtime policy. They return a plan, impact analysis, and rollback suggestions. Their schemas contain no Apply mode, operator identity, or confirmation token, and no mutation API is called.

The server targets MCP protocol version `2025-11-25`. Clients requesting another protocol version are rejected during initialization.

Successful Tool calls return a `rocketmq-mcp.v2` envelope with `request_id`, cluster, RFC 3339 observation time, freshness, cache status, partial status, warnings, and typed data. Correctable input and backend failures return Tool execution errors with a stable code, retryability, suggestions, and the request identifier.

Read-only Tool calls also return a ResourceLink for the corresponding live Resource. Tool and Resource requests share the application-level `QueryFacade`, bounded TTL cache, and singleflight coordination, so an identical query can be replayed without starting a second admin session while its entry is fresh.

## Build

Run commands from the repository root.

```bash
cargo check -p rocketmq-mcp
cargo test -p rocketmq-mcp
cargo clippy --all-targets -p rocketmq-mcp --features streamable-http -- -D warnings
cargo doc -p rocketmq-mcp --no-deps
```

Build the default stdio binary:

```bash
cargo build -p rocketmq-mcp --release
```

Build with Streamable HTTP support:

```bash
cargo build -p rocketmq-mcp --release --features streamable-http
```

## Configuration

Start from the checked-in example:

```bash
rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml
```

Important fields:

- `server.transport`: `stdio` or `streamable-http`.
- `server.http.bind`: socket address for HTTP transport, default `127.0.0.1:8089`.
- `server.http.endpoint`: MCP endpoint path, default `/mcp`.
- `server.http.allowed_origins`: allowed browser origins when origin validation is enabled.
- `clusters[].name`: logical cluster name used by tools, resources, and prompts.
- `clusters[].namesrv_addr`: RocketMQ namesrv address for admin queries.
- `audit.sink`: `memory`, `file`, or `tracing`.
- `cache.enabled`: enables or bypasses the shared query cache.
- `cache.max_entries`: maximum number of in-memory entries; it must be greater than zero when caching is enabled.
- `cache.*_ttl_ms`: per-query-family freshness windows for overview, topic, broker, and consumer-lag data.

Cache keys include the schema version, visibility class, query kind, resolved cluster, and normalized query parameters. Failures are not cached. Concurrent misses for the same key are coalesced, and `cache_status` reports `miss`, `hit`, or `bypass`. Embedders can call `McpApp::invalidate_cache()` to clear all entries explicitly. Cumulative hit, miss, bypass, eviction, invalidation, and coalesced-waiter counters are emitted at trace level after Tool and Resource requests.

Command-line overrides:

```bash
rocketmq-mcp --config rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml --transport stdio
rocketmq-mcp --config rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml --transport streamable-http --bind 127.0.0.1:8089 --endpoint /mcp
```

## stdio Usage

Use stdio for local desktop clients. Logs are written to stderr so stdout remains valid MCP JSON-RPC traffic.

```bash
cargo run -p rocketmq-mcp -- \
  --config rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml \
  --transport stdio
```

For a release binary:

```bash
target/release/rocketmq-mcp \
  --config rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml \
  --transport stdio
```

## Streamable HTTP Usage

Streamable HTTP requires the `streamable-http` feature and, when auth is enabled, a bearer token.

PowerShell:

```powershell
$env:ROCKETMQ_MCP_HTTP_TOKEN = "replace-with-a-long-random-token"
cargo run -p rocketmq-mcp --features streamable-http -- `
  --config rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml `
  --transport streamable-http `
  --bind 127.0.0.1:8089 `
  --endpoint /mcp
```

Bash:

```bash
export ROCKETMQ_MCP_HTTP_TOKEN=replace-with-a-long-random-token
cargo run -p rocketmq-mcp --features streamable-http -- \
  --config rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml \
  --transport streamable-http \
  --bind 127.0.0.1:8089 \
  --endpoint /mcp
```

Clients connect to `http://127.0.0.1:8089/mcp` and send:

```text
Authorization: Bearer replace-with-a-long-random-token
Accept: application/json, text/event-stream
```

## Claude Desktop

Build the binary first, then add a server entry to the Claude Desktop MCP configuration.

Windows example:

```json
{
  "mcpServers": {
    "rocketmq": {
      "command": "C:\\path\\to\\rocketmq-rust\\target\\release\\rocketmq-mcp.exe",
      "args": [
        "--config",
        "C:\\path\\to\\rocketmq-rust\\rocketmq-tools\\rocketmq-mcp\\conf\\mcp.example.toml",
        "--transport",
        "stdio"
      ]
    }
  }
}
```

macOS or Linux example:

```json
{
  "mcpServers": {
    "rocketmq": {
      "command": "/path/to/rocketmq-rust/target/release/rocketmq-mcp",
      "args": [
        "--config",
        "/path/to/rocketmq-rust/rocketmq-tools/rocketmq-mcp/conf/mcp.example.toml",
        "--transport",
        "stdio"
      ]
    }
  }
}
```

Use a copied config file for real clusters and keep secrets out of client logs.

## Cursor And Codex

Use a stdio MCP server definition with:

- Command: the built `rocketmq-mcp` binary.
- Args: `--config <config-path> --transport stdio`.
- Working directory: repository root, if the client supports it.

For HTTP-capable clients, use the Streamable HTTP URL `http://127.0.0.1:8089/mcp` and configure the bearer token as an HTTP authorization header.

## Tools

- `rocketmq_get_cluster_overview`: summarize one configured cluster.
- `rocketmq_list_topics`: list a filtered, cursor-paginated topic page.
- `rocketmq_describe_topic`: describe a topic with bounded queue data.
- `rocketmq_get_topic_route`: get bounded topic route data.
- `rocketmq_list_consumer_groups`: list a filtered, cursor-paginated consumer-group page.
- `rocketmq_get_consumer_lag`: get bounded consumer progress and lag rows.
- `rocketmq_describe_broker`: describe broker state.
- `rocketmq_diagnose_consumer_lag`: aggregate read-only evidence and return a diagnosis report.

Feature-gated planning Tools, available only with `change-planning`, never mutate the cluster:

- `rocketmq_plan_create_topic`
- `rocketmq_plan_update_topic_config`
- `rocketmq_plan_update_topic_permissions`
- `rocketmq_plan_update_broker_config`
- `rocketmq_plan_reset_consumer_offset`

## Resources

- `rocketmq://clusters/{cluster}/overview`
- `rocketmq://clusters/{cluster}/topics`
- `rocketmq://clusters/{cluster}/topics/{topic}`
- `rocketmq://clusters/{cluster}/topics/{topic}/route`
- `rocketmq://clusters/{cluster}/brokers`
- `rocketmq://clusters/{cluster}/brokers/{broker}`
- `rocketmq://clusters/{cluster}/consumer-groups`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag?topic={topic}`

`resources/list` returns cluster root Resources in cursor-paginated pages. `resources/templates/list` publishes the five parameterized forms. All accepted URIs are explicit cluster-scoped v2 URIs; unsupported or incomplete forms return Resource Not Found instead of a placeholder payload.
Cluster and RocketMQ entity names are UTF-8 percent-encoded as URI path or query components, including retry topics and groups that contain `%RETRY%`.

## Prompts

- `diagnose_consumer_lag`: guided consumer lag investigation.
- `broker_health_check`: guided broker health review.

## Troubleshooting

- `streamable-http transport requires the streamable-http feature`: rebuild or run with `--features streamable-http`.
- `ROCKETMQ_MCP_HTTP_TOKEN must be set when HTTP auth is required`: set the environment variable or disable auth only for a reviewed local test.
- HTTP `401`: check the `Authorization: Bearer <token>` header.
- HTTP `403` or rejected browser requests: check `server.http.allowed_origins` and `server.http.validate_origin`.
- HTTP `429`: raise `security.rate_limit_per_minute` only after reviewing client retry behavior.
- Empty or invalid stdio responses: ensure no wrapper script writes logs or banners to stdout.
- No cluster data: verify `clusters[].namesrv_addr`, local network access, and RocketMQ namesrv availability.
- Audit file errors: create the audit directory or use `audit.sink = "memory"` for local tests.
