# RocketMQ MCP

`rocketmq-mcp` is the Model Context Protocol server for RocketMQ-Rust AI SRE and diagnostics workflows. It exposes read-only RocketMQ context, diagnostic tools, and runbook prompts to MCP clients such as Claude Desktop, Cursor, Codex, and MCP Inspector.

The frozen MCP 2025-11-25 contract is documented in `rocketmq-doc/en/07-rocketmq-mcp-contract-v2.md`.
Production release gates, external-cluster expectations, and rollback guidance are documented in
`docs/production-validation.md`.

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
- `audit.enabled = true` records tool decisions and HTTP rejections through a bounded asynchronous writer.
- `server.stdio.log_to_stderr = true` keeps stdout reserved for MCP protocol frames.

For HTTP deployments, keep `server.http.bind` on loopback unless there is a reviewed network boundary. `server.http.auth.mode = "development-token"` is explicitly for loopback development. Production deployments must use `oauth-jwt`, validate signed issuer/audience-bound access tokens, and configure the public key through the declared environment variable. The server never forwards an incoming bearer token to RocketMQ.

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
- `server.http.bind`: socket address for the HTTPS transport, default `127.0.0.1:8089`.
- `server.http.endpoint`: MCP endpoint path, default `/mcp`.
- `server.http.public_base_url`: absolute public HTTPS origin used in protected-resource metadata and authentication challenges.
- `server.http.tls.cert_path` and `key_path`: server certificate chain and private key. The complete pair is verified before an atomic generation is published; failed reloads keep the last-known-good generation.
- `server.http.allowed_origins`: allowed browser origins when origin validation is enabled.
- `server.http.auth.mode`: `development-token` for loopback development or `oauth-jwt` for production JWT access tokens.
- `server.http.auth.issuer`, `audience`, `required_scopes`, `jwt_algorithm`, and `jwks_url`: OAuth resource-server validation settings. OAuth accepts only RS256 tokens carrying a `kid`, and the JWKS endpoint must use HTTPS.
- `server.http.auth.jwks_refresh_seconds` and `jwks_max_stale_seconds`: bounded refresh and last-known-good windows. A fetch or parse failure never clears an already verified key generation.
- `server.http.auth.jwt_key_env`: retained only for configuration compatibility; OAuth does not use a static key fallback.
- `server.http.auth.protected_resource_metadata_path`: unauthenticated OAuth protected-resource metadata endpoint.
- `clusters[].name`: logical cluster name used by tools, resources, and prompts.
- `clusters[].namesrv_addr`: RocketMQ namesrv address for admin queries.
- `security.permissions_file`: executable role, tool, and cluster policy. Claims roles and `rocketmq_clusters` are intersected with this policy.
- `security.max_concurrent_requests_per_cluster`: bounded concurrent Tool and Resource work per configured cluster.
- `security.rate_limit_per_minute`: per-principal, per-cluster, per-operation limit.
- `audit.sink`: `memory`, `file`, or `tracing`.
- `audit.queue_capacity`: bounded audit writer queue. Queue drops and sink failures are emitted as trace metrics.
- `cache.enabled`: enables or bypasses the shared query cache.
- `cache.max_entries`: maximum number of in-memory entries; it must be greater than zero when caching is enabled.
- `cache.*_ttl_ms`: per-query-family freshness windows for overview, topic, broker, and consumer-lag data.
- `diagnosis.consumer_lag_policy_profile`: server-owned policy identifier reported with each diagnosis.
- `diagnosis.consumer_lag_threshold`: server-owned threshold used by consumer-lag rules.

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

## Streamable HTTPS Usage

Streamable HTTPS requires the `streamable-http` feature and a valid certificate/key pair. Use a static token only for reviewed loopback development; use OAuth JWT validation in production.

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

Clients connect to `https://127.0.0.1:8089/mcp` and send:

```text
Authorization: Bearer replace-with-a-long-random-token
Accept: application/json, text/event-stream
```

For production, set `mode = "oauth-jwt"` and configure `issuer`, `audience`, `required_scopes`, `jwks_url`, and the JWKS refresh windows. Clients must send an RS256 access token with a known `kid` and valid signature, issuer, audience, expiry, and required scope. The server publishes OAuth protected-resource metadata at the configured `protected_resource_metadata_path`; this endpoint remains available without a bearer token for client discovery and its absolute HTTPS URI is included in bearer challenges.

`permissions.example.toml` is loaded at startup. Verified principal, client, roles, scopes, and `rocketmq_clusters` claims propagate through the real MCP handler to RBAC, cluster allow-list, rate-limit, and audit decisions; an HTTP request cannot substitute the local stdio identity. Tool, Resource, and Prompt discovery are filtered by this policy, and Resource reads and Tool calls are enforced again at execution time. Audit records contain the verified principal and client identifier but never store the bearer token.

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

Consumer-lag diagnoses use versioned Evidence Snapshots and a server-side rule policy. The Tool accepts only cluster, topic, and consumer group; historical `time_range` and caller-controlled thresholds are intentionally unavailable until a historical metrics source exists.

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
- HTTP token configuration error: set the selected development-token or OAuth JWT key environment variable. Production mode must not use `ROCKETMQ_MCP_HTTP_TOKEN`.
- HTTP `401`: check the access-token signature, expiry, issuer, audience, and `Authorization: Bearer <token>` header.
- HTTP `403`: check token scopes, role claims, `rocketmq_clusters`, `permissions.example.toml`, and browser origin policy.
- HTTP `429`: raise `security.rate_limit_per_minute` only after reviewing client retry behavior.
- Empty or invalid stdio responses: ensure no wrapper script writes logs or banners to stdout.
- No cluster data: verify `clusters[].namesrv_addr`, local network access, and RocketMQ namesrv availability.
- Audit file errors: create the audit directory or use `audit.sink = "memory"` for local tests. Check `audit_sink_failures` and `audit_dropped` trace metrics before increasing queue capacity.
