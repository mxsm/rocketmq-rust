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
- `observability`: enables in-process metrics and traces without selecting a remote exporter.
- `otlp`: enables metrics, traces, and logs through the implemented OTLP gRPC exporter.
- `change-planning`: registers non-mutating change planning Tools and still requires runtime policy.

## Safety Boundary

The default profile is diagnostics-oriented and read-only:

- `security.profile = "diagnose"` allows read-only and diagnosis tools.
- `security.allow_change_planning = false` blocks planning Tools unless explicitly enabled.
- `security.sanitize_output = true` redacts configured sensitive output patterns.
- `audit.enabled = true` records tool decisions and HTTP rejections through a bounded asynchronous writer.
- `server.stdio.log_to_stderr = true` keeps stdout reserved for MCP protocol frames.

For HTTP deployments, keep `server.http.bind` on loopback unless there is a reviewed network boundary. `server.http.auth.mode = "development-token"` is explicitly for loopback development. Production deployments must use `oauth-jwt` and validate signed issuer/audience-bound access tokens against the configured HTTPS JWKS endpoint. The server never forwards an incoming bearer token to RocketMQ.

When `change-planning` is compiled, planning Tools are still controlled by runtime policy. They return a plan, impact analysis, and rollback suggestions. Their schemas contain no Apply mode, operator identity, or confirmation token, and no mutation API is called.

The server targets MCP protocol version `2025-11-25`. Clients requesting another protocol version are rejected during initialization.

Successful Tool calls return a `rocketmq-mcp.v2` envelope with `request_id`, cluster, RFC 3339 observation time, freshness, cache status, partial status, warnings, and typed data. Correctable input and backend failures return Tool execution errors with a stable code, retryability, suggestions, and the request identifier.

Read-only Tool calls also return a ResourceLink for the corresponding live Resource. Tool and Resource requests share the application-level `QueryFacade`, bounded TTL cache, and singleflight coordination, so an identical query can be replayed without starting a second admin session while its entry is fresh.
Both surfaces pass through the same authorization, audit, redaction, row-bound, byte-bound, and stable-error pipeline. Arrays are bounded to 1,000 rows, structured output to 1 MiB, and truncation is reported with `partial = true` and the stable `output_rows_truncated` warning.

## Build

Run commands from the repository root.

```bash
cd rocketmq-ai/rocketmq-mcp
cargo check --locked
python scripts/check_read_only_boundary.py
cargo test --locked
cargo clippy --locked --all-targets --features streamable-http,otlp -- -D warnings
cargo doc --locked --no-deps
```

Build the default stdio binary:

```bash
cargo build --locked --release
```

Build with Streamable HTTP support:

```bash
cargo build --locked --release --features streamable-http,otlp
```

## Configuration

Start from the checked-in example:

```bash
rocketmq-ai/rocketmq-mcp/conf/mcp.example.toml
```

Important fields:

- `server.transport`: `stdio` or `streamable-http`.
- `server.http.bind`: socket address for the HTTPS transport, default `127.0.0.1:8089`.
- `server.http.endpoint`: MCP endpoint path, default `/mcp`.
- `server.http.public_base_url`: absolute public HTTPS origin used in protected-resource metadata and authentication challenges.
- `server.http.tls.cert_path` and `key_path`: server certificate chain and private key. The complete pair is verified before an atomic generation is published; failed reloads keep the last-known-good generation.
- `server.http.allowed_origins`: allowed browser origins when origin validation is enabled.
- `server.http.auth.mode`: `development-token` for loopback development or `oauth-jwt` for production JWT access tokens.
- `server.http.auth.development_tenant`: optional tenant attached to the loopback development identity.
- `server.http.auth.issuer`, `audience`, `required_scopes`, `jwt_algorithm`, and `jwks_url`: OAuth resource-server validation settings. OAuth accepts only RS256 tokens carrying a `kid`, and the JWKS endpoint must use HTTPS.
- `server.http.auth.jwks_ca_path`: optional readable PEM CA bundle for a private HTTPS JWKS issuer. Relative paths are resolved from the configuration file, parsed as certificates, and never logged.
- `server.http.auth.jwks_refresh_seconds` and `jwks_max_stale_seconds`: bounded refresh and last-known-good windows. A fetch or parse failure never clears an already verified key generation.
- `server.http.auth.jwt_key_env`: retained only for configuration compatibility; OAuth does not use a static key fallback.
- `server.http.auth.protected_resource_metadata_path`: unauthenticated OAuth protected-resource metadata endpoint.
- `clusters[].name`: logical cluster name used by tools, resources, and prompts.
- `clusters[].rocketmq_cluster_name`: optional physical NameServer cluster name when it differs from the logical MCP name.
- `clusters[].namesrv_addr`: RocketMQ namesrv address for admin queries.
- `clusters[].tenant`: optional exact-match tenant boundary for JWT `rocketmq_tenant` claims.
- `clusters[].credentials.file`: mounted YAML reference containing `access_key`, `secret_key`, and an optional
  `security_token`; the regular file is bounded to 64 KiB.
- `clusters[].credentials.access_key_env`, `secret_key_env`, and optional `security_token_env`: environment
  variable references used instead of a file. Inline credential values and mixed file/environment sources are rejected.
- `security.permissions_file`: executable role, tool, and cluster policy. Claims roles and `rocketmq_clusters` are intersected with this policy.
- `security.max_concurrent_requests_per_cluster`: bounded concurrent Tool and Resource work per configured cluster.
- `security.rate_limit_per_minute`: per-principal, per-cluster, per-operation limit.
- `audit.sink`: `memory`, `file`, or `tracing`.
- `audit.queue_capacity`: maximum number of accepted records waiting for the asynchronous writer.
- `audit.max_record_bytes`: maximum serialized NDJSON record size after redaction and deterministic field bounds.
- `audit.queue_max_bytes`: maximum bytes retained by the writer queue. Record-count and byte admission are enforced
  independently with non-blocking `try` semantics.
- `cache.enabled`: enables or bypasses the shared query cache.
- `cache.max_entries`: maximum number of in-memory entries; it must be greater than zero when caching is enabled.
- `cache.*_ttl_ms`: per-query-family freshness windows for overview, topic, broker, and consumer-lag data.
- `diagnosis.consumer_lag_policy_profile`: server-owned policy identifier reported with each diagnosis.
- `diagnosis.consumer_lag_threshold`: server-owned threshold used by consumer-lag rules.

OpenTelemetry settings use the same merge order as the other RocketMQ services:
service defaults, then the `[observability]` file section, then only telemetry
environment variables that are actually present. In other words, environment
variables override file values, and file values override defaults. Supported
environment overrides include `ROCKETMQ_METRICS_*`, the standard
`OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_PROTOCOL`, and the
MCP-specific `ROCKETMQ_MCP_TRACE_SAMPLE_RATIO`. An absent metrics environment
variable does not disable a signal selected in the file.

Treat `[observability.otlp].headers` as secret material. Do not place OTLP
headers or tokens in a broadly readable ConfigMap; use an access-restricted,
secret-mounted configuration file when headers are required. Raw headers,
resource attributes, and collector endpoints are not written to startup logs.

Cache keys include the schema version, visibility class, query kind, resolved cluster, and normalized query parameters. Failures are not cached. Concurrent misses for the same key are coalesced, and `cache_status` reports `miss`, `hit`, or `bypass`. Embedders can call `McpApp::invalidate_cache()` to clear all entries explicitly. Cumulative hit, miss, bypass, eviction, invalidation, and coalesced-waiter counters are emitted at trace level after Tool and Resource requests.

Audit records use `schema_version = 1`, redact sensitive assignments and control characters before admission, and
bound every variable-length field. Shutdown closes admission, drains accepted records in FIFO order, flushes the sink,
and then shuts down runtime tasks against the same absolute deadline. Embedders that need machine-readable lifecycle
evidence can call `McpApp::shutdown_with_deadline()` and inspect `McpShutdownReport`; `McpApp::shutdown()` retains the
ten-second compatibility wrapper and logs unhealthy reports.

Command-line overrides:

```bash
rocketmq-mcp --config rocketmq-ai/rocketmq-mcp/conf/mcp.example.toml --transport stdio
rocketmq-mcp --config rocketmq-ai/rocketmq-mcp/conf/mcp.example.toml --transport streamable-http --bind 127.0.0.1:8089 --endpoint /mcp
```

The configuration path is mandatory through `--config` or `ROCKETMQ_MCP_CONFIG`. Relative permission, TLS, JWKS CA, and audit paths are resolved from the configuration file's directory, so startup does not depend on the caller's current working directory.
RocketMQ request-signing credentials are resolved from the configured reference at startup and again for each new
read session, enabling mounted-secret rotation without placing a secret in the TOML, command line, logs, or debug
output. The associated Broker user should remain a normal user with only Topic/Group/Cluster `GET`; MCP rejects
mutation capability at both its compile-time dependency boundary and Broker authorization boundary.

## stdio Usage

Use stdio for local desktop clients. Logs are written to stderr so stdout remains valid MCP JSON-RPC traffic.

```bash
cargo run -- \
  --config conf/mcp.example.toml \
  --transport stdio
```

For a release binary:

```bash
target/release/rocketmq-mcp \
  --config rocketmq-ai/rocketmq-mcp/conf/mcp.example.toml \
  --transport stdio
```

## Streamable HTTPS Usage

Streamable HTTPS requires the `streamable-http` feature and a valid certificate/key pair. Use a static token only for reviewed loopback development; use OAuth JWT validation in production.

PowerShell:

```powershell
$env:ROCKETMQ_MCP_HTTP_TOKEN = "replace-with-a-long-random-token"
cargo run --features streamable-http -- `
  --config conf/mcp.example.toml `
  --transport streamable-http `
  --bind 127.0.0.1:8089 `
  --endpoint /mcp
```

Bash:

```bash
export ROCKETMQ_MCP_HTTP_TOKEN=replace-with-a-long-random-token
cargo run --features streamable-http -- \
  --config conf/mcp.example.toml \
  --transport streamable-http \
  --bind 127.0.0.1:8089 \
  --endpoint /mcp
```

Clients connect to `https://127.0.0.1:8089/mcp` and send:

```text
Authorization: Bearer replace-with-a-long-random-token
Accept: application/json, text/event-stream
```

For production, set `mode = "oauth-jwt"` and configure `issuer`, `audience`, `required_scopes`, `jwks_url`, and the JWKS refresh windows. Set `jwks_ca_path` when that HTTPS endpoint uses a private CA. Clients must send an RS256 access token with a known `kid` and valid signature, issuer, audience, expiry, and required scope. The server publishes OAuth protected-resource metadata at the configured `protected_resource_metadata_path`; this endpoint remains available without a bearer token for client discovery and its absolute HTTPS URI is included in bearer challenges.

`permissions.example.toml` is loaded at startup. Verified principal, client, roles, scopes, `rocketmq_tenant`, and `rocketmq_clusters` claims propagate through the real MCP handler to RBAC, tenant boundary, cluster allow-list, rate-limit, and audit decisions; an HTTP request cannot substitute the local stdio identity. Tool, Resource, and Prompt discovery are filtered by this policy, and Resource reads and Tool calls are enforced again at execution time. Audit records contain the verified principal and client identifier but never store the bearer token.

Authorization, source, and output failures expose stable, sanitized codes such as `unauthorized_scope`, `tenant_mismatch`, `cluster_not_allowed`, `rate_limited`, `source_unavailable`, and `output_too_large`. Error envelopes include `retryable` and `correlation_id`; they do not echo credentials or tenant details.

For OTLP export, build with `--features otlp` and either configure the
`[observability]` file section shown in `conf/mcp.example.toml` or set both
`OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`. A missing
or blank endpoint environment variable leaves the file selection unchanged.
When a non-blank endpoint environment variable is present, any protocol other
than `grpc` is rejected at startup; the endpoint authority is never written to
logs or the authenticated observability status Resource.
`ROCKETMQ_MCP_TRACE_SAMPLE_RATIO` optionally overrides trace sampling with a
finite value from `0.0` through `1.0`. When it is unset, the production default
remains `0.01`; invalid values fail startup without being echoed in the error.

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
        "/path/to/rocketmq-rust/rocketmq-ai/rocketmq-mcp/conf/mcp.example.toml",
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
- Working directory: arbitrary; paths are owned by the explicit configuration file.

For HTTP-capable clients, use the Streamable HTTPS URL `https://127.0.0.1:8089/mcp` and configure the bearer token as an HTTP authorization header.

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

- `rocketmq://clusters/{cluster}/capabilities`
- `rocketmq://clusters/{cluster}/overview`
- `rocketmq://clusters/{cluster}/topics`
- `rocketmq://clusters/{cluster}/topics/{topic}`
- `rocketmq://clusters/{cluster}/topics/{topic}/route`
- `rocketmq://clusters/{cluster}/brokers`
- `rocketmq://clusters/{cluster}/brokers/{broker}`
- `rocketmq://clusters/{cluster}/consumer-groups`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag?topic={topic}`
- `rocketmq://system/runtime/v1` (requires `rocketmq:diagnose`)
- `rocketmq://system/observability/v1` (requires `rocketmq:diagnose`)

The capability Resource identifies MCP `2025-11-25`, business schema `rocketmq-mcp.v2`, per-Tool schema digests, a total Tool-surface digest, the caller-visible Resource surface, and `mutation_supported = false`. System Resources expose only bounded, sanitized MCP-process runtime and observability state.

`resources/list` returns authorized cluster and system Resources in cursor-paginated pages. `resources/templates/list` publishes the five parameterized cluster forms. Unsupported or incomplete forms return Resource Not Found instead of a placeholder payload.
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
- Audit file errors: create the audit directory or use `audit.sink = "memory"` for local tests. Check accepted/written,
  count-capacity drops, byte-capacity drops, oversized records, sink/flush failures, and pending records/bytes before
  changing either queue limit.
