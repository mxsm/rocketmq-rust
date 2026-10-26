# RocketMQ MCP Query

`rocketmq-mcp-query` is the Model Context Protocol server for RocketMQ-Rust AI SRE and diagnostics workflows. It exposes read-only RocketMQ context, diagnostic tools, and runbook prompts to MCP clients such as Claude Desktop, Cursor, Codex, and MCP Inspector.

The `rocketmq-mcp` entrypoint re-exports this query API and launches it by default. This crate also
builds the standalone `rocketmq-mcp-query` binary. Both use the same runtime-owning entrypoint
and preserve the existing MCP protocol, tool names, configuration, and environment variables.

The frozen MCP 2025-11-25 contract is documented in the
[MCP contract](../../rocketmq-doc/en/07-rocketmq-mcp-contract-v2.md).
Production release gates, external-cluster expectations, and rollback guidance are documented in the
[production validation guide](docs/production-validation.md).
The checked-in surface and validation baseline are listed in the
[implementation baseline](docs/implementation-baseline.md).
The complete argument, output, pagination, authorization, and example reference for the checked-in Tool catalog is
available in the [Tool Reference](docs/tool-reference.md).

## What It Is

- A standalone MCP server binary named `rocketmq-mcp-query`.
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
- `change-planning`: deprecated. Registers non-mutating change planning Tools and still requires runtime policy.
- `security-admin`: registers `rocketmq_list_acl_users`, which lists the ACL users of a cluster without their passwords. A call still needs the scope `rocketmq:security-admin` and a role that holds the Tool.
- `auth`: feature marker enabled by `streamable-http`; it is not a switch to bypass HTTP authentication.

## Safety Boundary

The default profile is diagnostics-oriented and read-only:

- `security.profile = "diagnose"` allows read-only and diagnosis tools.
- `security.allow_change_planning = false` blocks planning Tools unless explicitly enabled. The setting is deprecated with the feature.
- `security.sanitize_output = true` redacts configured sensitive output patterns.
- `audit.enabled = true` records tool decisions and HTTP rejections through a bounded asynchronous writer.
- `server.stdio.log_to_stderr = true` keeps stdout reserved for MCP protocol frames.

For HTTP deployments, keep `server.http.bind` on loopback unless there is a reviewed network boundary. `server.http.auth.mode = "development-token"` is explicitly for loopback development. Production deployments must use `oauth-jwt` and validate signed issuer/audience-bound access tokens against the configured HTTPS JWKS endpoint. The server never forwards an incoming bearer token to RocketMQ.

When `change-planning` is compiled, planning Tools are still controlled by runtime policy. They return a plan, impact analysis, and rollback suggestions. Their schemas contain no Apply mode, operator identity, or confirmation token, and no mutation API is called.

When `security-admin` is compiled, `rocketmq_list_acl_users` is still refused to a caller without the scope `rocketmq:security-admin`, which no other scope and no local profile other than `security_admin` implies, and to a caller whose roles do not hold the Tool. No example role holds it. The Tool returns user names, types, and statuses. The password a Broker sends with each user is dropped inside the RocketMQ client, before the read reaches this server's code.

The server targets MCP protocol version `2025-11-25` and answers no other. A client that requests another version is rejected during initialization. Over Streamable HTTP, a request whose `MCP-Protocol-Version` header names another version gets HTTP 400 with the body `unsupported_protocol_version`, with or without an earlier `initialize`, and a request that names a version for itself in its metadata is held to the same one. `server/discover` advertises `2025-11-25` alone. A request without the header is still answered.

Successful Tool calls return a `rocketmq-mcp.v2` envelope with `request_id`, cluster, RFC 3339 observation time, freshness, cache status, partial status, warnings, and typed data. The envelope is sanitized and held to the row and byte bounds once, and the text block that mirrors it is written from that processed value, so the two always agree. The list Tools take `response_format`: `detailed` (default) returns every field of each item, `concise` only the fields that name it. Correctable input and source unavailability return [Tool execution errors](docs/tool-reference.md#error-responses) with a stable code, retryability, suggestions, and the request identifier.

Resource-backed Tool calls return a ResourceLink only when the selected target has one canonical, safely representable Resource URI. A target accepted by the existing Tool contract but not representable as a Resource keeps the Tool's normal success/error and data semantics; only the ResourceLink is omitted, while genuinely sensitive target values remain sanitized. Tool and Resource requests share the application-level `QueryFacade`, bounded TTL cache, and singleflight coordination, so an identical query can be replayed without starting a second admin session while its entry is fresh. Each verified request selects one of two closed query visibility classes: read-only HTTP principals and local read-only stdio profiles use `standard`, while principals or local profiles with diagnosis or planning access use `sensitive`. Requests share query state within a class, but ordinary cache entries, snapshots, singleflight work, and continuation cursors never cross classes.
Both surfaces pass through the same authorization, audit, redaction, row-bound, byte-bound, and stable-error pipeline. Arrays are bounded to 1,000 rows, structured output to 1 MiB, and truncation is reported with `partial = true` and the stable `output_rows_truncated` warning.

## Public API And Error Boundary

Embedders use `McpResult<T>` for MCP startup and transport operations. `McpError` is the single opaque operational-error facade: `McpApp::bootstrap`, `McpApp::bootstrap_validated`, `app::init_tracing`, and the stdio and Streamable HTTP `serve`, `serve_with_lifecycle`, and `build_router` entry points all return `McpResult`. There are no alternate or compatibility error-return APIs.

Transport clients receive fixed, redacted public errors rather than implementation causes. HTTP authentication and authorization rejections use their fixed protocol responses, while operational HTTP failures use a safe fixed error response. For stdio, stdout contains MCP JSON-RPC protocol frames only; diagnostics and implementation causes are never written there.

## Build

`rocketmq-mcp-query` is a standalone Cargo package. Unless a command says otherwise, run the commands below from the
MCP crate root, `rocketmq-ai/rocketmq-mcp-query`, after changing to it from the repository root.

```bash
cd rocketmq-ai/rocketmq-mcp-query
cargo fmt --all -- --check
cargo check --locked
python scripts/check_read_only_boundary.py
cargo test --locked
cargo test --locked --all-features
cargo clippy --locked --all-targets --features streamable-http -- -D warnings
cargo doc --locked --no-deps
```

From that same MCP crate root, build the default stdio binary:

```bash
cargo build --locked --release
```

From that same MCP crate root, build with Streamable HTTP support:

```bash
cargo build --locked --release --features streamable-http,otlp
```

## Configuration

Start from the checked-in example:

[`conf/mcp.example.toml`](conf/mcp.example.toml).

Important fields:

- `server.transport`: `stdio` or `streamable-http`.
- `server.request_timeout_ms`: time one request may spend on RocketMQ sources, default `25000`. Every query a
  Tool or Resource runs shares this budget, so a composite Tool such as the cluster overview takes at most one
  budget. When it ends the caller gets a `backend_timeout` Tool error. The value must be at least `1000` and less
  than `30000`, the HTTPS transport's request timeout, so that the structured error always arrives first.
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
- JWKS handling is shared with the control MCP through [`rocketmq-mcp-auth`](../rocketmq-mcp-auth/README.md). A JWKS entry is used when it is an RSA key of 2048 to 8192 bits with exponent 65537, its `use` is absent or `sig`, its `key_ops` is absent or lists `verify`, its `alg` is absent or `RS256`, and its `kid` is 1 to 128 printable ASCII characters. Other entries and members such as `x5c` are ignored. A token naming an unknown `kid` triggers one JWKS fetch; while the key is still missing or the fetch fails, the next fetch waits five seconds. A Bearer token above 16 KiB is rejected before it is parsed.
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
  A call that finds no free permit fails with `concurrency_limited`.
- `security.rate_limit_per_minute`: per-principal, per-cluster, per-operation budget. It holds one minute of
  calls and refills evenly, so a caller that used it up gets one call back every `60 / rate_limit_per_minute`
  seconds. A refused Tool call fails with `rate_limited` and names the wait in `retry_after_ms`.
- `security.http_rate_limit_per_minute`: optional per-principal budget of HTTP requests, whatever they ask for.
  It is counted apart from the budget of a single operation and defaults to ten times
  `rate_limit_per_minute`. A refused request gets HTTP `429` with a `Retry-After` header in whole seconds.
- `security.pseudonym_key_env`: optional name of an environment variable that holds the key for client and message
  pseudonyms, 32 to 1024 bytes after surrounding whitespace is removed (for example the output of
  `openssl rand -hex 32`). Replicas that share the key return the same pseudonym for the same identifier. Without it
  each process draws a random key, so pseudonyms change on restart and differ between replicas. The key itself never
  belongs in the configuration file.
- `audit.sink`: `memory`, `file`, or `tracing`.
- `audit.queue_capacity`: maximum number of accepted records waiting for the asynchronous writer.
- `audit.max_record_bytes`: maximum serialized NDJSON record size after redaction and deterministic field bounds.
- `audit.queue_max_bytes`: maximum bytes retained by the writer queue. Record-count and byte admission are enforced
  independently with non-blocking `try` semantics.
- `audit.max_file_bytes`: size at which the `file` sink seals the audit file, 64 MiB by default. The sink keeps the
  file open. A record that would make the file larger moves it to `<path>.000001`, `<path>.000002`, and so on, and
  starts a new file under `audit.path`. A record is never split, and the server never replaces or deletes a sealed
  file, so removing old files is for the operator. `0` keeps one file, whatever its size. When something else moves
  the file away or empties it, such as a log shipper, the next record starts a new file under `audit.path`.
- `cache.enabled`: enables or bypasses the shared query cache.
- `cache.max_entries`: maximum number of in-memory entries; it must be greater than zero when caching is enabled.
- `cache.*_ttl_ms`: per-query-family freshness windows for overview, topic, broker, and consumer-lag data.
  `cache.broker_config_ttl_ms`, `cache.message_metadata_ttl_ms`, and `cache.proxy_state_ttl_ms` are optional and
  give the Broker configuration summary, message metadata, and Proxy drain state a window of their own; each
  defaults to `cache.broker_metrics_ttl_ms`.
- `cache.stale_if_error_ms`: how long a result that is no longer current still answers when its source is
  unavailable or does not answer in time. Such a result carries the warning `stale_cache_value`, reports
  `cache_status = hit`, and states its real age in `freshness_ms`. `0`, the default, never serves one; the maximum
  is 300000. A paged result is served this way only for the same request and only while the server still keeps it
  for its cursors (`cache.cursor_snapshot_ttl_ms`).
- `diagnosis.consumer_lag_policy_profile`: server-owned policy identifier reported with each diagnosis.
- `diagnosis.consumer_lag_threshold`: backlog, in messages, up to which a Consumer Group is healthy. Default 1000.
- `diagnosis.consume_delay_threshold_ms`: how far behind production a group may be. A larger backlog is graded by its size and by this delay; below it, the backlog is only `low`. Default 300000.
- `diagnosis.pop_inflight_threshold`: in-flight messages from which a POP group counts as holding too many unacknowledged. Default 1000.
- `diagnosis.retry_activity_window_ms`: a retry or dead-letter Topic that received a message within this window counts as growing. Default 600000.
- `[[diagnosis.overrides]]`: thresholds for one `topic`, one `consumer_group`, or one pair of them. An entry sets only the thresholds it names and must name at least one. Every matching entry applies: a Topic entry first, then a Consumer Group entry, then the entry that names both. Names are matched exactly, and two entries may not select the same target.

Unknown keys are rejected at startup, in the configuration file and in the permissions file, so a misspelled key
cannot silently turn a setting off. The startup error names the key and the table that holds it, for example
``unknown key `tenent` in `clusters[0]` ``, and never includes a configured value. A key that is not a plain name is
rejected without being echoed. The one exception is the `[logging]` table, which is parsed by the logging type shared
with the other RocketMQ services and still ignores unknown keys.

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

Cache keys include the schema version, the stable `standard` or `sensitive` visibility name, query kind, resolved cluster, and normalized query parameters. The shared query state never retains a principal identifier, tenant, role, scope, client identifier, or bearer token. Concurrent misses for the same class and key are coalesced: one request reads the source, and the others get its result or its failure. A `source_unavailable` failure also answers identical requests for up to one second, so a cluster that is down is not asked again by every call; no other failure is kept. The Topic and Consumer Group names of a cluster are read once per visibility class while they are current, and every filter and page size is selected from them in memory. `cache_status` reports `miss`, `hit`, or `bypass`. Embedders can call `McpApp::invalidate_cache()` to clear all entries explicitly. Cumulative hit, miss, bypass, eviction, invalidation, and coalesced-waiter counters are emitted at trace level after Tool and Resource requests.

Audit records use `schema_version = 2`, redact sensitive assignments and control characters before admission, and
bound every variable-length field. Version 2 adds one optional field to version 1, `target`: the Topic, Consumer
Group, producer group, Broker, Proxy, and Controller names that a Tool call or a Resource read asked about, for
example `{"topic":"orders","consumer_group":"billing","broker_names":["broker-a"]}`. Only a value that is a valid
name of its kind is written, with at most eight Broker and eight Controller names (`"truncated":true` marks a longer
list), and never a message identifier, a key, a filter, or a completion prefix. A call that named nothing leaves a
record without `target`, which has exactly the fields of version 1. One file may hold records of both versions, and
a reader of version 1 reads version 2 by ignoring `target`. A record that would exceed `audit.max_record_bytes`
with its target is written without it. The `tracing` sink logs the same object as the text of one field,
`audit_target`. Shutdown closes admission, drains accepted records in FIFO order, flushes the sink,
and then shuts down runtime tasks against the same absolute deadline. Embedders that need machine-readable lifecycle
evidence can call `McpApp::shutdown_with_deadline()` and inspect `McpShutdownReport`; `McpApp::shutdown()` retains the
ten-second compatibility wrapper and logs unhealthy reports.

Command-line overrides:

```bash
# Run from rocketmq-ai/rocketmq-mcp-query, or use an absolute path to your copied config.
rocketmq-mcp --config conf/mcp.example.toml --transport stdio
rocketmq-mcp --config conf/mcp.example.toml --transport streamable-http --bind 127.0.0.1:8089 --endpoint /mcp
```

The configuration path is optional for local stdio startup. With neither `--config` nor `ROCKETMQ_MCP_CONFIG`,
the server uses embedded configuration and permissions: the `local-dev` cluster at `127.0.0.1:9876`, the
`diagnose` profile, output sanitization, and enabled audit records through stderr logging. No configuration
or audit files are required or created. RocketMQ queries still need a running local NameServer and Broker.
HTTP requires explicit configuration. `--config` takes precedence over `ROCKETMQ_MCP_CONFIG`, and an invalid
explicit configuration fails startup. Relative permission, TLS, JWKS CA, and audit paths are resolved from
the configuration file's directory, so startup does not depend on the caller's current working directory.
RocketMQ request-signing credentials are resolved from the configured reference at startup and again for each new
read session, enabling mounted-secret rotation without placing a secret in the TOML, command line, logs, or debug
output. The associated Broker user should remain a normal user with only Topic/Group/Cluster `GET`; MCP rejects
mutation capability at both its compile-time dependency boundary and Broker authorization boundary.

## stdio Usage

Use stdio for local desktop clients. Logs are written to stderr so stdout remains valid MCP JSON-RPC traffic.

```bash
cargo run --locked
```

For a release binary:

```bash
target/release/rocketmq-mcp-query
```

The process waits for MCP JSON-RPC requests on stdin. Add `--config <path>` to use custom cluster,
permissions, or audit settings.

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

Authorization, source, and output failures expose stable, sanitized codes such as `unauthorized_scope`, `tenant_mismatch`, `cluster_not_allowed`, `rate_limited`, `source_unavailable`, and `output_too_large`. A Topic, Consumer Group, Broker, message, or Proxy that does not exist is reported as the non-retryable `not_found` with an `entity` field instead of a retryable source failure. Error envelopes include `retryable` and `correlation_id`; they do not echo credentials or tenant details. The full code table is in `docs/tool-reference.md`.

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
      "command": "<repository-root>/rocketmq-ai/rocketmq-mcp-query/target/release/rocketmq-mcp-query.exe",
      "args": [
        "--config",
        "<repository-root>/rocketmq-ai/rocketmq-mcp-query/conf/mcp.example.toml",
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
      "command": "<repository-root>/rocketmq-ai/rocketmq-mcp-query/target/release/rocketmq-mcp-query",
      "args": [
        "--config",
        "<repository-root>/rocketmq-ai/rocketmq-mcp-query/conf/mcp.example.toml",
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

- Command: the built `rocketmq-mcp-query` binary.
- Args: `--config <config-path> --transport stdio`.
- Working directory: arbitrary; paths are owned by the explicit configuration file.

For HTTP-capable clients, use the Streamable HTTPS URL `https://127.0.0.1:8089/mcp` and configure the bearer token as an HTTP authorization header.

## Tools

The default feature set exposes 36 Tools:

- `rocketmq_list_clusters`: list the logical clusters the caller may query, with their Proxy and Controller aliases. It is the one Tool that takes no `cluster`.
- `rocketmq_list_brokers`: list the Broker instances of one cluster without counting its Topics and Consumer Groups.
- `rocketmq_get_cluster_overview`: summarize one configured cluster.
- `rocketmq_list_topics`: list a filtered, cursor-paginated topic page.
- `rocketmq_describe_topic`: describe a topic with bounded queue data.
- `rocketmq_get_topic_route`: get bounded topic route data.
- `rocketmq_list_consumer_groups`: list a filtered, cursor-paginated consumer-group page.
- `rocketmq_get_consumer_lag`: get bounded consumer progress and lag rows.
- `rocketmq_describe_broker`: describe broker state.
- `rocketmq_get_broker_diagnostics`: get bounded broker readiness, store, recovery, and security diagnostics.
- `rocketmq_get_broker_config_summary`: get a fixed allowlisted broker configuration summary.
- `rocketmq_get_broker_log_filter_state`: get temporary state for an allowlisted broker logger.
- `rocketmq_get_proxy_drain_state`: get bounded drain progress for a configured logical Proxy.
- `rocketmq_diagnose_consumer_lag`: grade the backlog of one Consumer Group on one Topic and name its likely causes from lag, connection, configuration, permission, and retry evidence.
- `rocketmq_diagnose_consumer_group`: explain the state of one Consumer Group over every Topic it consumes: connections, configuration, backlog, and retries.
- `rocketmq_diagnose_broker_health`: check one Broker, its master and its replicas: availability, readiness, store, replication, and configuration.
- `rocketmq_get_cluster_health`: check a cluster at a glance and name the Brokers to look at first.
- `rocketmq_diagnose_message_delivery`: check the path from one Topic to one Consumer Group and, with a message identifier, where that message stands; never returns a message body.
- `rocketmq_list_consumer_connections`: list bounded pseudonymous consumer connections.
- `rocketmq_list_producer_connections`: list bounded pseudonymous producer connections.
- `rocketmq_get_message_metadata`: get fixed body-free message metadata.
- `rocketmq_get_topic_config_state`: get bounded Topic configuration version observations.
- `rocketmq_get_consumer_group_config_state`: get bounded Consumer Group configuration version observations.
- `rocketmq_get_topic_stats`: get bounded per-queue Topic statistics and aggregate totals.
- `rocketmq_get_topic_config`: get fixed address-free Topic configuration observations.
- `rocketmq_get_consumer_group_details`: get fixed address-free group configuration and connection observations.
- `rocketmq_get_consumer_progress`: get bounded per-queue progress and complete aggregate totals.
- `rocketmq_get_ha_status`: get bounded HA and optional Controller synchronization observations.
- `rocketmq_get_controller_metadata`: get bounded metadata for configured logical Controllers.
- `rocketmq_get_nameserver_config_summary`: get fixed allowlisted NameServer configuration summaries.
- `rocketmq_list_topic_consumers`: list a cursor-paginated page of the Consumer Groups that consume one Topic, joined over every Broker that hosts it.
- `rocketmq_get_consumer_running_info`: ask one connected consumer client, named by its pseudonym, for its subscriptions, rates, and the queues it holds; never requests a thread dump.
- `rocketmq_get_dlq_summary`: count the dead letters of one Consumer Group and report when the newest one was stored, in total and for each Broker; never returns a message.
- `rocketmq_query_message_trace`: read the trace records clients wrote about one message; never returns a body, and returns client hosts, tags, and keys only with the diagnose scope, the hosts and keys as pseudonyms.
- `rocketmq_get_lite_broker_info`: get the Lite Topic limits, counters, and bounded Lite Topic and bound-group lists of one Broker.
- `rocketmq_find_messages_by_key`: find the messages of one Topic that carry an exact key in a window of at most 24 hours, at most 32 and by pseudonyms only; needs the scope `rocketmq:message-search`.

The five diagnosis Tools return one report shape with versioned Evidence Snapshots and stable rule identifiers. Each reads its evidence in one admin session, and a read that fails makes the report partial instead of failing the diagnosis. Thresholds belong to the server: historical `time_range` and caller-controlled thresholds are intentionally unavailable until a historical metrics source exists. Naming a message in `rocketmq_diagnose_message_delivery` also needs the permission for `rocketmq_get_message_metadata`.

The all-features surface has 42 Tools: the 36 above, five `change-planning` Tools, and one `security-admin` Tool.

The five `change-planning` Tools never mutate the cluster. They are deprecated and will be removed together with the feature: plan a change with a dry run of the control MCP server (`rocketmq-mcp-control`), which reads the state the change would be applied to and uses the arguments the execution takes. Each description names its replacement, and a server that has the feature compiled and `security.allow_change_planning = true` logs a deprecation warning at startup.

- `rocketmq_plan_create_topic`
- `rocketmq_plan_update_topic_config`
- `rocketmq_plan_update_topic_permissions`
- `rocketmq_plan_update_broker_config`
- `rocketmq_plan_reset_consumer_offset`

The `security-admin` feature adds one read-only Tool:

- `rocketmq_list_acl_users`: list the ACL users that each Broker of a cluster holds, with their type and status, at most 500 rows; never returns a password or an access rule; needs the scope `rocketmq:security-admin`.

## Resources

- `rocketmq://clusters/{cluster}/capabilities`
- `rocketmq://clusters/{cluster}/overview`
- `rocketmq://clusters/{cluster}/topics`
- `rocketmq://clusters/{cluster}/topics/{topic}`
- `rocketmq://clusters/{cluster}/topics/{topic}/route`
- `rocketmq://clusters/{cluster}/topics/{topic}/stats{?limit,cursor}`
- `rocketmq://clusters/{cluster}/topics/{topic}/config`
- `rocketmq://clusters/{cluster}/brokers`
- `rocketmq://clusters/{cluster}/brokers/{broker}`
- `rocketmq://clusters/{cluster}/brokers/{broker}/diagnostics`
- `rocketmq://clusters/{cluster}/brokers/{broker}/config-summary`
- `rocketmq://clusters/{cluster}/consumer-groups`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag?topic={topic}`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}/progress{?limit,cursor}`
- `rocketmq://system/runtime/v1` (requires `rocketmq:diagnose`)
- `rocketmq://system/observability/v1` (requires `rocketmq:diagnose`)

The capability Resource identifies MCP `2025-11-25`, business schema `rocketmq-mcp.v2`, per-Tool schema digests, a total Tool-surface digest, the caller-visible Resource surface, and `mutation_supported = false`. System Resources expose only bounded, sanitized MCP-process runtime and observability state.

`resources/list` returns authorized cluster and system Resources in cursor-paginated pages. `resources/templates/list` publishes 15 stable templates, filtered by the caller's backing Tool access on at least one configured cluster whose logical name is safely representable. Resource, template, and Prompt discovery ignore configured cluster names outside that closed logical-alias contract; this does not narrow the existing Tool parameter contract for those configured clusters. Unsupported, incomplete, invalidly encoded, or unauthorized forms fail closed.
Cluster and broker path values are bounded logical aliases. Topic and consumer-group values use the closed RocketMQ ASCII name contract with strict UTF-8 percent decoding, including retry topics and groups that contain `%RETRY%`. Among the five Resource templates added for broker diagnostics/configuration, topic statistics/configuration, and consumer progress, only topic statistics and consumer progress accept `limit` and `cursor`. Existing inventory, topic, route, consumer-group, and lag page templates retain their documented pagination parameters. Unknown, duplicate, empty, noncanonical, or invalid query values are rejected.

## Prompts

- `diagnose_consumer_lag`: guided consumer lag investigation.
- `broker_health_check`: guided broker health review.
- `diagnose_broker_health`: typed read-only and diagnostic broker investigation.
- `diagnose_message_delivery`: body-free delivery investigation with optional message metadata.
- `analyze_consumer_connections`: typed read-only consumer connectivity and progress analysis.

Prompt discovery and rendering recheck the caller's Tool, tenant, cluster, scope, and profile authorization. Prompt
arguments use closed kinds and reject unknown, missing, null, non-string, blank, unsafe, or overlong values. Prompts
provide guidance only: they neither execute Tools nor create cache, session, or mutation work.

Four Prompts start with a server-side diagnosis Tool when the caller may call it: `broker_health_check` with
`rocketmq_get_cluster_health` and `rocketmq_diagnose_broker_health`, `diagnose_broker_health` with
`rocketmq_diagnose_broker_health`, `diagnose_message_delivery` with `rocketmq_diagnose_message_delivery`, and
`analyze_consumer_connections` with `rocketmq_diagnose_consumer_group`. These Tools do not decide whether a Prompt
is available: a caller who may not call them gets the same Prompt without that first step.

## Argument completion

The server declares the `completions` capability. `completion/complete` suggests values for the arguments of the
five Prompts and the variables of the 15 Resource templates that name a cluster, a Topic, a Consumer Group, or a
Broker. Other arguments, such as `message_id`, `check_level`, `filter`, `limit`, and `cursor`, have no candidates.

- Candidates are the names that start with the typed value, ignoring ASCII case, in sorted order: at most 100, with
  the number of matches in `total` and `hasMore` when there are more.
- A completion needs the permission of the listing Tool that returns the same names: `rocketmq_list_clusters`,
  `rocketmq_list_topics`, `rocketmq_list_consumer_groups`, or `rocketmq_list_brokers`, on the cluster the names
  come from. Without it the result is empty, exactly as for an argument without candidates.
- A Topic, Consumer Group, or Broker is completed inside the cluster already chosen in the request's
  `context.arguments.cluster`. Without one the result is empty; the default cluster is not assumed.
- Topic and Consumer Group candidates come from the inventory snapshot that the listing Tools page through, and
  Broker candidates from the Broker list, so completing while typing does not query the cluster again.
- Every answered or refused completion is audited as `completion:<class>` and counts against a per-principal rate
  limit of its own, so typing does not spend the budget of Tool calls.

## Troubleshooting

- `streamable-http transport requires the streamable-http feature`: rebuild or run with `--features streamable-http`.
- HTTP authentication configuration error: set the referenced token environment variable for `development-token`. For `oauth-jwt`, configure the HTTPS issuer/JWKS endpoint, audience and scopes; `jwt_key_env` is compatibility-only and does not provide an OAuth key fallback.
- HTTP `401`: check the access-token signature, expiry, issuer, audience, and `Authorization: Bearer <token>` header.
- HTTP `403`: check token scopes, role claims, `rocketmq_clusters`, `permissions.example.toml`, and browser origin policy.
- HTTP `429`: the principal used up `security.http_rate_limit_per_minute`; `Retry-After` says when the next
  request is due. Raise the budget only after reviewing client retry behavior. A Tool error `rate_limited` is
  the budget of that one Tool instead (`security.rate_limit_per_minute`), and `concurrency_limited` is
  `security.max_concurrent_requests_per_cluster`.
- Empty or invalid stdio responses: ensure no wrapper script writes logs or banners to stdout.
- No cluster data: verify `clusters[].namesrv_addr`, local network access, and RocketMQ namesrv availability.
- Audit file errors: create the audit directory or use `audit.sink = "memory"` for local tests. Check accepted/written,
  count-capacity drops, byte-capacity drops, oversized records, sink/flush failures, and pending records/bytes before
  changing either queue limit. The warning `audit file could not be sealed` means that the full file could not be
  moved to its sealed name: records are still written, and the file grows past `audit.max_file_bytes` until the
  directory accepts the move.
