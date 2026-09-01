# RocketMQ MCP implementation baseline

This document freezes the checked-in MCP surface and records the validation evidence for the current revision.
It describes implemented behavior; it does not certify a live RocketMQ deployment.

## Revision and sources

| Item | Value |
| --- | --- |
| Base revision | `3b7cad90d53b2bf134dbf7289056fd19c978b372` |
| Base revision subject | `[ISSUE #9951]🚀Add typed HA, Controller, and NameServer observation tools to RocketMQ MCP(#9954)` |
| Baseline captured | 2026-09-01, Windows Issue #9955 worktree |
| MCP workspace | `rocketmq-ai/rocketmq-mcp` (standalone Cargo workspace) |
| Protocol source | [`src/protocol/server.rs`](../src/protocol/server.rs) |
| Tool catalog | [`src/tools/catalog.rs`](../src/tools/catalog.rs) |
| Resource registry and URI parser | [`src/resources/registry.rs`](../src/resources/registry.rs), [`src/resources/uri.rs`](../src/resources/uri.rs) |
| Prompt registry | [`src/prompts/registry.rs`](../src/prompts/registry.rs) |
| Default surface snapshot | [`mcp_protocol_surface.snap`](../src/protocol/snapshots/rocketmq_mcp__protocol__server__tests__mcp_protocol_surface.snap) |
| All-features surface snapshot | [`mcp_protocol_surface_with_change_planning.snap`](../src/protocol/snapshots/rocketmq_mcp__protocol__server__tests__mcp_protocol_surface_with_change_planning.snap) |
| Default tool contract snapshot | [`tool_contract_schema_metadata.snap`](../src/tools/snapshots/rocketmq_mcp__tools__catalog__tests__tool_contract_schema_metadata.snap) |
| All-features tool contract snapshot | [`tool_contract_schema_metadata_with_change_planning.snap`](../src/tools/snapshots/rocketmq_mcp__tools__catalog__tests__tool_contract_schema_metadata_with_change_planning.snap) |

The snapshots are generated contract evidence for this revision. Their schemas, descriptions, annotations, and
ordering remain the source of truth for details not reproduced in this summary.

## Protocol envelope

- MCP protocol version: `2025-11-25`.
- Business schema: `rocketmq-mcp.v2`.
- `mutation_supported`: `false` in the capability manifest.
- Successful tool responses contain `schema_version`, `request_id`, logical cluster, RFC 3339 `observed_at`,
  `freshness_ms`, `cache_status`, `partial`, `warnings`, and typed `data`.
- Tool and resource failures use stable sanitized codes, retryability, suggestions where applicable, and a
  correlation/request identifier. Internal addresses, credentials, bearer tokens, and sensitive assignments are
  not part of the public output.
- Initialization rejects a protocol version other than `2025-11-25`.

## Tool surface

### Default features: 24 tools

These 24 tools are present in the default catalog (`read-only`, `diagnose`, and `stdio`). The `diagnose` feature
is a capability marker that includes `read-only`; diagnosis execution is controlled by the configured profile and
required authorization scope, not by a separate feature-gated tool implementation:

| Tool | Risk | Input shape | Implemented behavior |
| --- | --- | --- | --- |
| `rocketmq_get_cluster_overview` | ReadOnly | `cluster` | Summarizes brokers, topic count, and consumer-group count for one configured cluster. |
| `rocketmq_list_topics` | ReadOnly | optional `cluster`, optional `filter`, optional page `limit`/`cursor` | Lists a bounded, filtered, cursor-paginated topic page. |
| `rocketmq_describe_topic` | ReadOnly | `cluster`, `topic`, optional page `limit`/`cursor` | Describes one topic with bounded queue route information. |
| `rocketmq_get_topic_route` | ReadOnly | `cluster`, `topic`, optional page `limit`/`cursor` | Returns bounded route data for one topic. |
| `rocketmq_list_consumer_groups` | ReadOnly | optional `cluster`, optional `filter`, optional page `limit`/`cursor` | Lists a bounded, filtered, cursor-paginated consumer-group page. |
| `rocketmq_get_consumer_lag` | ReadOnly | `cluster`, `topic`, `consumer_group`, optional page `limit`/`cursor` | Returns bounded per-queue consumer progress and lag. |
| `rocketmq_describe_broker` | ReadOnly | `cluster`, `broker_name` | Describes broker state for one broker name. |
| `rocketmq_diagnose_consumer_lag` | Diagnose | `cluster`, `topic`, `consumer_group` | Aggregates read-only lag, topic-route, and broker evidence into a diagnosis report. |
| `rocketmq_get_broker_diagnostics` | Diagnose | `cluster`, `broker_name` | Returns bounded readiness, store, recovery, and security diagnostics. |
| `rocketmq_get_broker_config_summary` | ReadOnly | `cluster`, `broker_name` | Returns a fixed allowlisted broker configuration summary. |
| `rocketmq_get_broker_log_filter_state` | Diagnose | `cluster`, `broker_name`, `logger` | Returns temporary state for an allowlisted broker logger. |
| `rocketmq_get_proxy_drain_state` | Diagnose | `cluster`, `proxy_name` | Returns bounded drain progress for a configured logical Proxy. |
| `rocketmq_list_consumer_connections` | ReadOnly | `cluster`, `consumer_group`, optional page `limit`/`cursor` | Lists bounded pseudonymous consumer connections. |
| `rocketmq_list_producer_connections` | ReadOnly | `cluster`, `topic`, `producer_group`, optional page `limit`/`cursor` | Lists bounded pseudonymous producer connections. |
| `rocketmq_get_message_metadata` | ReadOnly | `cluster`, `message_id` | Returns fixed body-free message metadata. |
| `rocketmq_get_topic_config_state` | ReadOnly | `cluster`, `topic`, bounded `broker_names` | Returns Topic configuration version observations. |
| `rocketmq_get_consumer_group_config_state` | ReadOnly | `cluster`, `group`, bounded `broker_names` | Returns Consumer Group configuration version observations. |
| `rocketmq_get_topic_stats` | ReadOnly | `cluster`, `topic`, optional page `limit`/`cursor` | Returns bounded per-queue Topic statistics and aggregate totals. |
| `rocketmq_get_topic_config` | ReadOnly | `cluster`, `topic` | Returns fixed address-free Topic configuration observations. |
| `rocketmq_get_consumer_group_details` | ReadOnly | `cluster`, `consumer_group` | Returns fixed address-free group configuration and connection observations. |
| `rocketmq_get_consumer_progress` | ReadOnly | `cluster`, `consumer_group`, optional page `limit`/`cursor` | Returns bounded per-queue progress and complete aggregate totals. |
| `rocketmq_get_ha_status` | Diagnose | `cluster`, optional bounded broker selection | Returns bounded HA and optional Controller synchronization observations. |
| `rocketmq_get_controller_metadata` | Diagnose | `cluster` | Returns bounded metadata for configured logical Controllers. |
| `rocketmq_get_nameserver_config_summary` | ReadOnly | `cluster` | Returns fixed allowlisted NameServer configuration summaries. |

Page `limit` is bounded to 1–200 and defaults to 50. The outer output policy additionally caps arrays at 1,000
rows and structured output at 1 MiB.

Consumer-lag diagnosis uses a server-owned policy profile and threshold. The request has no historical `time_range`
or caller-controlled threshold because the checked-in implementation has no historical metrics source.

### Optional `change-planning`: five tools

When compiled with `change-planning`, the all-features catalog has exactly 29 tools: the 24 defaults above
plus these five planning tools:

| Tool | Plan type | Required request | Result boundary |
| --- | --- | --- | --- |
| `rocketmq_plan_create_topic` | `create_topic` | `cluster`, `reason`, desired topic state | Immutable, ephemeral review plan; no mutation. |
| `rocketmq_plan_update_topic_config` | `update_topic_config` | `cluster`, `reason`, desired topic/config state | Immutable, ephemeral review plan; no mutation. |
| `rocketmq_plan_update_topic_permissions` | `update_topic_permissions` | `cluster`, `reason`, desired topic/permission state | Immutable, ephemeral review plan; no mutation. |
| `rocketmq_plan_update_broker_config` | `update_broker_config` | `cluster`, `reason`, desired broker/config state | Immutable, ephemeral review plan; no mutation. |
| `rocketmq_plan_reset_consumer_offset` | `reset_consumer_offset` | `cluster`, `reason`, desired topic/group offset state | Immutable, ephemeral review plan; no mutation. |

Every planning result has `mutates_cluster = false`, an expiry of 300 seconds, and no Apply mode, operator
identity, or confirmation token in its schema. Planning requires the feature, `security.allow_change_planning = true`,
the configured policy, and the `rocketmq:plan` scope. The default runtime setting is false. There is no Apply path
and no destructive tool in this baseline.

## Resource surface

`resources/list` advertises five cluster roots per authorized configured cluster and two protected system resources.
The fifteen cluster Resource forms below are the five listed roots plus ten parameterized forms accepted by
`resources/read`; parameterized and paginated forms are exposed through the fifteen templates below.

### Fifteen cluster Resource forms

| Form | Discovery/read behavior |
| --- | --- |
| `rocketmq://clusters/{cluster}/capabilities` | Listed cluster root; versioned capability and schema-digest manifest. |
| `rocketmq://clusters/{cluster}/overview` | Listed cluster root; cluster overview. |
| `rocketmq://clusters/{cluster}/topics` | Listed cluster root; topic inventory. |
| `rocketmq://clusters/{cluster}/topics/{topic}` | Parameterized topic details. |
| `rocketmq://clusters/{cluster}/topics/{topic}/route` | Parameterized topic route. |
| `rocketmq://clusters/{cluster}/topics/{topic}/stats{?limit,cursor}` | Parameterized topic statistics backed by one stable page snapshot. |
| `rocketmq://clusters/{cluster}/topics/{topic}/config` | Parameterized address-free topic configuration summary. |
| `rocketmq://clusters/{cluster}/brokers` | Listed cluster root; broker inventory. |
| `rocketmq://clusters/{cluster}/brokers/{broker}` | Parameterized broker details. |
| `rocketmq://clusters/{cluster}/brokers/{broker}/diagnostics` | Parameterized broker diagnostics; requires Diagnose authorization. |
| `rocketmq://clusters/{cluster}/brokers/{broker}/config-summary` | Parameterized allowlisted broker configuration summary. |
| `rocketmq://clusters/{cluster}/consumer-groups` | Listed cluster root; consumer-group inventory. |
| `rocketmq://clusters/{cluster}/consumer-groups/{group}` | Parameterized consumer-group details. |
| `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag?topic={topic}` | Parameterized consumer lag for one group/topic. |
| `rocketmq://clusters/{cluster}/consumer-groups/{group}/progress{?limit,cursor}` | Parameterized consumer progress backed by one stable page snapshot. |

Cluster and broker path values are bounded logical aliases and reject IP addresses, socket addresses, controls, and
separators. Topic and group values use the closed RocketMQ ASCII name contract with strict UTF-8 percent decoding,
including retry names such as `%RETRY%...`.
Unknown, incomplete, invalidly encoded, or unauthorized resources fail closed rather than returning a placeholder
payload. Among the five templates added for broker diagnostics/configuration, topic statistics/configuration, and
consumer progress, only topic statistics and consumer progress accept `limit` and `cursor`. Existing inventory,
topic, route, consumer-group, and lag page templates retain their listed pagination parameters. Duplicate, empty,
literal-null, noncanonical, unknown, or out-of-range query values are rejected.

### Two protected system Resources

These are never cluster-scoped and require diagnostic authorization (`rocketmq:diagnose`) and a valid diagnostic
role:

- `rocketmq://system/runtime/v1` — bounded, sanitized MCP runtime diagnostics.
- `rocketmq://system/observability/v1` — sanitized MCP observability status.

### Fifteen Resource templates

`resources/templates/list` publishes exactly these templates:

1. `rocketmq://clusters/{cluster}/topics/{topic}` (`rocketmq_topic`)
2. `rocketmq://clusters/{cluster}/topics/{topic}/route` (`rocketmq_topic_route`)
3. `rocketmq://clusters/{cluster}/consumer-groups/{group}` (`rocketmq_consumer_group`)
4. `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic}` (`rocketmq_consumer_lag`)
5. `rocketmq://clusters/{cluster}/brokers/{broker}` (`rocketmq_broker`)
6. `rocketmq://clusters/{cluster}/topics{?filter,limit,cursor}` (`rocketmq_topics_page`)
7. `rocketmq://clusters/{cluster}/topics/{topic}{?limit,cursor}` (`rocketmq_topic_page`)
8. `rocketmq://clusters/{cluster}/topics/{topic}/route{?limit,cursor}` (`rocketmq_topic_route_page`)
9. `rocketmq://clusters/{cluster}/consumer-groups{?filter,limit,cursor}` (`rocketmq_consumer_groups_page`)
10. `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic,limit,cursor}` (`rocketmq_consumer_lag_page`)
11. `rocketmq://clusters/{cluster}/brokers/{broker}/diagnostics` (`rocketmq_broker_diagnostics`)
12. `rocketmq://clusters/{cluster}/brokers/{broker}/config-summary` (`rocketmq_broker_config_summary`)
13. `rocketmq://clusters/{cluster}/topics/{topic}/stats{?limit,cursor}` (`rocketmq_topic_stats`)
14. `rocketmq://clusters/{cluster}/topics/{topic}/config` (`rocketmq_topic_config`)
15. `rocketmq://clusters/{cluster}/consumer-groups/{group}/progress{?limit,cursor}` (`rocketmq_consumer_progress`)

Templates are visible only when the principal can use their exact backing Tool on at least one allowed configured
cluster whose name satisfies the closed logical-alias Resource contract. Resource, template, and Prompt discovery
ignore configured clusters that cannot be represented canonically; Tool execution retains its existing configured-
cluster parameter contract. Discovery cursors are authenticated with a per-application CSPRNG key and bind the surface, offset,
registry generation, and canonical complete principal authorization context. They cannot be reused across
principals, visibility contexts, Resource/template surfaces, configuration/registry instances, or server restarts.

### Five Prompts

`prompts/list` advertises exactly:

- `diagnose_consumer_lag` — guided consumer-lag investigation with `cluster`, `topic`, and `consumer_group`.
- `broker_health_check` — guided broker-health review with `cluster`, optional `broker_name`, and optional
  `check_level` (`quick`, `standard`, or `deep`).
- `diagnose_broker_health` — broker diagnosis using cluster overview, broker description, diagnostics,
  allowlisted configuration, and HA evidence.
- `diagnose_message_delivery` — body-free route, topic, group, and progress diagnosis with conditional message
  metadata when `message_id` is present.
- `analyze_consumer_connections` — consumer connection, group-detail, and progress analysis.

Prompt discovery requires every unconditional Tool on one allowed configured cluster. `prompts/get` revalidates the
selected cluster and every conditional Tool. Availability authorization precedes prompt-specific schema errors.
Arguments use closed kinds and reject unknown, missing, null, non-string, blank, overlong, control/newline,
backtick, encoded delimiter, and template-delimiter values. Topic and group values use the closed RocketMQ ASCII
contract; message identifiers use a separate closed safe-token contract. Rendering inserts JSON-quoted values into
a fixed untrusted-data block and creates no Tool execution, cache entry, admin session, or mutation path.

## Features and transport availability

The `Cargo.toml` feature graph at this revision is:

| Feature | Default? | Effect |
| --- | --- | --- |
| `read-only` | Yes | Base read-only capability marker. |
| `diagnose` | Yes | Capability marker that includes `read-only`; diagnosis execution is controlled by profile and scope authorization. |
| `stdio` | Yes | Enables RMCP stdio transport support. |
| `streamable-http` | No | Enables RMCP Streamable HTTP server, TLS/network dependencies, and the `auth` feature. |
| `observability` | No | Enables in-process RocketMQ observability support without selecting a remote exporter. |
| `otlp` | No | Includes `observability` and OTLP metrics, traces, and logs through the OTLP gRPC exporter. |
| `auth` | No | Internal authentication feature; selected by `streamable-http`. |
| `change-planning` | No | Registers the five non-mutating planning tools. |

Default feature set: `read-only`, `diagnose`, `stdio`. The default binary is intended for local stdio. HTTPS is
available only with `streamable-http`; production OAuth requires that transport feature and its TLS/JWKS setup.

## Authorization and read-only adapter boundary

Authorization is applied to discovery and execution. The policy engine combines role includes, tool allow/deny
patterns, configured cluster allow-lists, principal `rocketmq_clusters`, exact tenant binding, and required scopes.
The default example profile is `diagnose`: it can use read-only and diagnosis tools, while planning remains disabled
unless explicitly enabled. HTTP principals are derived from the verified request; they cannot substitute the local
stdio identity. System resources require diagnostic scope and a valid diagnostic role.

The only RocketMQ admin-core dependency is `rocketmq-admin-core` with `default-features = false` and
`read-client-adapter`. The read-only boundary checker rejects mutation-client/admin features, mutation imports, and
forbidden direct dependencies. Credentials are resolved from a mounted bounded YAML file or environment references
for each new read session and are never serialized into public output.

## Output, cache, and audit behavior

- Redaction removes internal topology and sensitive fields recursively when `security.sanitize_output = true`.
- Output arrays are capped at 1,000 rows; structured JSON is capped at 1 MiB. Truncation sets `partial = true` and
  adds `output_rows_truncated`; an oversized result returns `output_too_large`.
- Cache entries use schema version, visibility class, query kind, resolved cluster, and normalized parameters. The
  configured TTLs cover overview, topic, broker, and consumer-lag families. Failures are not cached, concurrent
  identical misses are coalesced, and `cache_status` is `miss`, `hit`, or `bypass`. An explicit invalidation clears
  all entries.
- Tool and Resource calls share the query facade, cache, singleflight coordination, authorization, audit, redaction,
  row, byte, and stable-error policies. A Resource-backed Tool result includes a ResourceLink only when its target is
  canonical and safely representable by the closed Resource URI contract. A Tool-compatible but unrepresentable
  target keeps the Tool's existing success/error and data semantics and omits only the link; sensitive target values
  are still sanitized. Topic-statistics and consumer-progress continuation cursors can cross the Tool/Resource surface without
  another upstream call; disabling the response cache still reloads new first pages while preserving existing
  bounded continuation snapshots.
- Audit records use schema version 1, redact control characters and sensitive assignments, bound variable-length
  fields, and use a bounded non-blocking queue. Shutdown closes admission, drains accepted records FIFO, flushes the
  sink, and reports drops, sink/flush failures, or pending records/bytes.
- The default example sets cache enabled with 256 entries and TTLs of 3,000 ms (overview), 5,000 ms (topic),
  2,000 ms (broker), and 1,000 ms (consumer lag); security rate limit is 60 per minute and cluster concurrency is 8.
  These are example defaults, not a production capacity recommendation.

## Validation evidence

Evidence below applies to the uncommitted Issue #9955 worktree based on
`3b7cad90d53b2bf134dbf7289056fd19c978b372` and was captured on 2026-09-01 from a Windows local checkout.
Commands marked with a relative working directory are run from the repository root unless the directory is shown
as the command's current directory. The local environment had no reachable RocketMQ test cluster and did not
provide the four `ROCKETMQ_MCP_E2E_*` variables.

| Command | Working directory | Feature set | Result | Environment | Limitation |
| --- | --- | --- | --- | --- | --- |
| `cargo fmt --all -- --check` | `rocketmq-ai/rocketmq-mcp` | Cargo defaults | **failed (exit 1)** | Windows local checkout | Cargo/rustfmt reported `文件名或扩展名太长。 (os error 206)` and printed usage before formatting; no source files were changed by this command. |
| `cargo fmt -p rocketmq-mcp -- --check` | `rocketmq-ai/rocketmq-mcp` | Package `rocketmq-mcp` only | **passed (exit 0)** | Issue #9955 worktree on Windows | This narrower package-scoped check does not replace the failed mandatory all-package check; it validates every Rust file in the standalone MCP package. |
| `cargo check --locked` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0)** | Windows local checkout | No live RocketMQ connection was attempted. |
| `cargo check --locked --all-features` | `rocketmq-ai/rocketmq-mcp` | All declared features | **passed (exit 0)** | Windows local checkout | Validates the 29-Tool feature combination without a live cluster. |
| `python scripts/check_read_only_boundary.py` | `rocketmq-ai/rocketmq-mcp` | Metadata/dependency closure | **passed (exit 0)** | Windows local checkout | Printed `MCP read-only dependency boundary passed`; this checks the dependency/source boundary, not a live cluster. |
| `cargo test --locked` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0): 268 passed, 0 failed, 1 ignored; doc-tests 0** | Windows local checkout; `RUST_MIN_STACK` and `INSTA_UPDATE` unset | 259 library + 5 binary + 1 compatibility integration + 3 non-E2E integration tests passed. The external-cluster test remained ignored because it requires `ROCKETMQ_MCP_E2E_NAMESRV_ADDR`, `ROCKETMQ_MCP_E2E_TOPIC`, `ROCKETMQ_MCP_E2E_CONSUMER_GROUP`, and `ROCKETMQ_MCP_E2E_BROKER`. |
| `cargo test --locked --all-features` | `rocketmq-ai/rocketmq-mcp` | All declared features, including `streamable-http`, `otlp`, and `change-planning` | **passed (exit 0): 313 passed, 0 failed, 1 ignored; doc-tests 0** | Windows local checkout; `RUST_MIN_STACK` and `INSTA_UPDATE` unset | 304 library + 5 binary + 1 compatibility integration + 3 non-E2E integration tests passed. The same external-cluster test was ignored; this is not live-cluster evidence. |
| `cargo clippy --locked --all-targets --features streamable-http -- -D warnings` | `rocketmq-ai/rocketmq-mcp` | `streamable-http` plus default features | **passed (exit 0)** | Windows local checkout | No warning was accepted. |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | `rocketmq-ai/rocketmq-mcp` | All declared features | **passed (exit 0)** | Windows local checkout | No warning was accepted. |
| `cargo doc --locked --no-deps` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0)** | Windows local checkout | Documentation generated successfully; generated docs do not exercise a cluster. |
| `git diff --check` | repository root | Not applicable | **passed (exit 0)** | Windows local checkout | Checks whitespace/error markers only; it does not validate protocol behavior or production deployment. |
| `cargo test --locked --test integration external_cluster_exercises_mvp_tools_and_resources -- --ignored` | `rocketmq-ai/rocketmq-mcp` | **Default features only**: `read-only`, `diagnose`, `stdio` | **not run** | No reachable RocketMQ cluster; required `ROCKETMQ_MCP_E2E_*` variables absent | This exact command uses Cargo defaults; it is not an all-features run. The same test is also reported ignored by the separate all-features suite above; neither result certifies a production deployment. |

The failed formatter check is retained as evidence rather than hidden. The package-scoped
`cargo fmt -p rocketmq-mcp -- --check` passed (exit 0), but it does not replace the failed mandatory all-package
command. The passed checks establish only local build, boundary, test, lint, documentation, and whitespace
properties for this uncommitted Issue #9955 worktree. Production qualification remains environment-dependent and
must be rerun with the external dependencies and test principal listed above.
