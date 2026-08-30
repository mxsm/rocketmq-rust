# RocketMQ MCP implementation baseline

This document freezes the checked-in MCP surface and records the validation evidence for the current revision.
It describes implemented behavior; it does not certify a live RocketMQ deployment.

## Revision and sources

| Item | Value |
| --- | --- |
| Revision | `a69f712b0e5acc82469aee0d1a7bc57ba9e62c8c` |
| Revision subject | `[ISSUE #9883]♻️Consolidate AI projects under rocketmq-ai (#9884)` |
| Baseline captured | 2026-08-31, Windows local checkout |
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

### Default features: eight tools

These eight tools are present in the default catalog (`read-only`, `diagnose`, and `stdio`). The `diagnose` feature
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

Page `limit` is bounded to 1–200 and defaults to 50. The outer output policy additionally caps arrays at 1,000
rows and structured output at 1 MiB.

Consumer-lag diagnosis uses a server-owned policy profile and threshold. The request has no historical `time_range`
or caller-controlled threshold because the checked-in implementation has no historical metrics source.

### Optional `change-planning`: five tools

When compiled with `change-planning`, the all-features catalog has exactly thirteen tools: the eight defaults above
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
The ten cluster Resource forms below are the five listed roots plus the five parameterized forms accepted by
`resources/read`; parameterized forms are also exposed through the five templates below.

### Ten cluster Resource forms

| Form | Discovery/read behavior |
| --- | --- |
| `rocketmq://clusters/{cluster}/capabilities` | Listed cluster root; versioned capability and schema-digest manifest. |
| `rocketmq://clusters/{cluster}/overview` | Listed cluster root; cluster overview. |
| `rocketmq://clusters/{cluster}/topics` | Listed cluster root; topic inventory. |
| `rocketmq://clusters/{cluster}/topics/{topic}` | Parameterized topic details. |
| `rocketmq://clusters/{cluster}/topics/{topic}/route` | Parameterized topic route. |
| `rocketmq://clusters/{cluster}/brokers` | Listed cluster root; broker inventory. |
| `rocketmq://clusters/{cluster}/brokers/{broker}` | Parameterized broker details. |
| `rocketmq://clusters/{cluster}/consumer-groups` | Listed cluster root; consumer-group inventory. |
| `rocketmq://clusters/{cluster}/consumer-groups/{group}` | Parameterized consumer-group details. |
| `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag?topic={topic}` | Parameterized consumer lag for one group/topic. |

Names in URI path and query components use UTF-8 percent encoding. This includes retry topics/groups such as
`%RETRY%...`. Unknown, incomplete, invalidly encoded, or unauthorized resources fail closed rather than returning
a placeholder payload.

### Two protected system Resources

These are never cluster-scoped and require diagnostic authorization (`rocketmq:diagnose`) and a valid diagnostic
role:

- `rocketmq://system/runtime/v1` — bounded, sanitized MCP runtime diagnostics.
- `rocketmq://system/observability/v1` — sanitized MCP observability status.

### Five Resource templates

`resources/templates/list` publishes exactly these templates:

1. `rocketmq://clusters/{cluster}/topics/{topic}` (`rocketmq_topic`)
2. `rocketmq://clusters/{cluster}/topics/{topic}/route` (`rocketmq_topic_route`)
3. `rocketmq://clusters/{cluster}/consumer-groups/{group}` (`rocketmq_consumer_group`)
4. `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic}` (`rocketmq_consumer_lag`)
5. `rocketmq://clusters/{cluster}/brokers/{broker}` (`rocketmq_broker`)

### Two Prompts

`prompts/list` advertises exactly:

- `diagnose_consumer_lag` — guided consumer-lag investigation with `cluster`, `topic`, and `consumer_group`.
- `broker_health_check` — guided broker-health review with `cluster`, optional `broker_name`, and optional
  `check_level` (`quick`, `standard`, or `deep`).

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
  row, byte, and stable-error policies. Read-only tool results include a ResourceLink for the corresponding live
  Resource.
- Audit records use schema version 1, redact control characters and sensitive assignments, bound variable-length
  fields, and use a bounded non-blocking queue. Shutdown closes admission, drains accepted records FIFO, flushes the
  sink, and reports drops, sink/flush failures, or pending records/bytes.
- The default example sets cache enabled with 256 entries and TTLs of 3,000 ms (overview), 5,000 ms (topic),
  2,000 ms (broker), and 1,000 ms (consumer lag); security rate limit is 60 per minute and cluster concurrency is 8.
  These are example defaults, not a production capacity recommendation.

## Validation evidence

Evidence below applies exactly to revision `a69f712b0e5acc82469aee0d1a7bc57ba9e62c8c` and was captured on
2026-08-31 from a Windows local checkout. Commands marked with a relative working directory are run from the
repository root unless the directory is shown as the command's current directory. The local environment had no
reachable RocketMQ test cluster and did not provide the four `ROCKETMQ_MCP_E2E_*` variables.

| Command | Working directory | Feature set | Result | Environment | Limitation |
| --- | --- | --- | --- | --- | --- |
| `cargo fmt --all -- --check` | `rocketmq-ai/rocketmq-mcp` | Cargo defaults | **failed (exit 1)** | Windows local checkout | Cargo/rustfmt reported `文件名或扩展名太长。 (os error 206)` and printed usage before formatting; no source files were changed by this command. |
| `cargo fmt -p rocketmq-mcp -- --check` | `rocketmq-ai/rocketmq-mcp` | Package `rocketmq-mcp` only | **passed (exit 0)** | Clean `main` checkout and current documentation worktree on Windows | This narrower package-scoped check does not replace the failed mandatory all-package check; it demonstrates the failure is outside this package's own formatting scope. |
| `cargo check --locked` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0)** | Windows local checkout | Completed with five existing `dead_code` warnings from the path dependency `rocketmq-transport`; no live RocketMQ connection was attempted. |
| `python scripts/check_read_only_boundary.py` | `rocketmq-ai/rocketmq-mcp` | Metadata/dependency closure | **passed (exit 0)** | Windows local checkout | Printed `MCP read-only dependency boundary passed`; this checks the dependency/source boundary, not a live cluster. |
| `cargo test --locked` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0): 117 passed, 0 failed, 1 ignored; doc-tests 0** | Windows local checkout | 108 library + 5 binary + 1 compatibility integration + 3 non-E2E integration tests passed. The external-cluster test remained ignored because it requires `ROCKETMQ_MCP_E2E_NAMESRV_ADDR`, `ROCKETMQ_MCP_E2E_TOPIC`, `ROCKETMQ_MCP_E2E_CONSUMER_GROUP`, and `ROCKETMQ_MCP_E2E_BROKER`. |
| `cargo test --locked --all-features` | `rocketmq-ai/rocketmq-mcp` | All declared features, including `streamable-http`, `otlp`, and `change-planning` | **passed (exit 0): 141 passed, 0 failed, 1 ignored; doc-tests 0** | Windows local checkout | 132 library + 5 binary + 1 compatibility integration + 3 non-E2E integration tests passed. The same external-cluster test was ignored; this is not live-cluster evidence. |
| `cargo clippy --locked --all-targets --features streamable-http -- -D warnings` | `rocketmq-ai/rocketmq-mcp` | `streamable-http` plus default features | **passed (exit 0)** | Windows local checkout | Completed with the same five existing `dead_code` warnings from `rocketmq-transport`; no MCP Clippy warning failed the command. |
| `cargo doc --locked --no-deps` | `rocketmq-ai/rocketmq-mcp` | Default: `read-only`, `diagnose`, `stdio` | **passed (exit 0)** | Windows local checkout | Documentation generated successfully with the same five path-dependency warnings; generated docs do not exercise a cluster. |
| `git diff --check` | repository root | Not applicable | **passed (exit 0)** | Windows local checkout | Checks whitespace/error markers only; it does not validate protocol behavior or production deployment. |
| `cargo test --locked --test integration external_cluster_exercises_mvp_tools_and_resources -- --ignored` | `rocketmq-ai/rocketmq-mcp` | **Default features only**: `read-only`, `diagnose`, `stdio` | **not run** | No reachable RocketMQ cluster; required `ROCKETMQ_MCP_E2E_*` variables absent | This exact command uses Cargo defaults; it is not an all-features run. The same test is also reported ignored by the separate all-features suite above; neither result certifies a production deployment. |

The failed formatter check is retained as evidence rather than hidden. On the clean `main` checkout at this
revision, `cargo fmt --all -- --check` failed identically with `文件名或扩展名太长。 (os error 206)`, while the
package-scoped `cargo fmt -p rocketmq-mcp -- --check` passed (exit 0). The mandatory all-package formatter command
therefore remains failed; the package-scoped result is diagnostic evidence, not a replacement for that gate. The
passed checks establish only local build, boundary, test, lint, documentation, and whitespace properties for the
named revision. Production
qualification remains environment-dependent and must be rerun with the external dependencies and test principal
listed above.
