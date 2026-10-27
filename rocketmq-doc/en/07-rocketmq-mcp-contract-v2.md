---
title: "RocketMQ MCP v2 Contract"
permalink: /docs/rocketmq-mcp-contract-v2/
excerpt: "Frozen MCP 2025-11-25 Tool, Resource, pagination, response, and error contract for rocketmq-mcp."
last_modified_at: 2026-07-10T00:00:00+08:00
toc: true
classes: wide
---

# RocketMQ MCP v2 Contract

## Protocol Baseline

`rocketmq-mcp` supports MCP protocol revision `2025-11-25`. Initialization
requests for another revision are rejected with a JSON-RPC invalid-parameters
error that lists the supported revision. The server does not expose a
dual-version compatibility path.

Every Tool descriptor includes complete input and output JSON Schemas,
annotations, and `taskSupport: forbidden`. Input validation, business failures, and source
unavailability are Tool execution errors so a model can correct arguments
and retry. Unknown Tool names and malformed JSON-RPC requests remain protocol
errors.

## Tool Catalog

The default build exposes these read-only or diagnostic Tools:

| Tool | Risk | Pagination |
| --- | --- | --- |
| `rocketmq_get_cluster_overview` | Read only | Not applicable |
| `rocketmq_list_topics` | Read only | `filter`, `limit`, `cursor` |
| `rocketmq_describe_topic` | Read only | Queue `limit` and `cursor` |
| `rocketmq_get_topic_route` | Read only | Queue `limit` and `cursor` |
| `rocketmq_list_consumer_groups` | Read only | `filter`, `limit`, `cursor` |
| `rocketmq_get_consumer_lag` | Read only | Queue `limit` and `cursor` |
| `rocketmq_describe_broker` | Read only | Not applicable |
| `rocketmq_diagnose_consumer_lag` | Diagnose | Not applicable |
| `rocketmq_diagnose_consumer_group` | Diagnose | Not applicable |
| `rocketmq_diagnose_broker_health` | Diagnose | Not applicable |
| `rocketmq_get_cluster_health` | Diagnose | Not applicable |
| `rocketmq_diagnose_message_delivery` | Diagnose | Not applicable |

The Tool Reference of the query MCP (`rocketmq-ai/rocketmq-mcp-query/docs/tool-reference.md`)
lists the complete default catalog with every argument and output.

The optional `change-planning` feature adds these non-mutating Tools. The
feature and its Tools are deprecated and will be removed; a change is planned
with a dry run of the control MCP server instead:

- `rocketmq_plan_create_topic`
- `rocketmq_plan_update_topic_config`
- `rocketmq_plan_update_topic_permissions`
- `rocketmq_plan_update_broker_config`
- `rocketmq_plan_reset_consumer_offset`

Planning Tool input contains a cluster, reason, and desired state. It does not
contain an Apply mode, caller-supplied operator identity, or confirmation
credential. The returned plan has `mutates_cluster: false`; no RocketMQ
mutation API is reachable from this contract.

Tool names, risk levels, schemas, annotations, discovery, and dispatch IDs are
owned by one typed Catalog. Prompts, permissions, documentation, and snapshots
use the same canonical names without aliases.

## Success Envelope

Successful Tool calls return `structuredContent` matching this envelope:

```json
{
  "schema_version": "rocketmq-mcp.v2",
  "request_id": "9",
  "cluster": "prod-a",
  "observed_at": "2026-07-10T15:30:00.000Z",
  "freshness_ms": 0,
  "cache_status": "miss",
  "partial": false,
  "warnings": [],
  "data": {}
}
```

`observed_at` is RFC 3339. `freshness_ms` is zero for a newly loaded value and
increases for a cache hit. `cache_status` is `miss`, `hit`, or `bypass`.
`partial: true` requires at least one warning describing the unavailable
source. A text summary, serialized JSON text block, and a ResourceLink for the
corresponding replayable read model accompany `structuredContent`.

A Tool that is not bound to one cluster, `rocketmq_list_clusters`, answers in
the same envelope without `cluster` and without `source_failures`, and has no
ResourceLink.

Before a response is returned, it is validated against the Tool output schema
and limited to 1 MiB. The default output policy removes NameServer addresses,
broker addresses, broker address maps, and client IP addresses. A later
principal-aware topology scope may explicitly authorize those fields.

## Pagination

Bounded inputs use these fields:

```json
{
  "filter": "order",
  "limit": 50,
  "cursor": "opaque-cursor"
}
```

`limit` defaults to 50 and cannot exceed 200. Cursors are versioned and must be
treated as opaque. An invalid, stale, or out-of-range cursor is an actionable
Tool execution error.

Paged data contains:

```json
{
  "items": [],
  "count": 0,
  "total_count": 0,
  "has_more": false,
  "next_cursor": null
}
```

## Tool Execution Errors

Correctable Tool failures set `isError: true`, omit success
`structuredContent`, and return a JSON text block with this stable shape:

```json
{
  "schema_version": "rocketmq-mcp.v2",
  "request_id": "9",
  "tool": "rocketmq_get_cluster_overview",
  "code": "invalid_arguments",
  "retryable": false,
  "message": "invalid arguments: cluster is required",
  "suggestions": [
    "Correct the arguments using the Tool input schema and retry."
  ]
}
```

Stable codes include `invalid_arguments`, `not_found`, `source_unavailable`,
`permission_denied`, `rate_limited`, `concurrency_limited`,
`change_planning_disabled`, `internal_error`, `output_too_large`,
`backend_timeout`, `cancelled`, `cursor_expired`, `cursor_invalid`, and
`result_too_large`. A `rate_limited` error also carries `retry_after_ms`, the
wait after which one call is due; `concurrency_limited` is a cluster that
already serves as many requests as it may at once and names no wait.
A `not_found` error also carries `entity`, the kind of target that does not
exist. An `invalid_arguments` error carries `violations` when the server can
locate the arguments: up to three `path` and `constraint` pairs that name the
argument and the kind of rule it broke, never the value that was sent. The three paging codes call for different recoveries: request the first
page again, resend the cursor unchanged, or narrow the request. The Tool
Reference lists every code.

## Query Lifecycle and Diagnosis Provenance

Tools, Resources, and Diagnosis use the same `QueryFacade`. A simple query
owns one workflow-scoped `AdminSession`; composite cluster overview and
diagnosis workflows reuse that session for all admin-core calls.
The session is explicitly shut down and awaited after success, failure,
timeout, or MCP request cancellation. There is no process-global admin-client
pool.

`McpApp` owns one shared `QueryFacade` and bounded in-memory TTL cache. Cache
keys contain the schema version, visibility class, query kind, resolved
cluster, and normalized parameters. Errors are not cached. Identical
concurrent misses are coalesced by singleflight and start one `AdminSession`.
Cache disablement or a zero TTL bypasses storage without changing query
correctness. Trace-level cache metrics report cumulative hit, miss, bypass,
eviction, invalidation, and coalesced-waiter counts. Embedders can explicitly
clear all entries through `McpApp::invalidate_cache()`.

Consumer-lag diagnosis results include independent provenance fields:

```json
{
  "evidence_version": "rocketmq-mcp.evidence.consumer-lag.v3",
  "rules_version": "rocketmq-mcp.rules.consumer-lag.v3"
}
```

The consumer-lag diagnosis returns a replayable `evidence_snapshot`, a
server-owned `policy_profile`, `confidence_band`, `partial`,
`missing_evidence`, and `evidence_refs`. The snapshot is the only place the
report holds evidence: every other part names an evidence item by its `id`.
Evidence kind is one of `consumer_lag`, `topic_route`, `broker_summary`,
`consumer_group`, `topic_config`, or `topic_stats`. Evidence status is one of
`present`, `missing`, `unavailable`, `timeout`, `unauthorized`, or `invalid`.
Root causes may reference only `present` evidence, and each carries the stable
`rule_id` of the rule that drew it. A read that fails is missing evidence in a
partial report, never a failed diagnosis. Severity is graded from the size of
the backlog and from the consume delay, against server-owned thresholds that
the configuration can replace for single Topics and Consumer Groups; the
report states the thresholds it used. The Tool schema accepts cluster, topic,
and consumer group only; historical time-range analysis and caller-controlled
thresholds are unavailable until a historical metrics source is introduced.

Version 3 of the evidence and of the rules changed the report shape:
the `evidences` array and the `topic_description` evidence item are gone, the
Broker evidence is named `broker_summary` everywhere, `evidence_snapshot` is
always present, and `root_causes[].rule_id` is new.

Four more diagnosis Tools return the same report shape, each with its own
`report_type` and its own first version of the evidence and of the rules:

| Tool | `report_type` | Version suffix |
| --- | --- | --- |
| `rocketmq_diagnose_consumer_group` | `consumer_group` | `consumer-group.v1` |
| `rocketmq_diagnose_broker_health` | `broker_health` | `broker-health.v1` |
| `rocketmq_get_cluster_health` | `cluster_health` | `cluster-health.v1` |
| `rocketmq_diagnose_message_delivery` | `message_delivery` | `message-delivery.v1` |

They add the evidence kinds `consumer_progress`, `broker_diagnostics`,
`broker_config`, `ha_status`, `broker_inventory`, and `message_metadata`.
The reports of a Broker, of a cluster and of a message delivery are health
reports: every finding has its own severity, and the report is as severe as
its worst finding. No diagnosis result carries a message body or a message
identifier. A message is named by its pseudonyms, and a call that names a
message also needs the permission for `rocketmq_get_message_metadata`.

## Resource URIs

Resources are always scoped to an explicit configured cluster:

- `rocketmq://clusters/{cluster}/overview`
- `rocketmq://clusters/{cluster}/topics`
- `rocketmq://clusters/{cluster}/topics/{topic}`
- `rocketmq://clusters/{cluster}/topics/{topic}/route`
- `rocketmq://clusters/{cluster}/brokers`
- `rocketmq://clusters/{cluster}/brokers/{broker}`
- `rocketmq://clusters/{cluster}/consumer-groups`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}`
- `rocketmq://clusters/{cluster}/consumer-groups/{group}/lag?topic={topic}`

Resource readers execute live queries through `QueryFacade`. Successful
payloads identify `source` as `live`, include `observed_at`, `freshness_ms`, and
`cache_status`, set `partial` to `false`, and contain the same normalized read
models used by Tools. Source unavailability is returned as a Resource error rather
than being represented as an empty inventory.

Resource errors use Resource Not Found for invalid URIs, unknown clusters, and
unknown entities. Other failures retain a non-sensitive machine-readable data
code: `resource_permission_denied`, `resource_rate_limited`,
`resource_query_timeout`, `resource_query_cancelled`, or
`source_unavailable`.

`resources/list` exposes only the four cluster root Resources and uses an
opaque versioned MCP cursor when more than 50 descriptors exist.
`resources/templates/list` publishes RFC 6570 templates for topic, topic
route, consumer group, consumer lag, and broker Resources. Invalid cursors,
legacy URIs, and incomplete parameterized URIs are rejected rather than
silently mapped to another Resource.
Cluster and entity substitutions use UTF-8 percent-encoding, so names such as
`%RETRY%orders` round-trip without producing an invalid URI.

## Verification

The default and `change-planning` descriptors are locked by complete snapshots.
The stdio integration test verifies the protocol revision, discovery surface,
cluster-scoped Resources, Prompt rendering, actionable Tool errors, and stdout
JSON-RPC framing.

```bash
cargo test -p rocketmq-mcp
cargo test -p rocketmq-mcp --features change-planning
cargo clippy --all-targets -p rocketmq-mcp --all-features -- -D warnings
```
