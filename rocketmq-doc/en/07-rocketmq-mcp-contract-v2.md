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
annotations, and `taskSupport: forbidden`. Input validation and business or
backend failures are Tool execution errors so a model can correct arguments
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

The optional `change-planning` feature adds these non-mutating Tools:

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
  "cache_status": "bypass",
  "partial": false,
  "warnings": [],
  "data": {}
}
```

`observed_at` is RFC 3339. `freshness_ms` and `cache_status` become meaningful
when the shared query cache is introduced. `partial: true` requires at least
one warning describing the unavailable source. A text summary and serialized
JSON text block accompany `structuredContent` for client compatibility.

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

Stable codes include `invalid_arguments`, `backend_error`,
`permission_denied`, `rate_limited`, `change_planning_disabled`,
`internal_error`, and `output_too_large`.

## Resource URIs

Resources are always scoped to an explicit configured cluster:

- `rocketmq://clusters/{cluster}/overview`
- `rocketmq://clusters/{cluster}/topics`
- `rocketmq://clusters/{cluster}/brokers`
- `rocketmq://clusters/{cluster}/consumer-groups`

The current inventory readers identify placeholder results with
`partial: true` and a warning. They do not present an unqueried inventory as a
confirmed empty cluster. The QueryFacade stage replaces placeholders with live
queries while preserving these URIs.

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
