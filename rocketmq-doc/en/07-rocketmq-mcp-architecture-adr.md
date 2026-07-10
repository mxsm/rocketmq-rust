---
title: "RocketMQ MCP Architecture ADR"
permalink: /docs/rocketmq-mcp-architecture-adr/
excerpt: "Accepted clean-break architecture and public contract for the RocketMQ MCP server refactor."
last_modified_at: 2026-07-10T00:00:00+08:00
toc: true
classes: wide
---

# RocketMQ MCP Architecture ADR

## Status

Accepted for the unreleased `rocketmq-mcp` refactor.

## Context

The current MCP server is unused and has no compatibility commitment. Its
read-only tools, resources, prompts, configuration, protocol integration, and
admin-client lifecycle are therefore a clean-break migration surface. The
future implementation must be coherent for both stdio and Streamable HTTP,
preserve the deny-by-default safety boundary, and make workflow-level
diagnosis efficient without creating a global connection pool prematurely.

## Decisions

| ADR | Decision |
| --- | --- |
| ADR-001 | Use a single-process modular architecture. The server process owns protocol handling, authorization, query workflows, planning, and evidence generation through explicit module boundaries. |
| ADR-002 | Introduce a workflow-scoped `AdminSession` before considering any global admin-client pool. A workflow creates, owns, shuts down, and awaits its session. |
| ADR-003 | Route Tools, Resources, and Diagnosis through a shared `QueryFacade`; protocol handlers do not each assemble their own admin calls. |
| ADR-004 | Use cluster-scoped resource URIs in the form `rocketmq://clusters/{cluster}/...`. Resources are not implicitly bound to a default cluster. |
| ADR-005 | Derive identity and authorization from `RequestContext`, not Tool arguments, resource URI components, prompt arguments, or process-global mutable state. |
| ADR-006 | Separate Plan from Apply. A Plan is a reviewable, versioned representation of an intended change; Apply is a distinct, explicitly authorized operation. |
| ADR-007 | Version Evidence and Rules independently. Diagnosis output identifies the evidence and rule versions that produced it. |
| ADR-008 | Target MCP `2025-11-25` only. The refactor does not promise a dual-version protocol bridge with MCP `2025-06-18`. |
| ADR-009 | Replace the current implementation with a clean break because it is unused. Do not retain legacy Tool, URI, schema, configuration, or feature aliases. |
| ADR-010 | Keep a bounded, process-local TTL cache inside the shared `QueryFacade`. Keys include schema version, visibility class, query kind, resolved cluster, and normalized parameters; failures are not cached, and identical concurrent misses use singleflight. |

## Frozen Public Contract

The following names are the final public Tool contract for the refactor. They
are intentionally different from the current `mq_*` names and are introduced
without aliases.

### Query and Diagnosis Tools

| Tool |
| --- |
| `rocketmq_get_cluster_overview` |
| `rocketmq_list_topics` |
| `rocketmq_describe_topic` |
| `rocketmq_get_topic_route` |
| `rocketmq_list_consumer_groups` |
| `rocketmq_get_consumer_lag` |
| `rocketmq_describe_broker` |
| `rocketmq_diagnose_consumer_lag` |

### Plan Tools

| Tool |
| --- |
| `rocketmq_plan_create_topic` |
| `rocketmq_plan_update_topic_config` |
| `rocketmq_plan_update_topic_permissions` |
| `rocketmq_plan_update_broker_config` |
| `rocketmq_plan_reset_consumer_offset` |

The planning capability is gated by the `change-planning` feature. Plan Tools
must not apply a change. A later Apply capability requires a separate public
contract and authorization decision.

The refactor uses Rust 2021 or later and, after P1, uses one source file per
module under `rocketmq-tools/rocketmq-mcp/src` without `mod.rs`. No legacy
Tool, URI, schema, configuration, or feature alias is allowed.

## Module Direction

```text
transport and protocol
        -> RequestContext identity and authorization
        -> Tool, Resource, and Prompt handlers
        -> QueryFacade / bounded cache / planning workflows / diagnosis rules
        -> workflow-scoped AdminSession
        -> rocketmq-admin-core
```

`QueryFacade` owns cluster selection, request-scoped query composition,
normalization, and read-model construction. Resources and Tools are different
MCP presentations of the same query results. Diagnosis consumes the same
facade, adds versioned evidence, and evaluates versioned rules.

The cache stores normalized query results, not protocol envelopes. This keeps
request identifiers and output policy request-specific while allowing Tools
and Resources to reuse the same observed data. Visibility class is part of the
key so later principal-aware authorization can isolate results without
duplicating the query implementation.

## Consequences

P0 records the baseline and freezes the destination contract. It deliberately
does not change the production Tool names, resource URIs, protocol version,
feature flags, or admin lifecycle. P1 introduces the clean-break module and
protocol contract. P2 introduces `QueryFacade`, `AdminSession`, and measurable
workflow lifecycle counters. P3 adds live parameterized Resources, bounded TTL
caching, and singleflight without introducing a global admin-client pool.
Subsequent work may add planning and Apply only
through the Plan/Apply boundary defined here.

The migration cost is intentional: existing integrations must move directly to
the new MCP `2025-11-25` contract. The benefit is one stable architecture with
clear authorization ownership, cluster isolation, reusable query workflows,
and auditable diagnosis and change behavior.
