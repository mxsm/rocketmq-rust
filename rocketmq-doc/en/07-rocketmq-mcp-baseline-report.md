---
title: "RocketMQ MCP Baseline Report"
permalink: /docs/rocketmq-mcp-baseline-report/
excerpt: "Reproducible P0 baseline evidence for the RocketMQ MCP refactor."
last_modified_at: 2026-07-10T00:00:00+08:00
toc: true
classes: wide
---

# RocketMQ MCP Baseline Report

## Scope and Method

This P0 baseline is anchored to base commit `c79d7b0e`. It records source-level
and test-level evidence so later refactor work can compare a known starting
point without implying that the current implementation is the target public
contract. Static call-graph evidence is explicitly distinguished from runtime
measurement; this report does not fabricate live-cluster latency or counters.

## Current MCP Surface

At `c79d7b0e`, the default feature surface has 8 Tools, 4 Resources, 0
Resource templates, and 2 Prompts.

| Kind | Current entries |
| --- | --- |
| Tools | `mq_cluster_overview`, `mq_list_topics`, `mq_describe_topic`, `mq_query_topic_route`, `mq_list_consumer_groups`, `mq_query_consumer_lag`, `mq_describe_broker`, `mq_diagnose_consumer_lag` |
| Resources | `rocketmq://cluster/overview`, `rocketmq://topics`, `rocketmq://brokers`, `rocketmq://consumer-groups` |
| Resource templates | None |
| Prompts | `diagnose_consumer_lag`, `broker_health_check` |

The optional `dangerous-tools` feature exposes 5 additional planning entries:
`mq_create_topic`, `mq_update_topic_config`, `mq_update_topic_perm`,
`mq_update_broker_config`, and `mq_reset_consumer_offset`.

The current stdio integration request in
`rocketmq-tools/rocketmq-mcp/tests/integration.rs` requests MCP `2025-06-18`.
P1 replaces that integration with MCP `2025-11-25`; no dual-version promise is
made.

## Existing Snapshot Coverage Before P0

Before this PR, the Tool contract snapshots in
`rocketmq-tools/rocketmq-mcp/src/tools/snapshots/` covered the Tool name,
input schema type and required-field summary, output schema type, and the four
annotation hints. They did not freeze complete input or output schemas.

Before this PR, the protocol surface snapshots in
`rocketmq-tools/rocketmq-mcp/src/protocol/snapshots/` covered abbreviated Tool
schema presence and two annotations, Resource URI/name/MIME type, an empty
resource-template list, and Prompt name plus argument count. They did not
freeze complete Tool schemas and annotations, complete Resource descriptors,
Resource templates, or Prompt descriptors and arguments.

P0 expands both existing snapshot projections to serialize the complete MCP
descriptors. The default and `dangerous-tools` snapshot files remain separate,
so optional surface changes cannot silently escape the default contract test.

## Static Admin Lifecycle Call Graph

The following counts are source-call-graph counts, not measured runtime
counters or latency measurements. Each named `rocketmq-admin-core` entry point
constructs, starts, and shuts down an admin client for that operation.

### Cluster Overview: Three Complete Lifecycles

`AdminCoreAdapter::cluster_overview` in
`rocketmq-tools/rocketmq-mcp/src/adapter/admin_core_adapter.rs` invokes these
three operations:

1. `ClusterService::query_cluster_list_by_request_with_rpc_hook`, whose
   lifecycle is implemented in
   `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/cluster.rs`.
2. `TopicService::query_topic_list`, whose lifecycle is implemented in
   `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/topic/operations.rs`.
3. `ConsumerService::query_consumer_progress_by_request_with_rpc_hook`, whose
   lifecycle is implemented in
   `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/consumer.rs`.

Therefore one successful current cluster-overview call opens three complete
admin lifecycles.

### Normal No-Fallback High-Lag Diagnosis: Four Complete Lifecycles

`diagnose_consumer_lag` in
`rocketmq-tools/rocketmq-mcp/src/service/diagnosis_service.rs` runs the
following adapter operations in sequence. In the normal no-fallback path, a
high-lag result selects a top-lag broker and that broker is present in the
cluster listing, so all four occur:

1. `query_consumer_lag`, which calls
   `ConsumerService::query_consumer_progress_by_request_with_rpc_hook` in
   `rocketmq-tools/rocketmq-mcp/src/adapter/admin_core_adapter.rs` and
   `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/consumer.rs`.
2. `describe_topic`, which delegates to `query_topic_route`, then calls
   `TopicService::query_topic_route` in
   `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/topic/operations.rs`.
3. `query_topic_route`, which makes a second independent
   `TopicService::query_topic_route` lifecycle through the same source paths.
4. `describe_broker`, which calls
   `ClusterService::query_cluster_list_by_request_with_rpc_hook` through
   `rocketmq-tools/rocketmq-mcp/src/adapter/admin_core_adapter.rs` and
   `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/cluster.rs`.

Thus the normal no-fallback high-lag diagnosis path statically opens four
complete admin lifecycles.

If `describe_broker` does not find the selected broker in the cluster listing,
`AdminCoreAdapter::describe_broker` additionally calls
`BrokerService::query_broker_runtime_stats_by_request_with_rpc_hook` in
`rocketmq-tools/rocketmq-mcp/src/adapter/admin_core_adapter.rs` and
`rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/broker/operations.rs`.
That fallback opens a fifth complete admin lifecycle. `diagnose_consumer_lag`
still returns its report with partial/error broker evidence because the
adapter error is recorded as diagnosis evidence rather than propagated as the
workflow result.

The existing `ReadOnlyAdminAdapter` fake seam in `admin_core_adapter.rs`
supports deterministic workflow tests, but P0 adds no counter seam and no
production lifecycle refactor.

## Observed Baseline Validation

At the baseline, the following results were observed:

| Command | Result |
| --- | --- |
| `cargo check -p rocketmq-mcp` | Passed |
| `cargo test -p rocketmq-mcp` | Passed: 40 unit tests and 1 stdio integration test |

These results establish build and test health only. They do not measure a live
RocketMQ cluster, connection setup, request latency, throughput, or lifecycle
counter values.

## Measurable Exit Targets

### P1 Contract Replacement

P1 is complete only when the following evidence is updated together:

| Target | Evidence and command |
| --- | --- |
| MCP target version | The stdio integration initializes with `2025-11-25`; run `cargo test -p rocketmq-mcp --test integration`. |
| Public Tool and resource contract | Default and `change-planning` contract snapshots contain only the frozen names and cluster-scoped `rocketmq://clusters/{cluster}/...` URIs; run `cargo test -p rocketmq-mcp snapshot` and `cargo test -p rocketmq-mcp --features change-planning snapshot`. |
| Clean break | Source review and focused `rg` evidence show no legacy Tool, URI, schema, configuration, or feature aliases in the P1 module tree. |
| Module layout | Source review confirms Rust 2021+ modules have no `mod.rs` under `rocketmq-tools/rocketmq-mcp/src`. |

### P2 Query and Lifecycle Replacement

P2 is complete only when deterministic tests at the injected admin-session
seam prove the following targets for a selected cluster, including the
session-start and session-shutdown counters:

| Target | Evidence and command |
| --- | --- |
| Shared query path | Tool, Resource, and Diagnosis tests execute through `QueryFacade`, not duplicate adapter composition. |
| Lifecycle ownership | A cluster overview and each high-lag diagnosis path, including the selected-broker-missing fallback, each start and shut down exactly one workflow-scoped `AdminSession`; no global pool is introduced. |
| Diagnosis provenance | Output tests assert explicit Evidence and Rules version fields. |

The P2 evidence command must run the focused counter tests, for example
`cargo test -p rocketmq-mcp query_facade` and
`cargo test -p rocketmq-mcp admin_session`, followed by the full
`cargo test -p rocketmq-mcp`. The baseline report should then record the test
names, counter values, and source paths. It must still avoid claiming
live-cluster latency unless a separately documented controlled measurement
produces it.

### P2 Recorded Evidence

P2 replaces the static P0 lifecycle estimate with deterministic tests at the
injected `AdminSessionFactory` boundary. The implementation is in
`rocketmq-tools/rocketmq-mcp/src/adapter/admin_session.rs` and
`rocketmq-tools/rocketmq-mcp/src/adapter/query_facade.rs`. Tool dispatch uses
the `ReadOnlyQuery` contract in
`rocketmq-tools/rocketmq-mcp/src/tools/executor.rs`; Resource reads use the
same contract in `rocketmq-tools/rocketmq-mcp/src/resources/reader.rs`.

| Test | Start | Shutdown | Additional counters |
| --- | ---: | ---: | --- |
| `query_facade_cluster_overview_starts_and_shuts_down_one_admin_session` | 1 | 1 | broker 1, topic 1, consumer-group 1 |
| `query_facade_high_lag_diagnosis_reuses_one_session_and_one_route_query` | 1 | 1 | lag 1, route 1, broker 1, runtime fallback 0 |
| `query_facade_missing_selected_broker_fallback_reuses_the_workflow_session` | 1 | 1 | lag 1, route 1, broker 1, runtime fallback 1 |
| `query_facade_resource_read_uses_one_session_and_live_query_data` | 1 | 1 | topic 1 |
| `query_facade_backend_failure_shuts_down_the_started_session_once` | 1 | 1 | failed topic query 1 |
| `query_facade_timeout_shuts_down_the_started_session_once` | 1 | 1 | timed-out operation is dropped before awaited shutdown |
| `query_facade_cancellation_shuts_down_the_started_session_once` | 1 | 1 | MCP cancellation token wins before awaited shutdown |

The diagnosis tests also assert
`rocketmq-mcp.evidence.consumer-lag.v1` and
`rocketmq-mcp.rules.consumer-lag.v1`. The route counter remains 1 because the
topic description is derived from the same normalized route read model.

Recorded validation:

```bash
cargo test -p rocketmq-mcp query_facade
cargo test -p rocketmq-mcp admin_session
cargo test -p rocketmq-mcp --lib
cargo test -p rocketmq-mcp --features change-planning --lib
cargo test -p rocketmq-mcp --all-features
cargo test -p rocketmq-admin-core --lib
cargo clippy -p rocketmq-mcp --all-targets --all-features -- -D warnings
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
.\scripts\runtime-audit.ps1 -SkipBaseline
```

These are deterministic local lifecycle and contract results. They do not
claim live-cluster connection latency, request latency, or throughput.

### P3 Resource and Cache Replacement

P3 replaces placeholder Resource discovery with the final cluster-scoped URI
surface and adds a shared bounded TTL cache inside `QueryFacade`. The cache is
configured by `cache.enabled`, `cache.max_entries`, and the query-family TTL
fields. Its normalized key includes schema version, visibility class, query
kind, resolved cluster, filter, page limit, and cursor where applicable.

| Target | Recorded deterministic evidence |
| --- | --- |
| Tool/Resource reuse | `query_facade_reuses_cached_results_across_tool_and_resource_queries`: first Tool result is `miss`, Resource replay is `hit`, start 1, shutdown 1, topic query 1. |
| Singleflight | `query_facade_singleflight_coalesces_concurrent_identical_misses`: 8 concurrent calls produce 1 miss, 7 hits, 7 coalesced waiters, start 1, shutdown 1, topic query 1. |
| Visibility isolation | `query_facade_cache_isolates_visibility_classes`: two visibility classes produce two misses and two independent admin sessions. |
| Cache bounds | Cache unit tests cover hit, miss, expiry, FIFO eviction, explicit invalidation, disabled bypass, failed-load retry, and concurrent singleflight. |
| URI contract | URI and reader tests cover all four roots and five parameterized forms, reject legacy/incomplete forms, and map unknown live entities to Resource Not Found. |
| Discovery pagination | `registry_cursor_resumes_resource_discovery` returns 50 descriptors and resumes the remaining two with the opaque cursor; invalid cursors are rejected. |

The default and `change-planning` protocol snapshots now contain five Resource
templates. Read-only Tool results include a ResourceLink, and Resource payloads
include `observed_at`, `freshness_ms`, and `cache_status`. Cache counters are
emitted through trace-level events after Tool and Resource requests.

Recorded validation:

```bash
cargo test -p rocketmq-mcp
cargo test -p rocketmq-mcp --all-features
cargo clippy -p rocketmq-mcp --all-targets --all-features -- -D warnings
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
cargo doc -p rocketmq-mcp --no-deps --all-features
```

These tests use an injected session factory and do not claim live-cluster
latency or throughput. Production cluster integration remains a P6 release
gate.
