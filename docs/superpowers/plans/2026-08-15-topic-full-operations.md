# Topic Full Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver every Java Dashboard Topic operation in the Rust Web Dashboard `/topics` workspace with truthful broker-backed data, safe confirmations, and the existing dark design system.

**Architecture:** Keep Axum handlers thin, put validation and mutation serialization in `topic_service`, and isolate RocketMQ Topic administration in a focused `DashboardAdminClient` child module that reuses `rocketmq-admin-core::TopicAdmin`. The React page owns catalog/filter/selection state, while focused dialogs own one operation and protect real in-flight work from duplicate submissions and stale presentation updates.

**Tech Stack:** Rust 2024, Axum, Tokio, Serde, `rocketmq-admin-core`; React 19, TypeScript, Vite, Vitest, Testing Library, Radix/shadcn-style shared controls, Lucide icons.

## Global Constraints

- Work only in `D:\Github\Rust\rocketmq-rust\.worktrees\topic-full-operations` on branch `mxsm/topic-full-operations` until final integration.
- Preserve the existing dark operations-console design; do not copy the Java Dashboard's light styling.
- Implement all Java Topic operations: list/filter, create, edit, status, route, consumers, configuration, send, reset, skip, broker delete, and whole-Topic delete.
- System Topics are readable but cannot be created over, edited, sent to, reset, skipped, or deleted through these operations; Retry and DLQ Topics retain the Java Dashboard operations.
- Use existing shared frontend controls and design tokens; add no dependency, gradient, glow, hard-coded white, or internal parity/migration copy.
- Keep valid read data visible when a discovery, mutation, or sibling-tab request fails.
- Never log message bodies, credentials, or complete mutation requests.
- Mutations use a synchronous in-flight guard until the server promise settles; generation invalidation only suppresses stale presentation.
- Mutation results must retain partial target failures and never render an HTTP-200 partial failure as global success.
- Use TDD for every behavior change: RED test, smallest GREEN implementation, focused rerun, then commit.
- Do not touch `rocketmq-dashboard-gpui`, `rocketmq-dashboard-tauri`, dependencies, or unrelated root files.

---

## File structure

### Backend

- `backend/src/model/topic_model.rs` — HTTP Topic catalog, detail, operation request, and result DTOs.
- `backend/src/admin/dashboard_admin_client/topic.rs` — all concrete Topic admin calls, mapping helpers, system protection, and per-target aggregation.
- `backend/src/admin/dashboard_admin_client.rs` — declare the child module, import its extension methods, and remove the old inline Topic methods after their behavior moves.
- `backend/src/service/topic_service.rs` — mutation serialization, create-only guard, trimmed input validation, and calls to the concrete admin client.
- `backend/src/api/topic_api.rs` — thin handlers and broker-name query extraction.
- `backend/src/api/router.rs` — focused Topic operation routes.

### Frontend

- `frontend/src/types/topic.ts` — exact camel-case HTTP contracts.
- `frontend/src/api/topic_api.ts` — URL-encoded Topic API calls.
- `frontend/src/pages/topics/topic-model.ts` — pure metadata filtering, metrics, permissions, and action availability.
- `frontend/src/pages/topics/TopicFilterToolbar.tsx` — text, message-type/category, cluster, and broker filters.
- `frontend/src/components/TopicMutationDialog.tsx` — shared Create/Edit form using discovered targets.
- `frontend/src/pages/topics/TopicDetailContent.tsx` — independent Overview, Route/Status, Consumers, and Configuration resources.
- `frontend/src/components/TopicSendMessageDialog.tsx` — send form and structured result.
- `frontend/src/components/TopicResetOffsetDialog.tsx` — timestamp reset confirmation and result.
- `frontend/src/components/TopicSkipBacklogDialog.tsx` — exact consumer-group confirmation and result.
- `frontend/src/components/TopicDeleteDialog.tsx` — broker-scoped and whole-Topic deletion confirmations.
- `frontend/src/pages/TopicListPage.tsx` — page orchestration, row actions, focused invalidation, and notices.
- `frontend/src/styles/globals.css` — only the new dense Topic layouts required by these components.

---

### Task 1: Backend Topic catalog and read contracts

**Files:**
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/model/topic_model.rs`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/admin/dashboard_admin_client/topic.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/admin/dashboard_admin_client.rs:14-85,303-371`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/service/topic_service.rs:14-45`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/api/topic_api.rs:14-66`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/api/router.rs:40-52`

**Interfaces:**
- Consumes: `rocketmq_admin_core::core::topic::{TopicAdmin, TopicCatalogRequest, GetTopicConfigRequest}`.
- Produces: `TopicListView.targets`, enriched `TopicInfo`, `TopicConfigView`, `TopicConsumersView`, and enriched `TopicStatsInfo` used by all later frontend tasks.

- [ ] **Step 1: Add failing DTO and mapping tests**

Add tests in `topic_model.rs` and the new `dashboard_admin_client/topic.rs` that require the exact catalog fields and preserve queue offsets:

```rust
#[test]
fn topic_catalog_dto_serializes_authoritative_metadata() {
    let topic = TopicInfo {
        topic: "orders".into(),
        broker_name: Some("broker-a".into()),
        brokers: vec!["broker-a".into()],
        clusters: vec!["DefaultCluster".into()],
        read_queue_count: 8,
        write_queue_count: 8,
        perm: 6,
        category: "NORMAL".into(),
        message_type: "NORMAL".into(),
        order: false,
        system_topic: false,
    };
    let json = serde_json::to_value(topic).expect("topic serializes");
    assert_eq!(json["messageType"], "NORMAL");
    assert_eq!(json["brokers"][0], "broker-a");
    assert_eq!(json["systemTopic"], false);
}

#[test]
fn maps_core_stats_without_losing_queue_identity() {
    let view = map_topic_stats(core_topic::TopicStats {
        topic: "orders".into(),
        total_message_count: 9,
        queue_count: 1,
        offsets: vec![core_topic::TopicQueueOffset {
            broker_name: "broker-a".into(),
            queue_id: 2,
            min_offset: 3,
            max_offset: 12,
            last_update_timestamp: 1_700_000_000_000,
        }],
    });
    assert_eq!(view.total_message_count, 9);
    assert_eq!(view.offsets[0].queue_id, 2);
}
```

- [ ] **Step 2: Run the focused tests and capture RED**

Run:

```powershell
cargo test topic --lib
```

Expected: FAIL because the enriched DTO fields, child module, and mapper do not exist.

- [ ] **Step 3: Define the read DTOs**

Extend `topic_model.rs` with these exact shapes while retaining the existing `broker_name`, aggregate offset fields, and camel-case serialization for compatibility:

```rust
pub struct TopicInfo {
    pub topic: String,
    pub broker_name: Option<String>,
    pub brokers: Vec<String>,
    pub clusters: Vec<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: u32,
    pub category: String,
    pub message_type: String,
    pub order: bool,
    pub system_topic: bool,
}

pub struct TopicTargetOptionView {
    pub cluster_name: String,
    pub broker_names: Vec<String>,
}

pub struct TopicListView {
    pub items: Vec<TopicInfo>,
    pub total: usize,
    pub targets: Vec<TopicTargetOptionView>,
}

pub struct TopicConfigView {
    pub topic_name: String,
    pub broker_name: String,
    pub cluster_name: Option<String>,
    pub broker_name_list: Vec<String>,
    pub cluster_name_list: Vec<String>,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub order: bool,
    pub message_type: String,
    pub attributes: BTreeMap<String, String>,
    pub inconsistent_fields: Vec<String>,
}

pub struct TopicConsumerView {
    pub consumer_group: String,
    pub total_diff: i64,
    pub inflight_diff: i64,
    pub consume_tps: f64,
}

pub struct TopicConsumersView {
    pub items: Vec<TopicConsumerView>,
}
```

Add `TopicQueueOffsetView` and extend `TopicStatsInfo` with `total_message_count` and `offsets`, deriving the old aggregate min/max values from the same offsets.

- [ ] **Step 4: Implement the focused Topic admin module**

Declare `mod topic;` after `run_admin_rpc!` in `dashboard_admin_client.rs`. Move the existing six Topic methods into `dashboard_admin_client/topic.rs`, import `TopicAdmin`, and replace the old name-derived list with:

```rust
pub async fn list_topics(&self) -> Result<TopicListView, DashboardError> {
    let catalog = run_admin_rpc!(self, |admin| admin.get_topic_catalog(&TopicCatalogRequest {
        skip_system_topics: false,
        skip_retry_and_dlq_topics: false,
    }))?;
    Ok(map_topic_catalog(catalog))
}
```

Add methods with exact signatures:

```rust
pub async fn topic_config(&self, topic: &str, broker_name: Option<&str>) -> Result<TopicConfigView, DashboardError>;
pub async fn topic_consumers(&self, topic: &str) -> Result<TopicConsumersView, DashboardError>;
```

Use `TopicAdmin::get_topic_stats` for stats, `GetTopicConfigRequest::try_new` for configuration, and `TopicAdmin::get_topic_consumers` for consumers. Map core DTOs field-for-field and sort catalog targets, brokers, clusters, and consumers for deterministic UI output.

Call these concrete methods through `state.admin_client` in `topic_service`; do not expand `rocketmq-dashboard-common::DashboardAdminFacade` for Web-only rich DTOs. Existing common-facade consumers continue calling the same `DashboardAdminClient` compatibility methods after their mechanical move.

- [ ] **Step 5: Add thin service, handler, and route functions**

Add:

```rust
pub async fn topic_config(state: &AppState, topic: &str, broker_name: Option<&str>) -> Result<TopicConfigView, DashboardError>;
pub async fn topic_consumers(state: &AppState, topic: &str) -> Result<TopicConsumersView, DashboardError>;
```

Expose:

```text
GET /api/topics/{topic}/config?brokerName=broker-a
GET /api/topics/{topic}/consumers
```

The handler query type is `TopicConfigQuery { broker_name: Option<String> }`; it passes `as_deref()` and does not duplicate validation.

- [ ] **Step 6: Run focused and backend tests**

Run:

```powershell
cargo fmt --all
cargo test topic --lib
cargo test --all-targets --all-features
```

Expected: all Topic tests and the existing 35-test backend baseline pass.

- [ ] **Step 7: Commit the read contract**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/backend/src
git commit -m "feat(dashboard): expose full topic read model"
```

---

### Task 2: Backend structured create and edit operations

**Files:**
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/model/topic_model.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/admin/dashboard_admin_client/topic.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/service/topic_service.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/api/topic_api.rs`

**Interfaces:**
- Consumes: Task 1 catalog targets and `TopicMutationRequest`.
- Produces: `TopicOperationResult`, `TopicTargetResult`, `resolve_topic_targets`, and structured POST/PUT responses used by the UI.

- [ ] **Step 1: Write failing validation and aggregation tests**

Add exact cases:

```rust
#[test]
fn resolves_clusters_and_brokers_to_unique_canonical_brokers() {
    let targets = vec![TopicTargetOptionView {
        cluster_name: "DefaultCluster".into(),
        broker_names: vec!["broker-a".into(), "broker-b".into()],
    }];
    assert_eq!(
        resolve_topic_targets(&targets, &[" DefaultCluster ".into()], &["broker-a".into()]).unwrap(),
        vec!["broker-a", "broker-b"]
    );
}

#[test]
fn rejects_queue_counts_outside_one_through_128() {
    let error = validate_topic_mutation(&TopicMutationRequest {
        topic: "orders".into(),
        read_queue_count: 129,
        write_queue_count: 8,
        perm: 6,
        broker_name_list: vec!["broker-a".into()],
        cluster_name_list: vec![],
        order: Some(false),
        message_type: Some("NORMAL".into()),
    }).expect_err("invalid queue count");
    assert!(matches!(error, DashboardError::Validation(message) if message.contains("1 and 128")));
}

#[test]
fn partial_target_failure_is_not_global_success() {
    let result = build_operation_result("UPDATE", "orders", vec![
        TopicTargetResult::success("broker-a", "saved"),
        TopicTargetResult::failure("broker-b", "unavailable"),
    ]);
    assert!(!result.success);
    assert_eq!(result.target_count, 2);
}
```

- [ ] **Step 2: Run focused tests and capture RED**

Run:

```powershell
cargo test topic --lib
```

Expected: FAIL because the operation result and validation helpers do not exist.

- [ ] **Step 3: Add exact mutation result DTOs**

```rust
pub struct TopicTargetResult {
    pub target: String,
    pub success: bool,
    pub message: String,
}

pub struct TopicOperationResult {
    pub operation: String,
    pub topic: String,
    pub success: bool,
    pub target_count: usize,
    pub message: String,
    pub targets: Vec<TopicTargetResult>,
}

impl TopicTargetResult {
    fn success(target: impl Into<String>, message: impl Into<String>) -> Self {
        Self { target: target.into(), success: true, message: message.into() }
    }

    fn failure(target: impl Into<String>, message: impl Into<String>) -> Self {
        Self { target: target.into(), success: false, message: message.into() }
    }
}
```

Keep `message` so clients that only read the old mutation message remain source-compatible. `build_operation_result` sets `success = targets.iter().all(|target| target.success)` and creates a neutral summary containing counts, never an unconditional success claim.

- [ ] **Step 4: Validate and canonicalize create/edit input once**

In `topic_service.rs`, trim Topic and every target, reject duplicate create names case-sensitively, enforce queue counts `1..=128`, permission `1..=7` with at least read or write bit, and message type in `NORMAL|FIFO|DELAY|TRANSACTION`. Use the mutation lock for the entire authoritative catalog-check plus mutation sequence.

Change the service signatures to return `TopicOperationResult`:

```rust
pub async fn create_topic(state: &AppState, request: TopicMutationRequest) -> Result<TopicOperationResult, DashboardError>;
pub async fn create_or_update_topic(state: &AppState, request: TopicMutationRequest) -> Result<TopicOperationResult, DashboardError>;
```

- [ ] **Step 5: Execute one admin-core upsert per canonical broker**

In `DashboardAdminClient::create_or_update_topic`, load the catalog within the acquired admin session, resolve cluster selections to broker names, deduplicate and sort targets, then call `TopicAdmin::upsert_topic` with one `broker_names` entry and no cluster names per iteration. Continue after a failed broker and append a `TopicTargetResult` using `stable_error_message(&error)`; do not use `?` inside the per-target loop.

For Edit, call `require_mutable_topic` before the loop and reject an authoritative `system_topic`. For Create, reject a catalog collision before any side effect.

- [ ] **Step 6: Return structured handler responses and rerun tests**

Update POST/PUT handler response generics to `ApiResponse<TopicOperationResult>`.

Run:

```powershell
cargo fmt --all
cargo test topic --lib
cargo test --all-targets --all-features
```

Expected: all tests pass and existing response tests still see a `message` field.

- [ ] **Step 7: Commit create/edit support**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/backend/src
git commit -m "feat(dashboard): add structured topic upserts"
```

---

### Task 3: Backend send, offset, and deletion operations

**Files:**
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/model/topic_model.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/admin/dashboard_admin_client/topic.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/service/topic_service.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/api/topic_api.rs`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/api/router.rs`

**Interfaces:**
- Consumes: Task 2 `TopicOperationResult` and authoritative Topic guard.
- Produces: send/reset/skip/broker-delete/whole-delete endpoints used by Tasks 7 and 8.

- [ ] **Step 1: Write failing operation contract tests**

Add tests for exact send classification, system protection, and safe skip timestamp conversion:

```rust
#[test]
fn send_ok_is_the_only_successful_send_status() {
    assert_eq!(canonical_send_status("SendOk"), "SEND_OK");
    assert_eq!(canonical_send_status("FlushDiskTimeout"), "FLUSH_DISK_TIMEOUT");
    assert!(is_successful_send_status("SEND_OK"));
    assert!(is_successful_send_status("SEND_OK (COMMIT_MESSAGE)"));
    assert!(!is_successful_send_status("FLUSH_DISK_TIMEOUT"));
}

#[test]
fn system_topic_rejects_every_mutating_operation() {
    let topic = TopicInfo {
        topic: "RMQ_SYS_TRACE_TOPIC".into(),
        broker_name: Some("broker-a".into()),
        brokers: vec!["broker-a".into()],
        clusters: vec!["DefaultCluster".into()],
        read_queue_count: 1,
        write_queue_count: 1,
        perm: 6,
        category: "SYSTEM".into(),
        message_type: "SYSTEM".into(),
        order: false,
        system_topic: true,
    };
    for operation in ["EDIT", "SEND", "RESET_OFFSET", "SKIP_BACKLOG", "DELETE_BROKER", "DELETE_TOPIC"] {
        assert!(ensure_topic_operation_allowed(&topic, operation).is_err());
    }
}

#[test]
fn skip_timestamp_uses_current_epoch_millis() {
    let now = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
    assert_eq!(epoch_millis(now).unwrap(), 1_700_000_000_123);
}
```

Also add a serialization test proving the message body is absent from `Debug` output by implementing a manual redacted `Debug` for `TopicTestMessageRequest`.

- [ ] **Step 2: Run focused tests and capture RED**

Run:

```powershell
cargo test topic --lib
```

Expected: FAIL because the requests, guards, and functions do not exist.

- [ ] **Step 3: Add request and response DTOs**

Define:

```rust
pub struct TopicTestMessageRequest {
    pub key: String,
    pub tag: String,
    pub message_body: String,
    pub trace_enabled: bool,
}

pub struct TopicSendResultView {
    pub topic: String,
    pub success: bool,
    pub send_status: String,
    pub message_id: Option<String>,
    pub broker_name: Option<String>,
    pub queue_id: Option<i32>,
    pub queue_offset: u64,
    pub transaction_id: Option<String>,
    pub region_id: Option<String>,
    pub local_transaction_state: Option<String>,
}

pub struct TopicResetOffsetRequest {
    pub consumer_group: String,
    pub reset_timestamp: u64,
    pub force: bool,
}

pub struct TopicSkipOffsetRequest {
    pub consumer_group: String,
}

pub struct TopicOffsetResult {
    pub operation: String,
    pub topic: String,
    pub consumer_group: String,
    pub success: bool,
    pub affected_queue_count: usize,
    pub applied_timestamp: u64,
    pub message: String,
}
```

Implement redacted `Debug` for `TopicTestMessageRequest` that prints `message_body: "[REDACTED]"`.

- [ ] **Step 4: Implement admin methods with authoritative guards**

Add exact methods:

```rust
pub async fn send_topic_test_message(&self, topic: &str, request: TopicTestMessageRequest) -> Result<TopicSendResultView, DashboardError>;
pub async fn reset_topic_consumer_offset(&self, topic: &str, request: TopicResetOffsetRequest) -> Result<TopicOffsetResult, DashboardError>;
pub async fn skip_topic_consumer_offset(&self, topic: &str, request: TopicSkipOffsetRequest) -> Result<TopicOffsetResult, DashboardError>;
pub async fn delete_topic_from_broker(&self, topic: &str, broker_name: &str) -> Result<TopicOperationResult, DashboardError>;
pub async fn delete_topic(&self, topic: &str) -> Result<TopicOperationResult, DashboardError>;
```

Within the same admin session, load the catalog item, reject `system_topic`, and validate the consumer group against `get_topic_consumer_groups`. Reset uses the supplied timestamp. Skip captures `SystemTime::now()`, converts it with `epoch_millis`, and calls `reset_topic_consumer_offset` with `force: true`.

Send trims key/tag, preserves body bytes, and rejects a blank body. Normalize admin-core's Debug-style status prefixes to the wire values `SEND_OK`, `FLUSH_DISK_TIMEOUT`, `FLUSH_SLAVE_TIMEOUT`, and `SLAVE_NOT_AVAILABLE`, preserving any transaction-state suffix. Map `success` only from `SEND_OK`, including that suffix. Retry and DLQ are allowed because only `system_topic` is rejected.

Broker delete verifies the broker is present in the Topic's authoritative broker set. Whole delete runs one `DeleteTopicAdminRequest { cluster_name: Some(cluster), broker_name: None }` per sorted cluster, continues after failures, and returns the structured per-cluster result.

- [ ] **Step 5: Add serialized service methods and routes**

All mutating service methods acquire `topic_mutation_lock`. Expose:

```text
POST   /api/topics/{topic}/test-message
POST   /api/topics/{topic}/consumer-offset/reset
POST   /api/topics/{topic}/consumer-offset/skip
DELETE /api/topics/{topic}/brokers/{broker}
DELETE /api/topics/{topic}
```

Path Topic and Broker are authoritative. The test-message handler constructs the core request with the decoded path Topic; no request body may override it.

- [ ] **Step 6: Run backend validation**

Run:

```powershell
cargo fmt --all
cargo test topic --lib
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: all commands exit zero; no tracing field or error contains message body text.

- [ ] **Step 7: Commit Topic operations**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/backend/src
git commit -m "feat(dashboard): add complete topic operations"
```

---

### Task 4: Frontend Topic contracts, API, and filtering model

**Files:**
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/types/topic.ts`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/api/topic_api.ts`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/topic-model.ts`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/topic-model.test.ts`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/TopicFilterToolbar.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/TopicFilterToolbar.test.tsx`

**Interfaces:**
- Consumes: Tasks 1–3 camel-case HTTP contracts.
- Produces: typed `topicApi`, `TopicFilters`, `TopicFilterToolbar`, `getTopicActionAvailability`, and catalog metadata used by every remaining UI task.

- [ ] **Step 1: Write failing model tests for every Java filter**

```typescript
const topic = (overrides: Partial<TopicInfo>): TopicInfo => ({
  topic: 'orders', brokerName: 'broker-a', brokers: ['broker-a'], clusters: ['DefaultCluster'],
  readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL', messageType: 'NORMAL',
  order: false, systemTopic: false, ...overrides
});
const fifoTopic = topic({ topic: 'fifo-orders', messageType: 'FIFO' });
const unspecifiedTopic = topic({ topic: 'legacy-orders', messageType: 'UNSPECIFIED' });
const retryTopic = topic({ topic: '%RETRY%orders', category: 'RETRY', messageType: 'RETRY' });
const dlqTopic = topic({ topic: '%DLQ%orders', category: 'DLQ', messageType: 'DLQ' });
const systemTopic = topic({ topic: 'RMQ_SYS_TRACE_TOPIC', category: 'SYSTEM', messageType: 'SYSTEM', systemTopic: true });
const fixtures = [fifoTopic, unspecifiedTopic, retryTopic, dlqTopic, systemTopic];

it('filters authoritative message types and operational categories together', () => {
  const result = filterTopics(fixtures, {
    query: '',
    brokerName: 'broker-a',
    clusterName: 'DefaultCluster',
    messageTypes: ['FIFO', 'UNSPECIFIED'],
    categories: ['APPLICATION', 'RETRY']
  });
  expect(result.map((topic) => topic.topic)).toEqual(['fifo-orders', 'legacy-orders', '%RETRY%orders']);
});

it('allows Java operations on retry and dlq but none on system topics', () => {
  expect(getTopicActionAvailability(retryTopic).send).toBe(true);
  expect(getTopicActionAvailability(dlqTopic).skip).toBe(true);
  expect(getTopicActionAvailability(systemTopic)).toEqual({
    edit: false, send: false, reset: false, skip: false, deleteBroker: false, deleteTopic: false
  });
});
```

- [ ] **Step 2: Run the model test and capture RED**

Run:

```powershell
npm test -- --run src/pages/topics/topic-model.test.ts
```

Expected: FAIL because filters still infer categories from names and support only one category/broker.

- [ ] **Step 3: Define exact TypeScript contracts and API calls**

Mirror every Task 1–3 field in `types/topic.ts`. Add API methods:

```typescript
config: (topic: string, brokerName?: string) => apiClient.get<TopicConfigView>(
  `/api/topics/${encodeURIComponent(topic)}/config${brokerName ? `?brokerName=${encodeURIComponent(brokerName)}` : ''}`
),
consumers: (topic: string) => apiClient.get<TopicConsumersView>(`/api/topics/${encodeURIComponent(topic)}/consumers`),
sendTestMessage: (topic: string, request: TopicTestMessageRequest) =>
  apiClient.post<TopicSendResultView>(`/api/topics/${encodeURIComponent(topic)}/test-message`, request),
resetOffset: (topic: string, request: TopicResetOffsetRequest) =>
  apiClient.post<TopicOffsetResult>(`/api/topics/${encodeURIComponent(topic)}/consumer-offset/reset`, request),
skipBacklog: (topic: string, request: TopicSkipOffsetRequest) =>
  apiClient.post<TopicOffsetResult>(`/api/topics/${encodeURIComponent(topic)}/consumer-offset/skip`, request),
deleteFromBroker: (topic: string, broker: string) => apiClient.delete<TopicOperationResult>(
  `/api/topics/${encodeURIComponent(topic)}/brokers/${encodeURIComponent(broker)}`
)
```

Build every path with `encodeURIComponent`, including the optional `brokerName` query value.

- [ ] **Step 4: Replace inferred filtering with catalog metadata**

Use exact unions:

```typescript
export type TopicMessageType = 'NORMAL' | 'DELAY' | 'FIFO' | 'TRANSACTION' | 'UNSPECIFIED';
export type TopicCategory = 'APPLICATION' | 'RETRY' | 'DLQ' | 'SYSTEM';
```

`getTopicCategory` reads `topic.systemTopic` and `topic.category`; it no longer guesses from the Topic name except for a compatibility fallback when fields are absent. Text, broker, and cluster filters combine with AND. Message-type and operational-category selections form one Java-compatible classification union: a row matches when either its message type or category is selected; when both selection arrays are empty, classification is unrestricted.

- [ ] **Step 5: Build the shared filter toolbar**

Use `DropdownMenuCheckboxItem` for multi-select Type and Category menus, shared `Select` for Cluster and Broker, and the existing `QueryToolbar` search/reset affordance. The trigger text shows `All types`, one selected label, or `N types`; every checkbox has an accessible name.

- [ ] **Step 6: Run focused frontend tests**

Run:

```powershell
npm test -- --run src/pages/topics/topic-model.test.ts src/pages/topics/TopicFilterToolbar.test.tsx
```

Expected: all filter combinations, reset, and keyboard selection tests pass.

- [ ] **Step 7: Commit frontend domain support**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/types/topic.ts rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/api/topic_api.ts rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics
git commit -m "feat(dashboard): add topic operation contracts"
```

---

### Task 5: Create and edit Topic dialog

**Files:**
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicMutationDialog.tsx`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicMutationDialog.test.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/test/deferred.ts`

**Interfaces:**
- Consumes: `TopicTargetOption`, `TopicConfigView`, `TopicMutationRequest`, and `TopicOperationResult` from Task 4.
- Produces: a mode-aware dialog used by `TopicListPage` in Task 8.

- [ ] **Step 1: Write failing Create/Edit tests**

Add tests that require target selectors, immutable Edit identity, inconsistent-field messaging, exact permission bits, form preservation, and duplicate-submit locking:

```typescript
const targets: TopicTargetOption[] = [{ clusterName: 'DefaultCluster', brokerNames: ['broker-a', 'broker-b'] }];
const config: TopicConfigView = {
  topicName: 'orders', brokerName: 'broker-a', clusterName: 'DefaultCluster',
  brokerNameList: ['broker-a'], clusterNameList: ['DefaultCluster'], readQueueNums: 8,
  writeQueueNums: 8, perm: 6, order: false, messageType: 'NORMAL', attributes: {},
  inconsistentFields: ['writeQueueNums']
};
const successResult: TopicOperationResult = {
  operation: 'UPDATE', topic: 'orders', success: true, targetCount: 2, message: '2 targets saved',
  targets: [
    { target: 'broker-a', success: true, message: 'saved' },
    { target: 'broker-b', success: true, message: 'saved' }
  ]
};
export const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => { resolve = next; });
  return { promise, resolve };
};

it('loads edit config, locks the topic name, and submits selected canonical brokers', async () => {
  const user = userEvent.setup();
  const onSubmit = vi.fn().mockResolvedValue(successResult);
  render(<TopicMutationDialog open mode="edit" config={config} targets={targets}
    onOpenChange={vi.fn()} onSubmit={onSubmit} />);
  expect(screen.getByRole('textbox', { name: 'Topic name' })).toBeDisabled();
  expect(screen.getByText(/Broker configurations disagree: writeQueueNums/)).toBeInTheDocument();
  await user.click(screen.getByRole('checkbox', { name: 'broker-b' }));
  await user.click(screen.getByRole('button', { name: 'Save topic' }));
  await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Save changes' }));
  expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
    topic: 'orders', brokerNameList: ['broker-a', 'broker-b'], perm: 6
  }));
});

it('keeps the real mutation locked after close and reopen until the promise settles', async () => {
  const user = userEvent.setup();
  const pending = deferred<TopicOperationResult>();
  const onSubmit = vi.fn().mockReturnValue(pending.promise);
  const base = { mode: 'edit' as const, config, targets, onOpenChange: vi.fn(), onSubmit };
  const { rerender } = render(<StrictMode><TopicMutationDialog {...base} open /></StrictMode>);
  await user.click(screen.getByRole('button', { name: 'Save topic' }));
  await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Save changes' }));
  rerender(<StrictMode><TopicMutationDialog {...base} open={false} /></StrictMode>);
  rerender(<StrictMode><TopicMutationDialog {...base} open /></StrictMode>);
  await user.click(screen.getByRole('button', { name: 'Save topic' }));
  await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Save changes' }));
  expect(onSubmit).toHaveBeenCalledTimes(1);
  await act(async () => pending.resolve(successResult));
});
```

Place the exported `deferred` function in `src/test/deferred.ts` and import it in this and later dialog tests; keep the fixture constants in `TopicMutationDialog.test.tsx`.

- [ ] **Step 2: Run the dialog test and capture RED**

Run:

```powershell
npm test -- --run src/components/TopicMutationDialog.test.tsx
```

Expected: FAIL because only Create with comma-separated free text exists.

- [ ] **Step 3: Implement the mode-aware form**

Use props:

```typescript
interface TopicMutationDialogProps {
  open: boolean;
  mode: 'create' | 'edit';
  targets: TopicTargetOption[];
  config?: TopicConfigView | null;
  loadingConfig?: boolean;
  configError?: string | null;
  onRetryConfig?: () => void;
  onOpenChange: (open: boolean) => void;
  onSubmit: (request: TopicMutationRequest) => Promise<TopicOperationResult>;
}
```

Replace comma-separated fields with discovered Cluster and Broker checkbox groups. Selecting a cluster includes its brokers in the confirmation but sends the chosen `clusterNameList` and any independently chosen `brokerNameList`; the backend remains authoritative and deduplicates.

Use Read, Write, and Inherit toggles to build permission bits `4`, `2`, and `1`. Require Read or Write, queue counts `1..=128`, and one target. Keep the existing message-type select and ordered checkbox. Render backend target results inside the dialog; close only when `result.success` is true.

- [ ] **Step 4: Add robust async ownership**

Use a synchronous `pendingRef` to reject a second submit before React state commits. Use `requestRef` plus captured mode/Topic identity to suppress stale results. Set `mountedRef.current = true` at each effect setup so React Strict Mode cleanup/setup does not strand the busy state. Do not clear `pendingRef` until the original promise settles.

- [ ] **Step 5: Run focused tests**

Run:

```powershell
npm test -- --run src/components/TopicMutationDialog.test.tsx
```

Expected: Create, Edit, rejection preservation, partial result, duplicate-submit, stale identity, focus, and Strict Mode cases pass.

- [ ] **Step 6: Commit Create/Edit UI**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicMutationDialog.tsx rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicMutationDialog.test.tsx rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/test/deferred.ts
git commit -m "feat(dashboard): add topic create and edit dialog"
```

---

### Task 6: Topic detail consumers and configuration

**Files:**
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/TopicDetailContent.tsx`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/TopicDetailContent.test.tsx`

**Interfaces:**
- Consumes: Task 4 read APIs and DTOs.
- Produces: four independently loaded detail tabs and callbacks for edit/reset/skip actions.

- [ ] **Step 1: Write failing lazy-resource tests**

```typescript
const topicFixture: TopicInfo = {
  topic: 'orders', brokerName: 'broker-a', brokers: ['broker-a'], clusters: ['DefaultCluster'],
  readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL', messageType: 'NORMAL',
  order: false, systemTopic: false
};
const configFixture: TopicConfigView = {
  topicName: 'orders', brokerName: 'broker-a', clusterName: 'DefaultCluster',
  brokerNameList: ['broker-a'], clusterNameList: ['DefaultCluster'], readQueueNums: 8,
  writeQueueNums: 8, perm: 6, order: false, messageType: 'NORMAL', attributes: {}, inconsistentFields: []
};

it('loads consumers independently and exposes group-scoped reset and skip actions', async () => {
  const user = userEvent.setup();
  const onReset = vi.fn();
  render(<TopicDetailContent topicName="orders" topic={topicFixture} onReset={onReset} onSkip={vi.fn()} />);
  await user.click(screen.getByRole('tab', { name: 'Consumers' }));
  expect(topicApi.consumers).toHaveBeenCalledWith('orders');
  const row = await screen.findByRole('row', { name: /order-service.*120.*4.*8.5/ });
  await user.click(within(row).getByRole('button', { name: 'Reset order-service' }));
  expect(onReset).toHaveBeenCalledWith('order-service');
});

it('keeps route data visible when configuration fails and retries only configuration', async () => {
  const user = userEvent.setup();
  vi.mocked(topicApi.config).mockRejectedValueOnce(new Error('config unavailable')).mockResolvedValueOnce(configFixture);
  render(<TopicDetailContent topicName="orders" topic={topicFixture} />);
  await user.click(screen.getByRole('tab', { name: 'Routes and status' }));
  expect(await screen.findByRole('table', { name: 'Topic routes' })).toBeInTheDocument();
  await user.click(screen.getByRole('tab', { name: 'Configuration' }));
  await user.click(screen.getByRole('button', { name: 'Retry configuration' }));
  await user.click(screen.getByRole('tab', { name: 'Routes and status' }));
  expect(topicApi.route).toHaveBeenCalledTimes(1);
  expect(topicApi.config).toHaveBeenCalledTimes(2);
  expect(screen.getByRole('table', { name: 'Topic routes' })).toBeInTheDocument();
});
```

- [ ] **Step 2: Run the detail test and capture RED**

Run:

```powershell
npm test -- --run src/pages/topics/TopicDetailContent.test.tsx
```

Expected: FAIL because Consumers does not exist and Configuration is read-only list metadata.

- [ ] **Step 3: Implement independent loaders and tabs**

Change `TopicDetailTab` to `'overview' | 'routes' | 'consumers' | 'configuration'`. Add `consumersRequestRef` and `configRequestRef`, reset them on Topic change, and lazily call only the selected resource. Keep resource-specific `loading`, `error`, `retry`, and empty states.

Render queue-offset rows under Route/Status using stable `${brokerName}:${queueId}` identities. Render consumers with group, total diff, inflight diff, and TPS. Render config with broker selector, effective values, attributes, and `inconsistentFields` warning.

Add optional callbacks:

```typescript
onEdit?: (config: TopicConfigView) => void;
onReset?: (consumerGroup: string) => void;
onSkip?: (consumerGroup: string) => void;
```

Hide these mutation callbacks for `topicInfo.systemTopic` while keeping all read tabs.

- [ ] **Step 4: Run focused tests**

Run:

```powershell
npm test -- --run src/pages/topics/TopicDetailContent.test.tsx src/pages/TopicDetailPage.test.tsx
```

Expected: lazy cache, resource-specific retry, Topic identity invalidation, and direct route reuse pass.

- [ ] **Step 5: Commit detail parity**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/TopicDetailContent.tsx rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/topics/TopicDetailContent.test.tsx
git commit -m "feat(dashboard): add topic consumers and configuration"
```

---

### Task 7: Send, reset, and skip dialogs

**Files:**
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicSendMessageDialog.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicSendMessageDialog.test.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicResetOffsetDialog.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicResetOffsetDialog.test.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicSkipBacklogDialog.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicSkipBacklogDialog.test.tsx`
- Delete: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicMaintenanceDialog.tsx`
- Delete: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicMaintenanceDialog.test.tsx`

**Interfaces:**
- Consumes: Task 4 mutation APIs and Task 6 consumer identities.
- Produces: focused operation dialogs used by `TopicListPage`.

- [ ] **Step 1: Write failing operation tests**

Add these concrete contract and lifecycle tests:

```typescript
const sendOkFixture: TopicSendResultView = {
  topic: 'orders', success: true, sendStatus: 'SEND_OK', messageId: 'msg-old', brokerName: 'broker-a',
  queueId: 1, queueOffset: 42, transactionId: null, regionId: null, localTransactionState: null
};
const resetOldGroupFixture: TopicOffsetResult = {
  operation: 'RESET_OFFSET', topic: 'orders', consumerGroup: 'order-service', success: true,
  affectedQueueCount: 8, appliedTimestamp: 1_786_762_800_000, message: '8 queues reset'
};
const skipOldGroupFixture: TopicOffsetResult = {
  operation: 'SKIP_BACKLOG', topic: 'orders', consumerGroup: 'order-service', success: true,
  affectedQueueCount: 8, appliedTimestamp: 1_786_762_800_000, message: '8 queues skipped'
};

it('sends the exact message and renders broker non-success as an alert', async () => {
  const user = userEvent.setup();
  vi.mocked(topicApi.sendTestMessage).mockResolvedValue({
    topic: 'orders', success: false, sendStatus: 'FLUSH_DISK_TIMEOUT', messageId: 'msg-1',
    brokerName: 'broker-a', queueId: 1, queueOffset: 42, transactionId: null,
    regionId: null, localTransactionState: null
  });
  render(<TopicSendMessageDialog open topic="orders" onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  await user.type(screen.getByRole('textbox', { name: 'Key' }), 'order-42');
  await user.type(screen.getByRole('textbox', { name: 'Tag' }), 'created');
  await user.type(screen.getByRole('textbox', { name: 'Message body' }), '{"id":42}');
  await user.click(screen.getByRole('checkbox', { name: 'Enable trace' }));
  await user.click(screen.getByRole('button', { name: 'Review send' }));
  await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Send test message' }));
  expect(topicApi.sendTestMessage).toHaveBeenCalledWith('orders', {
    key: 'order-42', tag: 'created', messageBody: '{"id":42}', traceEnabled: true
  });
  expect(await screen.findByRole('alert')).toHaveTextContent('FLUSH_DISK_TIMEOUT');
});

it('resets the captured group to the selected local time', async () => {
  const user = userEvent.setup();
  vi.mocked(topicApi.resetOffset).mockResolvedValue({
    operation: 'RESET_OFFSET', topic: 'orders', consumerGroup: 'order-service', success: true,
    affectedQueueCount: 8, appliedTimestamp: new Date('2026-08-15T10:30').getTime(), message: 'reset'
  });
  render(<TopicResetOffsetDialog open topic="orders" consumerGroup="order-service"
    onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
  await user.click(screen.getByRole('button', { name: 'Review reset' }));
  await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Reset offset' }));
  expect(topicApi.resetOffset).toHaveBeenCalledWith('orders', {
    consumerGroup: 'order-service', resetTimestamp: new Date('2026-08-15T10:30').getTime(), force: true
  });
});

it('requires the exact consumer group before skipping backlog', async () => {
  const user = userEvent.setup();
  vi.mocked(topicApi.skipBacklog).mockResolvedValue({
    operation: 'SKIP_BACKLOG', topic: 'orders', consumerGroup: 'order-service', success: true,
    affectedQueueCount: 8, appliedTimestamp: 1_786_762_800_000, message: 'skipped'
  });
  render(<TopicSkipBacklogDialog open topic="orders" consumerGroup="order-service"
    onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  const confirm = screen.getByRole('button', { name: 'Skip accumulated messages' });
  expect(confirm).toBeDisabled();
  await user.type(screen.getByRole('textbox', { name: 'Confirm consumer group' }), 'order-service');
  expect(confirm).toBeEnabled();
  await user.click(confirm);
  expect(topicApi.skipBacklog).toHaveBeenCalledWith('orders', { consumerGroup: 'order-service' });
});

it('keeps send locked across close and reopen and drops the old result', async () => {
  const user = userEvent.setup();
  const pending = deferred<TopicSendResultView>();
  vi.mocked(topicApi.sendTestMessage).mockReturnValue(pending.promise);
  const { rerender } = render(<TopicSendMessageDialog open topic="orders" onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  await user.type(screen.getByRole('textbox', { name: 'Message body' }), 'test');
  await user.click(screen.getByRole('button', { name: 'Review send' }));
  await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Send test message' }));
  rerender(<TopicSendMessageDialog open={false} topic="orders" onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  rerender(<TopicSendMessageDialog open topic="payments" onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  expect(topicApi.sendTestMessage).toHaveBeenCalledTimes(1);
  await act(async () => pending.resolve(sendOkFixture));
  expect(screen.queryByText('msg-old')).not.toBeInTheDocument();
});

it('keeps reset locked across group changes and drops the old result', async () => {
  const user = userEvent.setup();
  const pending = deferred<TopicOffsetResult>();
  vi.mocked(topicApi.resetOffset).mockReturnValue(pending.promise);
  const { rerender } = render(<TopicResetOffsetDialog open topic="orders" consumerGroup="order-service"
    onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  fireEvent.change(screen.getByLabelText('Reset time'), { target: { value: '2026-08-15T10:30' } });
  await user.click(screen.getByRole('button', { name: 'Review reset' }));
  await user.click(within(screen.getByRole('alertdialog')).getByRole('button', { name: 'Reset offset' }));
  rerender(<TopicResetOffsetDialog open topic="orders" consumerGroup="payment-service"
    onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  expect(topicApi.resetOffset).toHaveBeenCalledTimes(1);
  await act(async () => pending.resolve(resetOldGroupFixture));
  expect(screen.queryByText(/8 queues reset/)).not.toBeInTheDocument();
});

it('keeps skip locked across group changes and drops the old result', async () => {
  const user = userEvent.setup();
  const pending = deferred<TopicOffsetResult>();
  vi.mocked(topicApi.skipBacklog).mockReturnValue(pending.promise);
  const { rerender } = render(<TopicSkipBacklogDialog open topic="orders" consumerGroup="order-service"
    onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  await user.type(screen.getByRole('textbox', { name: 'Confirm consumer group' }), 'order-service');
  await user.click(screen.getByRole('button', { name: 'Skip accumulated messages' }));
  rerender(<TopicSkipBacklogDialog open topic="orders" consumerGroup="payment-service"
    onOpenChange={vi.fn()} onSucceeded={vi.fn()} />);
  expect(topicApi.skipBacklog).toHaveBeenCalledTimes(1);
  await act(async () => pending.resolve(skipOldGroupFixture));
  expect(screen.queryByText(/8 queues skipped/)).not.toBeInTheDocument();
});
```

Place each fixture in the test file that consumes it and import `deferred` from `src/test/deferred.ts` in all three files.

- [ ] **Step 2: Run all three new tests and capture RED**

Run:

```powershell
npm test -- --run src/components/TopicSendMessageDialog.test.tsx src/components/TopicResetOffsetDialog.test.tsx src/components/TopicSkipBacklogDialog.test.tsx
```

Expected: FAIL because the files do not exist.

- [ ] **Step 3: Implement Send**

Use shared Dialog/Input/Label/Button controls. Topic is read-only; body is required; key/tag optional; Trace is a labeled checkbox. The submit callback is internal and calls `topicApi.sendTestMessage`. Render Message ID, Broker, Queue ID/Offset, transaction ID, region, and local state. Use `role="status"` only for `result.success`; use `role="alert"` for a rejected request or broker non-success.

- [ ] **Step 4: Implement Reset**

Props are `open`, `topic`, `consumerGroup`, `onOpenChange`, and `onSucceeded`. Render an immutable Topic/group summary and one `datetime-local` input. The confirmation names Topic, group, and the localized selected time. Send `{ consumerGroup, resetTimestamp, force: true }` and render affected queue count.

- [ ] **Step 5: Implement Skip**

Render the current backlog warning and require exact consumer-group text. Send only `{ consumerGroup }`; the backend owns the applied timestamp. Render affected queue count and applied timestamp after success.

- [ ] **Step 6: Apply the shared mutation lifecycle**

Each dialog uses the same rules from Task 5: synchronous pending ref, generation plus captured Topic/group identity, Strict Mode-safe mounted ref, no automatic retry, form values preserved on failure, and focus restoration through the shared Dialog primitives.

- [ ] **Step 7: Run focused tests and remove the old reset component**

Run:

```powershell
npm test -- --run src/components/TopicSendMessageDialog.test.tsx src/components/TopicResetOffsetDialog.test.tsx src/components/TopicSkipBacklogDialog.test.tsx
```

Expected: all send-status, payload, confirmation, duplicate-submit, stale-result, and Strict Mode cases pass. Delete `TopicMaintenanceDialog` only after its replacement tests are green.

- [ ] **Step 8: Commit maintenance dialogs**

```powershell
git add -A rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components
git commit -m "feat(dashboard): add topic message and offset operations"
```

---

### Task 8: Delete dialogs and Topic page integration

**Files:**
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicDeleteDialog.tsx`
- Create: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/TopicDeleteDialog.test.tsx`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/TopicListPage.tsx`
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages/TopicListPage.test.tsx`

**Interfaces:**
- Consumes: Tasks 4–7 filters, dialogs, action availability, and operation results.
- Produces: the complete `/topics` operator workflow.

- [ ] **Step 1: Write failing deletion and page-parity tests**

Add a component test proving exact-name confirmation and broker-specific payload. Extend `TopicListPage.test.tsx` so one eligible row exposes all actions:

```typescript
await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
for (const action of [
  'View details', 'Edit configuration', 'Send test message', 'Reset consumer offset',
  'Skip accumulated messages', 'Delete from broker', 'Delete topic'
]) {
  expect(screen.getByRole('menuitem', { name: action })).toBeInTheDocument();
}
```

Add cases that System has view-only actions, Retry/DLQ retain operations, multi-select type/category filters work, edit loads config before enabling save, and successful delete closes an open detail sheet and removes the row after refresh.

Add deferred-promise cases proving an operation remains locked when the sheet/dialog closes and another Topic is selected, and stale errors/notices never replace the current page query state.

- [ ] **Step 2: Run focused tests and capture RED**

Run:

```powershell
npm test -- --run src/components/TopicDeleteDialog.test.tsx src/pages/TopicListPage.test.tsx
```

Expected: FAIL because deletion has no exact confirmation/broker mode and the page does not expose all actions.

- [ ] **Step 3: Implement the deletion dialog**

Use props:

```typescript
interface TopicDeleteDialogProps {
  open: boolean;
  topic: TopicInfo | null;
  mode: 'broker' | 'topic';
  brokerName?: string;
  onOpenChange: (open: boolean) => void;
  onSucceeded: (result: TopicOperationResult) => void;
}
```

Broker mode uses a shared Select populated from `topic.brokers`; Topic mode lists clusters/brokers. Both require typing the exact Topic name. Call `deleteFromBroker` or `delete`, keep partial results visible, and close only on full success. Use the real pending/generation pattern.

- [ ] **Step 4: Integrate enriched catalog, filters, table, and dialogs**

Replace the page-wide consumer discovery request with per-Topic `topicApi.consumers`. Use `TopicFilterToolbar`; derive cluster/broker options from `data.targets`; add Message type, Targets, Ordered, and Permission columns without removing the Topic full-page link.

Use a discriminated selection state so every operation captures exact identity:

```typescript
type TopicAction =
  | { kind: 'edit'; topic: TopicInfo }
  | { kind: 'send'; topic: TopicInfo }
  | { kind: 'reset'; topic: TopicInfo; consumerGroup?: string }
  | { kind: 'skip'; topic: TopicInfo; consumerGroup?: string }
  | { kind: 'delete-broker'; topic: TopicInfo; brokerName?: string }
  | { kind: 'delete-topic'; topic: TopicInfo };
```

The row menu uses `getTopicActionAvailability`; details pass `onEdit`, `onReset`, and `onSkip`. Edit loads `topicApi.config` with its own retry state. Reset/Skip load the selected Topic consumers and preserve any group passed from the Consumers tab.

Keep read-query error, operation error, and success notice separate. A failed operation never replaces the table with `ErrorState`. Refresh only the catalog/config/consumers/stat resources affected by a successful operation.

- [ ] **Step 5: Run page integration tests**

Run:

```powershell
npm test -- --run src/pages/TopicListPage.test.tsx src/components/TopicDeleteDialog.test.tsx src/pages/topics/TopicDetailContent.test.tsx
```

Expected: every Java action, filter, protected system row, eligible retry/DLQ row, deletion scope, stale response, and focused retry test passes.

- [ ] **Step 6: Commit complete page integration**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/frontend/src
git commit -m "feat(dashboard): complete topic operations workspace"
```

---

### Task 9: Dark-theme polish, full validation, and browser QA

**Files:**
- Modify: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/styles/globals.css`
- Modify only if QA finds a defect: Topic files changed in Tasks 4–8
- Update: `docs/superpowers/plans/2026-08-15-topic-full-operations.md` checkboxes

**Interfaces:**
- Consumes: the complete backend/frontend implementation.
- Produces: validated, reviewable Topic parity ready for branch integration.

- [ ] **Step 1: Add the smallest required Topic styles**

Add dense responsive rules for the multi-filter toolbar, target checkbox groups, operation-result rows, config inconsistency notice, send result grid, and consumer/config tables. Reuse existing CSS variables and ensure children use `min-width: 0`; table overflow stays local. At `max-width: 900px`, stack form/result grids to one column.

- [ ] **Step 2: Run source policy scans**

Run:

```powershell
rg -n "linear-gradient|radial-gradient|#[fF]{6}|Java-style|migration|API parity" rocketmq-dashboard/rocketmq-dashboard-web/frontend/src
rg -n "<button" rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/pages rocketmq-dashboard/rocketmq-dashboard-web/frontend/src/components/Topic*.tsx
```

Expected: no new gradient, hard-coded white, internal parity copy, or raw Topic button. Existing unrelated matches must be reported, not rewritten.

- [ ] **Step 3: Run full frontend validation**

Run from `rocketmq-dashboard/rocketmq-dashboard-web/frontend`:

```powershell
npm test -- --run
npm run build
```

Expected: all tests pass and production build exits zero. The known Vite chunk-size advisory is informational unless its threshold becomes an error.

- [ ] **Step 4: Run full backend validation**

Run from `rocketmq-dashboard/rocketmq-dashboard-web/backend`:

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
cargo test --all-targets --all-features
```

Expected: all commands exit zero.

- [ ] **Step 5: Verify repository hygiene**

Run from the worktree root:

```powershell
git diff --check
git status --short
git log --oneline --decorate -12
```

Expected: only intentional plan checkbox updates, if any, remain; no `dist`, logs, screenshots, `node_modules`, or root `producer.rs` change appears.

- [ ] **Step 6: Run browser QA against the local RocketMQ environment**

Use the already selected in-app browser. Capture the Java reference Topic list/form and the Rust `/topics` list/form at the same viewport. Exercise with the local NameServer `127.0.0.1:9876` and local Broker:

1. Verify all eight filters and Reset filters.
2. Create a disposable Topic and inspect catalog metadata.
3. Edit its queues/message type and verify config refresh.
4. Send a test message and inspect the structured result.
5. Inspect route/status and consumers.
6. For a disposable consumer group only, verify reset then skip confirmations and results.
7. Delete from one broker when multiple brokers are available; otherwise verify the selector truthfully exposes only the real broker.
8. Delete the disposable Topic and verify list/detail cleanup.
9. Compare reference and implementation screenshots side-by-side for layout, density, borders, typography, contrast, focus, overflow, and dialog sizing.

Do not run Reset/Skip/Delete against an existing user workload. If the local environment lacks a consumer or second broker, verify the truthful empty/disabled state and record that limitation rather than fabricating data.

- [ ] **Step 7: Fix only defects found by validation, rerun their focused RED/GREEN tests, then rerun Steps 2–5**

Every fix receives a failing regression test first. Do not broaden scope during polish.

- [ ] **Step 8: Commit final polish**

```powershell
git add rocketmq-dashboard/rocketmq-dashboard-web/frontend/src docs/superpowers/plans/2026-08-15-topic-full-operations.md
git commit -m "test(dashboard): verify complete topic operations"
```

After this commit, invoke `superpowers:verification-before-completion`, then `superpowers:requesting-code-review`, address any Critical/Important findings with TDD, invoke `superpowers:finishing-a-development-branch`, and merge `mxsm/topic-full-operations` into the original branch without overwriting the unrelated root `rocketmq-client/examples/quickstart/producer.rs` change.
