# RocketMQ Dashboard Web Topic Full Operations Design

**Date:** 2026-08-15

**Status:** Approved for implementation planning

**Target:** `rocketmq-dashboard/rocketmq-dashboard-web`
**Reference behavior:** `D:\Github\Java\rocketmq-dashboard\frontend-new\src\pages\Topic`

## 1. Goal

Bring every operator-facing Topic capability from the Java Dashboard into the Rust Web Dashboard `/topics` experience while preserving the current dense, dark operations-console design. The result must support discovery, inspection, configuration, testing, offset maintenance, and deletion without requiring the Java Dashboard.

The implementation will reuse the current Topic list and detail experience. It will not copy the Java page's light styling or reproduce its collection of isolated dialogs when the current detail layout can present the same information more clearly.

## 2. Scope

### Included capabilities

| Java Topic capability | Rust Web behavior |
| --- | --- |
| Topic list and text search | Keep the current list and add accurate server-derived metadata. |
| Message-type filters | Multi-select Normal, Delay, FIFO, Transaction, Unspecified, Retry, DLQ, and System filters. |
| Add Topic | Create a Topic against selected clusters and/or brokers. |
| Update Topic | Load the broker-backed configuration, expose inconsistent fields, and update selected targets. |
| Topic status | Show aggregate counts and per-queue offsets in the detail workspace. |
| Topic route | Show broker and queue routing in the detail workspace. |
| Consumer management | Show consumer group, backlog, inflight count, and consume TPS. |
| Topic configuration | Show the effective configuration, target lists, attributes, and inconsistent fields. |
| Send message | Send a real test message with key, tag, body, and trace option. |
| Reset consumer offset | Reset one consumer group's offsets to an explicit timestamp. |
| Skip accumulated messages | Advance one consumer group to the latest visible offsets. |
| Delete by broker | Delete the Topic from one selected broker. |
| Delete Topic | Delete the Topic across the selected cluster scope. |

### Non-goals

- Reproducing the Java Dashboard visual style.
- Adding new application routes solely for Topic operations.
- Changing GPUI or Tauri dashboards.
- Adding speculative Topic features that are absent from both the Java page and the existing Rust admin core.
- Treating an HTTP 200 response as proof that every broker-side operation succeeded.

## 3. Experience and information architecture

### 3.1 Topic list

The existing `/topics` page remains the entry point. It keeps the current dark page header and summary cards.

The filter toolbar contains:

- text search by Topic name;
- multi-select message type filters for Normal, Delay, FIFO, Transaction, and Unspecified;
- multi-select category filters for Retry, DLQ, and System;
- cluster filter;
- broker filter;
- Refresh and Create Topic actions.

The table shows Topic, category, message type, clusters/brokers, read queues, write queues, permission, ordered state, and operations. Long cluster or broker sets use a compact count with an accessible popover or tooltip instead of widening the table indefinitely. Loading, discovery error, empty result, refresh, and retry states remain visible without losing the current filters.

Selecting the Topic name opens the existing detail surface. The row operation menu exposes every permitted operation:

- View details;
- Edit configuration;
- Send test message;
- Reset consumer offset;
- Skip accumulated messages;
- Delete from broker;
- Delete Topic.

System Topics remain inspectable. Destructive and message-producing actions are not shown for system Topics, and the backend rejects attempts made outside the UI.

### 3.2 Topic detail

The existing Topic detail surface is organized into four tabs:

1. **Overview** — category, message type, queue counts, permission, ordered state, broker count, consumer count, total messages, and backlog.
2. **Route and status** — broker route data plus per-queue minimum offset, maximum offset, message count, and last update time.
3. **Consumers** — consumer group, total backlog, inflight backlog, consume TPS, and contextual Reset/Skip actions.
4. **Configuration** — effective values, cluster and broker targets, Topic attributes, and broker-inconsistent fields with an Edit action.

Each tab loads independently. A failure in consumers must not hide valid route, status, or configuration data. Refreshing one tab refreshes only that resource.

### 3.3 Create and edit

Create and Edit share one form model while keeping mode-specific validation:

- Topic name is required and immutable in Edit mode.
- At least one cluster or broker target is required.
- Cluster and broker choices come from the catalog response; operators do not enter comma-separated infrastructure identifiers.
- Read and write queue counts are integers from 1 through 128.
- Permission uses explicit Read and Write toggles plus an advanced Inherit toggle, producing RocketMQ's supported permission bitmasks from 1 through 7. At least Read or Write is required; the common Read/Write value is the default.
- Message type supports Normal, FIFO, Delay, and Transaction.
- Ordered state is explicit.

Edit loads the real Topic configuration first. When brokers disagree, the form shows the inconsistent fields and requires the operator to choose the intended target brokers before submission. The confirmation summary names the Topic and target count. The form remains open with its values intact after a validation or network failure.

## 4. Operational actions and safeguards

### 4.1 Send test message

The dialog captures Topic, key, tag, message body, and trace enabled. Topic is read-only. Body is required; key and tag are optional. The result renders structured fields: send status, message ID, broker, queue ID, queue offset, transaction ID, region, and local transaction state when present.

The action is unavailable for System Topics. Retry and DLQ Topics retain the same operational actions as the Java Dashboard. A request is considered successful only when the returned send status represents a successful broker send; other statuses render as an operation failure even when the HTTP request completed.

### 4.2 Reset consumer offset

The dialog requires a known consumer group and an explicit local date/time. The UI converts it to an epoch-millisecond timestamp and shows the resolved timestamp in the confirmation. The backend validates that the Topic and consumer group are non-empty and that the timestamp fits RocketMQ's signed Java-long range. The response reports affected queue count.

### 4.3 Skip accumulated messages

Skip is a dedicated operation, not an overloaded timestamp value in the public HTTP contract. The confirmation states that unread messages currently in the backlog will be skipped and requires the operator to type the consumer group name.

The backend captures its current epoch-millisecond time and invokes the existing reset-to-timestamp path with force enabled. RocketMQ resolves each queue to the latest visible offset at that time. This avoids passing Java Dashboard's `-1` sentinel through the Rust unsigned timestamp API while preserving the operator-visible result. The response reports the captured timestamp and affected queue count.

### 4.4 Delete from broker

The operator selects one broker from the Topic's actual route and types the Topic name to confirm. The backend resolves and validates the selected broker against current route data before deleting. The result identifies the broker and target count.

### 4.5 Delete Topic

The confirmation lists the resolved cluster/broker scope and requires the operator to type the exact Topic name. A System Topic is rejected. After success, the Topic is removed from the list and its open detail surface is closed. Partial broker failure remains visible and does not produce a global success notice.

### 4.6 Concurrency and stale-result rules

- A mutation has a real in-flight lock that remains active until its promise settles; closing a dialog or changing selection does not permit the same mutation to be submitted again.
- Query and mutation generations are separate. A stale response may finish but cannot update a new Topic, tab, consumer group, or dialog context.
- Topic, consumer-group, target-broker, and operation identity are captured when a mutation begins and included in the stale-response check.
- Dialog close, page navigation, selection changes, filter changes, and unmount invalidate presentation updates, but do not falsely report that a server-side mutation was cancelled.

## 5. Backend architecture and contracts

### 5.1 Boundaries

Axum handlers stay thin. Topic HTTP DTOs live in the web backend model layer, input validation and orchestration live in `topic_service`, and RocketMQ calls live on `DashboardAdminClient`.

The implementation reuses `rocketmq-admin-core::core::topic::TopicAdmin` and its existing catalog, configuration, consumer, reset, send, and delete capabilities. Web-only orchestration remains in the Web backend. Shared admin-core changes are limited to behavior that is genuinely missing or incorrectly structured for all consumers.

### 5.2 HTTP resources

Existing endpoints remain compatible:

- `GET /api/topics`
- `GET /api/topics/{topic}`
- `POST /api/topics`
- `PUT /api/topics/{topic}`
- `DELETE /api/topics/{topic}`
- `GET /api/topics/{topic}/route`
- `GET /api/topics/{topic}/stats`

The enriched list response adds message type, clusters, brokers, queue counts, permission, ordered state, and system flag. New operations are exposed as focused resources:

- `GET /api/topics/{topic}/config`
- `GET /api/topics/{topic}/consumers`
- `POST /api/topics/{topic}/test-message`
- `POST /api/topics/{topic}/consumer-offset/reset`
- `POST /api/topics/{topic}/consumer-offset/skip`
- `DELETE /api/topics/{topic}/brokers/{broker}`

All path values are URL encoded by the frontend client. Request bodies never repeat an authoritative path Topic; when a compatibility DTO contains it, the handler overwrites it with the decoded path value.

### 5.3 Result contracts

Read resources return concrete typed DTOs. Mutation resources return structured outcomes rather than prose-only messages:

```text
TopicOperationResult
  operation
  topic
  success
  targetCount
  message
  targets[]
    target
    success
    message
```

Send-message uses its own structured send-result DTO. Reset and Skip include consumer group, affected queue count, and applied timestamp. A target failure is preserved even if other targets succeeded. `success` is true only when all required targets succeeded.

### 5.4 Validation and authorization

- Trim operator inputs exactly once at the API boundary.
- Reject blank Topic, consumer group, broker, or message body values.
- Validate queue and permission ranges before making an admin call.
- Resolve target brokers and consumer groups from current broker data rather than trusting arbitrary UI values.
- Reject destructive or message-producing operations for System Topics on the backend.
- Preserve the dashboard's existing authentication and authorization boundary. The UI may hide unavailable actions, but backend validation is authoritative.
- Never log message bodies, credentials, or full mutation request objects.

## 6. Frontend component boundaries

`TopicListPage` coordinates page filters, catalog loading, pagination, selection, and detail opening. It does not own every dialog's form state.

Focused components own one responsibility each:

- `TopicFilterToolbar` — text, type, category, cluster, and broker filters;
- `TopicDataTable` — rows, columns, pagination, and operation dispatch;
- `TopicDetailContent` — tab selection and resource-specific loaders;
- `TopicMutationDialog` — Create/Edit form;
- `TopicSendMessageDialog` — send form and result;
- `TopicResetOffsetDialog` — reset form and confirmation;
- `TopicSkipBacklogDialog` — exact consumer-group confirmation;
- `TopicDeleteDialog` — broker-scoped and whole-Topic deletion modes;
- pure models/helpers — filtering, permission labels, operation availability, form validation, canonical action identities, and structured-result classification.

Existing shared Button, Input, Label, Dialog/AlertDialog, Select, table, notice, badge, and loading/error components are reused. Lucide supplies action icons. New styles use the existing dark design tokens; no gradients, glows, hard-coded white, or legacy Java styling are introduced.

## 7. Data flow and state behavior

1. The page loads the enriched Topic catalog once and derives the visible rows from explicit filter state.
2. Opening details captures the canonical Topic name and starts independent route, stats, consumers, and config loaders as their tabs are visited.
3. Opening an operation captures its canonical Topic and target identity. Any required discovery data is loaded before the confirmation becomes actionable.
4. Submit synchronously acquires the operation's in-flight guard before awaiting the API.
5. The backend revalidates targets, performs the admin operation, and returns a structured result.
6. The UI commits the result only if the presentation generation still matches. Successful resources are invalidated narrowly: catalog/config for Create/Edit/Delete, consumers/stats for Reset/Skip, and no catalog refresh for Send.
7. Errors remain next to the action that produced them. A mutation or export error never replaces valid table data or changes a read-query retry into a mutation retry.

## 8. Error and empty states

- Initial catalog discovery has a dedicated retry that repeats catalog discovery.
- Route, stats, consumers, and configuration failures stay within their own detail tab and have resource-specific Retry actions.
- Mutation errors remain inside their dialog with `role="alert"`; entered values and confirmation text are preserved.
- Partial outcomes show successful and failed targets separately and offer retry only for failed targets when the underlying operation is safely repeatable.
- Reset, Skip, and Delete are not automatically retried because their side effects may already have occurred.
- An empty consumer list is a valid state, not an error.
- Missing route/config metadata fails closed for target-specific mutations.

## 9. Testing strategy

Implementation follows test-driven development: add a focused failing test before each behavior change, then implement the smallest passing change.

### Backend tests

- DTO serialization and validation for catalog, create/edit, send, reset, skip, broker delete, and whole-Topic delete.
- Service tests proving path Topic authority, input trimming, queue ranges, system-Topic rejection, and target revalidation.
- Admin adapter tests for catalog mapping, partial target outcomes, send-status classification, reset fallback, skip timestamp capture, and broker-scoped deletion.
- Regression tests that partial success is not flattened into a global success.
- Tests that sensitive message bodies are not included in errors or tracing fields.

### Frontend tests

- Accurate multi-select filtering for every Java message/category option.
- Create/Edit validation, target selection, inconsistent configuration presentation, and form preservation on rejection.
- All row operations are discoverable for eligible Topics and absent for System Topics.
- Send, Reset, Skip, broker-delete, and whole-delete payloads use the captured canonical identities.
- Exact-name confirmations gate Skip and Delete.
- Deferred-promise tests cover duplicate submit, dialog close/reopen, Topic changes, consumer changes, unmount, and stale success/error/finally updates.
- Resource-specific loading, empty, error, and Retry behavior.
- Partial result rendering and safe retry semantics.
- Accessible dialog title/description, alert announcements, labels, focus recovery, and keyboard submission.

### Final validation

From the Web frontend:

```text
npm test -- --run
npm run build
```

From the Web backend:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
cargo test --all-targets --all-features
```

If `rocketmq-admin-core` or another root/shared crate changes, run its focused tests and every additional validation profile required by the repository `AGENTS.md` routing rules.

After automated checks, inspect the Java reference and Rust `/topics` page in the in-app browser at the same viewport. Verify all visible states, dialog focus, table density, responsive overflow, dark-theme contrast, and every operator action against a real local NameServer/Broker.

## 10. Acceptance criteria

- Every Java Topic operation listed in Section 2 is available in `/topics` for eligible Topics.
- Existing stats and route behavior remains available and is integrated into the detail surface.
- Filters use broker-derived message/category metadata rather than Topic-name-only guesses where authoritative data exists.
- Create and Edit use discoverable cluster/broker choices and return truthful per-target results.
- Consumers display backlog, inflight count, and consume TPS.
- Send returns and renders the real broker send outcome.
- Reset applies the selected timestamp; Skip advances to the latest visible offsets using a dedicated contract.
- Delete supports both broker-scoped and whole-Topic scope with exact-name confirmation.
- System Topics are protected in both frontend and backend.
- Duplicate submissions and stale asynchronous results cannot trigger or present an operation in the wrong context.
- The implementation uses the existing dark design system and shared controls.
- Focused and full project validations pass, and browser QA confirms parity on the local RocketMQ environment.
