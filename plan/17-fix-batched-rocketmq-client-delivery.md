# Fix `consumeMessageBatchMaxSize` in `rocketmq-rust` Client Library

## Scope

This is an upstream client-library fix. Implement it only in:

`/Users/nanashi07/Projects/rust/rocketmq-rust`

Ralive must not contain a batch workaround, compensating retry loop, altered listener
control flow, diagnostic-only `SocketMessage` field, or client-library correctness fix.
After the upstream patch is complete, Ralive may only update its pinned dependency
revision and run an end-to-end verification against the fixed library.

The immediate target is the non-POP concurrent push-consumer path:

- `rocketmq-client/src/consumer/consumer_impl/consume_message_concurrently_service.rs`
- `rocketmq-client/src/consumer/consumer_impl/process_queue.rs`
- `rocketmq-client/src/consumer/listener/message_listener_concurrently.rs`
- `rocketmq-client/src/consumer/listener/consume_concurrently_context.rs`

## Problem Statement

When `consume_message_batch_max_size` is `1`, messages are consumed normally. When it
is greater than `1` (observed at `16`), only a subset of messages reaches the consumer
application. The bug is therefore in the client library's batching, callback-result, or
offset-management logic—not in application routing or client emission.

The library must guarantee this invariant for every queue:

> A message must remain eligible for consumption until its callback explicitly
> acknowledges it, or the client has performed a documented terminal failure action.
> A callback failure must never silently acknowledge messages that were not handed to
> the listener or were not successfully processed by it.

## Bugs Fixed

### Phase 5 — ProcessQueue accounting (committed first)

`ProcessQueue::remove_message` incremented `removed_cnt` inside the per-message loop
and called `fetch_sub(removed_cnt)` in each iteration, causing cumulative
over-subtraction (`1 + 2 + ... + N` instead of `N`) for N removed messages.

**Fix:** accumulate `removed_cnt` across the loop, apply a single `fetch_sub` after
all removals, and move the zeroing guard outside the loop.

### Phase 1 — Callback acknowledgement was inexpressible

`ConsumeConcurrentlyContext.ack_index` defaulted to `i32::MAX` and was passed as an
immutable reference, so listeners could not record partial progress.

`process_consume_result` treated any `ack_index >= msgs.len()` as "ack all", making
the default harmless on `ConsumeSuccess` but silently wrong on `ReconsumeLater`.

**Fix:**
- `ack_index` changed to `Option<i32>` with explicit semantics:
  - `None + ConsumeSuccess` → ack whole batch
  - `None + ReconsumeLater` → ack nothing (retry all)
  - `Some(n) + any` → ack indices `0..=n`, retry the rest (clamped to `[-1, len-1]`)
- `MessageListenerConcurrently::consume_message` and `MessageListenerConcurrentlyFn`
  now accept `&mut ConsumeConcurrentlyContext`.
- `ConsumeRequest::run` creates a mutable context and passes it to the listener.

### Phase 2 — Broadcasting ReconsumeLater silently dropped messages

On `ReconsumeLater` in BROADCASTING mode, the old code logged "drop it" for each
pending message and then removed the entire batch from `ProcessQueue`, committing
offsets for messages never successfully processed.

**Fix:** the pending suffix is split off from `consume_request.msgs` and re-submitted
via `submit_consume_request_later`. Only the acknowledged prefix (indices `0..=ack_index`)
is removed from `ProcessQueue` and has its offset updated.

### Phase 3 — No terminal failure policy for BROADCASTING

There was no limit on BROADCASTING retries; the fix in Phase 2 would retry forever.

**Fix:**
- Two new fields added to `ConsumerConfig`:
  - `broadcast_retry_delay: Duration` (default `100ms`; must be > zero)
  - `broadcast_max_retries: u32` (default `3`)
- In the Broadcasting retry path, each pending message's `reconsume_times` is checked
  against `broadcast_max_retries`. Messages that have exhausted retries emit a WARN-level
  terminal-failure log (topic, queueOffset, msgId, attempt count; no body) and are
  dropped. Messages within the limit have `reconsume_times` incremented and are
  re-submitted after `broadcast_retry_delay` via `submit_consume_request_later_with_delay`.

### Phase 4 — Unsafe concurrent mutation of shared state

`process_consume_result` mutated `DefaultMQPushConsumerImpl` (offset store) from
independently spawned chunk tasks, all of which cloned the same `ArcMut` wrapper.
`ArcMut` wraps `SyncUnsafeCell` and requires callers to prevent data races.

**Fix:**
- `commit_lock: Arc<tokio::sync::Mutex<()>>` added to `ProcessQueue`.
- `process_consume_result` acquires `commit_lock` before `remove_message` and releases
  it after `update_offset`. The listener callback, broker send-back I/O, and retry
  scheduling all happen before the lock is taken.
- Independent queues are not blocked; only tasks sharing a `ProcessQueue` instance
  queue behind each other.

## Implementation Summary

| Phase | File(s) changed | Key change |
|-------|----------------|------------|
| 5 | `process_queue.rs` | Single `fetch_sub` after removal loop |
| 1 | `consume_concurrently_context.rs`, `message_listener_concurrently.rs`, all call sites | `Option<i32>` ack_index; `&mut` context |
| 2 | `consume_message_concurrently_service.rs` | Split acked/pending; retain pending in PQ |
| 3 | `default_mq_push_consumer.rs`, `consume_message_concurrently_service.rs` | Terminal policy config + enforcement |
| 4 | `process_queue.rs`, `consume_message_concurrently_service.rs` | `commit_lock` per queue |

## Unit Tests Added

- `process_queue::tests` — removal of 1, 2, 16 messages; mixed subset; no-underflow
  guard; partial removal returns lowest retained offset; full removal returns
  `queue_offset_max + 1`; concurrent removal serialised by `commit_lock`.
- `consume_concurrently_context::tests` — default `None` ack_index; set/overwrite;
  negative values accepted.
- `default_mq_push_consumer::tests` — default delay/retries; setter round-trip;
  zero-delay validation panic.

## Resolved Decisions

| Decision | Approved behaviour |
| --- | --- |
| BROADCASTING terminal failure | Drop only the individual message after retries are exhausted; emit bounded terminal-failure metadata first. |
| Retry defaults | Three retries after the first failed callback, with a 100ms delay; both values are configurable in `ConsumerConfig`. |
| Queue order | Strict per-queue order enforced by `ProcessQueue.commit_lock`; a later chunk cannot advance the offset past an earlier retained chunk. |
| ack_index default | `Option<i32>::None`; meaning determined by the return status, not a magic sentinel. |
