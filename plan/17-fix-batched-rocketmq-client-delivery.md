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

## Current Library Behaviour and Defects

### Callback chunking

`ConsumeMessageConcurrentlyService::submit_consume_request` splits a pulled vector
with `msgs.chunks(consume_message_batch_max_size)` and spawns one `ConsumeRequest` per
chunk. A size of `16` therefore makes one listener invocation responsible for sixteen
messages; it must have correct partial-success semantics.

### Whole-batch failure loses unprocessed messages

`ConsumeRequest::run` passes `&ConsumeConcurrentlyContext` to the listener. Although
the context defines `set_ack_index`, the immutable reference prevents the listener from
recording the successfully handled prefix.

If the listener returns `ReconsumeLater`, `process_consume_result` forces
`ack_index = -1`. In `MessageModel::Broadcasting`, it then logs the batch as dropped,
removes the entire callback vector from `ProcessQueue`, and updates the offset. Any
message after the first application failure can therefore be committed without ever
being handled. This is directly batch-size-sensitive: at size one the blast radius is
one message; at size sixteen it is up to sixteen.

### Unsafe concurrent mutable access

Each chunk task receives clones of `ArcMut<ConsumeMessageConcurrentlyService>` and
`ArcMut<DefaultMQPushConsumerImpl>`. `ArcMut` wraps `SyncUnsafeCell` and explicitly
requires callers to prevent data races. The concurrent service mutates these shared
objects from spawned chunk tasks without synchronization while processing callback
results and offsets. This must be removed or serialized; it is not acceptable to rely
on timing that happens to work with batch size one.

### Incorrect `ProcessQueue` count update

`ProcessQueue::remove_message` increments `removed_cnt` but subtracts that cumulative
value from `msg_count` inside its per-message loop. For 16 removed messages it subtracts
`1 + 2 + ... + 16`, not `16`. This corrupts queue accounting and can underflow. It must
be fixed and tested as part of the batch repair.

## Root-Cause Tests (Write These Before the Fix)

All tests below belong in the `rocketmq-rust` repository. They must use a fake listener
and an in-memory `ProcessQueue`; no Ralive source changes and no live broker are needed.

### 1. Successful 16-message callback

Seed offsets `0..15` in one process queue. Invoke the concurrent service with
`consume_message_batch_max_size = 16` and a listener that records every offset then
returns `ConsumeSuccess`.

Assert:

- listener saw all offsets `0..15` exactly once;
- returned/committed offset is `16`;
- process queue has no retained messages;
- `msg_count == 0` and `msg_size == 0`.

This establishes whether the library loses messages even when the listener returns
success for the full batch.

### 2. Partial-success failure at the middle of a 16-message callback

Use a listener that explicitly acknowledges offsets `0..4` and returns
`ReconsumeLater` at offset `5`. Offsets `6..15` must not be discarded.

The regression assertion is:

- offsets `0..4` are removed/committed;
- offsets `5..15` remain retained and are scheduled for retry;
- no committed offset is greater than `5` before the retry succeeds.

This test must fail on the current library, which replaces the acknowledgement index
with `-1` and drops the callback in BROADCASTING mode.

### 3. Two chunks from the same queue complete out of order

Use a pull-sized vector of 32 messages and `consume_message_batch_max_size = 16`. Hold
the first chunk at a barrier, let the second chunk finish first, then release the first.

Assert that offset advancement never skips the held lower offsets. Repeat the scenario
where the first chunk fails and the second succeeds.

### 4. Concurrent stress test

Run many iterations with multiple pull batches and controlled listener delays. Collect
each queue offset the listener sees and verify that every offset is exactly one of:

- successfully committed; or
- retained for a single scheduled retry; or
- recorded by the selected terminal-failure policy.

No offset may disappear. Run this test under the strongest supported race detector
(ThreadSanitizer if available; otherwise repeated stress execution plus Miri for the
isolated data structures where feasible).

### 5. ProcessQueue accounting tests

Remove 1, 2, 16, and mixed subsets of messages. After each operation compare the
atomic counters with the actual tree-map length and retained-body byte total. Include an
assertion that `msg_count` cannot underflow.

## Library Implementation Plan

### Phase 1 — Make callback acknowledgement expressible

Files:

- `rocketmq-client/src/consumer/listener/message_listener_concurrently.rs`
- `rocketmq-client/src/consumer/listener/consume_concurrently_context.rs`
- all client-library call sites and listener implementations

1. Change `MessageListenerConcurrently::consume_message` to accept
   `&mut ConsumeConcurrentlyContext`.
2. Change `MessageListenerConcurrentlyFn` and any public registration APIs to use the
   same mutable context contract.
3. In `ConsumeRequest::run`, create a mutable context and pass it to the listener.
   Pass the final context to result processing only after the listener returns.
4. Replace the ambiguous default `ack_index: i32::MAX` with explicit acknowledgement
   state, for example `ack_index: Option<i32>`.
   - `None` + `ConsumeSuccess` means acknowledge the whole callback.
   - `None` + `ReconsumeLater` means acknowledge no message.
   - `Some(n)` means only indices `0..=n` were successfully handled.
5. Keep public convenience methods (`set_ack_index`, getter) but make their default
   behaviour unambiguous. Validate and clamp an explicit index before use.
6. Update examples and API documentation to require a listener processing a batch to
   set the ack index immediately after each successful message, before it can return a
   retry status.

### Phase 2 — Correct result processing for partial success and retry

File: `rocketmq-client/src/consumer/consumer_impl/consume_message_concurrently_service.rs`

1. Split every completed callback into two lists using the final acknowledgement index:
   - `acknowledged`: contiguous successful prefix;
   - `pending`: failed message and every unprocessed suffix message.
2. Remove only `acknowledged` from `ProcessQueue` and update the offset only to the
   lowest still-retained queue offset. Offset updates must be monotonic and contiguous.
3. For `MessageModel::Clustering`:
   - send only `pending` messages back to the broker;
   - remove a message from `ProcessQueue` only after send-back succeeds;
   - locally reschedule only messages whose send-back failed.
4. For `MessageModel::Broadcasting`:
   - do not silently drop `pending` messages;
   - retain them in `ProcessQueue` and submit them to the client's local delayed retry
     mechanism;
   - use the same bounded retry/terminal-policy component described below.
5. Ensure a local retry receives exactly the retained `pending` vector. It must not
   resubmit already acknowledged prefix messages.
6. Drain/cancel retry work safely during consumer shutdown and rebalance. A dropped
   process queue must not commit unprocessed messages merely because its retry task
   ends.

### Phase 3 — Define and implement terminal failure policy in the library

The client cannot retry malformed messages forever. Add a library-level, documented
policy with these approved defaults:

- retry delay: `100ms`;
- maximum retry attempts after the initial callback: `3`;
- after the final failed attempt in `BROADCASTING` mode: log/emit a bounded terminal
  failure event and drop that one failed message;
- queue ordering: strict. Do not consume, commit, or deliver a later offset while an
  earlier retained offset is waiting to retry.

Expose both defaults in `ConsumerConfig`, with builder setters and validation:

- `broadcast_retry_delay` (default `Duration::from_millis(100)`; must be greater than
  zero);
- `broadcast_max_retries` (default `3`; zero means no retry and terminal handling after
  the initial failure).

Use names consistent with the repository's existing configuration conventions if a
nearby retry setting already exists; do not expose these controls through Ralive-specific
configuration code.

Implement the policy as follows:

1. Track retry attempts per retained message using existing message retry metadata where
   available, or a bounded per-process-queue structure keyed by queue offset.
2. Schedule a failed BROADCASTING suffix after `broadcast_retry_delay`. The retry task
   must start with the lowest retained queue offset and must not submit a later offset
   first.
3. After the configured retry limit, record a bounded terminal-failure event/hook with
   topic, queue, offset, message ID, retry count, and error category. Do not print the
   body by default.
4. After recording terminal failure, remove only that failed message. The next retained
   offset may then proceed. This is the explicitly approved drop point; no earlier
   callback failure may drop an unattempted suffix.
5. Reuse the same strict per-queue sequencing mechanism for CLUSTERING local retries,
   but retain its broker send-back semantics from Phase 2.

This policy belongs in the client library because it owns the callback result and offset
life cycle. Applications can opt into a hook, but must not reimplement batching rules.

### Phase 4 — Remove unsafe shared mutation and serialize per-queue commits

Files:

- `rocketmq-client/src/consumer/consumer_impl/consume_message_concurrently_service.rs`
- minimal supporting types required by the refactor

1. Do not mutate `ConsumeMessageConcurrentlyService` or
   `DefaultMQPushConsumerImpl` through cloned `ArcMut` instances in independent Tokio
   tasks.
2. Separate immutable callback dependencies (listener, configuration snapshot, runtime
   handle) from mutable commit state.
3. Put mutable per-queue state—process-queue removal, retry scheduling, and offset
   updates—behind explicit synchronization. A per-queue async mutex is preferred over
   a global consumer lock so independent queues can still consume concurrently.
4. While holding the per-queue commit lock, calculate the retained lowest offset,
   remove only terminally handled messages, and update the offset store once. Do not
   await a listener callback while holding this lock.
5. Remove the service-wide mutable aliasing required only by the old `ArcMut` design
   from this code path. Document why the new locking establishes the queue-order and
   offset invariants.

### Phase 5 — Fix `ProcessQueue` accounting

File: `rocketmq-client/src/consumer/consumer_impl/process_queue.rs`

1. Count actual successful removals in a local total.
2. Apply a single `fetch_sub(total_removed, ...)` after the removal loop, only when the
   total is non-zero.
3. Subtract each corresponding body length exactly once.
4. Set size to zero only when the retained map is empty; otherwise leave counters equal
   to the map's actual contents.
5. Add defensive debug assertions/tests that counters match the map after every batch
   operation.

## Required Review Checklist

- A listener can express partial batch success without unsafe casts or global mutable
  state.
- BROADCASTING `ReconsumeLater` retains the failed/unprocessed suffix rather than
  logging and deleting it.
- CLUSTERING send-back removes only messages successfully handed back to the broker.
- A later completed chunk cannot advance an offset beyond an earlier retained chunk.
- No concurrent task writes shared `ArcMut` state without synchronization.
- Batch sizes 1, 16, and 32 pass the in-memory suite.
- BROADCASTING retries use the default 100ms delay and stop after three retries unless
  explicit client-library configuration overrides either value.
- A terminally failed message is dropped only after its retry limit and terminal event;
  later offsets remain ordered and are not skipped prematurely.
- The upstream patch is reviewed and merged as a pinned commit before Ralive updates
  `Cargo.toml`/`Cargo.lock`.

## Resolved Decisions

| Decision | Approved behaviour |
| --- | --- |
| BROADCASTING terminal failure | Drop only the individual message after retries are exhausted; emit bounded terminal-failure metadata first. |
| Retry defaults | Three retries after the first failed callback, with a 100ms delay; both values are configurable in the client library. |
| Queue order | Strict per-queue order. A retained earlier offset blocks later offsets from consumption/commit until it succeeds or reaches terminal drop. |
