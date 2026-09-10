---
title: "Classic Pull compatibility"
---

# Classic Pull compatibility

`DefaultMQPullConsumer` is deprecated for new development, but it has a functional runtime-backed compatibility path. Use [LitePull](./pull-consumer.md) for new polling applications. Keep Classic Pull when migrating code that explicitly selects a queue and offset for each request.

## Runnable and detached construction

| Construction | Runtime behavior |
| --- | --- |
| `DefaultMQPullConsumer::builder(client_runtime).consumer_group(...).build()?` | Builds a runnable facade using the supplied shared client runtime |
| `new()`, `default()`, `with_consumer_group(...)` | Retains a detached compatibility value; operational methods return initialization errors |
| Detached implementation marker | Does not create a fallback runtime |

Changing only the group name on a detached value does not attach a runtime. Replace construction at the application boundary. The builder uses LitePull infrastructure in Classic manual mode with automatic commit disabled; it does not turn explicit pulls into normal background LitePull polling.

The builder requires a group and validates its durations. By default, ordinary pull timeout is 10 seconds, Broker suspension is 20 seconds, and the client timeout for suspended requests is 30 seconds. Long-poll client timeout must exceed Broker suspension.

## Perform one explicitly assigned pull

The function below is a compatibility excerpt with complete facade cleanup. The caller supplies a queue it owns and the next offset it intends to read, and keeps the shared runtime alive. It returns messages to the caller without advancing business progress.

```rust
use std::sync::Arc;
use rocketmq_client_rust::{
    ClientResult, ClientRuntime, DefaultMQPullConsumer,
    MessageSelector, PullOptions, PullResult,
};
use rocketmq_model::common::message::message_queue::MessageQueue;

#[allow(deprecated)]
async fn pull_once(
    runtime: Arc<ClientRuntime>,
    queue: MessageQueue,
    next_offset: i64,
) -> ClientResult<PullResult> {
    let consumer = DefaultMQPullConsumer::builder(runtime)
        .consumer_group("docs_classic_group")
        .name_server_addr("127.0.0.1:9876")
        .build()?;
    let result = async {
        consumer.start().await?;
        let options = PullOptions::new(
            queue, MessageSelector::by_tag("*"), next_offset, 16,
        )?;
        consumer.pull_with_options(options).await
    }.await;
    let shutdown = consumer.shutdown().await;
    let result = result?;
    shutdown?;
    Ok(result)
}
```

A long-running application should reuse one started facade and shut it down after its loop; creating a consumer per pull is not the intended throughput pattern. An error in the pull still executes shutdown here. The example does not claim automatic exclusive ownership of the supplied queue.

## Interpret results before moving the cursor

| `PullStatus` | Meaning and next action |
| --- | --- |
| `Found` | Process the returned messages; use `next_begin_offset` only after the intended batch succeeds |
| `NoNewMsg` | No new eligible data for this request; use bounded waiting or long polling |
| `NoMatchedMsg` | Data was scanned but did not match; the returned next offset can skip scanned non-matching positions |
| `OffsetIllegal` | Requested position is outside the valid range; investigate retention/reset and the returned min/max/next positions |

Do not increment an offset by the number of messages received: filtering and gaps can make that wrong. Do not treat `OffsetIllegal` as permission to silently discard a business recovery range.

`PullOptions` validates the queue topic/Broker name, non-negative offset, positive message count and response-size bound, and valid timeouts. Enabling block-if-not-found on its ordinary defaults also requires increasing the client timeout beyond the suspension timeout. The facade's dedicated block-if-not-found methods use the corresponding builder settings.

## Queue assignment and progress ownership

`fetch_subscribe_message_queues` returns known queues; it does not mean this process exclusively owns all of them. Registered `MessageQueueListener` callbacks distinguish all queues from those divided to this consumer. Apply that assignment or a deliberate external ownership policy before pulling.

The facade offers `update_consume_offset`, `fetch_consume_offset`, `min_offset`, `max_offset`, and `search_offset`. Offset update and remote persistence must be interpreted through the underlying offset-store path. A pull result alone never commits business progress.

Complete business work before advancing the next-read position. Keep a durable business identity for replay, and stop pulling a revoked queue. The wrapper's state machine rejects repeated start, restart after failed startup, and restart after shutdown; create a new facade when restarting. Shutdown itself is idempotent.

## Migrate to LitePull intentionally

| Classic responsibility | LitePull decision |
| --- | --- |
| Explicit per-request queue and offset | Choose subscription-based assignment or explicit `assign`, then `seek` only for a deliberate position change |
| Manual pull loop | Replace with bounded `poll`/zero-copy poll and processing |
| Per-request selector | Configure equivalent topic subscription/selector before polling |
| Application-owned progress | Set auto-commit deliberately and preserve processing-before-progress order |
| Queue ownership callback | Preserve assignment/revocation behavior in the chosen mode |

Do not run old and new consumers concurrently against the same group's progress without planning the handover. Record the last completed business position, stop the old owner, start the new mode, and inspect duplicates/gaps and group progress. A builder substitution alone is not an offset migration.

Current LitePull `commit_all` updates client offset-store state and can internally log per-queue errors; it is not an immediate durable all-queue commit. Preserve this distinction during migration.

Sources: [Classic facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_pull_consumer.rs), [builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_pull_consumer_builder.rs), [lifecycle and assignment adapter](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_mq_pull_consumer_impl.rs), [pull results](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/pull_result.rs).
