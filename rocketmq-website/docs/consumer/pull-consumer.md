---
title: "LitePull: poll, process and commit"
---

LitePull gives the application an explicit polling loop while the client manages route discovery, pulling and subscribed queue assignment. This page keeps the existing `pull-consumer` URL and describes `DefaultLitePullConsumer`; Classic Pull compatibility is a separate API.

Run [quick start](../getting-started/quick-start.md) for the complete application. The fragments below use the same `DocsFirstMessage` Topic and `docs_first_message_consumer` Group.

## Construct with the application's runtime

Inside the owning runtime, with an existing `Arc<ClientRuntime>` named `client`:

```rust
let consumer = DefaultLitePullConsumer::builder(client)
    .consumer_group("docs_first_message_consumer")
    .name_server_addr("127.0.0.1:9876")
    .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
    .auto_commit(false)
    .poll_timeout_millis(1_000)
    .build()?;
consumer.subscribe("DocsFirstMessage").await?;
consumer.start().await?;
```

Use the public client exports and the Model `ConsumeFromWhere` type, as shown in the [complete source](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs). The LitePull builder is fallible. Startup and subscription failures must not bypass cleanup of the application-owned runtime.

`subscribe(topic)` selects all messages. Use `subscribe_with_expression(topic, expression)` or `subscribe_with_selector` when an expression is needed. Match the Broker's filtering capabilities and keep subscriptions consistent within the group.

Subscription mode lets the client assign queues. Explicit assignment is a separate control path; do not combine assignment and subscription as though they were independent filters.

## Poll and finish the batch

```rust
let messages = consumer.poll_with_timeout(1_000).await;
for message in &messages {
    println!("RECEIVED id={}", message.msg_id());
}
if !messages.is_empty() {
    consumer.commit_all().await?;
}
```

The ordinary polling call returns a vector of message values. An empty vector may mean no eligible data, a timeout or client conditions that need diagnosis; it is not a structured network error report. A production loop should also observe client diagnostics and assignment instead of interpreting every empty poll as an empty Topic.

The example's completed work is printing message IDs. Replace that work with successful application processing. Keep failures from falling through to an unconditional batch commit. If processing uses concurrent workers, track completion per queue and advance only a contiguous completed prefix; a later successful message cannot justify skipping an earlier failed one.

`poll_with_timeout_zero_copy` returns owned `Arc<MessageExt>` values. Retained values keep their message data alive. Bound both application batches and downstream concurrency, regardless of which polling variant you select.

## What commit actually means

In the current implementation, `commit_all` updates assigned queue offsets in the client's offset store; it does **not** perform immediate remote persistence. Some per-queue failures are logged inside the implementation instead of being returned as a failed outer result. A returned `Ok(())` is therefore not proof that every queue progressed or that the Broker durably saved the position.

Periodic persistence and the orderly shutdown path perform separate offset-store work. The current `commit_sync` facade delegates to the commit path as well; its name is not a promise of a synchronous durable Broker acknowledgement. Map/set operations accept explicit persistence choices, but their return and logging behavior must be understood before using them as a correctness boundary.

Keep these positions separate:

| Position | Meaning |
| --- | --- |
| Pulled/available data | Data obtained by client background work |
| Application completion | Work actually finished by your code |
| Client committed offset | The position submitted to the client's offset store |
| Persisted group offset | Progress available through the applicable offset persistence path |

An offset represents progress within one queue. Application completion and persisted progress can diverge after a crash. The safe response is to tolerate replay through business idempotency, not to describe offset commit as an external transaction.

## Start position and repetition

`ConsumeFromFirstOffset` is an initial-position policy for a group without applicable stored progress. It does not rewind an existing group. In the tutorial, keep the same group and send new messages for a normal repeat run.

A manual offset reset changes what can be replayed or skipped and affects group members. It is a maintenance operation with business consequences, not a normal fix for an empty poll. First inspect routes, assignment, subscription expressions and current progress.

## Stop without losing lifecycle ownership

The example waits for five messages, a 60-second deadline or Ctrl+C. Each path reaches `consumer.shutdown().await`, then closes the shared client runtime, runtime owner and telemetry. Keep application workers under an owner as well; shutting down the consumer cannot finish business tasks that the application detached elsewhere.

Read [delivery and retry](../guides/delivery-and-retry.md) for duplicates and failure windows, or [first diagnosis](../operations/first-diagnosis.md) when polling does not produce expected data.

Sources: [public LitePull facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_lite_pull_consumer.rs), [commit and shutdown implementation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_lite_pull_consumer_impl.rs), [remote offset store](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/store/remote_broker_offset_store.rs).
