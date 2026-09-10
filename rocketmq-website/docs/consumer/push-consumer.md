---
title: "Push consumers"
---

# Push consumers

`DefaultMQPushConsumer` delivers messages to a registered listener. In ordinary Pull mode, the client performs route discovery, queue assignment and background pulls/long polling, then schedules callbacks. “Push” describes the application interface, not an unconditional Broker-initiated stream.

For a manually polled application, use [LitePull](./pull-consumer.md). For Broker-managed invisibility and receipt acknowledgement, see [POP](./pop.md); its successful listener result follows a different acknowledgement path.

## Start a consumer with a complete lifecycle

Create `DocsFirstMessage` using [quick start](../getting-started/quick-start.md), and repeat its group-creation command with `-g docs_push_group` for this consumer. The following function uses the tutorial's shared `Arc<ClientRuntime>`. The listener counts messages as processed for demonstration; replace that operation with bounded, idempotent business processing before returning success.

```rust
use std::sync::Arc;
use rocketmq_client_rust::{
    ClientResult, ClientRuntime, ConsumeConcurrentlyContext,
    ConsumeConcurrentlyStatus, DefaultMQPushConsumer,
    MessageListenerConcurrently, MQPushConsumer,
};
use rocketmq_model::common::message::message_ext::MessageExt;

struct CountListener;

impl MessageListenerConcurrently for CountListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> ClientResult<ConsumeConcurrentlyStatus> {
        println!("received={}", messages.len());
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

async fn consume_until_interrupt(
    client_runtime: Arc<ClientRuntime>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = DefaultMQPushConsumer::builder(client_runtime)
        .consumer_group("docs_push_group")
        .name_server_addr("127.0.0.1:9876")
        .consume_message_batch_max_size(1)
        .build();
    let outcome = async {
        consumer.subscribe("DocsFirstMessage", "*").await?;
        consumer.register_message_listener_concurrently(CountListener);
        consumer.start().await?;
        tokio::signal::ctrl_c().await?;
        Ok(())
    }.await;
    consumer.shutdown().await;
    outcome
}
```

Invoke the function on the owning RuntimeOwner, then close the shared ClientRuntime, RuntimeOwner, and telemetry. Register a listener and subscription before startup. Publish after the consumer starts when learning the default new-group behavior; an existing stored offset can override the initial-position setting.

## Choose concurrency and consumption model

| Choice | Meaning |
| --- | --- |
| Concurrent listener | Different batches can run concurrently; completion order can differ from queue order |
| Orderly listener | Serializes consumption within the relevant queue ownership/lock boundary |
| Clustering | Group members divide work; retries use the group's configured path |
| Broadcasting | Each instance receives its own copy; the ordinary concurrent implementation logs and drops failed deliveries rather than using clustered send-back retries |

The listener methods are synchronous. The client dispatches them through its managed blocking boundary, but downstream calls still need their own finite timeouts and capacity. Returning success immediately after handing work to an unowned background task can advance progress before the work commits.

Do not mix concurrent and orderly listener contracts or inconsistent subscriptions within one logical group. Group members should agree on topic, selector, consumption model and intended ordering. A second independent business application normally needs its own consumer group.

## Translate listener outcomes into progress

`ConsumeSuccess` marks the successfully processed prefix; `ReconsumeLater` treats the batch as unsuccessful. The default concurrent context acknowledges all messages on success. If partial acknowledgement is used, it is a prefix index, not an arbitrary subset of the batch.

In ordinary clustered concurrent consumption, failed messages go through the send-back path. Failures to send back remain pending for a later local attempt. Completed or successfully handed-off messages can be removed from the process queue, allowing its next safe offset to advance. Offset persistence is a separate step.

Consequently, business completion, listener success, progress update and persistent group progress are different events. Processing must tolerate replay after a crash in between them. Broadcasting requires an explicit application recovery policy because its failure behavior differs.

For orderly consumption, use `MessageListenerOrderly` and its `ConsumeOrderlyStatus`. A failed current queue can suspend and retry; that protects local sequence at the cost of queue progress. See [ordered messages](../guides/ordered-messages.md) for send-side mapping and failure boundaries.

## Rebalance and control memory

Route changes or group membership changes can revoke and assign queues. Treat queue ownership as temporary. Stop work belonging to revoked ownership and keep business idempotency effective across a move to another process.

| Setting family | What it bounds or influences |
| --- | --- |
| `pull_batch_size` | Requested network batch size |
| `consume_message_batch_max_size` | Messages passed to one listener invocation |
| `consume_thread_min` / `consume_thread_max` | Managed consumption concurrency controls, not permission to block indefinitely |
| `pull_threshold_for_queue` / `pull_threshold_size_for_queue` | Buffered queue count/size pressure |
| Topic thresholds and `pull_interval` | Topic-level pressure and pull pacing |
| `consume_from_where` | Initial position when no usable stored progress determines the start |

Network batch size and callback batch size are different. Larger caches can retain message bodies and delay shutdown; increasing threads does not fix a saturated database. Choose limits from the real handler cost and observe lag, pending count, retained bytes and retry rate together.

Suspension pauses intake according to the consumer path; it is not a transaction barrier that proves all callbacks finished. Use explicit shutdown when leaving the service lifecycle.

## Diagnose a running consumer

| Observation | Check next |
| --- | --- |
| No callbacks | Topic route, group configuration, listener registration, starting offset, selector |
| More instances but little additional throughput | Queue count/assignment and downstream bottleneck |
| Repeated delivery | Listener failures, send-back/ACK failures, rebalance and persisted progress |
| Lag rises while callbacks succeed | Handler latency, queue assignment, persistence and which cluster/group the metric describes |
| Broadcast failure disappears | The broadcast branch does not provide clustered retry semantics |

From `rocketmq-example/`, `consumer-cluster` demonstrates concurrent clustering and `consumer-orderly` demonstrates the orderly interface. Inspect their topic/group constants before provisioning and running them. The SQL and Tag examples have their own topics. Use [first diagnosis](../operations/first-diagnosis.md) for the cluster-side commands.

Sources: [Push facade/configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_push_consumer.rs), [Push lifecycle](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_mq_push_consumer_impl.rs), [concurrent processing](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/consume_message_concurrently_service.rs), [orderly processing](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/consume_message_orderly_service.rs).
