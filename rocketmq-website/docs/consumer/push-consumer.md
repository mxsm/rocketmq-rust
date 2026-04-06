---
sidebar_position: 2
title: Push Consumer
---

# Push Consumer

Push consumption in RocketMQ-Rust is implemented by `DefaultMQPushConsumer`. The client pulls messages in the background and dispatches them to your listener callbacks.

## Creating a Push Consumer

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("my_consumer_group")
        .name_server_addr("localhost:9876")
        .consume_thread_min(2)
        .consume_thread_max(20)
        .build();

    consumer.subscribe("TopicTest", "*").await?;
    consumer.start().await?;

    Ok(())
}
```

## Message Listeners

### Concurrent Message Listener

```rust
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;

struct MyListener;

impl MessageListenerConcurrently for MyListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for msg in messages {
            println!("Processing: {:?}", msg.msg_id());
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

consumer.register_message_listener_concurrently(MyListener);
```

### Ordered Message Listener

```rust
use rocketmq_client_rust::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use rocketmq_client_rust::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;

struct OrderListener;

impl MessageListenerOrderly for OrderListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> RocketMQResult<ConsumeOrderlyStatus> {
        for msg in messages {
            process_in_order(msg);
        }
        context.set_auto_commit(true);
        Ok(ConsumeOrderlyStatus::Success)
    }
}

consumer.register_message_listener_orderly(OrderListener);
```

## Subscription Patterns

### Single Topic

```rust
consumer.subscribe("TopicTest", "*").await?;
```

### Multiple Topics

```rust
consumer.subscribe("TopicA", "*").await?;
consumer.subscribe("TopicB", "tag1 || tag2").await?;
```

### Message Selectors

```rust
use rocketmq_client_rust::consumer::message_selector::MessageSelector;

let selector = MessageSelector::by_sql("amount > 100 AND region = 'us-west'");
consumer
    .subscribe_with_selector("OrderEvents", Some(selector))
    .await?;
```

## Concurrency Configuration

```rust
let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(2)
    .consume_thread_max(20)
    .pull_batch_size(32)
    .pull_interval(0)
    .pull_threshold_for_queue(1024)
    .pull_threshold_for_topic(10_000)
    .build();
```

## Offset Position

```rust
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset)
    .build();
```

## Pause and Resume

```rust
consumer.suspend().await;
// ...
consumer.resume().await;
```

## Best Practices

1. Size consume threads according to your business handler complexity.
2. Use concurrent listener for throughput, orderly listener for strict ordering.
3. Keep listener logic idempotent to handle retries.
4. Tune `pull_batch_size` and pull thresholds under real load.
5. Prefer server-side filtering to reduce unnecessary network transfer.

## Next Steps

- [Pull Consumer](./pull-consumer) - Learn about pull consumer
- [Message Filtering](./message-filtering) - Advanced filtering techniques
- [Client Configuration](../configuration/client-config) - Consumer configuration options
