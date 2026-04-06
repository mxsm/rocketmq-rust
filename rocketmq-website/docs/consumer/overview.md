---
sidebar_position: 1
title: Consumer Overview
---

# Consumer Overview

RocketMQ-Rust provides two consumer styles:

- `DefaultMQPushConsumer` for callback-driven processing.
- `DefaultLitePullConsumer` for polling-driven processing.

## Consumer Types

### Push Consumer

Push consumer is ideal for event-driven services.

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("push_group")
    .name_server_addr("localhost:9876")
    .build();

consumer.subscribe("TopicTest", "*").await?;
consumer.start().await?;
```

### Pull Consumer

Pull consumer is ideal for custom batching and replay workflows.

```rust
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let consumer = DefaultLitePullConsumer::builder()
    .consumer_group("pull_group")
    .name_server_addr("localhost:9876")
    .auto_commit(true)
    .build();

consumer.start().await?;
consumer.subscribe("TopicTest").await?;

loop {
    let messages = consumer.poll_with_timeout(1_000).await;
    for msg in messages {
        process_message(&msg);
    }
}
```

## Creating a Push Consumer

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
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
            println!("Received message: {:?}", msg.msg_id());
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("my_consumer_group")
        .name_server_addr("localhost:9876")
        .consume_thread_min(2)
        .consume_thread_max(10)
        .build();

    consumer.subscribe("TopicTest", "*").await?;
    consumer.register_message_listener_concurrently(MyListener);
    consumer.start().await?;

    let _ = tokio::signal::ctrl_c().await;
    consumer.shutdown().await;
    Ok(())
}
```

## Consumer Configuration

### Push Consumer Configuration

```rust
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(2)
    .consume_thread_max(20)
    .pull_batch_size(32)
    .pull_interval(0)
    .consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset)
    .message_model(MessageModel::Clustering)
    .max_reconsume_times(3)
    .build();
```

### Lite Pull Consumer Configuration

```rust
let consumer = DefaultLitePullConsumer::builder()
    .consumer_group("my_pull_group")
    .name_server_addr("localhost:9876")
    .pull_batch_size(32)
    .pull_threshold_for_queue(1_000)
    .pull_threshold_for_all(10_000)
    .auto_commit(false)
    .auto_commit_interval_millis(5_000)
    .build();
```

## Message Filtering

### Tag Filtering

```rust
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;
```

### SQL Filtering

```rust
use rocketmq_client_rust::consumer::message_selector::MessageSelector;

let selector = MessageSelector::by_sql("region = 'us-west' AND amount > 100");
consumer
    .subscribe_with_selector("OrderEvents", Some(selector))
    .await?;
```

## Retry Handling

```rust
impl MessageListenerConcurrently for MyListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for msg in messages {
            if msg.reconsume_times() >= 3 {
                eprintln!("Max retries exceeded: {:?}", msg.msg_id());
                continue;
            }

            if let Err(e) = process_message_safe(msg) {
                eprintln!("Process failed: {:?}", e);
                return Ok(ConsumeConcurrentlyStatus::ReconsumeLater);
            }
        }

        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}
```

## Performance Tuning

```rust
let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("perf_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(4)
    .consume_thread_max(32)
    .pull_batch_size(64)
    .pull_threshold_for_queue(2_000)
    .pull_threshold_for_topic(20_000)
    .build();
```

## Best Practices

1. Use push consumer for online event processing, pull consumer for controlled replay and batching.
2. Keep listener logic idempotent to handle at-least-once delivery semantics.
3. Tune thread counts and pull thresholds based on production traffic.
4. Use server-side filtering to reduce useless message transfer.
5. Set clear retry limits and add dead-letter handling paths.

## Next Steps

- [Push Consumer](./push-consumer) - Deep dive into push consumer
- [Pull Consumer](./pull-consumer) - Deep dive into pull consumer
- [Message Filtering](./message-filtering) - Advanced filtering techniques
