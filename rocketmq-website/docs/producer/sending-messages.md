---
sidebar_position: 2
title: Sending Messages
---

# Sending Messages

This page covers practical sending patterns using `DefaultMQProducer` and `MQProducer`.

## Message Types

### Basic Message

```rust
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("TopicTest")
    .body("Hello, RocketMQ!")
    .build()?;

producer.send(message).await?;
```

### Message with Tags

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .tags("order_created")
    .build()?;

producer.send(message).await?;
```

### Message with Keys

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .key("order_12345")
    .build()?;

producer.send(message).await?;
```

### Message with Properties

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .raw_property("region", "us-west")?
    .raw_property("priority", "high")?
    .raw_property("source", "mobile_app")?
    .build()?;

producer.send(message).await?;
```

## Send Strategies

### Sequential Sending

```rust
for msg in messages {
    producer.send(msg).await?;
}
```

### Concurrent Sending

```rust
use futures::future::join_all;

let tasks = messages
    .into_iter()
    .map(|msg| producer.send(msg))
    .collect::<Vec<_>>();

let results = join_all(tasks).await;
```

### Delayed Sending

```rust
let message = Message::builder()
    .topic("DelayedTopic")
    .body(body)
    .delay_level(3) // 1=1s, 2=5s, 3=10s ...
    .build()?;

producer.send(message).await?;
```

## Message Size Management

### Large Messages

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("my_group")
    .name_server_addr("localhost:9876")
    .compress_msg_body_over_howmuch(4 * 1024)
    .build();

let large_body = vec![0u8; 5 * 1024 * 1024];
let message = Message::builder()
    .topic("TopicTest")
    .body(large_body)
    .build()?;

producer.send(message).await?;
```

### Splitting Large Messages

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;

async fn split_and_send(
    producer: &mut DefaultMQProducer,
    topic: &str,
    data: Vec<u8>,
    chunk_size: usize,
) -> rocketmq_error::RocketMQResult<()> {
    let chunks: Vec<_> = data.chunks(chunk_size).collect();
    let total = chunks.len();

    for (i, chunk) in chunks.iter().enumerate() {
        let message = Message::builder()
            .topic(topic)
            .body_slice(chunk)
            .key(format!("chunk-{i}-{total}"))
            .build()?;

        producer.send(message).await?;
    }

    Ok(())
}
```

## Error Handling

### Retry on Failure

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::send_result::SendResult;

async fn send_with_retry(
    producer: &mut DefaultMQProducer,
    message: Message,
    max_retries: u32,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    let mut retry_count = 0;

    loop {
        match producer.send(message.clone()).await {
            Ok(result) => return Ok(result),
            Err(e) if retry_count < max_retries => {
                retry_count += 1;
                tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
                eprintln!("retry {} after error: {}", retry_count, e);
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Fallback Topic

```rust
async fn send_with_fallback(
    producer: &mut DefaultMQProducer,
    message: Message,
    fallback_topic: &str,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    match producer.send(message.clone()).await {
        Ok(result) => Ok(result),
        Err(_) => {
            let mut fallback_message = message;
            fallback_message.set_topic(fallback_topic.into());
            producer.send(fallback_message).await
        }
    }
}
```

## Monitoring Sends

```rust
let mut success_count = 0;
let mut failure_count = 0;

for message in messages {
    match producer.send(message).await {
        Ok(_) => success_count += 1,
        Err(e) => {
            failure_count += 1;
            eprintln!("Failed: {}", e);
        }
    }
}

println!("Success: {}, Failed: {}", success_count, failure_count);
```

## Common Use Cases

### Sending Order Events

```rust
async fn send_order_event(
    producer: &mut DefaultMQProducer,
    order_id: &str,
    event_type: &str,
    order_data: &Order,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    let body = serde_json::to_vec(order_data)?;

    let message = Message::builder()
        .topic("OrderEvents")
        .body(body)
        .tags(event_type)
        .key(order_id)
        .raw_property("event_type", event_type)?
        .raw_property("timestamp", chrono::Utc::now().to_rfc3339())?
        .build()?;

    producer.send(message).await
}
```

### Sending Logs

```rust
async fn send_log(
    producer: &mut DefaultMQProducer,
    level: &str,
    message_text: &str,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    let message = Message::builder()
        .topic("Logs")
        .body(message_text)
        .tags(level)
        .build()?;

    producer.send(message).await
}
```

## Best Practices

1. Set meaningful keys to simplify tracing and deduplication.
2. Use tags for coarse filtering and properties for rich metadata.
3. Apply retry with bounded attempts and visible logging.
4. Track send success rate and latency continuously.
5. Use batch sending when throughput is more important than per-message latency.
6. Validate payload size before send.
7. Keep producer-side schemas stable and versioned.

## Next Steps

- [Transaction Messages](./transaction-messages) - Implement transactional messaging
- [Client Configuration](../configuration/client-config) - Configure producer settings
- [Consumer Guide](../consumer/overview) - Learn about consuming messages
