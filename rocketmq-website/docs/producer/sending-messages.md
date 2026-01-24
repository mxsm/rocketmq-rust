---
sidebar_position: 2
title: Sending Messages
---

# Sending Messages

Learn advanced techniques for sending messages with RocketMQ-Rust producers.

## Message Types

### Basic Message

```rust
use rocketmq::model::Message;

let message = Message::new("TopicTest".to_string(), b"Hello, RocketMQ!".to_vec());
producer.send(message).await?;
```

### Message with Tags

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.set_tags("order_created");
producer.send(message).await?;
```

### Message with Keys

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.set_keys("order_12345");
producer.send(message).await?;
```

### Message with Properties

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.put_property("region", "us-west");
message.put_property("priority", "high");
message.put_property("source", "mobile_app");
producer.send(message).await?;
```

## Send Strategies

### Sequential Sending

Send messages in order:

```rust
let messages = vec![
    create_message("Step 1"),
    create_message("Step 2"),
    create_message("Step 3"),
];

for msg in messages {
    producer.send(msg).await?;
}
```

### Concurrent Sending

Send messages concurrently:

```rust
use futures::future::join_all;

let send_futures = messages.into_iter()
    .map(|msg| producer.send(msg))
    .collect::<Vec<_>>();

let results = join_all(send_futures).await;
```

### Delayed Sending

Schedule messages for future delivery:

```rust
let mut message = Message::new("DelayedTopic".to_string(), body);
// Delay level 1 = 1s, 2 = 5s, 3 = 10s, etc.
message.set_delay_time_level(3);
producer.send(message).await?;
```

## Message Size Management

### Large Messages

For messages larger than 4MB, use compression:

```rust
// Enable compression
producer_option.set_compress_msg_body_over_threshold(4 * 1024);

// Large message is compressed automatically
let large_body = vec![0u8; 5 * 1024 * 1024]; // 5MB
let message = Message::new("TopicTest".to_string(), large_body);
producer.send(message).await?;
```

### Splitting Large Messages

```rust
fn split_and_send(producer: &Producer, topic: &str, data: Vec<u8>, chunk_size: usize) -> Result<(), Error> {
    let chunks: Vec<_> = data.chunks(chunk_size).collect();
    let total_chunks = chunks.len();

    for (i, chunk) in chunks.iter().enumerate() {
        let mut message = Message::new(topic.to_string(), chunk.to_vec());
        message.put_property("chunk_index", i.to_string());
        message.put_property("total_chunks", total_chunks.to_string());
        message.put_property("message_id", unique_id());

        producer.send(message.clone()).await?;
    }

    Ok(())
}
```

## Error Handling

### Retry on Failure

```rust
async fn send_with_retry(
    producer: &Producer,
    message: Message,
    max_retries: u32,
) -> Result<SendResult, Error> {
    let mut retry_count = 0;

    loop {
        match producer.send(message.clone()).await {
            Ok(result) => return Ok(result),
            Err(e) if retry_count < max_retries => {
                retry_count += 1;
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Fallback to Backup Queue

```rust
async fn send_with_fallback(
    producer: &Producer,
    message: Message,
    fallback_topic: &str,
) -> Result<SendResult, Error> {
    match producer.send(message.clone()).await {
        Ok(result) => Ok(result),
        Err(_) => {
            let mut fallback_message = message.clone();
            fallback_message.set_topic(fallback_topic.to_string());
            fallback_message.put_property("original_topic", message.get_topic());
            producer.send(fallback_message).await
        }
    }
}
```

## Monitoring Sends

### Track Send Results

```rust
let mut success_count = 0;
let mut failure_count = 0;

for message in messages {
    match producer.send(message).await {
        Ok(result) => {
            success_count += 1;
            println!("Sent: {}, offset: {}", result.msg_id, result.queue_offset);
        }
        Err(e) => {
            failure_count += 1;
            eprintln!("Failed: {:?}", e);
        }
    }
}

println!("Success: {}, Failed: {}", success_count, failure_count);
```

### Performance Monitoring

```rust
use std::time::Instant;

let start = Instant::now();

for message in messages {
    producer.send(message).await?;
}

let elapsed = start.elapsed();
let throughput = messages.len() as f64 / elapsed.as_secs_f64();

println!("Sent {} messages in {:.2}s", messages.len(), elapsed.as_secs_f64());
println!("Throughput: {:.2} msg/s", throughput);
```

## Common Use Cases

### Sending Order Events

```rust
async fn send_order_event(
    producer: &Producer,
    order_id: &str,
    event_type: &str,
    order_data: &Order,
) -> Result<SendResult, Error> {
    let body = serde_json::to_vec(order_data)?;

    let mut message = Message::new("OrderEvents".to_string(), body);
    message.set_tags(event_type);
    message.set_keys(order_id);
    message.put_property("event_type", event_type);
    message.put_property("timestamp", Utc::now().to_rfc3339());

    producer.send(message).await
}

// Usage
let order = Order { id: "12345", amount: 99.99 };
send_order_event(&producer, "12345", "order_created", &order).await?;
```

### Sending Metrics

```rust
async fn send_metric(
    producer: &Producer,
    metric_name: &str,
    value: f64,
    tags: Vec<(&str, &str)>,
) -> Result<SendResult, Error> {
    let metric_data = MetricData {
        name: metric_name.to_string(),
        value,
        timestamp: Utc::now(),
        tags,
    };

    let body = serde_json::to_vec(&metric_data)?;
    let message = Message::new("Metrics".to_string(), body);

    producer.send(message).await
}
```

### Sending Logs

```rust
async fn send_log(
    producer: &Producer,
    level: &str,
    message: &str,
    context: HashMap<String, String>,
) -> Result<SendResult, Error> {
    let log_entry = LogEntry {
        level: level.to_string(),
        message: message.to_string(),
        timestamp: Utc::now(),
        context,
    };

    let body = serde_json::to_vec(&log_entry)?;
    let mut msg = Message::new("Logs".to_string(), body);
    msg.set_tags(level);

    producer.send(msg).await
}
```

## Best Practices

1. **Set meaningful keys**: Enable message tracking and querying
2. **Use appropriate tags**: Facilitate message filtering
3. **Handle failures gracefully**: Implement retry logic
4. **Monitor performance**: Track success rates and latency
5. **Batch when possible**: Use batch sending for high throughput
6. **Validate message size**: Check size before sending
7. **Use properties**: Store metadata in properties, not body
8. **Implement idempotency**: Handle potential duplicate sends

## Next Steps

- [Transaction Messages](./transaction-messages) - Implement transactional messaging
- [Configuration](../category/configuration) - Configure producer settings
- [Consumer Guide](../category/consumer) - Learn about consuming messages
