---
sidebar_position: 1
title: Producer Overview
---

# Producer Overview

Producers are responsible for sending messages to RocketMQ brokers. RocketMQ-Rust provides a powerful, async producer implementation with advanced features.

## Creating a Producer

```rust
use rocketmq::producer::Producer;
use rocketmq::conf::ProducerOption;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create producer configuration
    let mut producer_option = ProducerOption::default();
    producer_option.set_name_server_addr("localhost:9876");
    producer_option.set_group_name("my_producer_group");

    // Create and start producer
    let producer = Producer::new(producer_option);
    producer.start().await?;

    // Use producer...

    Ok(())
}
```

## Producer Configuration

### Basic Configuration

```rust
let mut producer_option = ProducerOption::default();

// Required
producer_option.set_name_server_addr("localhost:9876");
producer_option.set_group_name("producer_group");

// Optional
producer_option.set_send_msg_timeout(3000); // milliseconds
producer_option.set_retry_times_when_send_failed(2);
producer_option.set_max_message_size(4 * 1024 * 1024); // 4MB
producer_option.set_compress_msg_body_over_threshold(4 * 1024); // 4KB
```

### Advanced Configuration

```rust
// Enable message compression
producer_option.set_compress_msg_body_over_threshold(4 * 1024);

// Set retry policy
producer_option.set_retry_times_when_send_failed(3);
producer_option.set_retry_next_server(true);

// Configure TCP settings
producer_option.set_tcp_transport_try_lock_timeout(1000);
producer_option.set_tcp_transport_connect_timeout(3000);
```

## Message Sending Modes

### Synchronous Send

Blocks until the broker acknowledges:

```rust
use rocketmq::model::Message;

let message = Message::new("TopicTest".to_string(), b"Hello".to_vec());
let result = producer.send(message).await?;

println!("Send result: {:?}", result);
```

### Asynchronous Send

Non-blocking, returns immediately:

```rust
use rocketmq::model::Message;

let message = Message::new("TopicTest".to_string(), b"Hello".to_vec());

producer.send_async(message, |result| {
    match result {
        Ok(send_result) => println!("Message sent: {:?}", send_result),
        Err(e) => println!("Send failed: {:?}", e),
    }
}).await?;
```

### One-way Send

Fire-and-forget, no result:

```rust
let message = Message::new("TopicTest".to_string(), b"Hello".to_vec());

producer.send_oneway(message).await?;
// No result, best effort delivery
```

## Queue Selection

### Automatic Load Balancing

By default, messages are distributed across queues:

```
Round-robin queue selection:

Message 1 → Queue 0
Message 2 → Queue 1
Message 3 → Queue 2
Message 4 → Queue 3
Message 5 → Queue 0
...
```

### Custom Queue Selector

Route specific messages to specific queues:

```rust
use rocketmq::selector::MessageQueueSelector;

struct OrderIdSelector;

impl MessageQueueSelector for OrderIdSelector {
    fn select(
        &self,
        queues: &[MessageQueue],
        message: &Message,
        arg: &str,
    ) -> &MessageQueue {
        // Route by order_id to maintain ordering
        let hash = compute_hash(arg);
        let index = (hash % queues.len() as u64) as usize;
        &queues[index]
    }
}

// Send with selector
let selector = OrderIdSelector;
let order_id = "order_12345";
producer.send_with_selector(message, selector, order_id).await?;
```

## Error Handling

### Automatic Retry

RocketMQ-Rust automatically retries failed sends:

```rust
// Configure retry behavior
producer_option.set_retry_times_when_send_failed(3);
producer_option.set_retry_next_server(true);

// Automatic retry on failure
match producer.send(message).await {
    Ok(result) => println!("Sent: {:?}", result),
    Err(e) => {
        // Only returned after all retries exhausted
        eprintln!("Failed after retries: {:?}", e);
    }
}
```

### Handling Errors

```rust
use rocketmq::error::MQClientError;

match producer.send(message).await {
    Ok(result) => {
        println!("Message sent successfully");
        println!("Message ID: {}", result.msg_id);
        println!("Queue: {}", result.message_queue);
    }
    Err(MQClientError::BrokerNotFound) => {
        eprintln!("No broker available for topic");
    }
    Err(MQClientError::ServiceNotAvailable) => {
        eprintln!("Broker service not available");
    }
    Err(e) => {
        eprintln!("Send failed: {:?}", e);
    }
}
```

## Performance Tuning

### Batch Sending

Send multiple messages in one request:

```rust
let messages = vec![
    Message::new("TopicTest".to_string(), b"Msg1".to_vec()),
    Message::new("TopicTest".to_string(), b"Msg2".to_vec()),
    Message::new("TopicTest".to_string(), b"Msg3".to_vec()),
];

producer.send_batch(messages).await?;
```

### Message Compression

Automatically compress large messages:

```rust
// Compress messages larger than 4KB
producer_option.set_compress_msg_body_over_threshold(4 * 1024);

// Compression is automatic for large messages
let large_body = vec![0u8; 100 * 1024]; // 100KB
let message = Message::new("TopicTest".to_string(), large_body);
producer.send(message).await?; // Compressed automatically
```

### Connection Pooling

```rust
// Configure connection pool
producer_option.set_client_channel_max_idle_time_seconds(120);
producer_option.set_client_channel_expire_timeout_seconds(180);
```

## Monitoring

### Send Statistics

```rust
// Producer tracks statistics automatically
let stats = producer.get_stats()?;
println!("Messages sent: {}", stats.send_success_count);
println!("Messages failed: {}", stats.send_failure_count);
println!("Average RT: {} ms", stats.average_rt);
```

### Metrics

RocketMQ-Rust exposes metrics for monitoring:

- `rocketmq_producer_send_success_total`: Total successful sends
- `rocketmq_producer_send_failure_total`: Total failed sends
- `rocketmq_producer_send_latency_ms`: Send latency in milliseconds
- `rocketmq_producer_queue_size`: Current queue size

## Best Practices

1. **Use producer groups**: Group related producers
2. **Handle errors gracefully**: Implement proper error handling
3. **Monitor performance**: Track send success rate and latency
4. **Use appropriate send mode**: Choose sync/async/one-way based on needs
5. **Implement idempotency**: Handle potential duplicate sends
6. **Configure retries**: Set appropriate retry count and timeout
7. **Use batching**: Batch messages for higher throughput
8. **Set appropriate timeouts**: Balance between reliability and performance

## Next Steps

- [Sending Messages](./sending-messages) - Advanced message sending techniques
- [Transaction Messages](./transaction-messages) - Implement transactional messaging
- [Configuration](../category/configuration) - Detailed configuration options
