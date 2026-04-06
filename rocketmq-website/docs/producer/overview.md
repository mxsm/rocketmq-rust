---
sidebar_position: 1
title: Producer Overview
---

# Producer Overview

RocketMQ-Rust producers are built with `DefaultMQProducer` and the `MQProducer` trait. They support synchronous, asynchronous, one-way, selector-based, batch, and transactional send patterns.

## Creating a Producer

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut producer = DefaultMQProducer::builder()
        .producer_group("my_producer_group")
        .name_server_addr("localhost:9876")
        .build();

    producer.start().await?;
    // Use producer...
    producer.shutdown().await;
    Ok(())
}
```

## Producer Configuration

### Basic Configuration

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("producer_group")
    .name_server_addr("localhost:9876")
    .send_msg_timeout(3_000)
    .retry_times_when_send_failed(2)
    .max_message_size(4 * 1024 * 1024)
    .compress_msg_body_over_howmuch(4 * 1024)
    .build();
```

### Advanced Configuration

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("producer_group")
    .name_server_addr("localhost:9876")
    .retry_times_when_send_failed(3)
    .retry_times_when_send_async_failed(3)
    .retry_another_broker_when_not_store_ok(true)
    .send_msg_max_timeout_per_request(5_000)
    .batch_max_delay_ms(10)
    .batch_max_bytes(512 * 1024)
    .total_batch_max_bytes(4 * 1024 * 1024)
    .enable_backpressure_for_async_mode(true)
    .back_pressure_for_async_send_num(10_000)
    .back_pressure_for_async_send_size(64 * 1024 * 1024)
    .build();
```

## Message Sending Modes

### Synchronous Send

```rust
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("TopicTest")
    .tags("TagA")
    .body("Hello")
    .build()?;

let result = producer.send(message).await?;
println!("Send result: {:?}", result);
```

### Asynchronous Send

```rust
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("TopicTest")
    .body("Hello")
    .build()?;

producer
    .send_with_callback(message, |result, error| {
        if let Some(send_result) = result {
            println!("Message sent: {:?}", send_result);
        }
        if let Some(err) = error {
            eprintln!("Send failed: {}", err);
        }
    })
    .await?;
```

### One-way Send

```rust
let message = Message::builder().topic("TopicTest").body("Fire and forget").build()?;
producer.send_oneway(message).await?;
```

## Queue Selection

Use selector functions to route related messages to the same queue.

```rust
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder().topic("OrderEvents").body("order-123").build()?;
let order_id = 123_i64;

producer
    .send_with_selector(
        message,
        |queues: &[MessageQueue], _msg: &Message, id: &i64| {
            let index = (*id % queues.len() as i64) as usize;
            queues.get(index).cloned()
        },
        order_id,
    )
    .await?;
```

## Error Handling

```rust
match producer.send(message).await {
    Ok(result) => println!("Sent: {:?}", result),
    Err(e) => {
        // Returned after retries are exhausted.
        eprintln!("Send failed: {}", e);
    }
}
```

## Performance Tuning

### Batch Sending

```rust
let messages = vec![
    Message::builder().topic("TopicTest").body("Msg1").build()?,
    Message::builder().topic("TopicTest").body("Msg2").build()?,
    Message::builder().topic("TopicTest").body("Msg3").build()?,
];

producer.send_batch(messages).await?;
```

### Compression

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("producer_group")
    .name_server_addr("localhost:9876")
    .compress_msg_body_over_howmuch(4 * 1024)
    .build();
```

## Monitoring

RocketMQ-Rust does not expose a single `get_stats()` facade on `DefaultMQProducer`. In production, use:

- Send results and callback outcomes.
- Structured logs for latency and failures.
- Application metrics (for example counters/histograms around `send*` calls).

## Best Practices

1. Use dedicated producer groups per service boundary.
2. Set retry and timeout values per business SLA.
3. Use callback-based sending for high throughput paths.
4. Keep message payloads small and compress large bodies.
5. Use queue selectors for ordered business keys.
6. Add idempotency on the consumer side for at-least-once delivery.

## Next Steps

- [Sending Messages](./sending-messages) - Advanced message sending techniques
- [Transaction Messages](./transaction-messages) - Implement transactional messaging
- [Client Configuration](../configuration/client-config) - Detailed configuration options
