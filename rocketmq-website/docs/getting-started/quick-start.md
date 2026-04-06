---
sidebar_position: 2
title: Quick Start
---

# Quick Start

This guide will help you create your first RocketMQ-Rust producer and consumer in just a few minutes.

## Step 1: Start RocketMQ Server

First, ensure you have a RocketMQ server running. If you don't have one, use Docker:

```bash
# Start nameserver
docker run -d -p 9876:9876 --name rmqnamesrv apache/rocketmq:nameserver

# Start broker
docker run -d -p 10911:10911 -p 10909:10909 --name rmqbroker \
  -e "NAMESRV_ADDR=rmqnamesrv:9876" \
  --link rmqnamesrv:rmqnamesrv \
  apache/rocketmq:broker
```

## Step 2: Create a Producer

Create a new Rust project:

```bash
cargo new rocketmq-producer
cd rocketmq-producer
```

Add RocketMQ to your `Cargo.toml`:

```toml
[dependencies]
rocketmq-client-rust = "0.8"
rocketmq-common = "0.8"
rocketmq-error = "0.8"
tokio = { version = "1", features = ["full"] }
```

Create `src/main.rs`:

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut producer = DefaultMQProducer::builder()
        .producer_group("producer_group_1")
        .name_server_addr("localhost:9876")
        .build();

    producer.start().await?;

    let message = Message::builder()
        .topic("TopicTest")
        .tags("TagA")
        .body("Hello, RocketMQ-Rust!")
        .build()?;

    let result = producer.send_with_timeout(message, 3_000).await?;
    println!("Message sent: {:?}", result);

    producer.shutdown().await;
    Ok(())
}
```

## Step 3: Create a Consumer

Create another Rust project:

```bash
cargo new rocketmq-consumer
cd rocketmq-consumer
```

Add dependencies to `Cargo.toml`:

```toml
[dependencies]
rocketmq-client-rust = "0.8"
rocketmq-common = "0.8"
rocketmq-error = "0.8"
tokio = { version = "1", features = ["full"] }
```

Create `src/main.rs`:

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
        for msg in &messages {
            println!("Received message: {:?}", msg);
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("consumer_group_1")
        .name_server_addr("localhost:9876")
        .consume_thread_min(1)
        .consume_thread_max(1)
        .build();

    consumer.subscribe("TopicTest", "*").await?;
    consumer.register_message_listener_concurrently(MyListener);
    consumer.start().await?;

    println!("Consumer started. Press Ctrl+C to exit.");

    let _ = tokio::signal::ctrl_c().await;
    consumer.shutdown().await;

    Ok(())
}
```

## Step 4: Run and Test

1. Start the consumer:

    ```bash
    cargo run
    ```

2. In another terminal, start the producer:

    ```bash
    cargo run
    ```

You should see the consumer receive the message sent by the producer!

## Next Steps

Congratulations! You've created your first RocketMQ-Rust application. Continue learning:

- [Basic Concepts](./basic-concepts) - Learn about topics, messages, and queues
- [Producer Guide](../category/producer) - Advanced producer features
- [Consumer Guide](../category/consumer) - Advanced consumer features

## Common Issues

### Connection Refused

Make sure the RocketMQ server is running:

```bash
docker ps
```

### No Messages Received

Check that:

1. The topic names match between producer and consumer
2. The consumer is started before the producer sends messages
3. The nameserver address is correct

### Build Errors

Ensure you're using a recent version of Rust:

```bash
rustc --version  # Should be 1.70.0 or later
```
