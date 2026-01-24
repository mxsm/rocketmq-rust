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
rocketmq = "0.3"
tokio = { version = "1", features = ["full"] }
```

Create `src/main.rs`:

```rust
use rocketmq::producer::Producer;
use rocketmq::conf::ProducerOption;
use rocketmq::model::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure producer
    let mut producer_option = ProducerOption::default();
    producer_option.set_name_server_addr("localhost:9876");
    producer_option.set_group_name("producer_group_1");

    // Create producer
    let producer = Producer::new(producer_option);

    // Start producer
    producer.start().await?;

    // Create message
    let message = Message::new(
        "TopicTest".to_string(),
        b"Hello, RocketMQ-Rust!".to_vec(),
    );

    // Send message
    let result = producer.send(message).await?;
    println!("Message sent: {:?}", result);

    // Shutdown producer
    producer.shutdown().await?;

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
rocketmq = "0.3"
tokio = { version = "1", features = ["full"] }
```

Create `src/main.rs`:

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;
use rocketmq::listener::MessageListener;

struct MyListener;

impl MessageListener for MyListener {
    fn consume_message(
        &self,
        messages: Vec<rocketmq::model::MessageExt>,
    ) -> rocketmq::error::ConsumeResult {
        for msg in messages {
            println!("Received message: {:?}", msg);
        }
        rocketmq::error::ConsumeResult::Success
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure consumer
    let mut consumer_option = ConsumerOption::default();
    consumer_option.set_name_server_addr("localhost:9876");
    consumer_option.set_group_name("consumer_group_1");
    consumer_option.set_consume_thread_min(1);
    consumer_option.set_consume_thread_max(1);

    // Create consumer
    let consumer = PushConsumer::new(consumer_option);

    // Subscribe to topic
    consumer.subscribe("TopicTest", "*").await?;

    // Register message listener
    consumer.register_message_listener(Box::new(MyListener));

    // Start consumer
    consumer.start().await?;

    println!("Consumer started. Press Ctrl+C to exit.");

    // Keep running
    tokio::signal::ctrl_c().await?;

    // Shutdown consumer
    consumer.shutdown().await?;

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
