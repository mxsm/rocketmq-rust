---
sidebar_position: 1
title: Consumer Overview
---

# Consumer Overview

Consumers receive and process messages from RocketMQ brokers. RocketMQ-Rust provides both push and pull consumer implementations.

## Consumer Types

### Push Consumer

Messages are automatically pushed from the broker to the consumer:

```rust
use rocketmq::consumer::PushConsumer;

let consumer = PushConsumer::new(consumer_option);
consumer.subscribe("TopicTest", "*").await?;
consumer.start().await?;
```

**Benefits:**
- Event-driven architecture
- Automatic message pulling
- Built-in thread pool for concurrent processing
- Automatic offset management

### Pull Consumer

Consumer actively pulls messages from the broker:

```rust
use rocketmq::consumer::PullConsumer;

let consumer = PullConsumer::new(consumer_option);
consumer.start().await?;

loop {
    let messages = consumer.pull("TopicTest", "*", 32).await?;
    for msg in messages {
        process_message(msg);
    }
}
```

**Benefits:**
- Full control over message pulling
- Custom batch size
- Explicit control over processing flow
- Suitable for batch processing

## Creating a Push Consumer

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;
use rocketmq::listener::MessageListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure consumer
    let mut consumer_option = ConsumerOption::default();
    consumer_option.set_name_server_addr("localhost:9876");
    consumer_option.set_group_name("my_consumer_group");
    consumer_option.set_consume_thread_min(1);
    consumer_option.set_consume_thread_max(10);

    // Create consumer
    let consumer = PushConsumer::new(consumer_option);

    // Subscribe to topic
    consumer.subscribe("TopicTest", "*").await?;

    // Register message listener
    consumer.register_message_listener(Box::new(MyListener));

    // Start consumer
    consumer.start().await?;

    // Keep running
    tokio::signal::ctrl_c().await?;

    Ok(())
}
```

## Message Listener

Implement the `MessageListener` trait to handle messages:

```rust
use rocketmq::listener::MessageListener;
use rocketmq::error::ConsumeResult;

struct MyListener;

impl MessageListener for MyListener {
    fn consume_message(
        &self,
        messages: Vec<rocketmq::model::MessageExt>,
    ) -> ConsumeResult {
        for msg in messages {
            match process_message(&msg) {
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("Failed to process message: {:?}", e);
                    return ConsumeResult::SuspendCurrentQueueAMoment;
                }
            }
        }
        ConsumeResult::Success
    }
}

fn process_message(msg: &MessageExt) -> Result<(), Error> {
    println!("Received message: {}", String::from_utf8_lossy(msg.get_body()));
    Ok(())
}
```

## Consumer Configuration

### Basic Configuration

```rust
let mut consumer_option = ConsumerOption::default();

// Required
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

// Thread pool
consumer_option.set_consume_thread_min(2);
consumer_option.set_consume_thread_max(10);

// Message batch size
consumer_option.set_pull_batch_size(32);
consumer_option.set_pull_interval(0);
```

### Advanced Configuration

```rust
// Offset management
consumer_option.set_enable_msg_trace(true);
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// Retry settings
consumer_option.set_max_reconsume_times(3);

// Message model
consumer_option.set_message_model(MessageModel::Clustering);
```

## Message Consumption Models

### Clustering (Default)

Messages are distributed among consumers in a group:

```rust
consumer_option.set_message_model(MessageModel::Clustering);
```

Each message is consumed by only one consumer.

### Broadcasting

Each consumer receives all messages:

```rust
consumer_option.set_message_model(MessageModel::Broadcasting);
```

Every consumer in the group receives all messages.

## Message Filtering

### Tag-based Filtering

```rust
// Subscribe to specific tags
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;

// Subscribe to all tags
consumer.subscribe("OrderEvents", "*").await?;

// Exclude specific tag
consumer.subscribe("OrderEvents", "!(order_cancelled)").await?;
```

### SQL92 Filtering

```rust
// Subscribe using SQL expression
consumer.subscribe(
    "OrderEvents",
    "region = 'us-west' AND amount > 100"
).await?;
```

## Offset Management

### Starting Position

```rust
// Start from latest offset
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// Start from earliest offset
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);

// Start from specific timestamp
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromTimestamp);
consumer_option.set_consume_timestamp("20230101000000");
```

### Manual Offset Commit

```rust
// Disable auto commit
consumer_option.set_enable_auto_commit(false);

// Process messages
for msg in messages {
    process_message(msg);
    consumer.commit_sync(msg.get_queue_offset(), msg.get_queue_id())?;
}
```

## Error Handling

### Consume Results

```rust
impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        for msg in messages {
            match process_message(&msg) {
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    return ConsumeResult::ReconsumeLater;
                }
            }
        }
        ConsumeResult::Success
    }
}
```

### Retry Handling

```rust
// Configure max retry attempts
consumer_option.set_max_reconsume_times(3);

// Check retry count
impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        for msg in messages {
            let retry_count = msg.get_reconsume_times();
            if retry_count >= 3 {
                eprintln!("Max retries exceeded for message: {:?}", msg.get_msg_id());
                // Send to dead letter queue or log
                continue;
            }
            // Process message
        }
        ConsumeResult::Success
    }
}
```

## Performance Tuning

### Thread Pool Configuration

```rust
// Minimum threads (always active)
consumer_option.set_consume_thread_min(2);

// Maximum threads (scale up under load)
consumer_option.set_consume_thread_max(20);

// Process queue size
consumer_option.set_pull_threshold_for_all(10000);
```

### Batch Processing

```rust
// Increase pull batch size
consumer_option.set_pull_batch_size(64);

// Reduce pull interval
consumer_option.set_pull_interval(0);
```

### Concurrency Control

```rust
// Limit messages per queue
consumer_option.set_pull_threshold_for_queue(1000);

// Limit messages per consumer
consumer_option.set_pull_threshold_for_all(10000);
```

## Best Practices

1. **Use appropriate consumer model**: Choose clustering vs broadcasting based on requirements
2. **Handle errors gracefully**: Return appropriate consume results
3. **Implement idempotency**: Handle duplicate message processing
4. **Configure thread pool**: Balance between throughput and resource usage
5. **Monitor consumer lag**: Track message consumption backlog
6. **Use filtering**: Reduce unnecessary message processing
7. **Set appropriate retry limits**: Prevent infinite retry loops
8. **Implement dead letter queue**: Handle messages that fail after max retries

## Next Steps

- [Push Consumer](./push-consumer) - Deep dive into push consumer
- [Pull Consumer](./pull-consumer) - Deep dive into pull consumer
- [Message Filtering](./message-filtering) - Advanced filtering techniques
