---
sidebar_position: 3
title: Pull Consumer
---

# Pull Consumer

Pull consumption in RocketMQ-Rust is provided by `DefaultLitePullConsumer`. It gives you full control over polling frequency, queue assignment, and offset management.

## Creating a Pull Consumer

```rust
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let consumer = DefaultLitePullConsumer::builder()
        .consumer_group("my_consumer_group")
        .name_server_addr("localhost:9876")
        .auto_commit(true)
        .build();

    consumer.start().await?;
    consumer.subscribe_with_expression("TopicTest", "*").await?;

    Ok(())
}
```

## Pulling Messages

### Basic Pull

```rust
loop {
    let messages = consumer.poll_with_timeout(1_000).await;

    for msg in messages {
        println!("Received: {:?}", msg);
        process_message(&msg);
    }
}
```

### Pull with Manual Commit

```rust
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let consumer = DefaultLitePullConsumer::builder()
    .consumer_group("manual_commit_group")
    .name_server_addr("localhost:9876")
    .auto_commit(false)
    .build();

consumer.start().await?;
consumer.subscribe("TopicTest").await?;

loop {
    let messages = consumer.poll_with_timeout(1_000).await;
    for msg in &messages {
        process_message(msg);
    }

    if !messages.is_empty() {
        consumer.commit_all().await?;
    }
}
```

## Queue Assignment and Offset Control

### Assign Specific Queues

```rust
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let queues = consumer.fetch_message_queues("TopicTest").await?;
consumer.assign(queues).await;
```

### Seek to a Specific Offset

```rust
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let queues = consumer.assignment().await?;
if let Some(queue) = queues.iter().next() {
    consumer.seek(queue, 100).await?;
}
```

### Seek by Timestamp

```rust
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let queues = consumer.assignment().await?;
if let Some(queue) = queues.iter().next() {
    let offset = consumer.offset_for_timestamp(queue, 1_699_200_000_000).await?;
    consumer.seek(queue, offset).await?;
}
```

## Pull Strategies

### Sequential Strategy

```rust
loop {
    let messages = consumer.poll_with_timeout(1_000).await;
    for msg in messages {
        process_message(&msg);
    }
}
```

### Priority Strategy by Topic

```rust
consumer.subscribe_with_expression("ImportantEvents", "*").await?;
consumer.subscribe_with_expression("NormalEvents", "*").await?;

loop {
    let messages = consumer.poll_with_timeout(200).await;

    let mut processed = false;
    for msg in &messages {
        if msg.topic().as_str().contains("ImportantEvents") {
            process_message(msg);
            processed = true;
        }
    }

    if !processed {
        for msg in &messages {
            process_message(msg);
        }
    }
}
```

## Error Handling

```rust
loop {
    let messages = consumer.poll_with_timeout(500).await;

    if messages.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        continue;
    }

    for msg in messages {
        if let Err(e) = process_message_safe(&msg) {
            eprintln!("Process failed: {:?}", e);
        }
    }
}
```

## Performance Optimization

### Batch Processing

```rust
let batch_size = 100;

loop {
    let mut batch = Vec::with_capacity(batch_size);

    while batch.len() < batch_size {
        let messages = consumer.poll_with_timeout(100).await;
        if messages.is_empty() {
            break;
        }
        batch.extend(messages);
    }

    if !batch.is_empty() {
        process_batch(batch);
    }
}
```

### Parallel Processing

```rust
let messages = consumer.poll_with_timeout(1_000).await;

let handles: Vec<_> = messages
    .into_iter()
    .map(|msg| tokio::spawn(async move { process_message(msg) }))
    .collect();

for handle in handles {
    let _ = handle.await;
}
```

## Best Practices

1. Handle empty polls to avoid busy-wait loops.
2. Use `auto_commit(false)` when you need strict processing-and-commit control.
3. Keep polling loops simple and move heavy work to worker tasks.
4. Use `seek` and `offset_for_timestamp` for replay and recovery workflows.
5. Size polling and processing batches based on real production latency.

## When to Use Pull Consumer

Pull consumer is ideal when:

- You need fine-grained control over consumption pace.
- You need manual offset control.
- You implement custom batching and replay logic.
- You want explicit queue assignment and seek behavior.

## Next Steps

- [Push Consumer](./push-consumer) - Learn about push consumer
- [Message Filtering](./message-filtering) - Advanced filtering techniques
- [Client Configuration](../configuration/client-config) - Consumer configuration options
