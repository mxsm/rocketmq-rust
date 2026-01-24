---
sidebar_position: 3
title: Pull Consumer
---

# Pull Consumer

Pull Consumer gives you full control over when and how messages are retrieved from the broker.

## Creating a Pull Consumer

```rust
use rocketmq::consumer::PullConsumer;
use rocketmq::conf::ConsumerOption;

let mut consumer_option = ConsumerOption::default();
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

let consumer = PullConsumer::new(consumer_option);
consumer.start().await?;
```

## Pulling Messages

### Basic Pull

```rust
loop {
    let messages = consumer.pull("TopicTest", "*", 32).await?;

    for msg in messages {
        println!("Received: {:?}", msg);
        process_message(&msg);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
}
```

### Pull from Specific Queue

```rust
use rocketmq::model::MessageQueue;

// Get list of queues for a topic
let queues = consumer.fetch_subscribe_message_queues("TopicTest").await?;

for queue in queues {
    let messages = consumer.pull_from(&queue, "*", 32).await?;
    for msg in messages {
        process_message(&msg);
    }
}
```

### Pull with Offset

```rust
// Pull from specific offset
let messages = consumer
    .pull_from_offset(&queue, "*", 100, 32)
    .await?;
```

## Offset Management

### Manual Offset Tracking

```rust
struct OffsetTracker {
    offsets: HashMap<MessageQueue, u64>,
}

impl OffsetTracker {
    fn update(&mut self, queue: &MessageQueue, offset: u64) {
        self.offsets.insert(queue.clone(), offset);
    }

    fn get(&self, queue: &MessageQueue) -> Option<u64> {
        self.offsets.get(queue).copied()
    }
}

let mut tracker = OffsetTracker::new();

loop {
    let queue = select_queue();
    let offset = tracker.get(&queue).unwrap_or(0);

    let result = consumer.pull_from_offset(&queue, "*", offset, 32).await?;

    for msg in &result.messages {
        process_message(msg);
    }

    // Update offset after processing
    if let Some(next_offset) = result.next_begin_offset {
        tracker.update(&queue, next_offset);
    }
}
```

### Persistent Offset Storage

```rust
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};

struct FileOffsetStore {
    file_path: String,
    offsets: HashMap<String, u64>,
}

impl FileOffsetStore {
    fn load(&mut self) -> std::io::Result<()> {
        if let Ok(mut file) = File::open(&self.file_path) {
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            for line in contents.lines() {
                let parts: Vec<&str> = line.split('=').collect();
                if parts.len() == 2 {
                    let key = parts[0].to_string();
                    let offset = parts[1].parse().unwrap();
                    self.offsets.insert(key, offset);
                }
            }
        }
        Ok(())
    }

    fn save(&self) -> std::io::Result<()> {
        let mut file = File::create(&self.file_path)?;
        for (key, offset) in &self.offsets {
            writeln!(file, "{}={}", key, offset)?;
        }
        Ok(())
    }
}
```

## Pull Strategies

### Sequential Pull

```rust
let queues = consumer.fetch_subscribe_message_queues("TopicTest").await?;

loop {
    for queue in &queues {
        let messages = consumer.pull_from(queue, "*", 32).await?;
        for msg in messages {
            process_message(&msg);
        }
    }
}
```

### Round-Robin Pull

```rust
let queues = consumer.fetch_subscribe_message_queues("TopicTest").await?;
let mut index = 0;

loop {
    let queue = &queues[index % queues.len()];
    index += 1;

    let messages = consumer.pull_from(queue, "*", 32).await?;
    for msg in messages {
        process_message(&msg);
    }
}
```

### Priority-Based Pull

```rust
let high_priority_topic = "ImportantEvents";
let low_priority_topic = "NormalEvents";

loop {
    // Prioritize high priority topic
    let messages = consumer.pull(high_priority_topic, "*", 32).await?;
    if messages.is_empty() {
        // Fall back to low priority topic
        let messages = consumer.pull(low_priority_topic, "*", 32).await?;
        for msg in messages {
            process_message(&msg);
        }
    } else {
        for msg in messages {
            process_message(&msg);
        }
    }
}
```

## Error Handling

### Handle Pull Exception

```rust
loop {
    match consumer.pull("TopicTest", "*", 32).await {
        Ok(messages) => {
            for msg in messages {
                process_message(&msg);
            }
        }
        Err(PullError::NoNewMessage) => {
            // No new messages, wait and retry
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(PullError::OffsetIllegal) => {
            // Reset offset
            consumer.seek_to_begin("TopicTest").await?;
        }
        Err(e) => {
            eprintln!("Pull failed: {:?}", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
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
        let messages = consumer.pull("TopicTest", "*", 32).await?;
        batch.extend(messages);
    }

    process_batch(batch);
}
```

### Parallel Processing

```rust
use futures::stream::{StreamExt, TryStreamExt};

async fn pull_and_process(consumer: &PullConsumer, queue: &MessageQueue) -> Result<(), Error> {
    let messages = consumer.pull_from(queue, "*", 32).await?;

    let processed = futures::stream::iter(messages)
        .map(|msg| tokio::spawn(async move { process_message(msg) }))
        .collect::<Vec<_>>()
        .await;

    for result in processed {
        result??;
    }

    Ok(())
}
```

## Best Practices

1. **Handle empty pulls**: Don't busy-wait when no messages are available
2. **Commit offsets properly**: Ensure durability of consumer offsets
3. **Use appropriate batch sizes**: Balance throughput and latency
4. **Implement error handling**: Handle pull failures gracefully
5. **Monitor pull rate**: Track pull performance and latency

## When to Use Pull Consumer

Pull Consumer is ideal when:
- You need fine-grained control over message consumption
- You want to implement custom retry logic
- You need to process messages in batches
- You require explicit control over offset commits
- You're implementing custom load balancing

## Next Steps

- [Push Consumer](./push-consumer) - Learn about push consumer
- [Message Filtering](./message-filtering) - Advanced filtering techniques
- [Configuration](../category/configuration) - Consumer configuration options
