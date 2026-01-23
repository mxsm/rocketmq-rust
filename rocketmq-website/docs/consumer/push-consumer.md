---
sidebar_position: 2
title: Push Consumer
---

# Push Consumer

Push Consumer provides an event-driven approach to message consumption where messages are automatically delivered from the broker.

## Creating a Push Consumer

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;

let mut consumer_option = ConsumerOption::default();
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

let consumer = PushConsumer::new(consumer_option);
consumer.subscribe("TopicTest", "*").await?;
consumer.start().await?;
```

## Message Listeners

### Sequential Message Listener

Messages are processed sequentially within a queue:

```rust
use rocketmq::listener::MessageListenerConcurrently;

struct MyListener;

impl MessageListenerConcurrently for MyListener {
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
    ) -> ConsumeResult {
        for msg in messages {
            println!("Processing: {:?}", msg.get_msg_id());
            // Process message
        }
        ConsumeResult::Success
    }
}

consumer.register_message_listener(Box::new(MyListener));
```

### Ordered Message Listener

Maintain strict ordering within a queue:

```rust
use rocketmq::listener::MessageListenerOrderly;

struct OrderListener;

impl MessageListenerOrderly for OrderListener {
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
    ) -> ConsumeResult {
        // Messages are processed one by one in order
        for msg in messages {
            process_in_order(msg);
        }
        ConsumeResult::Success
    }
}

consumer.register_message_listener(Box::new(OrderListener));
```

## Subscription Patterns

### Single Topic

```rust
consumer.subscribe("TopicTest", "*").await?;
```

### Multiple Topics

```rust
consumer.subscribe("TopicA", "*").await?;
consumer.subscribe("TopicB", "tag1 || tag2").await?;
consumer.subscribe("TopicC", "region = 'us-west'").await?;
```

### Tag Filtering

```rust
// Subscribe to specific tags
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;

// Subscribe to all tags
consumer.subscribe("OrderEvents", "*").await?;

// Exclude tags
consumer.subscribe("OrderEvents", "!(order_cancelled)").await?;
```

## Concurrency Configuration

```rust
// Thread pool configuration
consumer_option.set_consume_thread_min(2);
consumer_option.set_consume_thread_max(20);

// Process queue size
consumer_option.set_process_queue_size(64);

// Pull batch size
consumer_option.set_pull_batch_size(32);

// Pull interval (milliseconds)
consumer_option.set_pull_interval(0);
```

## Advanced Features

### Pause and Resume

```rust
// Pause consumption
consumer.suspend();

// Resume consumption
consumer.resume();
```

### Message Selectors

```rust
// Use message selector to filter at broker side
use rocketmq::filter::MessageSelector;

let selector = MessageSelector::by_sql("amount > 100 AND region = 'us-west'");
consumer.subscribe_with_selector("OrderEvents", selector).await?;
```

### Offset Management

```rust
// Set starting position
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// Reset offset to specific timestamp
consumer.seek_by_timestamp("TopicTest", 1699200000000).await?;

// Reset offset to specific offset
consumer.seek_to_offset("TopicTest", 0, 100).await?;
```

## Best Practices

1. **Use appropriate thread pool size**: Match to your message processing complexity
2. **Handle exceptions**: Return appropriate consume results
3. **Monitor consumer lag**: Track how far behind you are
4. **Implement idempotency**: Handle duplicate message processing
5. **Use appropriate subscription filters**: Reduce unnecessary message delivery

## Next Steps

- [Pull Consumer](./pull-consumer) - Learn about pull consumer
- [Message Filtering](./message-filtering) - Advanced filtering techniques
- [Configuration](../category/configuration) - Consumer configuration options
