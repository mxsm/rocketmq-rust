---
sidebar_position: 4
title: Message Filtering
---

# Message Filtering

RocketMQ filtering helps consumers avoid processing irrelevant messages.

## Tag-based Filtering

### Basic Tag Subscription

```rust
// Subscribe to one tag
consumer.subscribe("OrderEvents", "order_created").await?;

// Subscribe to multiple tags
consumer
    .subscribe("OrderEvents", "order_created || order_paid")
    .await?;

// Subscribe to all tags
consumer.subscribe("OrderEvents", "*").await?;
```

### Setting Tags on Producer

```rust
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .tags("order_created")
    .build()?;

producer.send(message).await?;
```

## Filtering with Message Properties

Producer can attach structured metadata to support downstream filtering logic.

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .tags("order_created")
    .raw_property("amount", "150.00")?
    .raw_property("region", "us-west")?
    .raw_property("priority", "high")?
    .build()?;

producer.send(message).await?;
```

## Client-side Filtering (Property-based)

When business conditions are dynamic or complex, you can do additional checks in the listener.

```rust
use cheetah_string::CheetahString;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_common::common::message::MessageTrait;

consumer.register_message_listener_concurrently(|msgs, _ctx| {
    let region_key = CheetahString::from_static_str("region");
    let amount_key = CheetahString::from_static_str("amount");

    for msg in msgs {
        let region = msg
            .property(&region_key)
            .map(|v| v.to_string())
            .unwrap_or_default();

        let amount = msg
            .property(&amount_key)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        if region == "us-west" && amount > 100.0 {
            // process_message(msg);
        }
    }

    Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
});
```

## SQL92 Notes

RocketMQ broker supports SQL92-based server-side filtering, but current public consumer examples in this project focus on tag-expression subscriptions and optional client-side property checks.

If you need SQL92 at scale, verify your broker and client capability in an integration environment before adopting it in production.

## Filter Performance

### Tag Filtering

- Performance: very fast
- Location: broker side
- Best for: stable event categories

### Client-side Property Filtering

- Performance: lower than tag filtering (messages still reach consumer)
- Location: consumer side
- Best for: dynamic or complex business conditions

## Best Practices

1. Prefer tag filtering as the first-stage filter.
2. Keep tag taxonomy stable and business-oriented.
3. Put frequently queried dimensions into message properties.
4. Keep client-side filter logic lightweight.
5. Monitor rejection rate to tune producer tagging strategy.

## Examples

### Order Processing

```rust
// Producer
let message = Message::builder()
    .topic("OrderEvents")
    .body(order_json)
    .tags("order_created")
    .raw_property("region", &order.region)?
    .raw_property("amount", order.amount.to_string())?
    .build()?;
producer.send(message).await?;

// Consumer
consumer.subscribe("OrderEvents", "order_created").await?;
```

### Log Aggregation

```rust
// Producer
let message = Message::builder()
    .topic("ApplicationLogs")
    .body(log_entry)
    .tags(&log.level) // ERROR, WARN, INFO, DEBUG
    .raw_property("service", &log.service)?
    .raw_property("environment", &log.environment)?
    .build()?;
producer.send(message).await?;

// Consumer
consumer.subscribe("ApplicationLogs", "ERROR || WARN").await?;
```

### Event Routing

```rust
// Producer
let message = Message::builder()
    .topic("UserEvents")
    .body(event_json)
    .tags(&event.event_type)
    .raw_property("user_tier", &user.tier)?
    .build()?;
producer.send(message).await?;

// Consumer
consumer.subscribe("UserEvents", "login || logout || purchase").await?;
```

## Next Steps

- [Broker Configuration](../configuration/broker-config) - Configure filtering-related settings
- [Producer Guide](../category/producer) - Set tags and properties properly
- [Consumer Overview](./overview) - Learn consumption models and offset management
