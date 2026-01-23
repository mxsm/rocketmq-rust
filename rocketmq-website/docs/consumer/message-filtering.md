---
sidebar_position: 4
title: Message Filtering
---

# Message Filtering

RocketMQ provides powerful filtering capabilities to reduce unnecessary message processing.

## Tag-based Filtering

### Basic Tag Filtering

Subscribe to messages with specific tags:

```rust
// Subscribe to single tag
consumer.subscribe("OrderEvents", "order_created").await?;

// Subscribe to multiple tags
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;

// Subscribe to all tags
consumer.subscribe("OrderEvents", "*").await?;
```

### Setting Tags on Producer

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.set_tags("order_created");
producer.send(message).await?;
```

### Excluding Tags

```rust
// Exclude specific tag
consumer.subscribe("OrderEvents", "!(order_cancelled)").await?;
```

## SQL92 Expression Filtering

### Enable SQL92 Filtering

SQL92 filtering must be enabled on the broker:

```toml
# broker.properties
enablePropertyFilter=true
```

### Using SQL92 Expressions

```rust
// Numeric comparison
consumer.subscribe("OrderEvents", "amount > 100").await?;

// String comparison
consumer.subscribe("OrderEvents", "region = 'us-west'").await?;

// Logical operators
consumer.subscribe("OrderEvents", "amount > 100 AND region = 'us-west'").await?;

// Complex expressions
consumer.subscribe(
    "OrderEvents",
    "(region = 'us-west' OR region = 'us-east') AND amount > 100"
).await?;
```

### Setting Properties on Producer

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.put_property("amount", "150.00");
message.put_property("region", "us-west");
message.put_property("priority", "high");
producer.send(message).await?;
```

## SQL92 Syntax

### Comparison Operators

```rust
// Equality
"region = 'us-west'"

// Inequality
"amount != 0"

// Greater than
"amount > 100"

// Less than
"amount < 1000"

// Greater or equal
"amount >= 100"

// Less or equal
"amount <= 1000"
```

### Logical Operators

```rust
// AND
"amount > 100 AND region = 'us-west'"

// OR
"region = 'us-west' OR region = 'us-east'"

// NOT
"NOT (region = 'us-west')"

// Combination
"(region = 'us-west' OR region = 'us-east') AND amount > 100"
```

### Pattern Matching

```rust
// LIKE operator
"customer_id LIKE 'VIP%'"

// IS NULL
"description IS NULL"

// IS NOT NULL
"description IS NOT NULL"
```

### BETWEEN Operator

```rust
// Between
"amount BETWEEN 100 AND 1000"
```

### IN Operator

```rust
// In list
"region IN ('us-west', 'us-east', 'eu-west')"
```

## Filter Performance

### Tag Filtering

- **Performance**: Very fast, O(1) hash lookup
- **Location**: Broker side
- **Use case**: Simple categorization

### SQL92 Filtering

- **Performance**: Moderate, expression evaluation required
- **Location**: Broker side
- **Use case**: Complex filtering logic

### Client-side Filtering

```rust
impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        for msg in messages {
            let region = msg.get_property("region");
            let amount: f64 = msg.get_property("amount")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);

            if region == Some("us-west".to_string()) && amount > 100.0 {
                process_message(&msg);
            }
        }
        ConsumeResult::Success
    }
}
```

- **Performance**: Network overhead (all messages sent)
- **Location**: Consumer side
- **Use case**: Complex business logic

## Best Practices

1. **Use tag filtering when possible**: Most efficient method
2. **Filter at broker side**: Reduces network traffic
3. **Use meaningful tags**: Design tag hierarchy carefully
4. **Index frequently filtered properties**: Set message keys
5. **Avoid overly complex expressions**: Keep SQL92 simple
6. **Consider client-side filtering**: For complex business logic

## Examples

### Order Processing

```rust
// Producer
let mut message = Message::new("OrderEvents".to_string(), order_json);
message.set_tags("order_created");
message.put_property("region", &order.region);
message.put_property("amount", &order.amount.to_string());
producer.send(message).await?;

// Consumer
consumer.subscribe(
    "OrderEvents",
    "order_created AND amount > 1000"
).await?;
```

### Log Aggregation

```rust
// Producer
let mut message = Message::new("ApplicationLogs".to_string(), log_entry);
message.set_tags(&log.level); // ERROR, WARN, INFO, DEBUG
message.put_property("service", &log.service);
message.put_property("environment", &log.environment);
producer.send(message).await?;

// Consumer - Only errors from production
consumer.subscribe(
    "ApplicationLogs",
    "ERROR AND environment = 'production'"
).await?;
```

### Event Routing

```rust
// Producer
let mut message = Message::new("UserEvents".to_string(), event_json);
message.set_tags(&event.event_type);
message.put_property("user_tier", &user.tier);
producer.send(message).await?;

// Consumer - High priority events
consumer.subscribe(
    "UserEvents",
    "(login OR logout OR purchase) AND user_tier = 'premium'"
).await?;
```

## Next Steps

- [Configuration](../category/configuration) - Configure filtering
- [Producer Guide](../category/producer) - Setting message tags and properties
- [Best Practices](#) - Performance optimization
