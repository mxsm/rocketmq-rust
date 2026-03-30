---
sidebar_position: 2
title: 发送消息
---

# 发送消息

本章介绍 RocketMQ-Rust 生产者的高级消息发送技巧。

## 消息类型

### 基础消息

```rust
use rocketmq::model::Message;

let message = Message::new("TopicTest".to_string(), b"Hello, RocketMQ!".to_vec());
producer.send(message).await?;
```

### 带 Tag 的消息

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.set_tags("order_created");
producer.send(message).await?;
```

### 带 Key 的消息

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.set_keys("order_12345");
producer.send(message).await?;
```

### 带属性的消息

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.put_property("region", "us-west");
message.put_property("priority", "high");
message.put_property("source", "mobile_app");
producer.send(message).await?;
```

## 发送策略

### 顺序发送

按顺序逐条发送消息：

```rust
let messages = vec![
    create_message("Step 1"),
    create_message("Step 2"),
    create_message("Step 3"),
];

for msg in messages {
    producer.send(msg).await?;
}
```

### 并发发送

并发发送多条消息：

```rust
use futures::future::join_all;

let send_futures = messages.into_iter()
    .map(|msg| producer.send(msg))
    .collect::<Vec<_>>();

let results = join_all(send_futures).await;
```

### 延迟发送

将消息投递到未来时间点：

```rust
let mut message = Message::new("DelayedTopic".to_string(), body);
// Delay level 1 = 1s, 2 = 5s, 3 = 10s, etc.
message.set_delay_time_level(3);
producer.send(message).await?;
```

## 消息大小管理

### 大消息处理

对于大于 4MB 的消息，可启用压缩：

```rust
// 启用压缩
producer_option.set_compress_msg_body_over_threshold(4 * 1024);

// 大消息会自动压缩
let large_body = vec![0u8; 5 * 1024 * 1024]; // 5MB
let message = Message::new("TopicTest".to_string(), large_body);
producer.send(message).await?;
```

### 拆分大消息

```rust
fn split_and_send(producer: &Producer, topic: &str, data: Vec<u8>, chunk_size: usize) -> Result<(), Error> {
    let chunks: Vec<_> = data.chunks(chunk_size).collect();
    let total_chunks = chunks.len();

    for (i, chunk) in chunks.iter().enumerate() {
        let mut message = Message::new(topic.to_string(), chunk.to_vec());
        message.put_property("chunk_index", i.to_string());
        message.put_property("total_chunks", total_chunks.to_string());
        message.put_property("message_id", unique_id());

        producer.send(message.clone()).await?;
    }

    Ok(())
}
```

## 错误处理

### 失败重试

```rust
async fn send_with_retry(
    producer: &Producer,
    message: Message,
    max_retries: u32,
) -> Result<SendResult, Error> {
    let mut retry_count = 0;

    loop {
        match producer.send(message.clone()).await {
            Ok(result) => return Ok(result),
            Err(e) if retry_count < max_retries => {
                retry_count += 1;
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 回退到备用队列

```rust
async fn send_with_fallback(
    producer: &Producer,
    message: Message,
    fallback_topic: &str,
) -> Result<SendResult, Error> {
    match producer.send(message.clone()).await {
        Ok(result) => Ok(result),
        Err(_) => {
            let mut fallback_message = message.clone();
            fallback_message.set_topic(fallback_topic.to_string());
            fallback_message.put_property("original_topic", message.get_topic());
            producer.send(fallback_message).await
        }
    }
}
```

## 发送监控

### 跟踪发送结果

```rust
let mut success_count = 0;
let mut failure_count = 0;

for message in messages {
    match producer.send(message).await {
        Ok(result) => {
            success_count += 1;
            println!("Sent: {}, offset: {}", result.msg_id, result.queue_offset);
        }
        Err(e) => {
            failure_count += 1;
            eprintln!("Failed: {:?}", e);
        }
    }
}

println!("Success: {}, Failed: {}", success_count, failure_count);
```

### 性能监控

```rust
use std::time::Instant;

let start = Instant::now();

for message in messages {
    producer.send(message).await?;
}

let elapsed = start.elapsed();
let throughput = messages.len() as f64 / elapsed.as_secs_f64();

println!("Sent {} messages in {:.2}s", messages.len(), elapsed.as_secs_f64());
println!("Throughput: {:.2} msg/s", throughput);
```

## 常见业务场景

### 发送订单事件

```rust
async fn send_order_event(
    producer: &Producer,
    order_id: &str,
    event_type: &str,
    order_data: &Order,
) -> Result<SendResult, Error> {
    let body = serde_json::to_vec(order_data)?;

    let mut message = Message::new("OrderEvents".to_string(), body);
    message.set_tags(event_type);
    message.set_keys(order_id);
    message.put_property("event_type", event_type);
    message.put_property("timestamp", Utc::now().to_rfc3339());

    producer.send(message).await
}

// Usage
let order = Order { id: "12345", amount: 99.99 };
send_order_event(&producer, "12345", "order_created", &order).await?;
```

### 发送指标数据

```rust
async fn send_metric(
    producer: &Producer,
    metric_name: &str,
    value: f64,
    tags: Vec<(&str, &str)>,
) -> Result<SendResult, Error> {
    let metric_data = MetricData {
        name: metric_name.to_string(),
        value,
        timestamp: Utc::now(),
        tags,
    };

    let body = serde_json::to_vec(&metric_data)?;
    let message = Message::new("Metrics".to_string(), body);

    producer.send(message).await
}
```

### 发送日志

```rust
async fn send_log(
    producer: &Producer,
    level: &str,
    message: &str,
    context: HashMap<String, String>,
) -> Result<SendResult, Error> {
    let log_entry = LogEntry {
        level: level.to_string(),
        message: message.to_string(),
        timestamp: Utc::now(),
        context,
    };

    let body = serde_json::to_vec(&log_entry)?;
    let mut msg = Message::new("Logs".to_string(), body);
    msg.set_tags(level);

    producer.send(msg).await
}
```

## 最佳实践

1. **设置有业务意义的 Key**：方便追踪与查询消息
2. **合理使用 Tag**：便于消费端过滤
3. **优雅处理失败**：实现重试与降级策略
4. **持续监控性能**：关注成功率与延迟指标
5. **优先批量发送**：在高吞吐场景提升效率
6. **发送前校验消息大小**：避免超限失败
7. **把元数据放入 properties**：避免污染消息体
8. **实现幂等消费链路**：应对重复消息投递

## 下一步

- [事务消息](./transaction-messages) - 实现事务消息
- [配置](../configuration) - 配置生产者相关参数
- [消费者指南](../consumer/overview) - 了解消费者处理逻辑
