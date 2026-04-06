---
sidebar_position: 2
title: 发送消息
---

# 发送消息

本章介绍基于 `DefaultMQProducer` 与 `MQProducer` 的常见发送模式。

## 消息类型

### 基础消息

```rust
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("TopicTest")
    .body("Hello, RocketMQ!")
    .build()?;

producer.send(message).await?;
```

### 带 Tag 的消息

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .tags("order_created")
    .build()?;

producer.send(message).await?;
```

### 带 Key 的消息

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .key("order_12345")
    .build()?;

producer.send(message).await?;
```

### 带属性的消息

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .raw_property("region", "us-west")?
    .raw_property("priority", "high")?
    .raw_property("source", "mobile_app")?
    .build()?;

producer.send(message).await?;
```

## 发送策略

### 顺序发送

```rust
for msg in messages {
    producer.send(msg).await?;
}
```

### 并发发送

```rust
use futures::future::join_all;

let tasks = messages
    .into_iter()
    .map(|msg| producer.send(msg))
    .collect::<Vec<_>>();

let results = join_all(tasks).await;
```

### 延迟发送

```rust
let message = Message::builder()
    .topic("DelayedTopic")
    .body(body)
    .delay_level(3) // 1=1s, 2=5s, 3=10s ...
    .build()?;

producer.send(message).await?;
```

## 消息大小管理

### 大消息

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("my_group")
    .name_server_addr("localhost:9876")
    .compress_msg_body_over_howmuch(4 * 1024)
    .build();

let large_body = vec![0u8; 5 * 1024 * 1024];
let message = Message::builder()
    .topic("TopicTest")
    .body(large_body)
    .build()?;

producer.send(message).await?;
```

### 拆分大消息

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;

async fn split_and_send(
    producer: &mut DefaultMQProducer,
    topic: &str,
    data: Vec<u8>,
    chunk_size: usize,
) -> rocketmq_error::RocketMQResult<()> {
    let chunks: Vec<_> = data.chunks(chunk_size).collect();
    let total = chunks.len();

    for (i, chunk) in chunks.iter().enumerate() {
        let message = Message::builder()
            .topic(topic)
            .body_slice(chunk)
            .key(format!("chunk-{i}-{total}"))
            .build()?;

        producer.send(message).await?;
    }

    Ok(())
}
```

## 错误处理

### 失败重试

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::send_result::SendResult;

async fn send_with_retry(
    producer: &mut DefaultMQProducer,
    message: Message,
    max_retries: u32,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    let mut retry_count = 0;

    loop {
        match producer.send(message.clone()).await {
            Ok(result) => return Ok(result),
            Err(e) if retry_count < max_retries => {
                retry_count += 1;
                tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
                eprintln!("retry {} after error: {}", retry_count, e);
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 回退到备用队列

```rust
async fn send_with_fallback(
    producer: &mut DefaultMQProducer,
    message: Message,
    fallback_topic: &str,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    match producer.send(message.clone()).await {
        Ok(result) => Ok(result),
        Err(_) => {
            let mut fallback_message = message;
            fallback_message.set_topic(fallback_topic.into());
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
        Ok(_) => {
            success_count += 1;
        }
        Err(e) => {
            failure_count += 1;
            eprintln!("Failed: {}", e);
        }
    }
}

println!("Success: {}, Failed: {}", success_count, failure_count);
```

## 常见业务场景

### 发送订单事件

```rust
async fn send_order_event(
    producer: &mut DefaultMQProducer,
    order_id: &str,
    event_type: &str,
    order_data: &Order,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    let body = serde_json::to_vec(order_data)?;

    let message = Message::builder()
        .topic("OrderEvents")
        .body(body)
        .tags(event_type)
        .key(order_id)
        .raw_property("event_type", event_type)?
        .raw_property("timestamp", chrono::Utc::now().to_rfc3339())?
        .build()?;

    producer.send(message).await
}
```

### 发送日志

```rust
async fn send_log(
    producer: &mut DefaultMQProducer,
    level: &str,
    message_text: &str,
) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
    let message = Message::builder()
        .topic("Logs")
        .body(message_text)
        .tags(level)
        .build()?;

    producer.send(message).await
}
```

## 最佳实践

1. 业务关键消息设置可追踪 Key，便于排障与去重。
2. Tag 用于粗粒度过滤，属性用于补充元数据。
3. 重试需要限制次数，并记录失败日志。
4. 持续统计发送成功率与延迟。
5. 吞吐优先场景可使用批量发送。
6. 发送前校验消息体大小，避免超限失败。
7. 保持消息体 schema 稳定并进行版本化。

## 下一步

- [事务消息](./transaction-messages) - 实现事务消息
- [客户端配置](../configuration/client-config) - 配置生产者相关参数
- [消费者指南](../consumer/overview) - 了解消费者处理逻辑
