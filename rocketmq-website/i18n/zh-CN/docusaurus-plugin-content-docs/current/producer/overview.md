---
sidebar_position: 1
title: 生产者概览
---

# 生产者概览

RocketMQ-Rust 生产端基于 `DefaultMQProducer` 和 `MQProducer` trait，支持同步发送、异步回调发送、单向发送、队列选择发送、批量发送以及事务消息。

## 创建生产者

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut producer = DefaultMQProducer::builder()
        .producer_group("my_producer_group")
        .name_server_addr("localhost:9876")
        .build();

    producer.start().await?;
    // 使用 producer ...
    producer.shutdown().await;
    Ok(())
}
```

## 生产者配置

### 基础配置

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("producer_group")
    .name_server_addr("localhost:9876")
    .send_msg_timeout(3_000)
    .retry_times_when_send_failed(2)
    .max_message_size(4 * 1024 * 1024)
    .compress_msg_body_over_howmuch(4 * 1024)
    .build();
```

### 高级配置

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("producer_group")
    .name_server_addr("localhost:9876")
    .retry_times_when_send_failed(3)
    .retry_times_when_send_async_failed(3)
    .retry_another_broker_when_not_store_ok(true)
    .send_msg_max_timeout_per_request(5_000)
    .batch_max_delay_ms(10)
    .batch_max_bytes(512 * 1024)
    .total_batch_max_bytes(4 * 1024 * 1024)
    .enable_backpressure_for_async_mode(true)
    .back_pressure_for_async_send_num(10_000)
    .back_pressure_for_async_send_size(64 * 1024 * 1024)
    .build();
```

## 发送模式

### 同步发送

```rust
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("TopicTest")
    .tags("TagA")
    .body("Hello")
    .build()?;

let result = producer.send(message).await?;
println!("Send result: {:?}", result);
```

### 异步回调发送

```rust
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("TopicTest")
    .body("Hello")
    .build()?;

producer
    .send_with_callback(message, |result, error| {
        if let Some(send_result) = result {
            println!("Message sent: {:?}", send_result);
        }
        if let Some(err) = error {
            eprintln!("Send failed: {}", err);
        }
    })
    .await?;
```

### 单向发送

```rust
let message = Message::builder().topic("TopicTest").body("Fire and forget").build()?;
producer.send_oneway(message).await?;
```

## 队列选择

通过 selector 将同一业务键路由到同一队列。

```rust
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder().topic("OrderEvents").body("order-123").build()?;
let order_id = 123_i64;

producer
    .send_with_selector(
        message,
        |queues: &[MessageQueue], _msg: &Message, id: &i64| {
            let index = (*id % queues.len() as i64) as usize;
            queues.get(index).cloned()
        },
        order_id,
    )
    .await?;
```

## 错误处理

```rust
match producer.send(message).await {
    Ok(result) => println!("Sent: {:?}", result),
    Err(e) => {
        // 达到重试上限后返回
        eprintln!("Send failed: {}", e);
    }
}
```

## 性能优化

### 批量发送

```rust
let messages = vec![
    Message::builder().topic("TopicTest").body("Msg1").build()?,
    Message::builder().topic("TopicTest").body("Msg2").build()?,
    Message::builder().topic("TopicTest").body("Msg3").build()?,
];

producer.send_batch(messages).await?;
```

### 压缩

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("producer_group")
    .name_server_addr("localhost:9876")
    .compress_msg_body_over_howmuch(4 * 1024)
    .build();
```

## 监控建议

`DefaultMQProducer` 当前没有统一的 `get_stats()` 外观方法。生产环境建议：

- 记录发送结果与回调结果。
- 使用结构化日志采集延迟和失败信息。
- 在应用层对 `send*` 调用封装计数器/直方图。

## 最佳实践

1. 按服务边界拆分 producer group。
2. 重试与超时参数按业务 SLA 设置。
3. 高吞吐链路优先使用回调发送。
4. 控制消息体大小，对大消息启用压缩。
5. 需要顺序语义时使用队列选择器。
6. 消费端实现幂等，适配至少一次投递语义。

## 下一步

- [消息发送](./sending-messages) - 学习高级发送技巧
- [事务消息](./transaction-messages) - 学习分布式事务消息
- [客户端配置](../configuration/client-config) - 查看详细配置项
