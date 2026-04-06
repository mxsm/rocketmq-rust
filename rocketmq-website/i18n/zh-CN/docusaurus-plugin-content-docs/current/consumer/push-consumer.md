---
sidebar_position: 2
title: 推消费者
---

# 推消费者

RocketMQ-Rust 中的推模式由 `DefaultMQPushConsumer` 实现。客户端在后台拉取消息，并将消息批次分发给你注册的监听器。

## 创建推消费者

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("my_consumer_group")
        .name_server_addr("localhost:9876")
        .consume_thread_min(2)
        .consume_thread_max(20)
        .build();

    consumer.subscribe("TopicTest", "*").await?;
    consumer.start().await?;

    Ok(())
}
```

## 消息监听器

### 并发监听器

```rust
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;

struct MyListener;

impl MessageListenerConcurrently for MyListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for msg in messages {
            println!("Processing: {:?}", msg.msg_id());
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

consumer.register_message_listener_concurrently(MyListener);
```

### 顺序监听器

```rust
use rocketmq_client_rust::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use rocketmq_client_rust::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;

struct OrderListener;

impl MessageListenerOrderly for OrderListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> RocketMQResult<ConsumeOrderlyStatus> {
        for msg in messages {
            process_in_order(msg);
        }
        context.set_auto_commit(true);
        Ok(ConsumeOrderlyStatus::Success)
    }
}

consumer.register_message_listener_orderly(OrderListener);
```

## 订阅模式

### 单主题

```rust
consumer.subscribe("TopicTest", "*").await?;
```

### 多主题

```rust
consumer.subscribe("TopicA", "*").await?;
consumer.subscribe("TopicB", "tag1 || tag2").await?;
```

### 消息选择器

```rust
use rocketmq_client_rust::consumer::message_selector::MessageSelector;

let selector = MessageSelector::by_sql("amount > 100 AND region = 'us-west'");
consumer
    .subscribe_with_selector("OrderEvents", Some(selector))
    .await?;
```

## 并发与拉取参数

```rust
let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(2)
    .consume_thread_max(20)
    .pull_batch_size(32)
    .pull_interval(0)
    .pull_threshold_for_queue(1024)
    .pull_threshold_for_topic(10_000)
    .build();
```

## 消费起点

```rust
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset)
    .build();
```

## 暂停与恢复

```rust
consumer.suspend().await;
// ...
consumer.resume().await;
```

## 最佳实践

1. 按业务处理复杂度设置消费线程数。
2. 吞吐优先用并发监听，严格有序用顺序监听。
3. 监听逻辑保持幂等，适配重试语义。
4. 基于真实流量调优 `pull_batch_size` 与阈值参数。
5. 优先使用服务端过滤，减少无效消息传输。

## 下一步

- [拉取消费者](./pull-consumer) - 了解拉模式消费
- [消息过滤](./message-filtering) - 学习高级过滤能力
- [客户端配置](../configuration/client-config) - 查看消费者配置项
