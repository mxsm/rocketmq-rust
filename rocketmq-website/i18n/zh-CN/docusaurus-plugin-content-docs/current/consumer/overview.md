---
sidebar_position: 1
title: 消费者概览
---

# 消费者概览

RocketMQ-Rust 提供两种消费方式：

- `DefaultMQPushConsumer`：回调驱动，适合实时消费。
- `DefaultLitePullConsumer`：轮询驱动，适合批处理与回放控制。

## 消费模型

### 推模式消费者

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("push_group")
    .name_server_addr("localhost:9876")
    .build();

consumer.subscribe("TopicTest", "*").await?;
consumer.start().await?;
```

### 拉模式消费者

```rust
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let consumer = DefaultLitePullConsumer::builder()
    .consumer_group("pull_group")
    .name_server_addr("localhost:9876")
    .auto_commit(true)
    .build();

consumer.start().await?;
consumer.subscribe("TopicTest").await?;

loop {
    let messages = consumer.poll_with_timeout(1_000).await;
    for msg in messages {
        process_message(&msg);
    }
}
```

## 创建推消费者

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
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
            println!("Received message: {:?}", msg.msg_id());
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("my_consumer_group")
        .name_server_addr("localhost:9876")
        .consume_thread_min(2)
        .consume_thread_max(10)
        .build();

    consumer.subscribe("TopicTest", "*").await?;
    consumer.register_message_listener_concurrently(MyListener);
    consumer.start().await?;

    let _ = tokio::signal::ctrl_c().await;
    consumer.shutdown().await;
    Ok(())
}
```

## 配置要点

### 推模式配置

```rust
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(2)
    .consume_thread_max(20)
    .pull_batch_size(32)
    .pull_interval(0)
    .consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset)
    .message_model(MessageModel::Clustering)
    .max_reconsume_times(3)
    .build();
```

### 拉模式配置

```rust
let consumer = DefaultLitePullConsumer::builder()
    .consumer_group("my_pull_group")
    .name_server_addr("localhost:9876")
    .pull_batch_size(32)
    .pull_threshold_for_queue(1_000)
    .pull_threshold_for_all(10_000)
    .auto_commit(false)
    .auto_commit_interval_millis(5_000)
    .build();
```

## 消息过滤

### Tag 过滤

```rust
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;
```

### SQL 过滤

```rust
use rocketmq_client_rust::consumer::message_selector::MessageSelector;

let selector = MessageSelector::by_sql("region = 'us-west' AND amount > 100");
consumer
    .subscribe_with_selector("OrderEvents", Some(selector))
    .await?;
```

## 重试处理

```rust
impl MessageListenerConcurrently for MyListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for msg in messages {
            if msg.reconsume_times() >= 3 {
                eprintln!("Max retries exceeded: {:?}", msg.msg_id());
                continue;
            }

            if let Err(e) = process_message_safe(msg) {
                eprintln!("Process failed: {:?}", e);
                return Ok(ConsumeConcurrentlyStatus::ReconsumeLater);
            }
        }

        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}
```

## 性能调优

```rust
let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("perf_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(4)
    .consume_thread_max(32)
    .pull_batch_size(64)
    .pull_threshold_for_queue(2_000)
    .pull_threshold_for_topic(20_000)
    .build();
```

## 最佳实践

1. 在线事件处理优先推模式，批处理与回放优先拉模式。
2. 监听逻辑保持幂等，适配至少一次投递语义。
3. 线程数和阈值参数以真实流量压测结果为准。
4. 优先使用服务端过滤，减少无效消息传输。
5. 明确最大重试次数，并配置死信处理链路。

## 下一步

- [推消费者](./push-consumer) - 深入了解推模式消费
- [拉取消费者](./pull-consumer) - 深入了解拉模式消费
- [消息过滤](./message-filtering) - 学习高级过滤能力
