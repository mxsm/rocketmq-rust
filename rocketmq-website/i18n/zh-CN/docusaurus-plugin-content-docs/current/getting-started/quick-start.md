---
sidebar_position: 2
title: 快速开始
---
# 快速开始

本指南将帮助你在几分钟内创建第一个 RocketMQ-Rust 生产者与消费者。

## 第 1 步：启动 RocketMQ 服务

首先，确认你已经有可用的 RocketMQ 服务。如果暂时没有，可以使用 Docker：

```bash
# 启动 nameserver
docker run -d -p 9876:9876 --name rmqnamesrv apache/rocketmq:nameserver

# 启动 broker
docker run -d -p 10911:10911 -p 10909:10909 --name rmqbroker \
  -e "NAMESRV_ADDR=rmqnamesrv:9876" \
  --link rmqnamesrv:rmqnamesrv \
  apache/rocketmq:broker
```

## 第 2 步：创建生产者（Producer）

创建一个新的 Rust 项目：

```bash
cargo new rocketmq-producer
cd rocketmq-producer
```

在 `Cargo.toml` 中添加 RocketMQ 依赖：

```toml
[dependencies]
rocketmq-client-rust = "0.8"
rocketmq-common = "0.8"
rocketmq-error = "0.8"
tokio = { version = "1", features = ["full"] }
```

创建 `src/main.rs`：

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut producer = DefaultMQProducer::builder()
        .producer_group("producer_group_1")
        .name_server_addr("localhost:9876")
        .build();

    producer.start().await?;

    let message = Message::builder()
        .topic("TopicTest")
        .tags("TagA")
        .body("Hello, RocketMQ-Rust!")
        .build()?;

    let result = producer.send_with_timeout(message, 3_000).await?;
    println!("Message sent: {:?}", result);

    producer.shutdown().await;
    Ok(())
}
```

## 第 3 步：创建消费者（Consumer）

再创建一个 Rust 项目：

```bash
cargo new rocketmq-consumer
cd rocketmq-consumer
```

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
rocketmq-client-rust = "0.8"
rocketmq-common = "0.8"
rocketmq-error = "0.8"
tokio = { version = "1", features = ["full"] }
```

创建 `src/main.rs`：

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
        for msg in &messages {
            println!("Received message: {:?}", msg);
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("consumer_group_1")
        .name_server_addr("localhost:9876")
        .consume_thread_min(1)
        .consume_thread_max(1)
        .build();

    consumer.subscribe("TopicTest", "*").await?;
    consumer.register_message_listener_concurrently(MyListener);
    consumer.start().await?;

    println!("Consumer started. Press Ctrl+C to exit.");

    let _ = tokio::signal::ctrl_c().await;
    consumer.shutdown().await;

    Ok(())
}
```

## 第 4 步：运行验证

1. 先启动消费者：

   ```bash
   cargo run
   ```

2. 在另一个终端启动生产者：

   ```bash
   cargo run
   ```

应该能看到消费者成功接收到生产者发送的消息。

## 下一步

恭喜你完成第一个 RocketMQ-Rust 应用！可以继续学习：

- [基本概念](./basic-concepts) - 理解主题、消息与队列
- [生产者指南](../producer/overview) - 掌握生产者高级特性
- [消费者指南](../consumer/overview) - 掌握消费者高级特性

## 常见问题

### 连接被拒绝（Connection Refused）

请先确认 RocketMQ 服务已启动：

```bash
docker ps
```

### 未收到消息（No Messages Received）

请检查：

1. 生产者与消费者的 Topic 名称是否一致
2. 是否先启动了消费者，再启动生产者
3. nameserver 地址是否正确

### 构建错误（Build Errors）

请确认 Rust 版本足够新：

```bash
rustc --version  # 建议 1.70.0 或更高
```
