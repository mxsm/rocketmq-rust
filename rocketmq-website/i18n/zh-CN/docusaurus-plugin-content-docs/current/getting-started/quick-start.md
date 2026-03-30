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
rocketmq = "0.3"
tokio = { version = "1", features = ["full"] }
```

创建 `src/main.rs`：

```rust
use rocketmq::producer::Producer;
use rocketmq::conf::ProducerOption;
use rocketmq::model::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 配置生产者
    let mut producer_option = ProducerOption::default();
    producer_option.set_name_server_addr("localhost:9876");
    producer_option.set_group_name("producer_group_1");

    // 创建生产者
    let producer = Producer::new(producer_option);

    // 启动生产者
    producer.start().await?;

    // 构建消息
    let message = Message::new(
        "TopicTest".to_string(),
        b"Hello, RocketMQ-Rust!".to_vec(),
    );

    // 发送消息
    let result = producer.send(message).await?;
    println!("Message sent: {:?}", result);

    // 关闭生产者
    producer.shutdown().await?;

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
rocketmq = "0.3"
tokio = { version = "1", features = ["full"] }
```

创建 `src/main.rs`：

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;
use rocketmq::listener::MessageListener;

struct MyListener;

impl MessageListener for MyListener {
    fn consume_message(
        &self,
        messages: Vec<rocketmq::model::MessageExt>,
    ) -> rocketmq::error::ConsumeResult {
        for msg in messages {
            println!("Received message: {:?}", msg);
        }
        rocketmq::error::ConsumeResult::Success
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 配置消费者
    let mut consumer_option = ConsumerOption::default();
    consumer_option.set_name_server_addr("localhost:9876");
    consumer_option.set_group_name("consumer_group_1");
    consumer_option.set_consume_thread_min(1);
    consumer_option.set_consume_thread_max(1);

    // 创建消费者
    let consumer = PushConsumer::new(consumer_option);

    // 订阅 Topic
    consumer.subscribe("TopicTest", "*").await?;

    // 注册消息监听器
    consumer.register_message_listener(Box::new(MyListener));

    // 启动消费者
    consumer.start().await?;

    println!("Consumer started. Press Ctrl+C to exit.");

    // 持续运行
    tokio::signal::ctrl_c().await?;

    // 关闭消费者
    consumer.shutdown().await?;

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
