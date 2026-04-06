---
sidebar_position: 3
title: 拉取消费者
---

# 拉取消费者

RocketMQ-Rust 中的拉取模式由 `DefaultLitePullConsumer` 提供。它支持按需轮询、手动分配队列和显式管理消费位点。

## 创建拉取消费者

```rust
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;
use rocketmq_error::RocketMQResult;

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    let consumer = DefaultLitePullConsumer::builder()
        .consumer_group("my_consumer_group")
        .name_server_addr("localhost:9876")
        .auto_commit(true)
        .build();

    consumer.start().await?;
    consumer.subscribe_with_expression("TopicTest", "*").await?;

    Ok(())
}
```

## 拉取消息

### 基础轮询

```rust
loop {
    let messages = consumer.poll_with_timeout(1_000).await;

    for msg in messages {
        println!("Received: {:?}", msg);
        process_message(&msg);
    }
}
```

### 手动提交位点

```rust
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let consumer = DefaultLitePullConsumer::builder()
    .consumer_group("manual_commit_group")
    .name_server_addr("localhost:9876")
    .auto_commit(false)
    .build();

consumer.start().await?;
consumer.subscribe("TopicTest").await?;

loop {
    let messages = consumer.poll_with_timeout(1_000).await;
    for msg in &messages {
        process_message(msg);
    }

    if !messages.is_empty() {
        consumer.commit_all().await?;
    }
}
```

## 队列分配与位点控制

### 分配指定队列

```rust
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let queues = consumer.fetch_message_queues("TopicTest").await?;
consumer.assign(queues).await;
```

### 从指定 offset 开始消费

```rust
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let queues = consumer.assignment().await?;
if let Some(queue) = queues.iter().next() {
    consumer.seek(queue, 100).await?;
}
```

### 按时间戳回溯

```rust
use rocketmq_client_rust::consumer::lite_pull_consumer::LitePullConsumer;

let queues = consumer.assignment().await?;
if let Some(queue) = queues.iter().next() {
    let offset = consumer.offset_for_timestamp(queue, 1_699_200_000_000).await?;
    consumer.seek(queue, offset).await?;
}
```

## 拉取策略

### 顺序处理

```rust
loop {
    let messages = consumer.poll_with_timeout(1_000).await;
    for msg in messages {
        process_message(&msg);
    }
}
```

### 按主题优先级处理

```rust
consumer.subscribe_with_expression("ImportantEvents", "*").await?;
consumer.subscribe_with_expression("NormalEvents", "*").await?;

loop {
    let messages = consumer.poll_with_timeout(200).await;

    let mut processed = false;
    for msg in &messages {
        if msg.topic().as_str().contains("ImportantEvents") {
            process_message(msg);
            processed = true;
        }
    }

    if !processed {
        for msg in &messages {
            process_message(msg);
        }
    }
}
```

## 错误处理

```rust
loop {
    let messages = consumer.poll_with_timeout(500).await;

    if messages.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        continue;
    }

    for msg in messages {
        if let Err(e) = process_message_safe(&msg) {
            eprintln!("Process failed: {:?}", e);
        }
    }
}
```

## 性能优化

### 批量处理

```rust
let batch_size = 100;

loop {
    let mut batch = Vec::with_capacity(batch_size);

    while batch.len() < batch_size {
        let messages = consumer.poll_with_timeout(100).await;
        if messages.is_empty() {
            break;
        }
        batch.extend(messages);
    }

    if !batch.is_empty() {
        process_batch(batch);
    }
}
```

### 并行处理

```rust
let messages = consumer.poll_with_timeout(1_000).await;

let handles: Vec<_> = messages
    .into_iter()
    .map(|msg| tokio::spawn(async move { process_message(msg) }))
    .collect();

for handle in handles {
    let _ = handle.await;
}
```

## 最佳实践

1. 对空轮询做退避，避免忙等。
2. 需要严格一致性时使用 `auto_commit(false)` + `commit_all()`。
3. 保持拉取循环轻量，把重处理下沉到工作任务。
4. 使用 `seek` 与 `offset_for_timestamp` 支持回放与恢复。
5. 批量大小应基于真实流量和延迟目标调优。

## 何时使用拉取消费者

适用于以下场景：

- 需要精确控制消费节奏。
- 需要手动提交位点。
- 需要自定义批处理或回放逻辑。
- 需要显式队列分配与位点跳转。

## 下一步

- [推消费者](./push-consumer) - 了解推模式消费
- [消息过滤](./message-filtering) - 学习高级过滤能力
- [客户端配置](../configuration/client-config) - 查看消费者配置项
