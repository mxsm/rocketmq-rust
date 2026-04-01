---
sidebar_position: 3
title: Pull Consumer
---

# Pull Consumer

Pull Consumer 让你完全控制何时、以何种方式从 Broker 拉取消息。

## 创建 Pull Consumer

```rust
use rocketmq::consumer::PullConsumer;
use rocketmq::conf::ConsumerOption;

let mut consumer_option = ConsumerOption::default();
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

let consumer = PullConsumer::new(consumer_option);
consumer.start().await?;
```

## 拉取消息

### 基础拉取

```rust
loop {
    let messages = consumer.pull("TopicTest", "*", 32).await?;

    for msg in messages {
        println!("Received: {:?}", msg);
        process_message(&msg);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
}
```

### 从指定队列拉取

```rust
use rocketmq::model::MessageQueue;

// 获取 topic 下所有队列
let queues = consumer.fetch_subscribe_message_queues("TopicTest").await?;

for queue in queues {
    let messages = consumer.pull_from(&queue, "*", 32).await?;
    for msg in messages {
        process_message(&msg);
    }
}
```

### 指定位点拉取

```rust
// 从指定 offset 开始拉取
let messages = consumer
    .pull_from_offset(&queue, "*", 100, 32)
    .await?;
```

## 位点管理

### 手动位点追踪

```rust
struct OffsetTracker {
    offsets: HashMap<MessageQueue, u64>,
}

impl OffsetTracker {
    fn update(&mut self, queue: &MessageQueue, offset: u64) {
        self.offsets.insert(queue.clone(), offset);
    }

    fn get(&self, queue: &MessageQueue) -> Option<u64> {
        self.offsets.get(queue).copied()
    }
}

let mut tracker = OffsetTracker::new();

loop {
    let queue = select_queue();
    let offset = tracker.get(&queue).unwrap_or(0);

    let result = consumer.pull_from_offset(&queue, "*", offset, 32).await?;

    for msg in &result.messages {
        process_message(msg);
    }

    // 处理成功后更新位点
    if let Some(next_offset) = result.next_begin_offset {
        tracker.update(&queue, next_offset);
    }
}
```

### 持久化位点存储

```rust
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};

struct FileOffsetStore {
    file_path: String,
    offsets: HashMap<String, u64>,
}

impl FileOffsetStore {
    fn load(&mut self) -> std::io::Result<()> {
        if let Ok(mut file) = File::open(&self.file_path) {
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            for line in contents.lines() {
                let parts: Vec<&str> = line.split('=').collect();
                if parts.len() == 2 {
                    let key = parts[0].to_string();
                    let offset = parts[1].parse().unwrap();
                    self.offsets.insert(key, offset);
                }
            }
        }
        Ok(())
    }

    fn save(&self) -> std::io::Result<()> {
        let mut file = File::create(&self.file_path)?;
        for (key, offset) in &self.offsets {
            writeln!(file, "{}={}", key, offset)?;
        }
        Ok(())
    }
}
```

## 拉取策略

### 顺序遍历拉取

```rust
let queues = consumer.fetch_subscribe_message_queues("TopicTest").await?;

loop {
    for queue in &queues {
        let messages = consumer.pull_from(queue, "*", 32).await?;
        for msg in messages {
            process_message(&msg);
        }
    }
}
```

### 轮询拉取

```rust
let queues = consumer.fetch_subscribe_message_queues("TopicTest").await?;
let mut index = 0;

loop {
    let queue = &queues[index % queues.len()];
    index += 1;

    let messages = consumer.pull_from(queue, "*", 32).await?;
    for msg in messages {
        process_message(&msg);
    }
}
```

### 优先级拉取

```rust
let high_priority_topic = "ImportantEvents";
let low_priority_topic = "NormalEvents";

loop {
    // 优先拉取高优先级 topic
    let messages = consumer.pull(high_priority_topic, "*", 32).await?;
    if messages.is_empty() {
        // 高优先级无消息时回退到低优先级
        let messages = consumer.pull(low_priority_topic, "*", 32).await?;
        for msg in messages {
            process_message(&msg);
        }
    } else {
        for msg in messages {
            process_message(&msg);
        }
    }
}
```

## 错误处理

### 拉取异常处理

```rust
loop {
    match consumer.pull("TopicTest", "*", 32).await {
        Ok(messages) => {
            for msg in messages {
                process_message(&msg);
            }
        }
        Err(PullError::NoNewMessage) => {
            // 无新消息，等待后重试
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(PullError::OffsetIllegal) => {
            // 重置 offset
            consumer.seek_to_begin("TopicTest").await?;
        }
        Err(e) => {
            eprintln!("Pull failed: {:?}", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
```

## 性能优化

### 批处理

```rust
let batch_size = 100;

loop {
    let mut batch = Vec::with_capacity(batch_size);

    while batch.len() < batch_size {
        let messages = consumer.pull("TopicTest", "*", 32).await?;
        batch.extend(messages);
    }

    process_batch(batch);
}
```

### 并行处理

```rust
use futures::stream::{StreamExt, TryStreamExt};

async fn pull_and_process(consumer: &PullConsumer, queue: &MessageQueue) -> Result<(), Error> {
    let messages = consumer.pull_from(queue, "*", 32).await?;

    let processed = futures::stream::iter(messages)
        .map(|msg| tokio::spawn(async move { process_message(msg) }))
        .collect::<Vec<_>>()
        .await;

    for result in processed {
        result??;
    }

    Ok(())
}
```

## 最佳实践

1. **处理空拉取**：无消息时避免 busy-wait
2. **正确提交位点**：确保消费进度可恢复
3. **设置合适批量大小**：平衡吞吐与延迟
4. **完善错误处理**：优雅处理拉取失败
5. **监控拉取速率**：持续优化消费性能

## 适用场景

Pull Consumer 更适合：

- 需要精细控制消费节奏
- 需要自定义重试策略
- 需要批量处理消息
- 需要显式控制位点提交
- 需要实现自定义负载策略

## 下一步

- [Push Consumer](./push-consumer) - 了解 push consumer
- [消息过滤](./message-filtering) - 高级过滤策略
- [配置](../configuration) - 消费者配置项
