---
sidebar_position: 1
title: 消费者概览
---

# 消费者概览

消费者负责从 RocketMQ Broker 接收并处理消息。RocketMQ-Rust 同时提供 Push Consumer 与 Pull Consumer 两种实现。

## 消费者类型

### Push Consumer

消息由 Broker 自动推送给消费者：

```rust
use rocketmq::consumer::PushConsumer;

let consumer = PushConsumer::new(consumer_option);
consumer.subscribe("TopicTest", "*").await?;
consumer.start().await?;
```

**优势：**

- 事件驱动架构
- 自动拉取消息
- 内置线程池支持并发处理
- 自动管理消费位点

### Pull Consumer

由消费者主动从 Broker 拉取消息：

```rust
use rocketmq::consumer::PullConsumer;

let consumer = PullConsumer::new(consumer_option);
consumer.start().await?;

loop {
    let messages = consumer.pull("TopicTest", "*", 32).await?;
    for msg in messages {
        process_message(msg);
    }
}
```

**优势：**

- 完全掌控消息拉取
- 可自定义批量大小
- 处理流程控制更明确
- 适合批处理场景

## 创建 Push Consumer

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;
use rocketmq::listener::MessageListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 配置消费者
    let mut consumer_option = ConsumerOption::default();
    consumer_option.set_name_server_addr("localhost:9876");
    consumer_option.set_group_name("my_consumer_group");
    consumer_option.set_consume_thread_min(1);
    consumer_option.set_consume_thread_max(10);

    // 创建消费者
    let consumer = PushConsumer::new(consumer_option);

    // 订阅 topic
    consumer.subscribe("TopicTest", "*").await?;

    // 注册消息监听器
    consumer.register_message_listener(Box::new(MyListener));

    // 启动消费者
    consumer.start().await?;

    // 保持运行
    tokio::signal::ctrl_c().await?;

    Ok(())
}
```

## 消息监听器

通过实现 `MessageListener` trait 处理消息：

```rust
use rocketmq::listener::MessageListener;
use rocketmq::error::ConsumeResult;

struct MyListener;

impl MessageListener for MyListener {
    fn consume_message(
        &self,
        messages: Vec<rocketmq::model::MessageExt>,
    ) -> ConsumeResult {
        for msg in messages {
            match process_message(&msg) {
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("Failed to process message: {:?}", e);
                    return ConsumeResult::SuspendCurrentQueueAMoment;
                }
            }
        }
        ConsumeResult::Success
    }
    }

fn process_message(msg: &MessageExt) -> Result<(), Error> {
    println!("Received message: {}", String::from_utf8_lossy(msg.get_body()));
    Ok(())
}
```

## 消费者配置

### 基础配置

```rust
let mut consumer_option = ConsumerOption::default();

// 必填
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

// 线程池
consumer_option.set_consume_thread_min(2);
consumer_option.set_consume_thread_max(10);

// 拉取批量
consumer_option.set_pull_batch_size(32);
consumer_option.set_pull_interval(0);
```

### 高级配置

```rust
// 位点管理
consumer_option.set_enable_msg_trace(true);
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// 重试设置
consumer_option.set_max_reconsume_times(3);

// 消息模型
consumer_option.set_message_model(MessageModel::Clustering);
```

## 消息消费模型

### Clustering（默认）

同一消费组内的消息会分摊到不同消费者：

```rust
consumer_option.set_message_model(MessageModel::Clustering);
```

每条消息只会被组内一个消费者消费。

### Broadcasting

组内每个消费者都会收到全部消息：

```rust
consumer_option.set_message_model(MessageModel::Broadcasting);
```

## 消息过滤

### 基于 Tag 的过滤

```rust
// 订阅指定标签
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;

// 订阅全部标签
consumer.subscribe("OrderEvents", "*").await?;

// 排除特定标签
consumer.subscribe("OrderEvents", "!(order_cancelled)").await?;
```

### SQL92 过滤

```rust
// 使用 SQL 表达式订阅
consumer.subscribe(
    "OrderEvents",
    "region = 'us-west' AND amount > 100"
).await?;
```

## 位点管理

### 起始消费位置

```rust
// 从最新位点开始
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// 从最早位点开始
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);

// 从指定时间开始
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromTimestamp);
consumer_option.set_consume_timestamp("20230101000000");
```

### 手动提交位点

```rust
// 关闭自动提交
consumer_option.set_enable_auto_commit(false);

// 处理消息
for msg in messages {
    process_message(msg);
    consumer.commit_sync(msg.get_queue_offset(), msg.get_queue_id())?;
}
```

## 错误处理

### 消费结果

```rust
impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        for msg in messages {
            match process_message(&msg) {
                Ok(_) => continue,
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    return ConsumeResult::ReconsumeLater;
                }
            }
        }
        ConsumeResult::Success
    }
}
```

### 重试处理

```rust
// 配置最大重试次数
consumer_option.set_max_reconsume_times(3);

// 检查重试次数
impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        for msg in messages {
            let retry_count = msg.get_reconsume_times();
            if retry_count >= 3 {
                eprintln!("Max retries exceeded for message: {:?}", msg.get_msg_id());
                // 可写入死信队列或记录日志
                continue;
            }
            // 处理消息
        }
        ConsumeResult::Success
    }
}
```

## 性能调优

### 线程池配置

```rust
// 最小线程数（常驻）
consumer_option.set_consume_thread_min(2);

// 最大线程数（高负载时扩容）
consumer_option.set_consume_thread_max(20);

// 处理队列阈值
consumer_option.set_pull_threshold_for_all(10000);
```

### 批处理配置

```rust
// 增大单次拉取批量
consumer_option.set_pull_batch_size(64);

// 减少拉取间隔
consumer_option.set_pull_interval(0);
```

### 并发控制

```rust
// 单队列阈值
consumer_option.set_pull_threshold_for_queue(1000);

// 全局阈值
consumer_option.set_pull_threshold_for_all(10000);
```

## 最佳实践

1. **选择合适消费模型**：按场景选择集群或广播
2. **优雅处理消费异常**：返回正确的消费状态
3. **实现幂等消费**：应对重复消息投递
4. **合理配置线程池**：平衡吞吐与资源占用
5. **监控消费堆积**：跟踪 lag 变化
6. **利用过滤能力**：减少无效消息处理
7. **设置恰当重试上限**：避免无限重试
8. **引入死信机制**：处理超过重试上限的消息

## 下一步

- [Push Consumer](./push-consumer) - 深入了解 push consumer
- [Pull Consumer](./pull-consumer) - 深入了解 pull consumer
- [消息过滤](./message-filtering) - 高级过滤策略
