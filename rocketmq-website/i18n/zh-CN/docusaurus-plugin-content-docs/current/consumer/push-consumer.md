---
sidebar_position: 2
title: Push Consumer
---

# Push Consumer

Push Consumer 提供事件驱动的消费模式，消息会由 Broker 自动投递给消费者。

## 创建 Push Consumer

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;

let mut consumer_option = ConsumerOption::default();
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

let consumer = PushConsumer::new(consumer_option);
consumer.subscribe("TopicTest", "*").await?;
consumer.start().await?;
```

## 消息监听器

### 并发消息监听器

同一队列内允许并发处理（由线程池调度）：

```rust
use rocketmq::listener::MessageListenerConcurrently;

struct MyListener;

impl MessageListenerConcurrently for MyListener {
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
    ) -> ConsumeResult {
        for msg in messages {
            println!("Processing: {:?}", msg.get_msg_id());
            // 处理消息
        }
        ConsumeResult::Success
    }
}

consumer.register_message_listener(Box::new(MyListener));
```

### 顺序消息监听器

在单队列内保证严格顺序消费：

```rust
use rocketmq::listener::MessageListenerOrderly;

struct OrderListener;

impl MessageListenerOrderly for OrderListener {
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
    ) -> ConsumeResult {
        // 消息会按顺序逐条处理
        for msg in messages {
            process_in_order(msg);
        }
        ConsumeResult::Success
    }
}

consumer.register_message_listener(Box::new(OrderListener));
```

## 订阅模式

### 单 Topic 订阅

```rust
consumer.subscribe("TopicTest", "*").await?;
```

### 多 Topic 订阅

```rust
consumer.subscribe("TopicA", "*").await?;
consumer.subscribe("TopicB", "tag1 || tag2").await?;
consumer.subscribe("TopicC", "region = 'us-west'").await?;
```

### Tag 过滤订阅

```rust
// 订阅指定 tags
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;

// 订阅所有 tags
consumer.subscribe("OrderEvents", "*").await?;

// 排除 tags
consumer.subscribe("OrderEvents", "!(order_cancelled)").await?;
```

## 并发配置

```rust
// 线程池配置
consumer_option.set_consume_thread_min(2);
consumer_option.set_consume_thread_max(20);

// 处理队列大小
consumer_option.set_process_queue_size(64);

// 拉取批量
consumer_option.set_pull_batch_size(32);

// 拉取间隔（毫秒）
consumer_option.set_pull_interval(0);
```

## 高级特性

### 暂停与恢复消费

```rust
// 暂停消费
consumer.suspend();

// 恢复消费
consumer.resume();
```

### 消息选择器

```rust
// 在 broker 侧使用 selector 过滤
use rocketmq::filter::MessageSelector;

let selector = MessageSelector::by_sql("amount > 100 AND region = 'us-west'");
consumer.subscribe_with_selector("OrderEvents", selector).await?;
```

### 位点管理

```rust
// 设置起始消费位置
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// 重置到指定时间
consumer.seek_by_timestamp("TopicTest", 1699200000000).await?;

// 重置到指定 offset
consumer.seek_to_offset("TopicTest", 0, 100).await?;
```

## 最佳实践

1. **合理设置线程池规模**：匹配消息处理复杂度
2. **处理监听器异常**：返回正确消费结果
3. **监控消费 lag**：及时发现堆积
4. **实现幂等逻辑**：避免重复消费副作用
5. **使用精准订阅过滤**：减少无效消息投递

## 下一步

- [Pull Consumer](./pull-consumer) - 了解 pull consumer
- [消息过滤](./message-filtering) - 高级过滤策略
- [配置](../configuration) - 消费者配置项
