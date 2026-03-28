---
sidebar_position: 2
title: 消息模型
---
# 消息模型

理解 RocketMQ 的消息模型，是设计高质量消息应用的关键。

## 消息结构

### 基础消息

RocketMQ 中的一条消息通常包含以下字段：

```rust
pub struct Message {
    // Topic 名称
    topic: String,

    // 消息体（字节数组）
    body: Vec<u8>,

    // 可选标签，用于过滤
    tags: Option<String>,

    // 可选键，用于索引
    keys: Option<String>,

    // 可选属性
    properties: HashMap<String, String>,
}
```

### 消息示例

```rust
use rocketmq::model::Message;

// 创建基础消息
let mut message = Message::new(
    "OrderEvents".to_string(),
    b"{\"order_id\": \"12345\", \"amount\": 99.99}".to_vec(),
);

// 添加过滤标签
message.set_tags("order_created");

// 添加索引 Key
message.set_keys("order_12345");

// 添加自定义属性
message.put_property("region", "us-west");
message.put_property("priority", "high");
```

## Topics 与 Queues

### Topic

Topic 是消息的逻辑通道，用于分类消息：

- **层次化命名**：如 `orders`、`payments`、`logs`
- **多租户隔离**：不同应用可使用不同 Topic
- **逻辑隔离**：不同 Topic 的消息互不影响

### Queue

Topic 会被拆分成多个 Queue，以支持并行处理：

```text
Topic: OrderEvents (4 queues)

┌───────────────────────────────────────┐
│ Queue 0 │ Queue 1 │ Queue 2 │ Queue 3 │
├───────────────────────────────────────┤
│ Msg 0   │ Msg 1   │ Msg 2   │ Msg 3   │
│ Msg 4   │ Msg 5   │ Msg 6   │ Msg 7   │
│ Msg 8   │ Msg 9   │ Msg 10  │ Msg 11  │
└───────────────────────────────────────┘
```

**多队列的价值：**

- 多消费者并行消费
- 负载分摊
- 提升吞吐

## 消息类型

### 普通消息

无特殊语义的常规消息：

```rust
let message = Message::new("NormalTopic".to_string(), body);
producer.send(message).await?;
```

### 顺序消息

同一队列内按顺序消费的消息：

```rust
// 使用队列选择器，将相关消息路由到同一队列
let selector = |queue_list: &[MessageQueue], message: &Message, arg: &str| {
    let hash = compute_hash(arg); // 例如 order_id
    let index = (hash % queue_list.len() as u64) as usize;
    &queue_list[index]
};

producer.send_with_selector(message, selector, "order_123").await?;
```

### 事务消息

与本地事务保持原子性的消息：

```rust
let transaction_producer = TransactionProducer::new(option)?;

transaction_producer.send_transactional_message(message, |local_state| {
    // 执行本地事务
    let result = execute_database_transaction();

    // 返回事务状态
    match result {
        Ok(_) => TransactionStatus::CommitMessage,
        Err(_) => TransactionStatus::RollbackMessage,
    }
}).await?;
```

### 延迟消息

经过指定延迟后再投递的消息：

```rust
let mut message = Message::new("DelayedTopic".to_string(), body);
message.set_delay_time_level(3); // 延迟等级 3（例如 10 秒）
producer.send(message).await?;
```

## 消息过滤

### 基于 Tag 的过滤

在 Broker 侧按 Tag 进行过滤：

```rust
// 生产者设置 tag
message.set_tags("order_paid");

// 消费者订阅指定 tag
consumer.subscribe("OrderEvents", "order_paid || order_shipped").await?;
```

### SQL92 过滤

使用 SQL92 表达式进行高级过滤：

```rust
// 生产者设置属性
message.put_property("region", "us-west");
message.put_property("amount", "100");

// 消费者使用 SQL 表达式
consumer.subscribe("OrderEvents", "region = 'us-west' AND amount > 50").await?;
```

## 消息属性

### 系统属性

RocketMQ 会自动为每条消息写入系统属性：

- `MSG_ID`：全局唯一消息 ID
- `TOPIC`：Topic 名称
- `QUEUE_ID`：Queue ID
- `QUEUE_OFFSET`：消息在队列中的位置
- `STORE_SIZE`：消息存储大小
- `BORN_TIMESTAMP`：消息创建时间
- `STORE_TIMESTAMP`：消息落盘时间

### 用户属性

你也可以写入自定义属性：

```rust
message.put_property("source", "mobile_app");
message.put_property("version", "2.1.0");
message.put_property("user_id", "user_12345");
```

## 消息生命周期

```mermaid
stateDiagram-v2
    [*] --> Created: Producer creates
    Created --> Sent: Producer sends
    Sent --> Stored: Broker stores
    Stored --> Consumed: Consumer pulls
    Consumed --> Acknowledged: Consumer acknowledges
    Acknowledged --> [*]: Completed

    Sent --> Failed: Send fails
    Failed --> Retrying: Producer retries
    Retrying --> Sent: Retry succeeds
    Retrying --> DeadLetter: Max retries exceeded
```

### 发送流程

```text
1. 创建消息
2. 设置 topic、body、tags、keys、properties
3. 选择队列（负载均衡或自定义选择器）
4. 发送到 broker
5. broker 写入 CommitLog
6. broker 更新 ConsumeQueue
7. 返回发送结果给 producer
```

### 消费流程

```text
1. consumer 从 queue 拉取消息
2. 反序列化消息
3. 执行业务处理
4. 确认消费
5. 更新消费位点
6. 继续下一批消费
```

## 消息持久化

RocketMQ 提供高可靠的持久化机制：

```text
┌─────────────────────────────────────┐
│         CommitLog                   │
│  (Sequential storage of all msgs)   │
├─────────────────────────────────────┤
│ [Msg 1][Msg 2][Msg 3][Msg 4]...     │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│      ConsumeQueue per Queue         │
│  (Index structure for fast access)  │
├─────────────────────────────────────┤
│ Queue 0: [Offset 0][Offset 8]...    │
│ Queue 1: [Offset 16][Offset 24]...  │
└─────────────────────────────────────┘
```

## 最佳实践

1. **使用清晰的 Topic 命名规范**：便于治理与排障
2. **合理设置 Tag**：提升过滤效率
3. **写入消息 Key**：便于追踪与查询
4. **控制消息体大小**：通常建议小于 256KB
5. **将元数据放入 properties**：避免塞入 body
6. **明确顺序需求**：根据业务选择顺序或普通消息
7. **实现幂等消费**：应对至少一次语义下的重复消息

## 下一步

- [存储](../architecture/storage) - 了解持久化实现
- [生产者](../category/producer) - 学习生产者高级特性
- [消费者](../category/consumer) - 学习消费者高级特性
