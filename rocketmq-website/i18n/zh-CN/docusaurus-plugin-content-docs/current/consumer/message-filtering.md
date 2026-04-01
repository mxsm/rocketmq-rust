---
sidebar_position: 4
title: 消息过滤
---

# 消息过滤

RocketMQ 提供了强大的消息过滤能力，用于减少无效消息处理。

## 基于 Tag 的过滤

### 基础 Tag 过滤

按指定 tags 订阅消息：

```rust
// 订阅单个 tag
consumer.subscribe("OrderEvents", "order_created").await?;

// 订阅多个 tag
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;

// 订阅全部 tag
consumer.subscribe("OrderEvents", "*").await?;
```

### 生产者设置 Tag

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.set_tags("order_created");
producer.send(message).await?;
```

### 排除 Tag

```rust
// 排除指定 tag
consumer.subscribe("OrderEvents", "!(order_cancelled)").await?;
```

## SQL92 表达式过滤

### 启用 SQL92 过滤

需要在 Broker 端开启 SQL92 过滤能力：

```toml
# broker.properties
enablePropertyFilter=true
```

### 使用 SQL92 表达式

```rust
// 数值比较
consumer.subscribe("OrderEvents", "amount > 100").await?;

// 字符串比较
consumer.subscribe("OrderEvents", "region = 'us-west'").await?;

// 逻辑运算
consumer.subscribe("OrderEvents", "amount > 100 AND region = 'us-west'").await?;

// 复杂表达式
consumer.subscribe(
    "OrderEvents",
    "(region = 'us-west' OR region = 'us-east') AND amount > 100"
).await?;
```

### 生产者设置属性

```rust
let mut message = Message::new("OrderEvents".to_string(), body);
message.put_property("amount", "150.00");
message.put_property("region", "us-west");
message.put_property("priority", "high");
producer.send(message).await?;
```

## SQL92 语法示例

### 比较运算符

```rust
// 等于
"region = 'us-west'"

// 不等于
"amount != 0"

// 大于
"amount > 100"

// 小于
"amount < 1000"

// 大于等于
"amount >= 100"

// 小于等于
"amount <= 1000"
```

### 逻辑运算符

```rust
// AND
"amount > 100 AND region = 'us-west'"

// OR
"region = 'us-west' OR region = 'us-east'"

// NOT
"NOT (region = 'us-west')"

// 组合
"(region = 'us-west' OR region = 'us-east') AND amount > 100"
```

### 模式匹配

```rust
// LIKE 运算符
"customer_id LIKE 'VIP%'"

// IS NULL
"description IS NULL"

// IS NOT NULL
"description IS NOT NULL"
```

### BETWEEN 运算符

```rust
// Between
"amount BETWEEN 100 AND 1000"
```

### IN 运算符

```rust
// In list
"region IN ('us-west', 'us-east', 'eu-west')"
```

## 过滤性能

### Tag 过滤

- **性能**：很高（接近 O(1) 哈希匹配）
- **执行位置**：Broker 侧
- **适用场景**：简单分类过滤

### SQL92 过滤

- **性能**：中等（需要表达式求值）
- **执行位置**：Broker 侧
- **适用场景**：复杂过滤逻辑

### 客户端过滤

```rust
impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        for msg in messages {
            let region = msg.get_property("region");
            let amount: f64 = msg.get_property("amount")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);

            if region == Some("us-west".to_string()) && amount > 100.0 {
                process_message(&msg);
            }
        }
        ConsumeResult::Success
    }
}
```

- **性能**：网络开销较高（消息先到客户端再过滤）
- **执行位置**：Consumer 侧
- **适用场景**：复杂业务规则判断

## 最佳实践

1. **优先使用 Tag 过滤**：成本最低、性能最好
2. **尽量在 Broker 侧过滤**：减少网络传输和客户端开销
3. **设计有意义的 Tag 体系**：便于演进与治理
4. **将常用过滤字段写入消息属性（properties）**：便于 SQL92 表达式过滤
5. **避免过于复杂的表达式**：保持 SQL92 简洁
6. **考虑放在客户端二次过滤**：复杂业务逻辑

## 示例

### 订单处理

```rust
// Producer
let mut message = Message::new("OrderEvents".to_string(), order_json);
message.set_tags("order_created");
message.put_property("region", &order.region);
message.put_property("amount", &order.amount.to_string());
producer.send(message).await?;

// Consumer
consumer.subscribe(
    "OrderEvents",
    "order_created AND amount > 1000"
).await?;
```

### 日志聚合

```rust
// Producer
let mut message = Message::new("ApplicationLogs".to_string(), log_entry);
message.set_tags(&log.level); // ERROR, WARN, INFO, DEBUG
message.put_property("service", &log.service);
message.put_property("environment", &log.environment);
producer.send(message).await?;

// Consumer - 仅订阅生产环境 ERROR 日志
consumer.subscribe(
    "ApplicationLogs",
    "ERROR AND environment = 'production'"
).await?;
```

### 事件路由

```rust
// Producer
let mut message = Message::new("UserEvents".to_string(), event_json);
message.set_tags(&event.event_type);
message.put_property("user_tier", &user.tier);
producer.send(message).await?;

// Consumer - 高优先级事件
consumer.subscribe(
    "UserEvents",
    "(login OR logout OR purchase) AND user_tier = 'premium'"
).await?;
```

## 下一步

- [配置](../configuration) - 配置过滤相关参数
- [生产者指南](../producer/overview) - 设置消息标签与属性
- [消费者概览](./overview) - 查看消费模型与位点管理
