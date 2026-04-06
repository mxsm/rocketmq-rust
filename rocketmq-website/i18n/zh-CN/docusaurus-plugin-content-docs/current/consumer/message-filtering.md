---
sidebar_position: 4
title: 消息过滤
---

# 消息过滤

RocketMQ 的过滤机制可以帮助消费者减少无关消息处理。

## 基于 Tag 的过滤

### 基础 Tag 订阅

```rust
// 订阅单个 tag
consumer.subscribe("OrderEvents", "order_created").await?;

// 订阅多个 tag
consumer
    .subscribe("OrderEvents", "order_created || order_paid")
    .await?;

// 订阅全部 tag
consumer.subscribe("OrderEvents", "*").await?;
```

### 生产者设置 Tag

```rust
use rocketmq_common::common::message::message_single::Message;

let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .tags("order_created")
    .build()?;

producer.send(message).await?;
```

## 使用消息属性补充过滤维度

生产者可以附加结构化属性，供下游做更细粒度判断。

```rust
let message = Message::builder()
    .topic("OrderEvents")
    .body(body)
    .tags("order_created")
    .raw_property("amount", "150.00")?
    .raw_property("region", "us-west")?
    .raw_property("priority", "high")?
    .build()?;

producer.send(message).await?;
```

## 客户端二次过滤（属性判断）

当业务规则复杂或经常变化时，可在监听器里做二次筛选。

```rust
use cheetah_string::CheetahString;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_common::common::message::MessageTrait;

consumer.register_message_listener_concurrently(|msgs, _ctx| {
    let region_key = CheetahString::from_static_str("region");
    let amount_key = CheetahString::from_static_str("amount");

    for msg in msgs {
        let region = msg
            .property(&region_key)
            .map(|v| v.to_string())
            .unwrap_or_default();

        let amount = msg
            .property(&amount_key)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);

        if region == "us-west" && amount > 100.0 {
            // process_message(msg);
        }
    }

    Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
});
```

## SQL92 说明

RocketMQ Broker 支持 SQL92 服务端过滤，但当前项目的公开消费者示例主要围绕 Tag 表达式订阅与客户端属性二次过滤。

如果你计划在生产环境使用 SQL92，请先结合 Broker 配置与客户端能力进行集成验证。

## 过滤性能

### Tag 过滤

- 性能：高
- 执行位置：Broker 侧
- 适用：稳定的事件分类场景

### 客户端属性过滤

- 性能：低于 Tag 过滤（消息仍会先到消费者）
- 执行位置：Consumer 侧
- 适用：复杂或动态业务规则

## 最佳实践

1. 优先使用 Tag 作为第一层过滤。
2. 设计稳定、可维护的 Tag 体系。
3. 将高频查询维度放入消息属性。
4. 客户端二次过滤逻辑保持轻量。
5. 监控过滤后的命中率，反向优化生产者打标策略。

## 示例

### 订单处理

```rust
// Producer
let message = Message::builder()
    .topic("OrderEvents")
    .body(order_json)
    .tags("order_created")
    .raw_property("region", &order.region)?
    .raw_property("amount", order.amount.to_string())?
    .build()?;
producer.send(message).await?;

// Consumer
consumer.subscribe("OrderEvents", "order_created").await?;
```

### 日志聚合

```rust
// Producer
let message = Message::builder()
    .topic("ApplicationLogs")
    .body(log_entry)
    .tags(&log.level) // ERROR, WARN, INFO, DEBUG
    .raw_property("service", &log.service)?
    .raw_property("environment", &log.environment)?
    .build()?;
producer.send(message).await?;

// Consumer
consumer.subscribe("ApplicationLogs", "ERROR || WARN").await?;
```

### 事件路由

```rust
// Producer
let message = Message::builder()
    .topic("UserEvents")
    .body(event_json)
    .tags(&event.event_type)
    .raw_property("user_tier", &user.tier)?
    .build()?;
producer.send(message).await?;

// Consumer
consumer.subscribe("UserEvents", "login || logout || purchase").await?;
```

## 下一步

- [Broker 配置](../configuration/broker-config) - 配置过滤相关参数
- [生产者指南](../producer/overview) - 正确设置 tags 与 properties
- [消费者概览](./overview) - 理解消费模型与位点管理
