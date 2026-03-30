---
sidebar_position: 1
title: 生产者概览
---

# 生产者概览

生产者负责向 RocketMQ Broker 发送消息。RocketMQ-Rust 提供了能力完备的异步生产者实现，支持多种高级特性。

## 创建生产者

```rust
use rocketmq::producer::Producer;
use rocketmq::conf::ProducerOption;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建生产者配置
    let mut producer_option = ProducerOption::default();
    producer_option.set_name_server_addr("localhost:9876");
    producer_option.set_group_name("my_producer_group");

    // 创建并启动生产者
    let producer = Producer::new(producer_option);
    producer.start().await?;

    // 使用 producer ...

    Ok(())
}
```

## 生产者配置

### 基础配置

```rust
let mut producer_option = ProducerOption::default();

// 必填
producer_option.set_name_server_addr("localhost:9876");
producer_option.set_group_name("producer_group");

// 可选
producer_option.set_send_msg_timeout(3000); // 毫秒
producer_option.set_retry_times_when_send_failed(2);
producer_option.set_max_message_size(4 * 1024 * 1024); // 4MB
producer_option.set_compress_msg_body_over_threshold(4 * 1024); // 4KB
```

### 高级配置

```rust
// 启用消息压缩
producer_option.set_compress_msg_body_over_threshold(4 * 1024);

// 设置重试策略
producer_option.set_retry_times_when_send_failed(3);
producer_option.set_retry_next_server(true);

// 配置 TCP 参数
producer_option.set_tcp_transport_try_lock_timeout(1000);
producer_option.set_tcp_transport_connect_timeout(3000);
```

## 消息发送模式

### 同步发送

等待 Broker 确认后返回：

```rust
use rocketmq::model::Message;

let message = Message::new("TopicTest".to_string(), b"Hello".to_vec());
let result = producer.send(message).await?;

println!("Send result: {:?}", result);
```

### 异步发送

非阻塞发送，立即返回：

```rust
use rocketmq::model::Message;

let message = Message::new("TopicTest".to_string(), b"Hello".to_vec());

producer.send_async(message, |result| {
    match result {
        Ok(send_result) => println!("Message sent: {:?}", send_result),
        Err(e) => println!("Send failed: {:?}", e),
    }
}).await?;
```

### 单向发送

只发送不等待结果：

```rust
let message = Message::new("TopicTest".to_string(), b"Hello".to_vec());

producer.send_oneway(message).await?;
// 不返回结果，尽力投递
```

## 队列选择

### 自动负载均衡

默认情况下，消息会按轮询策略分发到多个队列：

```text
Round-robin queue selection:

Message 1 -> Queue 0
Message 2 -> Queue 1
Message 3 -> Queue 2
Message 4 -> Queue 3
Message 5 -> Queue 0
...
```

### 自定义队列选择器

可以通过闭包将特定消息路由到特定队列：

```rust
use rocketmq::common::message::message_queue::MessageQueue;

// 使用选择器闭包发送
let order_id = 12345i64;
producer.send_with_selector(
    message,
    |queues, _msg, order_id: &i64| {
        // 按 order_id 路由，保持同订单消息顺序
        let index = (*order_id % queues.len() as i64) as usize;
        queues.get(index).cloned()
    },
    order_id,
).await?;
```

也可以定义可复用的选择器函数：

```rust
// 定义可复用选择器
fn order_hash_selector(
    queues: &[MessageQueue],
    _msg: &dyn MessageTrait,
    order_id: &i64,
) -> Option<MessageQueue> {
    let index = (*order_id % queues.len() as i64) as usize;
    queues.get(index).cloned()
}

// 使用选择器函数
let order_id = 12345i64;
producer.send_with_selector(message, order_hash_selector, order_id).await?;
```

## 错误处理

### 自动重试

RocketMQ-Rust 会自动重试失败发送：

```rust
// 配置重试行为
producer_option.set_retry_times_when_send_failed(3);
producer_option.set_retry_next_server(true);

// 发送失败时自动重试
match producer.send(message).await {
    Ok(result) => println!("Sent: {:?}", result),
    Err(e) => {
        // 全部重试失败后才会返回
        eprintln!("Failed after retries: {:?}", e);
    }
}
```

### 细粒度错误处理

```rust
use rocketmq::error::MQClientError;

match producer.send(message).await {
    Ok(result) => {
        println!("Message sent successfully");
        println!("Message ID: {}", result.msg_id);
        println!("Queue: {}", result.message_queue);
    }
    Err(MQClientError::BrokerNotFound) => {
        eprintln!("No broker available for topic");
    }
    Err(MQClientError::ServiceNotAvailable) => {
        eprintln!("Broker service not available");
    }
    Err(e) => {
        eprintln!("Send failed: {:?}", e);
    }
}
```

## 性能调优

### 批量发送

一次请求发送多条消息：

```rust
let messages = vec![
    Message::new("TopicTest".to_string(), b"Msg1".to_vec()),
    Message::new("TopicTest".to_string(), b"Msg2".to_vec()),
    Message::new("TopicTest".to_string(), b"Msg3".to_vec()),
];

producer.send_batch(messages).await?;
```

### 消息压缩

自动压缩较大的消息：

```rust
// 压缩阈值设置为 4KB
producer_option.set_compress_msg_body_over_threshold(4 * 1024);

// 大消息会自动压缩
let large_body = vec![0u8; 100 * 1024]; // 100KB
let message = Message::new("TopicTest".to_string(), large_body);
producer.send(message).await?; // 自动压缩
```

### 连接池

```rust
// 配置连接池
producer_option.set_client_channel_max_idle_time_seconds(120);
producer_option.set_client_channel_expire_timeout_seconds(180);
```

## 监控

### 发送统计

```rust
// 生产者会自动采集统计信息
let stats = producer.get_stats()?;
println!("Messages sent: {}", stats.send_success_count);
println!("Messages failed: {}", stats.send_failure_count);
println!("Average RT: {} ms", stats.average_rt);
```

### 指标

RocketMQ-Rust 可暴露以下监控指标：

- `rocketmq_producer_send_success_total`：发送成功总数
- `rocketmq_producer_send_failure_total`：发送失败总数
- `rocketmq_producer_send_latency_ms`：发送延迟（毫秒）
- `rocketmq_producer_queue_size`：当前队列大小

## 最佳实践

1. **使用生产者分组**：将相关生产者归组管理
2. **优雅处理错误**：实现健壮的异常处理逻辑
3. **持续性能监控**：重点关注成功率与延迟
4. **选择合适发送模式**：按场景选择 sync/async/one-way
5. **实现幂等性**：处理可能出现的重复发送
6. **合理配置重试**：设置适当重试次数与超时
7. **优先批量发送**：在高吞吐场景提升效率
8. **设置合理超时**：平衡可靠性与性能

## 下一步

- [发送消息](./sending-messages) - 学习高级消息发送技巧
- [事务消息](./transaction-messages) - 实现事务消息
- [配置](../configuration/overview) - 查看详细配置选项
