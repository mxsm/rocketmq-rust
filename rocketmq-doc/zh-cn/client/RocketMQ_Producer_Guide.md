# RocketMQ Producer 实现指南

本文档解释了 RocketMQ Rust 版本的 `DefaultMQProducer` 和 `TransactionMQProducer` 的实现状态和使用方法。

## 目录

- [快速概览](#快速概览)
- [核心发送方法](#核心发送方法)
- [批量发送](#批量发送)
- [请求-应答模式](#请求-应答模式)
- [其他功能](#其他功能)
- [事务消息](#事务消息)
- [配置](#配置)
- [实现差异](#实现差异)

## 快速概览

### 实现状态

| 功能分类 | 状态 | 描述 |
|---------|------|------|
| 基本发送 | ✅ 已实现 | 支持同步、异步、单向发送 |
| 发送到指定队列 | ✅ 已实现 | 支持发送到指定 MessageQueue |
| 基于选择器的发送 | ✅ 已实现 | 支持自定义队列选择器 |
| 批量发送 | ✅ 已实现 | 支持批量同步/异步发送 |
| 请求-应答 | ✅ 已实现 | 支持同步和异步请求-应答模式 |
| 消息撤回 | ✅ 已实现 | 支持按 topic 和 handle 撤回消息 |
| 事务消息 | ✅ 单独实现 | 通过 `TransactionMQProducer` 实现，见下文 |
| 自动批量发送 | ✅ 已实现 | 两个版本都支持自动批量发送 |
| 背压控制 | ✅ 已实现 | 异步模式可配置背压 |

### 代码位置

- **Rust 实现**: [rocketmq-client/src/producer/default_mq_producer.rs](../../rocketmq-client/src/producer/default_mq_producer.rs)
- **Java 参考实现**: [Apache RocketMQ DefaultMQProducer.java](https://github.com/apache/rocketmq/blob/develop/client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java)

## 核心发送方法

### 1. 基本发送方法

#### 同步发送

```rust
// 使用示例
use rocketmq_client_rust::producer::DefaultMQProducer;

let mut producer = DefaultMQProducer::builder()
    .producer_group("my_producer_group")
    .namesrv_addr("localhost:9876")
    .build()
    .await?;

// 发送单条消息（同步）
let msg = Message::builder()
    .topic("test_topic")
    .body("Hello RocketMQ")
    .build()
    .unwrap();

let send_result = producer.send(msg).await?;
```

**方法签名**:
```rust
pub async fn send<M>(&mut self, msg: M) -> RocketMQResult<Option<SendResult>>
where
    M: MessageTrait + Send + Sync
```

#### 异步发送（带回调）

```rust
// 使用示例
producer.send_with_callback(msg, |result, error| {
    if let Some(send_result) = result {
        println!("发送成功: {:?}", send_result);
    }
    if let Some(err) = error {
        println!("发送失败: {}", err);
    }
}).await?;
```

**方法签名**:
```rust
pub async fn send_with_callback<M, F>(
    &mut self,
    msg: M,
    send_callback: F,
) -> RocketMQResult<()>
where
    M: MessageTrait + Send + Sync,
    F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static
```

#### 单向发送

```rust
let send_result = producer.send_oneway(msg).await?;
```

**方法签名**:
```rust
pub async fn send_oneway<M>(&mut self, msg: M) -> RocketMQResult<()>
where
    M: MessageTrait + Send + Sync
```

### 2. 发送到指定队列

```rust
use rocketmq_client_rust::common::message::message_queue::MessageQueue;

// 发送到指定队列
let mq = MessageQueue::new();
// 设置队列属性...

let send_result = producer.send_to_queue(msg, mq).await?;
```

**方法签名**:
```rust
pub async fn send_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> RocketMQResult<Option<SendResult>>
where
    M: MessageTrait + Send + Sync
```

### 3. 基于选择器的发送

```rust
// 使用自定义选择器进行队列选择
let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, _arg: &dyn std::any::Any| {
    // 自定义选择逻辑
    queues.first().cloned()
};

let send_result = producer.send_with_selector(msg, selector, 1).await?;
```

**方法签名**:
```rust
pub async fn send_with_selector<M, S, T>(
    &mut self,
    msg: M,
    selector: S,
    arg: T,
) -> RocketMQResult<Option<SendResult>>
where
    M: MessageTrait + Send + Sync,
    S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
```

## 批量发送

### 批量同步发送

```rust
let msgs = vec![msg1, msg2, msg3];
let send_result = producer.send_batch(msgs).await?;
```

**方法签名**:
```rust
pub async fn send_batch<M>(
    &mut self,
    msgs: Vec<M>,
) -> RocketMQResult<SendResult>
where
    M: MessageTrait + Send + Sync
```

### 批量异步发送

```rust
producer.send_batch_with_callback(msgs, |result, error| {
    // 处理发送结果
}).await?;
```

### 批量发送到指定队列

```rust
let mq = MessageQueue::new();
producer.send_batch_to_queue_with_callback(msgs, mq, callback).await?;
```

### 自动批量发送配置

两个版本都支持自动批量发送功能：

| 配置项 | Java 字段 | Rust 字段 | 描述 |
|---------|-----------|-----------|------|
| 自动批量开关 | `autoBatch` | `auto_batch` | 是否启用自动批量发送 |
| 批量累积器 | `produceAccumulator` | `produce_accumulator` | 自定义累积器实现 |
| 最大批量延迟 | `batchMaxDelayMs` | `batch_max_delay_ms` | 消息批量保存的最大时间（毫秒） |
| 单批次最大大小 | `batchMaxBytes` | `batch_max_bytes` | 单批次消息体的最大大小（字节） |
| 总批量最大大小 | `totalBatchMaxBytes` | `total_batch_max_bytes` | 累积器消息体总大小的最大值（字节） |

## 请求-应答模式

### 同步请求

```rust
use std::time::Duration;

// 发送请求并等待响应
let response_msg = producer.request_with_timeout(
    msg,
    Duration::from_secs(3)
).await?;
```

**方法签名**:
```rust
pub async fn request_with_timeout<M>(
    &mut self,
    msg: M,
    timeout: u64,
) -> RocketMQResult<Box<dyn MessageTrait>>
```

### 异步请求

```rust
producer.request_with_callback(msg, |response, error| {
    // 处理响应
}, timeout).await?;
```

## 其他功能

### 生命周期管理

```rust
// 启动 Producer
producer.start().await?;

// 关闭 Producer
producer.shutdown().await?;
```

### 消息撤回

```rust
// 按 topic 和 recallHandle 撤回消息
let result = producer.recall_message("test_topic", "recall_handle_123").await?;
```

## 事务消息

**重要说明**: 事务消息功能通过 `TransactionMQProducer` 实现，而非 `DefaultMQProducer`。

### TransactionMQProducer 概述

`TransactionMQProducer` 是专门用于处理事务消息的 Producer，提供以下功能：

| 功能 | 描述 |
|-----|------|
| 本地事务执行 | 在本地执行事务逻辑 |
| 事务提交 | 向 Broker 提交事务 |
| 事务回滚 | 支持事务回滚 |
| 事务监听器 | 通过 `TransactionListener` trait 自定义事务行为 |

### 使用示例

```rust
use cheetah_string::CheetahString;
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;

// 1. 创建事务监听器（可选）
struct MyTransactionListener;

impl TransactionListener for MyTransactionListener {
    // 实现事务监听器接口
}

// 2. 构建 TransactionMQProducer
let mut producer = TransactionMQProducer::builder()
    .producer_group("my_transaction_producer_group")
    .namesrv_addr("localhost:9876")
    .transaction_listener(MyTransactionListener)
    .build()
    .await?;

// 3. 启动 Producer
producer.start().await?;

// 4. 发送事务消息
let msg = Message::builder()
    .topic("transaction_topic")
    .body("Hello Transactional MQ!")
    .build()
    .unwrap();

let send_result = producer.send_message_in_transaction(
    msg,
    "transaction_arg" // 事务参数
).await?;

// 5. 处理事务结果
match send_result {
    Ok(result) => {
        println!("事务消息发送成功: {:?}", result);
    }
    Err(e) => {
        eprintln!("事务消息发送失败: {}", e);
    }
}

// 6. 关闭 Producer
producer.shutdown().await?;
```

### TransactionMQProducer 配置

| 配置项 | 描述 |
|--------|------|
| `transaction_listener` | 事务监听器接口实现 |
| `check_thread_pool_min_size` | 检查线程池最小大小 |
| `check_thread_pool_max_size` | 检查线程池最大大小 |
| `check_request_hold_max` | 最大挂起请求数 |

**代码位置**: [rocketmq-client/src/producer/transaction_mq_producer.rs](../../rocketmq-client/src/producer/transaction_mq_producer.rs)

## 配置

### ProducerConfig 默认值

| 参数 | 默认值 | 描述 |
|------|--------|------|
| `send_msg_timeout` | 3000 | 发送超时时间（毫秒） |
| `compress_msg_body_over_howmuch` | 4096 | 消息压缩阈值（字节） |
| `retry_times_when_send_failed` | 2 | 同步发送失败时的重试次数 |
| `retry_times_when_send_async_failed` | 2 | 异步发送失败时的重试次数 |
| `default_topic_queue_nums` | 4 | 默认 Topic 队列数 |
| `max_message_size` | 4194304 (4M) | 最大消息大小 |

### 压缩配置

| 参数 | 默认值 |
|------|--------|
| `compress_level` | 5 |
| `compress_type` | ZLIB |

### 背压控制

| 配置项 | Java 默认值 | Rust 默认值 |
|---------|-------------|-------------|
| 异步背压开关 | `enableBackpressureForAsyncMode` = false | `enable_backpressure_for_async_mode` = false |
| 异步发送数量限制 | `backpressureForAsyncSendNum` = 1024 | `back_pressure_for_async_send_num` = 1024 |
| 异步发送大小限制 | `backpressureForAsyncSendSize` = 100M | `back_pressure_for_async_send_size` = 100M |

**注意**: Rust 版本的默认异步发送数量限制已修正为 1024，与 Java 版本一致。

## 实现差异

### 1. 异步模型

**Java 版本**:
- 使用传统的回调接口 `SendCallback`、`RequestCallback`
- 使用 `ExecutorService` 执行异步任务

**Rust 版本**:
- 使用 `async/await` 异步语法
- 使用闭包（Fn trait）而非专用的回调接口
- 利用 `tokio` 运行时处理异步任务

### 2. 错误处理

**Java 版本**:
- 抛出受检异常: `MQClientException`、`MQBrokerException`、`RemotingException`、`InterruptedException`

**Rust 版本**:
- 使用 `Result<T, E>` 类型
- 返回 `RocketMQResult<T>` 包装错误

### 3. 类型系统

**Java 版本**:
- 使用接口和继承: `Message`、`MessageQueue`、`MessageQueueSelector`
- 使用泛型: `Collection<Message>`

**Rust 版本**:
- 使用 Trait: `MessageTrait`
- 使用泛型约束: `M: MessageTrait + Send + Sync`
- 使用 `Vec<T>` 而非集合类

### 4. 可变性

**Java 版本**:
- 大多数方法可以安全地在多线程环境中使用
- 使用 `volatile` 关键字保证可见性

**Rust 版本**:
- 使用 `&mut self` 表示可变借用
- 编译时保证线程安全

---

> **文档生成时间**: 2026-02-12
> **Rust 代码位置**: [rocketmq-client/src/producer/default_mq_producer.rs](../../rocketmq-client/src/producer/default_mq_producer.rs)
> **Java 代码位置**: [DefaultMQProducer.java](https://github.com/apache/rocketmq/blob/develop/client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java)
