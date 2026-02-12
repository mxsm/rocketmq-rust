# RocketMQ Producer Implementation Guide

This document explains the implementation status and usage methods of RocketMQ Rust version's `DefaultMQProducer` and `TransactionMQProducer`.

## Table of Contents

- [Quick Overview](#quick-overview)
- [Core Sending Methods](#core-sending-methods)
- [Batch Sending](#batch-sending)
- [Request-Reply Pattern](#request-reply-pattern)
- [Other Features](#other-features)
- [Transactional Messaging](#transactional-messaging)
- [Configuration](#configuration)
- [Implementation Differences](#implementation-differences)

## Quick Overview

### Implementation Status

| Feature Category | Status | Description |
|-----------------|--------|------|
| Basic Sending | ✅ Implemented | Supports sync, async, one-way sending |
| Send to Specified Queue | ✅ Implemented | Supports sending to specified MessageQueue |
| Selector-based Sending | ✅ Implemented | Supports custom queue selector |
| Batch Sending | ✅ Implemented | Supports batch sync/async sending |
| Request-Reply | ✅ Implemented | Supports sync and async request-reply pattern |
| Message Recall | ✅ Implemented | Supports message recall by topic and handle |
| Transactional Messaging | ✅ Separate Implementation | Implemented via `TransactionMQProducer`, see below |
| Auto-batching | ✅ Implemented | Both versions support auto-batching |
| Backpressure Control | ✅ Implemented | Configurable backpressure for async mode |

### Code Locations

- **Rust Implementation**: [rocketmq-client/src/producer/default_mq_producer.rs](../rocketmq-client/src/producer/default_mq_producer.rs)
- **Java Reference**: [Apache RocketMQ DefaultMQProducer.java](https://github.com/apache/rocketmq/blob/develop/client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java)

## Core Sending Methods

### 1. Basic Sending Methods

#### Synchronous Sending

```rust
// Usage example
use rocketmq_client_rust::producer::DefaultMQProducer;

let mut producer = DefaultMQProducer::builder()
    .producer_group("my_producer_group")
    .namesrv_addr("localhost:9876")
    .build()
    .await?;

// Send single message (sync)
let msg = Message::builder()
    .topic("test_topic")
    .body("Hello RocketMQ")
    .build()
    .unwrap();

let send_result = producer.send(msg).await?;
```

**Method Signature**:
```rust
pub async fn send<M>(&mut self, msg: M) -> RocketMQResult<Option<SendResult>>
where
    M: MessageTrait + Send + Sync
```

#### Asynchronous Sending (with callback)

```rust
// Usage example
producer.send_with_callback(msg, |result, error| {
    if let Some(send_result) = result {
        println!("Send succeeded: {:?}", send_result);
    }
    if let Some(err) = error {
        println!("Send failed: {}", err);
    }
}).await?;
```

**Method Signature**:
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

#### One-way Sending

```rust
let send_result = producer.send_oneway(msg).await?;
```

**Method Signature**:
```rust
pub async fn send_oneway<M>(&mut self, msg: M) -> RocketMQResult<()>
where
    M: MessageTrait + Send + Sync
```

### 2. Send to Specified Queue

```rust
use rocketmq_client_rust::common::message::message_queue::MessageQueue;

// Send to specified queue
let mq = MessageQueue::new();
// Set queue properties...

let send_result = producer.send_to_queue(msg, mq).await?;
```

**Method Signature**:
```rust
pub async fn send_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> RocketMQResult<Option<SendResult>>
where
    M: MessageTrait + Send + Sync
```

### 3. Selector-based Sending

```rust
// Use custom selector for queue selection
let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, _arg: &dyn std::any::Any| {
    // Custom selection logic
    queues.first().cloned()
};

let send_result = producer.send_with_selector(msg, selector, 1).await?;
```

**Method Signature**:
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

## Batch Sending

### Batch Synchronous Sending

```rust
let msgs = vec![msg1, msg2, msg3];
let send_result = producer.send_batch(msgs).await?;
```

**Method Signature**:
```rust
pub async fn send_batch<M>(
    &mut self,
    msgs: Vec<M>,
) -> RocketMQResult<SendResult>
where
    M: MessageTrait + Send + Sync
```

### Batch Asynchronous Sending

```rust
producer.send_batch_with_callback(msgs, |result, error| {
    // Handle sending result
}).await?;
```

### Batch Sending to Specified Queue

```rust
let mq = MessageQueue::new();
producer.send_batch_to_queue_with_callback(msgs, mq, callback).await?;
```

### Auto-batching Configuration

Both versions support auto-batching functionality:

| Configuration | Java Field | Rust Field | Description |
|------------|-------------|-----------|------|
| Auto-batching switch | `autoBatch` | `auto_batch` | Whether to enable auto-batching |
| Batch accumulator | `produceAccumulator` | `produce_accumulator` | Custom accumulator implementation |
| Max batch delay | `batchMaxDelayMs` | `batch_max_delay_ms` | Max hold time for message batching (milliseconds) |
| Max batch size | `batchMaxBytes` | `batch_max_bytes` | Max message body size for a single batch (bytes) |
| Total batch max bytes | `totalBatchMaxBytes` | `total_batch_max_bytes` | Max total message body size for accumulator (bytes) |

## Request-Reply Pattern

### Synchronous Request

```rust
use std::time::Duration;

// Send request and wait for response
let response_msg = producer.request_with_timeout(
    msg,
    Duration::from_secs(3)
).await?;
```

**Method Signature**:
```rust
pub async fn request_with_timeout<M>(
    &mut self,
    msg: M,
    timeout: u64,
) -> RocketMQResult<Box<dyn MessageTrait>>
```

### Asynchronous Request

```rust
producer.request_with_callback(msg, |response, error| {
    // Handle response
}, timeout).await?;
```

## Other Features

### Lifecycle Management

```rust
// Start Producer
producer.start().await?;

// Shutdown Producer
producer.shutdown().await?;
```

### Message Recall

```rust
// Recall message by topic and recallHandle
let result = producer.recall_message("test_topic", "recall_handle_123").await?;
```

## Transactional Messaging

**Important**: Transactional messaging functionality is implemented via `TransactionMQProducer`, not `DefaultMQProducer`.

### TransactionMQProducer Overview

`TransactionMQProducer` is a specialized producer for handling transactional messages, providing the following capabilities:

| Feature | Description |
|---------|-------------|
| Local transaction execution | Execute transaction logic locally |
| Transaction commit | Commit transaction to Broker |
| Transaction rollback | Supports transaction rollback |
| Transaction listener | Customize transaction behavior via `TransactionListener` trait |

### Usage Example

```rust
use cheetah_string::CheetahString;
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;

// 1. Create transaction listener (optional)
struct MyTransactionListener;

impl TransactionListener for MyTransactionListener {
    // Implement transaction listener interface
}

// 2. Build TransactionMQProducer
let mut producer = TransactionMQProducer::builder()
    .producer_group("my_transaction_producer_group")
    .namesrv_addr("localhost:9876")
    .transaction_listener(MyTransactionListener)
    .build()
    .await?;

// 3. Start Producer
producer.start().await?;

// 4. Send transactional message
let msg = Message::builder()
    .topic("transaction_topic")
    .body("Hello Transactional MQ!")
    .build()
    .unwrap();

let send_result = producer.send_message_in_transaction(
    msg,
    "transaction_arg" // transaction argument
).await?;

// 5. Handle transaction result
match send_result {
    Ok(result) => {
        println!("Transactional message sent successfully: {:?}", result);
    }
    Err(e) => {
        eprintln!("Transactional message send failed: {}", e);
    }
}

// 6. Shutdown Producer
producer.shutdown().await?;
```

### TransactionMQProducer Configuration

| Configuration | Description |
|-----------|-------------|
| `transaction_listener` | Transaction listener interface implementation |
| `check_thread_pool_min_size` | Check thread pool minimum size |
| `check_thread_pool_max_size` | Check thread pool maximum size |
| `check_request_hold_max` | Max pending requests |

**Code Location**: [rocketmq-client/src/producer/transaction_mq_producer.rs](../rocketmq-client/src/producer/transaction_mq_producer.rs)

## Configuration

### ProducerConfig Default Values

| Parameter | Default Value | Description |
|----------|---------------|----------|
| `send_msg_timeout` | 3000 | Send timeout (milliseconds) |
| `compress_msg_body_over_howmuch` | 4096 | Message compression threshold (bytes) |
| `retry_times_when_send_failed` | 2 | Retry times when sync send failed |
| `retry_times_when_send_async_failed` | 2 | Retry times when async send failed |
| `default_topic_queue_nums` | 4 | Default topic queue numbers |
| `max_message_size` | 4194304 (4M) | Max message size |

### Compression Configuration

| Parameter | Default Value |
|----------|--------|
| `compress_level` | 5 |
| `compress_type` | ZLIB |

### Backpressure Control

| Configuration | Java Default Value | Rust Default Value |
|-----------------------------|-------------|
| Async backpressure switch | `enableBackpressureForAsyncMode` = false | `enable_backpressure_for_async_mode` = false |
| Async send num limit | `backpressureForAsyncSendNum` = 1024 | `back_pressure_for_async_send_num` = 1024 |
| Async send size limit | `backpressureForAsyncSendSize` = 100M | `back_pressure_for_async_send_size` = 100M |

**Note**: Rust version's default async send num limit is corrected to 1024, matching Java.

## Implementation Differences

### 1. Async Model

**Java Version**:
- Uses traditional callback interfaces `SendCallback`, `RequestCallback`
- Uses `ExecutorService` for executing async tasks

**Rust Version**:
- Uses `async/await` async syntax
- Uses closures (Fn trait) instead of dedicated callback interfaces
- Leverages `tokio` runtime for async task handling

### 2. Error Handling

**Java Version**:
- Throws checked exceptions: `MQClientException`, `MQBrokerException`, `RemotingException`, `InterruptedException`

**Rust Version**:
- Uses `Result<T, E>` type
- Returns `RocketMQResult<T>` wrapping errors

### 3. Type System

**Java Version**:
- Uses interfaces and inheritance: `Message`, `MessageQueue`, `MessageQueueSelector`
- Uses generics: `Collection<Message>`

**Rust Version**:
- Uses Trait: `MessageTrait`
- Uses generic constraints: `M: MessageTrait + Send + Sync`
- Uses `Vec<T>` instead of collections

### 4. Mutability

**Java Version**:
- Most methods can be safely used in multi-threaded environments
- Uses `volatile` keyword for visibility guarantees

**Rust Version**:
- Uses `&mut self` indicating mutable borrow
- Compile-time thread safety guarantees

---

> **Document Generation Time**: 2026-02-12
> **Rust Code Location**: [rocketmq-client/src/producer/default_mq_producer.rs](../rocketmq-client/src/producer/default_mq_producer.rs)
> **Java Code Location**: [DefaultMQProducer.java](https://github.com/apache/rocketmq/blob/develop/client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java)
