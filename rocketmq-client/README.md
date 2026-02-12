# The Rust Implementation of Apache RocketMQ Client

## Overview

This project is the Rust implementation of Apache RocketMQ client. It provides feature parity with the RocketMQ Java client, supporting all major Producer capabilities.

## Producer Features

The RocketMQ Rust client provides comprehensive Producer functionality through two main implementations:

| Feature | DefaultMQProducer | TransactionMQProducer |
|---------|-------------------|----------------------|
| Basic Send (Sync/Async/Oneway) | ✅ | ✅ |
| Send to Specific Queue | ✅ | ✅ |
| Send with Selector | ✅ | ✅ |
| Batch Send | ✅ | ✅ |
| Request-Reply (RPC) | ✅ | ✅ |
| Message Recall | ✅ | ✅ |
| Transaction Messages | ❌ | ✅ |
| Auto Batch Sending | ✅ | ✅ |
| Backpressure Control | ✅ | ✅ |

### Detailed Feature List

#### 1. Basic Sending Methods
- **Synchronous Send**: Blocks until receive broker response
- **Asynchronous Send**: Non-blocking with callback
- **Oneway Send**: Fire-and-forget, no response expected
- **Timeout Support**: Configurable per-request timeout

#### 2. Queue Selection
- **Auto Selection**: Default load balancing across queues
- **Specific Queue**: Send to designated MessageQueue
- **Custom Selector**: Implement `MessageQueueSelector` for custom routing logic

#### 3. Batch Sending
- **Manual Batch**: Send multiple messages together
- **Auto Batch**: Automatic batching with configurable thresholds
- **Batch to Queue**: Send batches to specific queues

#### 4. Request-Reply Pattern (RPC)
- **Synchronous Request**: Send request and wait for response
- **Asynchronous Request**: Non-blocking with callback
- **Request with Selector**: Route requests via custom selector
- **Request to Queue**: Send requests to specific queues

#### 5. Transaction Messages (TransactionMQProducer only)
- **Local Transaction Execution**: Execute transaction logic locally
- **Transaction Commit/Rollback**: Full transaction state management
- **Transaction Listener**: Custom transaction behavior via `TransactionListener` trait

#### 6. Advanced Features
- **Message Recall**: Recall messages by topic and handle
- **Compression**: Automatic compression for large messages
- **Backpressure**: Configurable async backpressure control
- **Namespace Support**: Multi-tenant namespace isolation
- **Trace Integration**: Message tracing support

## How to send message

First, start the RocketMQ NameServer and Broker services.

- [**Send a single message**](#Send-a-single-message)

- [**Send batch messages**](#Send-batch-messages)

- [**Send RPC messages**](#Send-RPC-messages)

- [**Send transaction messages**](#Send-transaction-messages)

- [**Send with custom selector**](#Send-with-custom-selector)

- [**Message recall**](#Message-recall)

[**For more examples, you can check here**](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/examples)

### Send a single message

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::Result;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_rust::rocketmq;

pub const MESSAGE_COUNT: usize = 1;
pub const PRODUCER_GROUP: &str = "please_rename_unique_group_name";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "TagA";

#[rocketmq::main]
pub async fn main() -> Result<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let builder = DefaultMQProducer::builder();

    let mut producer = builder
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();

    producer.start().await?;

    for _ in 0..10 {
        let message = Message::with_tags(TOPIC, TAG, "Hello RocketMQ".as_bytes());

        let send_result = producer.send_with_timeout(message, 2000).await?;
        println!("send result: {}", send_result);
    }
    producer.shutdown().await;

    Ok(())
}
```

### Send batch messages

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "BatchProducerGroupName";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "TagA";

#[rocketmq::main]
pub async fn main() -> rocketmq_client_rust::Result<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let builder = DefaultMQProducer::builder();

    let mut producer = builder
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();
    producer.start().await?;

    let mut messages = Vec::new();
    messages.push(Message::with_keys(
        TOPIC,
        TAG,
        "OrderID001",
        "Hello world 0".as_bytes(),
    ));
    messages.push(Message::with_keys(
        TOPIC,
        TAG,
        "OrderID002",
        "Hello world 1".as_bytes(),
    ));
    messages.push(Message::with_keys(
        TOPIC,
        TAG,
        "OrderID003",
        "Hello world 2".as_bytes(),
    ));
    let send_result = producer.send_batch(messages).await?;
    println!("send result: {}", send_result);
    Ok(())
}
```

### Send RPC messages

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::Result;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_rust::rocketmq;

pub const MESSAGE_COUNT: usize = 1;
pub const PRODUCER_GROUP: &str = "please_rename_unique_group_name";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "RequestTopic";
pub const TAG: &str = "TagA";

#[rocketmq::main]
pub async fn main() -> Result<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let builder = DefaultMQProducer::builder();

    let mut producer = builder
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();

    producer.start().await?;
    let ttl = 3000;
    let message = producer
        .request(
            Message::with_tags(TOPIC, "", "Hello RocketMQ".as_bytes()),
            ttl,
        )
        .await?;
    println!("send result: {:?}", message);
    producer.shutdown().await;

    Ok(())
}
```

### Send transaction messages

```rust
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "transaction_producer_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TransactionTopic";

// Custom transaction listener
struct MyTransactionListener;

impl TransactionListener for MyTransactionListener {
    fn execute_local_transaction(
        &mut self,
        msg: &dyn rocketmq_common::common::message::MessageTrait,
        arg: &dyn std::any::Any,
    ) -> rocketmq_client_rust::Result<rocketmq_client_rust::producer::transaction_listener::LocalTransactionState> {
        // Implement local transaction logic here
        println!("Executing local transaction for message: {:?}", msg.get_keys());
        Ok(rocketmq_client_rust::producer::transaction_listener::LocalTransactionState::CommitMessage)
    }

    fn check_local_transaction(
        &mut self,
        msg: &dyn rocketmq_common::common::message::MessageTrait,
    ) -> rocketmq_client_rust::Result<rocketmq_client_rust::producer::transaction_listener::LocalTransactionState> {
        // Check transaction status
        Ok(rocketmq_client_rust::producer::transaction_listener::LocalTransactionState::CommitMessage)
    }
}

#[rocketmq::main]
pub async fn main() -> rocketmq_client_rust::Result<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = TransactionMQProducer::builder()
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .transaction_listener(MyTransactionListener)
        .build()
        .await?;

    producer.start().await?;

    let msg = Message::with_tags(TOPIC, "", "Hello Transactional RocketMQ".as_bytes());
    let result = producer.send_message_in_transaction(msg, Some("transaction_arg")).await?;

    println!("Transaction send result: {:?}", result);
    producer.shutdown().await;

    Ok(())
}
```

### Send with custom selector

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "selector_producer_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "SelectorTopic";

#[rocketmq::main]
pub async fn main() -> rocketmq_client_rust::Result<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();

    producer.start().await?;

    let msg = Message::with_tags(TOPIC, "", "Hello RocketMQ with Selector".as_bytes());

    // Custom queue selector - routes messages based on key
    let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, arg: &dyn std::any::Any| {
        if let Some(order_id) = arg.downcast_ref::<String>() {
            // Simple hash-based routing
            let hash = order_id.chars().map(|c| c as usize).sum::<usize>();
            let index = hash % queues.len();
            Some(queues[index].clone())
        } else {
            queues.first().cloned()
        }
    };

    let order_id = "ORDER12345".to_string();
    let result = producer.send_with_selector(msg, selector, order_id).await?;

    println!("Send result: {:?}", result);
    producer.shutdown().await;

    Ok(())
}
```

### Message recall

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use cheetah_string::CheetahString;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "recall_producer_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "RecallTopic";

#[rocketmq::main]
pub async fn main() -> rocketmq_client_rust::Result<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();

    producer.start().await?;

    // Send a message
    let msg = Message::with_tags(TOPIC, "", "Hello RocketMQ - Recallable".as_bytes());
    let send_result = producer.send(msg).await?;

    println!("Send result: {:?}", send_result);

    // Recall the message (if recall handle is available)
    // The recall handle is typically returned in the send result for recallable messages
    if let Some(recall_handle) = send_result.and_then(|r| r.recall_handle()) {
        let recall_result = producer.recall_message(
            CheetahString::from_static_str(TOPIC),
            CheetahString::from(recall_handle.as_str())
        ).await?;

        println!("Recall result: {}", recall_result);
    }

    producer.shutdown().await;

    Ok(())
}
```

