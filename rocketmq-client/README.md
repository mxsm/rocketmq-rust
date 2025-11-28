# The Rust Implementation of Apache RocketMQ Client

## Overview

This project is the Rust implementation of Apache RocketMQ client. It is based on the RocketMQ Java client

## How to send message

First, start the RocketMQ NameServer and Broker services.

- [**Send a single message**](#Send-a-single-message)

- [**Send batch messages**](#Send-batch-messages)

- [**Send RPC messages**](#Send-RPC-messages)

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

