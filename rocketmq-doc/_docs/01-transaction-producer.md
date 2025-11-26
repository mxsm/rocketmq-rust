---
title: "Transaction Producer"
permalink: /docs/producer/transaction-producer/
excerpt: "Transaction Producer in RocketMQ Rust"
last_modified_at: 2025-05-31T20:14:05
redirect_from:
  - /theme-setup/
toc: false
classes: wide
---

![Architecture](/assets/images/transaction-message-flow.png)

## ⚙️ Principle of RocketMQ Transaction Message Model

The transaction message process of RocketMQ is divided into three steps:

### 1. **Send "Half Message" (Prepared Message)**

The producer sends a special "**half message**", and this message:

- It will not be visible to consumers.
- It indicates that the message has been sent successfully but has not been submitted yet.

### 2. Execute local transactions

After successfully sending a half-message, the producer executes local transaction logic (such as database operations, etc.).

### 3. **Submit or roll back transaction messages**

- Local transaction is executed successfully ⇒ Commit the transaction message (the message becomes deliverable and can be consumed by the consumer).
- Local transaction fails ⇒ Roll back the transaction message (the message is deleted and will never be delivered).

### ✅ Reliable back-check mechanism (transaction status back-check)

If the Broker **has not received a commit or rollback instruction for a long time**, it will **actively inquire about the local transaction status from the producer**.

> Prevent semi - messages from hanging unresolved due to the producer crashing or network jitter.



## How to send transaction messages

```toml
[dependencies]
tokio = "1.45.1"
rocketmq-client-rust = { version = "0.5.0"}
rocketmq-error = { version = "0.5.0" }
rocketmq-common = { version = "0.5.0" }
cheetah-string ={version = "0.1.6"}
parking_lot = "0.12.4"
```

**code**：

```rust
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_client_rust::producer::local_transaction_state::LocalTransactionState;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;

pub const MESSAGE_COUNT: usize = 1;
pub const PRODUCER_GROUP: &str = "please_rename_unique_group_name";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "TagA";

#[tokio::main]
pub async fn main() -> RocketMQResult<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let builder = TransactionMQProducer::builder();

    let mut producer = builder
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .topics(vec![TOPIC])
        .transaction_listener(TransactionListenerImpl::default())
        .build();

    producer.start().await?;

    for _ in 0..10 {
        let message = Message::with_tags(TOPIC, TAG, "Hello RocketMQ".as_bytes());
        let send_result = producer
            .send_message_in_transaction::<()>(message, None)
            .await?;
        println!("send result: {}", send_result);
    }
    let _ = tokio::signal::ctrl_c().await;
    producer.shutdown().await;

    Ok(())
}

struct TransactionListenerImpl {
    local_trans: Arc<Mutex<HashMap<CheetahString, i32>>>,
    transaction_index: AtomicI32,
}

impl Default for TransactionListenerImpl {
    fn default() -> Self {
        Self {
            local_trans: Arc::new(Default::default()),
            transaction_index: Default::default(),
        }
    }
}

impl TransactionListener for TransactionListenerImpl {
    fn execute_local_transaction(
        &self,
        msg: &Message,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        let value = self
            .transaction_index
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let status = value % 3;
        let mut guard = self.local_trans.lock();
        guard.insert(
            msg.get_transaction_id().cloned().unwrap_or_default(),
            status,
        );
        LocalTransactionState::Unknown
    }

    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState {
        let guard = self.local_trans.lock();
        let status = guard
            .get(&msg.get_transaction_id().cloned().unwrap_or_default())
            .unwrap_or(&-1);
        match status {
            1 => LocalTransactionState::CommitMessage,
            2 => LocalTransactionState::RollbackMessage,
            _ => LocalTransactionState::Unknown,
        }
    }
}
```

