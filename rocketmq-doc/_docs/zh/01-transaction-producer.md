---
title: "Transaction Producer"
permalink: /zh/docs/producer/transaction-producer/
excerpt: "Transaction Producer in RocketMQ Rust"
last_modified_at: 2025-05-31T20:14:05
redirect_from:
  - /theme-setup/
toc: false
classes: wide
---

![Architecture](/assets/images/transaction-message-flow.png)

## ⚙️ RocketMQ 事务消息模型原理

RocketMQ 的事务消息流程分为 **三步**：

### 1. **发送“半消息”（Prepared Message）**

生产者发送一个特殊的“**半消息**”，这个消息：

- 不会对消费者可见。
- 表示消息发送成功，但还未提交。

### 2. **执行本地事务**

在发送半消息成功后，生产者执行本地事务逻辑（如数据库操作等）。

### 3. **提交 or 回滚事务消息**

- 本地事务执行成功 ⇒ 提交事务消息（消息变为可投递状态，消费者可消费）。
- 本地事务失败 ⇒ 回滚事务消息（消息被删除，永远不会投递）。

### ✅ 可靠回查机制（事务状态回查）

如果 Broker **长时间未收到提交或回滚指令**，会 **主动询问生产者** 本地事务状态。

> 防止因生产者宕机或网络抖动，导致半消息悬挂不决。



## 快速开始

```toml
[dependencies]
tokio = "1.45.1"
rocketmq-client-rust = { version = "0.5.0"}
rocketmq-error = { version = "0.5.0" }
rocketmq-common = { version = "0.5.0" }
cheetah-string ={version = "0.1.6"}
parking_lot = "0.12.4"
```

代码：

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

