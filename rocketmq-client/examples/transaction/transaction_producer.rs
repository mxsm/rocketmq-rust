/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_client::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client::producer::local_transaction_state::LocalTransactionState;
use rocketmq_client::producer::mq_producer::MQProducer;
use rocketmq_client::producer::transaction_listener::TransactionListener;
use rocketmq_client::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_client::Result;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::rocketmq;

pub const MESSAGE_COUNT: usize = 1;
pub const PRODUCER_GROUP: &str = "please_rename_unique_group_name";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "TagA";

#[rocketmq::main]
pub async fn main() -> Result<()> {
    //init logger
    rocketmq_common::log::init_logger();

    // create a producer builder with default configuration
    let builder = TransactionMQProducer::builder();

    let mut producer = builder
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .topics(vec![TOPIC.to_string()])
        .transaction_listener(TransactionListenerImpl::default())
        .build();

    producer.start().await?;

    for _ in 0..10 {
        let message = Message::with_tags(TOPIC, TAG, "Hello RocketMQ".as_bytes());
        let send_result = producer.send_message_in_transaction(message, None).await?;
        println!("send result: {}", send_result);
    }
    producer.shutdown().await;

    Ok(())
}

struct TransactionListenerImpl {
    local_trans: Arc<Mutex<HashMap<String, i32>>>,
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
    fn execute_local_transaction(&self, msg: &Message, arg: &dyn Any) -> LocalTransactionState {
        let value = self
            .transaction_index
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let status = value % 3;
        let mut guard = self.local_trans.lock();
        guard.insert(msg.tr().to_string(), status);
        LocalTransactionState::Unknown
    }

    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState {
        let mut guard = self.local_trans.lock();
        let status = guard.get(msg.get_transaction_id()).unwrap_or(&-1);
        match status {
            1 => LocalTransactionState::CommitMessage,
            2 => LocalTransactionState::RollbackMessage,
            _ => LocalTransactionState::Unknown,
        }
    }
}
