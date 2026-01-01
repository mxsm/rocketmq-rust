// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_client_rust::producer::local_transaction_state::LocalTransactionState;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const MESSAGE_COUNT: usize = 1;
pub const PRODUCER_GROUP: &str = "please_rename_unique_group_name";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "TagA";

#[rocketmq::main]
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
        let send_result = producer.send_message_in_transaction::<(), _>(message, None).await?;
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
        msg: &dyn MessageTrait,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        let value = self.transaction_index.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let status = value % 3;
        let mut guard = self.local_trans.lock();
        guard.insert(msg.get_transaction_id().cloned().unwrap_or_default(), status);
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
