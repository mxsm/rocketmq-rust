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

//! Demonstrates transactional message sending.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use rocketmq_client_rust::producer::local_transaction_state::LocalTransactionState;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
use rocketmq_client_rust::producer::transaction_mq_producer::TransactionMQProducer;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_transaction_send_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TransactionSendTestTopic";
pub const TAG: &str = "TransactionTag";

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    let telemetry_guard =
        rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
            .expect("telemetry logging bootstrap should initialize");
    let mut producer = TransactionMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .topics(vec![TOPIC])
        .transaction_listener(ExampleTransactionListener::default())
        .build();

    producer.start().await?;

    for index in 0..6 {
        let message = Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .key(format!("transaction-key-{index}"))
            .body(format!("transaction message {index}"))
            .build()?;

        let result = producer.send_message_in_transaction::<(), _>(message, None).await?;
        println!("transaction index={index}, send result: {result}");
    }

    producer.shutdown().await;
    telemetry_guard
        .shutdown()
        .expect("telemetry logging shutdown should succeed");

    Ok(())
}

#[derive(Default)]
struct ExampleTransactionListener {
    states: Arc<Mutex<HashMap<CheetahString, LocalTransactionState>>>,
    next_state: AtomicUsize,
}

impl TransactionListener for ExampleTransactionListener {
    fn execute_local_transaction(
        &self,
        msg: &dyn MessageTrait,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        let state = match self.next_state.fetch_add(1, Ordering::AcqRel) % 3 {
            0 => LocalTransactionState::CommitMessage,
            1 => LocalTransactionState::RollbackMessage,
            _ => LocalTransactionState::Unknown,
        };

        if let Some(transaction_id) = msg.transaction_id() {
            self.states
                .lock()
                .expect("transaction state mutex poisoned")
                .insert(CheetahString::from(transaction_id), state);
        }

        println!(
            "execute local transaction for {:?}, state={state:?}",
            msg.transaction_id()
        );
        state
    }

    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState {
        let transaction_id = msg.get_transaction_id().cloned().unwrap_or_default();
        let state = self
            .states
            .lock()
            .expect("transaction state mutex poisoned")
            .get(&transaction_id)
            .copied()
            .unwrap_or(LocalTransactionState::Unknown);

        println!("check local transaction id={transaction_id}, state={state:?}");
        state
    }
}
