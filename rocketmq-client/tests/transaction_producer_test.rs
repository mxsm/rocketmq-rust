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

use rocketmq_client_rust::producer::local_transaction_state::LocalTransactionState;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
use rocketmq_client_rust::producer::transaction_mq_produce_builder::TransactionMQProducerBuilder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageTrait;

struct TestTransactionListener;

impl TransactionListener for TestTransactionListener {
    fn execute_local_transaction(
        &self,
        _msg: &dyn MessageTrait,
        _arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }

    fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }
}

#[test]
fn test_transaction_producer_builder() {
    let _producer = TransactionMQProducerBuilder::new()
        .transaction_listener(TestTransactionListener)
        .check_thread_pool_min_size(2)
        .check_thread_pool_max_size(10)
        .check_request_hold_max(3000)
        .build();
}

#[test]
fn test_transaction_producer_default_config() {
    let _producer = TransactionMQProducerBuilder::new()
        .transaction_listener(TestTransactionListener)
        .build();
}

#[test]
fn test_transaction_producer_custom_config() {
    let _producer = TransactionMQProducerBuilder::new()
        .transaction_listener(TestTransactionListener)
        .check_thread_pool_min_size(5)
        .check_thread_pool_max_size(20)
        .check_request_hold_max(5000)
        .build();
}
