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
use std::sync::Arc;

use rocketmq_client_rust::producer::local_transaction_state::LocalTransactionState;
use rocketmq_client_rust::producer::transaction_listener::ArcTransactionListener;
use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
use rocketmq_client_rust::producer::transaction_mq_produce_builder::TransactionMQProducerBuilder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
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

#[test]
fn transaction_producer_runtime_config_accessors_match_java_facade() {
    let mut producer = TransactionMQProducerBuilder::new()
        .check_thread_pool_min_size(2)
        .check_thread_pool_max_size(8)
        .check_request_hold_max(3000)
        .build();

    assert_eq!(producer.check_thread_pool_min_size(), 2);
    assert_eq!(producer.check_thread_pool_max_size(), 8);
    assert_eq!(producer.check_request_hold_max(), 3000);

    producer.set_check_thread_pool_min_size(3);
    producer.set_check_thread_pool_max_size(9);
    producer.set_check_request_hold_max(4000);

    assert_eq!(producer.check_thread_pool_min_size(), 3);
    assert_eq!(producer.check_thread_pool_max_size(), 9);
    assert_eq!(producer.check_request_hold_max(), 4000);
}

#[test]
fn transaction_listener_arc_setter_matches_java_facade_reference() {
    let mut producer = TransactionMQProducerBuilder::new().build();
    let listener: ArcTransactionListener = Arc::new(TestTransactionListener);

    assert!(producer.transaction_listener().is_none());

    producer.set_transaction_listener_arc(listener.clone());

    let actual = producer
        .transaction_listener()
        .expect("transaction listener should be set");
    assert!(Arc::ptr_eq(&actual, &listener));
}

#[test]
fn transaction_executor_and_check_listener_facades_match_java_reference() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should build");
    let mut producer = TransactionMQProducerBuilder::new().build();
    let listener: ArcTransactionListener = Arc::new(TestTransactionListener);

    assert!(producer.executor_service().is_none());
    assert!(producer.transaction_check_listener().is_none());

    producer.set_executor_service(runtime.handle().clone());
    producer.set_transaction_check_listener_arc(listener.clone());

    assert!(producer.executor_service().is_some());
    let actual = producer
        .transaction_check_listener()
        .expect("transaction check listener should be set");
    assert!(Arc::ptr_eq(&actual, &listener));
}

#[tokio::test]
async fn transaction_send_without_listener_fails_before_start_or_send_like_java() {
    let mut producer = TransactionMQProducerBuilder::new().build();
    let msg = Message::builder()
        .topic("TopicTest")
        .body("transaction-body")
        .build_unchecked();

    let result = producer.send_message_in_transaction(msg, None::<()>).await;

    assert!(result
        .err()
        .is_some_and(|error| error.to_string().contains("TransactionListener is null")));
}
