// Copyright 2026 The RocketMQ Rust Authors
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

mod support;

use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::MessageQuery;
use rocketmq_client_rust::MessageRecall;
use rocketmq_client_rust::MessageSend;
use rocketmq_client_rust::ProducerLifecycle;
use rocketmq_client_rust::ProducerTopicAdmin;
use rocketmq_client_rust::RequestReply;
use rocketmq_client_rust::TransactionMQProducer;
use rocketmq_client_rust::TransactionSend;

fn assert_default_producer_capabilities<T>()
where
    T: ProducerLifecycle + MessageSend + MessageRecall + RequestReply + MessageQuery + ProducerTopicAdmin,
{
}

fn assert_transaction_capability<T: TransactionSend>() {}

#[test]
fn producer_types_implement_only_their_declared_capability_sets() {
    assert_default_producer_capabilities::<DefaultMQProducer>();
    assert_default_producer_capabilities::<TransactionMQProducer>();
    assert_transaction_capability::<TransactionMQProducer>();
}

#[test]
fn producer_and_transaction_handles_reuse_their_builder_session() {
    let runtime = support::client_runtime("producer-capability-session");
    let producer = DefaultMQProducer::builder(runtime.clone())
        .producer_group("producer-capability-session")
        .build();
    let transaction = TransactionMQProducer::builder(runtime)
        .producer_group("transaction-capability-session")
        .build();

    assert!(producer
        .client_session()
        .expect("public producer should have a session")
        .shares_runtime_with(
            transaction
                .client_session()
                .expect("public transaction producer should have a session"),
        ));
}
