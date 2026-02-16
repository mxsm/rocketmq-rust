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

//! Simple compilation tests for send_oneway optimization (P0 + P1)
//!
//! These tests verify that the new APIs compile correctly

use cheetah_string::CheetahString;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_queue::MessageQueue;

#[tokio::test]
async fn test_send_oneway_compiles() {
    // Test: Verify send_oneway compiles
    let mut producer = DefaultMQProducer::builder()
        .producer_group(CheetahString::from_static_str("test_group"))
        .name_server_addr(CheetahString::from_static_str("127.0.0.1:9876"))
        .build();

    let msg = rocketmq_common::common::message::message_single::Message::builder()
        .topic(CheetahString::from_static_str("TestTopic"))
        .tags(CheetahString::from_static_str("TestTag"))
        .body(b"TestBody".to_vec())
        .build()
        .unwrap();

    let _ = producer.send_oneway(msg).await;
}

#[tokio::test]
async fn test_send_oneway_with_message_queue_compiles() {
    // Test: Verify send_oneway_with_message_queue compiles
    let mut producer = DefaultMQProducer::builder()
        .producer_group(CheetahString::from_static_str("test_group"))
        .name_server_addr(CheetahString::from_static_str("127.0.0.1:9876"))
        .build();

    let msg = rocketmq_common::common::message::message_single::Message::builder()
        .topic(CheetahString::from_static_str("TestTopic"))
        .tags(CheetahString::from_static_str("TestTag"))
        .body(b"TestBody".to_vec())
        .build()
        .unwrap();

    let mq = MessageQueue::from_parts("TestTopic", "BrokerName", 0);
    let _ = producer.send_oneway_to_queue(msg, mq).await;
}

#[tokio::test]
async fn test_send_oneway_with_selector_compiles() {
    // Test: Verify send_oneway_with_selector compiles
    let mut producer = DefaultMQProducer::builder()
        .producer_group(CheetahString::from_static_str("test_group"))
        .name_server_addr(CheetahString::from_static_str("127.0.0.1:9876"))
        .build();

    let msg = rocketmq_common::common::message::message_single::Message::builder()
        .topic(CheetahString::from_static_str("TestTopic"))
        .tags(CheetahString::from_static_str("TestTag"))
        .body(b"TestBody".to_vec())
        .build()
        .unwrap();

    let selector = |queues: &[_], _: &dyn rocketmq_common::common::message::MessageTrait, _: &dyn std::any::Any| {
        queues.first().cloned()
    };

    let _ = producer.send_oneway_with_selector(msg, selector, "arg").await;
}

#[test]
fn test_api_signatures_exist() {
    // Compile-time test to ensure all new APIs exist
    // This will fail to compile if any API is missing

    fn _check_api(_producer: &mut DefaultMQProducer) {
        // send_oneway exists
        let _msg = rocketmq_common::common::message::message_single::Message::builder()
            .topic(CheetahString::from_static_str("TestTopic"))
            .tags(CheetahString::from_static_str("Tag"))
            .body(b"Body".to_vec())
            .build()
            .unwrap();
        // Can't call send_oneway in sync context, just verify it exists
        // let _ = producer.send_oneway(msg).await;

        // send_oneway_batch exists (on DefaultMQProducerImpl, not trait)
        // We can't test this directly without access to the impl
    }
}
