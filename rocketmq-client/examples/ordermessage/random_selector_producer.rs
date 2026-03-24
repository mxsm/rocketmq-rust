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

//! Example demonstrating the use of SelectMessageQueueByRandom for load-balanced message delivery.
//!
//! This example shows how to use random queue selection to distribute messages
//! evenly across available queues without ordering guarantees.

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::message_queue_selector::MessageQueueSelector;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::queue_selector::SelectMessageQueueByRandom;
use rocketmq_common::common::message::message_single::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = DefaultMQProducer::builder()
        .producer_group("random_producer_group")
        .name_server_addr("127.0.0.1:9876")
        .build();

    producer.start().await?;
    println!("Producer started successfully");

    let selector = SelectMessageQueueByRandom;

    for i in 0..10 {
        let message = Message::builder()
            .topic("RandomTopic")
            .body(format!("Message {}", i).as_bytes().to_vec())
            .keys(vec![format!("MSG_{}", i)])
            .build()?;

        let send_result = producer
            .send_with_selector(message, |mqs, msg, arg| selector.select(mqs, msg, arg), &())
            .await?;

        if let Some(result) = send_result {
            if let (Some(queue), Some(msg_id)) = (&result.message_queue, &result.msg_id) {
                println!("Sent message {} to queue {}: {}", i, queue.queue_id(), msg_id);
            }
        }
    }

    producer.shutdown().await;
    println!("Producer shutdown");

    Ok(())
}
