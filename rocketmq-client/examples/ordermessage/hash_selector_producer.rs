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

//! Example demonstrating the use of SelectMessageQueueByHash for ordered message delivery.
//!
//! This example shows how to use hash-based queue selection to ensure that messages
//! with the same key (e.g., order ID) are always sent to the same queue, maintaining
//! ordering guarantees.

#[path = "../support/mod.rs"]
mod support;

use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::MessageQueueSelector;
use rocketmq_client_rust::SelectMessageQueueByHash;
use rocketmq_model::common::message::message_single::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let example_runtime = support::ExampleClientRuntime::new("hash-selector-producer");
    let client_runtime = example_runtime.client_runtime();
    let mut producer = DefaultMQProducer::builder(client_runtime.clone())
        .producer_group("order_producer_group")
        .name_server_addr("127.0.0.1:9876")
        .build();

    producer.start().await?;
    println!("Producer started successfully");

    let selector = SelectMessageQueueByHash;

    for i in 0..10 {
        let order_id = i % 3;

        let message = Message::builder()
            .topic("OrderTopic")
            .body(format!("Order {} - Item {}", order_id, i).as_bytes().to_vec())
            .keys(vec![format!("ORDER_{}", order_id)])
            .build()?;

        let send_result = producer
            .send_with_selector(message, |mqs, msg, arg| selector.select(mqs, msg, arg), &order_id)
            .await?;

        if let Some(result) = send_result {
            if let (Some(queue), Some(msg_id)) = (&result.message_queue, &result.msg_id) {
                println!(
                    "Sent order {} message to queue {}: {}",
                    order_id,
                    queue.queue_id(),
                    msg_id
                );
            }
        }
    }

    producer.shutdown().await;
    println!("Producer shutdown");

    example_runtime.shutdown().await;

    Ok(())
}
