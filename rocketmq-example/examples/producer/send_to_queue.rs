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

//! # Send to Specific Queue Methods
//!
//! This example demonstrates methods for sending messages to a specific queue:
//! - `send_to_queue`: Send to a specific queue
//! - `send_to_queue_with_timeout`: Send to a specific queue with timeout
//! - `send_to_queue_with_callback`: Send to a specific queue with callback
//! - `send_to_queue_with_callback_timeout`: Send to a specific queue with callback and timeout
//! - `send_oneway_to_queue`: One-way send to a specific queue

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_send_to_queue";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "SendToQueueTestTopic";
pub const TAG: &str = "QueueTag";
pub const TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    println!("========== RocketMQ Send to Specific Queue Methods ==========\n");

    // Fetch available queues first
    let queues = producer.fetch_publish_message_queues(TOPIC).await?;
    if queues.is_empty() {
        println!("No queues available for topic: {}", TOPIC);
        return Ok(());
    }

    let target_queue = queues[0].clone();
    println!("Target Queue: {:?}\n", target_queue);

    // 1. Send to specific queue
    send_to_queue(&mut producer, target_queue.clone()).await?;

    // 2. Send to queue with timeout
    send_to_queue_with_timeout(&mut producer, target_queue.clone()).await?;

    // 3. Send to queue with callback
    send_to_queue_with_callback(&mut producer, target_queue.clone()).await?;

    // 4. Send to queue with callback and timeout
    send_to_queue_with_callback_timeout(&mut producer, target_queue.clone()).await?;

    // 5. One-way send to queue
    send_oneway_to_queue(&mut producer, target_queue).await?;

    producer.shutdown().await;
    println!("\n========== All examples completed ==========");
    Ok(())
}

/// 1. Send to specific queue
///
/// Sends a message directly to a specified message queue, bypassing
/// the default queue selection logic.
async fn send_to_queue(producer: &mut DefaultMQProducer, queue: MessageQueue) -> RocketMQResult<()> {
    println!("1. Send to Specific Queue");
    println!("   Method: producer.send_to_queue(message, queue).await");

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send to specific queue message")
        .build()?;

    match producer.send_to_queue(message, queue).await? {
        Some(result) => println!("   Result: {:?}", result),
        None => println!("   Result: None (async delivery)"),
    }

    println!("   Status: Completed\n");
    Ok(())
}

/// 2. Send to queue with timeout
///
/// Sends a message to a specific queue with a timeout limit.
async fn send_to_queue_with_timeout(producer: &mut DefaultMQProducer, queue: MessageQueue) -> RocketMQResult<()> {
    println!("2. Send to Queue with Timeout");
    println!("   Method: producer.send_to_queue_with_timeout(message, queue, timeout_ms).await");
    println!("   Timeout: {}ms", TIMEOUT_MS);

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send to queue with timeout message")
        .build()?;

    match producer.send_to_queue_with_timeout(message, queue, TIMEOUT_MS).await? {
        Some(result) => println!("   Result: {:?}", result),
        None => println!("   Result: None (async delivery)"),
    }

    println!("   Status: Completed\n");
    Ok(())
}

/// 3. Send to queue with callback
///
/// Sends a message to a specific queue asynchronously with callback.
async fn send_to_queue_with_callback(producer: &mut DefaultMQProducer, queue: MessageQueue) -> RocketMQResult<()> {
    println!("3. Send to Queue with Callback");
    println!("   Method: producer.send_to_queue_with_callback(message, queue, callback_fn).await");

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send to queue with callback message")
        .build()?;

    producer
        .send_to_queue_with_callback(
            message,
            queue,
            |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| match (result, error) {
                (Some(r), None) => println!("   Callback: Success - {:?}", r),
                (None, Some(e)) => println!("   Callback: Error - {}", e),
                _ => println!("   Callback: Unknown state"),
            },
        )
        .await?;

    println!("   Status: Completed\n");
    Ok(())
}

/// 4. Send to queue with callback and timeout
///
/// Combines queue targeting with callback delivery and timeout protection.
async fn send_to_queue_with_callback_timeout(
    producer: &mut DefaultMQProducer,
    queue: MessageQueue,
) -> RocketMQResult<()> {
    println!("4. Send to Queue with Callback and Timeout");
    println!("   Method: producer.send_to_queue_with_callback_timeout(message, queue, callback_fn, timeout_ms).await");
    println!("   Timeout: {}ms", TIMEOUT_MS);

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send to queue with callback and timeout message")
        .build()?;

    producer
        .send_to_queue_with_callback_timeout(
            message,
            queue,
            |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| match (result, error) {
                (Some(r), None) => println!("   Callback: Success - {:?}", r),
                (None, Some(e)) => println!("   Callback: Error - {}", e),
                _ => println!("   Callback: Unknown state"),
            },
            TIMEOUT_MS,
        )
        .await?;

    println!("   Status: Completed\n");
    Ok(())
}

/// 5. One-way send to queue
///
/// Sends a message to a specific queue without waiting for a response.
async fn send_oneway_to_queue(producer: &mut DefaultMQProducer, queue: MessageQueue) -> RocketMQResult<()> {
    println!("5. One-way Send to Queue");
    println!("   Method: producer.send_oneway_to_queue(message, queue).await");

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("One-way send to queue message")
        .build()?;

    producer.send_oneway_to_queue(message, queue).await?;

    println!("   Status: Sent (no response expected)\n");
    Ok(())
}
