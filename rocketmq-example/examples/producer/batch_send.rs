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

//! # Batch Send Methods
//!
//! This example demonstrates methods for sending multiple messages in a single call:
//! - `send_batch`: Send multiple messages
//! - `send_batch_with_timeout`: Send multiple messages with timeout
//! - `send_batch_to_queue`: Send multiple messages to a specific queue
//! - `send_batch_to_queue_with_timeout`: Send multiple messages to a specific queue with timeout
//! - `send_batch_with_callback`: Send multiple messages with callback
//! - `send_batch_with_callback_timeout`: Send multiple messages with callback and timeout
//! - `send_batch_to_queue_with_callback`: Send multiple messages to a specific queue with callback
//! - `send_batch_to_queue_with_callback_timeout`: Send multiple messages to a specific queue with
//!   callback and timeout

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_batch_send";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "BatchSendTestTopic";
pub const TAG: &str = "BatchTag";
pub const TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    println!("========== RocketMQ Batch Send Methods ==========\n");

    // Fetch available queues for queue-specific examples
    let queues = producer.fetch_publish_message_queues(TOPIC).await?;
    let target_queue = queues.first().cloned();

    // 1. Basic batch send
    batch_send(&mut producer).await?;

    // 2. Batch send with timeout
    batch_send_with_timeout(&mut producer).await?;

    // 3. Batch send to queue
    if let Some(queue) = &target_queue {
        batch_send_to_queue(&mut producer, queue.clone()).await?;
    }

    // 4. Batch send to queue with timeout
    if let Some(queue) = &target_queue {
        batch_send_to_queue_with_timeout(&mut producer, queue.clone()).await?;
    }

    // 5. Batch send with callback
    batch_send_with_callback(&mut producer).await?;

    // 6. Batch send with callback and timeout
    batch_send_with_callback_timeout(&mut producer).await?;

    // 7. Batch send to queue with callback
    if let Some(queue) = &target_queue {
        batch_send_to_queue_with_callback(&mut producer, queue.clone()).await?;
    }

    // 8. Batch send to queue with callback and timeout
    if let Some(queue) = &target_queue {
        batch_send_to_queue_with_callback_timeout(&mut producer, queue.clone()).await?;
    }

    producer.shutdown().await;
    println!("\n========== All examples completed ==========");
    Ok(())
}

/// 1. Basic batch send
///
/// Sends multiple messages in a single call for improved throughput.
async fn batch_send(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("1. Basic Batch Send");
    println!("   Method: producer.send_batch(messages_vec).await");
    println!("   Batch size: 3 messages");

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch message 2")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch message 3")
            .build()?,
    ];

    let result = producer.send_batch(messages).await?;
    println!("   Result: {:?}", result);

    println!("   Status: Completed\n");
    Ok(())
}

/// 2. Batch send with timeout
///
/// Sends multiple messages with a timeout limit.
async fn batch_send_with_timeout(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("2. Batch Send with Timeout");
    println!("   Method: producer.send_batch_with_timeout(messages_vec, timeout_ms).await");
    println!("   Timeout: {}ms, Batch size: 2 messages", TIMEOUT_MS);

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch timeout message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch timeout message 2")
            .build()?,
    ];

    let result = producer.send_batch_with_timeout(messages, TIMEOUT_MS).await?;
    println!("   Result: {:?}", result);

    println!("   Status: Completed\n");
    Ok(())
}

/// 3. Batch send to queue
///
/// Sends multiple messages to a specific queue.
async fn batch_send_to_queue(producer: &mut DefaultMQProducer, queue: MessageQueue) -> RocketMQResult<()> {
    println!("3. Batch Send to Queue");
    println!("   Method: producer.send_batch_to_queue(messages_vec, queue).await");
    println!("   Target Queue: {:?}", queue);

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue message 2")
            .build()?,
    ];

    let result = producer.send_batch_to_queue(messages, queue).await?;
    println!("   Result: {:?}", result);

    println!("   Status: Completed\n");
    Ok(())
}

/// 4. Batch send to queue with timeout
///
/// Sends multiple messages to a specific queue with timeout protection.
async fn batch_send_to_queue_with_timeout(producer: &mut DefaultMQProducer, queue: MessageQueue) -> RocketMQResult<()> {
    println!("4. Batch Send to Queue with Timeout");
    println!("   Method: producer.send_batch_to_queue_with_timeout(messages_vec, queue, timeout_ms).await");
    println!("   Target Queue: {:?}, Timeout: {}ms", queue, TIMEOUT_MS);

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue timeout message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue timeout message 2")
            .build()?,
    ];

    let result = producer
        .send_batch_to_queue_with_timeout(messages, queue, TIMEOUT_MS)
        .await?;
    println!("   Result: {:?}", result);

    println!("   Status: Completed\n");
    Ok(())
}

/// 5. Batch send with callback
///
/// Sends multiple messages asynchronously with callback delivery.
async fn batch_send_with_callback(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("5. Batch Send with Callback");
    println!("   Method: producer.send_batch_with_callback(messages_vec, callback_fn).await");

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch callback message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch callback message 2")
            .build()?,
    ];

    producer
        .send_batch_with_callback(
            messages,
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

/// 6. Batch send with callback and timeout
///
/// Combines callback delivery with timeout protection for batch sends.
async fn batch_send_with_callback_timeout(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("6. Batch Send with Callback and Timeout");
    println!("   Method: producer.send_batch_with_callback_timeout(messages_vec, callback_fn, timeout_ms).await");
    println!("   Timeout: {}ms", TIMEOUT_MS);

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch callback timeout message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch callback timeout message 2")
            .build()?,
    ];

    producer
        .send_batch_with_callback_timeout(
            messages,
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

/// 7. Batch send to queue with callback
///
/// Sends multiple messages to a specific queue with callback delivery.
async fn batch_send_to_queue_with_callback(
    producer: &mut DefaultMQProducer,
    queue: MessageQueue,
) -> RocketMQResult<()> {
    println!("7. Batch Send to Queue with Callback");
    println!("   Method: producer.send_batch_to_queue_with_callback(messages_vec, queue, callback_fn).await");
    println!("   Target Queue: {:?}", queue);

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue callback message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue callback message 2")
            .build()?,
    ];

    producer
        .send_batch_to_queue_with_callback(
            messages,
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

/// 8. Batch send to queue with callback and timeout
///
/// Full featured batch send with queue targeting, callback delivery, and timeout.
async fn batch_send_to_queue_with_callback_timeout(
    producer: &mut DefaultMQProducer,
    queue: MessageQueue,
) -> RocketMQResult<()> {
    println!("8. Batch Send to Queue with Callback and Timeout");
    println!(
        "   Method: producer.send_batch_to_queue_with_callback_timeout(messages_vec, queue, callback_fn, \
         timeout_ms).await"
    );
    println!("   Target Queue: {:?}, Timeout: {}ms", queue, TIMEOUT_MS);

    let messages = vec![
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue callback timeout message 1")
            .build()?,
        Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Batch to queue callback timeout message 2")
            .build()?,
    ];

    producer
        .send_batch_to_queue_with_callback_timeout(
            messages,
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
