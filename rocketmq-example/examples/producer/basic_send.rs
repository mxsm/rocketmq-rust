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

//! # Basic Send Methods
//!
//! This example demonstrates the basic send methods:
//! - `send`: Synchronous send, waits for broker response
//! - `send_with_timeout`: Send with timeout
//! - `send_with_callback`: Send with callback (async result delivery)
//! - `send_with_callback_timeout`: Send with callback and timeout
//! - `send_oneway`: One-way send (fire and forget, no response)

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_basic_send";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "BasicSendTestTopic";
pub const TAG: &str = "BasicTag";
pub const TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    println!("========== RocketMQ Basic Send Methods ==========\n");

    // 1. Basic synchronous send
    basic_send(&mut producer).await?;

    // 2. Send with timeout
    send_with_timeout(&mut producer).await?;

    // 3. Send with callback
    send_with_callback(&mut producer).await?;

    // 4. Send with callback and timeout
    send_with_callback_timeout(&mut producer).await?;

    // 5. One-way send
    send_oneway(&mut producer).await?;

    producer.shutdown().await;
    println!("\n========== All examples completed ==========");
    Ok(())
}

/// 1. Basic synchronous send
///
/// Sends a message and waits for the broker to acknowledge.
/// Returns the send result containing message ID, queue info, etc.
async fn basic_send(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("1. Basic Synchronous Send");
    println!("   Method: producer.send(message).await");

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Basic sync send message")
        .build()?;

    match producer.send(message).await? {
        Some(result) => println!("   Result: {:?}", result),
        None => println!("   Result: None (async delivery)"),
    }

    println!("   Status: Completed\n");
    Ok(())
}

/// 2. Send with timeout
///
/// Sends a message but returns an error if the specified timeout is exceeded.
/// Useful for preventing indefinite blocking.
async fn send_with_timeout(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("2. Send with Timeout");
    println!("   Method: producer.send_with_timeout(message, timeout_ms).await");
    println!("   Timeout: {}ms", TIMEOUT_MS);

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send with timeout message")
        .build()?;

    match producer.send_with_timeout(message, TIMEOUT_MS).await? {
        Some(result) => println!("   Result: {:?}", result),
        None => println!("   Result: None (async delivery)"),
    }

    println!("   Status: Completed\n");
    Ok(())
}

/// 3. Send with callback
///
/// Sends a message asynchronously. The result is delivered via the callback
/// function when the send completes, allowing the producer to continue work
/// without waiting.
async fn send_with_callback(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("3. Send with Callback");
    println!("   Method: producer.send_with_callback(message, callback_fn).await");
    println!("   Callback: Executes when send completes");

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send with callback message")
        .build()?;

    producer.send_with_callback(message,
            |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
                match (result, error) {
                    (Some(r), None) => println!("   Callback: Success - {:?}", r),
                    (None, Some(e)) => println!("   Callback: Error - {}", e),
                    _ => println!("   Callback: Unknown state"),
                }
            }).await?;

    println!("   Status: Completed\n");
    Ok(())
}

/// 4. Send with callback and timeout
///
/// Combines callback delivery with timeout protection. The callback will
/// be called with either success or timeout error.
async fn send_with_callback_timeout(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("4. Send with Callback and Timeout");
    println!("   Method: producer.send_with_callback_timeout(message, callback_fn, timeout_ms).await");
    println!("   Timeout: {}ms", TIMEOUT_MS);

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send with callback and timeout message")
        .build()?;

    producer.send_with_callback_timeout(message,
            |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
                match (result, error) {
                    (Some(r), None) => println!("   Callback: Success - {:?}", r),
                    (None, Some(e)) => println!("   Callback: Error - {}", e),
                    _ => println!("   Callback: Unknown state"),
                }
            }, TIMEOUT_MS).await?;

    println!("   Status: Completed\n");
    Ok(())
}

/// 5. One-way send (fire and forget)
///
/// Sends a message without waiting for any response from the broker.
/// Fastest send method but provides no reliability guarantees.
async fn send_oneway(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("5. One-way Send (Fire and Forget)");
    println!("   Method: producer.send_oneway(message).await");
    println!("   Note: Does not wait for broker response");

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("One-way send message")
        .build()?;

    producer.send_oneway(message).await?;

    println!("   Status: Sent (no response expected)\n");
    Ok(())
}
