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

//! # Send with Selector Methods
//!
//! This example demonstrates methods for sending messages with custom queue selection logic:
//! - `send_with_selector`: Send with custom queue selector
//! - `send_with_selector_timeout`: Send with selector and timeout
//! - `send_with_selector_callback`: Send with selector and callback
//! - `send_with_selector_callback_timeout`: Send with selector, callback, and timeout
//! - `send_oneway_with_selector`: One-way send with selector

use std::any::Any;
use std::sync::Arc;

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_client_rust::producer::send_callback::SendMessageCallback;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_send_with_selector";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "SendWithSelectorTestTopic";
pub const TAG: &str = "SelectorTag";
pub const TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    println!("========== RocketMQ Send with Selector Methods ==========\n");

    // 1. Send with selector
    send_with_selector(&mut producer).await?;

    // 2. Send with selector and timeout
    send_with_selector_timeout(&mut producer).await?;

    // 3. Send with selector and callback
    send_with_selector_callback(&mut producer).await?;

    // 4. Send with selector, callback, and timeout
    send_with_selector_callback_timeout(&mut producer).await?;

    // 5. One-way send with selector
    send_oneway_with_selector(&mut producer).await?;

    producer.shutdown().await;
    println!("\n========== All examples completed ==========");
    Ok(())
}

/// 1. Send with selector
///
/// Uses a custom selector function to choose which queue to send the message to.
/// The selector receives all available queues and chooses one based on custom logic.
async fn send_with_selector(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("1. Send with Selector");
    println!("   Method: producer.send_with_selector(message, selector_fn, arg).await");

    // Define a selector function
    let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, _arg: &dyn Any| -> Option<MessageQueue> {
        // Example: Select first queue (simple selection)
        queues.first().cloned()

        // Other possible selection strategies:
        // - Hash-based: queues[(msg_hash % queues.len())]
        // - Round-robin: maintain index and rotate
        // - Random: queues[random index]
        // - Attribute-based: based on message properties
    };

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send with selector message")
        .build()?;

    let arg = "selector-arg"; // Can be any type implementing Any

    match producer.send_with_selector(message, selector, arg).await? {
        Some(result) => println!("   Result: {:?}", result),
        None => println!("   Result: None (async delivery)"),
    }

    println!("   Status: Completed\n");
    Ok(())
}

/// 2. Send with selector and timeout
///
/// Combines custom queue selection with timeout protection.
async fn send_with_selector_timeout(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("2. Send with Selector and Timeout");
    println!("   Method: producer.send_with_selector_timeout(message, selector_fn, arg, timeout_ms).await");
    println!("   Timeout: {}ms", TIMEOUT_MS);

    let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, _arg: &dyn Any| -> Option<MessageQueue> {
        queues.first().cloned()
    };

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send with selector and timeout message")
        .build()?;

    let arg = "selector-arg";

    match producer
        .send_with_selector_timeout(message, selector, arg, TIMEOUT_MS)
        .await?
    {
        Some(result) => println!("   Result: {:?}", result),
        None => println!("   Result: None (async delivery)"),
    }

    println!("   Status: Completed\n");
    Ok(())
}

/// 3. Send with selector and callback
///
/// Combines custom queue selection with asynchronous callback delivery.
async fn send_with_selector_callback(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("3. Send with Selector and Callback");
    println!("   Method: producer.send_with_selector_callback(message, selector_fn, arg, callback_fn).await");

    let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, _arg: &dyn Any| -> Option<MessageQueue> {
        queues.first().cloned()
    };

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send with selector and callback message")
        .build()?;

    let arg = "selector-arg";

    let callback: SendMessageCallback = Arc::new(
        |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| match (result, error) {
            (Some(r), None) => println!("   Callback: Success - {:?}", r),
            (None, Some(e)) => println!("   Callback: Error - {}", e),
            _ => println!("   Callback: Unknown state"),
        },
    );

    producer
        .send_with_selector_callback(message, selector, arg, Some(callback))
        .await?;

    println!("   Status: Completed\n");
    Ok(())
}

/// 4. Send with selector, callback, and timeout
///
/// Full featured send with custom queue selection, callback delivery, and timeout protection.
async fn send_with_selector_callback_timeout(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("4. Send with Selector, Callback, and Timeout");
    println!(
        "   Method: producer.send_with_selector_callback_timeout(message, selector_fn, arg, callback_fn, \
         timeout_ms).await"
    );
    println!("   Timeout: {}ms", TIMEOUT_MS);

    let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, _arg: &dyn Any| -> Option<MessageQueue> {
        queues.first().cloned()
    };

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Send with selector, callback, and timeout message")
        .build()?;

    let arg = "selector-arg";

    let callback: SendMessageCallback = Arc::new(
        |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| match (result, error) {
            (Some(r), None) => println!("   Callback: Success - {:?}", r),
            (None, Some(e)) => println!("   Callback: Error - {}", e),
            _ => println!("   Callback: Unknown state"),
        },
    );

    producer
        .send_with_selector_callback_timeout(message, selector, arg, Some(callback), TIMEOUT_MS)
        .await?;

    println!("   Status: Completed\n");
    Ok(())
}

/// 5. One-way send with selector
///
/// One-way send with custom queue selection, no response expected.
async fn send_oneway_with_selector(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    println!("5. One-way Send with Selector");
    println!("   Method: producer.send_oneway_with_selector(message, selector_fn, arg).await");

    let selector = |queues: &[MessageQueue], _msg: &dyn MessageTrait, _arg: &dyn Any| -> Option<MessageQueue> {
        queues.first().cloned()
    };

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("One-way send with selector message")
        .build()?;

    let arg = "selector-arg";

    producer.send_oneway_with_selector(message, selector, arg).await?;

    println!("   Status: Sent (no response expected)\n");
    Ok(())
}
