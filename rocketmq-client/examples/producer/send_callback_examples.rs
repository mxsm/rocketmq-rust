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

//! # SendCallback Examples
//!
//! This example demonstrates various ways to use the `SendMessageCallback` API
//! for asynchronous message sending in RocketMQ Rust client.
//!
//! ## Run the example
//!
//! ```shell
//! cargo run --example send_callback_examples
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rocketmq_client::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client::producer::default_mq_producer::ProducerConfig;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use tracing::{error, info};

const TOPIC: &str = "CallbackExample";
const TAG: &str = "TagA";
const NAME_SERVER: &str = "127.0.0.1:9876";

#[rocketmq::main]
async fn main() -> RocketMQResult<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("=== SendCallback Examples ===\n");

    let mut producer = setup_producer().await?;

    // Example 1: Basic callback with match pattern
    info!("Example 1: Basic Callback Usage");
    send_with_basic_callback(&mut producer).await?;

    // Example 2: Callback with state tracking
    info!("\nExample 2: Callback with State Tracking");
    send_with_state_tracking(&mut producer).await?;

    // Example 3: Callback with custom error handling
    info!("\nExample 3: Custom Error Handling");
    send_with_error_handling(&mut producer).await?;

    // Example 4: Batch sending with callback
    info!("\nExample 4: Batch Sending with Progress Tracking");
    send_batch_with_progress(&mut producer).await?;

    // Shutdown
    producer.shutdown().await;
    info!("\n=== All Examples Completed ===");
    Ok(())
}

async fn setup_producer() -> RocketMQResult<DefaultMQProducer> {
    let mut producer = DefaultMQProducer::new(
        ProducerConfig {
            name_server_addr: NAME_SERVER.to_string(),
            group_name: "CallbackExampleProducerGroup".to_string(),
            ..Default::default()
        },
        None,
    );

    producer
        .start()
        .await
        .expect("Producer start failed");
    Ok(producer)
}

/// Example 1: Basic callback usage with pattern matching
async fn send_with_basic_callback(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Basic callback message")
        .build()?;

    producer
        .send_with_callback(message, |result, error| {
            match (result, error) {
                (Some(send_result), None) => {
                    info!("Message sent successfully!");
                    info!("  - Message ID: {:?}", send_result.msg_id);
                    info!("  - Queue: {:?}", send_result.message_queue);
                    info!("  - Status: {:?}", send_result.send_status);
                }
                (None, Some(err)) => {
                    error!("Failed to send message: {}", err);
                }
                _ => {
                    error!("Invalid callback state");
                }
            }
        })
        .await?;

    // Wait a moment for callback to execute
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    Ok(())
}

/// Example 2: Callback with state tracking using atomic counters
async fn send_with_state_tracking(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));
    let total_messages = 10;

    info!("Sending {} messages with state tracking...", total_messages);

    for i in 0..total_messages {
        let message = Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body(format!("State tracking message {}", i))
            .build()?;

        let success_cnt = Arc::clone(&success_count);
        let failure_cnt = Arc::clone(&failure_count);

        producer
            .send_with_callback(message, move |result, error| {
                match (result, error) {
                    (Some(_), None) => {
                        success_cnt.fetch_add(1, Ordering::Relaxed);
                    }
                    (None, Some(_)) => {
                        failure_cnt.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            })
            .await?;
    }

    // Wait for all callbacks to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let successful = success_count.load(Ordering::Relaxed);
    let failed = failure_count.load(Ordering::Relaxed);

    info!("Results:");
    info!("  Successful: {}/{}", successful, total_messages);
    info!("  Failed: {}/{}", failed, total_messages);

    Ok(())
}

/// Example 3: Callback with detailed error handling
async fn send_with_error_handling(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Error handling message")
        .build()?;

    producer
        .send_with_callback(message, |result, error| {
            if let Some(send_result) = result {
                info!("Send successful:");
                info!("  - Message ID: {:?}", send_result.msg_id);
                info!("  - Offset Message ID: {:?}", send_result.offset_msg_id);
                info!("  - Queue Offset: {}", send_result.queue_offset);
                info!("  - Status: {:?}", send_result.send_status);
                info!("  - Region ID: {:?}", send_result.region_id);
                info!("  - Trace Enabled: {}", send_result.trace_on);
            } else if let Some(err) = error {
                error!("Send failed:");
                error!("  - Error: {}", err);
                error!("  - Error type: {}", std::any::type_name_of_val(&err));

                // Detailed error source chain
                if let Some(source) = std::error::Error::source(err) {
                    error!("  - Source: {}", source);
                }

                // Integration point for retry mechanisms or monitoring systems
                error!("  - Consider retrying or alerting monitoring system");
            }
        })
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    Ok(())
}

/// Example 4: Batch sending with progress tracking
async fn send_batch_with_progress(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let batch_size = 5;
    let completed = Arc::new(AtomicUsize::new(0));

    info!("Sending {} messages in batch...", batch_size);

    for i in 0..batch_size {
        let message = Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .key(format!("OrderID{:03}", i))
            .body(format!("Batch message {}", i))
            .build()?;

        let completed_cnt = Arc::clone(&completed);
        let current_idx = i;

        producer
            .send_with_callback(message, move |result, error| {
                let count = completed_cnt.fetch_add(1, Ordering::Relaxed) + 1;

                match (result, error) {
                    (Some(send_result), None) => {
                        info!(
                            "  [{}/{}] Message {} sent successfully (ID: {:?})",
                            count, batch_size, current_idx, send_result.msg_id
                        );
                    }
                    (None, Some(err)) => {
                        error!("  [{}/{}] Message {} failed: {}", count, batch_size, current_idx, err);
                    }
                    _ => {}
                }

                if count == batch_size {
                    info!("Batch send completed!");
                }
            })
            .await?;
    }

    // Wait for all callbacks
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    Ok(())
}
