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

//! Cluster mode consumer example
//!
//! In cluster mode, messages are load-balanced across all consumer instances
//! in the same consumer group. Each message will be consumed by only one
//! consumer instance.
//!
//! This example demonstrates two ways to register message listeners:
//! 1. **Closure-based** (simple and direct)
//! 2. **Struct-based** (better for complex logic and testing)

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;
use rocketmq_rust::wait_for_signal;
use tracing::info;

pub const CONSUMER_GROUP: &str = "ClusterConsumerGroup";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "*";

// Toggle between closure and struct implementation
// Set to true to use closure, false to use struct
const USE_CLOSURE: bool = false;

/// Cluster mode consumer example
///
/// Demonstrates consuming messages in cluster mode with two listener approaches.
///
/// In cluster mode:
/// - Messages are distributed across all consumer instances in the same group
/// - Each message is consumed by only one consumer instance
/// - Suitable for parallel processing and load balancing
///
/// To test this example:
/// 1. Start multiple instances of this consumer (with the same consumer group)
/// 2. Send messages to the topic using a producer
/// 3. Observe that messages are distributed among the consumer instances
#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    // Initialize logger
    rocketmq_common::log::init_logger()?;

    // Create a push consumer with cluster mode
    let builder = DefaultMQPushConsumer::builder();

    let mut consumer = builder
        .consumer_group(CONSUMER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        // Set message model to Clustering (this is the default mode)
        .message_model(MessageModel::Clustering)
        // Set the number of threads for consuming messages
        .consume_thread_min(1)
        .consume_thread_max(10)
        // Set the maximum number of messages to consume at once
        .consume_message_batch_max_size(1)
        .build();

    // Subscribe to topic with tag filter
    consumer.subscribe(TOPIC, TAG).await?;

    // Register message listener
    if USE_CLOSURE {
        // Method 1: Using closure (simple and direct)
        let listener = |msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| {
            for msg in msgs {
                info!(
                    "[Closure] Received message [MsgId: {}, Topic: {}, Tags: {}]",
                    msg.msg_id(),
                    msg.topic(),
                    msg.tags().unwrap_or_default()
                );

                if let Some(body) = msg.get_body() {
                    match String::from_utf8(body.to_vec()) {
                        Ok(content) => {
                            info!("[Closure] Message content: {}", content);
                        }
                        Err(e) => {
                            info!("[Closure] Failed to parse message body as UTF-8: {}", e);
                        }
                    }
                }
            }
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        };
        // Wrap closure using ClosureListener helper for type system compatibility
        consumer.register_message_listener_concurrently(listener);
        info!("Registered message listener using closure");
    } else {
        // Method 2: Using struct implementation (recommended for production)
        consumer.register_message_listener_concurrently(ClusterMessageListener);
        info!("Registered message listener using struct");
    }

    // Start consumer
    consumer.start().await?;

    info!("Cluster consumer started. Group: {}, Topic: {}", CONSUMER_GROUP, TOPIC);

    // Wait for shutdown signal
    let _ = wait_for_signal().await;

    // Shutdown consumer
    consumer.shutdown().await;

    info!("Cluster consumer shutdown completed.");

    Ok(())
}
// ============================================================================
// Struct-based message listener implementation
// ============================================================================
// Recommended for production code when you need:
// - Complex message processing logic
// - State management in the listener
// - Easy unit testing
// - Code reusability across multiple consumers

/// Message listener for cluster mode consumption (struct implementation)
pub struct ClusterMessageListener;

impl MessageListenerConcurrently for ClusterMessageListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for msg in msgs {
            info!(
                "[Struct] Received message [MsgId: {}, Topic: {}, Tags: {}]",
                msg.msg_id(),
                msg.topic(),
                msg.tags().unwrap_or_default()
            );

            // Parse message body
            if let Some(body) = msg.get_body() {
                match String::from_utf8(body.to_vec()) {
                    Ok(content) => {
                        info!("[Struct] Message content: {}", content);
                    }
                    Err(e) => {
                        info!("[Struct] Failed to parse message body as UTF-8: {}", e);
                    }
                }
            }

            // Process the message here
            // In cluster mode, each message is consumed by only one consumer instance
            // If processing fails, you can return ConsumeReconsumeLater to retry
        }

        // Return success status
        // The message will not be consumed again by this or other consumer instances
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)

        // If you want to retry the message, return:
        // Ok(ConsumeConcurrentlyStatus::ConsumeReconsumeLater)
    }
}
