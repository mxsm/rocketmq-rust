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

//! Example demonstrating the usage of RecallMessageHandle
//!
//! RecallMessageHandle is used to encode and decode handles for recalling
//! delay messages in RocketMQ.

use rocketmq_common::RecallMessageHandle;
use rocketmq_common::RecallMessageHandleV1;

fn main() {
    println!("=== RocketMQ RecallMessageHandle Example ===\n");

    // Example 1: Building a recall handle
    println!("Example 1: Building a recall handle");
    let topic = "test_topic";
    let broker_name = "broker-0";
    let timestamp_str = "1707111111111";
    let message_id = "unique_msg_id_123";

    let encoded_handle = RecallMessageHandleV1::build_handle(topic, broker_name, timestamp_str, message_id);
    println!("Encoded handle: {}", encoded_handle);
    println!();

    // Example 2: Decoding a recall handle
    println!("Example 2: Decoding a recall handle");
    match RecallMessageHandle::decode_handle(&encoded_handle) {
        Ok(handle) => {
            println!("Successfully decoded handle:");
            println!("  Version: {}", handle.version());
            println!("  Topic: {}", handle.topic());
            println!("  Broker Name: {}", handle.broker_name());
            println!("  Timestamp: {}", handle.timestamp_str());
            println!("  Message ID: {}", handle.message_id());
            println!("  Display: {}", handle);
        }
        Err(e) => {
            eprintln!("Failed to decode handle: {}", e);
        }
    }
    println!();

    // Example 3: Error handling with invalid handles
    println!("Example 3: Error handling with invalid handles");

    let wrong_version =
        base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE, "v2 topic broker time msgid");
    let too_few_parts = base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE, "v1 topic broker");

    let invalid_handles = vec![
        ("", "Empty handle"),
        ("invalid_base64!", "Invalid Base64"),
        (wrong_version.as_str(), "Wrong version"),
        (too_few_parts.as_str(), "Too few parts"),
    ];

    for (handle_str, description) in invalid_handles {
        match RecallMessageHandle::decode_handle(handle_str) {
            Ok(_) => println!("  {} - Unexpectedly succeeded", description),
            Err(e) => println!("  {} - Expected error: {}", description, e),
        }
    }
    println!();

    // Example 4: Round-trip encoding and decoding
    println!("Example 4: Round-trip encoding and decoding");
    let original_topic = "production_topic";
    let original_broker = "broker-prod-1";
    let original_timestamp = "1707222222222";
    let original_msg_id = "prod_msg_456";

    let encoded =
        RecallMessageHandleV1::build_handle(original_topic, original_broker, original_timestamp, original_msg_id);

    match RecallMessageHandle::decode_handle(&encoded) {
        Ok(decoded) => {
            let matches = decoded.topic() == original_topic
                && decoded.broker_name() == original_broker
                && decoded.timestamp_str() == original_timestamp
                && decoded.message_id() == original_msg_id;

            if matches {
                println!("  ✓ Round-trip successful - all fields match!");
            } else {
                println!("  ✗ Round-trip failed - fields don't match");
            }
        }
        Err(e) => {
            eprintln!("  ✗ Round-trip failed with error: {}", e);
        }
    }
    println!();

    // Example 5: Creating HandleV1 directly
    println!("Example 5: Creating HandleV1 directly");
    let handle_v1 = RecallMessageHandleV1::new(
        "direct_topic".to_string(),
        "direct_broker".to_string(),
        "1707333333333".to_string(),
        "direct_msg_789".to_string(),
    );

    println!("  Topic: {}", handle_v1.topic());
    println!("  Broker: {}", handle_v1.broker_name());
    println!("  Timestamp: {}", handle_v1.timestamp_str());
    println!("  Message ID: {}", handle_v1.message_id());
    println!("  Version: {}", handle_v1.version());
    println!();

    println!("=== Example Complete ===");
}
