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

//! Examples of using the new Message Builder API

use rocketmq_common::common::message::message_single::Message;

fn main() {
    // Example 1: Simple message
    println!("=== Example 1: Simple Message ===");
    let msg = Message::builder()
        .topic("simple-topic")
        .body_slice(b"Hello, RocketMQ!")
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Body: {:?}", std::str::from_utf8(msg.body_slice()).unwrap());
    println!();

    // Example 2: Message with tags
    println!("=== Example 2: Message with Tags ===");
    let msg = Message::builder()
        .topic("tagged-topic")
        .body_slice(b"Tagged message")
        .tags("important")
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Tags: {:?}", msg.tags());
    println!("Body: {:?}", std::str::from_utf8(msg.body_slice()).unwrap());
    println!();

    // Example 3: Message with multiple keys
    println!("=== Example 3: Message with Keys ===");
    let msg = Message::builder()
        .topic("order-topic")
        .body_slice(b"Order data")
        .tags("order")
        .keys(vec!["order-123".to_string(), "user-456".to_string()])
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Tags: {:?}", msg.tags());
    println!("Keys: {:?}", msg.keys());
    println!();

    // Example 4: Delayed message
    println!("=== Example 4: Delayed Message ===");
    let msg = Message::builder()
        .topic("delayed-topic")
        .body_slice(b"Delayed message")
        .delay_level(3) // 10 seconds
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Delay Level: {}", msg.delay_level());
    println!();

    // Example 5: Message with custom properties
    println!("=== Example 5: Message with Properties ===");
    let msg = Message::builder()
        .topic("user-topic")
        .body_slice(b"User event")
        .tags("user-event")
        .key("user-789")
        .buyer_id("buyer-001")
        .instance_id("instance-1")
        .correlation_id("corr-123")
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Buyer ID: {:?}", msg.buyer_id());
    println!("Instance ID: {:?}", msg.instance_id());
    println!();

    // Example 6: Transaction message
    println!("=== Example 6: Transaction Message ===");
    let msg = Message::builder()
        .topic("transaction-topic")
        .body_slice(b"Transaction data")
        .transaction_id("tx-abc-123")
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Transaction ID: {:?}", msg.transaction_id());
    println!();

    // Example 7: Message with delay in milliseconds
    println!("=== Example 7: Message with Delay in MS ===");
    let msg = Message::builder()
        .topic("scheduled-topic")
        .body_slice(b"Scheduled message")
        .delay_millis(5000) // 5 seconds
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!();

    // Example 8: Message with all options
    println!("=== Example 8: Complete Message ===");
    let msg = Message::builder()
        .topic("complete-topic")
        .body_slice(b"Complete message with all options")
        .tags("complete")
        .keys(vec!["key1".to_string(), "key2".to_string()])
        .delay_level(2)
        .buyer_id("buyer-002")
        .instance_id("instance-2")
        .correlation_id("corr-456")
        .sharding_key("shard-1")
        .trace_switch(true)
        .wait_store_msg_ok(true)
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Tags: {:?}", msg.tags());
    println!("Keys: {:?}", msg.keys());
    println!("Delay Level: {}", msg.delay_level());
    println!("Wait Store: {}", msg.wait_store_msg_ok());
    println!();

    // Example 9: Validation with Result
    println!("=== Example 9: Builder Validation ===");
    let result = Message::builder().body_slice(b"Missing topic").build();

    match result {
        Ok(msg) => println!("Message created: {}", msg.topic()),
        Err(e) => println!("Validation error: {:?}", e),
    }
    println!();

    // Example 10: Using Bytes directly
    println!("=== Example 10: Using Bytes ===");
    let body_data = bytes::Bytes::from_static(b"Bytes body");
    let msg = Message::builder()
        .topic("bytes-topic")
        .body(body_data)
        .build_unchecked();

    println!("Topic: {}", msg.topic());
    println!("Body length: {}", msg.body_slice().len());
    println!();

    println!("All examples completed successfully!");
}
