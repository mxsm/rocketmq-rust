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

//! Integration tests for Phase 1 refactoring
//!
//! Tests cover:
//! - Producer basic functionality
//! - Concurrent access patterns (DashMap migration verification)
//! - Performance baseline establishment

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;

#[test]
fn test_producer_creation() {
    // Test: Basic producer creation
    let producer = DefaultMQProducer::new();

    // Producer should be created successfully with empty group (can be set later)
    assert_eq!(producer.producer_config().producer_group().as_str(), "");
}

#[test]
fn test_concurrent_producer_access() {
    // Test: Verify concurrent access to producer doesn't deadlock
    // This validates the DashMap migration

    let producer = Arc::new(DefaultMQProducer::new());
    let mut handles = vec![];

    for _i in 0..10 {
        let producer_clone = producer.clone();
        let handle = thread::spawn(move || {
            // Access producer configuration concurrently
            let _ = producer_clone.producer_config().producer_group();
            let _ = producer_clone.client_config();
        });
        handles.push(handle);
    }

    // Wait for all threads - test passes if no deadlock
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_performance_baseline_concurrent_reads() {
    // Performance baseline test: Establish metrics for Phase 2
    use std::time::Instant;

    let producer = Arc::new(DefaultMQProducer::new());

    // Benchmark concurrent reads
    let start = Instant::now();
    let mut handles = vec![];

    for _ in 0..50 {
        let producer_clone = producer.clone();
        let handle = thread::spawn(move || {
            for _ in 0..1000 {
                // Simulate typical read operations
                let _ = producer_clone.producer_config().producer_group();
                let _ = producer_clone.client_config();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    println!("✓ Concurrent config reads (50 threads × 1000 ops): {:?}", duration);

    // Baseline: should complete in reasonable time
    assert!(
        duration < Duration::from_secs(3),
        "Performance regression detected: {:?}",
        duration
    );
}

#[test]
fn test_message_creation() {
    // Test: Verify basic message creation works
    use rocketmq_common::common::message::message_single::Message;

    let msg = Message::new("test_topic", b"test_body");
    assert_eq!(msg.get_topic(), "test_topic");
}

#[tokio::test]
async fn test_producer_async_context() {
    // Test: Verify producer works in async context
    let producer = DefaultMQProducer::new();

    // Should be accessible in async context
    let group = producer.producer_config().producer_group();
    assert_eq!(group.as_str(), ""); // Default is empty, can be configured
}

#[test]
fn test_cheetah_string_operations() {
    // Test: Verify CheetahString operations for topic names
    let topic1 = CheetahString::from_static_str("topic_1");
    let topic2 = CheetahString::from_string("topic_2".to_string());

    assert_eq!(topic1.as_str(), "topic_1");
    assert_eq!(topic2.as_str(), "topic_2");
    assert_ne!(topic1, topic2);
}

#[test]
fn new_has_default_values() {
    let tpi = TopicPublishInfo::new();
    assert!(!tpi.order_topic);
    assert!(!tpi.have_topic_router_info);
    assert!(tpi.message_queue_list.is_empty());
    assert!(tpi.topic_route_data.is_none());
}

#[test]
fn ok_returns_true_when_non_empty() {
    let mut tpi = TopicPublishInfo::new();
    assert!(!tpi.ok());
    tpi.message_queue_list.push(MessageQueue::from_parts("t", "b", 1));
    assert!(tpi.ok());
}

#[test]
fn select_with_empty_list_returns_none() {
    let tpi = TopicPublishInfo::new();
    assert!(tpi.select_one_message_queue_filters(&[]).is_none());
}

#[test]
fn select_one_message_queue_returns_item_from_list() {
    let mut tpi = TopicPublishInfo::new();
    let mq1 = MessageQueue::from_parts("t", "b1", 1);
    let mq2 = MessageQueue::from_parts("t", "b2", 2);
    let mq3 = MessageQueue::from_parts("t", "b3", 3);
    tpi.message_queue_list = vec![mq1.clone(), mq2.clone(), mq3.clone()];
    for _ in 0..10 {
        let out = tpi.select_one_message_queue();
        assert!(out.is_some());
        let out = out.unwrap();
        assert!(out == mq1 || out == mq2 || out == mq3);
    }
}

#[test]
fn select_by_broker_prefers_different_one() {
    let mut tpi = TopicPublishInfo::new();
    let mq1 = MessageQueue::from_parts("t", "b1", 1);
    let mq2 = MessageQueue::from_parts("t", "b2", 2);
    tpi.message_queue_list = vec![mq1.clone(), mq2.clone()];
    let last = CheetahString::from("b1");
    let out = tpi.select_one_message_queue_by_broker(Some(&last));
    assert!(out.is_some());
    let out = out.unwrap();
    assert_ne!(out.get_broker_name(), &last);
}

#[test]
fn select_by_broker_falls_back_when_all_same() {
    let mut tpi = TopicPublishInfo::new();
    let mq1 = MessageQueue::from_parts("t", "same", 1);
    let mq2 = MessageQueue::from_parts("t", "same", 2);
    tpi.message_queue_list = vec![mq1.clone(), mq2.clone()];
    let last = CheetahString::from("same");
    let out = tpi.select_one_message_queue_by_broker(Some(&last));
    assert!(out.is_some());
    let out = out.unwrap();
    assert_eq!(out.get_broker_name(), &last);
}

#[test]
fn select_with_filters_matches_and_no_match() {
    let mut tpi = TopicPublishInfo::new();
    let mq1 = MessageQueue::from_parts("t", "b1", 1);
    let mq2 = MessageQueue::from_parts("t", "b2", 2);
    tpi.message_queue_list = vec![mq1.clone(), mq2.clone()];

    let filter_match = |mq: &MessageQueue| mq.get_queue_id() == 2;
    let out = tpi.select_one_message_queue_filters(&[&filter_match]);
    assert!(out.is_some());
    assert_eq!(out.unwrap(), mq2);

    let filter_none = |_: &MessageQueue| false;
    let out = tpi.select_one_message_queue_filters(&[&filter_none]);
    assert!(out.is_none());
}

#[test]
fn reset_index_does_not_panic_and_selection_works() {
    let mut tpi = TopicPublishInfo::new();
    tpi.message_queue_list.push(MessageQueue::from_parts("t", "b", 1));
    tpi.reset_index();
    let out = tpi.select_one_message_queue();
    assert!(out.is_some());
}
