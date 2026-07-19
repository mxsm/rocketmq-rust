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

//! Integration tests for PullMessageService
//!
//! Tests cover:
//! - Service lifecycle (creation, start, shutdown)
//! - Task execution (immediate and delayed)
//! - Concurrent access patterns
//! - Performance benchmarks
//! - Java alignment verification

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::consumer::consumer_impl::process_queue::ProcessQueue;
use rocketmq_client_rust::consumer::consumer_impl::pull_message_service::PullMessageService;
use rocketmq_client_rust::consumer::consumer_impl::pull_request::PullRequest;
use rocketmq_client_rust::factory::mq_client_instance::MQClientInstance;
use rocketmq_common::common::message::message_queue::MessageQueue;
/// Creates a mock MQClientInstance for testing
fn create_mock_client_instance() -> Arc<MQClientInstance> {
    let client_config = ClientConfig::default();
    MQClientInstance::new_arc(client_config, 0, CheetahString::from_static_str("test_client"), None)
}

/// Creates a test PullRequest
fn create_test_pull_request(consumer_group: &str, topic: &str) -> PullRequest {
    let mq = MessageQueue::from_parts(topic, "test_broker", 0);
    PullRequest {
        consumer_group: CheetahString::from_string(consumer_group.to_string()),
        message_queue: mq,
        process_queue: Arc::new(ProcessQueue::new()),
        next_offset: 0,
        previously_locked: false,
    }
}

#[tokio::test]
async fn test_service_creation() {
    let service = PullMessageService::new();
    assert!(!service.is_stopped());
    assert_eq!(service.get_service_name(), "PullMessageService");
}

#[tokio::test]
async fn test_service_creation_with_custom_capacity() {
    let service = PullMessageService::with_capacity(1024);
    assert!(!service.is_stopped());
}

#[test]
fn test_service_creation_with_custom_shards_normalizes_zero() {
    let service = PullMessageService::with_capacity_and_shards(1024, 0);

    assert_eq!(service.shard_count(), 1);
}

#[test]
fn test_same_queue_maps_to_same_pull_shard() {
    let service = PullMessageService::with_capacity_and_shards(1024, 4);
    let first = create_test_pull_request("test_group", "test_topic");
    let mut second = create_test_pull_request("test_group", "test_topic");
    second.set_next_offset(1024);

    assert_eq!(
        service.shard_index_for_pull_request("test_client", &first),
        service.shard_index_for_pull_request("test_client", &second)
    );
}

#[tokio::test]
async fn test_service_start_and_shutdown() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();

    // Start service
    let result = service.start(instance).await;
    assert!(result.is_ok());
    assert!(!service.is_stopped());

    // Shutdown service
    let result = service.shutdown(1000).await;
    assert!(result.is_ok());
    assert!(service.is_stopped());
}

#[tokio::test]
async fn test_start_spawns_configured_pull_workers() {
    let service = PullMessageService::with_capacity_and_shards(1024, 4);
    let instance = create_mock_client_instance();

    service.start(instance).await.unwrap();

    let mut snapshot = service.shard_snapshot().await;
    for _ in 0..100 {
        if snapshot.worker_task_count == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        snapshot = service.shard_snapshot().await;
    }

    assert_eq!(snapshot.shard_count, 4);
    assert_eq!(snapshot.worker_task_count, 4);
    assert_eq!(snapshot.shards.len(), 4);

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_double_start_warning() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();

    // First start should succeed
    assert!(service.start(instance.clone()).await.is_ok());

    // Second start should return Ok (idempotent)
    assert!(service.start(instance).await.is_ok());
}

#[tokio::test]
async fn test_double_shutdown_warning() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();

    service.start(instance).await.unwrap();

    // First shutdown
    assert!(service.shutdown(1000).await.is_ok());

    // Second shutdown should return Ok (idempotent)
    assert!(service.shutdown(1000).await.is_ok());
}

#[tokio::test]
async fn test_execute_pull_request_immediately() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    let pull_request = create_test_pull_request("test_group", "test_topic");

    // Execute should succeed
    service.execute_pull_request_immediately(pull_request).await;

    // Give it time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_execute_pull_request_after_shutdown() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();
    service.shutdown(100).await.unwrap();

    let pull_request = create_test_pull_request("test_group", "test_topic");

    // Should log warning but not panic
    service.execute_pull_request_immediately(pull_request).await;
}

#[tokio::test]
async fn test_execute_pull_request_later() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    let pull_request = create_test_pull_request("test_group", "test_topic");

    // Execute with delay
    service.execute_pull_request_later(pull_request, 50);

    // Wait for delayed execution
    tokio::time::sleep(Duration::from_millis(150)).await;

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_delayed_task_cancelled_after_shutdown() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    let pull_request = create_test_pull_request("test_group", "test_topic");

    // Schedule delayed task
    service.execute_pull_request_later(pull_request, 200);

    // Shutdown before task executes
    tokio::time::sleep(Duration::from_millis(50)).await;
    service.shutdown(100).await.unwrap();

    // Task should be cancelled due to is_stopped() check
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_delayed_requests_use_single_scheduler_task() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    for i in 0..1000 {
        let pull_request = create_test_pull_request(&format!("group_{}", i % 10), &format!("topic_{}", i % 5));
        service.execute_pull_request_later(pull_request, 60_000);
    }

    let mut snapshot = service.delayed_scheduler_snapshot();
    for _ in 0..100 {
        if snapshot.scheduler_task_count == 1 && snapshot.queue_depth == 1000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        snapshot = service.delayed_scheduler_snapshot();
    }

    assert_eq!(snapshot.submitted_count, 1000);
    assert_eq!(snapshot.queue_depth, 1000);
    assert_eq!(snapshot.scheduler_task_count, 1);

    service.shutdown(1000).await.unwrap();
    let snapshot = service.delayed_scheduler_snapshot();
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.cancelled_count, 1000);
}

#[tokio::test]
async fn test_execute_task() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    service.execute_task(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    });

    // Wait for task execution
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_execute_task_later() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    service.execute_task_later(
        move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        },
        50,
    );

    // Task should not have executed yet
    assert_eq!(counter.load(Ordering::SeqCst), 0);

    // Wait for delayed execution
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_delayed_tasks_with_same_deadline_keep_insert_order() {
    let service = PullMessageService::new();
    let observed = Arc::new(StdMutex::new(Vec::new()));

    for index in 0..8 {
        let observed = observed.clone();
        service.execute_task_later(
            move || {
                observed
                    .lock()
                    .expect("observed order lock should not be poisoned")
                    .push(index);
            },
            20,
        );
    }

    for _ in 0..100 {
        if observed
            .lock()
            .expect("observed order lock should not be poisoned")
            .len()
            == 8
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(
        *observed.lock().expect("observed order lock should not be poisoned"),
        (0..8).collect::<Vec<_>>()
    );
    let snapshot = service.delayed_scheduler_snapshot();
    assert_eq!(snapshot.queue_depth, 0);
    assert_eq!(snapshot.expired_count, 8);

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_execute_task_after_shutdown() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();
    service.shutdown(100).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Should log warning and not execute
    service.execute_task(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_concurrent_execute_immediately() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    let mut handles = vec![];

    // Spawn multiple tasks executing requests
    for i in 0_usize..10 {
        let service_clone = service.clone();
        let handle = tokio::spawn(async move {
            let pull_request = create_test_pull_request(&format!("group_{}", i), &format!("topic_{}", i));
            service_clone.execute_pull_request_immediately(pull_request).await;
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // Give time to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_concurrent_delayed_requests() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    // Schedule multiple delayed requests
    for i in 0_usize..10 {
        let service_clone = service.clone();
        let pull_request = create_test_pull_request(&format!("group_{}", i), &format!("topic_{}", i));
        service_clone.execute_pull_request_later(pull_request, 50 + (i as u64 * 10));
    }

    // Wait for all to execute
    tokio::time::sleep(Duration::from_millis(300)).await;

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_shutdown_timeout() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    // Shutdown with very short timeout
    let result = service.shutdown(1).await;
    assert!(result.is_ok()); // Should still succeed even with timeout
    assert!(service.is_stopped());
}

#[tokio::test]
async fn test_graceful_shutdown_with_pending_requests() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    // Submit multiple requests
    for i in 0..5 {
        let pull_request = create_test_pull_request(&format!("group_{}", i), &format!("topic_{}", i));
        service.execute_pull_request_immediately(pull_request).await;
    }

    // Graceful shutdown should wait for processing
    let result = service.shutdown(2000).await;
    assert!(result.is_ok());
    assert!(service.is_stopped());
}

#[tokio::test]
async fn test_is_stopped_flag() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();

    // Before start
    assert!(!service.is_stopped());

    // After start
    service.start(instance).await.unwrap();
    assert!(!service.is_stopped());

    // After shutdown
    service.shutdown(100).await.unwrap();
    assert!(service.is_stopped());
}

#[tokio::test]
async fn test_default_shutdown() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    // Use default shutdown timeout
    let result = service.shutdown_default().await;
    assert!(result.is_ok());
    assert!(service.is_stopped());
}

#[tokio::test]
async fn test_service_clone() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    // Clone should share the same state
    let service_clone = service.clone();

    service.shutdown(100).await.unwrap();

    // Both should show stopped
    assert!(service.is_stopped());
    assert!(service_clone.is_stopped());
}

#[tokio::test]
async fn test_high_throughput() {
    let service = PullMessageService::with_capacity(10000);
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    let start = std::time::Instant::now();

    // Submit 1000 requests
    for i in 0..1000 {
        let pull_request = create_test_pull_request(&format!("group_{}", i % 10), &format!("topic_{}", i % 5));
        service.execute_pull_request_immediately(pull_request).await;
    }

    let elapsed = start.elapsed();
    println!("Submitted 1000 requests in {:?}", elapsed);

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    service.shutdown(2000).await.unwrap();
}

#[tokio::test]
async fn test_mixed_immediate_and_delayed_requests() {
    let service = PullMessageService::new();
    let instance = create_mock_client_instance();
    service.start(instance).await.unwrap();

    // Mix of immediate and delayed
    for i in 0_usize..10 {
        let pull_request = create_test_pull_request(&format!("group_{}", i), &format!("topic_{}", i));

        if i.is_multiple_of(2) {
            service.execute_pull_request_immediately(pull_request).await;
        } else {
            service.execute_pull_request_later(pull_request, 50);
        }
    }

    // Wait for all to complete
    tokio::time::sleep(Duration::from_millis(300)).await;

    service.shutdown(1000).await.unwrap();
}

#[tokio::test]
async fn test_service_name() {
    let service = PullMessageService::new();
    assert_eq!(service.get_service_name(), "PullMessageService");
}

#[tokio::test]
async fn test_execute_before_start() {
    let service = PullMessageService::new();
    let pull_request = create_test_pull_request("test_group", "test_topic");

    // Should log warning but not panic
    service.execute_pull_request_immediately(pull_request).await;
}
