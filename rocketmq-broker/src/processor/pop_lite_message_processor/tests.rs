// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;

use std::sync::Arc;
use std::time::Duration;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_runtime::RuntimeContext;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StorePorts;

use crate::broker_runtime::BrokerMessageStore;
use crate::broker_runtime::BrokerRuntime;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingPolicy;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingService;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingServiceContext;
use crate::processor::pop_message_processor::QueueLockManager;

fn pop_lite_processor_for_test(runtime: &mut BrokerRuntime) -> Arc<PopLiteMessageProcessor<BrokerMessageStore>> {
    let inner = runtime.runtime_state_mut();
    let topic_config_manager = inner.topic_config_manager_handle();
    let subscription_group_lookup = inner.subscription_group_manager().config_lookup();
    let lite_event_dispatcher = inner.lite_event_dispatcher().clone();
    let service_context = inner.broker_service_context();
    let queue_lock_manager = service_context
        .clone()
        .map(QueueLockManager::new_with_service_context)
        .unwrap_or_else(QueueLockManager::new);
    let long_polling = PopLiteLongPollingServiceContext::try_with_resource_budget(
        PopLiteLongPollingPolicy::from_config(&inner.broker_config()),
        lite_event_dispatcher.clone(),
        service_context,
        inner.resource_budget(),
    )
    .expect("test Broker resource budget");
    let consumer_offset_manager = inner.consumer_offset_manager_handle();
    let escape_bridge = inner.escape_bridge();

    PopLiteMessageProcessor::new(PopLiteMessageProcessorContext::new(
        PopLiteMessagePolicy::from_config(&inner.broker_config()),
        topic_config_manager,
        subscription_group_lookup,
        PopLiteOffsetCapability::new(&consumer_offset_manager),
        PopLiteMessageStoreCapability::new(&escape_bridge),
        lite_event_dispatcher,
        queue_lock_manager,
        long_polling,
    ))
}

#[test]
fn transform_order_count_info_drops_queue_level_suffix_when_offset_entries_exist() {
    let result = PopLiteMessageProcessor::<StorePorts>::transform_order_count_info("0 qo0%100 1;0 0 1", 1);

    assert_eq!(result, "0 qo0%100 1");
}

#[test]
fn pop_lite_message_policy_captures_only_required_startup_values() {
    let broker_config = BrokerConfig {
        broker_ip1: CheetahString::from_static_str("192.0.2.10"),
        broker_permission: 4,
        max_client_event_count: 17,
        lite_event_full_dispatch_delay_time: 29,
        ..Default::default()
    };

    let policy = PopLiteMessagePolicy::from_config(&broker_config);

    assert_eq!(policy.broker_ip1, "192.0.2.10");
    assert_eq!(policy.broker_permission, 4);
    assert_eq!(policy.max_client_event_count, 17);
    assert_eq!(policy.lite_event_full_dispatch_delay_time, 29);
}

#[test]
fn pop_lite_message_processor_source_uses_only_explicit_capabilities() {
    let source = include_str!("../pop_lite_message_processor.rs");

    assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
    assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
    assert!(!source.contains(concat!("broker_runtime", "_inner")));
    assert!(source.contains("context: PopLiteMessageProcessorContext<MS>"));
    assert!(source.contains("consumer_offset: PopLiteOffsetCapability<MS>"));
    assert!(source.contains("message_store: PopLiteMessageStoreCapability<MS>"));
}

#[test]
fn pop_lite_message_providers_do_not_keep_runtime_or_store_alive() {
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let inner = runtime.runtime_state_mut();
    let offset_manager = inner.consumer_offset_manager_handle();
    let offset = PopLiteOffsetCapability::new(&offset_manager);
    let escape_bridge = inner.escape_bridge();
    let store = PopLiteMessageStoreCapability::new(&escape_bridge);
    let group = CheetahString::from_static_str("group");
    let topic = CheetahString::from_static_str("topic");

    assert!(store.is_available());
    drop(offset_manager);
    drop(escape_bridge);
    drop(runtime);

    assert!(!store.is_available());
    assert_eq!(offset.query_offset(&group, &topic), -1);
    assert_eq!(offset.query_then_erase_reset_offset(&topic, &group), None);
    offset.commit_offset("provider-shutdown-test", &group, &topic, 1);
}

#[tokio::test]
async fn pop_lite_long_polling_service_uses_weak_processor_back_reference() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}

    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let processor = pop_lite_processor_for_test(&mut runtime);
    let processor_weak = Arc::downgrade(&processor);
    let service = processor.pop_lite_long_polling_service.clone();

    assert_send_sync(&processor);
    drop(processor);

    assert!(processor_weak.upgrade().is_none());
    assert_eq!(Arc::strong_count(&service), 1);
}

#[tokio::test]
async fn pop_lite_long_polling_service_start_shutdown_and_restart_are_serialized() {
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let processor = pop_lite_processor_for_test(&mut runtime);
    let service = processor.pop_lite_long_polling_service.clone();

    PopLiteLongPollingService::start(&service).await;
    let first_task_group = service
        .task_group_for_test()
        .expect("long-polling task group should be installed");
    PopLiteLongPollingService::start(&service).await;
    assert_eq!(
        service
            .task_group_for_test()
            .expect("duplicate start should retain the task group")
            .task_count(),
        1
    );
    assert_eq!(first_task_group.task_count(), 1);
    assert!(service.is_running());

    service.shutdown().await;
    assert!(!service.is_running());
    assert!(service.task_group_for_test().is_none());

    PopLiteLongPollingService::start(&service).await;
    let restarted_task_group = service
        .task_group_for_test()
        .expect("restart should install a task group");
    assert_eq!(restarted_task_group.task_count(), 1);
    assert!(!restarted_task_group.cancellation_token().is_cancelled());
    service.shutdown().await;
    assert!(!service.is_running());
}

#[tokio::test]
async fn pop_lite_long_polling_service_uses_broker_parent_task_group() {
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let runtime_context = RuntimeContext::from_current("pop-lite-long-polling-parent-test");
    let broker_service = runtime_context.service_context("broker-service");
    let mut runtime =
        BrokerRuntime::new_with_service_context(broker_config, message_store_config, broker_service.clone());
    let parent_id = runtime
        .runtime_state_mut()
        .broker_service_task_group()
        .expect("broker service task group should exist")
        .id();
    let processor = pop_lite_processor_for_test(&mut runtime);
    let service = processor.pop_lite_long_polling_service.clone();

    PopLiteLongPollingService::start(&service).await;
    let task_group = service
        .task_group_for_test()
        .expect("POP Lite long-polling task group should be installed");

    assert_eq!(task_group.parent_id(), Some(parent_id));
    service.shutdown().await;
    let report = broker_service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[tokio::test]
async fn active_pop_lite_long_polling_scan_does_not_keep_owner_alive() {
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let processor = pop_lite_processor_for_test(&mut runtime);
    let service = processor.pop_lite_long_polling_service.clone();
    let processor_weak = Arc::downgrade(&processor);
    let service_weak = Arc::downgrade(&service);

    PopLiteLongPollingService::start(&service).await;
    drop(processor);
    drop(service);

    tokio::time::timeout(Duration::from_secs(1), async {
        while service_weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scan task should release the service owner");
    assert!(processor_weak.upgrade().is_none());
}

#[tokio::test]
async fn cancelled_store_read_releases_queue_lock_for_competing_pop_lite_request() {
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let processor = pop_lite_processor_for_test(&mut runtime);
    let entered_store = Arc::new(tokio::sync::Barrier::new(2));
    let release_store = Arc::new(tokio::sync::Barrier::new(2));
    processor
        .context
        .message_store
        .set_store_await_hook(Arc::clone(&entered_store), release_store);
    let lmq_name = CheetahString::from_static_str("%LMQ%$parent-topic$cancelled-store");
    let consumer_group = CheetahString::from_static_str("group-a");
    let header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("cancelled-store-client"),
        consumer_group: consumer_group.clone(),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 30_000,
        poll_time: 60_000,
        born_time: 0,
        attempt_id: None,
        rpc: None,
    };
    let processor_for_read = Arc::clone(&processor);
    let lmq_for_read = lmq_name.clone();
    let read_task = tokio::spawn(async move { processor_for_read.pop_from_events(&header, vec![lmq_for_read]).await });

    entered_store.wait().await;
    let lock_key = CheetahString::from_string(QueueLockManager::build_lock_key(&lmq_name, &consumer_group, 0));
    assert!(
        processor
            .context
            .queue_lock_manager
            .try_acquire_with_key(lock_key.clone())
            .await
            .is_none(),
        "the blocked Store read retains the exact queue lock entry"
    );

    read_task.abort();
    assert!(
        read_task.await.expect_err("cancelled Store read task").is_cancelled(),
        "the Store read future is cancelled at the deterministic barrier"
    );
    let competing = processor
        .context
        .queue_lock_manager
        .try_acquire_with_key(lock_key)
        .await
        .expect("cancellation releases the queue lock for a competing request");
    drop(competing);
}
