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

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_store::MessageStore;
use rocketmq_store::MessageStoreConfig;

use crate::broker::broker_runtime_config_state::BrokerPermissionState;
use crate::broker_runtime::BrokerMessageStore;
use crate::broker_runtime::BrokerRuntime;
use crate::processor::pop_message_processor::QueueLockManager;

pub(super) fn pop_lite_processor_for_test(
    runtime: &mut BrokerRuntime,
) -> Arc<PopLiteMessageProcessor<BrokerMessageStore>> {
    let inner = runtime.runtime_state_mut();
    let topic_config_manager = inner.topic_config_manager_handle();
    let subscription_group_lookup = inner.subscription_group_manager().config_lookup();
    let lite_event_dispatcher = inner.lite_event_dispatcher().clone();
    let service_context = inner.broker_service_context();
    let queue_lock_manager = QueueLockManager::new_with_service_context(service_context.clone());
    let consumer_offset_manager = inner.consumer_offset_manager_handle();
    let escape_bridge = inner.escape_bridge();

    PopLiteMessageProcessor::new(PopLiteMessageProcessorContext::new(
        PopLiteMessagePolicy::from_config(&inner.broker_config(), inner.broker_permission_state())
            .with_client_event_limit(inner.lite_client_event_limit()),
        topic_config_manager,
        subscription_group_lookup,
        PopLiteOffsetCapability::new(&consumer_offset_manager),
        PopLiteMessageStoreCapability::new(&escape_bridge),
        lite_event_dispatcher,
        queue_lock_manager,
    ))
}

#[test]
fn transform_order_count_info_drops_queue_level_suffix_when_offset_entries_exist() {
    let result = PopLiteMessageProcessor::<MessageStore>::transform_order_count_info("0 qo0%100 1;0 0 1", 1);

    assert_eq!(result, "0 qo0%100 1");
}

#[test]
fn pop_lite_message_policy_observes_live_permission_and_keeps_other_startup_values() {
    let broker_config = BrokerConfig {
        broker_ip1: CheetahString::from_static_str("192.0.2.10"),
        broker_permission: 4,
        max_client_event_count: 17,
        lite_event_full_dispatch_delay_time: 29,
        ..Default::default()
    };

    let broker_permission = BrokerPermissionState::new(broker_config.broker_permission);
    let policy = PopLiteMessagePolicy::from_config(&broker_config, broker_permission.clone());

    assert_eq!(policy.broker_ip1, "192.0.2.10");
    assert_eq!(policy.broker_permission.get(), 4);
    assert_eq!(policy.max_client_event_count.get(), 17);
    assert_eq!(policy.lite_event_full_dispatch_delay_time, 29);
    broker_permission.update(6);
    assert_eq!(policy.broker_permission.get(), 6);
    broker_permission.update(4);
    assert_eq!(policy.broker_permission.get(), 4);
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

#[tokio::test]
async fn d21_held_pop_lite_requeue_uses_published_limit_and_preserves_reserved_payloads() {
    let mut runtime = BrokerRuntime::new(
        Arc::new(BrokerConfig {
            max_client_event_count: 3,
            lite_event_full_dispatch_delay_time: 0,
            ..Default::default()
        }),
        Arc::new(MessageStoreConfig::default()),
    );
    let processor = pop_lite_processor_for_test(&mut runtime);
    let dispatcher = processor.context.lite_event_dispatcher.clone();
    let client = CheetahString::from_static_str("d21-pop-client");
    let group = CheetahString::from_static_str("d21-pop-group");
    let events: HashSet<_> = (0..3)
        .map(|i| CheetahString::from_string(format!("%LMQ%$parent${i}")))
        .collect();
    assert_eq!(
        dispatcher.do_full_dispatch_with_limit(&client, &group, &events, 3, 0),
        3
    );
    let batch = dispatcher.reserve_pending_events(&client).unwrap().commit();
    let mut locks = Vec::new();
    for event in &events {
        locks.push(
            processor
                .context
                .queue_lock_manager
                .try_acquire_with_key(QueueLockManager::build_lock_key(event, &group, 0).into())
                .await
                .unwrap(),
        );
    }
    let mut next = (*runtime.runtime_state_mut().broker_config()).clone();
    next.max_client_event_count = 1;
    runtime.runtime_state_mut().set_broker_config(next).unwrap();
    let selected_limit = processor.lite_dispatch_policy(&group).0;
    let header = PopLiteMessageRequestHeader {
        client_id: client.clone(),
        consumer_group: group,
        topic: "parent".into(),
        max_msg_num: 1,
        invisible_time: 30_000,
        poll_time: 0,
        born_time: 0,
        attempt_id: None,
        rpc: None,
    };
    let result = processor.execute_pop_lite_batch(&header, batch).await;
    assert_eq!(result.fetched_count, 0);
    let pending = dispatcher.pending_events(&client);
    let retained_count = dispatcher.budget_snapshot().current_count;
    let all = dispatcher.take_pending_events(&client);
    drop(locks);
    assert_eq!(
        selected_limit, 1,
        "the held POP owner must select the current published default"
    );
    assert_eq!(
        pending.len(),
        3,
        "previously reserved permits retain all original payloads on requeue"
    );
    assert_eq!(retained_count, 3, "in-place requeue must not acquire duplicate permits");
    assert_eq!(
        dispatcher.budget_snapshot().current_count,
        0,
        "consuming all events returns their permits"
    );
    assert!(pending.iter().all(|event| events.contains(event)));
    assert_eq!(
        all.into_iter().collect::<HashSet<_>>(),
        events,
        "no reserved/deferred event is discarded by a decrease"
    );
}
