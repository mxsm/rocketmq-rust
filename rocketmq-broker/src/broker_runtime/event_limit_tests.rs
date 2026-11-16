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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_store::MessageArrivingListener;

use super::*;
use crate::long_polling::notify_message_arriving_listener::NotifyMessageArrivingListener;

#[tokio::test]
async fn d21_held_arrival_listener_uses_published_default_for_new_notifications_without_dropping_old_events() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_string_lossy().into_owned();
    let config = Arc::new(BrokerConfig {
        store_path_root_dir: root.clone().into(),
        auth_config_path: directory.path().join("auth").to_string_lossy().into_owned().into(),
        max_client_event_count: 3,
        lite_event_full_dispatch_delay_time: 60_000,
        ..BrokerConfig::default()
    });
    let store = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.into(),
        ..Default::default()
    });
    let mut runtime = BrokerRuntime::new(config, store);
    runtime.initialize().await.unwrap();
    let (_processor, _) = runtime.init_processor_checked().unwrap();
    let state = &runtime.composition.state;
    let producer = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .unwrap()
        .producer
        .as_ref()
        .unwrap()
        .clone();
    let registry = state.lite_subscription_registry().clone();
    let dispatcher = state.lite_event_dispatcher().clone();
    let listener = NotifyMessageArrivingListener::new(
        producer,
        registry.clone(),
        dispatcher.clone(),
        state.subscription_group_manager().config_lookup(),
        state.broker_config().max_client_event_count as usize,
        60_000,
        60_000,
    )
    .with_client_event_limit(state.lite_client_event_limit());
    let parent = CheetahString::from_static_str("d21-event-parent");
    let group = CheetahString::from_static_str("d21-event-group");
    let old_client = CheetahString::from_static_str("d21-before-generation");
    let new_client = CheetahString::from_static_str("d21-after-generation");
    let events: HashSet<_> = (0..3)
        .map(|index| CheetahString::from_string(format!("%LMQ%$d21-event-parent${index}")))
        .collect();
    registry.add_complete_subscription(&old_client, &group, &parent, &events, 1, false);
    for event in &events {
        listener.arriving(event, 0, 1, None, 0, None, None);
    }
    let before = dispatcher.pending_events(&old_client);
    assert_eq!(
        before.len(),
        3,
        "the original generation must actually dispatch three identifiable events"
    );
    let mut next = (*runtime.composition.state.broker_config()).clone();
    next.max_client_event_count = 1;
    runtime.composition.state.set_broker_config(next).unwrap();
    registry.add_complete_subscription(&new_client, &group, &parent, &events, 1, false);
    for event in &events {
        listener.arriving(event, 0, 2, None, 0, None, None);
    }
    let after = dispatcher.pending_events(&new_client);
    let retained = dispatcher.pending_events(&old_client);
    let mut group_results = Vec::new();
    // Reads must preserve fallback for legacy stored nonpositive attributes,
    // even though today's update validator constrains newly written values.
    for (label, value, expected) in [
        ("override", Some(10), 3),
        ("zero", Some(0), 1),
        ("negative", Some(-1), 1),
        ("absent", None, 1),
    ] {
        let group = CheetahString::from_string(format!("d21-{label}-group"));
        let client = CheetahString::from_string(format!("d21-{label}-client"));
        let mut config =
            rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::new(
                group.clone(),
            );
        if let Some(value) = value {
            config.set_attributes(std::collections::HashMap::from([(
                rocketmq_model::common::attribute::subscription_group_attributes::LITE_SUB_CLIENT_MAX_EVENT_COUNT_ATTRIBUTE_NAME.into(),
                value.to_string().into())]));
        }
        runtime
            .composition
            .state
            .subscription_group_manager()
            .subscription_group_table()
            .insert(group.clone(), Arc::new(config));
        registry.add_complete_subscription(&client, &group, &parent, &events, 1, false);
        for event in &events {
            listener.arriving(event, 0, 3, None, 0, None, None);
        }
        group_results.push((dispatcher.pending_events(&client), expected));
    }
    let budget = dispatcher.budget_snapshot();
    runtime.shutdown().await;
    assert_eq!(
        after.len(),
        1,
        "the held real listener must use the new default for subsequent notifications"
    );
    assert!(after.iter().all(|event| events.contains(event)));
    assert_eq!(retained, before, "a decrease must not erase already queued payloads");
    for (payloads, expected) in group_results {
        assert_eq!(payloads.len(), expected);
        assert!(payloads.iter().all(|event| events.contains(event)));
    }
    assert_eq!(
        budget.admitted_count.checked_sub(budget.released_count),
        Some(budget.current_count as u64),
        "retained and released permits must account for all actual admissions"
    );
    assert!(
        budget.current_count <= 18,
        "new overflow may be dropped by existing limits; no duplicate input permit is retained"
    );
}
