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

use super::LiveSubscriptionGroupBroker;
use super::SubscriptionGroupBeforeBroker;
use super::SubscriptionGroupBeforeState;
use super::SubscriptionGroupConfigCasState;
use super::SubscriptionGroupPatch;
use super::patch_matches;
use super::safety_matches_before;
use super::safety_state;
use super::select_before_values;

fn state() -> SubscriptionGroupConfigCasState {
    SubscriptionGroupConfigCasState {
        version: 7,
        retry_max_times: 16,
        retry_queue_nums: 1,
        consume_timeout_minutes: 15,
        consume_enable: true,
        consume_from_min_enable: true,
        consume_broadcast_enable: true,
        consume_message_orderly: false,
        broker_id: 0,
        which_broker_when_consume_slowly: 1,
        notify_consumer_ids_changed_enable: true,
        group_sys_flag: 0,
    }
}

#[test]
fn before_patch_contains_only_requested_fields() {
    let before = select_before_values(
        SubscriptionGroupPatch {
            retry_max_times: Some(16),
            retry_queue_nums: Some(1),
            consume_timeout_minutes: Some(15),
        },
        &SubscriptionGroupPatch {
            retry_max_times: Some(8),
            retry_queue_nums: None,
            consume_timeout_minutes: Some(30),
        },
    )
    .expect("closed before patch");
    assert_eq!(
        before,
        SubscriptionGroupPatch {
            retry_max_times: Some(16),
            retry_queue_nums: None,
            consume_timeout_minutes: Some(15),
        }
    );
    assert!(patch_matches(&before, &before));
}

#[test]
fn safety_comparison_detects_non_allowlisted_changes() {
    let current = state();
    let before = SubscriptionGroupBeforeState {
        group: "orders-consumer".to_owned(),
        operation_id: "operation-1".to_owned(),
        expected_version: current.version,
        brokers: vec![SubscriptionGroupBeforeBroker {
            broker_addr: "127.0.0.1:10911".to_owned(),
            version: current.version,
            before: SubscriptionGroupPatch {
                retry_max_times: Some(current.retry_max_times),
                ..SubscriptionGroupPatch::default()
            },
            safety: safety_state(current),
        }],
        forward_patch: SubscriptionGroupPatch {
            retry_max_times: Some(8),
            ..SubscriptionGroupPatch::default()
        },
    };
    let live = vec![LiveSubscriptionGroupBroker {
        broker_addr: "127.0.0.1:10911".to_owned(),
        state: current,
    }];
    assert!(safety_matches_before(&live, &before));

    let mut changed = current;
    changed.consume_enable = false;
    let changed_live = vec![LiveSubscriptionGroupBroker {
        broker_addr: "127.0.0.1:10911".to_owned(),
        state: changed,
    }];
    assert!(!safety_matches_before(&changed_live, &before));
}
