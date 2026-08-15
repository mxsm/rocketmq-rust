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

use cheetah_string::CheetahString;
use rocketmq_admin_core::client_adapter::services::consumer::{
    UpdateSubscriptionGroupListRequest, UpdateSubscriptionGroupRequest,
};
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

fn config(group: &str) -> SubscriptionGroupConfig {
    SubscriptionGroupConfig::new(CheetahString::from(group))
}

#[test]
fn single_request_rejects_invalid_group_before_network_access() {
    for group in ["", "invalid.group"] {
        assert!(
            UpdateSubscriptionGroupRequest::try_new(Some("127.0.0.1:10911".to_owned()), None, config(group),).is_err()
        );
    }
    assert!(UpdateSubscriptionGroupRequest::try_new(
        Some("127.0.0.1:10911".to_owned()),
        None,
        config(&"a".repeat(256)),
    )
    .is_err());
}

#[test]
fn list_request_rejects_invalid_or_duplicate_groups_atomically() {
    assert!(UpdateSubscriptionGroupListRequest::try_new(
        Some("127.0.0.1:10911".to_owned()),
        None,
        vec![config("valid-group"), config("invalid.group")],
    )
    .is_err());
    assert!(UpdateSubscriptionGroupListRequest::try_new(
        Some("127.0.0.1:10911".to_owned()),
        None,
        vec![config("same-group"), config("same-group")],
    )
    .is_err());
}
