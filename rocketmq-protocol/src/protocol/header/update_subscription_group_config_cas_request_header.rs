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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

/// Closed, version-checked Subscription Group configuration patch.
///
/// Only retry limits and consume timeout are accepted. The Broker preserves
/// permissions, subscription policy, attributes, and every other field from
/// the current group configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::update_subscription_group_config_cas_request_header::UpdateSubscriptionGroupConfigCasRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSubscriptionGroupConfigCasRequestHeader {
    #[header(required)]
    pub group: CheetahString,
    #[header(required)]
    pub expected_version: u64,
    pub retry_max_times: Option<i32>,
    pub retry_queue_nums: Option<i32>,
    pub consume_timeout_minutes: Option<i32>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::UpdateSubscriptionGroupConfigCasRequestHeader;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn allowlisted_patch_round_trips_through_wire_map() {
        let header = UpdateSubscriptionGroupConfigCasRequestHeader {
            group: "orders-consumer".into(),
            expected_version: 42,
            retry_max_times: Some(8),
            retry_queue_nums: Some(4),
            consume_timeout_minutes: Some(30),
        };
        let map = header.to_map().expect("header should encode");

        assert_eq!(map.get("group").map(CheetahString::as_str), Some("orders-consumer"));
        assert_eq!(map.get("expectedVersion").map(CheetahString::as_str), Some("42"));
        assert_eq!(map.get("retryMaxTimes").map(CheetahString::as_str), Some("8"));
        assert_eq!(map.get("retryQueueNums").map(CheetahString::as_str), Some("4"));
        assert_eq!(map.get("consumeTimeoutMinutes").map(CheetahString::as_str), Some("30"));

        let decoded =
            <UpdateSubscriptionGroupConfigCasRequestHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(decoded, header);
    }

    #[test]
    fn omitted_patch_fields_remain_absent() {
        let header = UpdateSubscriptionGroupConfigCasRequestHeader {
            group: "orders-consumer".into(),
            expected_version: 0,
            ..Default::default()
        };
        let map = header.to_map().expect("header should encode");

        assert!(!map.contains_key("retryMaxTimes"));
        assert!(!map.contains_key("retryQueueNums"));
        assert!(!map.contains_key("consumeTimeoutMinutes"));
        assert_eq!(
            <UpdateSubscriptionGroupConfigCasRequestHeader as FromMap>::from(&map).expect("header should decode"),
            header
        );
    }

    #[test]
    fn missing_version_is_rejected_instead_of_becoming_zero() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("group"),
            CheetahString::from_static_str("orders-consumer"),
        );

        assert!(<UpdateSubscriptionGroupConfigCasRequestHeader as FromMap>::from(&map).is_err());
    }
}
