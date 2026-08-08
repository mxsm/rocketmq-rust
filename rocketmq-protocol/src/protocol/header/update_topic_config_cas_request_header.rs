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

/// Closed, version-checked Topic configuration patch.
///
/// This header intentionally excludes permission, attributes, message type,
/// routing scope, deletion, cleanup, and raw configuration fields. The Broker
/// preserves those values from the current Topic configuration and rejects an
/// empty patch. A distinct request code keeps the legacy generic Topic upsert
/// behavior unchanged and makes older Brokers reject this operation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::update_topic_config_cas_request_header::UpdateTopicConfigCasRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTopicConfigCasRequestHeader {
    #[header(required)]
    pub topic: CheetahString,
    #[header(required)]
    pub expected_version: u64,
    pub read_queue_nums: Option<i32>,
    pub write_queue_nums: Option<i32>,
    pub order: Option<bool>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::UpdateTopicConfigCasRequestHeader;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn allowlisted_patch_round_trips_through_wire_map() {
        let header = UpdateTopicConfigCasRequestHeader {
            topic: "orders".into(),
            expected_version: 42,
            read_queue_nums: Some(8),
            write_queue_nums: Some(6),
            order: Some(true),
        };
        let map = header.to_map().expect("header should encode");

        assert_eq!(map.get("topic").map(CheetahString::as_str), Some("orders"));
        assert_eq!(map.get("expectedVersion").map(CheetahString::as_str), Some("42"));
        assert_eq!(map.get("readQueueNums").map(CheetahString::as_str), Some("8"));
        assert_eq!(map.get("writeQueueNums").map(CheetahString::as_str), Some("6"));
        assert_eq!(map.get("order").map(CheetahString::as_str), Some("true"));

        let decoded = <UpdateTopicConfigCasRequestHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(decoded, header);
    }

    #[test]
    fn omitted_patch_fields_remain_absent() {
        let header = UpdateTopicConfigCasRequestHeader {
            topic: "orders".into(),
            expected_version: 0,
            ..Default::default()
        };
        let map = header.to_map().expect("header should encode");

        assert!(!map.contains_key("readQueueNums"));
        assert!(!map.contains_key("writeQueueNums"));
        assert!(!map.contains_key("order"));
        assert_eq!(
            <UpdateTopicConfigCasRequestHeader as FromMap>::from(&map).expect("header should decode"),
            header
        );
    }

    #[test]
    fn missing_version_is_rejected_instead_of_becoming_zero() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("orders"),
        );

        assert!(<UpdateTopicConfigCasRequestHeader as FromMap>::from(&map).is_err());
    }
}
