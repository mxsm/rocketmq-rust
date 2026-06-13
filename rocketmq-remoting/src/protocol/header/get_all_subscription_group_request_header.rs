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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetAllSubscriptionGroupRequestHeader {
    #[required]
    pub group_seq: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_version: Option<CheetahString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_group_num: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetAllSubscriptionGroupResponseHeader {
    #[required]
    pub total_group_num: i32,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_all_subscription_group_request_header_maps_java_fields() {
        let header = GetAllSubscriptionGroupRequestHeader {
            group_seq: 7,
            data_version: Some(CheetahString::from_static_str("dv-1")),
            max_group_num: Some(32),
        };

        let map = header.to_map().expect("header should encode");
        assert_eq!(map.get("groupSeq").map(|value| value.as_str()), Some("7"));
        assert_eq!(map.get("dataVersion").map(|value| value.as_str()), Some("dv-1"));
        assert_eq!(map.get("maxGroupNum").map(|value| value.as_str()), Some("32"));

        let decoded = <GetAllSubscriptionGroupRequestHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(decoded.group_seq, 7);
        assert_eq!(decoded.data_version.as_deref(), Some("dv-1"));
        assert_eq!(decoded.max_group_num, Some(32));
    }

    #[test]
    fn get_all_subscription_group_request_header_requires_group_seq_like_java_header() {
        let result = <GetAllSubscriptionGroupRequestHeader as FromMap>::from(&HashMap::new());

        assert!(result.is_err());
    }

    #[test]
    fn get_all_subscription_group_response_header_maps_total_group_num() {
        let header = GetAllSubscriptionGroupResponseHeader { total_group_num: 17 };

        let map = header.to_map().expect("header should encode");
        assert_eq!(map.get("totalGroupNum").map(|value| value.as_str()), Some("17"));

        let decoded = <GetAllSubscriptionGroupResponseHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(decoded.total_group_num, 17);
    }
}
