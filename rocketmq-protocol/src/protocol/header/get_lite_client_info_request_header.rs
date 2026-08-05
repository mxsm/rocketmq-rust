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

fn default_max_count() -> i32 {
    1000
}

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteClientInfoRequestHeader {
    pub parent_topic: Option<CheetahString>,
    pub group: Option<CheetahString>,
    pub client_id: Option<CheetahString>,

    #[serde(default = "default_max_count")]
    pub max_count: i32,
}

impl Default for GetLiteClientInfoRequestHeader {
    fn default() -> Self {
        Self {
            parent_topic: None,
            group: None,
            client_id: None,
            max_count: default_max_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_lite_client_info_request_header_defaults_max_count() {
        let header = GetLiteClientInfoRequestHeader::default();
        assert_eq!(header.max_count, 1000);
        assert!(header.parent_topic.is_none());
        assert!(header.group.is_none());
        assert!(header.client_id.is_none());
    }

    #[test]
    fn get_lite_client_info_request_header_serializes_to_map() {
        let header = GetLiteClientInfoRequestHeader {
            parent_topic: Some("parent".into()),
            group: Some("group".into()),
            client_id: Some("client".into()),
            max_count: 32,
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("parentTopic")).unwrap(),
            "parent"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("group")).unwrap(), "group");
        assert_eq!(map.get(&CheetahString::from_static_str("clientId")).unwrap(), "client");
        assert_eq!(map.get(&CheetahString::from_static_str("maxCount")).unwrap(), "32");
    }

    #[test]
    fn get_lite_client_info_request_header_deserializes_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("parentTopic"),
            CheetahString::from_static_str("parent"),
        );
        map.insert(
            CheetahString::from_static_str("group"),
            CheetahString::from_static_str("group"),
        );
        map.insert(
            CheetahString::from_static_str("clientId"),
            CheetahString::from_static_str("client"),
        );
        map.insert(
            CheetahString::from_static_str("maxCount"),
            CheetahString::from_static_str("16"),
        );

        let header = <GetLiteClientInfoRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.parent_topic, Some("parent".into()));
        assert_eq!(header.group, Some("group".into()));
        assert_eq!(header.client_id, Some("client".into()));
        assert_eq!(header.max_count, 16);
    }
}
