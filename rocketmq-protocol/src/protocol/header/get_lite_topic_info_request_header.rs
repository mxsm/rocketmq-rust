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

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteTopicInfoRequestHeader {
    #[required]
    pub parent_topic: CheetahString,

    #[required]
    pub lite_topic: CheetahString,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_lite_topic_info_request_header_creation() {
        let header = GetLiteTopicInfoRequestHeader {
            parent_topic: CheetahString::from("parent_topic"),
            lite_topic: CheetahString::from("lite_topic"),
        };

        assert_eq!(header.parent_topic, CheetahString::from("parent_topic"));
        assert_eq!(header.lite_topic, CheetahString::from("lite_topic"));
    }

    #[test]
    fn get_lite_topic_info_request_header_serializes_to_map() {
        let header = GetLiteTopicInfoRequestHeader {
            parent_topic: CheetahString::from("test_parent"),
            lite_topic: CheetahString::from("test_lite"),
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("parentTopic")).unwrap(),
            "test_parent"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("liteTopic")).unwrap(),
            "test_lite"
        );
    }

    #[test]
    fn get_lite_topic_info_request_header_deserializes_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("parentTopic"),
            CheetahString::from("deserialized_parent"),
        );
        map.insert(
            CheetahString::from_static_str("liteTopic"),
            CheetahString::from("deserialized_lite"),
        );

        let header = <GetLiteTopicInfoRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.parent_topic, CheetahString::from("deserialized_parent"));
        assert_eq!(header.lite_topic, CheetahString::from("deserialized_lite"));
    }

    #[test]
    fn get_lite_topic_info_request_header_clone() {
        let header = GetLiteTopicInfoRequestHeader {
            parent_topic: CheetahString::from("parent"),
            lite_topic: CheetahString::from("lite"),
        };

        let cloned = header.clone();
        assert_eq!(header.parent_topic, cloned.parent_topic);
        assert_eq!(header.lite_topic, cloned.lite_topic);
    }

    #[test]
    fn get_lite_topic_info_request_header_debug() {
        let header = GetLiteTopicInfoRequestHeader {
            parent_topic: CheetahString::from("parent"),
            lite_topic: CheetahString::from("lite"),
        };

        let debug_str = format!("{:?}", header);
        assert!(debug_str.contains("parent"));
        assert!(debug_str.contains("lite"));
    }
}
