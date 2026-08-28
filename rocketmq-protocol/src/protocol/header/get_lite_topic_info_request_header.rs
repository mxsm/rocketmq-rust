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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader"
)]
pub struct GetLiteTopicInfoRequestHeader {
    #[header(default, default_semantic = "literal:")]
    pub parent_topic: CheetahString,

    #[header(default, default_semantic = "literal:")]
    pub lite_topic: CheetahString,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

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
}
