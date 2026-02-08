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

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2)]
pub struct GetTopicStatsRequestHeader {
    #[required]
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_topic_stats_request_header_serialization() {
        let header = GetTopicStatsRequestHeader {
            topic: CheetahString::from("topic1"),
            topic_request_header: None,
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"topic\":\"topic1\""));
    }

    #[test]
    fn get_topic_stats_request_header_deserialization() {
        let json = r#"{"topic":"topic1"}"#;
        let header: GetTopicStatsRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.topic, "topic1");
    }

    #[test]
    fn get_topic_stats_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("topic"), CheetahString::from("topic1"));
        let header = <GetTopicStatsRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, "topic1");
    }
}
