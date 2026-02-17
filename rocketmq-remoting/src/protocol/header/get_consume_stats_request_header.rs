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

#[derive(Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
pub struct GetConsumeStatsRequestHeader {
    #[serde(rename = "consumerGroup")]
    pub consumer_group: CheetahString,
    #[serde(rename = "topic")]
    pub topic: CheetahString,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetConsumeStatsRequestHeader {
    pub fn get_consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }
    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = consumer_group;
    }

    pub fn get_topic(&self) -> &CheetahString {
        &self.topic
    }
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getters_and_setters() {
        let mut header = GetConsumeStatsRequestHeader {
            consumer_group: CheetahString::from("testGroup"),
            topic: CheetahString::from("testTopic"),
            topic_request_header: None,
        };

        assert_eq!(header.get_consumer_group(), "testGroup");
        assert_eq!(header.get_topic(), "testTopic");

        header.set_consumer_group(CheetahString::from("newGroup"));
        header.set_topic(CheetahString::from("newTopic"));

        assert_eq!(header.get_consumer_group(), "newGroup");
        assert_eq!(header.get_topic(), "newTopic");
    }
    #[test]
    fn get_consume_stats_request_header_serde() {
        let header = GetConsumeStatsRequestHeader {
            consumer_group: CheetahString::from("testGroup"),
            topic: CheetahString::from("testTopic"),
            topic_request_header: None,
        };

        let json = serde_json::to_string(&header).unwrap();

        let deserialized: GetConsumeStatsRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.get_consumer_group(), "testGroup");
        assert_eq!(deserialized.get_topic(), "testTopic");
    }

    #[test]
    fn get_consume_stats_request_header_deserialize_with_extra_fields() {
        let json = r#"
        {
            "consumerGroup": "testGroup",
            "topic": "testTopic",
            "extraField1": "extraValue1",
            "extraField2": "extraValue2"
        }
        "#;

        let deserialized: GetConsumeStatsRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.get_consumer_group(), "testGroup");
        assert_eq!(deserialized.get_topic(), "testTopic");
    }

    #[test]
    fn get_consume_stats_request_header_with_topic_request_header_some() {
        // Construct a header with a populated TopicRequestHeader to exercise the
        // #[serde(flatten)] behavior when the option is Some(...)
        let topic_header = TopicRequestHeader::default();

        let header = GetConsumeStatsRequestHeader {
            consumer_group: CheetahString::from("testGroup"),
            topic: CheetahString::from("testTopic"),
            topic_request_header: Some(topic_header),
        };

        let json = serde_json::to_string(&header).unwrap();
        println!("Serialized JSON with topic_request_header: {}", json);

        let deserialized: GetConsumeStatsRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.get_consumer_group(), "testGroup");
        assert_eq!(deserialized.get_topic(), "testTopic");
        assert!(deserialized.topic_request_header.is_some());
    }
}
