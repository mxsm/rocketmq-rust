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

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader"
)]
pub struct GetConsumeStatsRequestHeader {
    #[serde(rename = "consumerGroup")]
    #[header(required)]
    pub consumer_group: CheetahString,
    #[serde(default)]
    #[serde(rename = "topic")]
    #[header(default, default_semantic = "literal:")]
    pub topic: CheetahString,
    #[serde(rename = "topicList")]
    pub topic_list: Option<CheetahString>,
    #[serde(flatten)]
    #[header(flatten, presence = "always")]
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

    pub fn fetch_topic_list(&self) -> Vec<CheetahString> {
        self.topic_list
            .as_deref()
            .map(|topics| {
                topics
                    .split(';')
                    .filter(|topic| !topic.is_empty())
                    .map(CheetahString::from)
                    .collect()
            })
            .unwrap_or_default()
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
            topic_list: None,
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
            topic_list: None,
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
        let topic_header = TopicRequestHeader::default();

        let header = GetConsumeStatsRequestHeader {
            consumer_group: CheetahString::from("testGroup"),
            topic: CheetahString::from("testTopic"),
            topic_list: None,
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
