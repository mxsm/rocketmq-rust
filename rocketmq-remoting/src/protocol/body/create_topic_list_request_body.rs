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

use rocketmq_common::common::config::TopicConfig;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CreateTopicListRequestBody {
    pub topic_config_list: Vec<ArcMut<TopicConfig>>,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn default_create_topic_list_request_body() {
        let body = CreateTopicListRequestBody::default();
        assert!(body.topic_config_list.is_empty());
    }

    #[test]
    fn serialize_create_topic_list_request_body() {
        let topic = TopicConfig::new("test_topic");
        let body = CreateTopicListRequestBody {
            topic_config_list: vec![ArcMut::new(topic)],
        };
        let json = serde_json::to_string(&body).unwrap();

        assert!(json.contains("topicConfigList"));
        assert!(json.contains("test_topic"));
    }

    #[test]
    fn deserialize_create_topic_list_request_body() {
        let json = r#"
    {
        "topicConfigList": [
            {
                "topicName": "test_topic",
                "readQueueNums": 2,
                "writeQueueNums": 8,
                "perm": 6,
                "topicFilterType": "SINGLE_TAG",
                "topicSysFlag": 0,
                "order": false,
                "attributes": {}
            }
        ]
    }
    "#;

        let body: CreateTopicListRequestBody = serde_json::from_str(json).unwrap();

        assert_eq!(body.topic_config_list.len(), 1);

        let topic = &body.topic_config_list[0];

        assert_eq!(topic.topic_name.as_deref(), Some("test_topic"));
        assert_eq!(topic.read_queue_nums, 2);
    }
}
