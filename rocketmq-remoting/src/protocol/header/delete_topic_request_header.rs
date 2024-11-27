/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteTopicRequestHeader {
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl DeleteTopicRequestHeader {
    pub const TOPIC: &'static str = "topic";
}

impl CommandCustomHeader for DeleteTopicRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::TOPIC),
            self.topic.clone(),
        );
        if let Some(value) = self.topic_request_header.as_ref() {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }
}

impl FromMap for DeleteTopicRequestHeader {
    type Target = Self;

    fn from(map: &std::collections::HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(DeleteTopicRequestHeader {
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))
                .cloned()
                .unwrap_or_default(),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn delete_topic_request_header_to_map() {
        let header = DeleteTopicRequestHeader {
            topic: CheetahString::from("test_topic"),
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                DeleteTopicRequestHeader::TOPIC
            ))
            .unwrap(),
            &CheetahString::from("test_topic")
        );
    }

    #[test]
    fn delete_topic_request_header_to_map_with_topic_request_header() {
        let topic_request_header = TopicRequestHeader {
            // Initialize fields as needed
            rpc_request_header: None,
            lo: None,
        };
        let header = DeleteTopicRequestHeader {
            topic: CheetahString::from("test_topic"),
            topic_request_header: Some(topic_request_header),
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                DeleteTopicRequestHeader::TOPIC
            ))
            .unwrap(),
            &CheetahString::from("test_topic")
        );
        // Add assertions for fields from topic_request_header
    }

    #[test]
    fn delete_topic_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(DeleteTopicRequestHeader::TOPIC),
            CheetahString::from("test_topic"),
        );

        let header = <DeleteTopicRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert!(!header.topic_request_header.is_none());
    }

    #[test]
    fn delete_topic_request_header_from_map_with_topic_request_header() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(DeleteTopicRequestHeader::TOPIC),
            CheetahString::from("test_topic"),
        );
        // Add entries for fields from topic_request_header

        let header = <DeleteTopicRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert!(header.topic_request_header.is_some());
        // Add assertions for fields from topic_request_header
    }

    #[test]
    fn delete_topic_request_header_from_map_missing_topic() {
        let map = HashMap::new();

        let header = <DeleteTopicRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, CheetahString::default());
        assert!(!header.topic_request_header.is_none());
    }
}
