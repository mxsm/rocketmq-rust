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

use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

/// Represents the header of a reply message request.
#[derive(Serialize, Deserialize, Debug, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ReplyMessageRequestHeader {
    /// Producer group associated with the message.
    #[required]
    pub producer_group: CheetahString,

    /// The topic of the message.
    #[required]
    pub topic: CheetahString,

    /// Default topic used when the specified topic is not found.
    #[required]
    pub default_topic: CheetahString,

    /// Number of queues in the default topic.
    #[required]
    pub default_topic_queue_nums: i32,

    /// Queue ID of the message.
    #[required]
    pub queue_id: i32,

    /// System flags associated with the message.
    #[required]
    pub sys_flag: i32,

    /// Timestamp of when the message was born.
    #[required]
    pub born_timestamp: i64,

    /// Flags associated with the message.
    #[required]
    pub flag: i32,

    /// Properties of the message (nullable).
    pub properties: Option<CheetahString>,

    /// Number of times the message has been reconsumed (nullable).
    pub reconsume_times: Option<i32>,

    /// Whether the message processing is in unit mode (nullable).
    pub unit_mode: Option<bool>,

    /// Host where the message was born.
    #[required]
    pub born_host: CheetahString,

    /// Host where the message is stored.
    #[required]
    pub store_host: CheetahString,

    /// Timestamp of when the message was stored.
    #[required]
    pub store_timestamp: i64,

    #[serde(flatten)]
    pub topic_request: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod reply_message_request_header_tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;
    #[test]
    fn deserialize_from_map_with_all_fields_populates_struct_correctly() {
        let mut map: HashMap<CheetahString, CheetahString> = HashMap::new();
        map.insert("producerGroup".into(), "test_producer_group".into());
        map.insert("topic".into(), "test_topic".into());
        map.insert("defaultTopic".into(), "test_default_topic".into());
        map.insert("defaultTopicQueueNums".into(), "10".into());
        map.insert("queueId".into(), "1".into());
        map.insert("sysFlag".into(), "0".into());
        map.insert("flag".into(), "0".into());
        map.insert("bornTimestamp".into(), "1622547800".into());
        map.insert("bornHost".into(), "test_born_host".into());
        map.insert("storeHost".into(), "test_store_host".into());
        map.insert("storeTimestamp".into(), "1622547800".into());
        map.insert("unitMode".into(), "true".into());

        let header: ReplyMessageRequestHeader = <ReplyMessageRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.producer_group, "test_producer_group");
        assert_eq!(header.default_topic, "test_default_topic");
        assert_eq!(header.default_topic_queue_nums, 10);
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.flag, 0);
        assert_eq!(header.born_timestamp, 1622547800);
        assert_eq!(header.born_host, "test_born_host");
        assert_eq!(header.store_host, "test_store_host");
        assert_eq!(header.store_timestamp, 1622547800);
        assert_eq!(header.properties, None);
        assert_eq!(header.reconsume_times, None);
        assert_eq!(header.unit_mode, Some(true));
    }

    #[test]
    fn deserialize_from_map_with_invalid_number_fields_returns_none() {
        let mut map = HashMap::new();
        map.insert("producerGroup".into(), "test_producer_group".into());
        map.insert("topic".into(), "test_topic".into());
        map.insert("defaultTopic".into(), "test_default_topic".into());
        map.insert("defaultTopicQueueNums".into(), "invalid".into());
        let header: Result<ReplyMessageRequestHeader, rocketmq_error::RocketMQError> =
            <ReplyMessageRequestHeader as FromMap>::from(&map);
        assert!(header.is_err());
    }

    #[test]
    fn serialize_header_to_map() {
        let header = ReplyMessageRequestHeader {
            topic: "test_topic".into(),
            producer_group: "test_producer_group".into(),
            default_topic: "test_default_topic".into(),
            default_topic_queue_nums: 10,
            queue_id: 1,
            flag: 2,
            sys_flag: 0,
            born_timestamp: 1622547800,
            born_host: "test_born_host".into(),
            store_host: "test_store_host".into(),
            store_timestamp: 1622547800,
            properties: Some("test_properties".into()),
            reconsume_times: Some(1),
            unit_mode: Some(true),
            topic_request: None,
        };
        let map: HashMap<CheetahString, CheetahString> = header.to_map().unwrap();

        assert_eq!(map.get("topic").unwrap(), "test_topic");
        assert_eq!(map.get("producerGroup").unwrap(), "test_producer_group");
        assert_eq!(map.get("defaultTopicQueueNums").unwrap(), "10");
        assert_eq!(map.get("bornTimestamp").unwrap(), "1622547800");
        assert!(!map.contains_key("topicRequest"));
        assert_eq!(map.get("queueId").unwrap(), "1");
        assert_eq!(map.get("sysFlag").unwrap(), "0");
        assert_eq!(map.get("bornHost").unwrap(), "test_born_host");
        assert_eq!(map.get("storeHost").unwrap(), "test_store_host");
        assert_eq!(map.get("storeTimestamp").unwrap(), "1622547800");
        assert_eq!(map.get("flag").unwrap(), "2");
        assert_eq!(map.get("properties").unwrap(), "test_properties");
        assert_eq!(map.get("reconsumeTimes").unwrap(), "1");
        assert_eq!(map.get("unitMode").unwrap(), "true");
    }

    #[test]
    fn serialize_header_to_map_with_topic_request_header_includes_nested_fields() {
        let topic_request_header = TopicRequestHeader::default();
        let header = ReplyMessageRequestHeader {
            topic: "test_topic".into(),
            producer_group: "test_producer_group".into(),
            default_topic: "test_default_topic".into(),
            default_topic_queue_nums: 10,
            queue_id: 1,
            flag: 2,
            sys_flag: 0,
            born_timestamp: 1622547800,
            born_host: "test_born_host".into(),
            store_host: "test_store_host".into(),
            store_timestamp: 1622547800,
            properties: Some("test_properties".into()),
            reconsume_times: Some(1),
            unit_mode: Some(true),
            topic_request: Some(topic_request_header),
        };
        let map: HashMap<CheetahString, CheetahString> = header.to_map().unwrap();
        assert!(!map.contains_key("nestedField"));
    }
}
