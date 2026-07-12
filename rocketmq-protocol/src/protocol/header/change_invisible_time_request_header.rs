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

use std::fmt::Display;

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ChangeInvisibleTimeRequestHeader {
    #[required]
    pub consumer_group: CheetahString,

    #[required]
    pub topic: CheetahString,

    #[required]
    pub queue_id: i32,

    //startOffset popTime invisibleTime queueId
    #[required]
    pub extra_info: CheetahString,

    #[required]
    pub offset: i64,

    #[required]
    pub invisible_time: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub lite_topic: Option<CheetahString>,

    pub suspend: bool,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl Display for ChangeInvisibleTimeRequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChangeInvisibleTimeRequestHeader {{ consumer_group: {}, topic: {}, queue_id: {}, extra_info: {}, offset: \
             {}, invisible_time: {}, lite_topic: {:?}, suspend: {} }}",
            self.consumer_group,
            self.topic,
            self.queue_id,
            self.extra_info,
            self.offset,
            self.invisible_time,
            self.lite_topic,
            self.suspend
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use serde_json;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn change_invisible_time_request_header_display_format() {
        let header = ChangeInvisibleTimeRequestHeader {
            consumer_group: CheetahString::from("group1"),
            topic: CheetahString::from("topic1"),
            queue_id: 1,
            extra_info: CheetahString::from("info"),
            offset: 12345,
            invisible_time: 67890,
            lite_topic: None,
            suspend: false,
            topic_request_header: None,
        };
        assert_eq!(
            format!("{}", header),
            "ChangeInvisibleTimeRequestHeader { consumer_group: group1, topic: topic1, queue_id: 1, extra_info: info, \
             offset: 12345, invisible_time: 67890, lite_topic: None, suspend: false }"
        );
    }

    #[test]
    fn change_invisible_time_request_header_display_format_with_topic_request_header() {
        let header = ChangeInvisibleTimeRequestHeader {
            consumer_group: CheetahString::from("group1"),
            topic: CheetahString::from("topic1"),
            queue_id: 1,
            extra_info: CheetahString::from("info"),
            offset: 12345,
            invisible_time: 67890,
            lite_topic: Some(CheetahString::from("lite_topic")),
            suspend: true,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: None,
                lo: None,
            }),
        };
        assert_eq!(
            format!("{}", header),
            "ChangeInvisibleTimeRequestHeader { consumer_group: group1, topic: topic1, queue_id: 1, extra_info: info, \
             offset: 12345, invisible_time: 67890, lite_topic: Some(\"lite_topic\"), suspend: true }"
        );
    }

    #[test]
    fn change_invisible_time_request_header_serialize() {
        let header = ChangeInvisibleTimeRequestHeader {
            consumer_group: CheetahString::from("group1"),
            topic: CheetahString::from("topic1"),
            queue_id: 1,
            extra_info: CheetahString::from("info"),
            offset: 12345,
            invisible_time: 67890,
            lite_topic: Some(CheetahString::from("lite_topic")),
            suspend: true,
            topic_request_header: None,
        };
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(
            serialized,
            r#"{"consumerGroup":"group1","topic":"topic1","queueId":1,"extraInfo":"info","offset":12345,"invisibleTime":67890,"liteTopic":"lite_topic","suspend":true}"#
        );
    }

    #[test]
    fn change_invisible_time_request_header_deserialize() {
        let json = r#"{"consumerGroup":"group1","topic":"topic1","queueId":1,"extraInfo":"info","offset":12345,"invisibleTime":67890,"liteTopic":"lite_topic","suspend":true}"#;
        let header: ChangeInvisibleTimeRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.consumer_group, CheetahString::from("group1"));
        assert_eq!(header.topic, CheetahString::from("topic1"));
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.extra_info, CheetahString::from("info"));
        assert_eq!(header.offset, 12345);
        assert_eq!(header.invisible_time, 67890);
        assert_eq!(header.lite_topic.as_deref(), Some("lite_topic"));
        assert!(header.suspend);
    }

    #[test]
    fn change_invisible_time_request_header_maps_lite_topic_and_suspend_like_java_header() {
        let header = ChangeInvisibleTimeRequestHeader {
            consumer_group: CheetahString::from("group1"),
            topic: CheetahString::from("topic1"),
            queue_id: 1,
            extra_info: CheetahString::from("info"),
            offset: 12345,
            invisible_time: 67890,
            lite_topic: Some(CheetahString::from("lite_topic")),
            suspend: true,
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(map.get("liteTopic").map(|value| value.as_str()), Some("lite_topic"));
        assert_eq!(map.get("suspend").map(|value| value.as_str()), Some("true"));

        let decoded = <ChangeInvisibleTimeRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(decoded.lite_topic.as_deref(), Some("lite_topic"));
        assert!(decoded.suspend);
    }
}
