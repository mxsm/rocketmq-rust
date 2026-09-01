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

use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Default, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader",
    fast
)]
#[serde(rename_all = "camelCase")]
pub struct NotificationRequestHeader {
    #[header(required)]
    pub consumer_group: CheetahString,

    #[header(required)]
    pub topic: CheetahString,

    #[header(required)]
    pub queue_id: i32,

    #[header(required)]
    pub poll_time: i64,

    #[header(required)]
    pub born_time: i64,

    /// Indicates if the message is ordered; defaults to false.
    #[serde(default)]
    #[header(default, default_semantic = "literal:false")]
    pub order: bool,

    /// Attempt ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<CheetahString>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp_type: Option<CheetahString>,

    #[serde(rename = "exp", skip_serializing_if = "Option::is_none")]
    pub exp: Option<CheetahString>,

    #[serde(default)]
    #[header(default, default_semantic = "literal:false")]
    pub is_lite_consumer: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<CheetahString>,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;
    use crate::protocol::header_codec::HeaderCodec;
    use crate::HeaderMap;

    #[test]
    fn test_notification_request_header_serialization() {
        let header = NotificationRequestHeader {
            consumer_group: CheetahString::from("consumer_group_1"),
            topic: CheetahString::from("test_topic"),
            queue_id: 10,
            poll_time: 1234567890,
            born_time: 1234567891,
            order: true,
            attempt_id: Some(CheetahString::from("attempt_1")),
            exp_type: Some(CheetahString::from("TAG")),
            exp: Some(CheetahString::from("tag-a")),
            is_lite_consumer: false,
            client_id: Some(CheetahString::from("client-a")),
            topic_request_header: None,
        };

        let serialized = serde_json::to_string(&header).expect("Failed to serialize header");
        assert!(serialized.contains("\"expType\":\"TAG\""));
        assert!(serialized.contains("\"exp\":\"tag-a\""));

        let deserialized: NotificationRequestHeader =
            serde_json::from_str(&serialized).expect("Failed to deserialize header");
        assert_eq!(header.queue_id, deserialized.queue_id);
        assert_eq!(deserialized.exp_type.as_deref(), Some("TAG"));
        assert_eq!(deserialized.exp.as_deref(), Some("tag-a"));
    }

    #[test]
    fn notification_v3_defaults_and_required_fields_match_the_wire_contract() {
        let mut fields = HeaderMap::from([
            ("consumerGroup".into(), "group-a".into()),
            ("topic".into(), "topic-a".into()),
            ("queueId".into(), "3".into()),
            ("pollTime".into(), "15000".into()),
            ("bornTime".into(), "1720000000000".into()),
        ]);

        let decoded = <NotificationRequestHeader as HeaderCodec>::decode_from_map(&fields).unwrap();
        assert!(!decoded.order);
        assert!(!decoded.is_lite_consumer);
        assert_eq!(decoded.topic_request_header.as_ref().and_then(|header| header.lo), None);

        let encoded = <NotificationRequestHeader as crate::CommandCustomHeader>::to_map(&decoded).unwrap();
        assert_eq!(encoded.get("order").map(CheetahString::as_str), Some("false"));
        assert_eq!(encoded.get("isLiteConsumer").map(CheetahString::as_str), Some("false"));

        fields.remove("consumerGroup");
        assert!(<NotificationRequestHeader as HeaderCodec>::decode_from_map(&fields).is_err());
    }
}
