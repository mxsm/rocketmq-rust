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

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerSendMsgBackRequestHeader {
    #[header(required)]
    pub offset: i64,
    #[header(required)]
    pub group: CheetahString, //consumer group
    #[header(required)]
    pub delay_level: i32,
    pub origin_msg_id: Option<CheetahString>,
    pub origin_topic: Option<CheetahString>,
    #[serde(default)]
    #[header(default, default_semantic = "literal:false")]
    pub unit_mode: bool,
    pub max_reconsume_times: Option<i32>,
    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub rpc_request_header: Option<RpcRequestHeader>,
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn consumer_send_msg_back_request_header_serializes_correctly() {
        let header = ConsumerSendMsgBackRequestHeader {
            offset: 12345,
            group: CheetahString::from_static_str("test_group"),
            delay_level: 2,
            origin_msg_id: Some(CheetahString::from_static_str("msg_id")),
            origin_topic: Some(CheetahString::from_static_str("topic")),
            unit_mode: true,
            max_reconsume_times: Some(3),
            rpc_request_header: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(map.get(&CheetahString::from_static_str("offset")).unwrap(), "12345");
        assert_eq!(map.get(&CheetahString::from_static_str("group")).unwrap(), "test_group");
        assert_eq!(map.get(&CheetahString::from_static_str("delayLevel")).unwrap(), "2");
        assert_eq!(
            map.get(&CheetahString::from_static_str("originMsgId")).unwrap(),
            "msg_id"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("originTopic")).unwrap(),
            "topic"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("unitMode")).unwrap(), "true");
        assert_eq!(
            map.get(&CheetahString::from_static_str("maxReconsumeTimes")).unwrap(),
            "3"
        );
    }

    #[test]
    fn consumer_send_msg_back_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("offset"),
            CheetahString::from_static_str("12345"),
        );
        map.insert(
            CheetahString::from_static_str("group"),
            CheetahString::from_static_str("test_group"),
        );
        map.insert(
            CheetahString::from_static_str("delayLevel"),
            CheetahString::from_static_str("2"),
        );
        map.insert(
            CheetahString::from_static_str("originMsgId"),
            CheetahString::from_static_str("msg_id"),
        );
        map.insert(
            CheetahString::from_static_str("originTopic"),
            CheetahString::from_static_str("topic"),
        );
        map.insert(
            CheetahString::from_static_str("unitMode"),
            CheetahString::from_static_str("true"),
        );
        map.insert(
            CheetahString::from_static_str("maxReconsumeTimes"),
            CheetahString::from_static_str("3"),
        );

        let header = <ConsumerSendMsgBackRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.offset, 12345);
        assert_eq!(header.group, "test_group");
        assert_eq!(header.delay_level, 2);
        assert_eq!(header.origin_msg_id.unwrap(), "msg_id");
        assert_eq!(header.origin_topic.unwrap(), "topic");
        assert!(header.unit_mode);
        assert_eq!(header.max_reconsume_times.unwrap(), 3);
    }

    #[test]
    fn consumer_send_msg_back_request_header_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("offset"),
            CheetahString::from_static_str("12345"),
        );
        map.insert(
            CheetahString::from_static_str("group"),
            CheetahString::from_static_str("test_group"),
        );
        map.insert(
            CheetahString::from_static_str("delayLevel"),
            CheetahString::from_static_str("2"),
        );
        map.insert(
            CheetahString::from_static_str("unitMode"),
            CheetahString::from_static_str("true"),
        );

        let header = <ConsumerSendMsgBackRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.offset, 12345);
        assert_eq!(header.group, "test_group");
        assert_eq!(header.delay_level, 2);
        assert!(header.origin_msg_id.is_none());
        assert!(header.origin_topic.is_none());
        assert!(header.unit_mode);
        assert!(header.max_reconsume_times.is_none());
    }

    #[test]
    fn consumer_send_msg_back_request_header_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("offset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("group"),
            CheetahString::from_static_str("test_group"),
        );
        map.insert(
            CheetahString::from_static_str("delayLevel"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("unitMode"),
            CheetahString::from_static_str("true"),
        );

        let result = <ConsumerSendMsgBackRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
