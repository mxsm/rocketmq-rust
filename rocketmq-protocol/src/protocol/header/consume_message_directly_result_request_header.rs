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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader",
    lookup = "scan",
    fast
)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeMessageDirectlyResultRequestHeader {
    #[header(required)]
    pub consumer_group: CheetahString,
    pub client_id: Option<CheetahString>,
    pub msg_id: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
    pub topic: Option<CheetahString>,
    pub topic_sys_flag: Option<i32>,
    pub group_sys_flag: Option<i32>,
    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    #[test]
    fn consume_message_directly_result_request_header_serializes_correctly() {
        let header = ConsumeMessageDirectlyResultRequestHeader {
            consumer_group: CheetahString::from_static_str("test_group"),
            client_id: Some(CheetahString::from_static_str("client_id")),
            msg_id: Some(CheetahString::from_static_str("msg_id")),
            broker_name: Some(CheetahString::from_static_str("broker_name")),
            topic: Some(CheetahString::from_static_str("topic")),
            topic_sys_flag: Some(1),
            group_sys_flag: Some(2),
            topic_request_header: None,
        };
        let serialized = serde_json::to_string(&header).unwrap();
        let expected = r#"{"consumerGroup":"test_group","clientId":"client_id","msgId":"msg_id","brokerName":"broker_name","topic":"topic","topicSysFlag":1,"groupSysFlag":2}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn consume_message_directly_result_request_header_deserializes_correctly() {
        let data = r#"{"consumerGroup":"test_group","clientId":"client_id","msgId":"msg_id","brokerName":"broker_name","topic":"topic","topicSysFlag":1,"groupSysFlag":2}"#;
        let header: ConsumeMessageDirectlyResultRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.consumer_group, CheetahString::from_static_str("test_group"));
        assert_eq!(header.client_id.unwrap(), CheetahString::from_static_str("client_id"));
        assert_eq!(header.msg_id.unwrap(), CheetahString::from_static_str("msg_id"));
        assert_eq!(
            header.broker_name.unwrap(),
            CheetahString::from_static_str("broker_name")
        );
        assert_eq!(header.topic.unwrap(), CheetahString::from_static_str("topic"));
        assert_eq!(header.topic_sys_flag.unwrap(), 1);
        assert_eq!(header.group_sys_flag.unwrap(), 2);
    }

    #[test]
    fn consume_message_directly_result_request_header_handles_missing_optional_fields() {
        let data = r#"{"consumerGroup":"test_group"}"#;
        let header: ConsumeMessageDirectlyResultRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.consumer_group, CheetahString::from_static_str("test_group"));
        assert!(header.client_id.is_none());
        assert!(header.msg_id.is_none());
        assert!(header.broker_name.is_none());
        assert!(header.topic.is_none());
        assert!(header.topic_sys_flag.is_none());
        assert!(header.group_sys_flag.is_none());
    }

    #[test]
    fn consume_message_directly_result_request_header_handles_invalid_data() {
        let data = r#"{"consumerGroup":12345}"#;
        let result: Result<ConsumeMessageDirectlyResultRequestHeader, _> = serde_json::from_str(data);
        assert!(result.is_err());
    }

    #[test]
    fn business_and_rpc_broker_names_do_not_collide() {
        let header = ConsumeMessageDirectlyResultRequestHeader {
            consumer_group: "group-a".into(),
            broker_name: Some("outer".into()),
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some("base".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let map = header.to_map().unwrap();
        assert_eq!(map.get("brokerName").map(CheetahString::as_str), Some("outer"));
        assert_eq!(map.get("bname").map(CheetahString::as_str), Some("base"));

        let decoded = <ConsumeMessageDirectlyResultRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(decoded.broker_name.as_deref(), Some("outer"));
        assert_eq!(
            decoded
                .topic_request_header
                .and_then(|topic| topic.rpc_request_header)
                .and_then(|rpc| rpc.broker_name)
                .as_deref(),
            Some("base")
        );
    }

    #[test]
    fn required_consumer_group_rejects_present_empty() {
        let map = HashMap::from([(CheetahString::from_static_str("consumerGroup"), CheetahString::new())]);

        assert!(<ConsumeMessageDirectlyResultRequestHeader as FromMap>::from(&map).is_err());
    }
}
