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

use crate::code::request_code::RequestCode;
use crate::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::remoting_command::RemotingCommand;
use crate::rpc::rpc_request_header::RpcRequestHeader;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequestHeader {
    #[required]
    pub producer_group: CheetahString,

    #[required]
    pub topic: CheetahString,

    #[required]
    pub default_topic: CheetahString,

    #[required]
    pub default_topic_queue_nums: i32,

    #[required]
    pub queue_id: i32,

    #[required]
    pub sys_flag: i32,

    #[required]
    pub born_timestamp: i64,

    #[required]
    pub flag: i32,

    pub properties: Option<CheetahString>,
    pub reconsume_times: Option<i32>,
    pub unit_mode: Option<bool>,
    pub batch: Option<bool>,
    pub max_reconsume_times: Option<i32>,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl SendMessageRequestHeader {
    #[inline(always)]
    pub fn is_batch(&self) -> bool {
        self.batch.unwrap_or_default()
    }

    #[inline]
    fn topic_request_header_mut(&mut self) -> &mut TopicRequestHeader {
        self.topic_request_header
            .get_or_insert_with(TopicRequestHeader::default)
    }

    #[inline]
    fn rpc_request_header_mut(&mut self) -> &mut RpcRequestHeader {
        self.topic_request_header_mut()
            .rpc_request_header
            .get_or_insert_with(RpcRequestHeader::default)
    }
}

impl TopicRequestHeaderTrait for SendMessageRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header_mut().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        match self.topic_request_header {
            None => None,
            Some(ref value) => value.lo,
        }
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    fn topic(&self) -> &CheetahString {
        &self.topic
    }

    fn broker_name(&self) -> Option<&CheetahString> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .broker_name
            .as_ref()
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.rpc_request_header_mut().broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .namespace
            .as_deref()
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        self.rpc_request_header_mut().namespace = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .namespaced
            .as_ref()
            .cloned()
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        self.rpc_request_header_mut().namespaced = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .oneway
            .as_ref()
            .cloned()
    }

    fn set_oneway(&mut self, oneway: bool) {
        self.rpc_request_header_mut().oneway = Some(oneway);
    }

    fn queue_id(&self) -> i32 {
        self.queue_id
    }

    fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
}

/// Parses the request header from a `RemotingCommand` based on the `RequestCode`.
///
/// This function attempts to decode the command custom header from the provided `RemotingCommand`.
/// If the `RequestCode` is `SendMessageV2` or `SendBatchMessage`, it tries to decode the header
/// as `SendMessageRequestHeaderV2`. If successful, it converts the `V2` header to a `V1` header.
/// Otherwise, it decodes the header directly as `SendMessageRequestHeader`.
///
/// # Arguments
///
/// * `request` - A reference to the `RemotingCommand` containing the command and its headers.
/// * `request_code` - The `RequestCode` that indicates which version of the request header to
///   expect.
///
/// # Returns
///
/// * `Ok(SendMessageRequestHeader)` if the header is successfully parsed and converted (if
///   necessary).
/// * `Err(crate::Error)` if there is an error in decoding the header.
#[inline]
pub fn parse_request_header(
    request: &RemotingCommand,
    request_code: RequestCode,
) -> rocketmq_error::RocketMQResult<SendMessageRequestHeader> {
    let mut request_header_v2 = None;
    if RequestCode::SendMessageV2 == request_code || RequestCode::SendBatchMessage == request_code {
        // Attempt to decode the command custom header as SendMessageRequestHeaderV2
        request_header_v2 = request
            .decode_command_custom_header_fast::<SendMessageRequestHeaderV2>()
            .ok();
    }
    // Match on the result of the V2 header decoding
    match request_header_v2 {
        Some(header) => Ok(SendMessageRequestHeaderV2::create_send_message_request_header_v1(
            &header,
        )),
        None => request.decode_command_custom_header_fast::<SendMessageRequestHeader>(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::code::request_code::RequestCode;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;
    use crate::protocol::remoting_command::RemotingCommand;

    fn minimal_header() -> SendMessageRequestHeader {
        SendMessageRequestHeader {
            producer_group: CheetahString::from_static_str("test_producer_group"),
            topic: CheetahString::from_static_str("test_topic"),
            default_topic: CheetahString::from_static_str("test_default_topic"),
            default_topic_queue_nums: 8,
            queue_id: 1,
            sys_flag: 0,
            born_timestamp: 1622547800000,
            flag: 0,
            properties: None,
            reconsume_times: None,
            unit_mode: None,
            batch: None,
            max_reconsume_times: None,
            topic_request_header: None,
        }
    }

    fn assert_same_send_message_header(left: &SendMessageRequestHeader, right: &SendMessageRequestHeader) {
        assert_eq!(left.producer_group, right.producer_group);
        assert_eq!(left.topic, right.topic);
        assert_eq!(left.default_topic, right.default_topic);
        assert_eq!(left.default_topic_queue_nums, right.default_topic_queue_nums);
        assert_eq!(left.queue_id, right.queue_id);
        assert_eq!(left.sys_flag, right.sys_flag);
        assert_eq!(left.born_timestamp, right.born_timestamp);
        assert_eq!(left.flag, right.flag);
        assert_eq!(left.properties, right.properties);
        assert_eq!(left.reconsume_times, right.reconsume_times);
        assert_eq!(left.unit_mode, right.unit_mode);
        assert_eq!(left.batch, right.batch);
        assert_eq!(left.max_reconsume_times, right.max_reconsume_times);
    }

    #[test]
    fn send_message_request_header_fast_decode_matches_normal_decode() {
        let mut header = minimal_header();
        header.properties = Some(CheetahString::from_static_str("KEYS=abc"));
        header.reconsume_times = Some(2);
        header.unit_mode = Some(true);
        header.batch = Some(false);
        header.max_reconsume_times = Some(16);
        let request =
            RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_ext_fields(header.to_map().unwrap());

        let normal = request
            .decode_command_custom_header::<SendMessageRequestHeader>()
            .expect("normal decode should succeed");
        let fast = request
            .decode_command_custom_header_fast::<SendMessageRequestHeader>()
            .expect("fast decode should succeed");
        let parsed = parse_request_header(&request, RequestCode::SendMessage).expect("parse should use fast decode");

        assert_same_send_message_header(&normal, &fast);
        assert_same_send_message_header(&normal, &parsed);
    }

    #[test]
    fn send_message_v2_fast_decode_matches_normal_v1_conversion() {
        let mut header = minimal_header();
        header.properties = Some(CheetahString::from_static_str("TAGS=TagA"));
        header.reconsume_times = Some(1);
        header.unit_mode = Some(false);
        header.batch = Some(true);
        header.max_reconsume_times = Some(8);
        header.set_broker_name(CheetahString::from_static_str("broker-a"));
        let v2 = SendMessageRequestHeaderV2::create_send_message_request_header_v2(&header);
        let request =
            RemotingCommand::create_remoting_command(RequestCode::SendMessageV2).set_ext_fields(v2.to_map().unwrap());

        let normal_v2 = request
            .decode_command_custom_header::<SendMessageRequestHeaderV2>()
            .expect("normal V2 decode should succeed");
        let fast_v2 = request
            .decode_command_custom_header_fast::<SendMessageRequestHeaderV2>()
            .expect("fast V2 decode should succeed");
        let normal_v1 = SendMessageRequestHeaderV2::create_send_message_request_header_v1(&normal_v2);
        let fast_v1 = SendMessageRequestHeaderV2::create_send_message_request_header_v1(&fast_v2);
        let parsed =
            parse_request_header(&request, RequestCode::SendMessageV2).expect("parse should use V2 fast decode");

        assert_same_send_message_header(&normal_v1, &fast_v1);
        assert_same_send_message_header(&normal_v1, &parsed);
        assert_eq!(parsed.broker_name().map(CheetahString::as_str), Some("broker-a"));
    }

    #[test]
    fn parse_request_header_handles_invalid_request_code() {
        let request = RemotingCommand::create_remoting_command(RequestCode::SendBatchMessage);
        let request_code = RequestCode::SendBatchMessage;
        let result = parse_request_header(&request, request_code);
        assert!(result.is_err());
    }

    #[test]
    fn parse_request_header_handles_missing_header() {
        let request = RemotingCommand::create_remoting_command(RequestCode::SendMessageV2);
        let request_code = RequestCode::SendMessageV2;
        let result = parse_request_header(&request, request_code);
        assert!(result.is_err());
    }

    #[test]
    fn send_message_request_header_serializes_correctly() {
        let header = SendMessageRequestHeader {
            producer_group: CheetahString::from_static_str("test_producer_group"),
            topic: CheetahString::from_static_str("test_topic"),
            default_topic: CheetahString::from_static_str("test_default_topic"),
            default_topic_queue_nums: 8,
            queue_id: 1,
            sys_flag: 0,
            born_timestamp: 1622547800000,
            flag: 0,
            properties: Some(CheetahString::from_static_str("test_properties")),
            reconsume_times: Some(3),
            unit_mode: Some(true),
            batch: Some(false),
            max_reconsume_times: Some(5),
            topic_request_header: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("producerGroup")).unwrap(),
            "test_producer_group"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("topic")).unwrap(), "test_topic");
        assert_eq!(
            map.get(&CheetahString::from_static_str("defaultTopic")).unwrap(),
            "test_default_topic"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("defaultTopicQueueNums"))
                .unwrap(),
            "8"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("queueId")).unwrap(), "1");
        assert_eq!(map.get(&CheetahString::from_static_str("sysFlag")).unwrap(), "0");
        assert_eq!(
            map.get(&CheetahString::from_static_str("bornTimestamp")).unwrap(),
            "1622547800000"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("flag")).unwrap(), "0");
        assert_eq!(
            map.get(&CheetahString::from_static_str("properties")).unwrap(),
            "test_properties"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("reconsumeTimes")).unwrap(), "3");
        assert_eq!(map.get(&CheetahString::from_static_str("unitMode")).unwrap(), "true");
        assert_eq!(map.get(&CheetahString::from_static_str("batch")).unwrap(), "false");
        assert_eq!(
            map.get(&CheetahString::from_static_str("maxReconsumeTimes")).unwrap(),
            "5"
        );
    }

    #[test]
    fn topic_request_header_setters_initialize_missing_nested_headers() {
        let mut header = minimal_header();

        header.set_lo(Some(true));
        header.set_broker_name(CheetahString::from_static_str("broker-a"));
        header.set_namespace(CheetahString::from_static_str("ns-a"));
        header.set_namespaced(true);
        header.set_oneway(true);

        assert_eq!(header.lo(), Some(true));
        assert_eq!(header.broker_name().map(CheetahString::as_str), Some("broker-a"));
        assert_eq!(header.namespace(), Some("ns-a"));
        assert_eq!(header.namespaced(), Some(true));
        assert_eq!(header.oneway(), Some(true));

        let map = header.to_map().expect("header should encode");
        assert_eq!(map.get("lo").map(CheetahString::as_str), Some("true"));
        assert_eq!(map.get("brokerName").map(CheetahString::as_str), Some("broker-a"));
        assert_eq!(map.get("namespace").map(CheetahString::as_str), Some("ns-a"));
        assert_eq!(map.get("namespaced").map(CheetahString::as_str), Some("true"));
        assert_eq!(map.get("oneway").map(CheetahString::as_str), Some("true"));
    }

    #[test]
    fn send_message_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("producerGroup"),
            CheetahString::from_static_str("test_producer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("defaultTopic"),
            CheetahString::from_static_str("test_default_topic"),
        );
        map.insert(
            CheetahString::from_static_str("defaultTopicQueueNums"),
            CheetahString::from_static_str("8"),
        );
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("bornTimestamp"),
            CheetahString::from_static_str("1622547800000"),
        );
        map.insert(
            CheetahString::from_static_str("flag"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("properties"),
            CheetahString::from_static_str("test_properties"),
        );
        map.insert(
            CheetahString::from_static_str("reconsumeTimes"),
            CheetahString::from_static_str("3"),
        );
        map.insert(
            CheetahString::from_static_str("unitMode"),
            CheetahString::from_static_str("true"),
        );
        map.insert(
            CheetahString::from_static_str("batch"),
            CheetahString::from_static_str("false"),
        );
        map.insert(
            CheetahString::from_static_str("maxReconsumeTimes"),
            CheetahString::from_static_str("5"),
        );

        let header = <SendMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.producer_group, "test_producer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.default_topic, "test_default_topic");
        assert_eq!(header.default_topic_queue_nums, 8);
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.born_timestamp, 1622547800000);
        assert_eq!(header.flag, 0);
        assert_eq!(header.properties.unwrap(), "test_properties");
        assert_eq!(header.reconsume_times.unwrap(), 3);
        assert!(header.unit_mode.unwrap());
        assert!(!header.batch.unwrap());
        assert_eq!(header.max_reconsume_times.unwrap(), 5);
    }

    #[test]
    fn send_message_request_header_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("producerGroup"),
            CheetahString::from_static_str("test_producer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("defaultTopic"),
            CheetahString::from_static_str("test_default_topic"),
        );
        map.insert(
            CheetahString::from_static_str("defaultTopicQueueNums"),
            CheetahString::from_static_str("8"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("bornTimestamp"),
            CheetahString::from_static_str("1622547800000"),
        );
        map.insert(
            CheetahString::from_static_str("flag"),
            CheetahString::from_static_str("0"),
        );

        let header = <SendMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.producer_group, "test_producer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.default_topic, "test_default_topic");
        assert_eq!(header.default_topic_queue_nums, 8);
        //assert!(header.queue_id.is_some());
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.born_timestamp, 1622547800000);
        assert_eq!(header.flag, 0);
        assert!(header.properties.is_none());
        assert!(header.reconsume_times.is_none());
        assert!(header.unit_mode.is_none());
        assert!(header.batch.is_none());
        assert!(header.max_reconsume_times.is_none());
    }

    #[test]
    fn send_message_request_header_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("producerGroup"),
            CheetahString::from_static_str("test_producer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("defaultTopic"),
            CheetahString::from_static_str("test_default_topic"),
        );
        map.insert(
            CheetahString::from_static_str("defaultTopicQueueNums"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("bornTimestamp"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("flag"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <SendMessageRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
