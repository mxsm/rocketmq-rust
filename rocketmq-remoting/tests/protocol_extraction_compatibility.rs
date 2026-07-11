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

use std::any::TypeId;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader as CanonicalRequest;
use rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader as CanonicalResponse;
use rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_protocol::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader as LegacyRequest;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader as LegacyResponse;

#[test]
fn canonical_and_legacy_paths_are_the_same_types() {
    assert_eq!(TypeId::of::<CanonicalRequest>(), TypeId::of::<LegacyRequest>());
    assert_eq!(TypeId::of::<CanonicalResponse>(), TypeId::of::<LegacyResponse>());
    assert_eq!(
        TypeId::of::<rocketmq_protocol::RequestCode>(),
        TypeId::of::<rocketmq_remoting::RequestCode>(),
    );
    assert_eq!(
        TypeId::of::<rocketmq_protocol::RemotingCommand>(),
        TypeId::of::<rocketmq_remoting::RemotingCommand>(),
    );
}

#[test]
fn get_min_offset_json_and_ext_fields_match_frozen_wire_golden() {
    let header = CanonicalRequest {
        topic: CheetahString::from_static_str("orders"),
        queue_id: 7,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from_static_str("tenant-a")),
                namespaced: Some(true),
                broker_name: Some(CheetahString::from_static_str("broker-a")),
                oneway: Some(false),
            }),
            lo: Some(true),
        }),
    };

    let canonical_json = serde_json::to_vec(&header).unwrap();
    let legacy_json = serde_json::to_vec(&LegacyRequest::clone(&header)).unwrap();
    assert_eq!(canonical_json, legacy_json);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&canonical_json).unwrap(),
        serde_json::json!({
            "topic": "orders",
            "queueId": 7,
            "namespace": "tenant-a",
            "namespaced": true,
            "brokerName": "broker-a",
            "oneway": false,
            "lo": true
        }),
    );

    let fields = header.to_map().unwrap();
    assert_eq!(fields.get("topic").map(CheetahString::as_str), Some("orders"));
    assert_eq!(fields.get("queueId").map(CheetahString::as_str), Some("7"));
    assert_eq!(fields.get("lo").map(CheetahString::as_str), Some("true"));
}

#[test]
fn malformed_header_has_the_same_error_class_through_both_paths() {
    let malformed = br#"{"topic":"orders","queueId":"not-an-integer"}"#;
    let canonical = serde_json::from_slice::<CanonicalRequest>(malformed).unwrap_err();
    let legacy = serde_json::from_slice::<LegacyRequest>(malformed).unwrap_err();
    assert_eq!(canonical.classify(), legacy.classify());
}
