// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::any::TypeId;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader as CanonicalRequest;
use rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader as CanonicalResponse;
use rocketmq_protocol::protocol::RemotingCommand as CanonicalCommand;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader as LegacyRequest;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader as LegacyResponse;
use rocketmq_remoting::RemotingCommand as LegacyCommand;

const JSON_HEADER_GOLDEN: &str = "{\"code\":31,\"language\":\"RUST\",\"version\":123,\"opaque\":456,\"flag\":0,\"\
                                  remark\":null,\"extFields\":{\"topic\":\"orders\"},\"serializeTypeCurrentRPC\":\"\
                                  JSON\"}";
const ROCKETMQ_HEADER_GOLDEN: &[u8] = &[
    0, 31, 12, 0, 123, 0, 0, 1, 200, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 6,
    111, 114, 100, 101, 114, 115,
];

fn header() -> CanonicalRequest {
    CanonicalRequest {
        topic: "orders".into(),
        queue_id: 7,
        topic_request_header: None,
    }
}

#[test]
fn migrated_schema_paths_have_exact_type_identity() {
    assert_eq!(TypeId::of::<CanonicalRequest>(), TypeId::of::<LegacyRequest>());
    assert_eq!(TypeId::of::<CanonicalResponse>(), TypeId::of::<LegacyResponse>());
    assert_eq!(
        TypeId::of::<rocketmq_protocol::RequestCode>(),
        TypeId::of::<rocketmq_remoting::RequestCode>()
    );
}

#[test]
fn canonical_and_owner_facade_match_frozen_json_and_binary_headers() {
    assert_eq!(
        serde_json::to_string(&header()).unwrap(),
        r#"{"topic":"orders","queueId":7}"#
    );
    let ext_fields = HashMap::from([(CheetahString::from("topic"), CheetahString::from("orders"))]);
    let mut canonical_json = CanonicalCommand::with_resolved_defaults(123, SerializeType::JSON)
        .set_code(31)
        .set_opaque(456)
        .set_ext_fields(ext_fields.clone());
    let mut legacy_json = LegacyCommand::create_remoting_command(31)
        .set_version(123)
        .set_opaque(456)
        .set_serialize_type(SerializeType::JSON)
        .set_ext_fields(ext_fields.clone());
    assert_eq!(
        canonical_json.header_encode().unwrap().as_ref(),
        JSON_HEADER_GOLDEN.as_bytes()
    );
    assert_eq!(
        legacy_json.header_encode().unwrap().as_ref(),
        JSON_HEADER_GOLDEN.as_bytes()
    );

    let mut canonical_binary = CanonicalCommand::with_resolved_defaults(123, SerializeType::ROCKETMQ)
        .set_code(31)
        .set_opaque(456)
        .set_ext_fields(ext_fields.clone());
    let mut legacy_binary = LegacyCommand::create_remoting_command(31)
        .set_version(123)
        .set_opaque(456)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(ext_fields);
    assert_eq!(
        canonical_binary.header_encode().unwrap().as_ref(),
        ROCKETMQ_HEADER_GOLDEN
    );
    assert_eq!(legacy_binary.header_encode().unwrap().as_ref(), ROCKETMQ_HEADER_GOLDEN);
}

#[test]
fn selected_body_uses_frozen_json_and_round_trips_through_both_paths() {
    let body = KVTable {
        table: HashMap::from([(CheetahString::from("name"), CheetahString::from("value"))]),
    };
    let encoded = body.encode().unwrap();
    assert_eq!(encoded, br#"{"table":{"name":"value"}}"#);
    let canonical = KVTable::decode(&encoded).unwrap();
    let legacy = rocketmq_remoting::protocol::body::kv_table::KVTable::decode(&encoded).unwrap();
    assert_eq!(canonical.table, legacy.table);
}

#[test]
fn malformed_unknown_missing_and_boundary_inputs_keep_error_behavior() {
    for malformed in [
        br#"{"topic":"orders","queueId":"not-an-integer"}"#.as_slice(),
        br#"{"queueId":7}"#.as_slice(),
        br#"{"topic":"orders","queueId":2147483648}"#.as_slice(),
    ] {
        let canonical = serde_json::from_slice::<CanonicalRequest>(malformed).unwrap_err();
        let legacy = serde_json::from_slice::<LegacyRequest>(malformed).unwrap_err();
        assert_eq!(canonical.classify(), legacy.classify());
    }

    let with_unknown = br#"{"topic":"orders","queueId":7,"futureField":true}"#;
    let canonical: CanonicalRequest = serde_json::from_slice(with_unknown).unwrap();
    let legacy: LegacyRequest = serde_json::from_slice(with_unknown).unwrap();
    assert_eq!(canonical.topic, legacy.topic);
    assert_eq!(canonical.queue_id, legacy.queue_id);
}
