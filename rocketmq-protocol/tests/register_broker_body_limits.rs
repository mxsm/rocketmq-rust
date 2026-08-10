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

use std::io::Write;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use rocketmq_model::version::RocketMqVersion;
use rocketmq_protocol::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
use rocketmq_protocol::protocol::body::broker_body::register_broker_body::RegisterBrokerDecodeLimits;
use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_protocol::protocol::RemotingSerializable;

fn data_version_bytes() -> Vec<u8> {
    DataVersion::default()
        .encode()
        .expect("default DataVersion should encode")
}

fn payload_with_data_version() -> BytesMut {
    let data_version = data_version_bytes();
    let mut payload = BytesMut::new();
    payload.put_i32(i32::try_from(data_version.len()).expect("test DataVersion length should fit i32"));
    payload.extend_from_slice(&data_version);
    payload
}

fn compress(payload: &[u8]) -> Bytes {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(payload).expect("test payload should compress");
    Bytes::from(encoder.finish().expect("test compressor should finish"))
}

fn decode_compressed(
    payload: &[u8],
    limits: RegisterBrokerDecodeLimits,
) -> rocketmq_error::RocketMQResult<RegisterBrokerBody> {
    RegisterBrokerBody::decode_with_limits(&compress(payload), true, RocketMqVersion::V5_0_0, limits)
}

#[test]
fn rejects_wire_body_over_limit() {
    let limits = RegisterBrokerDecodeLimits {
        max_wire_bytes: 3,
        ..RegisterBrokerDecodeLimits::default()
    };

    let result = RegisterBrokerBody::decode_with_limits(
        &Bytes::from_static(br#"{"topicConfigSerializeWrapper":{}}"#),
        false,
        RocketMqVersion::V4_9_4,
        limits,
    );

    assert!(result.is_err());
}

#[test]
fn rejects_negative_data_version_length() {
    let mut payload = BytesMut::new();
    payload.put_i32(-1);

    assert!(decode_compressed(&payload, RegisterBrokerDecodeLimits::default()).is_err());
}

#[test]
fn rejects_negative_topic_count() {
    let mut payload = payload_with_data_version();
    payload.put_i32(-1);

    assert!(decode_compressed(&payload, RegisterBrokerDecodeLimits::default()).is_err());
}

#[test]
fn rejects_truncated_topic_entry() {
    let mut payload = payload_with_data_version();
    payload.put_i32(1);
    payload.put_i32(16);
    payload.extend_from_slice(b"short");

    assert!(decode_compressed(&payload, RegisterBrokerDecodeLimits::default()).is_err());
}

#[test]
fn rejects_topic_entry_that_cannot_be_decoded() {
    let mut payload = payload_with_data_version();
    payload.put_i32(1);
    payload.put_i32(3);
    payload.extend_from_slice(b"bad");

    assert!(decode_compressed(&payload, RegisterBrokerDecodeLimits::default()).is_err());
}

#[test]
fn rejects_invalid_filter_server_json() {
    let mut payload = payload_with_data_version();
    payload.put_i32(0);
    payload.put_i32(5);
    payload.extend_from_slice(b"nope!");

    assert!(decode_compressed(&payload, RegisterBrokerDecodeLimits::default()).is_err());
}

#[test]
fn rejects_negative_mapping_count_before_reserve() {
    let mut payload = payload_with_data_version();
    payload.put_i32(0);
    payload.put_i32(2);
    payload.extend_from_slice(b"[]");
    payload.put_i32(-1);

    assert!(decode_compressed(&payload, RegisterBrokerDecodeLimits::default()).is_err());
}

#[test]
fn rejects_mapping_count_over_limit() {
    let mut payload = payload_with_data_version();
    payload.put_i32(0);
    payload.put_i32(2);
    payload.extend_from_slice(b"[]");
    payload.put_i32(2);
    let limits = RegisterBrokerDecodeLimits {
        max_mapping_count: 1,
        ..RegisterBrokerDecodeLimits::default()
    };

    assert!(decode_compressed(&payload, limits).is_err());
}

#[test]
fn rejects_decompressed_body_over_limit_while_reading() {
    let payload = vec![0_u8; 1_024];
    let limits = RegisterBrokerDecodeLimits {
        max_decompressed_bytes: 32,
        ..RegisterBrokerDecodeLimits::default()
    };

    assert!(decode_compressed(&payload, limits).is_err());
}

#[test]
fn rejects_non_compressed_topic_count_over_limit() {
    let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
    wrapper
        .topic_config_serialize_wrapper
        .topic_config_table
        .insert("TopicA".into(), rocketmq_model::topic::TopicConfig::default());
    let body = RegisterBrokerBody::new(wrapper, Vec::new());
    let encoded = Bytes::from(body.encode(false));
    let limits = RegisterBrokerDecodeLimits {
        max_topic_count: 0,
        ..RegisterBrokerDecodeLimits::default()
    };

    let result = RegisterBrokerBody::decode_with_limits(&encoded, false, RocketMqVersion::V5_0_0, limits);

    assert!(result.is_err());
}

#[test]
fn accepts_valid_compressed_registration() {
    let body = RegisterBrokerBody::new(
        TopicConfigAndMappingSerializeWrapper::default(),
        vec!["127.0.0.1:12000".into()],
    );
    let encoded = Bytes::from(body.encode(true));

    let decoded = RegisterBrokerBody::decode_with_limits(
        &encoded,
        true,
        RocketMqVersion::V5_0_0,
        RegisterBrokerDecodeLimits::default(),
    )
    .expect("valid compressed registration should decode");

    assert_eq!(decoded.filter_server_list, body.filter_server_list);
}

#[test]
fn rejects_trailing_bytes_for_current_broker_format() {
    let mut payload = payload_with_data_version();
    payload.put_i32(0);
    payload.put_i32(2);
    payload.extend_from_slice(b"[]");
    payload.put_i32(0);
    payload.put_u8(1);

    assert!(decode_compressed(&payload, RegisterBrokerDecodeLimits::default()).is_err());
}
