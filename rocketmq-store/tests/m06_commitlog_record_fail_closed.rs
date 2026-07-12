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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_single::tags_string2tags_code;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::CRC32Utils::crc32;
use rocketmq_common::MessageDecoder::create_crc32;
use rocketmq_store::base::dispatch_request::DispatchRequest;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log::check_message_and_return_size;
use rocketmq_store::log_file::commit_log::CRC32_RESERVED_LEN;
use rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE;
use rocketmq_store_local::commit_log::record::MESSAGE_MAGIC_CODE;

const MESSAGE_MAGIC_CODE_V2: i32 = -626843477;
const BODY_LEN_OFFSET: usize = 84;

#[derive(Clone, Copy)]
enum Version {
    V1,
    V2,
}

#[derive(Debug, Eq, PartialEq)]
struct DispatchRequestProjection {
    topic: CheetahString,
    queue_id: i32,
    commit_log_offset: i64,
    msg_size: i32,
    tags_code: i64,
    store_timestamp: i64,
    consume_queue_offset: i64,
    keys: CheetahString,
    success: bool,
    uniq_key: Option<CheetahString>,
    sys_flag: i32,
    prepared_transaction_offset: i64,
    properties_map: Option<BTreeMap<String, String>>,
    bit_map: Option<Vec<u8>>,
    buffer_size: i32,
    msg_base_offset: i64,
    batch_size: i16,
    next_reput_from_offset: i64,
    offset_id: Option<CheetahString>,
}

impl From<&DispatchRequest> for DispatchRequestProjection {
    fn from(request: &DispatchRequest) -> Self {
        let properties_map = request.properties_map.as_ref().map(|properties| {
            properties
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect()
        });
        Self {
            topic: request.topic.clone(),
            queue_id: request.queue_id,
            commit_log_offset: request.commit_log_offset,
            msg_size: request.msg_size,
            tags_code: request.tags_code,
            store_timestamp: request.store_timestamp,
            consume_queue_offset: request.consume_queue_offset,
            keys: request.keys.clone(),
            success: request.success,
            uniq_key: request.uniq_key.clone(),
            sys_flag: request.sys_flag,
            prepared_transaction_offset: request.prepared_transaction_offset,
            properties_map,
            bit_map: request.bit_map.clone(),
            buffer_size: request.buffer_size,
            msg_base_offset: request.msg_base_offset,
            batch_size: request.batch_size,
            next_reput_from_offset: request.next_reput_from_offset,
            offset_id: request.offset_id.clone(),
        }
    }
}

fn frame(version: Version, sys_flag: i32, body: &[u8], topic: &[u8], properties: &[u8]) -> Bytes {
    let born_host_len = if sys_flag & (1 << 4) == 0 { 8 } else { 20 };
    let store_host_len = if sys_flag & (1 << 5) == 0 { 8 } else { 20 };
    let topic_len_width = match version {
        Version::V1 => 1,
        Version::V2 => 2,
    };
    let total_size = 4
        + 4
        + 4
        + 4
        + 4
        + 8
        + 8
        + 4
        + 8
        + born_host_len
        + 8
        + store_host_len
        + 4
        + 8
        + 4
        + body.len()
        + topic_len_width
        + topic.len()
        + 2
        + properties.len();
    let mut encoded = BytesMut::with_capacity(total_size);
    encoded.put_i32(total_size as i32);
    encoded.put_i32(match version {
        Version::V1 => MESSAGE_MAGIC_CODE,
        Version::V2 => MESSAGE_MAGIC_CODE_V2,
    });
    encoded.put_u32(crc32(body));
    encoded.put_i32(3);
    encoded.put_i32(7);
    encoded.put_i64(11);
    encoded.put_i64(13);
    encoded.put_i32(sys_flag);
    encoded.put_i64(17);
    encoded.put_bytes(0x11, born_host_len);
    encoded.put_i64(19);
    encoded.put_bytes(0x22, store_host_len);
    encoded.put_i32(23);
    encoded.put_i64(29);
    encoded.put_i32(body.len() as i32);
    encoded.extend_from_slice(body);
    match version {
        Version::V1 => encoded.put_u8(topic.len() as u8),
        Version::V2 => encoded.put_i16(topic.len() as i16),
    }
    encoded.extend_from_slice(topic);
    encoded.put_i16(properties.len() as i16);
    encoded.extend_from_slice(properties);
    encoded.freeze()
}

fn dispatch(
    bytes: &mut Bytes,
    check_crc: bool,
    read_body: bool,
) -> rocketmq_store::base::dispatch_request::DispatchRequest {
    dispatch_with(
        bytes,
        check_crc,
        false,
        read_body,
        Arc::new(MessageStoreConfig::default()),
    )
}

fn dispatch_with(
    bytes: &mut Bytes,
    check_crc: bool,
    check_dup_info: bool,
    read_body: bool,
    config: Arc<MessageStoreConfig>,
) -> rocketmq_store::base::dispatch_request::DispatchRequest {
    dispatch_with_policy(bytes, check_crc, check_dup_info, read_body, config, 0, &BTreeMap::new())
}

fn dispatch_with_policy(
    bytes: &mut Bytes,
    check_crc: bool,
    check_dup_info: bool,
    read_body: bool,
    config: Arc<MessageStoreConfig>,
    max_delay_level: i32,
    delay_level_table: &BTreeMap<i32, i64>,
) -> DispatchRequest {
    check_message_and_return_size(
        bytes,
        check_crc,
        check_dup_info,
        read_body,
        &config,
        max_delay_level,
        delay_level_table,
    )
}

fn property(name: &str, value: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(name.len() + value.len() + 2);
    bytes.extend_from_slice(name.as_bytes());
    bytes.push(1);
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(2);
    bytes
}

fn properties(entries: &[(&str, &str)]) -> Vec<u8> {
    entries.iter().flat_map(|(name, value)| property(name, value)).collect()
}

fn property_map(entries: &[(&str, &str)]) -> HashMap<CheetahString, CheetahString> {
    entries
        .iter()
        .map(|(name, value)| (CheetahString::from(*name), CheetahString::from(*value)))
        .collect()
}

fn property_crc_frame(body: &[u8]) -> Bytes {
    let reserved = CRC32_RESERVED_LEN as usize;
    let mut encoded = frame(Version::V1, 0, body, b"topic", &vec![0; reserved]).to_vec();
    let check_size = encoded.len() - reserved;
    let property_crc = crc32(&encoded[..check_size]);
    create_crc32(&mut encoded[check_size..], property_crc);
    Bytes::from(encoded)
}

fn assert_invalid_without_advance(mut input: Bytes) {
    let original = input.clone();
    let request = dispatch(&mut input, false, true);
    assert!(!request.success);
    assert_eq!(request.msg_size, -1);
    assert_eq!(input, original);
}

#[test]
fn every_truncated_prefix_is_fail_closed_and_transactional() {
    let valid = frame(Version::V1, 0, b"body", b"topic", b"");
    for len in 0..valid.len() {
        assert_invalid_without_advance(valid.slice(..len));
    }
}

#[test]
fn negative_signed_lengths_are_rejected_without_advance() {
    let mut negative_body = frame(Version::V1, 0, b"", b"topic", b"").to_vec();
    negative_body[BODY_LEN_OFFSET..BODY_LEN_OFFSET + 4].copy_from_slice(&(-1_i32).to_be_bytes());
    assert_invalid_without_advance(Bytes::from(negative_body));

    let mut negative_properties = frame(Version::V1, 0, b"", b"topic", b"").to_vec();
    let properties_len_offset = negative_properties.len() - 2;
    negative_properties[properties_len_offset..].copy_from_slice(&(-1_i16).to_be_bytes());
    assert_invalid_without_advance(Bytes::from(negative_properties));

    let mut negative_v2_topic = frame(Version::V2, 0, b"", b"", b"").to_vec();
    negative_v2_topic[BODY_LEN_OFFSET + 4..BODY_LEN_OFFSET + 6].copy_from_slice(&(-1_i16).to_be_bytes());
    assert_invalid_without_advance(Bytes::from(negative_v2_topic));
}

#[test]
fn declared_frame_beyond_available_bytes_is_fail_closed() {
    let mut invalid = frame(Version::V1, 0, b"body", b"topic", b"").to_vec();
    let declared = invalid.len() as i32 + 4;
    invalid[..4].copy_from_slice(&declared.to_be_bytes());
    assert_invalid_without_advance(Bytes::from(invalid));
}

#[test]
fn declared_boundary_prevents_reading_into_the_next_record() {
    let valid_next = frame(Version::V1, 0, b"next", b"topic", b"");
    let mut partial = frame(Version::V1, 0, b"body", b"topic", b"").to_vec();
    partial.truncate(BODY_LEN_OFFSET + 4 + 2);
    let declared = partial.len() as i32;
    partial[..4].copy_from_slice(&declared.to_be_bytes());
    partial.extend_from_slice(&valid_next);
    assert_invalid_without_advance(Bytes::from(partial));
}

#[test]
fn invalid_magic_and_body_crc_do_not_advance() {
    let mut illegal = frame(Version::V1, 0, b"body", b"topic", b"").to_vec();
    illegal[4..8].copy_from_slice(&123_i32.to_be_bytes());
    assert_invalid_without_advance(Bytes::from(illegal));

    let mut corrupt = frame(Version::V1, 0, b"body", b"topic", b"");
    let original = corrupt.clone();
    let body_offset = BODY_LEN_OFFSET + 4;
    let mut corrupt_vec = corrupt.to_vec();
    corrupt_vec[body_offset] ^= 1;
    corrupt = Bytes::from(corrupt_vec);
    let corrupt_original = corrupt.clone();
    let request = dispatch(&mut corrupt, true, true);
    assert!(!request.success);
    assert_eq!(request.msg_size, -1);
    assert_eq!(corrupt, corrupt_original);
    assert_ne!(corrupt, original);
}

#[test]
fn blank_advances_only_header_and_valid_message_advances_declared_size() {
    let valid = frame(Version::V1, 0, b"body", b"topic", b"");
    let mut blank_then_valid = BytesMut::new();
    blank_then_valid.put_i32(64);
    blank_then_valid.put_i32(BLANK_MAGIC_CODE);
    blank_then_valid.extend_from_slice(&valid);
    let mut blank_then_valid = blank_then_valid.freeze();
    let blank = dispatch(&mut blank_then_valid, false, false);
    assert!(blank.success);
    assert_eq!(blank.msg_size, 0);
    assert_eq!(blank_then_valid, valid);

    let second = frame(Version::V2, 0, b"next", b"topic-v2", b"");
    let mut two = BytesMut::new();
    two.extend_from_slice(&valid);
    two.extend_from_slice(&second);
    let mut two = two.freeze();
    let first_request = dispatch(&mut two, false, false);
    assert!(first_request.success);
    assert_eq!(first_request.msg_size as usize, valid.len());
    assert_eq!(two, second);
}

#[test]
fn blank_declared_size_is_bounded_before_advancement() {
    for (declared_size, available, valid) in [(8, 8, true), (64, 64, true), (7, 8, false), (64, 8, false)] {
        let mut encoded = BytesMut::with_capacity(available);
        encoded.put_i32(declared_size);
        encoded.put_i32(BLANK_MAGIC_CODE);
        encoded.put_bytes(0xA5, available - 8);
        let mut input = encoded.freeze();
        let original = input.clone();

        let request = dispatch(&mut input, false, false);

        assert_eq!(
            request.success, valid,
            "declared_size={declared_size}, available={available}"
        );
        assert_eq!(request.msg_size, if valid { 0 } else { -1 });
        if valid {
            assert_eq!(input, original.slice(8..));
        } else {
            assert_eq!(input, original);
        }
    }
}

#[test]
fn extra_bytes_size_mismatch_advances_declared_and_preserves_next_record() {
    let first = frame(Version::V1, 0, b"body", b"topic", b"");
    let second = frame(Version::V1, 0, b"next", b"topic", b"");
    let declared = first.len() + 4;
    let mut bytes = first.to_vec();
    bytes[..4].copy_from_slice(&(declared as i32).to_be_bytes());
    bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    bytes.extend_from_slice(&second);
    let mut bytes = Bytes::from(bytes);

    let mismatch = dispatch(&mut bytes, false, false);
    assert!(!mismatch.success);
    assert_eq!(mismatch.msg_size as usize, declared);
    assert_eq!(bytes, second);

    let next = dispatch(&mut bytes, false, false);
    assert!(next.success);
    assert_eq!(next.msg_size as usize, second.len());
    assert!(bytes.is_empty());
}

#[test]
fn force_property_crc_precedes_size_mismatch_and_uses_bounded_raw_frame() {
    let config = Arc::new(MessageStoreConfig {
        force_verify_prop_crc: true,
        ..MessageStoreConfig::default()
    });
    let valid = property_crc_frame(b"body");
    let mut valid_input = valid.clone();
    let valid_request = dispatch_with(&mut valid_input, true, false, false, Arc::clone(&config));
    assert!(valid_request.success);
    assert!(valid_input.is_empty());

    let mut dual_fault = valid.to_vec();
    let declared = dual_fault.len() + 4;
    dual_fault[..4].copy_from_slice(&(declared as i32).to_be_bytes());
    dual_fault.extend_from_slice(&[1, 2, 3, 4]);
    let mut dual_fault = Bytes::from(dual_fault);
    let original = dual_fault.clone();
    let request = dispatch_with(&mut dual_fault, true, false, false, config);
    assert!(!request.success);
    assert_eq!(request.msg_size, -1, "property CRC must fail before size mismatch");
    assert_eq!(dual_fault, original);
}

#[test]
fn size_mismatch_precedes_inner_batch_validation() {
    let mut properties = property(MessageConst::PROPERTY_INNER_BASE, "not-a-number");
    properties.extend_from_slice(&property(MessageConst::PROPERTY_INNER_NUM, "0"));
    let record = frame(Version::V1, 0, b"body", b"topic", &properties);
    let declared = record.len() + 4;
    let mut input = record.to_vec();
    input[..4].copy_from_slice(&(declared as i32).to_be_bytes());
    input.extend_from_slice(&[9, 8, 7, 6]);
    let mut input = Bytes::from(input);

    let request = dispatch(&mut input, false, false);
    assert!(!request.success);
    assert_eq!(request.msg_size as usize, declared);
    assert!(input.is_empty(), "size mismatch must consume the declared frame");
}

#[test]
fn duplicate_and_inner_batch_errors_leave_input_unchanged() {
    let properties = property(MessageConst::PROPERTY_KEYS, "key");
    let mut missing_dup = frame(Version::V1, 0, b"body", b"topic", &properties);
    let missing_dup_original = missing_dup.clone();
    let request = dispatch_with(
        &mut missing_dup,
        false,
        true,
        false,
        Arc::new(MessageStoreConfig::default()),
    );
    assert!(!request.success);
    assert_eq!(request.msg_size, -1);
    assert_eq!(missing_dup, missing_dup_original);

    let mut properties = property(MessageConst::PROPERTY_INNER_BASE, "bad");
    properties.extend_from_slice(&property(MessageConst::PROPERTY_INNER_NUM, "1"));
    let mut bad_batch = frame(Version::V1, 0, b"body", b"topic", &properties);
    let bad_batch_original = bad_batch.clone();
    let request = dispatch(&mut bad_batch, false, false);
    assert!(!request.success);
    assert_eq!(request.msg_size, -1);
    assert_eq!(bad_batch, bad_batch_original);
}

#[test]
fn valid_v1_dispatch_request_whole_value_golden_preserves_policy_fields() {
    let entries = [
        (MessageConst::PROPERTY_TAGS, "tag-v1"),
        (MessageConst::PROPERTY_KEYS, "key-a key-b"),
        (MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "uniq-v1"),
        (MessageConst::DUP_INFO, "left_right"),
        (MessageConst::PROPERTY_INNER_BASE, "101"),
        (MessageConst::PROPERTY_INNER_NUM, "4"),
    ];
    let encoded_properties = properties(&entries);
    let record = frame(
        Version::V1,
        (1 << 4) | (1 << 5),
        b"body-v1",
        b"topic-v1",
        &encoded_properties,
    );
    let mut input = record.clone();
    let tags = CheetahString::from("tag-v1");

    let actual = dispatch_with(&mut input, true, true, true, Arc::new(MessageStoreConfig::default()));
    let expected = DispatchRequest {
        topic: CheetahString::from("topic-v1"),
        queue_id: 3,
        commit_log_offset: 13,
        msg_size: record.len() as i32,
        tags_code: tags_string2tags_code(Some(&tags)),
        store_timestamp: 19,
        consume_queue_offset: 11,
        keys: CheetahString::from("key-a key-b"),
        success: true,
        uniq_key: Some(CheetahString::from("uniq-v1")),
        sys_flag: (1 << 4) | (1 << 5),
        prepared_transaction_offset: 29,
        properties_map: Some(property_map(&entries)),
        msg_base_offset: 101,
        batch_size: 4,
        ..DispatchRequest::default()
    };

    assert_eq!(
        DispatchRequestProjection::from(&actual),
        DispatchRequestProjection::from(&expected)
    );
    assert!(input.is_empty());
}

#[test]
fn valid_v2_dispatch_request_whole_value_goldens_preserve_delay_table_and_fallback() {
    let entries = [
        (MessageConst::PROPERTY_TAGS, "ignored-by-delay"),
        (MessageConst::PROPERTY_DELAY_TIME_LEVEL, "9"),
    ];
    let encoded_properties = properties(&entries);
    let record = frame(
        Version::V2,
        0,
        b"body-v2",
        TopicValidator::RMQ_SYS_SCHEDULE_TOPIC.as_bytes(),
        &encoded_properties,
    );
    let mut delay_table = BTreeMap::new();
    delay_table.insert(5, 4_000);
    let mut table_input = record.clone();

    let table_actual = dispatch_with_policy(
        &mut table_input,
        false,
        false,
        false,
        Arc::new(MessageStoreConfig::default()),
        5,
        &delay_table,
    );
    let table_expected = DispatchRequest {
        topic: CheetahString::from(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
        queue_id: 3,
        commit_log_offset: 13,
        msg_size: record.len() as i32,
        tags_code: 4_019,
        store_timestamp: 19,
        consume_queue_offset: 11,
        success: true,
        sys_flag: 0,
        prepared_transaction_offset: 29,
        properties_map: Some(property_map(&entries)),
        ..DispatchRequest::default()
    };
    assert_eq!(
        DispatchRequestProjection::from(&table_actual),
        DispatchRequestProjection::from(&table_expected)
    );
    assert!(table_input.is_empty());

    let fallback_entries = [
        (MessageConst::PROPERTY_TAGS, "also-ignored"),
        (MessageConst::PROPERTY_DELAY_TIME_LEVEL, "3"),
    ];
    let fallback_record = frame(
        Version::V2,
        0,
        b"body-v2",
        TopicValidator::RMQ_SYS_SCHEDULE_TOPIC.as_bytes(),
        &properties(&fallback_entries),
    );
    let mut fallback_input = fallback_record.clone();
    let fallback_actual = dispatch_with_policy(
        &mut fallback_input,
        false,
        false,
        false,
        Arc::new(MessageStoreConfig::default()),
        5,
        &BTreeMap::new(),
    );
    let fallback_expected = DispatchRequest {
        topic: CheetahString::from(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
        queue_id: 3,
        commit_log_offset: 13,
        msg_size: fallback_record.len() as i32,
        tags_code: 1_019,
        store_timestamp: 19,
        consume_queue_offset: 11,
        success: true,
        sys_flag: 0,
        prepared_transaction_offset: 29,
        properties_map: Some(property_map(&fallback_entries)),
        ..DispatchRequest::default()
    };
    assert_eq!(
        DispatchRequestProjection::from(&fallback_actual),
        DispatchRequestProjection::from(&fallback_expected)
    );
    assert!(fallback_input.is_empty());
}
