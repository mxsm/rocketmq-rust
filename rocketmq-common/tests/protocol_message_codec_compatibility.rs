// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::utils::crc32_utils::crc32;
use rocketmq_protocol::common::compression::compression_type::CompressionType;
use rocketmq_protocol::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_protocol::protocol::body::message_codec;

fn frozen_message_frame() -> Bytes {
    message_frame(
        message_codec::MESSAGE_MAGIC_CODE,
        7,
        0,
        b"body",
        b"topic",
        b"k\x01value\x02",
    )
}

fn message_frame(magic: i32, body_crc: u32, sys_flag: i32, body: &[u8], topic: &[u8], properties: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_i32(0);
    frame.put_i32(magic);
    frame.put_u32(body_crc);
    frame.put_i32(3);
    frame.put_i32(9);
    frame.put_i64(11);
    frame.put_i64(13);
    frame.put_i32(sys_flag);
    frame.put_i64(17);
    frame.put_slice(&[127, 0, 0, 1]);
    frame.put_i32(10911);
    frame.put_i64(19);
    frame.put_slice(&[127, 0, 0, 2]);
    frame.put_i32(10912);
    frame.put_i32(2);
    frame.put_i64(23);
    frame.put_i32(body.len() as i32);
    frame.put_slice(body);
    if magic == message_codec::MESSAGE_MAGIC_CODE_V2 {
        frame.put_i16(topic.len() as i16);
    } else {
        frame.put_u8(topic.len() as u8);
    }
    frame.put_slice(topic);
    frame.put_i16(properties.len() as i16);
    frame.put_slice(properties);
    let len = frame.len() as i32;
    frame[..4].copy_from_slice(&len.to_be_bytes());
    frame.freeze()
}

#[test]
fn canonical_and_legacy_decoders_match_frozen_frame() {
    let frame = frozen_message_frame();
    let canonical = message_codec::decode_message_frame(&mut frame.clone()).unwrap();
    let legacy = message_decoder::decode(&mut frame.clone(), true, false, false, false, false).unwrap();

    assert_eq!(canonical.topic.as_str(), legacy.topic().as_str());
    assert_eq!(canonical.body, legacy.body().unwrap());
    assert_eq!(canonical.queue_id, legacy.queue_id);
    assert_eq!(canonical.queue_offset, legacy.queue_offset);
    assert_eq!(canonical.commit_log_offset, legacy.commit_log_offset);
    assert_eq!(canonical.born_host, legacy.born_host);
    assert_eq!(canonical.store_host, legacy.store_host);
}

#[test]
fn canonical_and_legacy_decoders_reject_truncated_frame() {
    let mut canonical = frozen_message_frame().slice(..24);
    let mut legacy = canonical.clone();
    assert!(message_codec::decode_message_frame(&mut canonical).is_err());
    assert!(message_decoder::decode(&mut legacy, true, false, false, false, false).is_none());
}

#[test]
fn metadata_only_decode_does_not_check_unread_body_crc() {
    let frame = message_frame(message_codec::MESSAGE_MAGIC_CODE, 1, 0, b"bad-crc", b"topic", b"");
    let decoded = message_decoder::decode(&mut frame.clone(), false, false, false, false, true).unwrap();
    assert!(decoded.body().is_none());
}

#[test]
fn empty_body_decode_does_not_check_crc() {
    let frame = message_frame(message_codec::MESSAGE_MAGIC_CODE, 1, 0, b"", b"topic", b"");
    let decoded = message_decoder::decode(&mut frame.clone(), true, false, false, false, true).unwrap();
    assert!(decoded.body().is_none());
}

#[test]
fn nonempty_body_bad_crc_is_rejected() {
    let frame = message_frame(message_codec::MESSAGE_MAGIC_CODE, 1, 0, b"bad-crc", b"topic", b"");
    assert!(message_decoder::decode(&mut frame.clone(), true, false, false, false, true).is_none());
}

#[test]
fn compressed_body_is_crc_checked_before_decompression_and_then_expanded() {
    let original = b"compress me";
    let compressed = CompressionType::Zlib.try_compression(original).unwrap();
    let sys_flag = MessageSysFlag::COMPRESSED_FLAG | MessageSysFlag::COMPRESSION_ZLIB_TYPE;
    let frame = message_frame(
        message_codec::MESSAGE_MAGIC_CODE,
        crc32(&compressed),
        sys_flag,
        &compressed,
        b"topic",
        b"",
    );
    let decoded = message_decoder::decode(&mut frame.clone(), true, true, false, false, true).unwrap();
    assert_eq!(decoded.body().unwrap().as_ref(), original);
}

#[test]
fn v2_topic_and_properties_string_match_canonical_frame() {
    let topic = vec![b't'; 300];
    let properties = b"key\x01value\x02";
    let body = b"body";
    let frame = message_frame(
        message_codec::MESSAGE_MAGIC_CODE_V2,
        crc32(body),
        0,
        body,
        &topic,
        properties,
    );
    let canonical = message_codec::decode_message_frame(&mut frame.clone()).unwrap();
    let legacy = message_decoder::decode(&mut frame.clone(), true, false, false, true, true).unwrap();

    assert_eq!(canonical.topic.as_str(), legacy.topic().as_str());
    assert_eq!(
        legacy.properties().get("propertiesString").map(CheetahString::as_str),
        Some(canonical.properties_string.as_str())
    );
}
