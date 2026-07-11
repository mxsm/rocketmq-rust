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
use rocketmq_common::common::message::message_decoder;
use rocketmq_protocol::protocol::body::message_codec;

fn frozen_message_frame() -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_i32(0);
    frame.put_i32(message_codec::MESSAGE_MAGIC_CODE);
    frame.put_u32(7);
    frame.put_i32(3);
    frame.put_i32(9);
    frame.put_i64(11);
    frame.put_i64(13);
    frame.put_i32(0);
    frame.put_i64(17);
    frame.put_slice(&[127, 0, 0, 1]);
    frame.put_i32(10911);
    frame.put_i64(19);
    frame.put_slice(&[127, 0, 0, 2]);
    frame.put_i32(10912);
    frame.put_i32(2);
    frame.put_i64(23);
    frame.put_i32(4);
    frame.put_slice(b"body");
    frame.put_u8(5);
    frame.put_slice(b"topic");
    frame.put_i16(8);
    frame.put_slice(b"k\x01value\x02");
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
