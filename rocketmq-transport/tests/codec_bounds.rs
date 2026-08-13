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

#![cfg(feature = "test-support")]

use bytes::BytesMut;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::v1::FrameLimits;
use rocketmq_transport::test_support::RemotingCommandCodec;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

fn announced_frame(total_wire_bytes: usize, header_bytes: usize) -> BytesMut {
    let announced_bytes = total_wire_bytes - 4;
    let mut frame = BytesMut::with_capacity(8);
    frame.extend_from_slice(&(announced_bytes as i32).to_be_bytes());
    frame.extend_from_slice(&(header_bytes as u32).to_be_bytes());
    frame
}

fn command_with_encoded_header_bytes(target_header_bytes: usize) -> RemotingCommand {
    let empty = RemotingCommand::create_remoting_command(105)
        .set_opaque(1)
        .set_remark("");
    let mut codec = RemotingCommandCodec::with_limits(FrameLimits::java_compatibility());
    let mut wire = BytesMut::new();
    codec.encode(empty, &mut wire).unwrap();
    let empty_header_bytes = (u32::from_be_bytes(wire[4..8].try_into().unwrap()) & 0x00ff_ffff) as usize;
    assert!(target_header_bytes >= empty_header_bytes);
    RemotingCommand::create_remoting_command(105)
        .set_opaque(1)
        .set_remark("x".repeat(target_header_bytes - empty_header_bytes))
}

#[test]
fn fragmented_frame_waits_and_then_decodes_without_eager_megabyte_allocation() {
    let mut codec = RemotingCommandCodec::with_limits(FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 512,
        initial_read_bytes: 16,
    });
    let command = RemotingCommand::create_remoting_command(105);
    let mut encoded = BytesMut::new();
    codec.encode(command, &mut encoded).unwrap();

    let split = encoded.len() / 2;
    let tail = encoded.split_off(split);
    assert!(codec.decode(&mut encoded).unwrap().is_none());
    encoded.extend_from_slice(&tail);
    assert_eq!(codec.decode(&mut encoded).unwrap().unwrap().code(), 105);
    assert!(encoded.capacity() < 1024 * 1024);
}

#[test]
fn oversized_frame_is_rejected_before_body_allocation() {
    let mut codec = RemotingCommandCodec::with_limits(FrameLimits {
        max_frame_bytes: 32,
        max_header_bytes: 16,
        max_body_bytes: 16,
        initial_read_bytes: 8,
    });
    let mut announced = BytesMut::from(&[0, 0, 4, 0][..]);
    assert!(codec.decode(&mut announced).is_err());
    assert!(announced.capacity() < 1024);
}

#[test]
fn legacy_large_body_acceptance_requires_an_explicit_owner_profile() {
    let canonical = FrameLimits::default();
    let java = FrameLimits::java_compatibility();
    let legacy = FrameLimits::legacy_compatibility();

    assert_eq!(canonical.max_frame_bytes, 16 * 1024 * 1024);
    assert_eq!(canonical.max_header_bytes, 1024 * 1024);
    assert_eq!(canonical.max_body_bytes, 4 * 1024 * 1024);
    assert_eq!(java.max_frame_bytes, 16 * 1024 * 1024);
    assert_eq!(java.max_header_bytes, 4 * 1024 * 1024);
    assert_eq!(java.max_body_bytes, 16 * 1024 * 1024);
    assert_eq!(legacy, java);
    assert_eq!(canonical.initial_read_bytes, 8 * 1024);
}

#[test]
fn encode_and_decode_apply_the_same_total_wire_byte_limit() {
    let command = RemotingCommand::create_remoting_command(105).set_body(vec![7_u8; 64]);
    let mut unrestricted = RemotingCommandCodec::with_limits(FrameLimits::java_compatibility());
    let mut wire = BytesMut::new();
    unrestricted.encode(command.clone(), &mut wire).unwrap();
    let exact_wire_bytes = wire.len();

    let exact_limits = FrameLimits {
        max_frame_bytes: exact_wire_bytes,
        max_header_bytes: exact_wire_bytes,
        max_body_bytes: 64,
        initial_read_bytes: 8,
    };
    let mut exact_codec = RemotingCommandCodec::with_limits(exact_limits);
    let mut exact_encoded = BytesMut::new();
    exact_codec.encode(command.clone(), &mut exact_encoded).unwrap();
    assert!(exact_codec.decode(&mut exact_encoded).unwrap().is_some());

    let below_limits = FrameLimits {
        max_frame_bytes: exact_wire_bytes - 1,
        ..exact_limits
    };
    let mut below_encoder = RemotingCommandCodec::with_limits(below_limits);
    let mut destination = BytesMut::from(&b"existing"[..]);
    let original = destination.clone();
    assert!(below_encoder.encode(command, &mut destination).is_err());
    assert_eq!(destination, original, "failed encoding must be atomic");

    let mut below_decoder = RemotingCommandCodec::with_limits(below_limits);
    assert!(below_decoder.decode(&mut wire).is_err());
}

#[test]
fn encoder_rejects_body_over_limit_without_mutating_destination() {
    let limits = FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 16,
        initial_read_bytes: 8,
    };
    let command = RemotingCommand::create_remoting_command(105).set_body(vec![0_u8; 17]);
    let mut codec = RemotingCommandCodec::with_limits(limits);
    let mut destination = BytesMut::from(&b"existing"[..]);
    let original = destination.clone();

    assert!(codec.encode(command, &mut destination).is_err());
    assert_eq!(destination, original, "failed encoding must not append a partial frame");
}

#[test]
fn canonical_encoder_accepts_exact_header_limit_and_rejects_one_byte_over() {
    let limits = FrameLimits::default();
    let mut codec = RemotingCommandCodec::with_limits(limits);
    let mut exact = BytesMut::new();
    codec
        .encode(command_with_encoded_header_bytes(limits.max_header_bytes), &mut exact)
        .unwrap();
    let encoded_header_bytes = (u32::from_be_bytes(exact[4..8].try_into().unwrap()) & 0x00ff_ffff) as usize;
    assert_eq!(encoded_header_bytes, limits.max_header_bytes);
    assert!(codec.decode(&mut exact).unwrap().is_some());

    let mut destination = BytesMut::from(&b"existing"[..]);
    let original = destination.clone();
    assert!(codec
        .encode(
            command_with_encoded_header_bytes(limits.max_header_bytes + 1),
            &mut destination,
        )
        .is_err());
    assert_eq!(destination, original);
}

#[test]
fn canonical_encoder_accepts_exact_body_limit_and_rejects_one_byte_over() {
    let limits = FrameLimits::default();
    let mut codec = RemotingCommandCodec::with_limits(limits);
    let mut exact = BytesMut::new();
    codec
        .encode(
            RemotingCommand::create_remoting_command(105).set_body(vec![0_u8; limits.max_body_bytes]),
            &mut exact,
        )
        .unwrap();
    assert!(codec.decode(&mut exact).unwrap().is_some());

    let mut destination = BytesMut::from(&b"existing"[..]);
    let original = destination.clone();
    assert!(codec
        .encode(
            RemotingCommand::create_remoting_command(105).set_body(vec![0_u8; limits.max_body_bytes + 1]),
            &mut destination,
        )
        .is_err());
    assert_eq!(destination, original);
}

#[test]
fn canonical_limits_accept_exact_body_limit_with_non_empty_header() {
    let limits = FrameLimits::default();
    let mut codec = RemotingCommandCodec::with_limits(limits);
    let command = RemotingCommand::create_remoting_command(105).set_body(vec![0_u8; limits.max_body_bytes]);
    let mut encoded = BytesMut::new();

    codec.encode(command, &mut encoded).unwrap();
    assert!(encoded.len() > limits.max_body_bytes);

    let decoded = codec.decode(&mut encoded).unwrap().unwrap();
    assert_eq!(decoded.body().unwrap().len(), limits.max_body_bytes);
}

#[test]
fn canonical_limits_reject_announced_body_over_limit_before_allocation() {
    let limits = FrameLimits::default();
    let announced_total = 4 + limits.max_body_bytes + 1;
    let mut announced = BytesMut::with_capacity(8);
    announced.extend_from_slice(&(announced_total as i32).to_be_bytes());
    announced.extend_from_slice(&0_u32.to_be_bytes());
    let mut codec = RemotingCommandCodec::with_limits(limits);

    assert!(codec.decode(&mut announced).is_err());
    assert!(announced.capacity() < 1024);
}

#[test]
fn canonical_header_boundary_is_checked_from_the_eight_byte_announcement() {
    let limits = FrameLimits::default();
    for header_bytes in [limits.max_header_bytes - 1, limits.max_header_bytes] {
        let mut announced = announced_frame(8 + header_bytes, header_bytes);
        let capacity = announced.capacity();
        let mut codec = RemotingCommandCodec::with_limits(limits);
        assert!(codec.decode(&mut announced).unwrap().is_none());
        assert_eq!(announced.capacity(), capacity);
    }

    let header_bytes = limits.max_header_bytes + 1;
    let mut announced = announced_frame(8 + header_bytes, header_bytes);
    let capacity = announced.capacity();
    let mut codec = RemotingCommandCodec::with_limits(limits);
    assert!(codec.decode(&mut announced).is_err());
    assert_eq!(announced.capacity(), capacity);
}

#[test]
fn canonical_body_boundary_is_checked_from_the_eight_byte_announcement() {
    let limits = FrameLimits::default();
    for body_bytes in [limits.max_body_bytes - 1, limits.max_body_bytes] {
        let mut announced = announced_frame(8 + body_bytes, 0);
        let capacity = announced.capacity();
        let mut codec = RemotingCommandCodec::with_limits(limits);
        assert!(codec.decode(&mut announced).unwrap().is_none());
        assert_eq!(announced.capacity(), capacity);
    }

    let body_bytes = limits.max_body_bytes + 1;
    let mut announced = announced_frame(8 + body_bytes, 0);
    let capacity = announced.capacity();
    let mut codec = RemotingCommandCodec::with_limits(limits);
    assert!(codec.decode(&mut announced).is_err());
    assert_eq!(announced.capacity(), capacity);
}

#[test]
fn total_wire_boundary_counts_the_length_prefix() {
    let limits = FrameLimits {
        max_frame_bytes: 1024,
        max_header_bytes: 512,
        max_body_bytes: 1024,
        initial_read_bytes: 8,
    };
    for total_wire_bytes in [limits.max_frame_bytes - 1, limits.max_frame_bytes] {
        let mut announced = announced_frame(total_wire_bytes, 0);
        let mut codec = RemotingCommandCodec::with_limits(limits);
        assert!(codec.decode(&mut announced).unwrap().is_none());
    }

    let mut over = announced_frame(limits.max_frame_bytes + 1, 0);
    let capacity = over.capacity();
    let mut codec = RemotingCommandCodec::with_limits(limits);
    assert!(codec.decode(&mut over).is_err());
    assert_eq!(over.capacity(), capacity);
}

#[test]
fn full_24_bit_header_marker_obeys_the_explicit_endpoint_profile() {
    let max_header_bytes = 0x00ff_ffff;
    let limits = FrameLimits {
        max_frame_bytes: max_header_bytes + 8,
        max_header_bytes,
        max_body_bytes: 0,
        initial_read_bytes: 8,
    };
    let mut codec = RemotingCommandCodec::with_limits(limits);
    let mut exact = BytesMut::new();
    codec
        .encode(command_with_encoded_header_bytes(max_header_bytes), &mut exact)
        .unwrap();
    assert_eq!(exact.len(), limits.max_frame_bytes);
    assert!(codec.decode(&mut exact).unwrap().is_some());

    let mut below = BytesMut::new();
    codec
        .encode(command_with_encoded_header_bytes(max_header_bytes - 1), &mut below)
        .unwrap();
    assert_eq!(below.len(), limits.max_frame_bytes - 1);
    assert!(codec.decode(&mut below).unwrap().is_some());

    let over = FrameLimits {
        max_header_bytes: max_header_bytes + 1,
        max_frame_bytes: limits.max_frame_bytes + 1,
        ..limits
    };
    assert!(over.validate().is_err());
}

#[test]
fn invalid_frame_limit_profiles_fail_before_encoding_or_deserialization() {
    assert!(FrameLimits::try_new(7, 1, 1, 7).is_err());
    assert!(FrameLimits::try_new(1024, 0x0100_0000, 0, 8).is_err());
    assert!(FrameLimits::try_new(1024, 512, 512, 1025).is_err());
    assert!(serde_json::from_str::<FrameLimits>(
        r#"{"maxFrameBytes":1024,"maxHeaderBytes":512,"maxBodyBytes":512,"initialReadBytes":1025}"#,
    )
    .is_err());

    let invalid = FrameLimits {
        max_frame_bytes: 0,
        max_header_bytes: 0,
        max_body_bytes: 0,
        initial_read_bytes: usize::MAX,
    };
    let mut codec = RemotingCommandCodec::with_limits(invalid);
    let mut destination = BytesMut::from(&b"unchanged"[..]);
    let original = destination.clone();
    assert!(codec
        .encode(RemotingCommand::create_remoting_command(105), &mut destination)
        .is_err());
    assert_eq!(destination, original);
}

#[test]
fn encoder_preserves_the_protocol_24_bit_header_ceiling() {
    let limits = FrameLimits {
        max_frame_bytes: 64 * 1024 * 1024,
        max_header_bytes: 32 * 1024 * 1024,
        max_body_bytes: 0,
        initial_read_bytes: 8,
    };
    let command = RemotingCommand::create_remoting_command(105).set_remark("x".repeat(0x0100_0000));
    let mut codec = RemotingCommandCodec::with_limits(limits);
    let mut destination = BytesMut::new();

    assert!(codec.encode(command, &mut destination).is_err());
    assert!(destination.is_empty());
}

#[test]
fn consecutive_frames_decode_without_consuming_the_next_prefix() {
    let mut codec = RemotingCommandCodec::with_limits(FrameLimits::default());
    let mut wire = BytesMut::new();
    codec
        .encode(RemotingCommand::create_remoting_command(105).set_opaque(1), &mut wire)
        .unwrap();
    codec
        .encode(RemotingCommand::create_remoting_command(106).set_opaque(2), &mut wire)
        .unwrap();

    let first = codec.decode(&mut wire).unwrap().unwrap();
    let second = codec.decode(&mut wire).unwrap().unwrap();
    assert_eq!((first.code(), first.opaque()), (105, 1));
    assert_eq!((second.code(), second.opaque()), (106, 2));
    assert!(wire.is_empty());
}

#[test]
fn negative_and_forged_announcements_fail_without_buffer_growth() {
    let limits = FrameLimits::default();
    let cases = [
        BytesMut::from(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0][..]),
        BytesMut::from(&[0, 0, 0, 4, 0, 0, 0, 1][..]),
    ];

    for mut announced in cases {
        let capacity = announced.capacity();
        let mut codec = RemotingCommandCodec::with_limits(limits);
        assert!(codec.decode(&mut announced).is_err());
        assert_eq!(announced.capacity(), capacity);
    }
}
