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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SerializationError;

use super::remoting_command::RemotingCommand;

const FRAME_PREFIX_BYTES: usize = 8;
const SERIALIZE_TYPE_BYTES: usize = 4;
const MAX_HEADER_BYTES: usize = 0x00ff_ffff;

/// Immutable RocketMQ wire frame split into prefix, serialized header, and body segments.
///
/// Keeping the body in its existing [`Bytes`] allocation lets plaintext transports use vectored
/// writes without first copying the complete frame into a second contiguous buffer. The frame's
/// exact [`Self::encoded_len`] is also available before transport staging or TLS aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedFrame {
    prefix: [u8; FRAME_PREFIX_BYTES],
    header: Bytes,
    body: Bytes,
}

/// Immutable RocketMQ prefix and serialized header for a body stored outside memory.
///
/// The external body must contain exactly [`Self::body_len`] bytes and must be appended directly
/// after [`Self::segments`]. This type lets transports send a leased file region without first
/// materializing the body as [`Bytes`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedFrameHead {
    prefix: [u8; FRAME_PREFIX_BYTES],
    header: Bytes,
    body_len: usize,
}

impl EncodedFrame {
    /// Encodes a command into immutable wire segments.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the command header cannot be encoded, the header exceeds
    /// RocketMQ's 24-bit header-length field, or the complete frame exceeds its signed 32-bit
    /// length field.
    pub fn from_command(mut command: RemotingCommand) -> RocketMQResult<Self> {
        let body = command.take_body().unwrap_or_default();
        let (prefix, header) = encode_header_segments(&mut command, body.len())?;
        validate_announced_payload_len(&prefix, header.len(), body.len())?;
        Ok(Self { prefix, header, body })
    }

    /// Returns the complete encoded wire size, including the four-byte total-length prefix.
    #[inline]
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        FRAME_PREFIX_BYTES + self.header.len() + self.body.len()
    }

    /// Returns the immutable prefix, header, and body slices in wire order.
    #[inline]
    #[must_use]
    pub fn segments(&self) -> [&[u8]; 3] {
        [&self.prefix, self.header.as_ref(), self.body.as_ref()]
    }

    /// Appends the wire frame to a caller-owned contiguous buffer.
    ///
    /// This is intended for TLS record input and compatibility codecs. Plaintext transports should
    /// preserve [`Self::segments`] through to vectored socket writes.
    pub fn copy_to(&self, destination: &mut BytesMut) {
        destination.reserve(self.encoded_len());
        destination.put_slice(&self.prefix);
        destination.put_slice(&self.header);
        destination.put_slice(&self.body);
    }

    /// Materializes the complete frame as contiguous immutable bytes.
    ///
    /// Prefer [`Self::segments`] when the destination accepts vectored writes.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        let mut contiguous = BytesMut::with_capacity(self.encoded_len());
        self.copy_to(&mut contiguous);
        contiguous.freeze()
    }
}

impl EncodedFrameHead {
    /// Encodes a command prefix/header for an external body of `body_len` bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the command already has an in-memory body, header serialization
    /// fails, or the complete frame exceeds RocketMQ's signed 32-bit wire-length limit.
    pub fn from_command_and_body_len(mut command: RemotingCommand, body_len: usize) -> RocketMQResult<Self> {
        if command.body().is_some() {
            return Err(SerializationError::encode_failed(
                "remoting-command-file-body",
                "command must not contain an in-memory body when an external body length is supplied",
            )
            .into());
        }
        let (prefix, header) = encode_header_segments(&mut command, body_len)?;
        validate_announced_payload_len(&prefix, header.len(), body_len)?;
        Ok(Self {
            prefix,
            header,
            body_len,
        })
    }

    /// Returns the immutable prefix and serialized header slices in wire order.
    #[inline]
    #[must_use]
    pub fn segments(&self) -> [&[u8]; 2] {
        [&self.prefix, self.header.as_ref()]
    }

    /// Returns the exact number of external body bytes required by this frame.
    #[inline]
    #[must_use]
    pub const fn body_len(&self) -> usize {
        self.body_len
    }

    /// Returns the complete encoded frame size, including the external body.
    #[inline]
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        FRAME_PREFIX_BYTES + self.header.len() + self.body_len
    }
}

fn encode_header_segments(
    command: &mut RemotingCommand,
    body_len: usize,
) -> RocketMQResult<([u8; FRAME_PREFIX_BYTES], Bytes)> {
    let mut encoded_header = BytesMut::new();
    command.try_fast_header_encode_with_body_length(&mut encoded_header, body_len)?;
    if encoded_header.len() < FRAME_PREFIX_BYTES {
        return Err(SerializationError::encode_failed(
            "remoting-command",
            "encoded header omitted the RocketMQ frame prefix",
        )
        .into());
    }
    let prefix_bytes = encoded_header.split_to(FRAME_PREFIX_BYTES);
    let header = encoded_header.freeze();
    if header.len() > MAX_HEADER_BYTES {
        return Err(SerializationError::encode_failed(
            "remoting-command",
            format!(
                "encoded header is {} bytes, exceeding the 24-bit wire limit",
                header.len()
            ),
        )
        .into());
    }
    let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
    prefix.copy_from_slice(&prefix_bytes);
    let announced_header = u32::from_be_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]) & MAX_HEADER_BYTES as u32;
    if announced_header as usize != header.len() {
        return Err(SerializationError::encode_failed(
            "remoting-command",
            "fast header encoder produced an inconsistent header length",
        )
        .into());
    }
    Ok((prefix, header))
}

fn checked_payload_len(header_len: usize, body_len: usize) -> RocketMQResult<i32> {
    let payload_len = SERIALIZE_TYPE_BYTES
        .checked_add(header_len)
        .and_then(|length| length.checked_add(body_len))
        .ok_or_else(|| SerializationError::encode_failed("remoting-command", "encoded frame length overflow"))?;
    i32::try_from(payload_len).map_err(|_| {
        SerializationError::encode_failed(
            "remoting-command",
            format!("encoded payload is {payload_len} bytes, exceeding the signed 32-bit wire limit"),
        )
        .into()
    })
}

fn validate_announced_payload_len(
    prefix: &[u8; FRAME_PREFIX_BYTES],
    header_len: usize,
    body_len: usize,
) -> RocketMQResult<()> {
    let expected = checked_payload_len(header_len, body_len)?;
    let announced = i32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
    if announced != expected {
        return Err(SerializationError::encode_failed(
            "remoting-command",
            "fast header encoder produced inconsistent wire lengths",
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use cheetah_string::CheetahString;

    use super::EncodedFrame;
    use super::EncodedFrameHead;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::HeaderEncodeCapability;
    use crate::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use crate::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
    use crate::protocol::header_codec::HeaderCodecError;
    use crate::protocol::remoting_command::RemotingCommand;
    use crate::protocol::SerializeType;

    struct FailingDirectHeader;

    impl CommandCustomHeader for FailingDirectHeader {
        fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
            Some(HashMap::new())
        }

        fn encode_capability(&self) -> HeaderEncodeCapability {
            HeaderEncodeCapability::DirectBinary
        }

        fn encode_direct_binary(&self, _out: &mut BytesMut) -> Result<(), HeaderCodecError> {
            Err(HeaderCodecError::FastCodecUnavailable {
                header: "FailingDirectHeader",
            })
        }
    }

    fn legacy_contiguous(mut command: RemotingCommand) -> Bytes {
        let mut bytes = BytesMut::new();
        command.fast_header_encode(&mut bytes);
        if let Some(body) = command.take_body() {
            bytes.put(body);
        }
        bytes.freeze()
    }

    #[test]
    fn encoded_frame_matches_json_contiguous_encoding_byte_for_byte() {
        let command = RemotingCommand::create_remoting_command(105)
            .set_opaque(73)
            .set_remark("segmented-json")
            .set_command_custom_header(GetRouteInfoRequestHeader::new("json-topic", None))
            .set_body(Bytes::from_static(b"json-body"));
        let expected = legacy_contiguous(command.clone());
        let frame = EncodedFrame::from_command(command).expect("JSON command should encode");

        assert_eq!(frame.encoded_len(), expected.len());
        assert_eq!(frame.into_bytes(), expected);
    }

    #[test]
    fn encoded_frame_matches_rocketmq_contiguous_encoding_byte_for_byte() {
        let command = RemotingCommand::create_remoting_command(106)
            .set_opaque(74)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_command_custom_header(GetRouteInfoRequestHeader::new("binary-topic", Some(false)))
            .set_body(Bytes::from_static(b"binary-body"));
        let expected = legacy_contiguous(command.clone());
        let frame = EncodedFrame::from_command(command).expect("RocketMQ command should encode");

        assert_eq!(frame.encoded_len(), expected.len());
        assert_eq!(frame.into_bytes(), expected);
    }

    #[test]
    fn encoded_frame_exposes_prefix_header_and_body_without_coalescing() {
        let body = Bytes::from_static(b"owned-body");
        let frame = EncodedFrame::from_command(
            RemotingCommand::create_remoting_command(107)
                .set_opaque(75)
                .set_body(body.clone()),
        )
        .expect("command should encode");
        let [prefix, header, encoded_body] = frame.segments();

        assert_eq!(prefix.len(), 8);
        assert!(!header.is_empty());
        assert_eq!(encoded_body, body.as_ref());
        assert_eq!(frame.encoded_len(), prefix.len() + header.len() + encoded_body.len());
    }

    #[test]
    fn encoded_frame_head_matches_the_complete_frame_byte_for_byte() {
        let body = Bytes::from(vec![0x5a; 64 * 1024]);
        let command = RemotingCommand::create_remoting_command(108)
            .set_opaque(91)
            .set_remark("external-file-body");
        let head = EncodedFrameHead::from_command_and_body_len(command.clone(), body.len()).unwrap();
        let complete = EncodedFrame::from_command(command.set_body(body.clone()))
            .unwrap()
            .into_bytes();
        let [prefix, header] = head.segments();
        let mut reconstructed = BytesMut::with_capacity(head.encoded_len());
        reconstructed.put_slice(prefix);
        reconstructed.put_slice(header);
        reconstructed.put_slice(&body);

        assert_eq!(reconstructed.freeze(), complete);
        assert_eq!(head.body_len(), body.len());
    }

    #[test]
    fn frame_head_and_complete_frame_match_for_all_body_sizes_and_protocols() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            for body_len in [0, 1, 128, 4 * 1024, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
                let body = Bytes::from(vec![0x5a; body_len]);
                let command = RemotingCommand::create_remoting_command(108)
                    .set_opaque(92)
                    .set_serialize_type(serialize_type)
                    .set_remark("body-length-matrix");
                let head = EncodedFrameHead::from_command_and_body_len(command.clone(), body_len).unwrap();
                let complete = EncodedFrame::from_command(command.set_body(body.clone()))
                    .unwrap()
                    .into_bytes();
                let [prefix, header] = head.segments();
                let mut reconstructed = BytesMut::with_capacity(head.encoded_len());
                reconstructed.put_slice(prefix);
                reconstructed.put_slice(header);
                reconstructed.put_slice(&body);

                assert_eq!(
                    reconstructed.freeze(),
                    complete,
                    "{serialize_type:?} body_len={body_len}"
                );
            }
        }
    }

    #[test]
    fn explicit_body_length_rejects_an_in_memory_body_mismatch_atomically() {
        let mut command = RemotingCommand::create_remoting_command(109).set_body(Bytes::from_static(b"body"));
        let mut destination = BytesMut::from(&b"existing"[..]);

        assert!(command
            .try_fast_header_encode_with_body_length(&mut destination, 3)
            .is_err());
        assert_eq!(destination.as_ref(), b"existing");
    }

    #[test]
    fn encoded_frame_head_rejects_ambiguous_or_oversized_bodies() {
        let with_body = RemotingCommand::create_remoting_command(109).set_body(Bytes::from_static(b"ambiguous"));
        assert!(EncodedFrameHead::from_command_and_body_len(with_body, 9).is_err());

        let command = RemotingCommand::create_remoting_command(110);
        assert!(EncodedFrameHead::from_command_and_body_len(command, i32::MAX as usize).is_err());
    }

    #[test]
    fn encoded_frame_canonicalizes_ext_field_order_for_all_protocols() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let first = HashMap::from([
                (
                    CheetahString::from_static_str("zeta"),
                    CheetahString::from_static_str("last"),
                ),
                (
                    CheetahString::from_static_str("alpha"),
                    CheetahString::from_static_str("first"),
                ),
            ]);
            let second = HashMap::from([
                (
                    CheetahString::from_static_str("alpha"),
                    CheetahString::from_static_str("first"),
                ),
                (
                    CheetahString::from_static_str("zeta"),
                    CheetahString::from_static_str("last"),
                ),
            ]);
            let encode = |ext_fields| {
                EncodedFrame::from_command(
                    RemotingCommand::create_remoting_command(108)
                        .set_opaque(76)
                        .set_serialize_type(serialize_type)
                        .set_ext_fields(ext_fields),
                )
                .expect("command should encode")
                .into_bytes()
            };

            assert_eq!(encode(first), encode(second));
        }
    }

    #[test]
    fn direct_header_encoding_is_identical_after_multiple_command_clones() {
        let header = SendMessageRequestHeaderV2 {
            a: "producer-a".into(),
            b: "topic-a".into(),
            c: "TBW102".into(),
            d: 4,
            e: 2,
            f: 0,
            g: 42,
            h: 1,
            ..Default::default()
        };
        let original = RemotingCommand::create_remoting_command(310)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_command_custom_header(header);
        let first_clone = original.clone();
        let second_clone = first_clone.clone();

        let frames = [original, first_clone, second_clone].map(|command| {
            EncodedFrame::from_command(command)
                .expect("shared direct header should encode")
                .into_bytes()
        });
        assert_eq!(frames[0], frames[1]);
        assert_eq!(frames[1], frames[2]);

        for frame in frames {
            let mut input = BytesMut::from(frame.as_ref());
            let decoded = RemotingCommand::decode(&mut input)
                .expect("frame should decode")
                .expect("frame should be complete");
            let decoded_header = decoded
                .decode_command_custom_header::<SendMessageRequestHeaderV2>()
                .expect("direct fields should remain present");
            assert_eq!(decoded_header.a.as_str(), "producer-a");
            assert_eq!(decoded_header.b.as_str(), "topic-a");
            assert_eq!(decoded_header.g, 42);
        }
    }

    #[test]
    fn fallible_direct_header_encoding_rolls_back_the_destination() {
        let mut command = RemotingCommand::create_remoting_command(311)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_command_custom_header(FailingDirectHeader);
        let mut destination = BytesMut::from(&b"existing"[..]);

        assert!(command.try_fast_header_encode(&mut destination).is_err());
        assert_eq!(destination.as_ref(), b"existing");
    }

    #[test]
    fn materialized_direct_header_is_not_encoded_twice_after_clone() {
        let header = SendMessageRequestHeaderV2 {
            a: "producer-b".into(),
            b: "topic-b".into(),
            c: "TBW102".into(),
            d: 4,
            e: 3,
            f: 0,
            g: 43,
            h: 1,
            ..Default::default()
        };
        let mut original = RemotingCommand::create_remoting_command(312)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_command_custom_header(header);
        original.materialize_custom_header_to_ext_fields();
        let cloned = original.clone();

        let original_frame = EncodedFrame::from_command(original).unwrap().into_bytes();
        let cloned_frame = EncodedFrame::from_command(cloned).unwrap().into_bytes();
        assert_eq!(original_frame, cloned_frame);
    }
}
