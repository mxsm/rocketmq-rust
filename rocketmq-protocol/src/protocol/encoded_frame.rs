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

impl EncodedFrame {
    /// Encodes a command into immutable wire segments.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the command header cannot be encoded, the header exceeds
    /// RocketMQ's 24-bit header-length field, or the complete frame exceeds its signed 32-bit
    /// length field.
    pub fn from_command(mut command: RemotingCommand) -> RocketMQResult<Self> {
        let mut encoded_header = BytesMut::new();
        command.fast_header_encode(&mut encoded_header);
        let body = command.take_body().unwrap_or_default();
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
        let payload_len = SERIALIZE_TYPE_BYTES
            .checked_add(header.len())
            .and_then(|length| length.checked_add(body.len()))
            .ok_or_else(|| SerializationError::encode_failed("remoting-command", "encoded frame length overflow"))?;
        let total_len = i32::try_from(payload_len).map_err(|_| {
            SerializationError::encode_failed(
                "remoting-command",
                format!("encoded payload is {payload_len} bytes, exceeding the signed 32-bit wire limit"),
            )
        })?;
        let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
        prefix.copy_from_slice(&prefix_bytes);
        let announced_total = i32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
        let announced_header =
            u32::from_be_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]) & MAX_HEADER_BYTES as u32;
        if announced_total != total_len || announced_header as usize != header.len() {
            return Err(SerializationError::encode_failed(
                "remoting-command",
                "fast header encoder produced inconsistent wire lengths",
            )
            .into());
        }
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

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;

    use super::EncodedFrame;
    use crate::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use crate::protocol::remoting_command::RemotingCommand;
    use crate::protocol::SerializeType;

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
}
