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

use super::RemotingCommand;
use super::SerializeType;

impl RemotingCommand {
    /// Encode header with body length information
    #[inline]
    pub fn encode_header(&mut self) -> Option<Bytes> {
        let body_length = self.body.as_ref().map_or(0, |b| b.len());
        self.encode_header_with_body_length(body_length)
    }

    /// Optimized header encoding with pre-calculated capacity
    #[inline]
    pub fn encode_header_with_body_length(&mut self, body_length: usize) -> Option<Bytes> {
        // Encode header data
        let header_data = self.header_encode()?;
        let header_len = header_data.len();
        let (total_length, marked_header_length) =
            Self::checked_frame_lengths(header_len, body_length, self.serialize_type).ok()?;

        // Allocate exact capacity
        let mut result = BytesMut::with_capacity(8 + header_len);

        // Write total length
        result.put_i32(total_length);

        // Write serialize type with embedded header length
        result.put_i32(marked_header_length);

        // Write header data
        result.put(header_data);

        Some(result.freeze())
    }

    pub(super) fn checked_frame_lengths(
        header_length: usize,
        body_length: usize,
        serialize_type: SerializeType,
    ) -> rocketmq_error::RocketMQResult<(i32, i32)> {
        const MAX_HEADER_LENGTH: usize = 0x00ff_ffff;
        if header_length > MAX_HEADER_LENGTH {
            return Err(rocketmq_error::SerializationError::encode_failed(
                "remoting-command",
                format!("encoded header is {header_length} bytes, exceeding the 24-bit wire limit"),
            )
            .into());
        }
        let payload_length = 4usize
            .checked_add(header_length)
            .and_then(|length| length.checked_add(body_length))
            .ok_or_else(|| {
                rocketmq_error::SerializationError::encode_failed("remoting-command", "encoded frame length overflow")
            })?;
        let total_length = i32::try_from(payload_length).map_err(|_| {
            rocketmq_error::SerializationError::encode_failed(
                "remoting-command",
                format!("encoded payload is {payload_length} bytes, exceeding the signed 32-bit wire limit"),
            )
        })?;
        let header_length = i32::try_from(header_length).map_err(|_| {
            rocketmq_error::SerializationError::encode_failed("remoting-command", "encoded header length overflow")
        })?;
        Ok((
            total_length,
            RemotingCommand::mark_serialize_type(header_length, serialize_type),
        ))
    }

    #[inline]
    pub fn mark_serialize_type(header_length: i32, protocol_type: SerializeType) -> i32 {
        ((protocol_type.get_code() as i32) << 24) | (header_length & 0x00FFFFFF)
    }
}
