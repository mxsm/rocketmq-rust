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

use bytes::BytesMut;

use super::parse_header_length;
use super::parse_serialize_type;
use super::RemotingCommand;
use super::SerializeType;
use crate::rocketmq_serializable::RocketMQSerializable;

mod json;

use json::try_decode_json_header;
use json::try_decode_json_header_bytes;

impl RemotingCommand {
    /// Decodes one command with the historical 16 MiB announced-payload ceiling.
    ///
    /// Transport owners with an explicit total-wire limit should use
    /// [`Self::decode_with_max_frame_bytes`] so inbound and outbound policy stays symmetric.
    #[inline]
    pub fn decode(src: &mut BytesMut) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Self::decode_with_max_frame_bytes(src, 16 * 1024 * 1024 + 4)
    }

    /// Decodes one command bounded by a caller-owned complete wire-frame limit.
    ///
    /// `max_frame_bytes` includes the four-byte announced-length prefix.
    ///
    /// # Errors
    ///
    /// Returns a serialization error for a negative, overflowing, oversized, or malformed frame.
    #[inline]
    pub fn decode_with_max_frame_bytes(
        src: &mut BytesMut,
        max_frame_bytes: usize,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        const FRAME_HEADER_SIZE: usize = 4;
        const SERIALIZE_TYPE_SIZE: usize = 4;
        const MIN_PAYLOAD_SIZE: usize = SERIALIZE_TYPE_SIZE; // Minimum: just serialize_type field

        let available = src.len();

        // Early return if not enough data for frame header
        if available < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Read total size without advancing the buffer (peek)
        let announced_size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]);
        let total_size = usize::try_from(announced_size).map_err(|_| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "remoting_command",
                message: format!("Invalid negative frame size {announced_size}"),
            })
        })?;
        let full_frame_size = total_size.checked_add(FRAME_HEADER_SIZE).ok_or_else(|| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "remoting_command",
                message: format!("Frame size {total_size} overflows the wire envelope"),
            })
        })?;

        if full_frame_size > max_frame_bytes {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: format!("Wire frame size {full_frame_size} exceeds configured limit {max_frame_bytes}"),
                },
            ));
        }

        // Wait for complete frame
        if available < full_frame_size {
            return Ok(None);
        }

        // Now validate minimum total_size (we have the complete frame)
        if total_size < MIN_PAYLOAD_SIZE {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: format!("Invalid total_size {total_size}, minimum required is {MIN_PAYLOAD_SIZE}"),
                },
            ));
        }

        // Extract the complete frame before validating the marked header so
        // malformed complete frames preserve the established consume-on-error
        // behavior.
        let frame_data = src.split_to(full_frame_size);
        let ori_header_length = i32::from_be_bytes([frame_data[4], frame_data[5], frame_data[6], frame_data[7]]);
        let header_length = parse_header_length(ori_header_length);

        // Validate header length
        if header_length > total_size - SERIALIZE_TYPE_SIZE {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: format!("Invalid header length {header_length}, total size {total_size}"),
                },
            ));
        }

        let protocol_type = parse_serialize_type(ori_header_length)?;
        let frame = frame_data.freeze();
        let header_start = FRAME_HEADER_SIZE + SERIALIZE_TYPE_SIZE;
        let header_end = header_start + header_length;
        let mut cmd = match protocol_type {
            SerializeType::ROCKETMQ => {
                RocketMQSerializable::rocket_mq_protocol_decode_bytes(frame.slice(header_start..header_end))?
            }
            SerializeType::JSON => {
                if let Some(cmd) = try_decode_json_header_bytes(frame.slice(header_start..header_end)) {
                    cmd
                } else {
                    // Unsupported JSON shapes retain the Serde/SIMD
                    // compatibility path. Copying is confined to this cold
                    // fallback after consuming the complete frame.
                    let mut header_data = BytesMut::from(&frame[header_start..header_end]);
                    Self::decode_json_header_fallback(&mut header_data, header_length)?
                }
            }
        };
        cmd.set_serialize_type_ref(protocol_type);
        if header_end < frame.len() {
            cmd.set_body_mut_ref(frame.slice(header_end..));
        }
        Ok(Some(cmd))
    }

    #[cold]
    #[inline(never)]
    fn decode_json_header_fallback(
        src: &mut BytesMut,
        _header_length: usize,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        #[cfg(feature = "simd")]
        {
            let mut slice = src.split_to(_header_length).to_vec();
            simd_json::from_slice::<RemotingCommand>(&mut slice).map_err(|error| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "json",
                    message: format!("SIMD JSON deserialization error: {error}"),
                })
            })
        }

        #[cfg(not(feature = "simd"))]
        {
            serde_json::from_slice::<RemotingCommand>(src).map_err(|error| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "json",
                    message: format!("JSON deserialization error: {error}"),
                })
            })
        }
    }

    /// Optimized header decoding with type-based dispatch
    #[inline]
    pub fn header_decode(
        src: &mut BytesMut,
        header_length: usize,
        type_: SerializeType,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match type_ {
            SerializeType::JSON => {
                if let Some(cmd) = try_decode_json_header(src, header_length) {
                    return Ok(Some(cmd.set_serialize_type(SerializeType::JSON)));
                }
                let cmd = Self::decode_json_header_fallback(src, header_length)?;
                Ok(Some(cmd.set_serialize_type(SerializeType::JSON)))
            }
            SerializeType::ROCKETMQ => {
                // Deserialize binary header
                let cmd = RocketMQSerializable::rocket_mq_protocol_decode(src, header_length)?;
                Ok(Some(cmd.set_serialize_type(SerializeType::ROCKETMQ)))
            }
        }
    }
}
