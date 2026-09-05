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

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::SerializationError;
use serde::de::Error as _;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScopeHandle;
use crate::admission::PartialFramePermit;

/// A decoded command together with the complete frame size retained while processing it.
///
/// The sideband size deliberately lives in the transport layer so it cannot affect the
/// protocol command's wire shape, equality, debug output, or public data model.
pub(crate) struct DecodedCommand {
    pub(crate) command: RemotingCommand,
    pub(crate) retained_frame_bytes: usize,
    pub(crate) partial_frame_permit: Option<PartialFramePermit>,
}

/// Encodes a `RemotingCommand` into a `BytesMut` buffer.
///
/// This method takes a `RemotingCommand` and a mutable reference to a `BytesMut` buffer as
/// parameters. It first encodes the header of the `RemotingCommand` and calculates the lengths of
/// the header and body. It then reserves the necessary space in the `BytesMut` buffer and writes
/// the total length, serialize type, header, and body to the buffer.
///
/// # Arguments
///
/// * `item` - A `RemotingCommand` that is to be encoded.
/// * `dst` - A mutable reference to a `BytesMut` buffer where the encoded command will be written.
///
/// # Returns
///
/// * `Result<(), Self::Error>` - Returns `Ok(())` if the encoding is successful, otherwise returns
///   an `Err` with a `RemotingError`.
///
/// # Errors
///
/// This function will return an error if the encoding process fails.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RemotingCommandCodec {
    limits: FrameLimits,
}

/// Symmetric wire limits owned by one remoting endpoint.
///
/// `max_frame_bytes` is the complete wire size, including the four-byte length prefix. This
/// matches Netty's `LengthFieldBasedFrameDecoder` accounting used by RocketMQ Java.
///
/// | Endpoint owner | Profile |
/// | --- | --- |
/// | RocketMQ-compatible client/server | [`Self::java_compatibility`] |
/// | Hardened internal connection | [`Self::default`] |
/// | Tests and benchmarks | An explicit profile owned by the fixture |
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FrameLimits {
    /// Complete frame bytes on the wire, including the four-byte length prefix.
    pub max_frame_bytes: usize,
    /// Serialized command header bytes, excluding the eight-byte wire prefix and marker.
    pub max_header_bytes: usize,
    /// Command body bytes following the serialized header.
    pub max_body_bytes: usize,
    /// Initial decoder buffer allocation; growth remains bounded by the limits above.
    pub initial_read_bytes: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            // Match RocketMQ Java's default frame envelope. The body remains capped at 4 MiB,
            // while this headroom allows an exact-limit body to carry its remoting header.
            max_frame_bytes: 16 * 1024 * 1024,
            max_header_bytes: 1024 * 1024,
            max_body_bytes: 4 * 1024 * 1024,
            initial_read_bytes: 8 * 1024,
        }
    }
}

impl FrameLimits {
    const MIN_FRAME_BYTES: usize = 8;
    const MAX_HEADER_BYTES: usize = 0x00ff_ffff;
    const MAX_INITIAL_READ_BYTES: usize = 1024 * 1024;
    const MAX_PROTOCOL_FRAME_BYTES: usize = i32::MAX as usize + 4;

    /// Builds and validates one endpoint profile.
    ///
    /// # Errors
    ///
    /// Returns a typed argument error when the wire envelope cannot represent the profile or the
    /// initial allocation is outside the safe frame-owned range.
    pub fn try_new(
        max_frame_bytes: usize,
        max_header_bytes: usize,
        max_body_bytes: usize,
        initial_read_bytes: usize,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let limits = Self {
            max_frame_bytes,
            max_header_bytes,
            max_body_bytes,
            initial_read_bytes,
        };
        limits.validate()?;
        Ok(limits)
    }

    /// Validates that this profile is representable and safe to allocate.
    ///
    /// Public fields remain available for source compatibility; every codec and connection entry
    /// point calls this method before using a caller-constructed value.
    pub fn validate(self) -> rocketmq_error::RocketMQResult<()> {
        if !(Self::MIN_FRAME_BYTES..=Self::MAX_PROTOCOL_FRAME_BYTES).contains(&self.max_frame_bytes) {
            return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                "max frame bytes must be between {} and {}",
                Self::MIN_FRAME_BYTES,
                Self::MAX_PROTOCOL_FRAME_BYTES
            )));
        }
        if self.max_header_bytes > Self::MAX_HEADER_BYTES {
            return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                "max header bytes {} exceeds the 24-bit protocol ceiling {}",
                self.max_header_bytes,
                Self::MAX_HEADER_BYTES
            )));
        }
        if !(Self::MIN_FRAME_BYTES..=Self::MAX_INITIAL_READ_BYTES).contains(&self.initial_read_bytes)
            || self.initial_read_bytes > self.max_frame_bytes
        {
            return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                "initial read bytes must be between {} and {} and no larger than max frame bytes",
                Self::MIN_FRAME_BYTES,
                Self::MAX_INITIAL_READ_BYTES
            )));
        }
        Ok(())
    }

    pub(crate) fn safe_initial_read_bytes(self) -> usize {
        self.initial_read_bytes
            .clamp(Self::MIN_FRAME_BYTES, Self::MAX_INITIAL_READ_BYTES)
            .min(self.max_frame_bytes.max(Self::MIN_FRAME_BYTES))
    }

    pub(crate) fn validate_raw_payload(self, payload_len: usize) -> rocketmq_error::RocketMQResult<()> {
        self.validate()?;
        if payload_len > self.max_frame_bytes {
            return Err(encoding_limit_error("raw payload", payload_len, self.max_frame_bytes));
        }
        Ok(())
    }

    pub(crate) fn validate_frame_segments(self, segments: &[Bytes]) -> rocketmq_error::RocketMQResult<usize> {
        self.validate()?;
        let frame_len = checked_frame_segments_len(segments.iter().map(Bytes::len), self.max_frame_bytes)?;
        if frame_len < Self::MIN_FRAME_BYTES {
            return Err(encoding_limit_error("frame", frame_len, Self::MIN_FRAME_BYTES));
        }

        let mut envelope = [0_u8; Self::MIN_FRAME_BYTES];
        let mut copied = 0;
        for segment in segments {
            let count = (envelope.len() - copied).min(segment.len());
            envelope[copied..copied + count].copy_from_slice(&segment[..count]);
            copied += count;
            if copied == envelope.len() {
                break;
            }
        }
        let announced = i32::from_be_bytes(envelope[..4].try_into().expect("four-byte prefix"));
        let announced = usize::try_from(announced)
            .map_err(|_| encoding_limit_error("announced frame", usize::MAX, self.max_frame_bytes))?;
        let announced_wire_len = announced
            .checked_add(4)
            .ok_or_else(|| encoding_limit_error("announced frame", usize::MAX, self.max_frame_bytes))?;
        if announced_wire_len != frame_len {
            return Err(SerializationError::encode_failed(
                "remoting-command",
                format!("announced wire length {announced_wire_len} does not match segmented frame length {frame_len}"),
            )
            .into());
        }

        let header_marker = u32::from_be_bytes(envelope[4..].try_into().expect("four-byte header marker"));
        let header_len = (header_marker & 0x00ff_ffff) as usize;
        let body_len = frame_len
            .checked_sub(Self::MIN_FRAME_BYTES)
            .and_then(|payload| payload.checked_sub(header_len))
            .ok_or_else(|| {
                SerializationError::encode_failed(
                    "remoting-command",
                    format!("header length {header_len} exceeds segmented frame payload"),
                )
            })?;
        self.validate_encoded_lengths(frame_len, header_len, body_len)?;
        Ok(frame_len)
    }

    /// RocketMQ Java's externally compatible Netty frame envelope.
    pub const fn java_compatibility() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_header_bytes: 4 * 1024 * 1024,
            max_body_bytes: 16 * 1024 * 1024,
            initial_read_bytes: 8 * 1024,
        }
    }

    /// Compatibility alias for endpoints that have not adopted the semantic profile name yet.
    pub const fn legacy_compatibility() -> Self {
        Self::java_compatibility()
    }

    pub(crate) fn encode_command(
        self,
        command: RemotingCommand,
    ) -> Result<EncodedFrame, rocketmq_error::RocketMQError> {
        self.validate()?;
        self.validate_command_lower_bounds(&command)?;
        let frame = EncodedFrame::from_command(command)?;
        self.validate_encoded_frame(&frame)?;
        Ok(frame)
    }

    pub(crate) fn encode_frame_head(
        self,
        command: RemotingCommand,
        body_len: usize,
    ) -> Result<rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead, rocketmq_error::RocketMQError> {
        self.validate()?;
        self.validate_command_and_body_lower_bounds(&command, body_len)?;
        let head =
            rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead::from_command_and_body_len(command, body_len)?;
        let [_, header] = head.segments();
        self.validate_encoded_lengths(head.encoded_len(), header.len(), body_len)?;
        Ok(head)
    }

    fn validate_command_lower_bounds(&self, command: &RemotingCommand) -> Result<(), rocketmq_error::RocketMQError> {
        self.validate_command_and_body_lower_bounds(command, command.body().map_or(0, bytes::Bytes::len))
    }

    fn validate_command_and_body_lower_bounds(
        &self,
        command: &RemotingCommand,
        body_len: usize,
    ) -> Result<(), rocketmq_error::RocketMQError> {
        if body_len > self.max_body_bytes {
            return Err(encoding_limit_error("body", body_len, self.max_body_bytes));
        }
        let materialized_header_bytes =
            command
                .remark()
                .map_or(0, |remark| remark.len())
                .saturating_add(command.ext_fields().map_or(0, |fields| {
                    fields
                        .iter()
                        .map(|(key, value)| key.len().saturating_add(value.len()))
                        .fold(0_usize, usize::saturating_add)
                }));
        if materialized_header_bytes > self.max_header_bytes {
            return Err(encoding_limit_error(
                "materialized header fields",
                materialized_header_bytes,
                self.max_header_bytes,
            ));
        }
        let known_wire_bytes = 8_usize
            .checked_add(materialized_header_bytes)
            .and_then(|bytes| bytes.checked_add(body_len))
            .ok_or_else(|| encoding_limit_error("frame", usize::MAX, self.max_frame_bytes))?;
        if known_wire_bytes > self.max_frame_bytes {
            return Err(encoding_limit_error("frame", known_wire_bytes, self.max_frame_bytes));
        }
        Ok(())
    }

    fn validate_encoded_frame(&self, frame: &EncodedFrame) -> Result<(), rocketmq_error::RocketMQError> {
        let [_, header, body] = frame.segments();
        self.validate_encoded_lengths(frame.encoded_len(), header.len(), body.len())
    }

    fn validate_encoded_lengths(
        &self,
        frame_len: usize,
        header_len: usize,
        body_len: usize,
    ) -> Result<(), rocketmq_error::RocketMQError> {
        if header_len > self.max_header_bytes {
            return Err(encoding_limit_error("header", header_len, self.max_header_bytes));
        }
        if body_len > self.max_body_bytes {
            return Err(encoding_limit_error("body", body_len, self.max_body_bytes));
        }
        if frame_len > self.max_frame_bytes {
            return Err(encoding_limit_error("frame", frame_len, self.max_frame_bytes));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
struct DeserializedFrameLimits {
    max_frame_bytes: usize,
    max_header_bytes: usize,
    max_body_bytes: usize,
    initial_read_bytes: usize,
}

impl Default for DeserializedFrameLimits {
    fn default() -> Self {
        let limits = FrameLimits::default();
        Self {
            max_frame_bytes: limits.max_frame_bytes,
            max_header_bytes: limits.max_header_bytes,
            max_body_bytes: limits.max_body_bytes,
            initial_read_bytes: limits.initial_read_bytes,
        }
    }
}

impl<'de> Deserialize<'de> for FrameLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let limits = DeserializedFrameLimits::deserialize(deserializer)?;
        Self::try_new(
            limits.max_frame_bytes,
            limits.max_header_bytes,
            limits.max_body_bytes,
            limits.initial_read_bytes,
        )
        .map_err(D::Error::custom)
    }
}

fn encoding_limit_error(component: &str, actual: usize, limit: usize) -> rocketmq_error::RocketMQError {
    SerializationError::encode_failed(
        "remoting-command",
        format!("encoded {component} is {actual} bytes, exceeding configured limit {limit}"),
    )
    .into()
}

fn checked_frame_segments_len(
    lengths: impl IntoIterator<Item = usize>,
    max_frame_bytes: usize,
) -> rocketmq_error::RocketMQResult<usize> {
    lengths.into_iter().try_fold(0_usize, |total, length| {
        total
            .checked_add(length)
            .ok_or_else(|| encoding_limit_error("frame", usize::MAX, max_frame_bytes))
    })
}

impl Default for RemotingCommandCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RemotingCommandCodec {
    pub fn new() -> Self {
        Self::with_limits(FrameLimits::default())
    }

    pub fn with_limits(limits: FrameLimits) -> Self {
        Self { limits }
    }

    fn decode_with_metadata(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<DecodedCommand>, rocketmq_error::RocketMQError> {
        self.limits.validate()?;
        self.validate_announced_frame(src)?;
        let retained_frame_bytes = if src.len() >= 4 {
            let total = i32::from_be_bytes(src[..4].try_into().expect("four bytes checked"));
            (total > 0).then_some(total as usize + 4)
        } else {
            None
        };
        Ok(
            RemotingCommand::decode_with_max_frame_bytes(src, self.limits.max_frame_bytes)?.map(|command| {
                DecodedCommand {
                    command,
                    retained_frame_bytes: retained_frame_bytes.unwrap_or_default(),
                    partial_frame_permit: None,
                }
            }),
        )
    }
}

impl Decoder for RemotingCommandCodec {
    type Error = rocketmq_error::RocketMQError;
    type Item = RemotingCommand;

    /// Decodes a `RemotingCommand` from a `BytesMut` buffer.
    ///
    /// This method takes a mutable reference to a `BytesMut` buffer as a parameter.
    /// It first checks if there are at least 4 bytes in the buffer, if not, it returns `Ok(None)`.
    /// Then it reads the total size of the incoming data as a big-endian i32 from the first 4
    /// bytes. If the available data is less than the total size, it returns `Ok(None)`.
    /// It then splits the `BytesMut` buffer to get the command data including the total size and
    /// discards the first i32 (total size). It reads the header length as a big-endian i32 and
    /// checks if the header length is greater than the total size minus 4. If it is, it returns
    /// an error. It then splits the buffer again to get the header data and deserializes it
    /// into a `RemotingCommand`. If the total size minus 4 is greater than the header length,
    /// it sets the body of the `RemotingCommand`.
    ///
    /// # Arguments
    ///
    /// * `src` - A mutable reference to a `BytesMut` buffer from which the `RemotingCommand` will
    ///   be decoded.
    ///
    /// # Returns
    ///
    /// * `Result<Option<Self::Item>, Self::Error>` - Returns `Ok(Some(cmd))` if the decoding is
    ///   successful, otherwise returns an `Err` with a `RemotingError`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the decoding process fails.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, rocketmq_error::RocketMQError> {
        self.decode_with_metadata(src)
            .map(|decoded| decoded.map(|decoded| decoded.command))
    }
}

impl RemotingCommandCodec {
    fn validate_announced_frame(&self, src: &BytesMut) -> Result<(), rocketmq_error::RocketMQError> {
        if src.len() < 4 {
            return Ok(());
        }
        let total = i32::from_be_bytes(src[..4].try_into().expect("four bytes checked"));
        let total_wire_bytes = (total > 0)
            .then(|| (total as usize).checked_add(4))
            .flatten()
            .unwrap_or(usize::MAX);
        if total > 0 && total_wire_bytes > self.limits.max_frame_bytes {
            return Err(crate::error_helpers::decoding_error(
                total_wire_bytes,
                self.limits.max_frame_bytes,
            ));
        }
        if src.len() < 8 {
            return Ok(());
        }
        if total < 4 {
            return Err(crate::error_helpers::decoding_error(total.max(0) as usize, 4));
        }
        let header_marker = u32::from_be_bytes(src[4..8].try_into().expect("eight bytes checked"));
        let header = (header_marker & 0x00ff_ffff) as usize;
        let payload = total as usize - 4;
        if header > payload || header > self.limits.max_header_bytes {
            return Err(crate::error_helpers::decoding_error(
                header,
                self.limits.max_header_bytes,
            ));
        }
        let body = payload - header;
        if body > self.limits.max_body_bytes {
            return Err(crate::error_helpers::decoding_error(body, self.limits.max_body_bytes));
        }
        Ok(())
    }
}

impl Encoder<RemotingCommand> for RemotingCommandCodec {
    type Error = rocketmq_error::RocketMQError;

    /// Encodes a `RemotingCommand` into a `BytesMut` buffer.
    ///
    /// This method takes a `RemotingCommand` and a mutable reference to a `BytesMut` buffer as
    /// parameters. It first encodes the header of the `RemotingCommand` and calculates the
    /// lengths of the header and body. It then reserves the necessary space in the `BytesMut`
    /// buffer and writes the total length, serialize type, header, and body to the buffer.
    ///
    /// # Arguments
    ///
    /// * `item` - A `RemotingCommand` that is to be encoded.
    /// * `dst` - A mutable reference to a `BytesMut` buffer where the encoded command will be
    ///   written.
    ///
    /// # Returns
    ///
    /// * `Result<(), Self::Error>` - Returns `Ok(())` if the encoding is successful, otherwise
    ///   returns an `Err` with a `RemotingError`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the encoding process fails.
    fn encode(&mut self, item: RemotingCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.limits.encode_command(item)?.copy_to(dst);
        Ok(())
    }
}

pub(crate) struct SessionCommandDecoder {
    inner: RemotingCommandCodec,
    admission: AdmissionScopeHandle,
    partial_frame_permit: Option<PartialFramePermit>,
}

impl SessionCommandDecoder {
    pub(crate) fn new(inner: RemotingCommandCodec, admission: AdmissionScopeHandle) -> Self {
        Self {
            inner,
            admission,
            partial_frame_permit: None,
        }
    }

    fn reserve_announced_frame(&mut self, src: &BytesMut) -> Result<(), rocketmq_error::RocketMQError> {
        if self.partial_frame_permit.is_some() {
            return Ok(());
        }
        let Some(length_prefix) = src.get(..4) else {
            return Ok(());
        };
        self.inner.validate_announced_frame(src)?;
        let Ok(length_prefix) = <[u8; 4]>::try_from(length_prefix) else {
            return Ok(());
        };
        let total = i32::from_be_bytes(length_prefix);
        if total <= 0 {
            return Ok(());
        }
        let retained_frame_bytes = usize::try_from(total)
            .ok()
            .and_then(|total| total.checked_add(4))
            .ok_or_else(|| crate::error_helpers::decoding_error(usize::MAX, self.inner.limits.max_frame_bytes))?;
        let permit = self
            .admission
            .try_acquire(
                AdmissionResource::PartialFrame,
                retained_frame_bytes,
                AdmissionClass::Data,
            )
            .map_err(|_| {
                crate::error_helpers::network(crate::error_helpers::admission_queue_saturated(
                    "partial-frame-admission",
                ))
            })?;
        self.partial_frame_permit = Some(PartialFramePermit::new(permit));
        Ok(())
    }
}

impl Decoder for SessionCommandDecoder {
    type Error = rocketmq_error::RocketMQError;
    type Item = DecodedCommand;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Err(error) = self.reserve_announced_frame(src) {
            self.partial_frame_permit.take();
            return Err(error);
        }
        match self.inner.decode_with_metadata(src) {
            Ok(Some(mut decoded)) => {
                decoded.partial_frame_permit = self.partial_frame_permit.take();
                Ok(Some(decoded))
            }
            Ok(None) => Ok(None),
            Err(error) => {
                self.partial_frame_permit.take();
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_protocol::protocol::LanguageCode;

    #[tokio::test]
    async fn decode_handles_insufficient_data() {
        let mut decoder = RemotingCommandCodec::new();
        let mut src = BytesMut::from(&[0, 0, 0, 1][..]);
        assert!(matches!(decoder.decode(&mut src), Ok(None)));
    }

    #[tokio::test]
    async fn decode_handles_invalid_total_size() {
        let mut decoder = RemotingCommandCodec::new();
        // total_size = 1, which is less than minimum required (4 bytes for serialize_type)
        let mut src = BytesMut::from(&[0, 0, 0, 1, 0, 0, 0, 0][..]);
        assert!(decoder.decode(&mut src).is_err());
    }

    #[tokio::test]
    async fn encode_handles_empty_body() {
        let mut encoder = RemotingCommandCodec::new();
        let mut dst = BytesMut::new();
        let command = RemotingCommand::create_remoting_command(1)
            .set_code(1)
            .set_language(LanguageCode::JAVA)
            .set_opaque(1)
            .set_flag(1)
            .set_command_custom_header(GetRouteInfoRequestHeader::new("1111", Some(true)))
            .set_remark_option(Some("remark".to_string()));
        assert!(encoder.encode(command, &mut dst).is_ok());
    }

    #[tokio::test]
    async fn encode_handles_non_empty_body() {
        let mut encoder = RemotingCommandCodec::new();
        let mut dst = BytesMut::new();
        let command = RemotingCommand::create_remoting_command(1)
            .set_code(1)
            .set_language(LanguageCode::JAVA)
            .set_opaque(1)
            .set_flag(1)
            .set_body(Bytes::from("body"))
            .set_command_custom_header(GetRouteInfoRequestHeader::new("1111", Some(true)))
            .set_remark_option(Some("remark".to_string()));
        assert!(encoder.encode(command, &mut dst).is_ok());
    }

    #[test]
    fn checked_frame_segments_len_rejects_aggregate_overflow() {
        assert!(checked_frame_segments_len([usize::MAX, 1], usize::MAX).is_err());
    }
}
