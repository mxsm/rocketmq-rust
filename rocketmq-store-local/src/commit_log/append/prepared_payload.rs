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

//! Immutable CommitLog bytes validated before entering the append sequencer.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::commit_log::append_frame::AppendFrameKernel;
use crate::commit_log::header::HostWidth;

const LENGTH_PREFIX_BYTES: usize = size_of::<i32>();
const SYS_FLAG_POSITION: usize = 36;
const SYS_FLAG_END: usize = SYS_FLAG_POSITION + size_of::<i32>();

/// Whether an encoded payload contains one CommitLog frame or a length-prefixed frame batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedPayloadKind {
    /// Exactly one CommitLog frame.
    Single,
    /// One or more contiguous CommitLog frames.
    Batch,
}

/// Validated immutable metadata for one frame in a [`PreparedPayload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedFrame {
    start: usize,
    len: usize,
    born_host_width: HostWidth,
}

impl PreparedFrame {
    /// Returns this frame's range within the prepared payload.
    #[must_use]
    pub fn range(self) -> Range<usize> {
        self.start..self.start + self.len
    }

    /// Returns this frame's relative start.
    #[must_use]
    pub const fn start(self) -> usize {
        self.start
    }

    /// Returns this frame's validated byte length.
    #[must_use]
    pub const fn len(self) -> usize {
        self.len
    }

    /// Returns whether this descriptor is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    /// Returns the born-host width that selects the Store timestamp position.
    #[must_use]
    pub const fn born_host_width(self) -> HostWidth {
        self.born_host_width
    }
}

/// Immutable, structurally validated CommitLog bytes.
///
/// Stable fields are encoded before this value is created. Queue offsets, physical offsets,
/// Store timestamps, and checksums remain placeholders until a sequenced append is finalized.
#[derive(Debug, Clone)]
pub struct PreparedPayload {
    bytes: Bytes,
    frames: Arc<[PreparedFrame]>,
    message_count: i16,
    kind: PreparedPayloadKind,
    crc_trailer_bytes: usize,
}

impl PreparedPayload {
    /// Validates an encoded single-message frame.
    ///
    /// # Errors
    ///
    /// Returns [`PreparedPayloadError`] when the payload is malformed, contains multiple frames,
    /// or cannot safely accommodate the runtime fields and configured checksum trailer.
    pub fn try_single(bytes: Bytes, crc_trailer_bytes: usize) -> Result<Self, PreparedPayloadError> {
        Self::try_from_encoded(bytes, PreparedPayloadKind::Single, crc_trailer_bytes)
    }

    /// Validates one or more contiguous encoded batch frames.
    ///
    /// # Errors
    ///
    /// Returns [`PreparedPayloadError`] when any frame is malformed, the frame partition is not
    /// exact, or a runtime field/checksum trailer would be out of bounds.
    pub fn try_batch(bytes: Bytes, crc_trailer_bytes: usize) -> Result<Self, PreparedPayloadError> {
        Self::try_from_encoded(bytes, PreparedPayloadKind::Batch, crc_trailer_bytes)
    }

    fn try_from_encoded(
        bytes: Bytes,
        kind: PreparedPayloadKind,
        crc_trailer_bytes: usize,
    ) -> Result<Self, PreparedPayloadError> {
        if bytes.is_empty() {
            return Err(PreparedPayloadError::Empty);
        }
        if bytes.len() > i32::MAX as usize {
            return Err(PreparedPayloadError::PayloadTooLarge { len: bytes.len() });
        }

        let mut frames = Vec::new();
        let mut start = 0usize;
        while start < bytes.len() {
            let prefix_end = start
                .checked_add(LENGTH_PREFIX_BYTES)
                .ok_or(PreparedPayloadError::FrameRangeOverflow { start })?;
            let prefix = bytes
                .get(start..prefix_end)
                .ok_or(PreparedPayloadError::MissingLengthPrefix {
                    start,
                    remaining: bytes.len().saturating_sub(start),
                })?;
            let declared_len = AppendFrameKernel::declared_frame_length(prefix);
            let len = usize::try_from(declared_len)
                .map_err(|_| PreparedPayloadError::NonPositiveFrameLength { start, declared_len })?;
            if len == 0 {
                return Err(PreparedPayloadError::NonPositiveFrameLength { start, declared_len });
            }
            let end = start
                .checked_add(len)
                .ok_or(PreparedPayloadError::FrameRangeOverflow { start })?;
            let frame = bytes.get(start..end).ok_or(PreparedPayloadError::FrameOutOfBounds {
                start,
                declared_len,
                payload_len: bytes.len(),
            })?;
            let sys_flag_bytes = frame
                .get(SYS_FLAG_POSITION..SYS_FLAG_END)
                .ok_or(PreparedPayloadError::FixedHeaderTooShort { start, frame_len: len })?;
            let sys_flag = i32::from_be_bytes(
                sys_flag_bytes
                    .try_into()
                    .map_err(|_| PreparedPayloadError::FixedHeaderTooShort { start, frame_len: len })?,
            );
            let born_host_width = HostWidth::born(sys_flag);
            let timestamp_end = born_host_width
                .store_timestamp_position()
                .checked_add(size_of::<i64>())
                .ok_or(PreparedPayloadError::FrameRangeOverflow { start })?;
            if timestamp_end > len {
                return Err(PreparedPayloadError::FixedHeaderTooShort { start, frame_len: len });
            }
            if crc_trailer_bytes > len {
                return Err(PreparedPayloadError::CrcTrailerOutOfBounds {
                    start,
                    frame_len: len,
                    crc_trailer_bytes,
                });
            }
            frames.push(PreparedFrame {
                start,
                len,
                born_host_width,
            });
            start = end;
        }

        match (kind, frames.len()) {
            (PreparedPayloadKind::Single, 1) | (PreparedPayloadKind::Batch, 1..) => {}
            (PreparedPayloadKind::Single, actual) => {
                return Err(PreparedPayloadError::SingleFrameCount { actual });
            }
            (PreparedPayloadKind::Batch, 0) => return Err(PreparedPayloadError::EmptyBatch),
        }
        let message_count =
            i16::try_from(frames.len()).map_err(|_| PreparedPayloadError::TooManyFrames { actual: frames.len() })?;

        Ok(Self {
            bytes,
            frames: frames.into(),
            message_count,
            kind,
            crc_trailer_bytes,
        })
    }

    /// Returns the immutable stable bytes.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns all validated frame descriptors in FIFO order.
    #[must_use]
    pub fn frames(&self) -> &[PreparedFrame] {
        &self.frames
    }

    /// Returns whether this is a single frame or an encoded frame batch.
    #[must_use]
    pub const fn kind(&self) -> PreparedPayloadKind {
        self.kind
    }

    /// Returns the number of encoded CommitLog frames.
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Returns the validated message count used by ConsumeQueue offset accounting.
    #[must_use]
    pub const fn message_count(&self) -> i16 {
        self.message_count
    }

    /// Returns the exact number of payload bytes retained by the sequencer.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the checksum trailer length reserved in every frame.
    #[must_use]
    pub const fn crc_trailer_bytes(&self) -> usize {
        self.crc_trailer_bytes
    }

    /// Consumes this wrapper and returns the original immutable bytes.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

/// Structural validation failure for encoded CommitLog bytes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PreparedPayloadError {
    /// No frame bytes were supplied.
    #[error("prepared CommitLog payload is empty")]
    Empty,
    /// The aggregate cannot be represented by the CommitLog signed length domain.
    #[error("prepared CommitLog payload length {len} exceeds i32::MAX")]
    PayloadTooLarge { len: usize },
    /// Fewer than four bytes remain for a frame length.
    #[error("frame at byte {start} has only {remaining} bytes for its length prefix")]
    MissingLengthPrefix { start: usize, remaining: usize },
    /// A signed frame length was zero or negative.
    #[error("frame at byte {start} declares non-positive length {declared_len}")]
    NonPositiveFrameLength { start: usize, declared_len: i32 },
    /// A frame length points beyond the immutable payload.
    #[error("frame at byte {start} declares length {declared_len}, outside payload length {payload_len}")]
    FrameOutOfBounds {
        start: usize,
        declared_len: i32,
        payload_len: usize,
    },
    /// Offset arithmetic overflowed while validating the frame partition.
    #[error("frame range starting at byte {start} overflows usize")]
    FrameRangeOverflow { start: usize },
    /// A frame cannot contain all fixed runtime fields selected by its system flags.
    #[error("frame at byte {start} is too short for its fixed header: {frame_len} bytes")]
    FixedHeaderTooShort { start: usize, frame_len: usize },
    /// The configured checksum trailer cannot fit inside a frame.
    #[error("frame at byte {start} has {frame_len} bytes, smaller than checksum trailer {crc_trailer_bytes}")]
    CrcTrailerOutOfBounds {
        start: usize,
        frame_len: usize,
        crc_trailer_bytes: usize,
    },
    /// A single-message payload contained an unexpected frame count.
    #[error("single-message payload must contain exactly one frame, found {actual}")]
    SingleFrameCount { actual: usize },
    /// A batch payload contained no frames.
    #[error("batch payload must contain at least one frame")]
    EmptyBatch,
    /// ConsumeQueue accounting cannot represent the number of encoded frames.
    #[error("prepared CommitLog payload contains {actual} frames, exceeding i16::MAX")]
    TooManyFrames { actual: usize },
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::BytesMut;

    use super::*;

    fn encoded_frame(len: usize, sys_flag: i32) -> Bytes {
        let mut bytes = BytesMut::zeroed(len);
        bytes[..4].copy_from_slice(&(len as i32).to_be_bytes());
        bytes[SYS_FLAG_POSITION..SYS_FLAG_END].copy_from_slice(&sys_flag.to_be_bytes());
        bytes.freeze()
    }

    #[test]
    fn validates_exact_single_and_batch_partitions() {
        let first = encoded_frame(80, 0);
        let second = encoded_frame(96, 0x10);
        let mut batch = BytesMut::with_capacity(first.len() + second.len());
        batch.put(first);
        batch.put(second);

        let prepared = PreparedPayload::try_batch(batch.freeze(), 0).expect("valid batch");

        assert_eq!(prepared.kind(), PreparedPayloadKind::Batch);
        assert_eq!(prepared.frame_count(), 2);
        assert_eq!(prepared.frames()[0].range(), 0..80);
        assert_eq!(prepared.frames()[1].range(), 80..176);
        assert_eq!(prepared.frames()[1].born_host_width(), HostWidth::Ipv6);
    }

    #[test]
    fn rejects_truncated_frame_before_sequencing() {
        let mut frame = encoded_frame(80, 0).to_vec();
        frame[..4].copy_from_slice(&81_i32.to_be_bytes());

        let error = PreparedPayload::try_single(Bytes::from(frame), 0).expect_err("truncated frame");

        assert!(matches!(error, PreparedPayloadError::FrameOutOfBounds { .. }));
    }

    #[test]
    fn rejects_multiple_frames_in_single_payload() {
        let first = encoded_frame(80, 0);
        let second = encoded_frame(80, 0);
        let mut batch = BytesMut::new();
        batch.put(first);
        batch.put(second);

        let error = PreparedPayload::try_single(batch.freeze(), 0).expect_err("multiple frames");

        assert_eq!(error, PreparedPayloadError::SingleFrameCount { actual: 2 });
    }
}
