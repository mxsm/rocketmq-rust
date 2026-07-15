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

use super::record::BLANK_MAGIC_CODE;

const END_FILE_MIN_BLANK_LENGTH: i32 = 8;
/// Exact number of bytes physically written for a commit-log blank marker.
pub const BLANK_MARKER_LENGTH: usize = END_FILE_MIN_BLANK_LENGTH as usize;
const QUEUE_OFFSET_POSITION: usize = 20;
const PHYSICAL_OFFSET_POSITION: usize = 28;
const IPV4_STORE_TIMESTAMP_POSITION: usize = 56;
const IPV6_STORE_TIMESTAMP_POSITION: usize = 68;

/// Width of a host address encoded in a commit-log frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostWidth {
    /// Four-byte IP address followed by a four-byte port.
    Ipv4,
    /// Sixteen-byte IP address followed by a four-byte port.
    Ipv6,
}

impl HostWidth {
    fn store_timestamp_position(self) -> usize {
        match self {
            Self::Ipv4 => IPV4_STORE_TIMESTAMP_POSITION,
            Self::Ipv6 => IPV6_STORE_TIMESTAMP_POSITION,
        }
    }
}

/// CRC work that the Store adapter must perform after frame finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendFrameCrcPlan {
    /// The legacy path performs no property CRC update.
    Disabled,
    /// Calculate CRC over `[..covered_end]` and write it to the trailer range.
    Trailer {
        /// Exclusive end of the CRC-covered bytes.
        covered_end: usize,
        /// Inclusive start of the reserved CRC trailer.
        trailer_start: usize,
        /// Exclusive end of the reserved CRC trailer.
        trailer_end: usize,
    },
}

/// The eight-byte marker written before rolling to the next commit-log segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlankMarker {
    bytes: [u8; BLANK_MARKER_LENGTH],
    declared_wrote_bytes: i32,
}

impl BlankMarker {
    /// Returns the exact marker bytes written to the mapped file.
    pub fn bytes(&self) -> &[u8; BLANK_MARKER_LENGTH] {
        &self.bytes
    }

    /// Returns the legacy logical byte count used to advance the segment.
    pub fn declared_wrote_bytes(&self) -> i32 {
        self.declared_wrote_bytes
    }
}

/// Result of deciding whether an encoded frame fits the current segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentAppendDecision {
    /// Append the frame to the current segment.
    Append,
    /// Write a blank marker and roll to the next segment.
    Roll,
}

/// Pure commit-log append-frame algorithms owned by the Local storage crate.
pub struct AppendFrameKernel;

impl AppendFrameKernel {
    /// Decides whether a frame fits, preserving the legacy strict `>` comparison.
    ///
    /// # Panics
    ///
    /// Panics in overflow-checking builds when the legacy `i32` addition overflows.
    pub fn segment_append_decision(encoded_len: i32, max_blank: i32) -> SegmentAppendDecision {
        if encoded_len + END_FILE_MIN_BLANK_LENGTH > max_blank {
            SegmentAppendDecision::Roll
        } else {
            SegmentAppendDecision::Append
        }
    }

    /// Builds the exact eight-byte big-endian end-of-file marker.
    pub fn blank_marker(max_blank: i32) -> BlankMarker {
        let mut bytes = [0; BLANK_MARKER_LENGTH];
        bytes[0..4].copy_from_slice(&max_blank.to_be_bytes());
        bytes[4..8].copy_from_slice(&BLANK_MAGIC_CODE.to_be_bytes());
        BlankMarker {
            bytes,
            declared_wrote_bytes: max_blank,
        }
    }

    /// Finalizes one standard or zero-copy frame and returns the Store CRC plan.
    ///
    /// # Panics
    ///
    /// Panics when `frame` does not contain the fixed runtime-field offsets for
    /// `born_host_width`, or when the legacy `i32` CRC-range arithmetic overflows.
    pub fn finalize_frame(
        frame: &mut [u8],
        queue_offset: i64,
        physical_offset: i64,
        store_timestamp: i64,
        born_host_width: HostWidth,
        crc_reserved_length: i32,
    ) -> AppendFrameCrcPlan {
        Self::patch_runtime_fields(frame, queue_offset, physical_offset, store_timestamp, born_host_width);
        if crc_reserved_length == 0 {
            AppendFrameCrcPlan::Disabled
        } else {
            let covered_end = (frame.len() as i32 - crc_reserved_length) as usize;
            AppendFrameCrcPlan::Trailer {
                covered_end,
                trailer_start: covered_end,
                trailer_end: frame.len(),
            }
        }
    }

    /// Finalizes one batch frame while preserving the legacy CRC no-op.
    ///
    /// # Panics
    ///
    /// Panics when `frame` does not contain the fixed runtime-field offsets for
    /// `born_host_width`.
    pub fn finalize_batch_frame(
        frame: &mut [u8],
        queue_offset: i64,
        physical_offset: i64,
        store_timestamp: i64,
        born_host_width: HostWidth,
    ) -> AppendFrameCrcPlan {
        Self::patch_runtime_fields(frame, queue_offset, physical_offset, store_timestamp, born_host_width);
        AppendFrameCrcPlan::Disabled
    }

    fn patch_runtime_fields(
        frame: &mut [u8],
        queue_offset: i64,
        physical_offset: i64,
        store_timestamp: i64,
        born_host_width: HostWidth,
    ) {
        frame[QUEUE_OFFSET_POSITION..QUEUE_OFFSET_POSITION + 8].copy_from_slice(&queue_offset.to_be_bytes());
        frame[PHYSICAL_OFFSET_POSITION..PHYSICAL_OFFSET_POSITION + 8].copy_from_slice(&physical_offset.to_be_bytes());
        let timestamp_position = born_host_width.store_timestamp_position();
        frame[timestamp_position..timestamp_position + 8].copy_from_slice(&store_timestamp.to_be_bytes());
    }
}
