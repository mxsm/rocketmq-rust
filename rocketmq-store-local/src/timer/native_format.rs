// Copyright 2026 The RocketMQ Rust Authors
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

//! On-disk records of the `native` timer engine, a tiered time-bucket log.
//!
//! Everything lives under `timer-native/` in the store root:
//!
//! ```text
//! timer-native/
//!   FORMAT                 lane count and hash algorithm, fixed for the life of the directory
//!   checkpoint.0, .1       A/B checkpoints; the valid one with the higher sequence wins
//!   cancel.log             recalls, appended in Timer CQ order
//!   day/<start_ms>         day buckets: index entries whose payload was copied
//!   hour/<start_ms>        hour buckets: index entries pointing into the CommitLog or at copies
//!   minute/<start_ms>      minute runs: index entries sorted by (due_ms, cq_offset)
//!   payload/<day>/<seg>    payload copies of timers due on that UTC day
//! ```
//!
//! A bucket file is a 64-byte header followed by frames of at most 4 KiB, each holding up to
//! 85 entries of 48 bytes and a CRC32C. Frames are only appended; recovery cuts every file back
//! to the length its checkpoint recorded and replays the Timer CQ from there. Multi-byte fields
//! are big-endian, like the other timer storage records.

mod cancel;
mod checkpoint;
mod entry;
mod frame;
mod layout;

pub use cancel::NativeCancelRecord;
pub use checkpoint::NativeBucketLength;
pub use checkpoint::NativeCheckpoint;
pub use checkpoint::NativeCheckpointSlot;
pub use checkpoint::NativePayloadSegmentLength;
pub use entry::NativeDueCursor;
pub use entry::NativeIndexEntry;
pub use entry::NativeLocator;
pub use entry::NATIVE_INDEX_ENTRY_SIZE;
pub use frame::decode_native_frame;
pub use frame::decode_native_frames;
pub use frame::encode_native_frames;
pub use frame::NativeFileHeader;
pub use frame::NativeFrame;
pub use frame::NativeFrameOrder;
pub use frame::NATIVE_FILE_HEADER_SIZE;
pub use frame::NATIVE_FRAME_HEADER_SIZE;
pub use frame::NATIVE_FRAME_MAX_ENTRIES;
pub use layout::native_key_hash;
pub use layout::native_lane;
pub use layout::NativeBucketId;
pub use layout::NativeBucketKind;
pub use layout::NativeFormat;
pub use layout::NativePayloadSegment;
pub use layout::NATIVE_CANCEL_LOG_NAME;
pub use layout::NATIVE_FORMAT_FILE_NAME;
pub use layout::NATIVE_FORMAT_SIZE;
pub use layout::NATIVE_PAYLOAD_DIR;
pub use layout::NATIVE_TIMER_DIR;
use thiserror::Error;

/// A `native` timer record that cannot be encoded or decoded.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum NativeFormatError {
    #[error("native {record} needs {needed} bytes, found {actual}")]
    Truncated {
        record: &'static str,
        needed: usize,
        actual: usize,
    },
    #[error("native {record} starts with magic {found:#010x}")]
    BadMagic { record: &'static str, found: u32 },
    #[error("native {record} version {found} is not supported")]
    UnsupportedVersion { record: &'static str, found: u16 },
    #[error("native {record} checksum does not match its bytes")]
    ChecksumMismatch { record: &'static str },
    #[error("native {record} has an invalid {field}")]
    InvalidField { record: &'static str, field: &'static str },
}

/// Reads big-endian fields from a record without panicking on short input.
struct FieldReader<'a> {
    record: &'static str,
    bytes: &'a [u8],
    at: usize,
}

impl<'a> FieldReader<'a> {
    fn new(record: &'static str, bytes: &'a [u8]) -> Self {
        Self { record, bytes, at: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], NativeFormatError> {
        let end = self.at.checked_add(length).ok_or(NativeFormatError::Truncated {
            record: self.record,
            needed: usize::MAX,
            actual: self.bytes.len(),
        })?;
        let taken = self.bytes.get(self.at..end).ok_or(NativeFormatError::Truncated {
            record: self.record,
            needed: end,
            actual: self.bytes.len(),
        })?;
        self.at = end;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], NativeFormatError> {
        let mut array = [0u8; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    fn u8(&mut self) -> Result<u8, NativeFormatError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, NativeFormatError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, NativeFormatError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn i32(&mut self) -> Result<i32, NativeFormatError> {
        Ok(i32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, NativeFormatError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, NativeFormatError> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    fn position(&self) -> usize {
        self.at
    }
}

/// Splits `bytes` into its body and the big-endian CRC32C trailer, and checks the trailer.
fn checked_body<'a>(record: &'static str, bytes: &'a [u8]) -> Result<&'a [u8], NativeFormatError> {
    let body_len = bytes.len().checked_sub(4).ok_or(NativeFormatError::Truncated {
        record,
        needed: 4,
        actual: bytes.len(),
    })?;
    let (body, trailer) = bytes.split_at(body_len);
    let mut stored = [0u8; 4];
    stored.copy_from_slice(trailer);
    if crate::timer::storage_format::crc32c(body) != u32::from_be_bytes(stored) {
        return Err(NativeFormatError::ChecksumMismatch { record });
    }
    Ok(body)
}

/// Appends the CRC32C of everything in `output` so far.
fn push_checksum(output: &mut Vec<u8>) {
    let checksum = crate::timer::storage_format::crc32c(output);
    output.extend_from_slice(&checksum.to_be_bytes());
}
