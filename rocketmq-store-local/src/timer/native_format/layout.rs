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

use std::path::PathBuf;

use super::checked_body;
use super::push_checksum;
use super::FieldReader;
use super::NativeFormatError;

/// Directory of the `native` engine under the store root.
pub const NATIVE_TIMER_DIR: &str = "timer-native";
/// File holding the [`NativeFormat`] record.
pub const NATIVE_FORMAT_FILE_NAME: &str = "FORMAT";
/// Append-only log of recalls.
pub const NATIVE_CANCEL_LOG_NAME: &str = "cancel.log";
/// Size of the [`NativeFormat`] record.
pub const NATIVE_FORMAT_SIZE: usize = 64;

const FORMAT_MAGIC: u32 = 0x544E_464D;
const FORMAT_VERSION: u16 = 1;
/// FNV-1a 64 for lanes and recall keys.
const HASH_FNV1A_64: u8 = 1;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x100_0000_01b3;

/// The settings a `native` directory is written with. They cannot change while it holds
/// timers: a lane count change moves timers between lanes, and so between delivery watermarks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFormat {
    pub lane_count: u16,
    pub created_ms: i64,
}

impl NativeFormat {
    /// Encodes the record.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError::InvalidField`] when `lane_count` is zero.
    pub fn encode(&self) -> Result<[u8; NATIVE_FORMAT_SIZE], NativeFormatError> {
        if self.lane_count == 0 {
            return Err(NativeFormatError::InvalidField {
                record: "FORMAT",
                field: "lane count",
            });
        }
        let mut output = Vec::with_capacity(NATIVE_FORMAT_SIZE);
        output.extend_from_slice(&FORMAT_MAGIC.to_be_bytes());
        output.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        output.extend_from_slice(&(NATIVE_FORMAT_SIZE as u16).to_be_bytes());
        output.extend_from_slice(&self.lane_count.to_be_bytes());
        output.push(HASH_FNV1A_64);
        output.resize(16, 0);
        output.extend_from_slice(&self.created_ms.to_be_bytes());
        output.resize(NATIVE_FORMAT_SIZE - 4, 0);
        push_checksum(&mut output);
        let mut record = [0u8; NATIVE_FORMAT_SIZE];
        record.copy_from_slice(&output);
        Ok(record)
    }

    /// Decodes the record.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError`] when `bytes` is not a complete record of a supported
    /// version, or names a hash algorithm or lane count this build cannot use.
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeFormatError> {
        let record = "FORMAT";
        let bytes = bytes.get(..NATIVE_FORMAT_SIZE).ok_or(NativeFormatError::Truncated {
            record,
            needed: NATIVE_FORMAT_SIZE,
            actual: bytes.len(),
        })?;
        let mut reader = FieldReader::new(record, bytes);
        let magic = reader.u32()?;
        if magic != FORMAT_MAGIC {
            return Err(NativeFormatError::BadMagic { record, found: magic });
        }
        let version = reader.u16()?;
        if version != FORMAT_VERSION {
            return Err(NativeFormatError::UnsupportedVersion { record, found: version });
        }
        checked_body(record, bytes)?;
        if usize::from(reader.u16()?) != NATIVE_FORMAT_SIZE {
            return Err(NativeFormatError::InvalidField { record, field: "size" });
        }
        let lane_count = reader.u16()?;
        if lane_count == 0 {
            return Err(NativeFormatError::InvalidField {
                record,
                field: "lane count",
            });
        }
        if reader.u8()? != HASH_FNV1A_64 {
            return Err(NativeFormatError::InvalidField {
                record,
                field: "hash algorithm",
            });
        }
        reader.take(16 - reader.position())?;
        let created_ms = reader.i64()?;
        Ok(Self { lane_count, created_ms })
    }
}

/// The tier a bucket file belongs to, which fixes the width of its time window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum NativeBucketKind {
    /// Entries pointing into the CommitLog, unordered within the hour.
    Hour = 1,
    /// Entries pointing at payload copies, unordered within the day.
    Day = 2,
    /// Entries sorted by `(due_ms, cq_offset)`, written by promotion.
    Minute = 3,
}

impl NativeBucketKind {
    /// Width of one window.
    pub const fn window_ms(self) -> i64 {
        match self {
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
            Self::Minute => 60_000,
        }
    }

    /// Directory of this kind under [`NATIVE_TIMER_DIR`].
    pub const fn dir_name(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Minute => "minute",
        }
    }

    pub(super) fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Hour),
            2 => Some(Self::Day),
            3 => Some(Self::Minute),
            _ => None,
        }
    }
}

/// One bucket file: a tier and the UTC start of its window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NativeBucketId {
    kind: NativeBucketKind,
    start_ms: i64,
}

impl NativeBucketId {
    /// The bucket of `kind` whose window holds `due_ms`.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError::InvalidField`] for a deadline before the Unix epoch.
    pub fn containing(kind: NativeBucketKind, due_ms: i64) -> Result<Self, NativeFormatError> {
        if due_ms < 0 {
            return Err(NativeFormatError::InvalidField {
                record: "bucket",
                field: "deadline",
            });
        }
        Ok(Self {
            kind,
            start_ms: due_ms - due_ms % kind.window_ms(),
        })
    }

    /// The bucket of `kind` starting at `start_ms`, or `None` when no window starts there.
    pub fn starting_at(kind: NativeBucketKind, start_ms: i64) -> Option<Self> {
        (start_ms >= 0 && start_ms % kind.window_ms() == 0).then_some(Self { kind, start_ms })
    }

    /// Parses a file name of `kind`, as [`Self::relative_path`] writes it.
    pub fn parse(kind: NativeBucketKind, file_name: &str) -> Option<Self> {
        if file_name.len() != 20 || !file_name.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        Self::starting_at(kind, file_name.parse().ok()?)
    }

    pub const fn kind(self) -> NativeBucketKind {
        self.kind
    }

    /// First millisecond of the window.
    pub const fn start_ms(self) -> i64 {
        self.start_ms
    }

    /// First millisecond after the window.
    pub const fn end_ms(self) -> i64 {
        self.start_ms.saturating_add(self.kind.window_ms())
    }

    /// Whether `due_ms` falls in the window.
    pub const fn holds(self, due_ms: i64) -> bool {
        due_ms >= self.start_ms && due_ms < self.end_ms()
    }

    /// Path of the bucket file under [`NATIVE_TIMER_DIR`].
    pub fn relative_path(self) -> PathBuf {
        PathBuf::from(self.kind.dir_name()).join(format!("{:020}", self.start_ms))
    }
}

/// FNV-1a 64 of `bytes`.
fn fnv1a64(bytes: impl IntoIterator<Item = u8>) -> u64 {
    bytes.into_iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
    })
}

/// The delivery lane of a real topic queue. Timers of one queue share a lane, so a lane
/// delivers them in `(due_ms, cq_offset)` order. The hash is the one the Extended timeline uses,
/// reduced in 64 bits on every platform.
pub fn native_lane(real_topic: &str, real_queue_id: i32, lane_count: u16) -> u16 {
    let hash = fnv1a64(real_topic.bytes().chain(real_queue_id.to_be_bytes().iter().copied()));
    // The remainder is below `lane_count`, so it fits.
    (hash % u64::from(lane_count.max(1))) as u16
}

/// The hash an index entry carries for recalls to find its timer: of the timer's unique key.
/// Every form of recall key names that unique key, and a match is confirmed against the full
/// recall key and the generation before a timer is cancelled.
pub fn native_key_hash(unique_key: &[u8]) -> u64 {
    fnv1a64(unique_key.iter().copied())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_round_trips_and_rejects_what_it_cannot_use() {
        let format = NativeFormat {
            lane_count: 16,
            created_ms: 1_700_000_000_123,
        };
        let bytes = format.encode().unwrap();
        assert_eq!(NativeFormat::decode(&bytes), Ok(format));

        let mut corrupt = bytes;
        corrupt[20] ^= 1;
        assert_eq!(
            NativeFormat::decode(&corrupt),
            Err(NativeFormatError::ChecksumMismatch { record: "FORMAT" })
        );
        assert!(matches!(
            NativeFormat::decode(&bytes[..63]),
            Err(NativeFormatError::Truncated { needed: 64, .. })
        ));
        assert!(NativeFormat {
            lane_count: 0,
            created_ms: 0
        }
        .encode()
        .is_err());
    }

    #[test]
    fn a_bucket_covers_its_utc_window_and_names_its_file_after_the_start() {
        let hour = NativeBucketId::containing(NativeBucketKind::Hour, 7_200_000 + 59_999).unwrap();
        assert_eq!((hour.start_ms(), hour.end_ms()), (7_200_000, 10_800_000));
        assert!(hour.holds(7_200_000) && !hour.holds(10_800_000));
        assert_eq!(hour.relative_path(), PathBuf::from("hour").join("00000000000007200000"));
        assert_eq!(
            NativeBucketId::parse(NativeBucketKind::Hour, "00000000000007200000"),
            Some(hour)
        );
        // A name that does not start a window, or is not a 20-digit number, is not a bucket.
        assert_eq!(
            NativeBucketId::parse(NativeBucketKind::Hour, "00000000000007200001"),
            None
        );
        assert_eq!(NativeBucketId::parse(NativeBucketKind::Hour, "7200000"), None);

        let day = NativeBucketId::containing(NativeBucketKind::Day, 86_400_000 * 3 + 1).unwrap();
        assert_eq!(day.start_ms(), 86_400_000 * 3);
        let minute = NativeBucketId::containing(NativeBucketKind::Minute, 61_000).unwrap();
        assert_eq!((minute.start_ms(), minute.end_ms()), (60_000, 120_000));
        assert!(NativeBucketId::containing(NativeBucketKind::Day, -1).is_err());
    }

    #[test]
    fn hashes_are_fnv1a_64_on_every_platform() {
        assert_eq!(native_key_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(native_key_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        // The lane hash takes the topic, then the queue id in big-endian order.
        let mut topic_and_queue = b"TopicA".to_vec();
        topic_and_queue.extend_from_slice(&3i32.to_be_bytes());
        assert_eq!(
            u64::from(native_lane("TopicA", 3, 16)),
            native_key_hash(&topic_and_queue) % 16
        );
        assert_eq!(native_lane("TopicA", 3, 1), 0);
    }
}
