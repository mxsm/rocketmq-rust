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

use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;

use thiserror::Error;

pub const TIMER_STORAGE_FORMAT_VERSION: u16 = 2;
pub const TIMER_LOG_RECORD_VERSION: u16 = 2;
pub const TIMER_STORAGE_FORMAT_SIZE: usize = 64;
const TIMER_STORAGE_MAGIC: u32 = 0x544D_5232;

/// Stable logical address of a timer record.
///
/// V2 keeps the V1 40-byte logical address unit even though the physical V2 record is larger.
/// This makes an offline migration preserve every Wheel and `previous_offset` reference.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerLogOffset(u64);

impl TimerLogOffset {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TimerLogOffset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Logical first offset owned by one physical timer-log segment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerSegmentId(u64);

impl TimerSegmentId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Immutable storage-policy fields whose changes require an explicit migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerStorageFingerprint {
    pub precision_ms: u64,
    pub wheel_slots: u64,
    pub segment_size: u64,
    pub page_size: u32,
    pub record_version: u16,
    pub delete_key_mode: u8,
}

impl TimerStorageFingerprint {
    pub fn validate(self, physical_record_size: usize) -> Result<Self, TimerStorageFormatError> {
        if self.precision_ms == 0 || self.wheel_slots == 0 || self.page_size == 0 {
            return Err(TimerStorageFormatError::InvalidPolicy(
                "precision, wheel slots, and page size must be non-zero".into(),
            ));
        }
        if self.record_version != TIMER_LOG_RECORD_VERSION {
            return Err(TimerStorageFormatError::UnsupportedVersion(self.record_version));
        }
        if self.segment_size < physical_record_size as u64 * 2 {
            return Err(TimerStorageFormatError::InvalidPolicy(format!(
                "segment size {} must fit a data record and a blank marker",
                self.segment_size
            )));
        }
        if !self.segment_size.is_multiple_of(physical_record_size as u64)
            || !self.segment_size.is_multiple_of(self.page_size as u64)
        {
            return Err(TimerStorageFormatError::InvalidPolicy(format!(
                "segment size {} must be divisible by record size {} and page size {}",
                self.segment_size, physical_record_size, self.page_size
            )));
        }
        Ok(self)
    }

    pub fn policy_hash(self) -> u64 {
        let mut bytes = [0u8; 39];
        bytes[0..8].copy_from_slice(&self.precision_ms.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.wheel_slots.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.segment_size.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[28..30].copy_from_slice(&self.record_version.to_be_bytes());
        bytes[30] = self.delete_key_mode;
        let first = crc32c(&bytes[..31]) as u64;
        bytes[31..39].copy_from_slice(&first.to_be_bytes());
        (first << 32) | crc32c(&bytes) as u64
    }

    pub fn encode(self) -> [u8; TIMER_STORAGE_FORMAT_SIZE] {
        let mut bytes = [0u8; TIMER_STORAGE_FORMAT_SIZE];
        bytes[0..4].copy_from_slice(&TIMER_STORAGE_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&TIMER_STORAGE_FORMAT_VERSION.to_be_bytes());
        bytes[6..8].copy_from_slice(&(TIMER_STORAGE_FORMAT_SIZE as u16).to_be_bytes());
        bytes[8..16].copy_from_slice(&self.precision_ms.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.wheel_slots.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.segment_size.to_be_bytes());
        bytes[32..36].copy_from_slice(&self.page_size.to_be_bytes());
        bytes[36..38].copy_from_slice(&self.record_version.to_be_bytes());
        bytes[38] = self.delete_key_mode;
        bytes[40..48].copy_from_slice(&self.policy_hash().to_be_bytes());
        let checksum = crc32c(&bytes[..60]);
        bytes[60..64].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TimerStorageFormatError> {
        if bytes.len() != TIMER_STORAGE_FORMAT_SIZE {
            return Err(TimerStorageFormatError::InvalidLength(bytes.len()));
        }
        if read_u32(bytes, 0) != TIMER_STORAGE_MAGIC {
            return Err(TimerStorageFormatError::InvalidMagic);
        }
        let version = read_u16(bytes, 4);
        if version != TIMER_STORAGE_FORMAT_VERSION {
            return Err(TimerStorageFormatError::UnsupportedVersion(version));
        }
        if read_u16(bytes, 6) as usize != TIMER_STORAGE_FORMAT_SIZE {
            return Err(TimerStorageFormatError::InvalidLength(read_u16(bytes, 6) as usize));
        }
        if crc32c(&bytes[..60]) != read_u32(bytes, 60) {
            return Err(TimerStorageFormatError::ChecksumMismatch);
        }
        let fingerprint = Self {
            precision_ms: read_u64(bytes, 8),
            wheel_slots: read_u64(bytes, 16),
            segment_size: read_u64(bytes, 24),
            page_size: read_u32(bytes, 32),
            record_version: read_u16(bytes, 36),
            delete_key_mode: bytes[38],
        };
        if fingerprint.policy_hash() != read_u64(bytes, 40) {
            return Err(TimerStorageFormatError::ChecksumMismatch);
        }
        Ok(fingerprint)
    }

    pub fn load_or_create(self, path: &Path) -> Result<(), TimerStorageFormatError> {
        if path.exists() {
            let mut bytes = Vec::new();
            OpenOptions::new().read(true).open(path)?.read_to_end(&mut bytes)?;
            let stored = Self::decode(&bytes)?;
            if stored != self {
                return Err(TimerStorageFormatError::PolicyMismatch {
                    stored,
                    configured: self,
                });
            }
            return Ok(());
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.write_all(&self.encode())?;
        file.sync_data()?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum TimerStorageFormatError {
    #[error("timer storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("timer storage format has invalid length {0}")]
    InvalidLength(usize),
    #[error("timer storage format magic is invalid")]
    InvalidMagic,
    #[error("timer storage version {0} is unsupported")]
    UnsupportedVersion(u16),
    #[error("timer storage checksum does not match")]
    ChecksumMismatch,
    #[error("timer storage policy is invalid: {0}")]
    InvalidPolicy(String),
    #[error(
        "timer storage policy differs from the durable format; migrate the timer store before changing it: stored={stored:?}, configured={configured:?}"
    )]
    PolicyMismatch {
        stored: TimerStorageFingerprint,
        configured: TimerStorageFingerprint,
    },
}

/// CRC32C (Castagnoli) used by every V2 timer-storage record.
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
        }
    }
    !crc
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(bytes[offset..offset + 2].try_into().expect("fixed u16 field"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed u32 field"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed u64 field"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn fingerprint_round_trips_and_detects_corruption() {
        let fingerprint = TimerStorageFingerprint {
            precision_ms: 1_000,
            wheel_slots: 604_800,
            segment_size: 102_400,
            page_size: 4_096,
            record_version: 2,
            delete_key_mode: 0,
        };
        let encoded = fingerprint.encode();
        assert_eq!(TimerStorageFingerprint::decode(&encoded).unwrap(), fingerprint);
        let mut corrupt = encoded;
        corrupt[12] ^= 1;
        assert!(matches!(
            TimerStorageFingerprint::decode(&corrupt),
            Err(TimerStorageFormatError::ChecksumMismatch)
        ));
    }
}
