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

//! Versioned progress contracts for rebuildable stores derived from the CommitLog.

use std::error::Error as StdError;
use std::fmt;

/// Current on-disk version of [`DerivedCheckpoint`].
pub const DERIVED_CHECKPOINT_FORMAT_VERSION: u16 = 1;

/// Fixed byte length of the version-one derived checkpoint format.
pub const DERIVED_CHECKPOINT_ENCODED_LEN: usize = 32;

const CHECKPOINT_MAGIC: [u8; 8] = *b"RMQDCUR\0";
const CHECKSUM_OFFSET: usize = DERIVED_CHECKPOINT_ENCODED_LEN - 4;

/// Stable owner of one independently rebuildable derived view.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DerivedEngine {
    ConsumeQueue = 1,
    Index = 2,
    RocksDb = 3,
    Tiered = 4,
    Compaction = 5,
}

impl DerivedEngine {
    /// Returns the stable metadata-file component for this engine.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConsumeQueue => "consume_queue",
            Self::Index => "index",
            Self::RocksDb => "rocksdb",
            Self::Tiered => "tiered",
            Self::Compaction => "compaction",
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::ConsumeQueue),
            2 => Some(Self::Index),
            3 => Some(Self::RocksDb),
            4 => Some(Self::Tiered),
            5 => Some(Self::Compaction),
            _ => None,
        }
    }
}

/// Stable idempotency key for one record read from the authoritative CommitLog.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DerivedRecordId {
    source_epoch: u64,
    physical_offset: u64,
    length: u32,
}

impl DerivedRecordId {
    /// Creates a record identity after validating its half-open physical range.
    ///
    /// # Errors
    ///
    /// Returns [`DerivedRecordIdError`] when `length` is zero or the range overflows.
    pub const fn try_new(source_epoch: u64, physical_offset: u64, length: u32) -> Result<Self, DerivedRecordIdError> {
        if length == 0 {
            return Err(DerivedRecordIdError::EmptyRecord);
        }
        if physical_offset.checked_add(length as u64).is_none() {
            return Err(DerivedRecordIdError::RangeOverflow);
        }
        Ok(Self {
            source_epoch,
            physical_offset,
            length,
        })
    }

    /// Returns the source generation that owns the physical offset namespace.
    pub const fn source_epoch(self) -> u64 {
        self.source_epoch
    }

    /// Returns the inclusive starting physical offset.
    pub const fn physical_offset(self) -> u64 {
        self.physical_offset
    }

    /// Returns the encoded record length.
    pub const fn length(self) -> u32 {
        self.length
    }

    /// Returns the exclusive physical end offset.
    pub const fn end_offset(self) -> u64 {
        self.physical_offset + self.length as u64
    }
}

/// Invalid CommitLog identity supplied to a derived engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedRecordIdError {
    EmptyRecord,
    RangeOverflow,
}

impl fmt::Display for DerivedRecordIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyRecord => "derived record length must be non-zero",
            Self::RangeOverflow => "derived record physical range overflows",
        })
    }
}

impl StdError for DerivedRecordIdError {}

/// Exclusive CommitLog position durably completed by one derived engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DerivedCursor {
    source_epoch: u64,
    next_offset: u64,
}

impl DerivedCursor {
    /// Creates the initial cursor for one CommitLog source epoch.
    pub const fn genesis(source_epoch: u64) -> Self {
        Self {
            source_epoch,
            next_offset: 0,
        }
    }

    /// Restores a cursor whose next record starts at `next_offset`.
    pub const fn restore(source_epoch: u64, next_offset: u64) -> Self {
        Self {
            source_epoch,
            next_offset,
        }
    }

    /// Returns the source generation that owns this offset namespace.
    pub const fn source_epoch(self) -> u64 {
        self.source_epoch
    }

    /// Returns the exclusive durable cursor and next expected record offset.
    pub const fn next_offset(self) -> u64 {
        self.next_offset
    }

    /// Classifies a replayed record without mutating or persisting this cursor.
    ///
    /// # Errors
    ///
    /// Returns [`CursorAdvanceError`] for a different source epoch, a physical gap, or a record
    /// that only partially overlaps the committed prefix.
    pub const fn prepare(self, record: DerivedRecordId) -> Result<CursorAdvanceDisposition, CursorAdvanceError> {
        if record.source_epoch != self.source_epoch {
            return Err(CursorAdvanceError::SourceEpochMismatch {
                expected: self.source_epoch,
                actual: record.source_epoch,
            });
        }

        let record_end = record.end_offset();
        if record_end <= self.next_offset {
            return Ok(CursorAdvanceDisposition::AlreadyCommitted);
        }
        if record.physical_offset < self.next_offset {
            return Err(CursorAdvanceError::PartialOverlap {
                committed: self.next_offset,
                record_start: record.physical_offset,
                record_end,
            });
        }
        if record.physical_offset > self.next_offset {
            return Err(CursorAdvanceError::Gap {
                expected: self.next_offset,
                actual: record.physical_offset,
            });
        }

        Ok(CursorAdvanceDisposition::Advance(CursorAdvance {
            previous: self,
            next: Self::restore(self.source_epoch, record_end),
            record,
        }))
    }
}

/// Result of classifying a replayed record against a durable cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorAdvanceDisposition {
    /// The full record is already within the committed prefix.
    AlreadyCommitted,
    /// The record is exactly contiguous and may be applied and committed.
    Advance(CursorAdvance),
}

/// Prepared contiguous cursor transition. Creating this value does not imply persistence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorAdvance {
    previous: DerivedCursor,
    next: DerivedCursor,
    record: DerivedRecordId,
}

impl CursorAdvance {
    /// Returns the cursor that was durable before this transition.
    pub const fn previous_cursor(self) -> DerivedCursor {
        self.previous
    }

    /// Returns the cursor to publish only after durable metadata commit.
    pub const fn next_cursor(self) -> DerivedCursor {
        self.next
    }

    /// Returns the CommitLog identity covered by this transition.
    pub const fn record(self) -> DerivedRecordId {
        self.record
    }
}

/// Violation of continuous per-engine cursor progression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorAdvanceError {
    SourceEpochMismatch {
        expected: u64,
        actual: u64,
    },
    Gap {
        expected: u64,
        actual: u64,
    },
    PartialOverlap {
        committed: u64,
        record_start: u64,
        record_end: u64,
    },
}

impl fmt::Display for CursorAdvanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceEpochMismatch { expected, actual } => {
                write!(formatter, "source epoch mismatch: expected {expected}, got {actual}")
            }
            Self::Gap { expected, actual } => {
                write!(
                    formatter,
                    "derived cursor gap: expected offset {expected}, got {actual}"
                )
            }
            Self::PartialOverlap {
                committed,
                record_start,
                record_end,
            } => write!(
                formatter,
                "derived record {record_start}..{record_end} partially overlaps committed offset {committed}"
            ),
        }
    }
}

impl StdError for CursorAdvanceError {}

/// Bootstrap representation for migrating an owner that previously stored only an offset.
///
/// Version zero has no source epoch. A caller may upgrade it only while it can prove the owning
/// CommitLog epoch out of band. New writers must persist [`DerivedCheckpoint`] instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LegacyDerivedCursorV0 {
    next_offset: u64,
}

impl LegacyDerivedCursorV0 {
    /// Wraps a previously persisted exclusive physical offset.
    pub const fn new(next_offset: u64) -> Self {
        Self { next_offset }
    }

    /// Returns the legacy exclusive physical offset.
    pub const fn next_offset(self) -> u64 {
        self.next_offset
    }
}

/// Versioned, payload-free durable metadata for one derived engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DerivedCheckpoint {
    engine: DerivedEngine,
    cursor: DerivedCursor,
}

impl DerivedCheckpoint {
    /// Creates the current checkpoint version for one engine.
    pub const fn new(engine: DerivedEngine, cursor: DerivedCursor) -> Self {
        Self { engine, cursor }
    }

    /// Upgrades a proven version-zero offset into the current epoch-aware contract.
    pub const fn upgrade_legacy(engine: DerivedEngine, source_epoch: u64, legacy: LegacyDerivedCursorV0) -> Self {
        Self::new(engine, DerivedCursor::restore(source_epoch, legacy.next_offset))
    }

    /// Returns the durable format version emitted by [`Self::encode`].
    pub const fn format_version(self) -> u16 {
        DERIVED_CHECKPOINT_FORMAT_VERSION
    }

    /// Returns the sole engine allowed to own this metadata.
    pub const fn engine(self) -> DerivedEngine {
        self.engine
    }

    /// Returns the durable cursor contained in this checkpoint.
    pub const fn cursor(self) -> DerivedCursor {
        self.cursor
    }

    /// Encodes the fixed-size version-one checkpoint.
    ///
    /// The format contains magic, version, engine, source epoch, exclusive offset, and CRC32. It
    /// intentionally contains no message payload, topic, queue, or second WAL record.
    pub fn encode(self) -> [u8; DERIVED_CHECKPOINT_ENCODED_LEN] {
        let mut encoded = [0_u8; DERIVED_CHECKPOINT_ENCODED_LEN];
        encoded[..8].copy_from_slice(&CHECKPOINT_MAGIC);
        encoded[8..10].copy_from_slice(&DERIVED_CHECKPOINT_FORMAT_VERSION.to_be_bytes());
        encoded[10] = self.engine as u8;
        encoded[11] = 0;
        encoded[12..20].copy_from_slice(&self.cursor.source_epoch.to_be_bytes());
        encoded[20..28].copy_from_slice(&self.cursor.next_offset.to_be_bytes());
        let checksum = crc32(&encoded[..CHECKSUM_OFFSET]);
        encoded[CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
        encoded
    }

    /// Decodes and validates one fixed-size checkpoint for `expected_engine`.
    ///
    /// # Errors
    ///
    /// Returns [`DerivedCheckpointDecodeError`] for a malformed, corrupted, unsupported, or
    /// cross-engine checkpoint. Callers must fail readiness rather than guessing a cursor.
    pub fn decode(encoded: &[u8], expected_engine: DerivedEngine) -> Result<Self, DerivedCheckpointDecodeError> {
        if encoded.len() != DERIVED_CHECKPOINT_ENCODED_LEN {
            return Err(DerivedCheckpointDecodeError::InvalidLength {
                expected: DERIVED_CHECKPOINT_ENCODED_LEN,
                actual: encoded.len(),
            });
        }
        if encoded[..8] != CHECKPOINT_MAGIC {
            return Err(DerivedCheckpointDecodeError::InvalidMagic);
        }

        let version = u16::from_be_bytes([encoded[8], encoded[9]]);
        if version != DERIVED_CHECKPOINT_FORMAT_VERSION {
            return Err(DerivedCheckpointDecodeError::UnsupportedVersion(version));
        }
        if encoded[11] != 0 {
            return Err(DerivedCheckpointDecodeError::InvalidReservedByte(encoded[11]));
        }

        let engine =
            DerivedEngine::from_code(encoded[10]).ok_or(DerivedCheckpointDecodeError::UnknownEngine(encoded[10]))?;
        if engine != expected_engine {
            return Err(DerivedCheckpointDecodeError::EngineMismatch {
                expected: expected_engine,
                actual: engine,
            });
        }

        let expected_checksum = u32::from_be_bytes([
            encoded[CHECKSUM_OFFSET],
            encoded[CHECKSUM_OFFSET + 1],
            encoded[CHECKSUM_OFFSET + 2],
            encoded[CHECKSUM_OFFSET + 3],
        ]);
        let actual_checksum = crc32(&encoded[..CHECKSUM_OFFSET]);
        if actual_checksum != expected_checksum {
            return Err(DerivedCheckpointDecodeError::ChecksumMismatch);
        }

        let source_epoch = u64::from_be_bytes([
            encoded[12],
            encoded[13],
            encoded[14],
            encoded[15],
            encoded[16],
            encoded[17],
            encoded[18],
            encoded[19],
        ]);
        let next_offset = u64::from_be_bytes([
            encoded[20],
            encoded[21],
            encoded[22],
            encoded[23],
            encoded[24],
            encoded[25],
            encoded[26],
            encoded[27],
        ]);
        Ok(Self::new(engine, DerivedCursor::restore(source_epoch, next_offset)))
    }
}

/// Failure while validating durable derived-progress metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedCheckpointDecodeError {
    InvalidLength {
        expected: usize,
        actual: usize,
    },
    InvalidMagic,
    UnsupportedVersion(u16),
    UnknownEngine(u8),
    EngineMismatch {
        expected: DerivedEngine,
        actual: DerivedEngine,
    },
    InvalidReservedByte(u8),
    ChecksumMismatch,
}

impl fmt::Display for DerivedCheckpointDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, actual } => {
                write!(
                    formatter,
                    "invalid checkpoint length: expected {expected}, got {actual}"
                )
            }
            Self::InvalidMagic => formatter.write_str("invalid derived checkpoint magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported derived checkpoint version {version}")
            }
            Self::UnknownEngine(engine) => write!(formatter, "unknown derived engine code {engine}"),
            Self::EngineMismatch { expected, actual } => write!(
                formatter,
                "derived checkpoint owner mismatch: expected {}, got {}",
                expected.as_str(),
                actual.as_str()
            ),
            Self::InvalidReservedByte(byte) => {
                write!(formatter, "invalid derived checkpoint reserved byte {byte}")
            }
            Self::ChecksumMismatch => formatter.write_str("derived checkpoint checksum mismatch"),
        }
    }
}

impl StdError for DerivedCheckpointDecodeError {}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}
