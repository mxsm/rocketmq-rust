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

use super::FieldReader;
use super::NativeFormatError;

/// Size of one encoded [`NativeIndexEntry`].
pub const NATIVE_INDEX_ENTRY_SIZE: usize = 48;

const LOCATOR_COMMIT_LOG: u8 = 0;
const LOCATOR_PAYLOAD: u8 = 1;

/// A position in delivery order. A lane delivers its timers by `(due_ms, cq_offset)`, and its
/// watermark is the cursor of the last timer it delivered in an unbroken run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NativeDueCursor {
    pub due_ms: i64,
    pub cq_offset: i64,
}

impl NativeDueCursor {
    /// The cursor of a lane that has delivered nothing.
    pub const START: Self = Self {
        due_ms: i64::MIN,
        cq_offset: i64::MIN,
    };
}

/// Where the message of a timer is read from at delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NativeLocator {
    /// The source record in the CommitLog, for timers due within the hour buckets' horizon.
    CommitLog { offset: i64 },
    /// A copy in the payload store's partition of the timer's UTC due day, for timers due
    /// later than the CommitLog keeps its files. A read checks the record against the entry's
    /// `cq_offset`.
    Payload { segment_id: u32, offset: u32 },
}

/// One timer in a bucket file.
///
/// | Bytes | Field |
/// |---|---|
/// | 0..8 | `due_ms`, the original deadline |
/// | 8..16 | `cq_offset`, the Timer CQ offset of the source: its identity, and the order within a millisecond |
/// | 16..24 | locator value: a CommitLog offset, or segment id and offset in the high and low 32 bits |
/// | 24..32 | `generation` |
/// | 32..40 | `key_hash`, see [`super::native_key_hash`] |
/// | 40..44 | `size` of the record the locator points at |
/// | 44..46 | `lane` |
/// | 46 | locator kind: 0 CommitLog, 1 payload copy |
/// | 47 | flags, none defined in version 1 |
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NativeIndexEntry {
    pub due_ms: i64,
    pub cq_offset: i64,
    pub locator: NativeLocator,
    pub generation: u64,
    pub key_hash: u64,
    pub size: i32,
    pub lane: u16,
}

impl NativeIndexEntry {
    /// The entry's position in delivery order.
    pub const fn cursor(&self) -> NativeDueCursor {
        NativeDueCursor {
            due_ms: self.due_ms,
            cq_offset: self.cq_offset,
        }
    }

    /// Appends the encoded entry to `output`.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError::InvalidField`] for a negative deadline, Timer CQ offset or
    /// CommitLog offset, or a size that is not positive.
    pub fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), NativeFormatError> {
        self.validate()?;
        let (kind, locator) = match self.locator {
            NativeLocator::CommitLog { offset } => (LOCATOR_COMMIT_LOG, offset as u64),
            NativeLocator::Payload { segment_id, offset } => {
                (LOCATOR_PAYLOAD, (u64::from(segment_id) << 32) | u64::from(offset))
            }
        };
        output.extend_from_slice(&self.due_ms.to_be_bytes());
        output.extend_from_slice(&self.cq_offset.to_be_bytes());
        output.extend_from_slice(&locator.to_be_bytes());
        output.extend_from_slice(&self.generation.to_be_bytes());
        output.extend_from_slice(&self.key_hash.to_be_bytes());
        output.extend_from_slice(&self.size.to_be_bytes());
        output.extend_from_slice(&self.lane.to_be_bytes());
        output.push(kind);
        output.push(0);
        Ok(())
    }

    /// Decodes the entry at the start of `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError`] when fewer than [`NATIVE_INDEX_ENTRY_SIZE`] bytes remain,
    /// the locator kind or a flag is unknown, or a field is out of range.
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeFormatError> {
        let mut reader = FieldReader::new("index entry", bytes);
        let due_ms = reader.i64()?;
        let cq_offset = reader.i64()?;
        let locator = reader.u64()?;
        let generation = reader.u64()?;
        let key_hash = reader.u64()?;
        let size = reader.i32()?;
        let lane = reader.u16()?;
        let kind = reader.u8()?;
        let flags = reader.u8()?;
        let invalid = |field| NativeFormatError::InvalidField {
            record: "index entry",
            field,
        };
        if flags != 0 {
            return Err(invalid("flags"));
        }
        let locator = match kind {
            LOCATOR_COMMIT_LOG => NativeLocator::CommitLog {
                offset: i64::try_from(locator).map_err(|_| invalid("CommitLog offset"))?,
            },
            LOCATOR_PAYLOAD => NativeLocator::Payload {
                segment_id: (locator >> 32) as u32,
                offset: locator as u32,
            },
            _ => return Err(invalid("locator kind")),
        };
        let entry = Self {
            due_ms,
            cq_offset,
            locator,
            generation,
            key_hash,
            size,
            lane,
        };
        entry.validate()?;
        Ok(entry)
    }

    fn validate(&self) -> Result<(), NativeFormatError> {
        let invalid = |field| NativeFormatError::InvalidField {
            record: "index entry",
            field,
        };
        if self.due_ms < 0 {
            return Err(invalid("deadline"));
        }
        if self.cq_offset < 0 {
            return Err(invalid("Timer CQ offset"));
        }
        if self.size <= 0 {
            return Err(invalid("size"));
        }
        if matches!(self.locator, NativeLocator::CommitLog { offset } if offset < 0) {
            return Err(invalid("CommitLog offset"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(locator: NativeLocator) -> NativeIndexEntry {
        NativeIndexEntry {
            due_ms: 1_700_000_000_250,
            cq_offset: 42,
            locator,
            generation: 3,
            key_hash: 0x0123_4567_89ab_cdef,
            size: 512,
            lane: 7,
        }
    }

    fn encoded(entry: &NativeIndexEntry) -> Vec<u8> {
        let mut bytes = Vec::new();
        entry.encode_into(&mut bytes).unwrap();
        bytes
    }

    #[test]
    fn both_locators_round_trip_in_48_bytes() {
        for locator in [
            NativeLocator::CommitLog { offset: 9_876_543_210 },
            NativeLocator::Payload {
                segment_id: 2,
                offset: 0xFFFF_FF00,
            },
        ] {
            let entry = entry(locator);
            let bytes = encoded(&entry);
            assert_eq!(bytes.len(), NATIVE_INDEX_ENTRY_SIZE);
            assert_eq!(NativeIndexEntry::decode(&bytes), Ok(entry));
        }
        let bytes = encoded(&entry(NativeLocator::Payload {
            segment_id: 2,
            offset: 5,
        }));
        assert_eq!(&bytes[16..24], &((2u64 << 32) | 5).to_be_bytes());
        assert_eq!(bytes[46], 1);
    }

    #[test]
    fn unknown_kinds_flags_and_out_of_range_fields_are_rejected() {
        let valid = encoded(&entry(NativeLocator::CommitLog { offset: 64 }));
        for (at, value, field) in [(46, 9, "locator kind"), (47, 1, "flags")] {
            let mut bytes = valid.clone();
            bytes[at] = value;
            assert_eq!(
                NativeIndexEntry::decode(&bytes),
                Err(NativeFormatError::InvalidField {
                    record: "index entry",
                    field
                })
            );
        }
        assert!(matches!(
            NativeIndexEntry::decode(&valid[..47]),
            Err(NativeFormatError::Truncated { .. })
        ));
        let mut negative = entry(NativeLocator::CommitLog { offset: 64 });
        negative.due_ms = -1;
        assert!(negative.encode_into(&mut Vec::new()).is_err());
        let mut empty = entry(NativeLocator::CommitLog { offset: 64 });
        empty.size = 0;
        assert!(empty.encode_into(&mut Vec::new()).is_err());
    }

    #[test]
    fn delivery_order_is_the_deadline_then_the_timer_cq_offset() {
        let mut first = entry(NativeLocator::CommitLog { offset: 0 });
        let mut second = first;
        second.cq_offset = first.cq_offset + 1;
        assert!(first.cursor() < second.cursor());
        first.due_ms += 1;
        assert!(second.cursor() < first.cursor());
        assert!(NativeDueCursor::START < second.cursor());
    }
}
