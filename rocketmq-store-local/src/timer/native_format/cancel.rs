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

use super::checked_body;
use super::push_checksum;
use super::FieldReader;
use super::NativeFormatError;

const CANCEL_HEADER_SIZE: usize = 30;

/// One recall in the cancel log: the deadline of the timer it cancels, which scopes it to that
/// timer's hour, its recall key and its generation.
///
/// Encoded as its total length, `due_ms`, `tombstone_cq_offset`, `generation`, the key length
/// and the key, then a CRC32C of all of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeCancelRecord {
    pub due_ms: i64,
    /// The Timer CQ offset of the tombstone that carried the recall.
    pub tombstone_cq_offset: i64,
    /// The generation of the timer the recall cancels.
    pub generation: u64,
    /// The recall key as `TIMER_DEL_UNIQKEY` carries it: the unique key alone,
    /// `<real topic>_<unique key>`, or the topic-aware canonical form.
    pub key: Vec<u8>,
}

impl NativeCancelRecord {
    /// Appends the encoded record to `output`.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError::InvalidField`] for an empty key or one longer than
    /// `u16::MAX` bytes.
    pub fn encode_into(&self, output: &mut Vec<u8>) -> Result<(), NativeFormatError> {
        let key_len =
            u16::try_from(self.key.len())
                .ok()
                .filter(|length| *length > 0)
                .ok_or(NativeFormatError::InvalidField {
                    record: "cancel record",
                    field: "key",
                })?;
        let start = output.len();
        let length = (CANCEL_HEADER_SIZE + self.key.len() + 4) as u32;
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&self.due_ms.to_be_bytes());
        output.extend_from_slice(&self.tombstone_cq_offset.to_be_bytes());
        output.extend_from_slice(&self.generation.to_be_bytes());
        output.extend_from_slice(&key_len.to_be_bytes());
        output.extend_from_slice(&self.key);
        let mut record = output.split_off(start);
        push_checksum(&mut record);
        output.extend_from_slice(&record);
        Ok(())
    }

    /// Decodes the record at the start of `bytes` and returns it with its encoded length.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError`] when the record is cut short or corrupt, or its key does
    /// not fill its length.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), NativeFormatError> {
        let record = "cancel record";
        let mut reader = FieldReader::new(record, bytes);
        let length = reader.u32()? as usize;
        let invalid = |field| NativeFormatError::InvalidField { record, field };
        if length < CANCEL_HEADER_SIZE + 5 {
            return Err(invalid("length"));
        }
        let bytes = bytes.get(..length).ok_or(NativeFormatError::Truncated {
            record,
            needed: length,
            actual: bytes.len(),
        })?;
        checked_body(record, bytes)?;
        let due_ms = reader.i64()?;
        let tombstone_cq_offset = reader.i64()?;
        let generation = reader.u64()?;
        let key_len = usize::from(reader.u16()?);
        if CANCEL_HEADER_SIZE + key_len + 4 != length {
            return Err(invalid("key length"));
        }
        let key = reader.take(key_len)?.to_vec();
        Ok((
            Self {
                due_ms,
                tombstone_cq_offset,
                generation,
                key,
            },
            length,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cancel(key: &[u8]) -> NativeCancelRecord {
        NativeCancelRecord {
            due_ms: 1_800_000_000_000,
            tombstone_cq_offset: 512,
            generation: 2,
            key: key.to_vec(),
        }
    }

    #[test]
    fn cancel_records_follow_each_other_in_the_log() {
        let first = cancel(b"TopicA_7F0000010001");
        let second = cancel(b"7F0000010002");
        let mut log = Vec::new();
        first.encode_into(&mut log).unwrap();
        let first_length = log.len();
        second.encode_into(&mut log).unwrap();
        assert_eq!(first_length, CANCEL_HEADER_SIZE + first.key.len() + 4);
        assert_eq!(&log[20..28], &2u64.to_be_bytes());

        let (decoded, length) = NativeCancelRecord::decode(&log).unwrap();
        assert_eq!((decoded, length), (first.clone(), first_length));
        let (decoded, _) = NativeCancelRecord::decode(&log[length..]).unwrap();
        assert_eq!(decoded, second);
    }

    #[test]
    fn a_cut_or_damaged_cancel_record_is_reported() {
        let mut bytes = Vec::new();
        cancel(b"TopicA_key").encode_into(&mut bytes).unwrap();
        assert!(matches!(
            NativeCancelRecord::decode(&bytes[..bytes.len() - 1]),
            Err(NativeFormatError::Truncated { .. })
        ));
        let mut flipped = bytes.clone();
        flipped[CANCEL_HEADER_SIZE] ^= 1;
        assert_eq!(
            NativeCancelRecord::decode(&flipped),
            Err(NativeFormatError::ChecksumMismatch {
                record: "cancel record"
            })
        );
        assert!(cancel(b"").encode_into(&mut Vec::new()).is_err());
    }
}
