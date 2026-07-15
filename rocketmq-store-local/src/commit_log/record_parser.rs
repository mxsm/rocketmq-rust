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

//! Runtime-neutral, bounds-checked decoding for one CommitLog record.

use bytes::Bytes;

use super::header::HostWidth;
use super::header::MESSAGE_MAGIC_CODE;
use super::header::MESSAGE_MAGIC_CODE_V2;
use super::record::BLANK_MAGIC_CODE;

/// CommitLog record encoding version selected by its magic code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitLogRecordVersion {
    /// One-byte topic length.
    V1,
    /// Two-byte signed topic length.
    V2,
}

/// Controls whether body bytes are retained and checksum-verified.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitLogRecordBodyMode {
    /// Validate and skip the bounded body bytes.
    Skip,
    /// Retain body bytes without verifying the body checksum.
    Read,
    /// Retain body bytes and verify a non-empty body checksum.
    ReadAndVerify,
}

/// Runtime-neutral checksum port used only for body verification.
pub trait CommitLogRecordChecksum {
    /// Computes the checksum for the supplied body bytes.
    fn checksum(&self, bytes: &[u8]) -> u32;
}

/// Field being decoded when a structural error occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitLogRecordField {
    /// Declared record size.
    TotalSize,
    /// Record magic code.
    MagicCode,
    /// Stored body checksum.
    BodyCrc,
    /// Queue identifier.
    QueueId,
    /// Message flag.
    Flag,
    /// Logical queue offset.
    QueueOffset,
    /// Physical CommitLog offset.
    PhysicalOffset,
    /// System flag bitmap.
    SysFlag,
    /// Producer timestamp.
    BornTimestamp,
    /// Producer host bytes.
    BornHost,
    /// Store timestamp.
    StoreTimestamp,
    /// Store host bytes.
    StoreHost,
    /// Reconsume count.
    ReconsumeTimes,
    /// Prepared transaction offset.
    PreparedTransactionOffset,
    /// Body length prefix.
    BodyLength,
    /// Body bytes.
    Body,
    /// Topic length prefix.
    TopicLength,
    /// Topic bytes.
    Topic,
    /// Properties length prefix.
    PropertiesLength,
    /// Property bytes.
    Properties,
    /// Complete declared record frame.
    Record,
}

/// Structural or body-checksum decoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitLogRecordErrorKind {
    /// A bounded field did not fit in the declared record frame.
    Truncated {
        /// Field whose bytes were unavailable.
        field: CommitLogRecordField,
        /// Number of bytes required by the field.
        needed: usize,
        /// Number of bytes remaining in the bounded frame.
        remaining: usize,
    },
    /// A signed length was negative and therefore rejected before conversion.
    NegativeLength {
        /// Length field that was negative.
        field: CommitLogRecordField,
        /// Original signed value.
        value: i64,
    },
    /// The magic code is neither a supported message nor blank marker.
    IllegalMagic {
        /// Unsupported magic value.
        magic_code: i32,
    },
    /// A requested non-empty body checksum verification failed.
    BodyCrcMismatch {
        /// Checksum computed from the bounded body.
        computed: u32,
        /// Checksum stored in the record header.
        stored: u32,
    },
}

/// Failed record decoding with an optional declared size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitLogRecordError {
    /// Declared size when the four-byte size prefix was available.
    pub declared_size: Option<i32>,
    /// Specific decoding failure.
    pub kind: CommitLogRecordErrorKind,
}

/// Fully decoded, runtime-neutral CommitLog message record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitLogRecord {
    /// Exact frame bounded by `declared_size`.
    pub raw_frame: Bytes,
    /// Size declared in the record prefix.
    pub declared_size: i32,
    /// Size consumed by all decoded fields.
    pub computed_size: i32,
    /// Record encoding version.
    pub version: CommitLogRecordVersion,
    /// Stored body checksum.
    pub body_crc: i32,
    /// Queue identifier.
    pub queue_id: i32,
    /// Message flag.
    pub flag: i32,
    /// Logical queue offset.
    pub queue_offset: i64,
    /// Physical CommitLog offset.
    pub physical_offset: i64,
    /// System flag bitmap.
    pub sys_flag: i32,
    /// Producer timestamp.
    pub born_timestamp: i64,
    /// Raw producer host bytes.
    pub born_host: Bytes,
    /// Store timestamp.
    pub store_timestamp: i64,
    /// Raw store host bytes.
    pub store_host: Bytes,
    /// Reconsume count.
    pub reconsume_times: i32,
    /// Prepared transaction offset.
    pub prepared_transaction_offset: i64,
    /// Decoded body length.
    pub body_len: i32,
    /// Body bytes when requested by the body mode.
    pub body: Option<Bytes>,
    /// Raw topic bytes.
    pub topic: Bytes,
    /// Decoded properties length.
    pub properties_len: i16,
    /// Raw property bytes.
    pub properties: Bytes,
}

impl CommitLogRecord {
    /// Returns whether decoded fields consume exactly the declared frame.
    pub fn has_exact_declared_size(&self) -> bool {
        self.declared_size == self.computed_size
    }
}

/// Successful blank-marker or message decoding result.
#[allow(
    clippy::large_enum_variant,
    reason = "CommitLog decoding is a per-message hot path; retaining the record inline avoids a heap allocation"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitLogRecordOutcome {
    /// End-of-segment blank marker.
    Blank {
        /// Size carried by the marker header.
        declared_size: i32,
    },
    /// Decoded CommitLog message.
    Message(CommitLogRecord),
}

struct RecordReader {
    frame: Bytes,
    index: usize,
    declared_size: i32,
}

impl RecordReader {
    fn new(frame: Bytes, index: usize, declared_size: i32) -> Self {
        Self {
            frame,
            index,
            declared_size,
        }
    }

    fn remaining(&self) -> usize {
        self.frame.len() - self.index
    }

    fn take(&mut self, len: usize, field: CommitLogRecordField) -> Result<Bytes, CommitLogRecordError> {
        let remaining = self.remaining();
        let Some(end) = self.index.checked_add(len) else {
            return Err(self.truncated(field, len, remaining));
        };
        if end > self.frame.len() {
            return Err(self.truncated(field, len, remaining));
        }
        let bytes = self.frame.slice(self.index..end);
        self.index = end;
        Ok(bytes)
    }

    fn truncated(&self, field: CommitLogRecordField, needed: usize, remaining: usize) -> CommitLogRecordError {
        CommitLogRecordError {
            declared_size: Some(self.declared_size),
            kind: CommitLogRecordErrorKind::Truncated {
                field,
                needed,
                remaining,
            },
        }
    }

    fn read_i16(&mut self, field: CommitLogRecordField) -> Result<i16, CommitLogRecordError> {
        let bytes = self.take(2, field)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u8(&mut self, field: CommitLogRecordField) -> Result<u8, CommitLogRecordError> {
        Ok(self.take(1, field)?[0])
    }

    fn read_i32(&mut self, field: CommitLogRecordField) -> Result<i32, CommitLogRecordError> {
        let bytes = self.take(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self, field: CommitLogRecordField) -> Result<i64, CommitLogRecordError> {
        let bytes = self.take(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

fn top_level_i32(input: &Bytes, offset: usize, field: CommitLogRecordField) -> Result<i32, CommitLogRecordError> {
    let remaining = input.len().saturating_sub(offset);
    let Some(end) = offset.checked_add(4) else {
        return Err(CommitLogRecordError {
            declared_size: None,
            kind: CommitLogRecordErrorKind::Truncated {
                field,
                needed: 4,
                remaining,
            },
        });
    };
    let Some(bytes) = input.get(offset..end) else {
        return Err(CommitLogRecordError {
            declared_size: None,
            kind: CommitLogRecordErrorKind::Truncated {
                field,
                needed: 4,
                remaining,
            },
        });
    };
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Decodes one CommitLog record without mutating the input cursor.
///
/// Every field read is bounded by the declared record size. Signed lengths are
/// rejected before conversion, and body verification is delegated to `checksum`.
pub fn decode_commit_log_record<C: CommitLogRecordChecksum>(
    input: &Bytes,
    body_mode: CommitLogRecordBodyMode,
    checksum: &C,
) -> Result<CommitLogRecordOutcome, CommitLogRecordError> {
    let declared_size = top_level_i32(input, 0, CommitLogRecordField::TotalSize)?;
    if declared_size < 8 {
        return Err(CommitLogRecordError {
            declared_size: Some(declared_size),
            kind: CommitLogRecordErrorKind::NegativeLength {
                field: CommitLogRecordField::TotalSize,
                value: i64::from(declared_size),
            },
        });
    }
    let declared_len = declared_size as usize;
    if input.len() < declared_len {
        return Err(CommitLogRecordError {
            declared_size: Some(declared_size),
            kind: CommitLogRecordErrorKind::Truncated {
                field: CommitLogRecordField::Record,
                needed: declared_len,
                remaining: input.len(),
            },
        });
    }
    let magic_code = top_level_i32(input, 4, CommitLogRecordField::MagicCode).map_err(|mut error| {
        error.declared_size = Some(declared_size);
        error
    })?;
    if magic_code == BLANK_MAGIC_CODE {
        return Ok(CommitLogRecordOutcome::Blank { declared_size });
    }
    let version = match magic_code {
        MESSAGE_MAGIC_CODE => CommitLogRecordVersion::V1,
        MESSAGE_MAGIC_CODE_V2 => CommitLogRecordVersion::V2,
        _ => {
            return Err(CommitLogRecordError {
                declared_size: Some(declared_size),
                kind: CommitLogRecordErrorKind::IllegalMagic { magic_code },
            });
        }
    };
    let raw_frame = input.slice(..declared_len);
    let mut reader = RecordReader::new(raw_frame.clone(), 8, declared_size);
    let body_crc = reader.read_i32(CommitLogRecordField::BodyCrc)?;
    let queue_id = reader.read_i32(CommitLogRecordField::QueueId)?;
    let flag = reader.read_i32(CommitLogRecordField::Flag)?;
    let queue_offset = reader.read_i64(CommitLogRecordField::QueueOffset)?;
    let physical_offset = reader.read_i64(CommitLogRecordField::PhysicalOffset)?;
    let sys_flag = reader.read_i32(CommitLogRecordField::SysFlag)?;
    let born_timestamp = reader.read_i64(CommitLogRecordField::BornTimestamp)?;
    let born_host_len = HostWidth::born(sys_flag).encoded_len();
    let born_host = reader.take(born_host_len, CommitLogRecordField::BornHost)?;
    let store_timestamp = reader.read_i64(CommitLogRecordField::StoreTimestamp)?;
    let store_host_len = HostWidth::store(sys_flag).encoded_len();
    let store_host = reader.take(store_host_len, CommitLogRecordField::StoreHost)?;
    let reconsume_times = reader.read_i32(CommitLogRecordField::ReconsumeTimes)?;
    let prepared_transaction_offset = reader.read_i64(CommitLogRecordField::PreparedTransactionOffset)?;
    let body_len = reader.read_i32(CommitLogRecordField::BodyLength)?;
    if body_len < 0 {
        return Err(CommitLogRecordError {
            declared_size: Some(declared_size),
            kind: CommitLogRecordErrorKind::NegativeLength {
                field: CommitLogRecordField::BodyLength,
                value: i64::from(body_len),
            },
        });
    }
    let body_bytes = reader.take(body_len as usize, CommitLogRecordField::Body)?;
    let body = match body_mode {
        CommitLogRecordBodyMode::Skip => None,
        CommitLogRecordBodyMode::Read => Some(body_bytes),
        CommitLogRecordBodyMode::ReadAndVerify => {
            if !body_bytes.is_empty() {
                let computed = checksum.checksum(body_bytes.as_ref());
                let stored = body_crc as u32;
                if computed != stored {
                    return Err(CommitLogRecordError {
                        declared_size: Some(declared_size),
                        kind: CommitLogRecordErrorKind::BodyCrcMismatch { computed, stored },
                    });
                }
            }
            Some(body_bytes)
        }
    };
    let topic_len = match version {
        CommitLogRecordVersion::V1 => usize::from(reader.read_u8(CommitLogRecordField::TopicLength)?),
        CommitLogRecordVersion::V2 => {
            let value = reader.read_i16(CommitLogRecordField::TopicLength)?;
            if value < 0 {
                return Err(CommitLogRecordError {
                    declared_size: Some(declared_size),
                    kind: CommitLogRecordErrorKind::NegativeLength {
                        field: CommitLogRecordField::TopicLength,
                        value: i64::from(value),
                    },
                });
            }
            value as usize
        }
    };
    let topic = reader.take(topic_len, CommitLogRecordField::Topic)?;
    let properties_len = reader.read_i16(CommitLogRecordField::PropertiesLength)?;
    if properties_len < 0 {
        return Err(CommitLogRecordError {
            declared_size: Some(declared_size),
            kind: CommitLogRecordErrorKind::NegativeLength {
                field: CommitLogRecordField::PropertiesLength,
                value: i64::from(properties_len),
            },
        });
    }
    let properties = reader.take(properties_len as usize, CommitLogRecordField::Properties)?;
    let computed_size = reader.index as i32;

    Ok(CommitLogRecordOutcome::Message(CommitLogRecord {
        raw_frame,
        declared_size,
        computed_size,
        version,
        body_crc,
        queue_id,
        flag,
        queue_offset,
        physical_offset,
        sys_flag,
        born_timestamp,
        born_host,
        store_timestamp,
        store_host,
        reconsume_times,
        prepared_transaction_offset,
        body_len,
        body,
        topic,
        properties_len,
        properties,
    }))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;

    use super::super::header::BORN_HOST_V6_FLAG;
    use super::super::header::STORE_HOST_V6_FLAG;
    use super::decode_commit_log_record;
    use super::CommitLogRecordBodyMode;
    use super::CommitLogRecordChecksum;
    use super::CommitLogRecordOutcome;
    use super::CommitLogRecordVersion;
    use super::MESSAGE_MAGIC_CODE;
    use super::MESSAGE_MAGIC_CODE_V2;

    struct SpyChecksum {
        calls: Cell<usize>,
    }

    impl CommitLogRecordChecksum for SpyChecksum {
        fn checksum(&self, _bytes: &[u8]) -> u32 {
            self.calls.set(self.calls.get() + 1);
            7
        }
    }

    fn frame(topic_bytes: &[u8], declared_topic_len: u8) -> Bytes {
        let total_size = 88 + 2 + 1 + topic_bytes.len() + 2;
        let mut bytes = BytesMut::with_capacity(total_size);
        bytes.put_i32(total_size as i32);
        bytes.put_i32(-626843481);
        bytes.put_u32(7);
        bytes.put_i32(0);
        bytes.put_i32(0);
        bytes.put_i64(0);
        bytes.put_i64(0);
        bytes.put_i32(0);
        bytes.put_i64(0);
        bytes.put_bytes(0, 8);
        bytes.put_i64(0);
        bytes.put_bytes(0, 8);
        bytes.put_i32(0);
        bytes.put_i64(0);
        bytes.put_i32(2);
        bytes.put_slice(&[1, 2]);
        bytes.put_u8(declared_topic_len);
        bytes.put_slice(topic_bytes);
        bytes.put_i16(0);
        bytes.freeze()
    }

    fn complete_frame(version: CommitLogRecordVersion, sys_flag: i32) -> Bytes {
        let born_host_len = if sys_flag & BORN_HOST_V6_FLAG == 0 { 8 } else { 20 };
        let store_host_len = if sys_flag & STORE_HOST_V6_FLAG == 0 { 8 } else { 20 };
        let topic = b"topic";
        let properties = b"K\x01V\x02";
        let topic_width = if version == CommitLogRecordVersion::V1 { 1 } else { 2 };
        let total_size = 4
            + 4
            + 4
            + 4
            + 4
            + 8
            + 8
            + 4
            + 8
            + born_host_len
            + 8
            + store_host_len
            + 4
            + 8
            + 4
            + 2
            + topic_width
            + topic.len()
            + 2
            + properties.len();
        let mut bytes = BytesMut::with_capacity(total_size);
        bytes.put_i32(total_size as i32);
        bytes.put_i32(if version == CommitLogRecordVersion::V1 {
            MESSAGE_MAGIC_CODE
        } else {
            MESSAGE_MAGIC_CODE_V2
        });
        bytes.put_u32(7);
        bytes.put_i32(3);
        bytes.put_i32(5);
        bytes.put_i64(11);
        bytes.put_i64(13);
        bytes.put_i32(sys_flag);
        bytes.put_i64(17);
        bytes.put_bytes(0x11, born_host_len);
        bytes.put_i64(19);
        bytes.put_bytes(0x22, store_host_len);
        bytes.put_i32(23);
        bytes.put_i64(29);
        bytes.put_i32(2);
        bytes.put_slice(&[1, 2]);
        if version == CommitLogRecordVersion::V1 {
            bytes.put_u8(topic.len() as u8);
        } else {
            bytes.put_i16(topic.len() as i16);
        }
        bytes.put_slice(topic);
        bytes.put_i16(properties.len() as i16);
        bytes.put_slice(properties);
        bytes.freeze()
    }

    #[test]
    fn checksum_modes_call_zero_or_once() {
        for mode in [CommitLogRecordBodyMode::Skip, CommitLogRecordBodyMode::Read] {
            let checksum = SpyChecksum { calls: Cell::new(0) };
            assert!(decode_commit_log_record(&frame(b"topic", 5), mode, &checksum).is_ok());
            assert_eq!(checksum.calls.get(), 0);
        }

        let checksum = SpyChecksum { calls: Cell::new(0) };
        assert!(
            decode_commit_log_record(&frame(b"topic", 5), CommitLogRecordBodyMode::ReadAndVerify, &checksum,).is_ok()
        );
        assert_eq!(checksum.calls.get(), 1);
    }

    #[test]
    fn checksum_precedes_later_topic_boundary_failure() {
        let checksum = SpyChecksum { calls: Cell::new(0) };
        let result = decode_commit_log_record(&frame(b"x", 5), CommitLogRecordBodyMode::ReadAndVerify, &checksum);

        assert!(result.is_err());
        assert_eq!(checksum.calls.get(), 1);
    }

    #[test]
    fn v1_v2_and_all_host_width_combinations_preserve_raw_dto() {
        for version in [CommitLogRecordVersion::V1, CommitLogRecordVersion::V2] {
            for sys_flag in [
                0,
                BORN_HOST_V6_FLAG,
                STORE_HOST_V6_FLAG,
                BORN_HOST_V6_FLAG | STORE_HOST_V6_FLAG,
            ] {
                let input = complete_frame(version, sys_flag);
                let checksum = SpyChecksum { calls: Cell::new(0) };
                let outcome = decode_commit_log_record(&input, CommitLogRecordBodyMode::Read, &checksum)
                    .expect("complete record should decode");
                let CommitLogRecordOutcome::Message(record) = outcome else {
                    panic!("expected message record");
                };

                assert_eq!(record.version, version);
                assert_eq!(record.sys_flag, sys_flag);
                assert_eq!(record.queue_id, 3);
                assert_eq!(record.flag, 5);
                assert_eq!(record.queue_offset, 11);
                assert_eq!(record.physical_offset, 13);
                assert_eq!(record.born_timestamp, 17);
                assert_eq!(
                    record.born_host.len(),
                    if sys_flag & BORN_HOST_V6_FLAG == 0 { 8 } else { 20 }
                );
                assert_eq!(record.store_timestamp, 19);
                assert_eq!(
                    record.store_host.len(),
                    if sys_flag & STORE_HOST_V6_FLAG == 0 { 8 } else { 20 }
                );
                assert_eq!(record.reconsume_times, 23);
                assert_eq!(record.prepared_transaction_offset, 29);
                assert_eq!(record.body_len, 2);
                assert_eq!(record.body.as_deref(), Some(&[1, 2][..]));
                assert_eq!(record.topic.as_ref(), b"topic");
                assert_eq!(record.properties.as_ref(), b"K\x01V\x02");
                assert!(record.has_exact_declared_size());
                assert_eq!(record.raw_frame, input);
                assert_eq!(checksum.calls.get(), 0);
            }
        }
    }
}
