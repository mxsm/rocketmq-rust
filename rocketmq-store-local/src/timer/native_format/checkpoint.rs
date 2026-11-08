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
use super::entry::NativeDueCursor;
use super::layout::NativeBucketId;
use super::layout::NativeBucketKind;
use super::push_checksum;
use super::FieldReader;
use super::NativeFormatError;

const CHECKPOINT_MAGIC: u32 = 0x544E_434B;
const CHECKPOINT_VERSION: u16 = 1;
const CHECKPOINT_HEADER_SIZE: usize = 64;
const WATERMARK_SIZE: usize = 16;
const BUCKET_LENGTH_SIZE: usize = 17;
const PAYLOAD_LENGTH_SIZE: usize = 16;

/// How far one bucket file was durable when the checkpoint was taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeBucketLength {
    pub bucket: NativeBucketId,
    pub durable_length: u64,
}

/// How far one payload segment, of the partition of a UTC due day, was durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativePayloadSegmentLength {
    pub due_day_utc: i32,
    pub segment_id: u32,
    pub durable_length: u64,
}

/// Which of the two checkpoint files a checkpoint is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeCheckpointSlot {
    A,
    B,
}

impl NativeCheckpointSlot {
    /// The file this slot is written to.
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::A => "checkpoint.0",
            Self::B => "checkpoint.1",
        }
    }

    /// The slot the next checkpoint goes to, so a torn write leaves this one intact.
    pub const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }
}

/// Everything recovery needs to restart from a group commit.
///
/// Recovery cuts every bucket file, the cancel log and every payload segment back to the
/// lengths recorded here, deletes files the checkpoint does not name, and replays the Timer CQ
/// from `source_cq_offset`. Cutting first makes the replay idempotent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeCheckpoint {
    /// Higher wins between the two slots.
    pub sequence: u64,
    pub written_ms: i64,
    /// The first Timer CQ offset not yet in the checkpointed files.
    pub source_cq_offset: i64,
    /// CommitLog cleanup must keep every file at or after this offset.
    pub commit_log_fence: i64,
    pub cancel_log_length: u64,
    /// One cursor per lane, in lane order: the last timer the lane delivered in an unbroken run.
    pub lane_watermarks: Vec<NativeDueCursor>,
    pub buckets: Vec<NativeBucketLength>,
    pub payload_segments: Vec<NativePayloadSegmentLength>,
}

impl NativeCheckpoint {
    /// Encodes the checkpoint: a 64-byte header, the lane watermarks, bucket lengths and payload
    /// lengths, and a CRC32C of all of it.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError::InvalidField`] when there are no lanes, or more lanes,
    /// buckets or payload segments than the counts can hold.
    pub fn encode(&self) -> Result<Vec<u8>, NativeFormatError> {
        let invalid = |field| NativeFormatError::InvalidField {
            record: "checkpoint",
            field,
        };
        let lanes = u16::try_from(self.lane_watermarks.len())
            .ok()
            .filter(|lanes| *lanes > 0)
            .ok_or(invalid("lane count"))?;
        let buckets = u32::try_from(self.buckets.len()).map_err(|_| invalid("bucket count"))?;
        let payloads = u32::try_from(self.payload_segments.len()).map_err(|_| invalid("payload segment count"))?;
        let length = CHECKPOINT_HEADER_SIZE
            + self.lane_watermarks.len() * WATERMARK_SIZE
            + self.buckets.len() * BUCKET_LENGTH_SIZE
            + self.payload_segments.len() * PAYLOAD_LENGTH_SIZE
            + 4;
        let length = u32::try_from(length).map_err(|_| invalid("length"))?;

        let mut output = Vec::with_capacity(length as usize);
        output.extend_from_slice(&CHECKPOINT_MAGIC.to_be_bytes());
        output.extend_from_slice(&CHECKPOINT_VERSION.to_be_bytes());
        output.extend_from_slice(&(CHECKPOINT_HEADER_SIZE as u16).to_be_bytes());
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&lanes.to_be_bytes());
        output.resize(16, 0);
        output.extend_from_slice(&self.sequence.to_be_bytes());
        output.extend_from_slice(&self.written_ms.to_be_bytes());
        output.extend_from_slice(&self.source_cq_offset.to_be_bytes());
        output.extend_from_slice(&self.commit_log_fence.to_be_bytes());
        output.extend_from_slice(&self.cancel_log_length.to_be_bytes());
        output.extend_from_slice(&buckets.to_be_bytes());
        output.extend_from_slice(&payloads.to_be_bytes());
        debug_assert_eq!(output.len(), CHECKPOINT_HEADER_SIZE);
        for watermark in &self.lane_watermarks {
            output.extend_from_slice(&watermark.due_ms.to_be_bytes());
            output.extend_from_slice(&watermark.cq_offset.to_be_bytes());
        }
        for bucket in &self.buckets {
            output.push(bucket.bucket.kind() as u8);
            output.extend_from_slice(&bucket.bucket.start_ms().to_be_bytes());
            output.extend_from_slice(&bucket.durable_length.to_be_bytes());
        }
        for payload in &self.payload_segments {
            output.extend_from_slice(&payload.due_day_utc.to_be_bytes());
            output.extend_from_slice(&payload.segment_id.to_be_bytes());
            output.extend_from_slice(&payload.durable_length.to_be_bytes());
        }
        push_checksum(&mut output);
        Ok(output)
    }

    /// Decodes a checkpoint file.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError`] when the file is short, torn, of another version, or its
    /// counts disagree with its length.
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeFormatError> {
        let record = "checkpoint";
        let mut reader = FieldReader::new(record, bytes);
        let magic = reader.u32()?;
        if magic != CHECKPOINT_MAGIC {
            return Err(NativeFormatError::BadMagic { record, found: magic });
        }
        let version = reader.u16()?;
        if version != CHECKPOINT_VERSION {
            return Err(NativeFormatError::UnsupportedVersion { record, found: version });
        }
        let invalid = |field| NativeFormatError::InvalidField { record, field };
        if usize::from(reader.u16()?) != CHECKPOINT_HEADER_SIZE {
            return Err(invalid("header size"));
        }
        let length = reader.u32()? as usize;
        let bytes = bytes.get(..length).ok_or(NativeFormatError::Truncated {
            record,
            needed: length,
            actual: bytes.len(),
        })?;
        checked_body(record, bytes)?;
        let lanes = usize::from(reader.u16()?);
        reader.take(16 - reader.position())?;
        let sequence = reader.u64()?;
        let written_ms = reader.i64()?;
        let source_cq_offset = reader.i64()?;
        let commit_log_fence = reader.i64()?;
        let cancel_log_length = reader.u64()?;
        let buckets = reader.u32()? as usize;
        let payloads = reader.u32()? as usize;
        // The counts must fill the checked length exactly, so a count is never trusted for an
        // allocation on its own.
        let expected = lanes
            .checked_mul(WATERMARK_SIZE)
            .zip(buckets.checked_mul(BUCKET_LENGTH_SIZE))
            .zip(payloads.checked_mul(PAYLOAD_LENGTH_SIZE))
            .and_then(|((lanes, buckets), payloads)| {
                CHECKPOINT_HEADER_SIZE
                    .checked_add(lanes)?
                    .checked_add(buckets)?
                    .checked_add(payloads)?
                    .checked_add(4)
            });
        if lanes == 0 || expected != Some(length) {
            return Err(invalid("counts"));
        }
        let mut lane_watermarks = Vec::with_capacity(lanes);
        for _ in 0..lanes {
            lane_watermarks.push(NativeDueCursor {
                due_ms: reader.i64()?,
                cq_offset: reader.i64()?,
            });
        }
        let mut bucket_lengths = Vec::with_capacity(buckets);
        for _ in 0..buckets {
            let kind = NativeBucketKind::from_code(reader.u8()?).ok_or(invalid("bucket kind"))?;
            let bucket = NativeBucketId::starting_at(kind, reader.i64()?).ok_or(invalid("bucket window"))?;
            bucket_lengths.push(NativeBucketLength {
                bucket,
                durable_length: reader.u64()?,
            });
        }
        let mut payload_segments = Vec::with_capacity(payloads);
        for _ in 0..payloads {
            payload_segments.push(NativePayloadSegmentLength {
                due_day_utc: reader.i32()?,
                segment_id: reader.u32()?,
                durable_length: reader.u64()?,
            });
        }
        Ok(Self {
            sequence,
            written_ms,
            source_cq_offset,
            commit_log_fence,
            cancel_log_length,
            lane_watermarks,
            buckets: bucket_lengths,
            payload_segments,
        })
    }

    /// Picks the checkpoint to recover from, given the contents of slot A and slot B, `None`
    /// for a slot whose file does not exist. A slot that does not decode is a torn write and
    /// loses to the other.
    ///
    /// Returns `Ok(None)` when neither file exists.
    ///
    /// # Errors
    ///
    /// Returns the error of slot A when files exist but neither decodes, and
    /// [`NativeFormatError::InvalidField`] when both decode with the same sequence.
    pub fn select(
        a: Option<&[u8]>,
        b: Option<&[u8]>,
    ) -> Result<Option<(NativeCheckpointSlot, Self)>, NativeFormatError> {
        let decoded_a = a.map(Self::decode);
        let decoded_b = b.map(Self::decode);
        match (decoded_a, decoded_b) {
            (None, None) => Ok(None),
            (Some(Ok(a)), Some(Ok(b))) => match a.sequence.cmp(&b.sequence) {
                std::cmp::Ordering::Greater => Ok(Some((NativeCheckpointSlot::A, a))),
                std::cmp::Ordering::Less => Ok(Some((NativeCheckpointSlot::B, b))),
                std::cmp::Ordering::Equal => Err(NativeFormatError::InvalidField {
                    record: "checkpoint",
                    field: "sequence",
                }),
            },
            (Some(Ok(a)), _) => Ok(Some((NativeCheckpointSlot::A, a))),
            (_, Some(Ok(b))) => Ok(Some((NativeCheckpointSlot::B, b))),
            (Some(Err(error)), _) | (None, Some(Err(error))) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(sequence: u64) -> NativeCheckpoint {
        NativeCheckpoint {
            sequence,
            written_ms: 1_700_000_000_000,
            source_cq_offset: 1_024,
            commit_log_fence: 4_096,
            cancel_log_length: 300,
            lane_watermarks: vec![
                NativeDueCursor::START,
                NativeDueCursor {
                    due_ms: 1_700_000_000_500,
                    cq_offset: 77,
                },
            ],
            buckets: vec![
                NativeBucketLength {
                    bucket: NativeBucketId::containing(NativeBucketKind::Hour, 1_700_000_000_000).unwrap(),
                    durable_length: 4_160,
                },
                NativeBucketLength {
                    bucket: NativeBucketId::containing(NativeBucketKind::Day, 1_800_000_000_000).unwrap(),
                    durable_length: 64,
                },
            ],
            payload_segments: vec![NativePayloadSegmentLength {
                due_day_utc: 20_833,
                segment_id: 1,
                durable_length: 1 << 20,
            }],
        }
    }

    #[test]
    fn a_checkpoint_round_trips_with_every_list() {
        let original = checkpoint(9);
        let bytes = original.encode().unwrap();
        assert_eq!(bytes.len(), 64 + 2 * 16 + 2 * 17 + 16 + 4);
        assert_eq!(NativeCheckpoint::decode(&bytes), Ok(original));
        // Bytes past the recorded length, such as a longer file's stale tail, are ignored.
        let mut longer = bytes.clone();
        longer.extend_from_slice(&[0xAB; 32]);
        assert_eq!(NativeCheckpoint::decode(&longer), Ok(checkpoint(9)));
    }

    #[test]
    fn a_torn_or_inconsistent_checkpoint_does_not_decode() {
        let bytes = checkpoint(1).encode().unwrap();
        assert!(matches!(
            NativeCheckpoint::decode(&bytes[..bytes.len() - 1]),
            Err(NativeFormatError::Truncated { .. })
        ));
        let mut flipped = bytes.clone();
        flipped[70] ^= 1;
        assert_eq!(
            NativeCheckpoint::decode(&flipped),
            Err(NativeFormatError::ChecksumMismatch { record: "checkpoint" })
        );
        let no_lanes = NativeCheckpoint {
            lane_watermarks: Vec::new(),
            ..checkpoint(1)
        };
        assert!(no_lanes.encode().is_err());
    }

    #[test]
    fn recovery_takes_the_newer_slot_unless_its_write_was_torn() {
        let older = checkpoint(4).encode().unwrap();
        let newer = checkpoint(5).encode().unwrap();
        let torn = &newer[..newer.len() / 2];

        let (slot, chosen) = NativeCheckpoint::select(Some(&older), Some(&newer)).unwrap().unwrap();
        assert_eq!((slot, chosen.sequence), (NativeCheckpointSlot::B, 5));
        assert_eq!(slot.other(), NativeCheckpointSlot::A);
        let (slot, chosen) = NativeCheckpoint::select(Some(&older), Some(torn)).unwrap().unwrap();
        assert_eq!((slot, chosen.sequence), (NativeCheckpointSlot::A, 4));
        let (slot, _) = NativeCheckpoint::select(None, Some(&older)).unwrap().unwrap();
        assert_eq!(slot, NativeCheckpointSlot::B);

        assert_eq!(NativeCheckpoint::select(None, None), Ok(None));
        assert!(NativeCheckpoint::select(Some(torn), None).is_err());
        assert!(NativeCheckpoint::select(Some(&older), Some(&older)).is_err());
    }
}
