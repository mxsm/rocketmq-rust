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

//! Runtime-field binding for an already validated CommitLog payload.

use crate::commit_log::append_frame::AppendFrameCrcPlan;
use crate::commit_log::append_frame::AppendFrameKernel;

use super::prepared_payload::PreparedPayload;
use super::prepared_payload::PreparedPayloadKind;

/// Per-frame runtime fields calculated before mapped-file reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FinalizedFrame {
    queue_offset: i64,
    physical_offset: i64,
}

/// A prepared payload bound to its final queue/physical offsets and Store timestamp.
///
/// Construction performs every fallible range and offset calculation. [`Self::write_into`] then
/// executes only the stable-byte copy, fixed-field patches, and caller-provided checksum write.
pub struct FinalizedAppend<'a> {
    prepared: &'a PreparedPayload,
    frames: Vec<FinalizedFrame>,
    store_timestamp: i64,
}

impl<'a> FinalizedAppend<'a> {
    /// Binds runtime fields for all frames without touching mapped memory.
    ///
    /// Batch frame queue offsets advance by one in encoded order. Physical offsets advance by each
    /// frame's relative byte start.
    ///
    /// # Errors
    ///
    /// Returns [`FinalizeAppendError`] when queue or physical offset arithmetic overflows.
    pub fn try_new(
        prepared: &'a PreparedPayload,
        first_queue_offset: i64,
        first_physical_offset: i64,
        store_timestamp: i64,
    ) -> Result<Self, FinalizeAppendError> {
        let mut frames = Vec::with_capacity(prepared.frame_count());
        for (index, prepared_frame) in prepared.frames().iter().copied().enumerate() {
            let relative_physical_offset =
                i64::try_from(prepared_frame.start()).map_err(|_| FinalizeAppendError::PhysicalOffsetOverflow {
                    first_physical_offset,
                    frame_start: prepared_frame.start(),
                })?;
            let physical_offset = first_physical_offset.checked_add(relative_physical_offset).ok_or(
                FinalizeAppendError::PhysicalOffsetOverflow {
                    first_physical_offset,
                    frame_start: prepared_frame.start(),
                },
            )?;
            let queue_offset =
                match prepared.kind() {
                    PreparedPayloadKind::Single => first_queue_offset,
                    PreparedPayloadKind::Batch => first_queue_offset.checked_add(index as i64).ok_or(
                        FinalizeAppendError::QueueOffsetOverflow {
                            first_queue_offset,
                            frame_index: index,
                        },
                    )?,
                };
            frames.push(FinalizedFrame {
                queue_offset,
                physical_offset,
            });
        }
        Ok(Self {
            prepared,
            frames,
            store_timestamp,
        })
    }

    /// Returns the exact destination bytes required.
    #[must_use]
    pub const fn required_bytes(&self) -> usize {
        self.prepared.retained_bytes()
    }

    /// Returns final physical offsets in encoded frame order.
    #[must_use]
    pub fn physical_offsets(&self) -> impl ExactSizeIterator<Item = i64> + '_ {
        self.frames.iter().map(|frame| frame.physical_offset)
    }

    /// Copies and patches the payload into an exclusive mapped-file staging buffer.
    ///
    /// The checksum callback is invoked once per frame when a trailer was reserved. It receives
    /// that mutable frame and the range plan selected by [`AppendFrameKernel`].
    ///
    /// # Errors
    ///
    /// Returns [`FinalizeAppendError::DestinationTooSmall`] without modifying the destination when
    /// it cannot hold the complete payload.
    pub fn write_into<F>(&self, destination: &mut [u8], mut finalize_crc: F) -> Result<(), FinalizeAppendError>
    where
        F: FnMut(&mut [u8], AppendFrameCrcPlan),
    {
        if destination.len() < self.required_bytes() {
            return Err(FinalizeAppendError::DestinationTooSmall {
                required: self.required_bytes(),
                available: destination.len(),
            });
        }
        destination[..self.required_bytes()].copy_from_slice(self.prepared.bytes());
        for (prepared_frame, finalized_frame) in self.prepared.frames().iter().copied().zip(self.frames.iter().copied())
        {
            let frame = &mut destination[prepared_frame.range()];
            let crc_plan = AppendFrameKernel::finalize_frame(
                frame,
                finalized_frame.queue_offset,
                finalized_frame.physical_offset,
                self.store_timestamp,
                prepared_frame.born_host_width(),
                self.prepared.crc_trailer_bytes() as i32,
            );
            if !matches!(crc_plan, AppendFrameCrcPlan::Disabled) {
                finalize_crc(frame, crc_plan);
            }
        }
        Ok(())
    }
}

/// Failure to bind or copy final append fields.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FinalizeAppendError {
    /// A batch queue offset cannot be represented.
    #[error("queue offset overflow: first offset {first_queue_offset}, frame index {frame_index}")]
    QueueOffsetOverflow {
        first_queue_offset: i64,
        frame_index: usize,
    },
    /// A frame's physical offset cannot be represented.
    #[error("physical offset overflow: first offset {first_physical_offset}, frame start {frame_start}")]
    PhysicalOffsetOverflow {
        first_physical_offset: i64,
        frame_start: usize,
    },
    /// The caller supplied a staging buffer smaller than the validated payload.
    #[error("append destination has {available} bytes but requires {required}")]
    DestinationTooSmall { required: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;

    use super::*;

    const QUEUE_OFFSET_POSITION: usize = 20;
    const PHYSICAL_OFFSET_POSITION: usize = 28;
    const STORE_TIMESTAMP_POSITION: usize = 56;

    fn frame(len: usize) -> Bytes {
        let mut bytes = BytesMut::zeroed(len);
        bytes[..4].copy_from_slice(&(len as i32).to_be_bytes());
        bytes.freeze()
    }

    fn read_i64(bytes: &[u8], start: usize) -> i64 {
        i64::from_be_bytes(bytes[start..start + 8].try_into().expect("i64 bytes"))
    }

    #[test]
    fn patches_sequential_batch_offsets_and_physical_positions() {
        let first = frame(80);
        let second = frame(88);
        let mut bytes = BytesMut::new();
        bytes.put(first);
        bytes.put(second);
        let prepared = PreparedPayload::try_batch(bytes.freeze(), 0).expect("prepared batch");
        let finalized = FinalizedAppend::try_new(&prepared, 41, 1024, 77).expect("finalized batch");
        let mut destination = vec![0; finalized.required_bytes()];

        finalized
            .write_into(&mut destination, |_, _| unreachable!("CRC disabled"))
            .expect("write");

        assert_eq!(read_i64(&destination, QUEUE_OFFSET_POSITION), 41);
        assert_eq!(read_i64(&destination, PHYSICAL_OFFSET_POSITION), 1024);
        assert_eq!(read_i64(&destination, STORE_TIMESTAMP_POSITION), 77);
        assert_eq!(read_i64(&destination[80..], QUEUE_OFFSET_POSITION), 42);
        assert_eq!(read_i64(&destination[80..], PHYSICAL_OFFSET_POSITION), 1104);
        assert_eq!(finalized.physical_offsets().collect::<Vec<_>>(), vec![1024, 1104]);
    }

    #[test]
    fn rejects_small_destination_before_copying() {
        let prepared = PreparedPayload::try_single(frame(80), 0).expect("prepared");
        let finalized = FinalizedAppend::try_new(&prepared, 1, 2, 3).expect("finalized");
        let mut destination = vec![0xA5; 79];

        let error = finalized
            .write_into(&mut destination, |_, _| {})
            .expect_err("small destination");

        assert_eq!(
            error,
            FinalizeAppendError::DestinationTooSmall {
                required: 80,
                available: 79
            }
        );
        assert!(destination.iter().all(|byte| *byte == 0xA5));
    }
}
