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

use std::error::Error as StdError;
use std::fmt;

use bytes::Bytes;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_store_api::DerivedRecordIdError;

use super::owner::CheckpointPersistence;
use super::owner::DerivedCursorOwner;
use super::owner::DerivedCursorOwnerError;
use crate::commit_log::record::is_blank_message;
use crate::commit_log::record::CommitLogFrameCursor;
use crate::commit_log::record::CommitLogFrameSource;
use crate::commit_log::record::FrameCursorStartError;

/// Idempotent application boundary for one derived view.
pub trait DerivedReplaySink {
    type Error: StdError + Send + Sync + 'static;

    /// Applies one frame using `record` as its stable idempotency key.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the derived view is not durably updated. Implementations may
    /// report an error after an uncertain commit; replay will present the same key again.
    fn apply(&mut self, record: DerivedRecordId, frame: &Bytes) -> Result<DerivedReplayApply, Self::Error>;
}

/// Result of applying one replayed record to an idempotent derived view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedReplayApply {
    Applied,
    Duplicate,
}

/// Why a bounded CommitLog replay stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedReplayStop {
    EndOfSource,
    DirtyTail,
    BlankMarker,
}

/// Reproducible summary of one replay pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DerivedReplayReport {
    applied: u64,
    duplicates: u64,
    committed_records: u64,
    committed_offset: u64,
    stop: DerivedReplayStop,
}

impl DerivedReplayReport {
    /// Returns the number of newly applied derived records.
    pub const fn applied(self) -> u64 {
        self.applied
    }

    /// Returns the number of idempotent duplicate applications observed after restart.
    pub const fn duplicates(self) -> u64 {
        self.duplicates
    }

    /// Returns the number of records whose cursor metadata was durably committed in this pass.
    pub const fn committed_records(self) -> u64 {
        self.committed_records
    }

    /// Returns the exclusive durable cursor at the end of this pass.
    pub const fn committed_offset(self) -> u64 {
        self.committed_offset
    }

    /// Returns why replay stopped.
    pub const fn stop(self) -> DerivedReplayStop {
        self.stop
    }
}

/// Replays complete CommitLog frames from the owner's durable cursor.
///
/// The derived effect is applied before cursor persistence. A crash in between may repeat an
/// effect, so `sink` must use [`DerivedRecordId`] for idempotency. A partial tail never advances
/// the cursor.
///
/// # Errors
///
/// Fails closed on an invalid start cursor, unrepresentable record identity, sink error, or cursor
/// persistence error.
pub fn replay_derived<S, P, K>(
    source: S,
    owner: &mut DerivedCursorOwner<P>,
    sink: &mut K,
) -> Result<DerivedReplayReport, DerivedReplayError<K::Error, P::Error>>
where
    S: CommitLogFrameSource,
    P: CheckpointPersistence,
    K: DerivedReplaySink,
{
    let start_offset = usize::try_from(owner.cursor().next_offset())
        .map_err(|_| DerivedReplayError::OffsetNotRepresentable(owner.cursor().next_offset()))?;
    let source_epoch = owner.cursor().source_epoch();
    let mut cursor =
        CommitLogFrameCursor::try_from_offset(source, start_offset).map_err(DerivedReplayError::StartCursor)?;
    let mut applied = 0_u64;
    let mut duplicates = 0_u64;
    let mut committed_records = 0_u64;
    let mut blank_marker = false;

    while let Some((frame, offset, frame_size)) = cursor.next_message() {
        if is_blank_message(&frame) {
            blank_marker = true;
            break;
        }

        let physical_offset =
            u64::try_from(offset).map_err(|_| DerivedReplayError::OffsetNotRepresentable(offset as u64))?;
        let length = u32::try_from(frame_size).map_err(|_| DerivedReplayError::FrameTooLarge(frame_size))?;
        let record =
            DerivedRecordId::try_new(source_epoch, physical_offset, length).map_err(DerivedReplayError::Record)?;
        match sink.apply(record, &frame).map_err(DerivedReplayError::Sink)? {
            DerivedReplayApply::Applied => applied = applied.saturating_add(1),
            DerivedReplayApply::Duplicate => duplicates = duplicates.saturating_add(1),
        }
        owner.commit(record).map_err(DerivedReplayError::Owner)?;
        committed_records = committed_records.saturating_add(1);
    }

    let stop = if blank_marker {
        DerivedReplayStop::BlankMarker
    } else if cursor.has_unconsumed_tail() {
        DerivedReplayStop::DirtyTail
    } else {
        DerivedReplayStop::EndOfSource
    };

    Ok(DerivedReplayReport {
        applied,
        duplicates,
        committed_records,
        committed_offset: owner.cursor().next_offset(),
        stop,
    })
}

/// Failure while replaying the authoritative CommitLog into one derived view.
#[derive(Debug)]
pub enum DerivedReplayError<SinkError, PersistenceError> {
    StartCursor(FrameCursorStartError),
    OffsetNotRepresentable(u64),
    FrameTooLarge(usize),
    Record(DerivedRecordIdError),
    Sink(SinkError),
    Owner(DerivedCursorOwnerError<PersistenceError>),
}

impl<SinkError: fmt::Display, PersistenceError: fmt::Display> fmt::Display
    for DerivedReplayError<SinkError, PersistenceError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StartCursor(error) => write!(formatter, "invalid replay start cursor: {error}"),
            Self::OffsetNotRepresentable(offset) => {
                write!(
                    formatter,
                    "replay offset {offset} is not representable on this platform"
                )
            }
            Self::FrameTooLarge(length) => {
                write!(formatter, "CommitLog frame length {length} exceeds the replay contract")
            }
            Self::Record(error) => write!(formatter, "invalid replay record: {error}"),
            Self::Sink(error) => write!(formatter, "derived replay sink failed: {error}"),
            Self::Owner(error) => write!(formatter, "derived cursor owner failed: {error}"),
        }
    }
}

impl<SinkError, PersistenceError> StdError for DerivedReplayError<SinkError, PersistenceError>
where
    SinkError: StdError + 'static,
    PersistenceError: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::StartCursor(error) => Some(error),
            Self::Record(error) => Some(error),
            Self::Sink(error) => Some(error),
            Self::Owner(error) => Some(error),
            Self::OffsetNotRepresentable(_) | Self::FrameTooLarge(_) => None,
        }
    }
}
