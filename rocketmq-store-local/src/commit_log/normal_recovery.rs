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

//! Runtime-neutral per-segment orchestration for normal CommitLog recovery.

use super::recovery::NormalRecoveryAction;
use super::recovery::NormalRecoveryEvent;
use super::recovery::NormalRecoveryOffsetError;
use super::recovery::NormalRecoveryState;

/// One adapter-produced record consumed by normal segment recovery.
#[derive(Debug, PartialEq, Eq)]
pub enum NormalRecoveryRecord<R> {
    /// A validated message frame and its segment-relative layout.
    Message {
        /// Start of the frame relative to its containing segment.
        relative_start: u64,
        /// Validated frame size.
        size: u64,
        /// Adapter-owned record payload.
        record: R,
    },
    /// An end-of-segment blank marker.
    Blank {
        /// Adapter-owned record payload.
        record: R,
    },
    /// An invalid record.
    Invalid {
        /// Frame start relative to the segment when conversion succeeded.
        relative_start: Option<u64>,
        /// Adapter-owned record payload.
        record: R,
    },
    /// The adapter source has no further record in this segment.
    SourceEnded,
}

/// Side-effect observation emitted around normal recovery state transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalRecoveryObservation {
    /// A message was accepted by the offset state machine.
    MessageAccepted,
    /// A blank marker was encountered.
    Blank,
    /// An invalid record was encountered.
    Invalid {
        /// Frame start relative to the segment when conversion succeeded.
        relative_start: Option<u64>,
    },
}

/// Terminal outcome of driving one normal recovery segment.
#[derive(Debug, PartialEq, Eq)]
pub enum NormalRecoverySegmentOutcome<E> {
    /// Continue normal recovery at the next segment.
    ContinueNextSegment,
    /// Stop normal recovery at the current watermarks.
    StopRecovery,
    /// The record adapter failed with its original error.
    AdapterFailed(E),
    /// The offset state machine rejected an event.
    StateFailed(NormalRecoveryOffsetError),
}

impl NormalRecoveryState {
    /// Drives records from one segment until the state selects the next segment or stops recovery.
    ///
    /// The segment-start state transition precedes `on_segment_started`. Message transitions
    /// precede observation, while blank and invalid observations precede their transitions.
    /// `SourceEnded` is never observed. The driver retains ownership of each record payload and
    /// drops it after the observer returns or after a state rejection.
    pub fn drive_segment<R, E, Next, Started, Observe>(
        &mut self,
        segment_base: u64,
        mut next_record: Next,
        on_segment_started: Started,
        mut observe: Observe,
    ) -> NormalRecoverySegmentOutcome<E>
    where
        Next: FnMut() -> Result<NormalRecoveryRecord<R>, E>,
        Started: FnOnce(),
        Observe: FnMut(NormalRecoveryObservation, &mut R),
    {
        let started_action = match self.apply(NormalRecoveryEvent::SegmentStarted {
            base_offset: segment_base,
        }) {
            Ok(action) => action,
            Err(error) => return NormalRecoverySegmentOutcome::StateFailed(error),
        };
        on_segment_started();
        match started_action {
            NormalRecoveryAction::ContinueRecord => {}
            NormalRecoveryAction::ContinueNextSegment => {
                return NormalRecoverySegmentOutcome::ContinueNextSegment;
            }
            NormalRecoveryAction::StopRecovery => return NormalRecoverySegmentOutcome::StopRecovery,
        }

        loop {
            let record = match next_record() {
                Ok(record) => record,
                Err(error) => return NormalRecoverySegmentOutcome::AdapterFailed(error),
            };
            match record {
                NormalRecoveryRecord::Message {
                    relative_start,
                    size,
                    mut record,
                } => {
                    let action = match self.apply(NormalRecoveryEvent::MessageAccepted {
                        segment_base,
                        relative_start,
                        size,
                    }) {
                        Ok(action) => action,
                        Err(error) => return NormalRecoverySegmentOutcome::StateFailed(error),
                    };
                    match action {
                        NormalRecoveryAction::ContinueRecord => {
                            observe(NormalRecoveryObservation::MessageAccepted, &mut record);
                        }
                        NormalRecoveryAction::ContinueNextSegment => {
                            return NormalRecoverySegmentOutcome::ContinueNextSegment;
                        }
                        NormalRecoveryAction::StopRecovery => {
                            return NormalRecoverySegmentOutcome::StopRecovery;
                        }
                    }
                }
                NormalRecoveryRecord::Blank { mut record } => {
                    observe(NormalRecoveryObservation::Blank, &mut record);
                    match self.apply(NormalRecoveryEvent::Blank) {
                        Ok(NormalRecoveryAction::ContinueRecord) => {}
                        Ok(NormalRecoveryAction::ContinueNextSegment) => {
                            return NormalRecoverySegmentOutcome::ContinueNextSegment;
                        }
                        Ok(NormalRecoveryAction::StopRecovery) => {
                            return NormalRecoverySegmentOutcome::StopRecovery;
                        }
                        Err(error) => return NormalRecoverySegmentOutcome::StateFailed(error),
                    }
                }
                NormalRecoveryRecord::Invalid {
                    relative_start,
                    mut record,
                } => {
                    observe(NormalRecoveryObservation::Invalid { relative_start }, &mut record);
                    match self.apply(NormalRecoveryEvent::InvalidRecord) {
                        Ok(NormalRecoveryAction::ContinueRecord) => {}
                        Ok(NormalRecoveryAction::ContinueNextSegment) => {
                            return NormalRecoverySegmentOutcome::ContinueNextSegment;
                        }
                        Ok(NormalRecoveryAction::StopRecovery) => {
                            return NormalRecoverySegmentOutcome::StopRecovery;
                        }
                        Err(error) => return NormalRecoverySegmentOutcome::StateFailed(error),
                    }
                }
                NormalRecoveryRecord::SourceEnded => match self.apply(NormalRecoveryEvent::SourceEnded) {
                    Ok(NormalRecoveryAction::ContinueRecord) => {}
                    Ok(NormalRecoveryAction::ContinueNextSegment) => {
                        return NormalRecoverySegmentOutcome::ContinueNextSegment;
                    }
                    Ok(NormalRecoveryAction::StopRecovery) => {
                        return NormalRecoverySegmentOutcome::StopRecovery;
                    }
                    Err(error) => return NormalRecoverySegmentOutcome::StateFailed(error),
                },
            }
        }
    }
}
