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

//! Runtime-neutral per-segment orchestration for abnormal CommitLog recovery.

use super::recovery::AbnormalRecoveryAction;
use super::recovery::AbnormalRecoveryDispatchGate;
use super::recovery::AbnormalRecoveryEvent;
use super::recovery::AbnormalRecoveryOffsetError;
use super::recovery::AbnormalRecoveryState;

/// One adapter-produced record consumed by abnormal segment recovery.
#[derive(Debug, PartialEq, Eq)]
pub enum AbnormalRecoveryRecord<R> {
    /// A validated message frame and its recovery metadata.
    Message {
        /// Start of the input frame relative to its containing segment.
        relative_start: u64,
        /// Size accepted by the record parser.
        validated_size: u64,
        /// Absolute end derived from the encoded physical offset and input size.
        confirm_candidate_end: i64,
        /// Dispatch boundary for this message.
        dispatch_gate: AbnormalRecoveryDispatchGate,
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

/// Side-effect observation emitted around abnormal recovery state transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbnormalRecoveryObservation {
    /// A message was accepted and is eligible for dispatch.
    DispatchMessage,
    /// A message was accepted but excluded by the confirm boundary.
    SkipMessageDispatch,
    /// A blank marker was accepted and must notify the file-end hook.
    Blank,
    /// An invalid record was encountered.
    Invalid {
        /// Frame start relative to the segment when conversion succeeded.
        relative_start: Option<u64>,
    },
}

/// Terminal outcome of driving one abnormal recovery segment.
#[derive(Debug, PartialEq, Eq)]
pub enum AbnormalRecoverySegmentOutcome<E> {
    /// Continue abnormal recovery at the next segment.
    ContinueNextSegment,
    /// Stop abnormal recovery at the current watermarks.
    StopRecovery,
    /// The record adapter failed with its original error.
    AdapterFailed(E),
    /// The offset state machine rejected an event.
    StateFailed(AbnormalRecoveryOffsetError),
    /// The state machine returned an action that is invalid for the current record kind.
    UnexpectedAction(AbnormalRecoveryAction),
}

impl AbnormalRecoveryState {
    /// Drives records from one segment until the state selects the next segment or stops recovery.
    ///
    /// The segment-start state transition precedes `on_segment_started`. Message and blank state
    /// transitions precede observation, while invalid observation precedes its transition.
    /// `SourceEnded` is never observed. The driver retains ownership of each record payload and
    /// drops it after the observer returns or after a state rejection.
    pub fn drive_abnormal_segment<R, E, Next, Started, Observe>(
        &mut self,
        segment_base: u64,
        mut next_record: Next,
        on_segment_started: Started,
        mut observe: Observe,
    ) -> AbnormalRecoverySegmentOutcome<E>
    where
        Next: FnMut() -> Result<AbnormalRecoveryRecord<R>, E>,
        Started: FnOnce(),
        Observe: FnMut(AbnormalRecoveryObservation, &mut R),
    {
        match self.apply(AbnormalRecoveryEvent::SegmentStarted {
            base_offset: segment_base,
        }) {
            Ok(AbnormalRecoveryAction::ContinueRecord) => {}
            Ok(AbnormalRecoveryAction::ContinueNextSegment) => {
                return AbnormalRecoverySegmentOutcome::ContinueNextSegment;
            }
            Ok(AbnormalRecoveryAction::StopRecovery) => {
                return AbnormalRecoverySegmentOutcome::StopRecovery;
            }
            Ok(action) => return AbnormalRecoverySegmentOutcome::UnexpectedAction(action),
            Err(error) => return AbnormalRecoverySegmentOutcome::StateFailed(error),
        }
        on_segment_started();

        loop {
            let record = match next_record() {
                Ok(record) => record,
                Err(error) => return AbnormalRecoverySegmentOutcome::AdapterFailed(error),
            };
            match record {
                AbnormalRecoveryRecord::Message {
                    relative_start,
                    validated_size,
                    confirm_candidate_end,
                    dispatch_gate,
                    mut record,
                } => {
                    let action = match self.apply(AbnormalRecoveryEvent::MessageAccepted {
                        segment_base,
                        relative_start,
                        validated_size,
                        confirm_candidate_end,
                        dispatch_gate,
                    }) {
                        Ok(action) => action,
                        Err(error) => return AbnormalRecoverySegmentOutcome::StateFailed(error),
                    };
                    match action {
                        AbnormalRecoveryAction::DispatchMessage => {
                            observe(AbnormalRecoveryObservation::DispatchMessage, &mut record);
                        }
                        AbnormalRecoveryAction::SkipMessageDispatch => {
                            observe(AbnormalRecoveryObservation::SkipMessageDispatch, &mut record);
                        }
                        action => return AbnormalRecoverySegmentOutcome::UnexpectedAction(action),
                    }
                }
                AbnormalRecoveryRecord::Blank { mut record } => match self.apply(AbnormalRecoveryEvent::Blank) {
                    Ok(AbnormalRecoveryAction::NotifyFileEndAndContinueNextSegment) => {
                        observe(AbnormalRecoveryObservation::Blank, &mut record);
                        return AbnormalRecoverySegmentOutcome::ContinueNextSegment;
                    }
                    Ok(action) => return AbnormalRecoverySegmentOutcome::UnexpectedAction(action),
                    Err(error) => return AbnormalRecoverySegmentOutcome::StateFailed(error),
                },
                AbnormalRecoveryRecord::Invalid {
                    relative_start,
                    mut record,
                } => {
                    observe(AbnormalRecoveryObservation::Invalid { relative_start }, &mut record);
                    match self.apply(AbnormalRecoveryEvent::InvalidRecord) {
                        Ok(AbnormalRecoveryAction::ContinueNextSegment) => {
                            return AbnormalRecoverySegmentOutcome::ContinueNextSegment;
                        }
                        Ok(AbnormalRecoveryAction::StopRecovery) => {
                            return AbnormalRecoverySegmentOutcome::StopRecovery;
                        }
                        Ok(action) => return AbnormalRecoverySegmentOutcome::UnexpectedAction(action),
                        Err(error) => return AbnormalRecoverySegmentOutcome::StateFailed(error),
                    }
                }
                AbnormalRecoveryRecord::SourceEnded => match self.apply(AbnormalRecoveryEvent::SourceEnded) {
                    Ok(AbnormalRecoveryAction::ContinueNextSegment) => {
                        return AbnormalRecoverySegmentOutcome::ContinueNextSegment;
                    }
                    Ok(AbnormalRecoveryAction::StopRecovery) => {
                        return AbnormalRecoverySegmentOutcome::StopRecovery;
                    }
                    Ok(action) => return AbnormalRecoverySegmentOutcome::UnexpectedAction(action),
                    Err(error) => return AbnormalRecoverySegmentOutcome::StateFailed(error),
                },
            }
        }
    }
}
