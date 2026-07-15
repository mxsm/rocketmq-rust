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

use crate::commit_log::append::AppendMessageResult;
use crate::commit_log::append::AppendMessageStatus;

/// Successful completion of a bounded CommitLog append attempt.
pub enum CommitLogAppendCompleted<S> {
    /// The initial append or its single EOF retry completed successfully.
    PutOk {
        result: AppendMessageResult,
        /// The old segment when the append crossed an EOF boundary.
        rolled_segment: Option<S>,
    },
    /// The single permitted EOF retry returned a status other than `PutOk`.
    RetryRejected {
        result: AppendMessageResult,
        /// The old segment that caused the first EOF result.
        rolled_segment: S,
    },
}

/// An append attempt that did not complete successfully, including terminal append rejections.
pub enum CommitLogAppendAborted<S, E> {
    /// Neither the supplied initial segment nor a newly acquired segment was available.
    InitialSegmentUnavailable,
    /// Preparing the initial active segment failed.
    InitialActiveLockFailed { error: E },
    /// The initial append rejected an illegal message or properties size.
    InitialMessageIllegal { result: AppendMessageResult },
    /// The initial append returned an unknown error.
    InitialUnknown { result: AppendMessageResult },
    /// The first append reached EOF, but no replacement segment was available.
    RolledSegmentUnavailable { first_eof: AppendMessageResult, old: S },
    /// The replacement segment could not be prepared for the retry.
    RolledActiveLockFailed {
        first_eof: AppendMessageResult,
        old: S,
        error: E,
    },
}

/// Outcome of one initial CommitLog append and at most one EOF retry.
pub enum CommitLogAppendOutcome<S, E> {
    Completed(CommitLogAppendCompleted<S>),
    Aborted(CommitLogAppendAborted<S, E>),
}

/// Runs the pure, bounded control flow around a CommitLog append operation.
pub struct CommitLogAppendAttempt;

impl CommitLogAppendAttempt {
    /// Runs one append, acquiring an initial segment when needed, and retries at most once on EOF.
    pub fn run<S, E, IsFull, Acquire, LockActive, Append>(
        initial_segment: Option<S>,
        mut is_full: IsFull,
        mut acquire: Acquire,
        mut lock_active: LockActive,
        mut append: Append,
    ) -> CommitLogAppendOutcome<S, E>
    where
        IsFull: FnMut(&S) -> bool,
        Acquire: FnMut() -> Option<S>,
        LockActive: FnMut(&S) -> Result<(), E>,
        Append: FnMut(&S) -> AppendMessageResult,
    {
        let segment = match initial_segment {
            Some(segment) => {
                if is_full(&segment) {
                    match acquire() {
                        Some(acquired) => {
                            drop(segment);
                            acquired
                        }
                        None => {
                            drop(segment);
                            return CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialSegmentUnavailable);
                        }
                    }
                } else {
                    segment
                }
            }
            None => match acquire() {
                Some(segment) => segment,
                None => {
                    return CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialSegmentUnavailable);
                }
            },
        };

        if let Err(error) = lock_active(&segment) {
            return CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialActiveLockFailed { error });
        }

        let first = append(&segment);
        match first.status {
            AppendMessageStatus::PutOk => CommitLogAppendOutcome::Completed(CommitLogAppendCompleted::PutOk {
                result: first,
                rolled_segment: None,
            }),
            AppendMessageStatus::MessageSizeExceeded | AppendMessageStatus::PropertiesSizeExceeded => {
                CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialMessageIllegal { result: first })
            }
            AppendMessageStatus::UnknownError => {
                CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::InitialUnknown { result: first })
            }
            AppendMessageStatus::EndOfFile => {
                let old = segment;
                let Some(rolled) = acquire() else {
                    return CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::RolledSegmentUnavailable {
                        first_eof: first,
                        old,
                    });
                };
                if let Err(error) = lock_active(&rolled) {
                    return CommitLogAppendOutcome::Aborted(CommitLogAppendAborted::RolledActiveLockFailed {
                        first_eof: first,
                        old,
                        error,
                    });
                }

                let retry = append(&rolled);
                match retry.status {
                    AppendMessageStatus::PutOk => CommitLogAppendOutcome::Completed(CommitLogAppendCompleted::PutOk {
                        result: retry,
                        rolled_segment: Some(old),
                    }),
                    AppendMessageStatus::EndOfFile
                    | AppendMessageStatus::MessageSizeExceeded
                    | AppendMessageStatus::PropertiesSizeExceeded
                    | AppendMessageStatus::UnknownError => {
                        CommitLogAppendOutcome::Completed(CommitLogAppendCompleted::RetryRejected {
                            result: retry,
                            rolled_segment: old,
                        })
                    }
                }
            }
        }
    }
}
