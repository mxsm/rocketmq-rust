// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Crate-private canonical writer completion.

use std::io;
use std::sync::Arc;

use rocketmq_error::SharedError;

use crate::deadline::RequestDeadline;
#[cfg(test)]
use crate::dispatch::ResponseOperationalFailure;
use crate::dispatch::WriteProgress;
use crate::error_helpers::connection_failed;
use crate::error_helpers::connection_failed_without_source;
use crate::error_helpers::network;
use crate::error_helpers::write_timeout;
use crate::error_helpers::write_timeout_caused_by;
use crate::error_helpers::TransportStage;

/// Result returned by the sole session-writer owner.
pub(crate) type WriterResult = Result<(), WriterFailure>;

/// One operational writer failure and the progress it can prove.
///
/// The physical source and descriptor are captured once in a shared canonical
/// error. Every failed member of a micro-batch receives the same allocation.
#[derive(Clone, Debug)]
pub(crate) enum WriterFailure {
    Operational {
        progress: WriteProgress,
        error: SharedError,
        prewrite_deadline_cutoff: Option<tokio::time::Instant>,
    },
    Rejected(WriterRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriterRejection {
    DeadlineExpired,
    Cancelled,
    SessionClosed,
}

impl WriterFailure {
    /// Captures a socket failure without formatting its source.
    #[track_caller]
    pub(crate) fn from_io(progress: WriteProgress, error: io::Error) -> Self {
        Self::Operational {
            progress,
            error: connection_failed(stage_for_progress(progress), error),
            prewrite_deadline_cutoff: None,
        }
    }

    /// Captures an unexpected writer/session failure that has no lower source.
    #[track_caller]
    pub(crate) fn connection_failed(progress: WriteProgress) -> Self {
        Self::Operational {
            progress,
            error: connection_failed_without_source(stage_for_progress(progress)),
            prewrite_deadline_cutoff: None,
        }
    }

    /// Captures a writer-owned elapsed deadline after writing began.
    #[track_caller]
    pub(crate) fn write_timeout_caused_by(
        progress: WriteProgress,
        timeout_millis: u64,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        debug_assert_eq!(progress, WriteProgress::PossiblyPartial);
        Self::Operational {
            progress,
            error: write_timeout_caused_by(stage_for_progress(progress), timeout_millis, source),
            prewrite_deadline_cutoff: None,
        }
    }

    /// Captures a prewrite cutoff shared by a batch so only the member whose
    /// deadline caused it is converted to a normal deadline outcome.
    #[track_caller]
    pub(crate) fn prewrite_timeout(timeout_millis: u64, cutoff: tokio::time::Instant) -> Self {
        Self::Operational {
            progress: WriteProgress::NotStarted,
            error: write_timeout(TransportStage::BeforeWrite, timeout_millis),
            prewrite_deadline_cutoff: Some(cutoff),
        }
    }

    pub(crate) const fn deadline_expired() -> Self {
        Self::Rejected(WriterRejection::DeadlineExpired)
    }

    pub(crate) const fn cancelled() -> Self {
        Self::Rejected(WriterRejection::Cancelled)
    }

    pub(crate) const fn session_closed() -> Self {
        Self::Rejected(WriterRejection::SessionClosed)
    }

    #[track_caller]
    pub(crate) fn completion_dropped(progress: WriteProgress) -> Self {
        Self::connection_failed(progress)
    }

    pub(crate) const fn progress(&self) -> Option<WriteProgress> {
        match self {
            Self::Operational { progress, .. } => Some(*progress),
            Self::Rejected(_) => None,
        }
    }

    /// Reuses the physical failure for queued work that the poisoned writer
    /// never attempted.
    pub(crate) fn for_queued_follower(&self) -> Self {
        match self {
            Self::Operational { error, .. } => Self::Operational {
                progress: WriteProgress::NotStarted,
                error: Arc::clone(error),
                prewrite_deadline_cutoff: None,
            },
            Self::Rejected(rejection) => Self::Rejected(*rejection),
        }
    }

    /// Returns the immutable canonical cause shared by every failed completion.
    #[cfg(test)]
    pub(crate) fn error(&self) -> Option<&SharedError> {
        match self {
            Self::Operational { error, .. } => Some(error),
            Self::Rejected(_) => None,
        }
    }

    pub(crate) fn was_caused_by(&self, owner_deadline: RequestDeadline) -> bool {
        matches!(
            self,
            Self::Operational {
                prewrite_deadline_cutoff: Some(cutoff),
                ..
            } if owner_deadline.instant() <= *cutoff
        )
    }

    pub(crate) fn into_operational(self) -> Result<(WriteProgress, SharedError), WriterRejection> {
        match self {
            Self::Operational { progress, error, .. } => Ok((progress, error)),
            Self::Rejected(rejection) => Err(rejection),
        }
    }

    #[cfg(test)]
    pub(crate) fn into_response(self) -> Result<ResponseOperationalFailure, WriterRejection> {
        self.into_operational()
            .map(|(progress, error)| ResponseOperationalFailure::transport(progress, error))
    }

    pub(crate) fn into_network(self) -> Result<rocketmq_error::RocketMQError, WriterRejection> {
        self.into_operational().map(|(_, error)| network(error))
    }
}

const fn stage_for_progress(progress: WriteProgress) -> TransportStage {
    match progress {
        WriteProgress::NotStarted => TransportStage::BeforeWrite,
        WriteProgress::PossiblyPartial => TransportStage::Writing,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;
    use std::io;

    use rocketmq_error::RocketMQError;

    use super::WriterFailure;
    use crate::dispatch::ResponseOperationalFailure;
    use crate::dispatch::WriteProgress;

    #[derive(Debug)]
    struct PanicDisplay;

    impl fmt::Display for PanicDisplay {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("canonical writer failures must not format their source")
        }
    }

    impl Error for PanicDisplay {}

    #[test]
    fn writer_clones_share_the_canonical_error_and_typed_io_source() {
        let failure = WriterFailure::from_io(WriteProgress::PossiblyPartial, io::Error::other(PanicDisplay));
        let cloned = failure.clone();

        assert!(std::sync::Arc::ptr_eq(
            failure.error().expect("operational error"),
            cloned.error().expect("cloned operational error")
        ));
        assert!(failure
            .error()
            .expect("operational error")
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .and_then(io::Error::get_ref)
            .is_some_and(|source| source.is::<PanicDisplay>()));
    }

    #[test]
    fn response_and_outbound_views_reuse_the_same_canonical_error() {
        let failure = WriterFailure::from_io(WriteProgress::NotStarted, io::Error::other(PanicDisplay));
        let expected = std::sync::Arc::clone(failure.error().expect("operational error"));
        let response = failure.clone().into_response().expect("operational response failure");
        let outbound = failure.into_network().expect("operational outbound failure");

        let ResponseOperationalFailure::Transport { source, .. } = response else {
            panic!("writer failure must remain a transport response failure")
        };
        assert!(std::sync::Arc::ptr_eq(&expected, &source));
        let RocketMQError::Network(source) = outbound else {
            panic!("outbound writer failure must use the canonical Network carrier")
        };
        assert!(std::sync::Arc::ptr_eq(&expected, &source));
    }

    #[test]
    fn writer_timeouts_keep_stage_progress_and_source_truth() {
        let before = WriterFailure::prewrite_timeout(37, tokio::time::Instant::now());
        let writing = WriterFailure::write_timeout_caused_by(WriteProgress::PossiblyPartial, 37, PanicDisplay);

        let before = before.error().expect("operational error");
        let writing = writing.error().expect("operational error");
        assert_eq!(before.code().as_str(), "transport.write.timeout");
        assert_eq!(before.context().to_string(), "phase=<redacted>, timeout_ms=37");
        assert!(before.source().is_none());
        assert_eq!(
            writing.context().to_string(),
            "phase=<redacted>, timeout_ms=37, source_present=<redacted>"
        );
        assert!(writing.source().is_some_and(|source| source.is::<PanicDisplay>()));
    }
}
