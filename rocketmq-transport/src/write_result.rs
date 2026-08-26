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

use rocketmq_error::RocketMQError;
use rocketmq_error::SharedRocketMQError;

use crate::dispatch::ResponseError;
use crate::dispatch::WriteProgress;

/// Result returned by the sole session-writer owner.
pub(crate) type WriterResult = Result<(), WriterFailure>;

/// A terminal canonical-writer failure and the write progress it can prove.
///
/// The source is shared because one failed micro-batch completes every member
/// with the same immutable error snapshot. Converting it to either legacy or
/// response-facing errors retains that shared snapshot instead of formatting it
/// into a new string error. Legacy public facades use a separate projection so
/// their historic target and network-error shape remain intact per caller.
#[derive(Clone, Debug)]
pub(crate) struct WriterFailure {
    progress: WriteProgress,
    source: SharedRocketMQError,
    legacy: LegacyWriterFailure,
}

#[derive(Clone, Debug)]
enum LegacyWriterFailure {
    ConnectionFailed,
    DeadlineExceededBeforeSend,
    WriteTimeout { timeout_millis: u64 },
}

impl WriterFailure {
    /// Captures a socket failure once without formatting its source. The legacy
    /// facade receives a stable compatibility reason while typed callers retain
    /// the original shared error chain.
    pub(crate) fn from_io(progress: WriteProgress, error: io::Error) -> Self {
        Self {
            progress,
            source: SharedRocketMQError::new(RocketMQError::from(error)),
            legacy: LegacyWriterFailure::ConnectionFailed,
        }
    }

    pub(crate) fn connection_failed(progress: WriteProgress, reason: impl Into<Arc<str>>) -> Self {
        let reason = reason.into();
        Self {
            progress,
            source: SharedRocketMQError::new(RocketMQError::network_connection_failed(
                "transport-session-writer",
                reason.as_ref(),
            )),
            legacy: LegacyWriterFailure::ConnectionFailed,
        }
    }

    pub(crate) fn deadline_exceeded_before_send() -> Self {
        Self {
            progress: WriteProgress::NotStarted,
            source: SharedRocketMQError::new(RocketMQError::network_deadline_exceeded_before_send(
                "transport-session-writer",
            )),
            legacy: LegacyWriterFailure::DeadlineExceededBeforeSend,
        }
    }

    pub(crate) fn write_timeout(timeout_millis: u64) -> Self {
        Self {
            progress: WriteProgress::PossiblyPartial,
            source: SharedRocketMQError::new(RocketMQError::network_write_timeout(
                "transport-session-writer",
                timeout_millis,
            )),
            legacy: LegacyWriterFailure::WriteTimeout { timeout_millis },
        }
    }

    pub(crate) fn completion_dropped(progress: WriteProgress) -> Self {
        Self::connection_failed(progress, Arc::<str>::from("writer completion dropped"))
    }

    pub(crate) const fn progress(&self) -> WriteProgress {
        self.progress
    }

    /// Returns the immutable canonical cause shared by every completion in a
    /// failed writer micro-batch.
    #[cfg(test)]
    pub(crate) fn source(&self) -> &SharedRocketMQError {
        &self.source
    }

    pub(crate) fn into_response(self) -> ResponseError {
        ResponseError::Transport {
            progress: self.progress,
            source: self.source.into_error(),
        }
    }

    pub(crate) fn into_legacy_for_target(
        self,
        target: String,
        connection_failed_reason: &'static str,
    ) -> RocketMQError {
        match self.legacy {
            LegacyWriterFailure::ConnectionFailed => {
                RocketMQError::network_connection_failed(target, connection_failed_reason)
            }
            LegacyWriterFailure::DeadlineExceededBeforeSend => {
                RocketMQError::network_deadline_exceeded_before_send(target)
            }
            LegacyWriterFailure::WriteTimeout { timeout_millis } => {
                RocketMQError::network_write_timeout(target, timeout_millis)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;
    use std::io;

    use rocketmq_error::NetworkError;
    use rocketmq_error::RocketMQError;

    use super::WriterFailure;
    use crate::dispatch::ResponseError;
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
    fn response_conversion_retains_the_shared_typed_source_chain_and_identity() {
        let failure = WriterFailure::from_io(WriteProgress::PossiblyPartial, io::Error::other(PanicDisplay));
        let response = failure.clone().into_response();
        let second_response = failure.into_response();

        assert!(matches!(
            &response,
            ResponseError::Transport {
                progress: WriteProgress::PossiblyPartial,
                ..
            }
        ));
        let ResponseError::Transport { source, .. } = &response else {
            panic!("writer failure must become a transport response error")
        };
        let ResponseError::Transport {
            source: second_source, ..
        } = &second_response
        else {
            panic!("writer failure must become a transport response error")
        };
        let RocketMQError::Shared(shared) = source else {
            panic!("response completion must retain a shared typed source")
        };
        let RocketMQError::Shared(second_shared) = second_source else {
            panic!("response completion must retain a shared typed source")
        };
        assert!(std::ptr::eq(shared.as_error(), second_shared.as_error()));
        let RocketMQError::IO(io_error) = shared.as_error() else {
            panic!("writer I/O failure must retain the original typed error")
        };
        assert!(io_error.get_ref().is_some_and(|source| source.is::<PanicDisplay>()));
        assert!(Error::source(&response).is_some());
    }

    #[test]
    fn legacy_projection_preserves_each_callers_network_target_and_timeout_shape() {
        let source = WriterFailure::from_io(WriteProgress::PossiblyPartial, io::Error::other(PanicDisplay));
        let direct = source
            .clone()
            .into_legacy_for_target("direct-target".to_string(), "canonical writer failure");
        let queued = source.into_legacy_for_target("queued-target".to_string(), "canonical writer failure");

        assert!(matches!(
            direct,
            RocketMQError::Network(NetworkError::ConnectionFailed { addr, reason })
                if addr == "direct-target" && reason == "canonical writer failure"
        ));
        assert!(matches!(
            queued,
            RocketMQError::Network(NetworkError::ConnectionFailed { addr, reason })
                if addr == "queued-target" && reason == "canonical writer failure"
        ));
        assert!(matches!(
            WriterFailure::from_io(WriteProgress::NotStarted, io::Error::other(PanicDisplay)).into_legacy_for_target(
                "sendfile-target".to_string(),
                "sendfile mode requires an eligible file and plaintext TCP connection",
            ),
            RocketMQError::Network(NetworkError::ConnectionFailed { addr, reason })
                if addr == "sendfile-target" && reason.contains("sendfile")
        ));
        assert!(matches!(
            WriterFailure::completion_dropped(WriteProgress::NotStarted)
                .into_legacy_for_target("dropped-target".to_string(), "writer completion dropped"),
            RocketMQError::Network(NetworkError::ConnectionFailed { addr, reason })
                if addr == "dropped-target" && reason == "writer completion dropped"
        ));
        assert!(matches!(
            WriterFailure::deadline_exceeded_before_send()
                .into_legacy_for_target("deadline-target".to_string(), "unused connection reason"),
            RocketMQError::Network(NetworkError::DeadlineExceededBeforeSend { addr }) if addr == "deadline-target"
        ));
        assert!(matches!(
            WriterFailure::write_timeout(37)
                .into_legacy_for_target("timeout-target".to_string(), "unused connection reason"),
            RocketMQError::Network(NetworkError::WriteTimeout { addr, timeout_ms: 37 }) if addr == "timeout-target"
        ));
    }

    #[test]
    fn completion_drop_preserves_the_observed_write_progress() {
        assert_eq!(
            WriterFailure::completion_dropped(WriteProgress::NotStarted).progress(),
            WriteProgress::NotStarted
        );
        assert_eq!(
            WriterFailure::completion_dropped(WriteProgress::PossiblyPartial).progress(),
            WriteProgress::PossiblyPartial
        );
    }
}
