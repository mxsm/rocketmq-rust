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

use std::error::Error;
use std::fmt;

use rocketmq_runtime::RuntimeError;

use super::DeferredCommitError;
use super::HandlerOutcomeContractError;
use super::ProtocolNoResponseReason;
use super::RequestId;
use super::ResponseBindingError;
use super::ResponseError;
use super::ResponseErrorKind;
use super::ResponsePlan;
use super::ResponsePlanError;
use super::WriteProgress;
use crate::admission::AdmissionError;
use crate::dispatch::remoting_request::RemotingRequestBuildError;

/// Terminal result of one channel-free embedded V2 dispatch.
///
/// Each variant owns the corresponding affine handler state. The result is
/// intentionally not cloneable, so a response plan or deferred proof cannot
/// be completed through more than one path.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
///
/// fn outcomes_are_affine(outcome: &EmbeddedDispatchOutcome) {
///     let _: EmbeddedDispatchOutcome = outcome.clone();
/// }
/// ```
#[must_use]
#[derive(Debug)]
#[non_exhaustive]
pub enum EmbeddedDispatchOutcome {
    /// An owned response plan was accepted by the in-process response boundary.
    Reply(ResponsePlan),
    /// The original request was one-way and every produced response was discarded.
    OneWay {
        /// The immutable request identity captured before processor mutation.
        request_id: RequestId,
    },
    /// Trusted deferred storage accepted the request lifecycle.
    Deferred {
        /// The immutable request identity accepted by deferred storage.
        request_id: RequestId,
    },
    /// Protocol policy permits this request to complete without a response.
    NoReply {
        /// The immutable request identity carried by the protocol marker.
        request_id: RequestId,
        /// The audited protocol reason for suppressing a response.
        reason: ProtocolNoResponseReason,
    },
}

/// Stable category for a channel-free embedded V2 dispatch failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EmbeddedDispatchErrorKind {
    /// The process-local session or request identity namespace was exhausted.
    IdentityExhausted,
    /// The lifecycle owner could not create or admit the request task.
    Runtime,
    /// The parent task group was cancelled.
    Cancelled,
    /// The embedded session closed before terminal completion.
    SessionClosed,
    /// The immutable request deadline elapsed.
    DeadlineExceeded,
    /// The configured admission policy rejected the request without a response plan.
    Admission,
    /// Trusted request construction rejected inconsistent ingress facts.
    RequestConstruction,
    /// A processor error could not be converted to an owned response plan.
    ResponseConstruction,
    /// Immutable response binding rejected the plan.
    ResponseBinding,
    /// The processor violated the affine handler-outcome contract.
    HandlerContract,
    /// Trusted deferred storage could not commit the provisional waiter.
    DeferredCommit,
    /// A one-way request produced a deferred or no-reply contract.
    OneWayContract,
    /// The caller dropped the sole terminal result receiver.
    CompletionClosed,
    /// In-process response handoff failed with a typed response error.
    Response {
        /// Stable response failure category.
        kind: ResponseErrorKind,
        /// Known response-write progress, when applicable.
        progress: Option<WriteProgress>,
    },
}

/// Typed, redacted failure from channel-free embedded V2 dispatch.
///
/// The error preserves the concrete internal source for diagnostics without
/// retaining the request command, body, principal, or security decision.
pub struct EmbeddedDispatchError {
    kind: EmbeddedDispatchErrorKind,
    source: EmbeddedDispatchErrorSource,
}

impl EmbeddedDispatchError {
    /// Returns the stable, payload-free failure category.
    #[must_use]
    pub const fn kind(&self) -> EmbeddedDispatchErrorKind {
        self.kind
    }

    pub(crate) fn identity_exhausted() -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::IdentityExhausted,
            EmbeddedDispatchErrorSource::IdentityExhausted(IdentityExhausted),
        )
    }

    pub(crate) fn runtime(error: RuntimeError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::Runtime,
            EmbeddedDispatchErrorSource::Runtime(error),
        )
    }

    pub(crate) fn cancelled() -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::Cancelled,
            EmbeddedDispatchErrorSource::Stop(EmbeddedStopError::Cancelled),
        )
    }

    pub(crate) fn session_closed() -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::SessionClosed,
            EmbeddedDispatchErrorSource::Stop(EmbeddedStopError::SessionClosed),
        )
    }

    pub(crate) fn deadline_exceeded() -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::DeadlineExceeded,
            EmbeddedDispatchErrorSource::Stop(EmbeddedStopError::DeadlineExceeded),
        )
    }

    pub(crate) fn admission(error: AdmissionError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::Admission,
            EmbeddedDispatchErrorSource::Admission(error),
        )
    }

    pub(crate) fn request_construction(error: RemotingRequestBuildError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::RequestConstruction,
            EmbeddedDispatchErrorSource::RequestConstruction(error),
        )
    }

    pub(crate) fn response_construction(error: ResponsePlanError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::ResponseConstruction,
            EmbeddedDispatchErrorSource::ResponseConstruction(error),
        )
    }

    pub(crate) fn response_binding(error: ResponseBindingError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::ResponseBinding,
            EmbeddedDispatchErrorSource::ResponseBinding(error),
        )
    }

    pub(crate) fn handler_contract(error: HandlerOutcomeContractError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::HandlerContract,
            EmbeddedDispatchErrorSource::HandlerContract(error),
        )
    }

    pub(crate) fn deferred_commit(error: DeferredCommitError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::DeferredCommit,
            EmbeddedDispatchErrorSource::DeferredCommit(error),
        )
    }

    pub(crate) fn one_way_contract(outcome: &'static str) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::OneWayContract,
            EmbeddedDispatchErrorSource::OneWayContract(OneWayContract { outcome }),
        )
    }

    pub(crate) fn completion_closed() -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::CompletionClosed,
            EmbeddedDispatchErrorSource::CompletionClosed(CompletionClosed),
        )
    }

    pub(crate) fn response(error: ResponseError) -> Self {
        let kind = match error {
            ResponseError::Cancelled => EmbeddedDispatchErrorKind::Cancelled,
            ResponseError::SessionClosed => EmbeddedDispatchErrorKind::SessionClosed,
            ResponseError::DeadlineExceeded => EmbeddedDispatchErrorKind::DeadlineExceeded,
            ref error => EmbeddedDispatchErrorKind::Response {
                kind: error.kind(),
                progress: error.write_progress(),
            },
        };
        Self::new(kind, EmbeddedDispatchErrorSource::Response(error))
    }

    fn new(kind: EmbeddedDispatchErrorKind, source: EmbeddedDispatchErrorSource) -> Self {
        Self { kind, source }
    }
}

impl fmt::Debug for EmbeddedDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddedDispatchError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for EmbeddedDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "embedded V2 dispatch failed: {:?}", self.kind)
    }
}

impl Error for EmbeddedDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_error())
    }
}

enum EmbeddedDispatchErrorSource {
    IdentityExhausted(IdentityExhausted),
    Runtime(RuntimeError),
    Stop(EmbeddedStopError),
    Admission(AdmissionError),
    RequestConstruction(RemotingRequestBuildError),
    ResponseConstruction(ResponsePlanError),
    ResponseBinding(ResponseBindingError),
    HandlerContract(HandlerOutcomeContractError),
    DeferredCommit(DeferredCommitError),
    OneWayContract(OneWayContract),
    CompletionClosed(CompletionClosed),
    Response(ResponseError),
}

impl EmbeddedDispatchErrorSource {
    fn as_error(&self) -> &(dyn Error + 'static) {
        match self {
            Self::IdentityExhausted(error) => error,
            Self::Runtime(error) => error,
            Self::Stop(error) => error,
            Self::Admission(error) => error,
            Self::RequestConstruction(error) => error,
            Self::ResponseConstruction(error) => error,
            Self::ResponseBinding(error) => error,
            Self::HandlerContract(error) => error,
            Self::DeferredCommit(error) => error,
            Self::OneWayContract(error) => error,
            Self::CompletionClosed(error) => error,
            Self::Response(error) => error,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("process-local embedded request identity namespace exhausted")]
struct IdentityExhausted;

#[derive(Debug, thiserror::Error)]
enum EmbeddedStopError {
    #[error("embedded dispatch parent was cancelled")]
    Cancelled,
    #[error("embedded dispatch session closed")]
    SessionClosed,
    #[error("embedded dispatch deadline exceeded")]
    DeadlineExceeded,
}

#[derive(Debug, thiserror::Error)]
#[error("one-way request completed with {outcome}")]
struct OneWayContract {
    outcome: &'static str,
}

#[derive(Debug, thiserror::Error)]
#[error("embedded dispatch terminal receiver closed")]
struct CompletionClosed;

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::dispatch::ResponseTerminalState;

    #[test]
    fn response_completion_errors_have_one_deterministic_public_kind_and_typed_source() {
        assert_response_kind(ResponseError::Cancelled, EmbeddedDispatchErrorKind::Cancelled);
        assert_response_kind(ResponseError::SessionClosed, EmbeddedDispatchErrorKind::SessionClosed);
        assert_response_kind(
            ResponseError::DeadlineExceeded,
            EmbeddedDispatchErrorKind::DeadlineExceeded,
        );
        assert_response_kind(
            ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Completed,
            },
            EmbeddedDispatchErrorKind::Response {
                kind: ResponseErrorKind::AlreadyCompleted,
                progress: None,
            },
        );
        assert_response_kind(
            ResponseError::QueueSaturated,
            EmbeddedDispatchErrorKind::Response {
                kind: ResponseErrorKind::QueueSaturated,
                progress: Some(WriteProgress::NotStarted),
            },
        );

        let secret = "embedded-sensitive-transport-cause";
        let error = EmbeddedDispatchError::response(ResponseError::Transport {
            progress: WriteProgress::PossiblyPartial,
            source: RocketMQError::network_connection_failed(secret, secret),
        });
        assert_eq!(
            error.kind(),
            EmbeddedDispatchErrorKind::Response {
                kind: ResponseErrorKind::Transport,
                progress: Some(WriteProgress::PossiblyPartial),
            }
        );
        assert!(Error::source(&error).is_some_and(|source| source.downcast_ref::<ResponseError>().is_some()));
        assert!(!format!("{error:?}").contains(secret));
        assert!(!error.to_string().contains(secret));
    }

    fn assert_response_kind(error: ResponseError, expected: EmbeddedDispatchErrorKind) {
        let error = EmbeddedDispatchError::response(error);
        assert_eq!(error.kind(), expected);
        assert!(Error::source(&error).is_some_and(|source| source.downcast_ref::<ResponseError>().is_some()));
    }
}
