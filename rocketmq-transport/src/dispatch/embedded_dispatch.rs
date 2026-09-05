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
use super::ProtocolNoResponseReason;
use super::RemotingResponse;
use super::RequestId;
use super::ResponseOperationalFailure;
use super::WriteProgress;
use crate::contract::TransportContractViolation;

/// Terminal result of one channel-free embedded dispatch.
///
/// Each variant owns the corresponding affine handler state. The result is
/// intentionally not cloneable, so a remoting response or deferred proof cannot
/// be completed through more than one path.
///
/// ```compile_fail
/// use rocketmq_transport::api::EmbeddedDispatchOutcome;
///
/// fn outcomes_are_affine(outcome: &EmbeddedDispatchOutcome) {
///     let _: EmbeddedDispatchOutcome = outcome.clone();
/// }
/// ```
#[must_use]
#[derive(Debug)]
#[non_exhaustive]
pub enum EmbeddedDispatchOutcome {
    /// An owned remoting response was accepted by the in-process response boundary.
    Reply(RemotingResponse),
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
    /// Bounded admission rejected the request before processor execution.
    AdmissionRejected,
    /// The process-local session or request identity namespace was exhausted.
    IdentityExhausted,
    /// The parent lifecycle was cancelled.
    Cancelled,
    /// The embedded session closed before terminal completion.
    SessionClosed,
    /// The immutable request deadline elapsed.
    DeadlineExceeded,
    /// The sole terminal receiver closed before accepting completion.
    CompletionClosed,
}

/// Stable category for a channel-free embedded dispatch failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum EmbeddedDispatchErrorKind {
    /// The lifecycle owner could not create or admit the request task.
    Runtime,
    /// Trusted request construction rejected inconsistent ingress facts.
    RequestConstruction,
    /// A processor error could not be converted to an owned remoting response.
    ResponseConstruction,
    /// Immutable response binding rejected the response.
    ResponseBinding,
    /// The processor violated the affine handler-outcome contract.
    HandlerContract,
    /// Trusted deferred storage could not commit the provisional waiter.
    DeferredCommit,
    /// A one-way request produced a deferred or no-reply contract.
    OneWayContract,
    /// In-process response handoff failed with a typed response error.
    Response {
        /// Stable response operation.
        operation: &'static str,
        /// Known response-write progress.
        progress: WriteProgress,
    },
}

/// Typed, redacted failure from channel-free embedded dispatch.
///
/// The error preserves the concrete internal source for diagnostics without
/// retaining the request command, body, principal, or security decision.
pub(crate) struct EmbeddedDispatchError {
    kind: EmbeddedDispatchErrorKind,
    source: EmbeddedDispatchErrorSource,
}

impl EmbeddedDispatchError {
    /// Returns the stable, payload-free failure category.
    #[must_use]
    #[cfg(test)]
    pub const fn kind(&self) -> EmbeddedDispatchErrorKind {
        self.kind
    }

    pub(crate) fn runtime(error: RuntimeError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::Runtime,
            EmbeddedDispatchErrorSource::Runtime(error),
        )
    }

    pub(crate) fn request_construction(error: TransportContractViolation) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::RequestConstruction,
            EmbeddedDispatchErrorSource::Contract(error),
        )
    }

    pub(crate) fn response_construction(error: TransportContractViolation) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::ResponseConstruction,
            EmbeddedDispatchErrorSource::Contract(error),
        )
    }

    pub(crate) fn response_binding(error: TransportContractViolation) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::ResponseBinding,
            EmbeddedDispatchErrorSource::Contract(error),
        )
    }

    pub(crate) fn handler_contract(error: TransportContractViolation) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::HandlerContract,
            EmbeddedDispatchErrorSource::Contract(error),
        )
    }

    pub(crate) fn deferred_commit(error: DeferredCommitError) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::DeferredCommit,
            EmbeddedDispatchErrorSource::DeferredCommit(error),
        )
    }

    pub(crate) fn one_way_contract(violation: TransportContractViolation) -> Self {
        Self::new(
            EmbeddedDispatchErrorKind::OneWayContract,
            EmbeddedDispatchErrorSource::Contract(violation),
        )
    }

    pub(crate) fn response(error: ResponseOperationalFailure) -> Self {
        let kind = EmbeddedDispatchErrorKind::Response {
            operation: error.operation(),
            progress: error.write_progress(),
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
        write!(formatter, "embedded dispatch failed: {:?}", self.kind)
    }
}

impl Error for EmbeddedDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_error())
    }
}

enum EmbeddedDispatchErrorSource {
    Runtime(RuntimeError),
    Contract(TransportContractViolation),
    DeferredCommit(DeferredCommitError),
    Response(ResponseOperationalFailure),
}

impl EmbeddedDispatchErrorSource {
    fn as_error(&self) -> &(dyn Error + 'static) {
        match self {
            Self::Runtime(error) => error,
            Self::Contract(error) => error,
            Self::DeferredCommit(error) => error,
            Self::Response(error) => error,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::*;
    #[test]
    fn response_operational_failure_retains_typed_source_and_redacts_views() {
        let secret = "embedded-sensitive-transport-cause";
        let error = EmbeddedDispatchError::response(ResponseOperationalFailure::Transport {
            progress: WriteProgress::PossiblyPartial,
            source: RocketMQError::network_connection_failed(secret, secret),
        });
        assert_eq!(
            error.kind(),
            EmbeddedDispatchErrorKind::Response {
                operation: "transport",
                progress: WriteProgress::PossiblyPartial,
            }
        );
        assert!(
            Error::source(&error).is_some_and(|source| source.downcast_ref::<ResponseOperationalFailure>().is_some())
        );
        assert!(!format!("{error:?}").contains(secret));
        assert!(!error.to_string().contains(secret));
    }

    #[test]
    fn one_way_contract_retains_each_closed_outcome_identity() {
        for expected in [
            TransportContractViolation::OneWayDeferredHandlerOutcome,
            TransportContractViolation::OneWayNoReplyHandlerOutcome,
            TransportContractViolation::OneWayInvalidRejection,
        ] {
            let error = EmbeddedDispatchError::one_way_contract(expected.clone());
            let actual = Error::source(&error)
                .and_then(|source| source.downcast_ref::<TransportContractViolation>())
                .expect("one-way dispatch error retains its canonical contract source");
            assert_eq!(actual, &expected);
            assert_eq!(
                actual.condition(),
                rocketmq_error::CanonicalCondition::FailedPrecondition
            );
        }
    }
}
