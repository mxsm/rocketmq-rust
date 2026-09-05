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

use std::fmt;

use rocketmq_error::CanonicalCondition;
use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::RuntimeContractViolation;

use crate::dispatch::ProtocolNoResponseReason;
use crate::dispatch::RequestId;

/// A deterministic Transport contract violation.
///
/// Variants retain only closed, static, or bounded numeric evidence. Typed
/// runtime contract causes remain inspectable through [`std::error::Error::source`],
/// while public formatting never renders their text or any request payload.
#[derive(Clone, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TransportContractViolation {
    #[error("transport admission budget contract is invalid")]
    AdmissionBudget(#[source] RuntimeContractViolation),
    #[error("transport admission scope has zero capacity")]
    AdmissionZeroScopeCapacity {
        scope: &'static str,
        dimension: BudgetDimension,
    },
    #[error("transport admission scope-key capacity is zero")]
    AdmissionZeroMaxScopeKeys,
    #[error("deferred admission waiter capacity is zero")]
    DeferredAdmissionZeroWaiterCapacity(#[source] RuntimeContractViolation),
    #[error("deferred admission retained-byte capacity is zero")]
    DeferredAdmissionZeroRetainedByteCapacity(#[source] RuntimeContractViolation),
    #[error("deferred admission exceeds process capacity")]
    DeferredAdmissionExceedsProcessCapacity(#[source] RuntimeContractViolation),
    #[error("deferred admission conflicts with the installed configuration")]
    DeferredAdmissionConflict,
    #[error("deferred admission budget contract is invalid")]
    DeferredAdmissionBudget(#[source] RuntimeContractViolation),
    #[error("deferred retained-size accounting overflowed")]
    DeferredRetainedSizeOverflow,
    #[error("deferred retained-size declaration underreported owned storage")]
    DeferredRetainedSizeUnderreported,
    #[error("deferred response recovery margin is zero")]
    DeferredExpiryZeroRecoveryMargin,
    #[error("deferred response write margin is zero")]
    DeferredExpiryZeroWriteMargin,
    #[error("deferred response transition is invalid: operation={operation}, state={state}")]
    DeferredResponseInvalidTransition {
        operation: &'static str,
        state: &'static str,
    },
    #[error("deferred response cannot bind to a one-way request")]
    DeferredResponseBindingRejected,
    #[error("session cleanup ownership invariant was violated")]
    SessionCleanupInvariant,
    #[error("protocol no-response is unavailable for one-way requests")]
    ProtocolNoResponseOneWayRequest,
    #[error("protocol no-response reason is unsupported for this request code")]
    ProtocolNoResponseUnsupported {
        request_code: i32,
        reason: ProtocolNoResponseReason,
    },
    #[error("deferred response capability is unavailable")]
    DeferredResponderUnavailable,
    #[error("the deferred responder was already taken")]
    DeferredResponderAlreadyTaken,
    #[error("the handler outcome was already completed")]
    HandlerOutcomeAlreadyCompleted,
    #[error("a deferred registration was supplied before a responder was taken")]
    DeferredResponderNotTaken,
    #[error("a reply was supplied after the deferred responder was taken")]
    ReplyAfterDeferredTaken,
    #[error("a no-response marker was supplied after the deferred responder was taken")]
    NoReplyAfterDeferredTaken,
    #[error("the deferred registration belongs to a different request")]
    DeferredRegistrationRequestMismatch { expected: RequestId, actual: RequestId },
    #[error("the protocol no-response marker does not match the original request")]
    NoResponseIdentityMismatch,
    #[error("the protocol no-response marker violates request policy")]
    NoResponsePolicyMismatch {
        request_code: i32,
        reason: ProtocolNoResponseReason,
    },
    #[error("one-way requests cannot complete with a deferred handler outcome")]
    OneWayDeferredHandlerOutcome,
    #[error("one-way requests cannot complete with a no-reply handler outcome")]
    OneWayNoReplyHandlerOutcome,
    #[error("one-way request rejection produced an invalid handler outcome")]
    OneWayInvalidRejection,
    #[error("response commands cannot build inbound remoting requests")]
    RequestFromResponseCommand,
    #[error("original request identity does not match the owned command")]
    OriginalCommandMismatch,
    #[error("request owner does not match the session owner")]
    SessionOwnerMismatch,
    #[error("network ingress requires a network session")]
    NetworkSessionMismatch,
    #[error("embedded ingress requires an embedded session")]
    EmbeddedSessionMismatch,
    #[error("network peer does not match the session effective peer")]
    NetworkPeerMismatch,
    #[error("embedded ingress requires an authenticated principal")]
    MissingEmbeddedAuthentication,
    #[error("one-way requests cannot reserve a deferred response")]
    OneWayDeferredResponse,
    #[error("deferred response seed does not match its canonical owner")]
    DeferredResponseOwnerMismatch,
    #[error("remoting response head must not already own a body")]
    ResponseHeadHasBody,
    #[error("remoting response head must be a response command")]
    ResponseRequestHead,
    #[error("remoting response head must not be one-way")]
    ResponseOneWayHead,
    #[error("remoting response body length overflowed")]
    ResponseBodyLengthOverflow,
    #[error("remoting response body is too large")]
    ResponseBodyTooLarge { actual: u64, maximum: u64 },
    #[error("remoting response body length is not representable on this platform")]
    ResponseBodyLengthNotRepresentable { actual: u64 },
    #[error("response binding is unavailable for a one-way request")]
    ResponseBindingOneWayRequest,
    #[error("trusted embedded authentication is unavailable")]
    RequestContextMissingEmbeddedAuthentication,
}

impl TransportContractViolation {
    /// Creates a violation for a response head that already owns a body.
    #[must_use]
    pub const fn response_head_has_body() -> Self {
        Self::ResponseHeadHasBody
    }

    /// Creates a violation for a request command used as a response head.
    #[must_use]
    pub const fn response_request_head() -> Self {
        Self::ResponseRequestHead
    }

    /// Creates a violation for a response head marked as one-way.
    #[must_use]
    pub const fn response_one_way_head() -> Self {
        Self::ResponseOneWayHead
    }

    /// Creates a violation when aggregate response body length overflows.
    #[must_use]
    pub const fn response_body_length_overflow() -> Self {
        Self::ResponseBodyLengthOverflow
    }

    /// Creates a violation for a response body above the protocol ceiling.
    #[must_use]
    pub const fn response_body_too_large(actual: u64, maximum: u64) -> Self {
        Self::ResponseBodyTooLarge { actual, maximum }
    }

    /// Creates a violation for a body length outside the platform address space.
    #[must_use]
    pub const fn response_body_length_not_representable(actual: u64) -> Self {
        Self::ResponseBodyLengthNotRepresentable { actual }
    }

    /// Returns the protocol-independent condition for this violation.
    #[must_use]
    pub const fn condition(&self) -> CanonicalCondition {
        match self {
            Self::DeferredAdmissionConflict
            | Self::DeferredResponseInvalidTransition { .. }
            | Self::DeferredResponseBindingRejected
            | Self::SessionCleanupInvariant
            | Self::DeferredResponderUnavailable
            | Self::DeferredResponderAlreadyTaken
            | Self::HandlerOutcomeAlreadyCompleted
            | Self::DeferredResponderNotTaken
            | Self::ReplyAfterDeferredTaken
            | Self::NoReplyAfterDeferredTaken
            | Self::DeferredRegistrationRequestMismatch { .. }
            | Self::NoResponseIdentityMismatch
            | Self::NoResponsePolicyMismatch { .. }
            | Self::OneWayDeferredHandlerOutcome
            | Self::OneWayNoReplyHandlerOutcome
            | Self::OneWayInvalidRejection
            | Self::OriginalCommandMismatch
            | Self::SessionOwnerMismatch
            | Self::NetworkSessionMismatch
            | Self::EmbeddedSessionMismatch
            | Self::NetworkPeerMismatch
            | Self::OneWayDeferredResponse
            | Self::DeferredResponseOwnerMismatch
            | Self::ResponseBindingOneWayRequest => CanonicalCondition::FailedPrecondition,
            Self::DeferredRetainedSizeOverflow
            | Self::DeferredRetainedSizeUnderreported
            | Self::ResponseBodyLengthOverflow
            | Self::ResponseBodyTooLarge { .. }
            | Self::ResponseBodyLengthNotRepresentable { .. } => CanonicalCondition::ResourceExhausted,
            Self::MissingEmbeddedAuthentication | Self::RequestContextMissingEmbeddedAuthentication => {
                CanonicalCondition::Unauthenticated
            }
            Self::AdmissionBudget(_)
            | Self::AdmissionZeroScopeCapacity { .. }
            | Self::AdmissionZeroMaxScopeKeys
            | Self::DeferredAdmissionZeroWaiterCapacity(_)
            | Self::DeferredAdmissionZeroRetainedByteCapacity(_)
            | Self::DeferredAdmissionExceedsProcessCapacity(_)
            | Self::DeferredAdmissionBudget(_)
            | Self::DeferredExpiryZeroRecoveryMargin
            | Self::DeferredExpiryZeroWriteMargin
            | Self::ProtocolNoResponseOneWayRequest
            | Self::ProtocolNoResponseUnsupported { .. }
            | Self::RequestFromResponseCommand
            | Self::ResponseHeadHasBody
            | Self::ResponseRequestHead
            | Self::ResponseOneWayHead => CanonicalCondition::InvalidArgument,
        }
    }
}

impl fmt::Debug for TransportContractViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportContractViolation")
            .field("condition", &self.condition())
            .field("message", &self.to_string())
            .finish()
    }
}
