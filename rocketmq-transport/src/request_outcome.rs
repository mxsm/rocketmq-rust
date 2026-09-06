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

//! Exhaustive outcomes for response-aware outbound requests.

use std::fmt;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

/// Physical progress reached by one outbound request attempt.
///
/// This stage describes transport progress only. In particular,
/// [`Self::ResponseReceived`] does not assert that the remote business
/// operation did or did not complete. Callers can use the stage to decide
/// whether retrying an operation is safe without inferring progress from an
/// error descriptor or message.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboundRequestStage {
    /// No request bytes were written to the socket.
    BeforeWrite,
    /// The socket write started, so partial delivery is possible.
    Writing,
    /// The request was written and Transport was awaiting its response.
    AwaitingResponse,
    /// A response arrived before final response hooks or deadline validation completed.
    ///
    /// A final-hook failure is an operational error at this stage. A caller
    /// deadline observed after receipt is a typed deadline rejection at this
    /// stage.
    ResponseReceived,
}

/// Result of a response-aware outbound request.
///
/// Operational failures are returned separately as
/// [`crate::api::TransportError`]. Normal transport rejections and deterministic
/// request-contract failures remain typed values so callers can make policy
/// decisions without parsing error text. Callers must handle all three outcome
/// variants explicitly.
pub enum OutboundRequestOutcome {
    /// A response passed Transport's final hooks and deadline validation.
    Response(RemotingCommand),
    /// Transport declined or stopped the attempt without an operational failure.
    Rejected(OutboundRequestRejection),
    /// A deterministic request contract was not satisfied.
    Contract(OutboundRequestContract),
}

impl fmt::Debug for OutboundRequestOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response(response) => formatter
                .debug_struct("Response")
                .field("code", &response.code())
                .finish(),
            Self::Rejected(rejection) => formatter.debug_tuple("Rejected").field(rejection).finish(),
            Self::Contract(contract) => formatter.debug_tuple("Contract").field(contract).finish(),
        }
    }
}

/// Closed reasons for a normal outbound request rejection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboundRequestRejectionReason {
    /// The caller's immutable request deadline elapsed.
    DeadlineExpired,
    /// The request was cancelled before completion.
    Cancelled,
    /// The transport client is stopping and no longer accepts requests.
    ClientStopping,
    /// The selected session closed before the attempt completed.
    SessionClosed,
    /// The outbound queue could not admit the request.
    QueueSaturated,
    /// No healthy endpoint session was available for the attempt.
    EndpointUnavailable,
}

/// A normal outbound request rejection with bounded diagnostic facts.
///
/// Rejections do not carry an operational error source. The stage records the
/// physical progress reached before Transport declined or stopped the request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundRequestRejection {
    reason: OutboundRequestRejectionReason,
    stage: OutboundRequestStage,
    timeout_millis: Option<u64>,
    remote_addr_present: bool,
}

impl OutboundRequestRejection {
    const fn new(
        reason: OutboundRequestRejectionReason,
        stage: OutboundRequestStage,
        timeout_millis: Option<u64>,
        remote_addr_present: bool,
    ) -> Self {
        Self {
            reason,
            stage,
            timeout_millis,
            remote_addr_present,
        }
    }

    pub(crate) const fn deadline_expired(
        stage: OutboundRequestStage,
        timeout_millis: u64,
        remote_addr_present: bool,
    ) -> Self {
        Self::new(
            OutboundRequestRejectionReason::DeadlineExpired,
            stage,
            Some(timeout_millis),
            remote_addr_present,
        )
    }

    pub(crate) const fn cancelled(stage: OutboundRequestStage, remote_addr_present: bool) -> Self {
        Self::without_timeout(OutboundRequestRejectionReason::Cancelled, stage, remote_addr_present)
    }

    pub(crate) const fn client_stopping(stage: OutboundRequestStage, remote_addr_present: bool) -> Self {
        Self::without_timeout(
            OutboundRequestRejectionReason::ClientStopping,
            stage,
            remote_addr_present,
        )
    }

    pub(crate) const fn session_closed(stage: OutboundRequestStage, remote_addr_present: bool) -> Self {
        Self::without_timeout(
            OutboundRequestRejectionReason::SessionClosed,
            stage,
            remote_addr_present,
        )
    }

    pub(crate) const fn queue_saturated(stage: OutboundRequestStage, remote_addr_present: bool) -> Self {
        Self::without_timeout(
            OutboundRequestRejectionReason::QueueSaturated,
            stage,
            remote_addr_present,
        )
    }

    pub(crate) const fn endpoint_unavailable(stage: OutboundRequestStage, remote_addr_present: bool) -> Self {
        Self::without_timeout(
            OutboundRequestRejectionReason::EndpointUnavailable,
            stage,
            remote_addr_present,
        )
    }

    const fn without_timeout(
        reason: OutboundRequestRejectionReason,
        stage: OutboundRequestStage,
        remote_addr_present: bool,
    ) -> Self {
        Self::new(reason, stage, None, remote_addr_present)
    }

    /// Returns the closed rejection reason.
    #[must_use]
    pub const fn reason(&self) -> OutboundRequestRejectionReason {
        self.reason
    }

    /// Returns the physical request stage reached before rejection.
    #[must_use]
    pub const fn stage(&self) -> OutboundRequestStage {
        self.stage
    }

    /// Returns the original caller budget for a deadline rejection.
    ///
    /// Other rejection reasons return `None`.
    #[must_use]
    pub const fn timeout_millis(&self) -> Option<u64> {
        self.timeout_millis
    }

    /// Returns whether a remote address was known at the rejection point.
    ///
    /// The address itself is deliberately not exposed by this value.
    #[must_use]
    pub const fn remote_addr_present(&self) -> bool {
        self.remote_addr_present
    }
}

/// Closed reasons for deterministic outbound request contract failures.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboundRequestContractReason {
    /// The NameServer request lane has no configured endpoint.
    NameServerEndpointMissing,
}

/// A deterministic outbound request contract failure.
///
/// Contract failures do not carry an operational error source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundRequestContract {
    reason: OutboundRequestContractReason,
    stage: OutboundRequestStage,
    remote_addr_present: bool,
}

impl OutboundRequestContract {
    pub(crate) const fn name_server_endpoint_missing() -> Self {
        Self {
            reason: OutboundRequestContractReason::NameServerEndpointMissing,
            stage: OutboundRequestStage::BeforeWrite,
            remote_addr_present: false,
        }
    }

    /// Returns the closed contract-failure reason.
    #[must_use]
    pub const fn reason(&self) -> OutboundRequestContractReason {
        self.reason
    }

    /// Returns the physical request stage reached before the contract failure.
    #[must_use]
    pub const fn stage(&self) -> OutboundRequestStage {
        self.stage
    }

    /// Returns whether a remote address was present at the contract boundary.
    ///
    /// The address itself is deliberately not exposed by this value.
    #[must_use]
    pub const fn remote_addr_present(&self) -> bool {
        self.remote_addr_present
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_metadata_is_exposed_only_for_deadline_rejections() {
        let deadline = OutboundRequestRejection::deadline_expired(OutboundRequestStage::ResponseReceived, 37, true);
        let unavailable = OutboundRequestRejection::endpoint_unavailable(OutboundRequestStage::BeforeWrite, false);

        assert_eq!(deadline.timeout_millis(), Some(37));
        assert_eq!(deadline.stage(), OutboundRequestStage::ResponseReceived);
        assert!(deadline.remote_addr_present());
        assert_eq!(unavailable.timeout_millis(), None);
    }

    #[test]
    fn missing_nameserver_contract_is_source_free_before_write_metadata() {
        let contract = OutboundRequestContract::name_server_endpoint_missing();

        assert_eq!(
            contract.reason(),
            OutboundRequestContractReason::NameServerEndpointMissing
        );
        assert_eq!(contract.stage(), OutboundRequestStage::BeforeWrite);
        assert!(!contract.remote_addr_present());
    }
}
