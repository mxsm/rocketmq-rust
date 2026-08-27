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

//! Exhaustive V2 handler outcomes and inline response-contract state.

use std::fmt;

use rocketmq_error::RocketMQError;

use super::DeferredResponder;
use super::DeferredResponseSeed;
use super::OriginalRequestIdentity;
use super::RequestId;
use super::ResponsePlan;
use super::TakeDeferredResponderError;

mod oneway;

/// The one terminal contract outcome returned by a V2 request handler.
///
/// Each variant owns an affine capability or response payload. A handler must
/// either return a standard response plan, prove that trusted deferred storage
/// owns the response lifecycle, or return a request-bound protocol marker.
/// There is deliberately no direct-write bypass variant.
///
/// ```
/// use rocketmq_transport::api::v2::HandlerOutcome;
///
/// fn inspect(outcome: HandlerOutcome) {
///     match outcome {
///         HandlerOutcome::Reply(plan) => {
///             let _ = plan.response_code();
///         }
///         HandlerOutcome::Deferred(registration) => {
///             let _ = registration.request_id();
///         }
///         HandlerOutcome::NoReply(marker) => {
///             let _ = marker.reason();
///         }
///     }
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::HandlerOutcome;
///
/// fn outcomes_are_affine(outcome: &HandlerOutcome) {
///     let _: HandlerOutcome = outcome.clone();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::HandlerOutcome;
///
/// fn no_direct_write_bypass_exists() -> HandlerOutcome {
///     HandlerOutcome::AlreadyWritten
/// }
/// ```
#[must_use]
#[derive(Debug)]
pub enum HandlerOutcome {
    /// Return one owned response plan through canonical response binding and delivery.
    Reply(ResponsePlan),
    /// Complete the inline contract with a sealed deferred-registry proof.
    Deferred(DeferredRegistration),
    /// Complete the request without a direct response where protocol policy permits it.
    NoReply(ProtocolNoResponse),
}

/// Proof that trusted deferred storage accepted one request's response lifecycle.
///
/// The fields and seal are private, and this stage intentionally provides no
/// production constructor. Deferred registry integration will create this
/// proof only after it owns the responder, resume data, and wait permit.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::{DeferredRegistration, RequestId};
///
/// fn cannot_forge(request_id: RequestId) -> DeferredRegistration {
///     DeferredRegistration { request_id }
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::DeferredRegistration;
///
/// fn registrations_are_affine(registration: &DeferredRegistration) {
///     let _: DeferredRegistration = registration.clone();
/// }
/// ```
#[must_use]
pub struct DeferredRegistration {
    request_id: RequestId,
    seal: DeferredRegistrationSeal,
}

#[cfg(not(test))]
struct DeferredRegistrationSeal;

#[cfg(test)]
struct DeferredRegistrationSeal {
    drop_probe: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

#[cfg(test)]
impl Drop for DeferredRegistrationSeal {
    fn drop(&mut self) {
        if let Some(probe) = &self.drop_probe {
            probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl DeferredRegistration {
    /// Returns the exact request accepted by trusted deferred storage.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    #[cfg(test)]
    pub(crate) fn for_test(request_id: RequestId) -> Self {
        Self {
            request_id,
            seal: DeferredRegistrationSeal { drop_probe: None },
        }
    }

    #[cfg(test)]
    fn with_drop_probe(request_id: RequestId, drop_probe: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self {
            request_id,
            seal: DeferredRegistrationSeal {
                drop_probe: Some(drop_probe),
            },
        }
    }
}

impl fmt::Debug for DeferredRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = &self.seal;
        formatter
            .debug_struct("DeferredRegistration")
            .field("request_id", &self.request_id)
            .field("sealed", &true)
            .finish()
    }
}

/// Closed protocol reason for completing a request without a direct response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolNoResponseReason {
    /// The peer-requested callback was handled without a direct response frame.
    CallbackHandled,
    /// The peer-requested notification was handled without a direct response frame.
    NotificationHandled,
}

impl ProtocolNoResponseReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CallbackHandled => "callback_handled",
            Self::NotificationHandled => "notification_handled",
        }
    }
}

/// Failure to create a protocol no-response marker for an inbound request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtocolNoResponseError {
    /// One-way handling is an ingress property and cannot be claimed as a handler outcome.
    #[error("protocol no-response is unavailable for one-way requests")]
    OneWayRequest,
    /// The original request code does not permit the requested closed reason.
    #[error("protocol no-response reason {reason:?} is unsupported for request code {request_code}")]
    Unsupported {
        /// Original raw request code captured at ingress.
        request_code: i32,
        /// Closed reason rejected by the audited protocol policy.
        reason: ProtocolNoResponseReason,
    },
}

impl From<ProtocolNoResponseError> for RocketMQError {
    fn from(error: ProtocolNoResponseError) -> Self {
        match error {
            ProtocolNoResponseError::OneWayRequest => {
                Self::illegal_argument("protocol_no_response: category=one_way_request")
            }
            ProtocolNoResponseError::Unsupported { request_code, reason } => Self::illegal_argument(format!(
                "protocol_no_response: category=unsupported, request_code={request_code}, reason={}",
                reason.as_str()
            )),
        }
    }
}

/// Request-bound proof that the protocol permits no direct response frame.
///
/// Construct markers with [`crate::api::v2::RemotingRequest::protocol_no_response`].
/// The marker is affine so it cannot complete multiple handler contracts.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ProtocolNoResponse;
///
/// fn markers_are_affine(marker: &ProtocolNoResponse) {
///     let _: ProtocolNoResponse = marker.clone();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::{ProtocolNoResponse, ProtocolNoResponseReason, RequestId};
///
/// fn cannot_forge(request_id: RequestId) -> ProtocolNoResponse {
///     ProtocolNoResponse {
///         request_id,
///         original_code: 39,
///         reason: ProtocolNoResponseReason::CallbackHandled,
///     }
/// }
/// ```
#[must_use]
pub struct ProtocolNoResponse {
    request_id: RequestId,
    original_code: i32,
    reason: ProtocolNoResponseReason,
}

impl ProtocolNoResponse {
    pub(super) fn from_original(
        original: OriginalRequestIdentity,
        reason: ProtocolNoResponseReason,
    ) -> Result<Self, ProtocolNoResponseError> {
        validate_protocol_no_response(original, reason)?;
        Ok(Self {
            request_id: original.request_id(),
            original_code: original.original_code(),
            reason,
        })
    }

    /// Returns the exact ingress request identity bound to this marker.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the original raw request code captured at ingress.
    #[must_use]
    pub const fn original_code(&self) -> i32 {
        self.original_code
    }

    /// Returns the closed protocol reason represented by this marker.
    #[must_use]
    pub const fn reason(&self) -> ProtocolNoResponseReason {
        self.reason
    }
}

impl fmt::Debug for ProtocolNoResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtocolNoResponse")
            .field("request_id", &self.request_id)
            .field("original_code", &self.original_code)
            .field("reason", &self.reason)
            .finish()
    }
}

fn validate_protocol_no_response(
    original: OriginalRequestIdentity,
    reason: ProtocolNoResponseReason,
) -> Result<(), ProtocolNoResponseError> {
    if original.is_one_way() {
        return Err(ProtocolNoResponseError::OneWayRequest);
    }
    if protocol_no_response_allowed(original.original_code(), reason) {
        Ok(())
    } else {
        Err(ProtocolNoResponseError::Unsupported {
            request_code: original.original_code(),
            reason,
        })
    }
}

fn protocol_no_response_allowed(request_code: i32, reason: ProtocolNoResponseReason) -> bool {
    matches!(
        (request_code, reason),
        (39 | 220, ProtocolNoResponseReason::CallbackHandled)
            | (40 | 200_073, ProtocolNoResponseReason::NotificationHandled)
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InlineResponseState {
    Open,
    OpenWithDeferred,
    DeferredTaken,
    Completed,
}

/// Allocation-free handler-contract state owned by one request stack frame.
pub(crate) struct InlineResponseSlot {
    state: InlineResponseState,
    deferred_seed: Option<DeferredResponseSeed>,
}

impl Default for InlineResponseSlot {
    fn default() -> Self {
        Self::disabled()
    }
}

impl InlineResponseSlot {
    pub(crate) const fn disabled() -> Self {
        Self {
            state: InlineResponseState::Open,
            deferred_seed: None,
        }
    }

    #[cfg(test)]
    pub(crate) const fn deferred_capable() -> Self {
        Self {
            state: InlineResponseState::OpenWithDeferred,
            deferred_seed: None,
        }
    }

    pub(crate) const fn with_deferred_seed(seed: DeferredResponseSeed) -> Self {
        Self {
            state: InlineResponseState::OpenWithDeferred,
            deferred_seed: Some(seed),
        }
    }

    pub(crate) const fn has_deferred_capability(&self) -> bool {
        matches!(self.state, InlineResponseState::OpenWithDeferred)
    }

    pub(crate) fn mark_deferred_taken(
        &mut self,
        original: OriginalRequestIdentity,
    ) -> Result<(), HandlerOutcomeContractError> {
        if original.is_one_way() {
            return Err(HandlerOutcomeContractError::DeferredUnavailable);
        }
        match self.state {
            InlineResponseState::Open => Err(HandlerOutcomeContractError::DeferredUnavailable),
            InlineResponseState::OpenWithDeferred => {
                drop(self.deferred_seed.take());
                self.state = InlineResponseState::DeferredTaken;
                Ok(())
            }
            InlineResponseState::DeferredTaken => Err(HandlerOutcomeContractError::DeferredAlreadyTaken),
            InlineResponseState::Completed => Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted),
        }
    }

    pub(crate) fn take_deferred_responder(
        &mut self,
        original: OriginalRequestIdentity,
    ) -> Result<DeferredResponder, TakeDeferredResponderError> {
        if original.is_one_way() {
            return Err(TakeDeferredResponderError::OneWayRequest);
        }
        match self.state {
            InlineResponseState::Open => Err(TakeDeferredResponderError::Unavailable),
            InlineResponseState::OpenWithDeferred => {
                let Some(seed) = self.deferred_seed.take() else {
                    return Err(TakeDeferredResponderError::Unavailable);
                };
                self.state = InlineResponseState::DeferredTaken;
                Ok(seed.into_responder(original))
            }
            InlineResponseState::DeferredTaken => Err(TakeDeferredResponderError::AlreadyTaken),
            InlineResponseState::Completed => Err(TakeDeferredResponderError::OutcomeCompleted),
        }
    }

    pub(crate) fn resolve(
        &mut self,
        original: OriginalRequestIdentity,
        outcome: HandlerOutcome,
    ) -> Result<HandlerOutcome, HandlerOutcomeContractError> {
        let state = std::mem::replace(&mut self.state, InlineResponseState::Completed);
        drop(self.deferred_seed.take());
        match state {
            InlineResponseState::Open | InlineResponseState::OpenWithDeferred => match outcome {
                outcome @ HandlerOutcome::Reply(_) => Ok(outcome),
                HandlerOutcome::Deferred(_) => Err(HandlerOutcomeContractError::DeferredResponderNotTaken),
                HandlerOutcome::NoReply(marker) => {
                    validate_marker(&marker, original)?;
                    Ok(HandlerOutcome::NoReply(marker))
                }
            },
            InlineResponseState::DeferredTaken => match outcome {
                HandlerOutcome::Reply(_) => Err(HandlerOutcomeContractError::ReplyAfterDeferredTaken),
                HandlerOutcome::Deferred(registration) => {
                    if registration.request_id() != original.request_id() {
                        return Err(HandlerOutcomeContractError::DeferredRegistrationRequestMismatch {
                            expected: original.request_id(),
                            actual: registration.request_id(),
                        });
                    }
                    Ok(HandlerOutcome::Deferred(registration))
                }
                HandlerOutcome::NoReply(_) => Err(HandlerOutcomeContractError::NoReplyAfterDeferredTaken),
            },
            InlineResponseState::Completed => Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted),
        }
    }

    /// Consumes the affine inline reply state when an original one-way request
    /// discards an owned legacy response before plan validation.
    pub(crate) fn resolve_legacy_oneway_reply(&mut self) -> Result<(), HandlerOutcomeContractError> {
        let state = std::mem::replace(&mut self.state, InlineResponseState::Completed);
        drop(self.deferred_seed.take());
        match state {
            InlineResponseState::Open | InlineResponseState::OpenWithDeferred => Ok(()),
            InlineResponseState::DeferredTaken => Err(HandlerOutcomeContractError::ReplyAfterDeferredTaken),
            InlineResponseState::Completed => Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted),
        }
    }
}

fn validate_marker(
    marker: &ProtocolNoResponse,
    original: OriginalRequestIdentity,
) -> Result<(), HandlerOutcomeContractError> {
    if marker.request_id != original.request_id() || marker.original_code != original.original_code() {
        return Err(HandlerOutcomeContractError::NoResponseIdentityMismatch);
    }
    if original.is_one_way() || !protocol_no_response_allowed(original.original_code(), marker.reason) {
        return Err(HandlerOutcomeContractError::NoResponsePolicyMismatch {
            request_code: original.original_code(),
            reason: marker.reason,
        });
    }
    Ok(())
}

/// Failure while consuming one affine handler outcome against inline request state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum HandlerOutcomeContractError {
    #[error("deferred response capability is unavailable")]
    DeferredUnavailable,
    #[error("the deferred responder was already taken")]
    DeferredAlreadyTaken,
    #[error("the handler outcome was already completed")]
    OutcomeAlreadyCompleted,
    #[error("a deferred registration was supplied before a responder was taken")]
    DeferredResponderNotTaken,
    #[error("a reply was supplied after the deferred responder was taken")]
    ReplyAfterDeferredTaken,
    #[error("a no-response marker was supplied after the deferred responder was taken")]
    NoReplyAfterDeferredTaken,
    #[error("the deferred registration belongs to request {actual:?}, not {expected:?}")]
    DeferredRegistrationRequestMismatch { expected: RequestId, actual: RequestId },
    #[error("the protocol no-response marker does not match the original request identity")]
    NoResponseIdentityMismatch,
    #[error("protocol no-response reason {reason:?} is invalid for request code {request_code}")]
    NoResponsePolicyMismatch {
        request_code: i32,
        reason: ProtocolNoResponseReason,
    },
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use bytes::Bytes;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

    use super::*;
    use crate::dispatch::ResponseBody;
    use crate::file_region::FileRegion;
    use crate::file_region::FileRegionLease;
    use crate::file_region::FileRegionSequence;

    fn identity(owner: u64, code: RequestCode) -> OriginalRequestIdentity {
        identity_from_counter(owner, code, &AtomicU64::new(1))
    }

    fn identity_from_counter(owner: u64, code: RequestCode, sequence: &AtomicU64) -> OriginalRequestIdentity {
        let command = RemotingCommand::create_remoting_command(code.to_i32()).set_opaque(owner as i32);
        OriginalRequestIdentity::capture(owner, sequence, &command).expect("test identity should allocate")
    }

    fn response_head(code: i32) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code(code)
    }

    struct CountingLease {
        file: File,
        accesses: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl FileRegionLease for CountingLease {
        fn file(&self) -> &File {
            self.accesses.fetch_add(1, Ordering::SeqCst);
            &self.file
        }
    }

    impl Drop for CountingLease {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_file_plan(
        response_code: i32,
    ) -> (ResponsePlan, Arc<CountingLease>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let mut file = tempfile::tempfile().expect("temporary file");
        file.write_all(b"file-body").expect("write file body");
        let accesses = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let lease = Arc::new(CountingLease {
            file,
            accesses: Arc::clone(&accesses),
            drops: Arc::clone(&drops),
        });
        let region = FileRegion::try_new(lease.clone(), 0, 9).expect("file region");
        let regions = FileRegionSequence::single(region);
        let plan = ResponsePlan::file_regions(response_head(response_code), regions).expect("file plan");
        (plan, lease, accesses, drops)
    }

    #[test]
    fn inline_slot_tracks_the_four_states_without_allocating_deferred_state() {
        let allocations_before = crate::dispatch::deferred_responder::deferred_state_allocations();

        let original = identity(11, RequestCode::CheckTransactionState);
        let mut disabled = InlineResponseSlot::disabled();
        assert!(!disabled.has_deferred_capability());
        assert_eq!(
            disabled.mark_deferred_taken(original),
            Err(HandlerOutcomeContractError::DeferredUnavailable)
        );

        let mut capable = InlineResponseSlot::deferred_capable();
        assert!(capable.has_deferred_capability());
        assert_eq!(capable.mark_deferred_taken(original), Ok(()));
        assert!(!capable.has_deferred_capability());
        assert_eq!(
            capable.mark_deferred_taken(original),
            Err(HandlerOutcomeContractError::DeferredAlreadyTaken)
        );
        assert_eq!(
            crate::dispatch::deferred_responder::deferred_state_allocations(),
            allocations_before
        );
        let registration = DeferredRegistration::for_test(original.request_id());
        assert!(matches!(
            capable.resolve(original, HandlerOutcome::Deferred(registration)),
            Ok(HandlerOutcome::Deferred(_))
        ));
        assert_eq!(
            capable.mark_deferred_taken(original),
            Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted)
        );
        assert_eq!(
            crate::dispatch::deferred_responder::deferred_state_allocations(),
            allocations_before
        );
    }

    #[test]
    fn reply_moves_segment_and_file_owners_without_copy_or_file_access() {
        let original = identity(12, RequestCode::CheckTransactionState);
        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"second");
        let first_ptr = first.as_ptr();
        let second_ptr = second.as_ptr();
        let plan = ResponsePlan::segments(response_head(7), vec![first, second]).expect("segment plan");
        let ResponseBody::Segments(before) = plan.test_body() else {
            panic!("segments should retain their representation");
        };
        let allocation = before.as_ptr();
        let mut slot = InlineResponseSlot::disabled();
        let HandlerOutcome::Reply(plan) = slot
            .resolve(original, HandlerOutcome::Reply(plan))
            .expect("open reply should resolve")
        else {
            panic!("reply should remain a reply");
        };
        let ResponseBody::Segments(after) = plan.test_body() else {
            panic!("segments should retain their representation");
        };
        assert_eq!(after.as_ptr(), allocation);
        assert_eq!(after[0].as_ptr(), first_ptr);
        assert_eq!(after[1].as_ptr(), second_ptr);

        let (plan, lease, accesses, drops) = counting_file_plan(8);
        let ResponseBody::FileRegions(before) = plan.test_body() else {
            panic!("file plan representation");
        };
        let allocation = before.regions().as_ptr();
        assert_eq!(accesses.load(Ordering::SeqCst), 1);
        let mut slot = InlineResponseSlot::disabled();
        let HandlerOutcome::Reply(plan) = slot
            .resolve(original, HandlerOutcome::Reply(plan))
            .expect("file reply should resolve")
        else {
            panic!("reply should remain a reply");
        };
        let ResponseBody::FileRegions(after) = plan.test_body() else {
            panic!("file plan representation");
        };
        assert_eq!(after.regions().as_ptr(), allocation);
        assert_eq!(accesses.load(Ordering::SeqCst), 1);
        drop(plan);
        assert_eq!(Arc::strong_count(&lease), 1);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn legal_no_reply_is_revalidated_and_cross_request_or_policy_markers_fail_closed() {
        let first = identity(21, RequestCode::CheckTransactionState);
        let second = identity(22, RequestCode::CheckTransactionState);
        let marker =
            ProtocolNoResponse::from_original(first, ProtocolNoResponseReason::CallbackHandled).expect("legal marker");
        let mut cross_request = InlineResponseSlot::disabled();
        assert!(matches!(
            cross_request.resolve(second, HandlerOutcome::NoReply(marker)),
            Err(HandlerOutcomeContractError::NoResponseIdentityMismatch)
        ));
        assert!(matches!(
            cross_request.resolve(
                second,
                HandlerOutcome::Reply(ResponsePlan::command(response_head(9)).expect("reply"))
            ),
            Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted)
        ));

        let invalid = ProtocolNoResponse {
            request_id: first.request_id(),
            original_code: first.original_code(),
            reason: ProtocolNoResponseReason::NotificationHandled,
        };
        let mut policy = InlineResponseSlot::disabled();
        assert!(matches!(
            policy.resolve(first, HandlerOutcome::NoReply(invalid)),
            Err(HandlerOutcomeContractError::NoResponsePolicyMismatch { .. })
        ));

        let legal =
            ProtocolNoResponse::from_original(first, ProtocolNoResponseReason::CallbackHandled).expect("legal marker");
        let mut accepted = InlineResponseSlot::disabled();
        assert!(matches!(
            accepted.resolve(first, HandlerOutcome::NoReply(legal)),
            Ok(HandlerOutcome::NoReply(_))
        ));
    }

    #[test]
    fn no_reply_revalidation_rejects_four_cross_requests_and_every_identity_component() {
        let legal = [
            (
                RequestCode::CheckTransactionState,
                ProtocolNoResponseReason::CallbackHandled,
            ),
            (
                RequestCode::ResetConsumerClientOffset,
                ProtocolNoResponseReason::CallbackHandled,
            ),
            (
                RequestCode::NotifyConsumerIdsChanged,
                ProtocolNoResponseReason::NotificationHandled,
            ),
            (
                RequestCode::NotifyUnsubscribeLite,
                ProtocolNoResponseReason::NotificationHandled,
            ),
        ];
        for (index, (code, reason)) in legal.into_iter().enumerate() {
            let source = identity(51 + index as u64, code);
            let other_owner = identity(61 + index as u64, code);
            let marker = ProtocolNoResponse::from_original(source, reason).expect("legal marker");
            let mut slot = InlineResponseSlot::disabled();
            assert!(matches!(
                slot.resolve(other_owner, HandlerOutcome::NoReply(marker)),
                Err(HandlerOutcomeContractError::NoResponseIdentityMismatch)
            ));
        }

        let sequence = AtomicU64::new(1);
        let first = identity_from_counter(71, RequestCode::CheckTransactionState, &sequence);
        let second = identity_from_counter(71, RequestCode::CheckTransactionState, &sequence);
        let marker =
            ProtocolNoResponse::from_original(first, ProtocolNoResponseReason::CallbackHandled).expect("legal marker");
        let mut same_owner = InlineResponseSlot::disabled();
        assert!(matches!(
            same_owner.resolve(second, HandlerOutcome::NoReply(marker)),
            Err(HandlerOutcomeContractError::NoResponseIdentityMismatch)
        ));

        let wrong_code = ProtocolNoResponse {
            request_id: first.request_id(),
            original_code: RequestCode::NotifyConsumerIdsChanged.to_i32(),
            reason: ProtocolNoResponseReason::CallbackHandled,
        };
        let mut raw_code = InlineResponseSlot::disabled();
        assert!(matches!(
            raw_code.resolve(first, HandlerOutcome::NoReply(wrong_code)),
            Err(HandlerOutcomeContractError::NoResponseIdentityMismatch)
        ));

        let mut command = RemotingCommand::create_remoting_command(RequestCode::CheckTransactionState.to_i32());
        command.mark_oneway_rpc_ref();
        let one_way = OriginalRequestIdentity::capture(72, &AtomicU64::new(1), &command)
            .expect("one-way identity should allocate");
        let forged = ProtocolNoResponse {
            request_id: one_way.request_id(),
            original_code: one_way.original_code(),
            reason: ProtocolNoResponseReason::CallbackHandled,
        };
        let mut one_way_slot = InlineResponseSlot::disabled();
        assert!(matches!(
            one_way_slot.resolve(one_way, HandlerOutcome::NoReply(forged)),
            Err(HandlerOutcomeContractError::NoResponsePolicyMismatch { .. })
        ));
    }

    #[test]
    fn deferred_proofs_require_take_match_identity_and_drop_once_on_failure() {
        let first = identity(31, RequestCode::ResetConsumerClientOffset);
        let second = identity(32, RequestCode::ResetConsumerClientOffset);
        let drops = Arc::new(AtomicUsize::new(0));
        let registration = DeferredRegistration::with_drop_probe(first.request_id(), Arc::clone(&drops));
        let mut untaken = InlineResponseSlot::deferred_capable();
        assert!(matches!(
            untaken.resolve(first, HandlerOutcome::Deferred(registration)),
            Err(HandlerOutcomeContractError::DeferredResponderNotTaken)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let registration = DeferredRegistration::with_drop_probe(first.request_id(), Arc::clone(&drops));
        let mut mismatch = InlineResponseSlot::deferred_capable();
        mismatch.mark_deferred_taken(second).expect("take deferred responder");
        assert!(matches!(
            mismatch.resolve(second, HandlerOutcome::Deferred(registration)),
            Err(HandlerOutcomeContractError::DeferredRegistrationRequestMismatch { .. })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 2);

        let sequence = AtomicU64::new(1);
        let first = identity_from_counter(33, RequestCode::ResetConsumerClientOffset, &sequence);
        let second = identity_from_counter(33, RequestCode::ResetConsumerClientOffset, &sequence);
        let registration = DeferredRegistration::with_drop_probe(first.request_id(), Arc::clone(&drops));
        let mut same_owner = InlineResponseSlot::deferred_capable();
        same_owner.mark_deferred_taken(second).expect("take deferred responder");
        assert!(matches!(
            same_owner.resolve(second, HandlerOutcome::Deferred(registration)),
            Err(HandlerOutcomeContractError::DeferredRegistrationRequestMismatch { .. })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn reply_and_no_reply_after_defer_drop_payloads_and_complete_the_slot() {
        let original = identity(41, RequestCode::CheckTransactionState);
        let (plan, lease, accesses, drops) = counting_file_plan(10);
        drop(lease);
        let mut slot = InlineResponseSlot::deferred_capable();
        slot.mark_deferred_taken(original).expect("take deferred responder");
        assert!(matches!(
            slot.resolve(original, HandlerOutcome::Reply(plan)),
            Err(HandlerOutcomeContractError::ReplyAfterDeferredTaken)
        ));
        assert_eq!(accesses.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            slot.resolve(
                original,
                HandlerOutcome::NoReply(
                    ProtocolNoResponse::from_original(original, ProtocolNoResponseReason::CallbackHandled)
                        .expect("legal marker")
                )
            ),
            Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted)
        ));

        let mut no_reply = InlineResponseSlot::deferred_capable();
        no_reply.mark_deferred_taken(original).expect("take deferred responder");
        assert!(matches!(
            no_reply.resolve(
                original,
                HandlerOutcome::NoReply(
                    ProtocolNoResponse::from_original(original, ProtocolNoResponseReason::CallbackHandled)
                        .expect("legal marker")
                )
            ),
            Err(HandlerOutcomeContractError::NoReplyAfterDeferredTaken)
        ));
    }

    #[test]
    fn closed_error_mapping_contains_only_stable_contract_fields() {
        let mapped = RocketMQError::from(ProtocolNoResponseError::Unsupported {
            request_code: -91,
            reason: ProtocolNoResponseReason::NotificationHandled,
        });
        let display = mapped.to_string();
        assert!(display.contains("protocol_no_response"));
        assert!(display.contains("request_code=-91"));
        assert!(display.contains("notification_handled"));
        assert!(!display.contains("body"));
        assert!(!display.contains("peer"));
        assert!(!display.contains("principal"));
    }
}
