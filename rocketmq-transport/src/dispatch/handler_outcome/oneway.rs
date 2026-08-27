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

//! Affine validation for outcomes forbidden by immutable one-way ingress.

use super::protocol_no_response_allowed;
use super::DeferredRegistration;
use super::HandlerOutcomeContractError;
use super::InlineResponseSlot;
use super::InlineResponseState;
use super::OriginalRequestIdentity;
use super::ProtocolNoResponse;

impl InlineResponseSlot {
    /// Atomically consumes and validates a deferred proof that is forbidden
    /// only because the immutable original request is one-way.
    pub(crate) fn consume_oneway_deferred(
        &mut self,
        original: OriginalRequestIdentity,
        registration: DeferredRegistration,
    ) -> Result<(), HandlerOutcomeContractError> {
        let state = std::mem::replace(&mut self.state, InlineResponseState::Completed);
        drop(self.deferred_seed.take());
        match state {
            InlineResponseState::Open => {
                if registration.request_id() != original.request_id() {
                    return Err(HandlerOutcomeContractError::DeferredRegistrationRequestMismatch {
                        expected: original.request_id(),
                        actual: registration.request_id(),
                    });
                }
                Ok(())
            }
            InlineResponseState::OpenWithDeferred => Err(HandlerOutcomeContractError::DeferredUnavailable),
            InlineResponseState::DeferredTaken => Err(HandlerOutcomeContractError::DeferredAlreadyTaken),
            InlineResponseState::Completed => Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted),
        }
    }

    /// Atomically consumes and validates a no-response marker that is
    /// forbidden only because the immutable original request is one-way.
    pub(crate) fn consume_oneway_no_reply(
        &mut self,
        original: OriginalRequestIdentity,
        marker: ProtocolNoResponse,
    ) -> Result<(), HandlerOutcomeContractError> {
        let state = std::mem::replace(&mut self.state, InlineResponseState::Completed);
        drop(self.deferred_seed.take());
        match state {
            InlineResponseState::Open => validate_oneway_marker(&marker, original),
            InlineResponseState::OpenWithDeferred => Err(HandlerOutcomeContractError::DeferredUnavailable),
            InlineResponseState::DeferredTaken => Err(HandlerOutcomeContractError::NoReplyAfterDeferredTaken),
            InlineResponseState::Completed => Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted),
        }
    }
}

fn validate_oneway_marker(
    marker: &ProtocolNoResponse,
    original: OriginalRequestIdentity,
) -> Result<(), HandlerOutcomeContractError> {
    if marker.request_id != original.request_id() || marker.original_code != original.original_code() {
        return Err(HandlerOutcomeContractError::NoResponseIdentityMismatch);
    }
    if !protocol_no_response_allowed(original.original_code(), marker.reason) {
        return Err(HandlerOutcomeContractError::NoResponsePolicyMismatch {
            request_code: original.original_code(),
            reason: marker.reason,
        });
    }
    Ok(())
}

#[cfg(test)]
impl ProtocolNoResponse {
    pub(crate) const fn for_test(
        request_id: super::RequestId,
        original_code: i32,
        reason: super::ProtocolNoResponseReason,
    ) -> Self {
        Self {
            request_id,
            original_code,
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

    use super::*;
    use crate::dispatch::ProtocolNoResponseReason;

    fn one_way_identity(owner: u64, code: RequestCode) -> OriginalRequestIdentity {
        let mut command = RemotingCommand::create_remoting_command(code.to_i32()).set_opaque(owner as i32);
        command.mark_oneway_rpc_ref();
        OriginalRequestIdentity::capture(owner, &AtomicU64::new(1), &command)
            .expect("test one-way identity should allocate")
    }

    #[test]
    fn forbidden_oneway_affine_proofs_validate_identity_and_consume_the_slot_once() {
        let original = one_way_identity(81, RequestCode::CheckTransactionState);
        let foreign = one_way_identity(82, RequestCode::CheckTransactionState);
        let drops = Arc::new(AtomicUsize::new(0));

        let mut valid_deferred = InlineResponseSlot::disabled();
        assert_eq!(
            valid_deferred.consume_oneway_deferred(
                original,
                DeferredRegistration::with_drop_probe(original.request_id(), Arc::clone(&drops)),
            ),
            Ok(())
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            valid_deferred.consume_oneway_deferred(
                original,
                DeferredRegistration::with_drop_probe(original.request_id(), Arc::clone(&drops)),
            ),
            Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 2);

        let mut cross_deferred = InlineResponseSlot::disabled();
        assert!(matches!(
            cross_deferred.consume_oneway_deferred(
                original,
                DeferredRegistration::with_drop_probe(foreign.request_id(), Arc::clone(&drops)),
            ),
            Err(HandlerOutcomeContractError::DeferredRegistrationRequestMismatch { .. })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert_eq!(
            cross_deferred.consume_oneway_deferred(
                original,
                DeferredRegistration::with_drop_probe(original.request_id(), Arc::clone(&drops)),
            ),
            Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 4);

        let marker = ProtocolNoResponse::for_test(
            original.request_id(),
            original.original_code(),
            ProtocolNoResponseReason::CallbackHandled,
        );
        let mut valid_no_reply = InlineResponseSlot::disabled();
        assert_eq!(valid_no_reply.consume_oneway_no_reply(original, marker), Ok(()));
        assert_eq!(
            valid_no_reply.consume_oneway_no_reply(
                original,
                ProtocolNoResponse::for_test(
                    original.request_id(),
                    original.original_code(),
                    ProtocolNoResponseReason::CallbackHandled,
                ),
            ),
            Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted)
        );

        let mut cross_no_reply = InlineResponseSlot::disabled();
        assert_eq!(
            cross_no_reply.consume_oneway_no_reply(
                original,
                ProtocolNoResponse::for_test(
                    foreign.request_id(),
                    original.original_code(),
                    ProtocolNoResponseReason::CallbackHandled,
                ),
            ),
            Err(HandlerOutcomeContractError::NoResponseIdentityMismatch)
        );
        assert_eq!(
            cross_no_reply.consume_oneway_no_reply(
                original,
                ProtocolNoResponse::for_test(
                    original.request_id(),
                    original.original_code(),
                    ProtocolNoResponseReason::CallbackHandled,
                ),
            ),
            Err(HandlerOutcomeContractError::OutcomeAlreadyCompleted)
        );
    }
}
