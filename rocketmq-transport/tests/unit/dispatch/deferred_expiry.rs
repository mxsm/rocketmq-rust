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

use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use tokio::time::Instant;

use super::*;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::contract::TransportContractViolation;
use crate::deadline::RequestDeadline;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredClaimOutcome;
use crate::dispatch::DeferredExpiryOutcome;
use crate::dispatch::DeferredParts;
use crate::dispatch::DeferredRegistry;
use crate::dispatch::DeferredRegistryOutcome;
use crate::dispatch::DeferredRequest;
use crate::dispatch::DeferredRetainedSizeParts;
use crate::dispatch::DeferredTerminalReason;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestId;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseSink;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

struct PartsFixture {
    _runtime: RuntimeOwner,
    parent: rocketmq_runtime::TaskGroup,
    session: Arc<EmbeddedSessionRecord>,
    request_id: RequestId,
    admission: DeferredAdmission,
    terminals: Arc<parking_lot::Mutex<Vec<(&'static str, &'static str)>>>,
    parts: Option<DeferredParts>,
}

impl PartsFixture {
    fn new(owner: u64, deadline: Option<RequestDeadline>) -> Self {
        let runtime = RuntimeOwner::plan(RuntimeConfig::server_default("deferred-expiry-acceptance"))
            .expect("test runtime configuration is valid")
            .build()
            .expect("expiry acceptance runtime");
        let parent = runtime
            .root_context()
            .component("deferred-expiry-acceptance")
            .task_group()
            .clone();
        let session = Arc::new(EmbeddedSessionRecord::new(owner));
        let command = RemotingCommand::create_remoting_command(11).set_opaque(owner as i32);
        let original =
            OriginalRequestIdentity::capture(owner, &AtomicU64::new(1), &command).expect("expiry acceptance identity");
        let request_id = original.request_id();
        let control = RequestControlView::from_meta(
            &RequestMeta::new(std::time::Instant::now(), deadline),
            session.view().state().clone(),
            &parent,
        );
        let (sink, _receiver) = ResponseSink::local(control.clone());
        let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
        let responder = sink
            .deferred_seed_for_test(telemetry, session.view().id(), control)
            .into_responder(original);
        let controller = AdmissionController::new(AdmissionLimits::default());
        let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(8, 8 * 1024 * 1024))
            .expect("expiry acceptance admission");
        let retained = DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0))
            .expect("expiry acceptance retained size");
        let permit = match admission.try_reserve(retained) {
            crate::dispatch::DeferredAdmissionAcquireOutcome::Acquired(permit) => permit,
            crate::dispatch::DeferredAdmissionAcquireOutcome::WaiterCapacityExhausted(_) => {
                panic!("expiry acceptance waiter capacity was unexpectedly exhausted")
            }
            crate::dispatch::DeferredAdmissionAcquireOutcome::RetainedByteCapacityExhausted(_) => {
                panic!("expiry acceptance retained-byte capacity was unexpectedly exhausted")
            }
            crate::dispatch::DeferredAdmissionAcquireOutcome::ParentCapacityExhausted(_) => {
                panic!("expiry acceptance parent capacity was unexpectedly exhausted")
            }
        };
        Self {
            _runtime: runtime,
            parent,
            session,
            request_id,
            admission,
            terminals,
            parts: Some(DeferredParts::new(responder, permit)),
        }
    }

    fn take_parts(&mut self) -> DeferredParts {
        self.parts.take().expect("fixture owns affine deferred parts")
    }
}

fn assert_recovered(fixture: &PartsFixture, parts: DeferredParts) {
    assert_eq!(parts.request_id(), fixture.request_id);
    assert_eq!(parts.session_id(), fixture.session.view().id());
    assert_eq!(fixture.admission.snapshot().waiting_count(), 1);
    assert_eq!(fixture.admission.snapshot().retained_bytes(), parts.retained_bytes());
    let responder = parts.into_responder();
    assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
    assert_eq!(
        responder
            .cancel()
            .expect("recovered responder remains affine and usable"),
        crate::dispatch::DeferredResponseOutcome::Cancelled
    );
}

#[tokio::test(start_paused = true)]
async fn affine_expiry_attachment_outcomes_return_the_exact_live_parts() {
    let now = Instant::now();
    let valid_margins = DeferredExpiryMargins::new(Duration::from_secs(2), Duration::from_secs(1));

    enum ExpectedExpiry {
        Contract(TransportContractViolation),
        Outcome(DeferredExpiryOutcome),
    }

    let cases = [
        (
            ExpectedExpiry::Contract(TransportContractViolation::DeferredExpiryZeroRecoveryMargin),
            None,
            now + Duration::from_secs(20),
            DeferredExpiryMargins::new(Duration::ZERO, Duration::from_secs(1)),
        ),
        (
            ExpectedExpiry::Contract(TransportContractViolation::DeferredExpiryZeroWriteMargin),
            None,
            now + Duration::from_secs(20),
            DeferredExpiryMargins::new(Duration::from_secs(1), Duration::ZERO),
        ),
        (
            ExpectedExpiry::Outcome(DeferredExpiryOutcome::ProtocolAlreadyExpired),
            None,
            now,
            valid_margins,
        ),
        (
            ExpectedExpiry::Outcome(DeferredExpiryOutcome::OwnerAlreadyExpired),
            Some(RequestDeadline::after(Duration::ZERO)),
            now + Duration::from_secs(20),
            valid_margins,
        ),
        (
            ExpectedExpiry::Outcome(DeferredExpiryOutcome::OwnerBudgetInsufficient),
            Some(RequestDeadline::after(Duration::from_secs(3))),
            now + Duration::from_secs(20),
            valid_margins,
        ),
    ];

    for (index, (expected, deadline, protocol_at, margins)) in cases.into_iter().enumerate() {
        let mut fixture = PartsFixture::new(9_810 + index as u64, deadline);
        let mut parts = fixture.take_parts();
        let result = parts.try_with_expiry(protocol_at, margins);
        match expected {
            ExpectedExpiry::Contract(expected) => assert_eq!(result, Err(expected)),
            ExpectedExpiry::Outcome(expected) => assert_eq!(result, Ok(expected)),
        }
        assert_recovered(&fixture, parts);
    }

    let mut fixture = PartsFixture::new(9_820, Some(RequestDeadline::after(Duration::from_secs(30))));
    let mut attached = fixture.take_parts();
    assert_eq!(
        attached.try_with_expiry(now + Duration::from_secs(20), valid_margins),
        Ok(DeferredExpiryOutcome::Attached)
    );
    let first_expiry = attached.expiry().expect("first expiry remains attached");
    assert_eq!(
        attached.try_with_expiry(
            now + Duration::from_secs(21),
            DeferredExpiryMargins::new(Duration::ZERO, Duration::ZERO),
        ),
        Ok(DeferredExpiryOutcome::AlreadyAttached)
    );
    assert_eq!(attached.expiry(), Some(first_expiry));
    assert_recovered(&fixture, attached);
}

#[tokio::test(start_paused = true)]
async fn equal_owner_and_protocol_boundaries_fail_closed_to_owner() {
    let now = Instant::now();
    let mut fixture = PartsFixture::new(9_821, Some(RequestDeadline::after(Duration::from_secs(10))));
    let mut parts = fixture.take_parts();
    assert_eq!(
        parts.try_with_expiry(
            now + Duration::from_secs(5),
            DeferredExpiryMargins::new(Duration::from_secs(3), Duration::from_secs(2)),
        ),
        Ok(DeferredExpiryOutcome::Attached)
    );
    let expiry = parts.expiry().expect("attached expiry");
    assert_eq!(expiry.resume_cutoff(), Some(expiry.protocol_at()));
    assert_eq!(expiry.kind(), DeferredExpiryKind::OwnerDeadline);
    assert_eq!(
        parts
            .into_responder()
            .cancel()
            .expect("equal-boundary parts remain usable"),
        crate::dispatch::DeferredResponseOutcome::Cancelled
    );
}

#[tokio::test(start_paused = true)]
async fn owner_only_deadline_rejects_claim_without_protocol_expiry_or_response() {
    let mut fixture = PartsFixture::new(9_822, Some(RequestDeadline::after(Duration::from_secs(5))));
    let registry = DeferredRegistry::<()>::new();
    let registration = match registry.register(DeferredRequest::new((), fixture.take_parts())) {
        DeferredRegistryOutcome::Registered(registration) => registration,
        DeferredRegistryOutcome::DuplicateRequest(_) => {
            panic!("owner-only deferred registration was classified as a duplicate")
        }
        DeferredRegistryOutcome::IdentityExhausted(_) => {
            panic!("owner-only deferred registration exhausted the identity space")
        }
        DeferredRegistryOutcome::ParentCancelled => {
            panic!("owner-only deferred registration was unexpectedly parent-cancelled")
        }
        DeferredRegistryOutcome::SessionClosed => {
            panic!("owner-only deferred registration was unexpectedly session-closed")
        }
        DeferredRegistryOutcome::DeadlineExpired => {
            panic!("owner-only deferred registration was unexpectedly deadline-expired")
        }
        DeferredRegistryOutcome::BuilderRejected { .. } => {
            panic!("owner-only deferred registration was unexpectedly builder-rejected")
        }
        DeferredRegistryOutcome::ContractViolation { .. } => {
            panic!("owner-only deferred registration violated a contract")
        }
        DeferredRegistryOutcome::OperationalFailure { .. } => {
            panic!("owner-only deferred registration failed operationally")
        }
    };
    let id = registration.deferred_id();
    registration.commit().expect("owner-only registration commit");
    assert_eq!(fixture.admission.snapshot().waiting_count(), 1);

    tokio::time::advance(Duration::from_secs(5)).await;
    let outcome = registry
        .claim(id, crate::dispatch::DeferredWakeReason::Timeout)
        .await
        .expect("expired owner-only request should converge to a normal outcome");
    assert!(matches!(outcome, DeferredClaimOutcome::DeadlineExpired));
    assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
    assert_eq!(
        fixture.terminals.lock().as_slice(),
        [("pull_message", DeferredTerminalReason::OwnerDeadline.as_str())]
    );
    assert_eq!(registry.sweep_expired(NonZeroUsize::MIN).stats().examined(), 0);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_priority_is_parent_then_session_then_owner_deadline() {
    enum ExpectedLifecycleOutcome {
        ParentCancelled,
        SessionClosed,
        DeadlineExpired,
    }

    let priorities = [
        (
            true,
            true,
            ExpectedLifecycleOutcome::ParentCancelled,
            DeferredTerminalReason::ParentCancelled,
        ),
        (
            false,
            true,
            ExpectedLifecycleOutcome::SessionClosed,
            DeferredTerminalReason::SessionClosed,
        ),
        (
            false,
            false,
            ExpectedLifecycleOutcome::DeadlineExpired,
            DeferredTerminalReason::OwnerDeadline,
        ),
    ];

    for (index, (cancel_parent, close_session, expected_kind, expected_reason)) in priorities.into_iter().enumerate() {
        let mut fixture = PartsFixture::new(9_830 + index as u64, Some(RequestDeadline::after(Duration::ZERO)));
        if close_session {
            fixture.session.close();
        }
        if cancel_parent {
            fixture.parent.cancel();
        }
        let outcome = DeferredRegistry::<()>::new().register(DeferredRequest::new((), fixture.take_parts()));
        match expected_kind {
            ExpectedLifecycleOutcome::ParentCancelled => {
                assert!(matches!(outcome, DeferredRegistryOutcome::ParentCancelled));
            }
            ExpectedLifecycleOutcome::SessionClosed => {
                assert!(matches!(outcome, DeferredRegistryOutcome::SessionClosed));
            }
            ExpectedLifecycleOutcome::DeadlineExpired => {
                assert!(matches!(outcome, DeferredRegistryOutcome::DeadlineExpired));
            }
        }
        assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
        assert_eq!(
            fixture.terminals.lock().as_slice(),
            [("pull_message", expected_reason.as_str())]
        );
    }
}
