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
use crate::deadline::RequestDeadline;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredParts;
use crate::dispatch::DeferredRegistry;
use crate::dispatch::DeferredRegistryErrorKind;
use crate::dispatch::DeferredRequest;
use crate::dispatch::DeferredRetainedSizeParts;
use crate::dispatch::DeferredTerminalReason;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseSink;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

struct PartsFixture {
    _runtime: RuntimeOwner,
    parent: rocketmq_runtime::TaskGroup,
    session: Arc<EmbeddedSessionRecord>,
    admission: DeferredAdmission,
    terminals: Arc<parking_lot::Mutex<Vec<(&'static str, &'static str)>>>,
    parts: Option<DeferredParts>,
}

impl PartsFixture {
    fn new(owner: u64, deadline: Option<RequestDeadline>) -> Self {
        let runtime = RuntimeOwner::new(RuntimeConfig::server_default("deferred-expiry-acceptance"))
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
        let permit = admission.try_reserve(retained).expect("expiry acceptance permit");
        Self {
            _runtime: runtime,
            parent,
            session,
            admission,
            terminals,
            parts: Some(DeferredParts::new(responder, permit)),
        }
    }

    fn take_parts(&mut self) -> DeferredParts {
        self.parts.take().expect("fixture owns affine deferred parts")
    }
}

fn assert_recovered(fixture: &PartsFixture, error: DeferredExpiryError, expected: DeferredExpiryErrorKind) {
    assert_eq!(error.kind(), expected);
    let request_id = error.request_id();
    let parts = error.into_parts();
    assert_eq!(parts.request_id(), request_id);
    assert_eq!(parts.session_id(), fixture.session.view().id());
    assert_eq!(fixture.admission.snapshot().waiting_count(), 1);
    assert_eq!(fixture.admission.snapshot().retained_bytes(), parts.retained_bytes());
    let responder = parts.into_responder();
    assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
    responder
        .cancel()
        .expect("recovered responder remains affine and usable");
}

#[tokio::test(start_paused = true)]
async fn affine_expiry_attachment_errors_return_the_exact_live_parts() {
    let now = Instant::now();
    let valid_margins = DeferredExpiryMargins::new(Duration::from_secs(2), Duration::from_secs(1));

    let cases = [
        (
            DeferredExpiryErrorKind::ZeroRecoveryMargin,
            None,
            now + Duration::from_secs(20),
            DeferredExpiryMargins::new(Duration::ZERO, Duration::from_secs(1)),
        ),
        (
            DeferredExpiryErrorKind::ZeroWriteMargin,
            None,
            now + Duration::from_secs(20),
            DeferredExpiryMargins::new(Duration::from_secs(1), Duration::ZERO),
        ),
        (
            DeferredExpiryErrorKind::ProtocolAlreadyExpired,
            None,
            now,
            valid_margins,
        ),
        (
            DeferredExpiryErrorKind::OwnerAlreadyExpired,
            Some(RequestDeadline::after(Duration::ZERO)),
            now + Duration::from_secs(20),
            valid_margins,
        ),
        (
            DeferredExpiryErrorKind::OwnerBudgetInsufficient,
            Some(RequestDeadline::after(Duration::from_secs(3))),
            now + Duration::from_secs(20),
            valid_margins,
        ),
    ];

    for (index, (expected, deadline, protocol_at, margins)) in cases.into_iter().enumerate() {
        let mut fixture = PartsFixture::new(9_810 + index as u64, deadline);
        let error = fixture
            .take_parts()
            .try_with_expiry(protocol_at, margins)
            .expect_err("invalid expiry attachment must return ownership");
        assert_recovered(&fixture, error, expected);
    }

    let mut fixture = PartsFixture::new(9_820, Some(RequestDeadline::after(Duration::from_secs(30))));
    let attached = fixture
        .take_parts()
        .try_with_expiry(now + Duration::from_secs(20), valid_margins)
        .expect("first expiry attachment succeeds");
    let error = attached
        .try_with_expiry(now + Duration::from_secs(21), valid_margins)
        .expect_err("second expiry attachment fails without replacing ownership");
    assert_recovered(&fixture, error, DeferredExpiryErrorKind::AlreadyAttached);
}

#[tokio::test(start_paused = true)]
async fn equal_owner_and_protocol_boundaries_fail_closed_to_owner() {
    let now = Instant::now();
    let mut fixture = PartsFixture::new(9_821, Some(RequestDeadline::after(Duration::from_secs(10))));
    let parts = fixture
        .take_parts()
        .try_with_expiry(
            now + Duration::from_secs(5),
            DeferredExpiryMargins::new(Duration::from_secs(3), Duration::from_secs(2)),
        )
        .expect("equal boundary policy");
    let expiry = parts.expiry().expect("attached expiry");
    assert_eq!(expiry.resume_cutoff(), Some(expiry.protocol_at()));
    assert_eq!(expiry.kind(), DeferredExpiryKind::OwnerDeadline);
    parts
        .into_responder()
        .cancel()
        .expect("equal-boundary parts remain usable");
}

#[tokio::test(start_paused = true)]
async fn owner_only_deadline_rejects_claim_without_protocol_expiry_or_response() {
    let mut fixture = PartsFixture::new(9_822, Some(RequestDeadline::after(Duration::from_secs(5))));
    let registry = DeferredRegistry::<()>::new();
    let registration = registry
        .register(DeferredRequest::new((), fixture.take_parts()))
        .expect("owner-only deferred registration");
    let id = registration.deferred_id();
    registration.commit().expect("owner-only registration commit");
    assert_eq!(fixture.admission.snapshot().waiting_count(), 1);

    tokio::time::advance(Duration::from_secs(5)).await;
    let error = registry
        .claim(id, crate::dispatch::DeferredWakeReason::Timeout)
        .await
        .expect_err("expired owner-only request cannot be claimed");
    assert_eq!(error.kind(), crate::dispatch::DeferredClaimErrorKind::DeadlineExpired);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(DeferredTerminalReason::OwnerDeadline)
    );
    assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
    assert_eq!(registry.sweep_expired(NonZeroUsize::MIN).stats().examined(), 0);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_priority_is_parent_then_session_then_owner_deadline() {
    let priorities = [
        (
            true,
            true,
            DeferredRegistryErrorKind::ParentCancelled,
            DeferredTerminalReason::ParentCancelled,
        ),
        (
            false,
            true,
            DeferredRegistryErrorKind::SessionClosed,
            DeferredTerminalReason::SessionClosed,
        ),
        (
            false,
            false,
            DeferredRegistryErrorKind::DeadlineExpired,
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
        let error = DeferredRegistry::<()>::new()
            .register(DeferredRequest::new((), fixture.take_parts()))
            .expect_err("stopped lifecycle cannot register");
        assert_eq!(error.kind(), expected_kind);
        assert!(error.into_request().is_none(), "lifecycle failure consumes ownership");
        assert_eq!(fixture.admission.snapshot().waiting_count(), 0);
        assert_eq!(
            fixture.terminals.lock().as_slice(),
            [("pull_message", expected_reason.as_str())]
        );
    }
}
