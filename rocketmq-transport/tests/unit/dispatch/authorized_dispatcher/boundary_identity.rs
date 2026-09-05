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

use super::harness::*;
use crate::telemetry::TransportTelemetry;
#[tokio::test]
async fn pre_admission_deadline_and_admission_rejection_record_one_terminal_span_without_processor_clone() {
    let limits = AdmissionLimits {
        queued: crate::admission::ResourceLimit { count: 2, bytes: 1024 },
        control_reserve: crate::admission::ResourceLimit { count: 1, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let mut harness = DispatchHarness::new_with_limits("dispatch-pre-admission", limits).await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let (telemetry, boundary_metrics) = TransportTelemetry::with_boundary_metric_capture();
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new_with_telemetry(
        processor,
        Vec::new(),
        telemetry,
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    let outcome = dispatcher
        .dispatch(
            &harness.authorized,
            session,
            harness.context(Some(RequestDeadline::after(Duration::ZERO))),
            command,
            256,
            None,
        )
        .await
        .expect("expired request produces boundary response");
    assert_eq!(outcome, DispatchOutcome::Rejected);
    let response = harness.receive().await;
    assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    assert_eq!(boundary_metrics.snapshot(), (1, 1, 1, 1));
    assert_eq!(
        boundary_metrics.rejections(),
        vec![("deadline_expired", "failed", "rejected", "inline", "transport_written",)]
    );

    let queued = harness
        .admission_scope
        .try_acquire(AdmissionResource::Queued, 1, AdmissionClass::Data)
        .expect("hold the only queued permit");
    let command = request(false);
    let (session, _) = harness.request_session(&command);
    let outcome = dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("queue rejection produces boundary response");
    assert_eq!(outcome, DispatchOutcome::Rejected);
    let response = harness.receive().await;
    assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemBusy);
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    assert_eq!(boundary_metrics.snapshot(), (2, 2, 2, 2));
    assert_eq!(
        boundary_metrics.rejections(),
        vec![
            ("deadline_expired", "failed", "rejected", "inline", "transport_written",),
            (
                "admission_rejected",
                "failed",
                "rejected",
                "inline",
                "transport_written",
            ),
        ]
    );
    drop(queued);
    harness.shutdown().await;
}

#[tokio::test]
async fn undelivered_boundary_responses_close_the_session_path_without_an_operational_error() {
    let limits = AdmissionLimits {
        queued: crate::admission::ResourceLimit { count: 1, bytes: 1 },
        control_reserve: crate::admission::ResourceLimit { count: 0, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let mut saturated = DispatchHarness::new_with_limits("dispatch-boundary-response-saturated", limits).await;
    let (telemetry, saturated_metrics) = TransportTelemetry::with_boundary_metric_capture();
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new_with_telemetry(
        TestProcessor::new(Behavior::Reply).0,
        Vec::new(),
        telemetry,
    ));
    let command = request(false);
    let (session, _) = saturated.request_session(&command);
    let outcome = dispatcher
        .dispatch(
            &saturated.authorized,
            session,
            saturated.context(Some(RequestDeadline::after(Duration::ZERO))),
            command,
            256,
            None,
        )
        .await
        .expect("queue saturation is a source-free boundary-response outcome");
    assert_eq!(outcome, DispatchOutcome::CloseSession);
    assert_eq!(saturated_metrics.snapshot(), (1, 1, 1, 1));
    assert_eq!(saturated.admission_controller.snapshot().queued.rejected_count, 1);
    saturated.assert_no_response_frame().await;
    saturated.shutdown().await;

    let mut closed = DispatchHarness::new("dispatch-boundary-response-closed").await;
    let (telemetry, closed_metrics) = TransportTelemetry::with_boundary_metric_capture();
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new_with_telemetry(
        TestProcessor::new(Behavior::Reply).0,
        Vec::new(),
        telemetry,
    ));
    let command = request(false);
    let (session, _) = closed.request_session(&command);
    session.abort();
    let outcome = dispatcher
        .dispatch(
            &closed.authorized,
            session,
            closed.context(Some(RequestDeadline::after(Duration::ZERO))),
            command,
            256,
            None,
        )
        .await
        .expect("closed response ownership is a source-free boundary outcome");
    assert_eq!(outcome, DispatchOutcome::SessionClosed);
    assert_eq!(closed_metrics.snapshot(), (1, 1, 1, 1));
    closed.assert_no_response_frame().await;
    closed.shutdown().await;
}

#[tokio::test]
async fn authorization_denial_records_one_terminal_span_without_clone_hook_or_processor_observation() {
    let security = Arc::new(TransportSecurity::secure_enforced(None, None));
    let mut harness = DispatchHarness::new_with_security("dispatch-auth-denial", security).await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let (telemetry, boundary_metrics) = TransportTelemetry::with_boundary_metric_capture();
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new_with_telemetry(
        processor,
        vec![Arc::new(hook(false, false, Arc::clone(&hook_events)))],
        telemetry,
    ));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    let outcome = dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("authorization denial produces boundary response");
    assert_eq!(outcome, DispatchOutcome::Rejected);
    let response = harness.receive().await;

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
    assert_eq!(response.opaque(), 811);
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert_eq!(state.events.lock().expect("event lock").as_slice(), ["ordering"]);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    assert!(hook_events.lock().expect("hook events").is_empty());
    assert_eq!(boundary_metrics.snapshot(), (1, 1, 1, 1));
    assert_eq!(
        boundary_metrics.rejections(),
        vec![("security_denied", "failed", "rejected", "inline", "transport_written",)]
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn one_way_boundary_rejection_is_failed_without_a_response_write() {
    let mut harness = DispatchHarness::new("dispatch-oneway-deadline-rejection").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let (telemetry, boundary_metrics) = TransportTelemetry::with_boundary_metric_capture();
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new_with_telemetry(
        processor,
        Vec::new(),
        telemetry,
    ));
    let command = request(true);
    let (session, _) = harness.request_session(&command);

    let outcome = dispatcher
        .dispatch(
            &harness.authorized,
            session,
            harness.context(Some(RequestDeadline::after(Duration::ZERO))),
            command,
            256,
            None,
        )
        .await
        .expect("expired one-way request is rejected without a response write");

    assert_eq!(outcome, DispatchOutcome::Rejected);
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    harness.assert_no_response().await;
    assert_eq!(boundary_metrics.snapshot(), (1, 1, 1, 1));
    assert_eq!(
        boundary_metrics.rejections(),
        vec![("deadline_expired", "failed", "rejected", "no_response", "failed",)]
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn handler_route_one_way_boundary_rejections_do_not_write_frames() {
    let mut expired = DispatchHarness::new("handler-oneway-deadline-rejection").await;
    let command = request(true);
    let (session, original) = expired.request_session(&command);
    let outcome = expired
        .authorized
        .dispatch_handler(
            expired.context(Some(RequestDeadline::after(Duration::ZERO))),
            original,
            command,
            256,
            None,
            RequestOrdering::Concurrent,
            session,
            |_operation, _command| async {},
        )
        .await
        .expect("expired one-way handler request is rejected without a response write");
    assert_eq!(outcome, DispatchOutcome::Rejected);
    expired.assert_no_response().await;
    expired.shutdown().await;

    let security = Arc::new(TransportSecurity::secure_enforced(None, None));
    let mut denied = DispatchHarness::new_with_security("handler-oneway-security-rejection", security).await;
    let command = request(true);
    let (session, original) = denied.request_session(&command);
    let outcome = denied
        .authorized
        .dispatch_handler(
            denied.context(None),
            original,
            command,
            256,
            None,
            RequestOrdering::Concurrent,
            session,
            |_operation, _command| async {},
        )
        .await
        .expect("denied one-way handler request is rejected without a response write");
    assert_eq!(outcome, DispatchOutcome::Rejected);
    denied.assert_no_response().await;
    denied.shutdown().await;

    let limits = AdmissionLimits {
        queued: crate::admission::ResourceLimit { count: 1, bytes: 1024 },
        control_reserve: crate::admission::ResourceLimit { count: 1, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let mut saturated = DispatchHarness::new_with_limits("handler-oneway-admission-rejection", limits).await;
    let queued = saturated
        .admission_scope
        .try_acquire(AdmissionResource::Queued, 1, AdmissionClass::Data)
        .expect("hold the only queued permit");
    let command = request(true);
    let (session, original) = saturated.request_session(&command);
    let outcome = saturated
        .authorized
        .dispatch_handler(
            saturated.context(None),
            original,
            command,
            256,
            None,
            RequestOrdering::Concurrent,
            session,
            |_operation, _command| async {},
        )
        .await
        .expect("saturated one-way handler request is rejected without a response write");
    assert_eq!(outcome, DispatchOutcome::Rejected);
    saturated.assert_no_response().await;
    drop(queued);
    saturated.shutdown().await;
}

#[tokio::test]
async fn canonical_session_mismatch_fails_before_ordering_clone_or_response_capability_derivation() {
    let mut first = DispatchHarness::new("dispatch-owner-first").await;
    let mut second = DispatchHarness::new("dispatch-owner-second").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));
    let command = request(false);
    let (second_session, _) = second.request_session(&command);

    let error = dispatcher
        .dispatch(
            &first.authorized,
            second_session,
            second.context(None),
            command,
            256,
            None,
        )
        .await
        .expect_err("cross-session capability splice must fail closed");

    assert!(matches!(error, AuthorizedDispatchError::SessionMismatch));
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert!(state.events.lock().expect("event lock").is_empty());
    assert!(state.observations.lock().expect("observation lock").is_empty());
    first.assert_no_response().await;
    second.assert_no_response().await;
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn cross_owner_identity_splice_cannot_reach_structured_rejection() {
    let mut first = DispatchHarness::new("dispatch-identity-first-reject").await;
    let mut second = DispatchHarness::new("dispatch-identity-second-reject").await;
    let (processor, state) = TestProcessor::new(Behavior::Reject);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));
    let command = request(false);
    let (_, foreign_original) = second.request_session(&command);
    let spliced_session = first
        .session
        .clone()
        .with_original_request_identity(Some(foreign_original));

    let error = dispatcher
        .dispatch(
            &first.authorized,
            spliced_session,
            first.context(None),
            command,
            256,
            None,
        )
        .await
        .expect_err("foreign request identity must fail before structured rejection");

    assert!(matches!(error, AuthorizedDispatchError::SessionMismatch));
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    assert!(state.events.lock().expect("event lock").is_empty());
    assert!(state.observations.lock().expect("observation lock").is_empty());
    first.assert_no_response().await;
    second.assert_no_response().await;
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn cross_owner_identity_splice_cannot_queue_for_admitted_deadline_response() {
    let mut first = DispatchHarness::new("dispatch-identity-first-deadline").await;
    let mut second = DispatchHarness::new("dispatch-identity-second-deadline").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let dispatcher = Arc::new(TestAuthorizedDispatcherCore::new(processor, Vec::new()));

    let predecessor = request(false);
    let (predecessor_session, _) = first.request_session(&predecessor);
    dispatcher
        .dispatch(
            &first.authorized,
            predecessor_session,
            first.context(None),
            predecessor,
            256,
            None,
        )
        .await
        .expect("submit ordered predecessor");
    state.entered.notified().await;
    let ordering_events_before_splice = state.events.lock().expect("event lock").len();

    let spliced_command = request(false);
    let (_, foreign_original) = second.request_session(&spliced_command);
    let spliced_session = first
        .session
        .clone()
        .with_original_request_identity(Some(foreign_original));
    let error = dispatcher
        .dispatch(
            &first.authorized,
            spliced_session,
            first.context(Some(RequestDeadline::after(Duration::from_millis(10)))),
            spliced_command,
            256,
            None,
        )
        .await
        .expect_err("foreign identity must fail before entering ordered deadline queue");

    assert!(matches!(error, AuthorizedDispatchError::SessionMismatch));
    assert_eq!(
        state.events.lock().expect("event lock").len(),
        ordering_events_before_splice
    );
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());

    state.resume.notify_one();
    let response = first.receive().await;
    wait_for_observation_count(&state, 1).await;
    assert_eq!(response.code(), 71);
    first.assert_no_response().await;
    second.assert_no_response().await;
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    first.shutdown().await;
    second.shutdown().await;
}
