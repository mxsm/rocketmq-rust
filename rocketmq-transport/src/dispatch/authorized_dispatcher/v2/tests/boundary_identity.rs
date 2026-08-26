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
#[tokio::test]
async fn pre_admission_deadline_and_admission_rejection_never_clone_or_observe_processor() {
    let limits = AdmissionLimits {
        queued: crate::admission::ResourceLimit { count: 2, bytes: 1024 },
        control_reserve: crate::admission::ResourceLimit { count: 1, bytes: 0 },
        ..AdmissionLimits::default()
    };
    let mut harness = DispatchHarness::new_with_limits("dispatch-v2-pre-admission", limits).await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
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
    drop(queued);
    harness.shutdown().await;
}

#[tokio::test]
async fn authorization_denial_runs_after_ordering_without_clone_hook_process_or_observation() {
    let security = Arc::new(TransportSecurity::secure_enforced(None, None));
    let mut harness = DispatchHarness::new_with_security("dispatch-v2-auth-denial", security).await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(hook(false, false, Arc::clone(&hook_events)))],
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
    harness.shutdown().await;
}

#[tokio::test]
async fn canonical_session_mismatch_fails_before_ordering_clone_or_response_capability_derivation() {
    let mut first = DispatchHarness::new("dispatch-v2-owner-first").await;
    let mut second = DispatchHarness::new("dispatch-v2-owner-second").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
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

    assert!(matches!(error, AuthorizedDispatchV2Error::SessionMismatch));
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
    let mut first = DispatchHarness::new("dispatch-v2-identity-first-reject").await;
    let mut second = DispatchHarness::new("dispatch-v2-identity-second-reject").await;
    let (processor, state) = TestProcessor::new(Behavior::Reject);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
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

    assert!(matches!(error, AuthorizedDispatchV2Error::SessionMismatch));
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
    let mut first = DispatchHarness::new("dispatch-v2-identity-first-deadline").await;
    let mut second = DispatchHarness::new("dispatch-v2-identity-second-deadline").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));

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

    assert!(matches!(error, AuthorizedDispatchV2Error::SessionMismatch));
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
