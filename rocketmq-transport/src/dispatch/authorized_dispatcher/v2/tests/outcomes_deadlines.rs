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
async fn one_way_reply_and_structured_rejection_are_consumed_without_write_or_observation() {
    for (name, behavior, expected_processes) in [
        ("dispatch-v2-oneway-reply", Behavior::Reply, 1),
        ("dispatch-v2-oneway-reject", Behavior::Reject, 0),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let (processor, state) = TestProcessor::new(behavior);
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
        let command = request(true);
        let (session, _) = harness.request_session(&command);

        dispatcher
            .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
            .await
            .expect("dispatch one-way request");
        harness.drain_requests().await;
        harness.assert_no_response().await;

        assert_eq!(state.clones.load(Ordering::SeqCst), 1);
        assert_eq!(state.processes.load(Ordering::SeqCst), expected_processes);
        assert!(state.observations.lock().expect("observation lock").is_empty());
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn one_way_processor_error_reports_fixed_category_without_write_observation() {
    let mut harness = DispatchHarness::new("dispatch-v2-oneway-processor-error").await;
    let (processor, state) = TestProcessor::new(Behavior::Error);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
    let command = request(true);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("submit one-way processor error");
    tokio::time::timeout(Duration::from_secs(2), async {
        while dispatcher.reported_failure_categories().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one-way processor error reporter");
    harness.drain_requests().await;

    assert_eq!(dispatcher.reported_failure_categories(), ["processor_error"]);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn one_way_processor_and_after_hook_errors_report_one_combined_category() {
    let mut harness = DispatchHarness::new("dispatch-v2-oneway-combined-error").await;
    let (processor, state) = TestProcessor::new(Behavior::Error);
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
        processor,
        vec![Arc::new(hook(true, false, Arc::clone(&hook_events)))],
    ));
    let command = request(true);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("submit combined one-way failure");
    tokio::time::timeout(Duration::from_secs(2), async {
        while dispatcher.reported_failure_categories().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("combined one-way failure reporter");
    harness.drain_requests().await;

    assert_eq!(
        dispatcher.reported_failure_categories(),
        ["processor_error_after_hook_error"]
    );
    assert_eq!(
        hook_events.lock().expect("hook event lock").as_slice(),
        ["before", "after"]
    );
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.assert_no_response().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn queued_deadline_expiry_suppresses_original_oneway_before_binding_or_write() {
    let mut harness = DispatchHarness::new("dispatch-v2-queued-oneway-deadline").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));

    let first = request(false);
    let (first_session, _) = harness.request_session(&first);
    dispatcher
        .dispatch(
            &harness.authorized,
            first_session,
            harness.context(None),
            first,
            256,
            None,
        )
        .await
        .expect("submit ordered predecessor");
    state.entered.notified().await;

    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let second = request(true);
    let (second_session, _) = harness.request_session(&second);
    dispatcher
        .dispatch(
            &harness.authorized,
            second_session,
            harness.context(Some(deadline)),
            second,
            256,
            None,
        )
        .await
        .expect("queue ordered one-way request");
    tokio::time::sleep(deadline.remaining()).await;
    assert!(deadline.is_expired());

    state.resume.notify_one();
    let response = harness.receive().await;
    assert_eq!(response.code(), 71, "only the ordered predecessor may write");
    harness.drain_requests().await;
    harness.assert_no_response().await;

    assert_eq!(state.clones.load(Ordering::SeqCst), 2);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert_eq!(state.observations.lock().expect("observation lock").len(), 1);
    harness.shutdown().await;
}

#[tokio::test]
async fn direct_expired_oneway_admission_completes_without_reaching_binding() {
    let mut harness = DispatchHarness::new("dispatch-v2-direct-oneway-deadline").await;
    let (processor, state) = TestProcessor::new(Behavior::Reply);
    let dispatcher = AuthorizedCommandDispatcherV2::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
    let command = request(true);
    let (session, original) = harness.request_session(&command);
    let builder = RemotingRequestBuilder::new(
        original,
        Instant::now(),
        harness.context(Some(RequestDeadline::after(Duration::ZERO))),
        RequestLifecycleProvenance::from_network_session(&session),
        command,
    );

    dispatcher
        .execute_admitted(
            processor,
            session.clone(),
            AdmissionClass::Data,
            original,
            session.remote_addr(),
            Instant::now(),
            builder,
        )
        .await
        .expect("expired one-way plan must be consumed before binding");

    harness.assert_no_response().await;
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn one_way_deferred_outcome_is_a_policy_error_without_inline_write_or_observation() {
    let mut harness = DispatchHarness::new("dispatch-v2-oneway-deferred-policy").await;
    let (processor, state) = TestProcessor::new(Behavior::Deferred);
    let dispatcher = AuthorizedCommandDispatcherV2::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
    let builder_command = request(false);
    let (session, builder_original) = harness.request_session(&builder_command);
    let builder = RemotingRequestBuilder::new(
        builder_original,
        Instant::now(),
        harness.context(None),
        RequestLifecycleProvenance::from_network_session(&session),
        builder_command,
    )
    .reserve_deferred_response();
    // DSP-02 rejects reserving deferred capability for a one-way request.
    // Calling the private admitted seam with a separate immutable one-way
    // identity isolates the post-resolution policy branch defensively.
    let one_way_command = request(true);
    let one_way_original =
        OriginalRequestIdentity::capture(session.session_id(), &AtomicU64::new(41), &one_way_command)
            .expect("capture one-way policy identity");

    let result = dispatcher
        .execute_admitted(
            processor,
            session.clone(),
            AdmissionClass::Data,
            one_way_original,
            session.remote_addr(),
            Instant::now(),
            builder,
        )
        .await;

    assert!(matches!(
        result,
        Err(AuthorizedDispatchV2Error::OneWayOutcome { outcome: "deferred" })
    ));
    harness.assert_no_response().await;
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn one_way_no_reply_marker_failure_maps_resolves_and_drops_without_write() {
    let mut harness = DispatchHarness::new("dispatch-v2-oneway-no-reply-policy").await;
    let (processor, state) = TestProcessor::new(Behavior::NoReply);
    let dispatcher = AuthorizedCommandDispatcherV2::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
    let mut command = request(true).set_code(39);
    command.set_opaque_mut(811);
    let (session, original) = harness.request_session(&command);
    let builder = RemotingRequestBuilder::new(
        original,
        Instant::now(),
        harness.context(None),
        RequestLifecycleProvenance::from_network_session(&session),
        command,
    );

    dispatcher
        .execute_admitted(
            processor,
            session.clone(),
            AdmissionClass::Control,
            original,
            session.remote_addr(),
            Instant::now(),
            builder,
        )
        .await
        .expect("one-way marker construction error maps to a consumed reply");

    harness.assert_no_response().await;
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn queued_deadline_expiry_attempts_one_plan_write_and_observes_not_started() {
    let mut harness = DispatchHarness::new("dispatch-v2-queued-deadline-observation").await;
    let (processor, state) = TestProcessor::new(Behavior::WaitReply);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));

    let first = request(false);
    let (first_session, _) = harness.request_session(&first);
    dispatcher
        .dispatch(
            &harness.authorized,
            first_session,
            harness.context(None),
            first,
            256,
            None,
        )
        .await
        .expect("submit ordered predecessor");
    state.entered.notified().await;

    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let second = request(false);
    let (second_session, _) = harness.request_session(&second);
    dispatcher
        .dispatch(
            &harness.authorized,
            second_session,
            harness.context(Some(deadline)),
            second,
            256,
            None,
        )
        .await
        .expect("queue ordered deadline request");
    tokio::time::sleep(deadline.remaining()).await;
    assert!(deadline.is_expired());

    state.resume.notify_one();
    let response = harness.receive().await;
    assert_eq!(response.code(), 71, "only the ordered predecessor may write");
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.observations.lock().expect("observation lock").len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued deadline observation");
    harness.assert_no_response().await;

    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 2);
        assert!(matches!(
            observations[1].outcome(),
            ResponseWriteOutcomeV2::Failed {
                kind: ResponseErrorKind::DeadlineExceeded,
                progress: Some(WriteProgress::NotStarted),
            }
        ));
    }
    assert_eq!(state.clones.load(Ordering::SeqCst), 2);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    harness.shutdown().await;
}

#[tokio::test]
async fn legal_protocol_no_response_resolves_without_inline_write_or_observation() {
    let mut harness = DispatchHarness::new("dispatch-v2-no-reply").await;
    let (processor, state) = TestProcessor::new(Behavior::NoReply);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
    let mut command = request(false).set_code(39);
    command.set_opaque_mut(811);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("dispatch protocol no-response");
    harness.drain_requests().await;
    harness.assert_no_response().await;

    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.shutdown().await;
}

#[tokio::test]
async fn deferred_transfer_and_reply_after_defer_resolve_once_without_inline_write() {
    for (name, behavior, expected_error) in [
        ("dispatch-v2-deferred", Behavior::Deferred, false),
        ("dispatch-v2-reply-after-defer", Behavior::ReplyAfterDeferred, true),
    ] {
        let mut harness = DispatchHarness::new(name).await;
        let (processor, state) = TestProcessor::new(behavior);
        let dispatcher = AuthorizedCommandDispatcherV2::new(TestProcessor::new(Behavior::Reply).0, Vec::new());
        let command = request(false);
        let (session, original) = harness.request_session(&command);
        let builder = RemotingRequestBuilder::new(
            original,
            Instant::now(),
            harness.context(None),
            RequestLifecycleProvenance::from_network_session(&session),
            command,
        )
        .reserve_deferred_response();

        let result = dispatcher
            .execute_admitted(
                processor,
                session.clone(),
                AdmissionClass::Data,
                original,
                session.remote_addr(),
                Instant::now(),
                builder,
            )
            .await;

        if expected_error {
            assert!(matches!(
                result,
                Err(AuthorizedDispatchV2Error::HandlerContract(
                    HandlerOutcomeContractError::ReplyAfterDeferredTaken
                ))
            ));
        } else {
            result.expect("sealed deferred proof should complete without an inline write");
        }
        harness.assert_no_response().await;
        assert_eq!(state.processes.load(Ordering::SeqCst), 1);
        assert!(state.observations.lock().expect("observation lock").is_empty());
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn real_dispatch_reports_handler_contract_failure_with_fixed_category() {
    let mut harness = DispatchHarness::new("dispatch-v2-handler-contract-report").await;
    let (processor, state) = TestProcessor::new(Behavior::UnclaimedDeferred);
    let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(processor, Vec::new()));
    let command = request(false);
    let (session, _) = harness.request_session(&command);

    dispatcher
        .dispatch(&harness.authorized, session, harness.context(None), command, 256, None)
        .await
        .expect("submit handler-contract failure");
    tokio::time::timeout(Duration::from_secs(2), async {
        while dispatcher.reported_failure_categories().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler-contract reporter");
    harness.drain_requests().await;

    assert_eq!(dispatcher.reported_failure_categories(), ["handler_contract"]);
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    harness.assert_no_response().await;
    harness.shutdown().await;
}
