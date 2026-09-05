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

use super::*;

#[tokio::test(start_paused = true)]
async fn public_oneway_waiter_lets_the_admitted_deadline_candidate_complete_and_drain() {
    let fixture = EmbeddedFixture::new("embedded-public-oneway-deadline");
    let (processor, state, _) = TestProcessor::new(Behavior::Wait);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let outcome = {
        let dispatch = dispatcher.dispatch_embedded(
            &fixture.task_group,
            Principal::new("broker-proxy"),
            Some(RequestDeadline::after(Duration::from_millis(100))),
            request(true).0,
        );
        tokio::pin!(dispatch);
        tokio::select! {
            () = state.entered.notified() => {}
            result = &mut dispatch => panic!("wait processor completed before the deadline was advanced: {result:?}"),
        }

        tokio::time::advance(Duration::from_millis(100)).await;
        dispatch
            .await
            .expect("the processor deadline plan must complete through one-way policy")
    };
    assert!(matches!(outcome, EmbeddedDispatchOutcome::OneWay { .. }));
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.rejects.load(Ordering::SeqCst), 1);
    assert_eq!(state.processes.load(Ordering::SeqCst), 1);
    assert!(state.observations.lock().expect("observation lock").is_empty());
    assert_eq!(dispatcher.core.reported_failure_categories(), ["deadline"]);
    assert_eq!(fixture.task_group.task_count(), 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn admitted_non_oneway_deadline_response_is_observed_without_terminal_failure_reporting() {
    let fixture = EmbeddedFixture::new("embedded-post-queue-reply-deadline");
    let (processor, state, _) = TestProcessor::new(Behavior::Reply);
    let dispatcher = fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    );
    let command = request(false).0;
    let request_started = Instant::now();
    let (session_id, original) = capture_identity(&command).expect("embedded identity");
    let session = EmbeddedSessionRecord::new(session_id);
    let context = RequestContext::try_embedded_with_caller(
        EmbeddedCaller::BrokerProxy,
        Some(Principal::new("broker-proxy")),
        Some(RequestDeadline::after(Duration::ZERO)),
    )
    .expect("authenticated embedded context");
    let lifecycle = RequestLifecycleProvenance::from_embedded_session(&session, &fixture.task_group);
    let builder =
        RemotingRequestBuilder::new(original, request_started, context, lifecycle, command).reserve_deferred_response();
    assert_eq!(state.clones.load(Ordering::SeqCst), 0);
    let mut admitted_processor = dispatcher.core.clone_explicit_processor();

    let outcome = execute_admitted(
        &dispatcher.core,
        &mut admitted_processor,
        builder,
        original,
        request_started,
        false,
    )
    .await
    .expect("expired local handoff is a source-free deadline outcome");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::DeadlineExceeded));
    assert_eq!(state.clones.load(Ordering::SeqCst), 1);
    assert_eq!(state.rejects.load(Ordering::SeqCst), 0);
    assert_eq!(state.processes.load(Ordering::SeqCst), 0);
    assert!(dispatcher.core.reported_failure_categories().is_empty());
    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 1);
        assert!(matches!(
            observations[0].outcome(),
            ResponseWriteOutcome::Failed {
                completion: Some(crate::dispatch::ResponseCompletionOutcome::DeadlineExpired),
                progress: Some(crate::dispatch::WriteProgress::NotStarted),
            }
        ));
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn parent_cancellation_wins_an_inflight_processor_and_drains_under_the_existing_task_group() {
    let fixture = EmbeddedFixture::new("embedded-parent-cancel");
    let (processor, state, _) = TestProcessor::new(Behavior::Wait);
    let dispatcher = Arc::new(fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    ));
    let task_group = fixture.task_group.clone();
    let dispatch = tokio::spawn({
        let dispatcher = Arc::clone(&dispatcher);
        let task_group = task_group.clone();
        async move {
            dispatcher
                .dispatch_embedded(&task_group, Principal::new("broker-proxy"), None, request(false).0)
                .await
        }
    });
    state.entered.notified().await;
    task_group.cancel();
    let outcome = dispatch
        .await
        .expect("dispatch join")
        .expect("cancellation is a normal embedded dispatch outcome");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Cancelled));
    assert!(state.observations.lock().expect("observation lock").is_empty());
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_public_future_closes_terminal_and_session_while_accepted_work_remains_parent_owned() {
    let fixture = EmbeddedFixture::new("embedded-public-drop");
    let (processor, state, _) = TestProcessor::new(Behavior::Wait);
    let dispatcher = Arc::new(fixture.dispatcher(
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    ));
    let task_group = fixture.task_group.clone();
    let dispatch = tokio::spawn({
        let dispatcher = Arc::clone(&dispatcher);
        async move {
            dispatcher
                .dispatch_embedded(&task_group, Principal::new("broker-proxy"), None, request(false).0)
                .await
        }
    });
    state.entered.notified().await;
    dispatch.abort();
    assert!(dispatch
        .await
        .expect_err("public dispatch future must be dropped")
        .is_cancelled());

    state.resume.notify_one();
    dispatcher.core.wait_for_failure_report().await;
    assert_eq!(dispatcher.core.reported_failure_categories(), ["completion_closed"]);
    {
        let observations = state.observations.lock().expect("observation lock");
        assert_eq!(observations.len(), 1);
        assert!(matches!(
            observations[0].outcome(),
            ResponseWriteOutcome::Failed {
                completion: Some(crate::dispatch::ResponseCompletionOutcome::SessionClosed),
                progress: Some(crate::dispatch::WriteProgress::NotStarted),
            }
        ));
    }
    fixture.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn terminal_stop_priority_is_cancel_then_session_close_then_deadline() {
    let fixture = EmbeddedFixture::new("embedded-stop-priority");
    let deadline_record = EmbeddedSessionRecord::new(91);
    let deadline_meta = RequestMeta::new(Instant::now(), Some(RequestDeadline::after(Duration::from_millis(1))));
    let deadline_control = RequestControlView::from_meta(
        &deadline_meta,
        deadline_record.view().state().clone(),
        &fixture.task_group,
    );
    let (_deadline_sender, mut deadline_receiver) = terminal();
    deadline_receiver.attach_control(deadline_control, false);
    tokio::time::advance(Duration::from_millis(1)).await;
    let deadline_outcome = deadline_receiver
        .receive()
        .await
        .expect("deadline is a source-free terminal outcome");
    assert!(matches!(deadline_outcome, EmbeddedDispatchOutcome::DeadlineExceeded));

    let closed_record = EmbeddedSessionRecord::new(92);
    let closed_meta = RequestMeta::new(Instant::now(), Some(RequestDeadline::after(Duration::ZERO)));
    let closed_control =
        RequestControlView::from_meta(&closed_meta, closed_record.view().state().clone(), &fixture.task_group);
    let (_closed_sender, mut closed_receiver) = terminal();
    closed_receiver.attach_control(closed_control, false);
    closed_record.close();
    let closed_outcome = closed_receiver
        .receive()
        .await
        .expect("session close is a source-free terminal outcome");
    assert!(matches!(closed_outcome, EmbeddedDispatchOutcome::SessionClosed));

    let cancelled_record = EmbeddedSessionRecord::new(93);
    let cancelled_meta = RequestMeta::new(Instant::now(), Some(RequestDeadline::after(Duration::ZERO)));
    let cancelled_control = RequestControlView::from_meta(
        &cancelled_meta,
        cancelled_record.view().state().clone(),
        &fixture.task_group,
    );
    let (_cancelled_sender, mut cancelled_receiver) = terminal();
    cancelled_receiver.attach_control(cancelled_control, false);
    cancelled_record.close();
    fixture.task_group.cancel();

    let outcome = cancelled_receiver
        .receive()
        .await
        .expect("parent cancellation is a source-free terminal outcome");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Cancelled));
    fixture.shutdown().await;
}

#[tokio::test]
async fn terminal_distinguishes_an_external_stop_claim_from_receiver_drop() {
    let fixture = EmbeddedFixture::new("embedded-terminal-state");
    let record = EmbeddedSessionRecord::new(94);
    let meta = RequestMeta::new(Instant::now(), None);
    let control = RequestControlView::from_meta(&meta, record.view().state().clone(), &fixture.task_group);
    let (sender, mut receiver) = terminal();
    receiver.attach_control(control, false);

    fixture.task_group.cancel();
    let outcome = receiver
        .receive()
        .await
        .expect("parent cancellation must claim the terminal");
    assert!(matches!(outcome, EmbeddedDispatchOutcome::Cancelled));
    assert_eq!(
        sender.complete(Ok(EmbeddedDispatchOutcome::CompletionClosed)),
        terminal::TerminalPublishOutcome::AlreadyCompleted
    );

    let (sender, receiver) = terminal();
    drop(receiver);
    assert_eq!(
        sender.complete(Ok(EmbeddedDispatchOutcome::Cancelled)),
        terminal::TerminalPublishOutcome::ReceiverDropped
    );
    fixture.shutdown().await;
}
