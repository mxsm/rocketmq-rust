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
use crate::dispatch::ResponseState;

fn deadline_responder(
    harness: &NetworkHarness,
    control: RequestControlView,
    sequence: u64,
    enqueued: Arc<tokio::sync::Notify>,
) -> (
    crate::dispatch::DeferredResponder,
    Arc<ResponseState>,
    OriginalRequestIdentity,
) {
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        enqueued,
    );
    let seed = sink
        .network_deferred_seed(&harness.session)
        .expect("canonical deferred deadline seed");
    let command = RemotingCommand::create_remoting_command(31).set_opaque(-(sequence as i32));
    let original = OriginalRequestIdentity::capture(
        harness.session.session_view().id().owner_id(),
        &AtomicU64::new(sequence),
        &command,
    )
    .expect("deadline request identity");
    let responder = seed.into_responder(original);
    let state = Arc::clone(responder.response_state());
    responder.register().expect("deadline responder registration");
    responder.claim().expect("deadline responder claim");
    (responder, state, original)
}

#[tokio::test]
async fn production_seed_shares_the_canonical_slot_and_rejects_another_session_owner() {
    let mut harness = NetworkHarness::new(
        "deferred-network-canonical-seed",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let other = NetworkHarness::new(
        "deferred-network-canonical-seed-other",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let session_id = harness.session.session_view().id();
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        harness.session.session_view().state().clone(),
        harness.session.task_group(),
    );
    let sink = ResponseSink::network_plan(harness.session.clone(), AdmissionClass::Data, control);
    assert!(sink.network_deferred_seed(&other.session).is_none());
    let seed = sink
        .network_deferred_seed(&harness.session)
        .expect("the canonical session owner can mint one private deferred seed");
    let duplicate = sink.clone();
    let command = RemotingCommand::create_remoting_command(31).set_opaque(-1_129);
    let original = OriginalRequestIdentity::capture(session_id.owner_id(), &AtomicU64::new(1), &command)
        .expect("request identity");

    let receipt = seed
        .into_responder(original)
        .respond(
            ResponsePlan::bytes(response_head(129, 7), Bytes::from_static(b"canonical-deferred"))
                .expect("deferred response plan"),
        )
        .await
        .expect("deferred response uses the canonical writer");
    assert_eq!(receipt.request_id(), original.request_id());
    assert_eq!(
        harness.receive().await.body().map(Bytes::as_ref),
        Some(&b"canonical-deferred"[..])
    );
    assert!(matches!(
        duplicate
            .send_plan(
                ResponsePlan::command(response_head(130, 8))
                    .expect("duplicate response plan")
                    .bind(original)
                    .expect("duplicate binding")
            )
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Completed
        })
    ));

    other.shutdown().await;
    harness.shutdown().await;
}

#[tokio::test]
async fn dropping_a_waiting_deferred_send_completes_rsp_and_def_not_started() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "deferred-network-waiting-drop",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move {
        blocker_connection
            .send_command(response_head(121, 1_121))
            .await
            .expect("blocker should write after resume");
    });
    checked.notified().await;

    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = harness.control("deferred-network-waiting-drop-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let state = Arc::new(ResponseState::open());
    let state_for_assert = Arc::clone(&state);
    let mut claim = state.begin_sending().expect("deferred send claim");
    let send = tokio::spawn(async move {
        sink.send_deferred_plan(
            bind(
                ResponsePlan::bytes(response_head(122, 1_122), Bytes::from_static(b"deferred-waiting"))
                    .expect("response plan"),
                831,
                1_122,
            ),
            &mut claim,
        )
        .await
    });
    enqueued.notified().await;
    send.abort();
    assert!(send.await.expect_err("aborted deferred send").is_cancelled());
    assert_eq!(
        state_for_assert.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted
        })
    );
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(123, 1_123)).expect("duplicate plan"),
                832,
                1_123,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));

    resume.notify_one();
    blocker.await.expect("blocker task");
    assert_eq!(harness.receive().await.opaque(), 1_121);
    harness.shutdown().await;
}

#[tokio::test]
async fn dropping_a_writer_claimed_deferred_send_completes_rsp_and_def_possibly_partial() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "deferred-network-claimed-drop",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = harness.control("deferred-network-claimed-drop-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let state = Arc::new(ResponseState::open());
    let state_for_assert = Arc::clone(&state);
    let mut claim = state.begin_sending().expect("deferred send claim");
    let send = tokio::spawn(async move {
        sink.send_deferred_plan(
            bind(
                ResponsePlan::bytes(response_head(124, 1_124), Bytes::from_static(b"deferred-started"))
                    .expect("response plan"),
                833,
                1_124,
            ),
            &mut claim,
        )
        .await
    });
    enqueued.notified().await;
    checked.notified().await;
    send.abort();
    assert!(send.await.expect_err("aborted deferred send").is_cancelled());
    assert_eq!(
        state_for_assert.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::PossiblyPartial
        })
    );
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(125, 1_125)).expect("duplicate plan"),
                834,
                1_125,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        })
    ));

    resume.notify_one();
    assert_eq!(harness.receive().await.opaque(), 1_124);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn prestart_deadline_resumes_both_outer_claims_without_a_delegated_sending_leak() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "deferred-network-prestart-deadline",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move {
        blocker_connection
            .send_command(response_head(126, 1_126))
            .await
            .expect("blocker should write after resume");
    });
    checked.notified().await;

    let enqueued = Arc::new(tokio::sync::Notify::new());
    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let (control, _parent) = harness.control("deferred-network-deadline-control", Some(deadline));
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let state = Arc::new(ResponseState::open());
    let state_for_assert = Arc::clone(&state);
    let mut claim = state.begin_sending().expect("deferred send claim");
    let send = tokio::spawn(async move {
        sink.send_deferred_plan(
            bind(
                ResponsePlan::bytes(response_head(127, 1_127), Bytes::from_static(b"expired")).expect("response plan"),
                835,
                1_127,
            ),
            &mut claim,
        )
        .await
    });
    enqueued.notified().await;
    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        send.await.expect("deferred send task"),
        Err(ResponseError::DeadlineExceeded)
    ));
    assert_eq!(
        state_for_assert.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted
        })
    );
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(128, 1_128)).expect("duplicate plan"),
                836,
                1_128,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));

    resume.notify_one();
    blocker.await.expect("blocker task");
    assert_eq!(harness.receive().await.opaque(), 1_126);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn canonical_deferred_deadline_bypass_reaches_the_post_encode_enqueue_gate() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut harness = NetworkHarness::new(
        "deferred-deadline-post-encode-bypass",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), Some(deadline)),
        harness.session.session_view().state().clone(),
        harness.session.task_group(),
    );
    let sink = ResponseSink::network_plan_with_enqueue_gate(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&checked),
        Arc::clone(&resume),
    );
    let seed = sink
        .network_deferred_seed(&harness.session)
        .expect("canonical network seed");
    let command = RemotingCommand::create_remoting_command(31).set_opaque(-1_131);
    let original = OriginalRequestIdentity::capture(
        harness.session.session_view().id().owner_id(),
        &AtomicU64::new(1),
        &command,
    )
    .expect("request identity");
    let responder = seed.into_responder(original);
    responder.register().expect("registry response registration");
    responder.claim().expect("registry response claim");
    tokio::time::advance(Duration::from_millis(10)).await;
    let send = tokio::spawn(responder.respond_deadline());
    checked.notified().await;
    resume.notify_one();
    let receipt = send
        .await
        .expect("deadline response task")
        .expect("deadline bypass ignores only the expired request deadline");
    assert_eq!(receipt.request_id(), original.request_id());
    let response = harness.receive().await;
    assert_eq!(response.opaque(), original.original_opaque());
    assert_eq!(
        rocketmq_protocol::code::response_code::ResponseCode::from(response.code()),
        rocketmq_protocol::code::response_code::ResponseCode::SystemError
    );
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn expired_canonical_deadline_remains_writable_while_waiting_behind_the_session_writer() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "deferred-deadline-writer-wait",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move { blocker_connection.send_command(response_head(132, 1_132)).await });
    checked.notified().await;

    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), Some(deadline)),
        harness.session.session_view().state().clone(),
        harness.session.task_group(),
    );
    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (responder, state, original) = deadline_responder(&harness, control, 1_132, Arc::clone(&enqueued));
    tokio::time::advance(Duration::from_millis(10)).await;
    let send = tokio::spawn(responder.respond_deadline());
    enqueued.notified().await;
    assert_eq!(state.terminal_state(), None);
    resume.notify_one();
    blocker.await.expect("blocker task").expect("blocker write completes");
    let receipt = send
        .await
        .expect("deadline task")
        .expect("expired deadline is bypassed at writer wait");
    assert_eq!(receipt.request_id(), original.request_id());
    assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Completed));
    assert_eq!(harness.receive().await.opaque(), 1_132);
    assert_eq!(harness.receive().await.opaque(), original.original_opaque());
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn parent_cancellation_still_wins_for_an_expired_deadline_response_waiting_in_the_writer_queue() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "deferred-deadline-parent-wins",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move { blocker_connection.send_command(response_head(133, 1_133)).await });
    checked.notified().await;

    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), Some(deadline)),
        harness.session.session_view().state().clone(),
        harness.session.task_group(),
    );
    let parent = harness.session.task_group().clone();
    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (responder, state, _original) = deadline_responder(&harness, control, 1_133, Arc::clone(&enqueued));
    tokio::time::advance(Duration::from_millis(10)).await;
    let send = tokio::spawn(responder.respond_deadline());
    enqueued.notified().await;
    parent.cancel();
    let error = send
        .await
        .expect("deadline task")
        .expect_err("parent cancellation must not be bypassed");
    assert_eq!(error.kind(), crate::dispatch::DeferredResponseErrorKind::Cancelled);
    assert_eq!(
        state.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted
        })
    );
    resume.notify_one();
    blocker.await.expect("blocker task").expect("blocker write completes");
    assert_eq!(harness.receive().await.opaque(), 1_133);
    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn session_close_still_wins_for_an_expired_deadline_response_waiting_in_the_writer_queue() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let harness = NetworkHarness::new(
        "deferred-deadline-session-wins",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move { blocker_connection.send_command(response_head(134, 1_134)).await });
    checked.notified().await;

    let deadline = RequestDeadline::after(Duration::from_millis(10));
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), Some(deadline)),
        harness.session.session_view().state().clone(),
        harness.session.task_group(),
    );
    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (responder, state, _original) = deadline_responder(&harness, control, 1_134, Arc::clone(&enqueued));
    tokio::time::advance(Duration::from_millis(10)).await;
    let send = tokio::spawn(responder.respond_deadline());
    enqueued.notified().await;
    harness.session.abort();
    let error = send
        .await
        .expect("deadline task")
        .expect_err("session close must not be bypassed");
    assert_eq!(error.kind(), crate::dispatch::DeferredResponseErrorKind::SessionClosed);
    assert_eq!(
        state.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted
        })
    );
    resume.notify_one();
    let _ = blocker.await.expect("blocker task observes session close");
    harness.shutdown().await;
}
