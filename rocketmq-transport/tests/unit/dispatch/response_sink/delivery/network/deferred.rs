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
    let sink = ResponseSink::network(harness.session.clone(), AdmissionClass::Data, control);
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
            RemotingResponse::bytes(response_head(129, 7), Bytes::from_static(b"canonical-deferred"))
                .expect("deferred remoting response"),
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
            .send_response(
                RemotingResponse::command(response_head(130, 8))
                    .expect("duplicate remoting response")
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
    let sink = ResponseSink::network_with_enqueue_observer(
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
        sink.send_deferred_response(
            bind(
                RemotingResponse::bytes(response_head(122, 1_122), Bytes::from_static(b"deferred-waiting"))
                    .expect("remoting response"),
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
    assert_eq!(state_for_assert.terminal_reason(), None);
    let before_duplicate = harness.session.writer_snapshot();
    let duplicate_error = duplicate
        .send_response(bind(
            RemotingResponse::command(response_head(123, 1_123)).expect("duplicate response"),
            832,
            1_123,
        ))
        .await;
    assert!(matches!(
        duplicate_error,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));
    let after_duplicate = harness.session.writer_snapshot();
    assert_eq!(after_duplicate.accepted, before_duplicate.accepted);
    assert_eq!(after_duplicate.queued_items, before_duplicate.queued_items);

    resume.notify_one();
    blocker.await.expect("blocker task");
    assert_eq!(harness.receive().await.opaque(), 1_121);
    tokio::time::timeout(Duration::from_secs(2), async {
        while harness.session.writer_snapshot().queued_items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled deferred envelope should leave the writer queue");
    let writer = harness.session.writer_snapshot();
    assert_eq!(writer.accepted, 2);
    assert_eq!(writer.queued_items, 0);
    assert_eq!(writer.queued_bytes, 0);
    assert_eq!(writer.completed, 1);
    assert_eq!(writer.failed, 1);
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
    let sink = ResponseSink::network_with_enqueue_observer(
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
        sink.send_deferred_response(
            bind(
                RemotingResponse::bytes(response_head(124, 1_124), Bytes::from_static(b"deferred-started"))
                    .expect("remoting response"),
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
    assert_eq!(state_for_assert.terminal_reason(), None);
    let before_duplicate = harness.session.writer_snapshot();
    let duplicate_error = duplicate
        .send_response(bind(
            RemotingResponse::command(response_head(125, 1_125)).expect("duplicate response"),
            834,
            1_125,
        ))
        .await;
    assert!(matches!(
        duplicate_error,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        })
    ));
    let after_duplicate = harness.session.writer_snapshot();
    assert_eq!(after_duplicate.accepted, before_duplicate.accepted);
    assert_eq!(after_duplicate.queued_items, before_duplicate.queued_items);

    resume.notify_one();
    assert_eq!(harness.receive().await.opaque(), 1_124);
    let writer = harness.session.writer_snapshot();
    assert_eq!(writer.accepted, 1);
    assert_eq!(writer.queued_items, 0);
    assert_eq!(writer.queued_bytes, 0);
    assert_eq!(writer.completed, 1);
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
    let sink = ResponseSink::network_with_enqueue_observer(
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
        sink.send_deferred_response(
            bind(
                RemotingResponse::bytes(response_head(127, 1_127), Bytes::from_static(b"expired"))
                    .expect("remoting response"),
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
    assert_eq!(state_for_assert.terminal_reason(), None);
    let before_duplicate = harness.session.writer_snapshot();
    let duplicate_error = duplicate
        .send_response(bind(
            RemotingResponse::command(response_head(128, 1_128)).expect("duplicate response"),
            836,
            1_128,
        ))
        .await;
    assert!(matches!(
        duplicate_error,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));
    let after_duplicate = harness.session.writer_snapshot();
    assert_eq!(after_duplicate.accepted, before_duplicate.accepted);
    assert_eq!(after_duplicate.queued_items, before_duplicate.queued_items);

    resume.notify_one();
    blocker.await.expect("blocker task");
    assert_eq!(harness.receive().await.opaque(), 1_126);
    tokio::time::timeout(Duration::from_secs(2), async {
        while harness.session.writer_snapshot().queued_items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("expired deferred envelope should leave the writer queue");
    let writer = harness.session.writer_snapshot();
    assert_eq!(writer.accepted, 2);
    assert_eq!(writer.queued_items, 0);
    assert_eq!(writer.queued_bytes, 0);
    assert_eq!(writer.completed, 1);
    assert_eq!(writer.failed, 1);
    assert_eq!(writer.deadline_expired, 1);
    harness.shutdown().await;
}
