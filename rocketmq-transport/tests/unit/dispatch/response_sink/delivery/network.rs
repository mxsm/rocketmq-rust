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

#[path = "network/deferred.rs"]
mod deferred;

fn expect_completed(
    result: Result<ResponseCompletionOutcome, ResponseOperationalFailure>,
    context: &str,
) -> ResponseReceipt {
    match result {
        Ok(ResponseCompletionOutcome::Completed(receipt)) => receipt,
        Ok(other) => panic!("{context}: unexpected normal response outcome {other:?}"),
        Err(error) => panic!("{context}: unexpected operational response failure {error:?}"),
    }
}

#[tokio::test]
async fn network_prepares_and_writes_all_four_bodies_before_issuing_receipts() {
    let mut harness = NetworkHarness::new(
        "network-response-four-bodies",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let encodes = Arc::new(AtomicUsize::new(0));
    let (file_response, file_accesses, file_drops) = counting_file_response(94, 903, b"file-body");
    let responses = [
        RemotingResponse::command(response_head(91, 900)).expect("empty remoting response"),
        RemotingResponse::bytes(
            response_head(92, 901).set_command_custom_header(CountingHeader {
                encodes: Arc::clone(&encodes),
            }),
            Bytes::from_static(b"bytes-body"),
        )
        .expect("bytes remoting response"),
        RemotingResponse::segments(
            response_head(93, 902),
            vec![Bytes::from_static(b"segment-"), Bytes::from_static(b"body")],
        )
        .expect("segments remoting response"),
        file_response,
    ];
    let expected_bodies: [&[u8]; 4] = [b"", b"bytes-body", b"segment-body", b"file-body"];
    assert_eq!(file_accesses.load(Ordering::SeqCst), 1);
    assert_eq!(file_drops.load(Ordering::SeqCst), 0);

    for (index, (response, expected_body)) in responses.into_iter().zip(expected_bodies).enumerate() {
        let owner = 801 + index as u64;
        let (control, _parent) = harness.control("network-response-four-bodies-control", None);
        let sink = ResponseSink::network(harness.session.clone(), AdmissionClass::Data, control);
        let flushes_before = harness.flushes.load(Ordering::SeqCst);
        let receipt = expect_completed(
            sink.send_response(bind(response, owner, 1_001 + index as i32)).await,
            "network response should complete through writer",
        );
        assert_eq!(receipt.request_id().owner_id(), owner);
        assert_eq!(receipt.disposition(), ResponseDisposition::TransportWritten);
        assert!(harness.flushes.load(Ordering::SeqCst) > flushes_before);
        let received = harness.receive().await;
        assert_eq!(received.opaque(), 1_001 + index as i32);
        assert_eq!(received.body().map(Bytes::as_ref).unwrap_or_default(), expected_body);
        if index == 3 {
            assert!(file_accesses.load(Ordering::SeqCst) >= 2);
            assert_eq!(file_drops.load(Ordering::SeqCst), 1);
        }
    }
    assert_eq!(encodes.load(Ordering::SeqCst), 1);
    assert_eq!(file_drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn network_flush_failure_reports_exact_progress_and_never_issues_or_retries_a_receipt() {
    let harness = NetworkHarness::new_with_flush_failure("network-response-flush-failure").await;
    let flushes = Arc::clone(&harness.flushes);
    let (control, _parent) = harness.control("network-response-flush-failure-control", None);
    let sink = ResponseSink::network(harness.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();
    let (response, accesses, drops) = counting_file_response(117, 1_117, b"written-before-flush-fails");
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    let error = sink
        .send_response(bind(response, 824, 1_117))
        .await
        .expect_err("flush failure must prevent a receipt");
    assert!(matches!(
        error,
        ResponseOperationalFailure::Transport {
            progress: WriteProgress::PossiblyPartial,
            ..
        }
    ));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(118, 1_118)).expect("duplicate remoting response"),
                825,
                1_118,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        ))
    ));
    let frames = harness.close_and_collect_frames().await;
    assert_eq!(frames.len(), 1, "flush failure must write exactly one frame");
    let received = &frames[0];
    assert_eq!(received.opaque(), 1_117);
    assert_eq!(
        received.body().map(Bytes::as_ref),
        Some(&b"written-before-flush-fails"[..])
    );
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
    assert!(accesses.load(Ordering::SeqCst) >= 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn network_rejects_pre_encode_stops_and_preserves_typed_encode_failures() {
    let harness = NetworkHarness::new(
        "network-response-preflight-errors",
        FrameLimits {
            max_header_bytes: 0,
            ..FrameLimits::default()
        },
        AdmissionLimits::default(),
        None,
    )
    .await;
    let encodes = Arc::new(AtomicUsize::new(0));
    let (expired, _parent) = harness.control(
        "network-response-expired-control",
        Some(RequestDeadline::after(Duration::ZERO)),
    );
    let sink = ResponseSink::network(harness.session.clone(), AdmissionClass::Data, expired);
    let response = RemotingResponse::command(response_head(95, 910).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("remoting response");
    assert!(matches!(
        sink.send_response(bind(response, 805, 1_005)).await,
        Ok(ResponseCompletionOutcome::DeadlineExpired)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 0);

    let (control, _parent) = harness.control("network-response-encode-control", None);
    let sink = ResponseSink::network(harness.session.clone(), AdmissionClass::Data, control);
    let response = RemotingResponse::command(response_head(96, 911).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("remoting response");
    let error = sink
        .send_response(bind(response, 806, 1_006))
        .await
        .expect_err("zero header limit should reject encoding");
    let ResponseOperationalFailure::Encode { source } = error else {
        panic!("encoding failure should preserve its typed source");
    };
    assert_eq!(source.kind().code().as_str(), "SERIALIZATION_FAILED");
    assert_eq!(encodes.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn network_rechecks_control_after_encoding_without_retrying_the_encoder() {
    let harness = NetworkHarness::new(
        "network-response-post-encode-stop",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let encodes = Arc::new(AtomicUsize::new(0));
    let (control, parent) = harness.control("network-response-post-encode-control", None);
    let sink = ResponseSink::network_with_enqueue_gate(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&checked),
        resume,
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(
        sink.send_response(bind(
            RemotingResponse::command(response_head(97, 1_007).set_command_custom_header(CountingHeader {
                encodes: Arc::clone(&encodes),
            }))
            .expect("remoting response"),
            807,
            1_007,
        )),
    );
    checked.notified().await;
    let duplicate = tokio::spawn(duplicate.send_response(bind(
        RemotingResponse::command(response_head(98, 1_008)).expect("duplicate remoting response"),
        808,
        1_008,
    )));
    tokio::task::yield_now().await;
    assert!(
        !duplicate.is_finished(),
        "concurrent duplicate must wait for the claimed owner"
    );
    parent.cancel();

    assert!(matches!(
        send.await.expect("response send task"),
        Ok(ResponseCompletionOutcome::Cancelled)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        duplicate.await.expect("duplicate response task"),
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Cancelled
        ))
    ));

    harness.shutdown().await;
}

#[tokio::test]
async fn network_queue_rejection_is_not_started_and_drops_a_file_lease_once() {
    let harness = NetworkHarness::new(
        "network-response-queue-reject",
        FrameLimits::default(),
        AdmissionLimits {
            queued: ResourceLimit { count: 64, bytes: 1 },
            ..AdmissionLimits::default()
        },
        None,
    )
    .await;
    let (response, accesses, drops) = counting_file_response(99, 1_009, b"leased body");
    let (control, _parent) = harness.control("network-response-queue-reject-control", None);
    let sink = ResponseSink::network(harness.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();

    assert!(matches!(
        sink.send_response(bind(response, 809, 1_009)).await,
        Ok(ResponseCompletionOutcome::QueueSaturated)
    ));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(100, 1_010)).expect("duplicate remoting response"),
                810,
                1_010,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        ))
    ));
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn network_preserves_session_close_before_and_after_encoding() {
    let pre_encode = NetworkHarness::new(
        "network-response-pre-encode-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let encodes = Arc::new(AtomicUsize::new(0));
    let (control, _parent) = pre_encode.control("network-response-pre-encode-close-control", None);
    let sink = ResponseSink::network(pre_encode.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();
    pre_encode.session.abort();
    let response = RemotingResponse::command(response_head(112, 1_112).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("remoting response");
    assert!(matches!(
        sink.send_response(bind(response, 819, 1_112)).await,
        Ok(ResponseCompletionOutcome::SessionClosed)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 0);
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(113, 1_113)).expect("duplicate remoting response"),
                820,
                1_113,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Closed
        ))
    ));
    pre_encode.shutdown().await;

    let post_encode = NetworkHarness::new(
        "network-response-post-encode-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = post_encode.control("network-response-post-encode-close-control", None);
    let sink = ResponseSink::network_with_enqueue_gate(
        post_encode.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&checked),
        resume,
    );
    let response = RemotingResponse::command(response_head(114, 1_114).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("remoting response");
    let send = tokio::spawn(sink.send_response(bind(response, 821, 1_114)));
    checked.notified().await;
    post_encode.session.abort();
    assert!(matches!(
        send.await.expect("response send task"),
        Ok(ResponseCompletionOutcome::SessionClosed)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 1);
    post_encode.shutdown().await;
}

#[tokio::test]
async fn dropping_a_waiting_network_send_cancels_before_start_and_writes_no_response_bytes() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "network-response-waiting-drop",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;

    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move {
        blocker_connection
            .send_command(response_head(101, 1_101))
            .await
            .expect("blocker should write after the barrier resumes");
    });
    checked.notified().await;

    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = harness.control("network-response-waiting-drop-control", None);
    let sink = ResponseSink::network_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(
        sink.send_response(bind(
            RemotingResponse::bytes(response_head(102, 1_102), Bytes::from_static(b"must-not-write"))
                .expect("remoting response"),
            811,
            1_102,
        )),
    );
    enqueued.notified().await;
    send.abort();
    assert!(send
        .await
        .expect_err("aborted response send should stop")
        .is_cancelled());

    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(103, 1_103)).expect("duplicate remoting response"),
                812,
                1_103,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        ))
    ));
    resume.notify_one();
    blocker.await.expect("blocker task should complete");
    let received = harness.receive().await;
    assert_eq!(received.opaque(), 1_101);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command())
            .await
            .is_err(),
        "cancelled waiting response must not write a second frame"
    );
    assert_eq!(harness.session.writer_snapshot().deadline_expired, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn dropping_a_writer_claimed_network_send_is_terminally_possibly_partial_and_not_retried() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "network-response-claimed-drop",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;

    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = harness.control("network-response-claimed-drop-control", None);
    let sink = ResponseSink::network_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(
        sink.send_response(bind(
            RemotingResponse::bytes(response_head(104, 1_104), Bytes::from_static(b"write-once"))
                .expect("remoting response"),
            813,
            1_104,
        )),
    );
    enqueued.notified().await;
    checked.notified().await;
    send.abort();
    assert!(send
        .await
        .expect_err("aborted response send should stop")
        .is_cancelled());

    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(105, 1_105)).expect("duplicate remoting response"),
                814,
                1_105,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        ))
    ));
    resume.notify_one();
    let received = harness.receive().await;
    assert_eq!(received.opaque(), 1_104);
    assert_eq!(received.body().map(Bytes::as_ref), Some(&b"write-once"[..]));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command())
            .await
            .is_err(),
        "the terminal duplicate must not enqueue a second frame"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn session_close_after_writer_claim_but_before_start_stays_closed_and_writes_nothing() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let harness = NetworkHarness::new(
        "network-response-claimed-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = harness.control("network-response-claimed-close-control", None);
    let sink = ResponseSink::network_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let (response, accesses, drops) = counting_file_response(115, 1_115, b"must-not-start");
    let send = tokio::spawn(sink.send_response(bind(response, 822, 1_115)));
    enqueued.notified().await;
    checked.notified().await;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    harness.session.abort();
    assert!(matches!(
        send.await.expect("response send task"),
        Ok(ResponseCompletionOutcome::SessionClosed)
    ));
    let duplicate_result = duplicate
        .send_response(bind(
            RemotingResponse::command(response_head(116, 1_116)).expect("duplicate remoting response"),
            823,
            1_116,
        ))
        .await;
    assert!(
        matches!(
            duplicate_result,
            Ok(ResponseCompletionOutcome::AlreadyCompleted(
                ResponseTerminalState::Closed
            ))
        ),
        "unexpected duplicate result: {duplicate_result:?}"
    );
    resume.notify_one();
    let frames = harness.close_and_collect_frames().await;
    assert!(
        frames.is_empty(),
        "session close before writer start must not write a frame"
    );
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn session_close_after_writer_start_reports_possibly_partial_and_never_retries() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "network-response-started-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let (control, _parent) = harness.control("network-response-started-close-control", None);
    let sink = ResponseSink::network(harness.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();
    let send = tokio::spawn(
        sink.send_response(bind(
            RemotingResponse::bytes(response_head(119, 1_119), Bytes::from(vec![0x5a; 2 * 1024 * 1024]))
                .expect("remoting response"),
            826,
            1_119,
        )),
    );
    checked.notified().await;
    resume.notify_one();
    while harness.session.writer_snapshot().queued_items != 0 {
        assert!(
            !send.is_finished(),
            "large response must still be blocked in socket I/O"
        );
        tokio::task::yield_now().await;
    }
    assert!(
        !send.is_finished(),
        "large response must still be blocked in socket I/O"
    );
    harness.session.abort();

    assert!(matches!(
        send.await.expect("response send task"),
        Err(ResponseOperationalFailure::Transport {
            progress: WriteProgress::PossiblyPartial,
            ..
        })
    ));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(120, 1_120)).expect("duplicate remoting response"),
                827,
                1_120,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        ))
    ));
    let observed = tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command()).await;
    assert!(
        !matches!(observed, Ok(Some(Ok(_)))),
        "a partial original frame must not be followed by a retry"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn cancelling_a_waiting_network_send_preserves_the_reason_without_deadline_diagnostics() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let harness = NetworkHarness::new(
        "network-response-waiting-cancel",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move {
        blocker_connection
            .send_command(response_head(106, 1_106))
            .await
            .expect("blocker should write after the barrier resumes");
    });
    checked.notified().await;

    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, parent) = harness.control("network-response-waiting-cancel-control", None);
    let sink = ResponseSink::network_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let (response, accesses, drops) = counting_file_response(107, 1_107, b"cancelled");
    let send = tokio::spawn(sink.send_response(bind(response, 815, 1_107)));
    enqueued.notified().await;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    parent.cancel();
    assert!(matches!(
        send.await.expect("response send task"),
        Ok(ResponseCompletionOutcome::Cancelled)
    ));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(108, 1_108)).expect("duplicate remoting response"),
                816,
                1_108,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Cancelled
        ))
    ));
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    resume.notify_one();
    blocker.await.expect("blocker task should complete");
    assert_eq!(harness.session.writer_snapshot().deadline_expired, 0);
    let frames = harness.close_and_collect_frames().await;
    assert_eq!(frames.len(), 1, "parent cancellation must not retry the response");
    assert_eq!(frames[0].opaque(), 1_106);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn deadline_of_a_waiting_network_send_is_not_started_and_writes_no_response_bytes() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "network-response-waiting-deadline",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let mut blocker_connection = harness.session.connection();
    let blocker = tokio::spawn(async move {
        blocker_connection
            .send_command(response_head(109, 1_109))
            .await
            .expect("blocker should write after the barrier resumes");
    });
    checked.notified().await;

    let enqueued = Arc::new(tokio::sync::Notify::new());
    let deadline = RequestDeadline::after(Duration::from_secs(1));
    let (control, _parent) = harness.control("network-response-waiting-deadline-control", Some(deadline));
    let sink = ResponseSink::network_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(sink.send_response(bind(
        RemotingResponse::bytes(response_head(110, 1_110), Bytes::from_static(b"expired")).expect("remoting response"),
        817,
        1_110,
    )));
    enqueued.notified().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        send.await.expect("response send task"),
        Ok(ResponseCompletionOutcome::DeadlineExpired)
    ));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(111, 1_111)).expect("duplicate remoting response"),
                818,
                1_111,
            ))
            .await,
        Ok(ResponseCompletionOutcome::AlreadyCompleted(
            ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        ))
    ));

    resume.notify_one();
    blocker.await.expect("blocker task should complete");
    assert_eq!(harness.receive().await.opaque(), 1_109);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command())
            .await
            .is_err(),
        "expired waiting response must not reach the socket"
    );
    assert_eq!(harness.session.writer_snapshot().deadline_expired, 1);

    harness.shutdown().await;
}
