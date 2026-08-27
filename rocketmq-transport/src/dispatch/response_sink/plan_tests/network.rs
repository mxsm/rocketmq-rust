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

#[tokio::test]
async fn network_plan_prepares_and_writes_all_four_bodies_before_issuing_receipts() {
    let mut harness = NetworkHarness::new(
        "network-plan-four-bodies",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let encodes = Arc::new(AtomicUsize::new(0));
    let plans = [
        ResponsePlan::command(response_head(91, 900)).expect("empty response plan"),
        ResponsePlan::bytes(
            response_head(92, 901).set_command_custom_header(CountingHeader {
                encodes: Arc::clone(&encodes),
            }),
            Bytes::from_static(b"bytes-body"),
        )
        .expect("bytes response plan"),
        ResponsePlan::segments(
            response_head(93, 902),
            vec![Bytes::from_static(b"segment-"), Bytes::from_static(b"body")],
        )
        .expect("segments response plan"),
        {
            let mut file = tempfile::tempfile().expect("temporary file");
            file.write_all(b"file-body").expect("write file body");
            let region = FileRegion::try_new(Arc::new(file), 0, 9).expect("file region");
            ResponsePlan::file_regions(response_head(94, 903), FileRegionSequence::single(region))
                .expect("file response plan")
        },
    ];
    let expected_bodies: [&[u8]; 4] = [b"", b"bytes-body", b"segment-body", b"file-body"];

    for (index, (plan, expected_body)) in plans.into_iter().zip(expected_bodies).enumerate() {
        let owner = 801 + index as u64;
        let (control, _parent) = harness.control("network-plan-four-bodies-control", None);
        let sink = ResponseSink::network_plan(harness.session.clone(), AdmissionClass::Data, control);
        let flushes_before = harness.flushes.load(Ordering::SeqCst);
        let receipt = sink
            .send_plan(bind(plan, owner, 1_001 + index as i32))
            .await
            .expect("network plan should complete through writer");
        assert_eq!(receipt.request_id().owner_id(), owner);
        assert_eq!(receipt.disposition(), ResponseDisposition::TransportWritten);
        assert!(harness.flushes.load(Ordering::SeqCst) > flushes_before);
        let received = harness.receive().await;
        assert_eq!(received.opaque(), 1_001 + index as i32);
        assert_eq!(received.body().map(Bytes::as_ref).unwrap_or_default(), expected_body);
    }
    assert_eq!(encodes.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn network_plan_flush_failure_reports_exact_progress_and_never_issues_or_retries_a_receipt() {
    let mut harness = NetworkHarness::new_with_flush_failure("network-plan-flush-failure").await;
    let (control, _parent) = harness.control("network-plan-flush-failure-control", None);
    let sink = ResponseSink::network_plan(harness.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();

    let error = sink
        .send_plan(bind(
            ResponsePlan::bytes(
                response_head(117, 1_117),
                Bytes::from_static(b"written-before-flush-fails"),
            )
            .expect("response plan"),
            824,
            1_117,
        ))
        .await
        .expect_err("flush failure must prevent a receipt");
    assert!(matches!(
        error,
        ResponseError::Transport {
            progress: WriteProgress::PossiblyPartial,
            ..
        }
    ));
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(118, 1_118)).expect("duplicate response plan"),
                825,
                1_118,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        })
    ));
    let received = harness.receive().await;
    assert_eq!(received.opaque(), 1_117);
    assert_eq!(
        received.body().map(Bytes::as_ref),
        Some(&b"written-before-flush-fails"[..])
    );
    assert_eq!(harness.flushes.load(Ordering::SeqCst), 0);
    let second = tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command()).await;
    assert!(
        !matches!(second, Ok(Some(Ok(_)))),
        "terminal duplicate must not write a second frame"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn network_plan_rejects_pre_encode_stops_and_preserves_typed_encode_failures() {
    let harness = NetworkHarness::new(
        "network-plan-preflight-errors",
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
        "network-plan-expired-control",
        Some(RequestDeadline::after(Duration::ZERO)),
    );
    let sink = ResponseSink::network_plan(harness.session.clone(), AdmissionClass::Data, expired);
    let plan = ResponsePlan::command(response_head(95, 910).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("response plan");
    assert!(matches!(
        sink.send_plan(bind(plan, 805, 1_005)).await,
        Err(ResponseError::DeadlineExceeded)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 0);

    let (control, _parent) = harness.control("network-plan-encode-control", None);
    let sink = ResponseSink::network_plan(harness.session.clone(), AdmissionClass::Data, control);
    let plan = ResponsePlan::command(response_head(96, 911).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("response plan");
    let error = sink
        .send_plan(bind(plan, 806, 1_006))
        .await
        .expect_err("zero header limit should reject encoding");
    let ResponseError::Encode { source } = error else {
        panic!("encoding failure should preserve its typed source");
    };
    assert_eq!(source.kind().code().as_str(), "SERIALIZATION_FAILED");
    assert_eq!(encodes.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn network_plan_rechecks_control_after_encoding_without_retrying_the_encoder() {
    let harness = NetworkHarness::new(
        "network-plan-post-encode-stop",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let encodes = Arc::new(AtomicUsize::new(0));
    let (control, parent) = harness.control("network-plan-post-encode-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_gate(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&checked),
        resume,
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(
        sink.send_plan(bind(
            ResponsePlan::command(response_head(97, 1_007).set_command_custom_header(CountingHeader {
                encodes: Arc::clone(&encodes),
            }))
            .expect("response plan"),
            807,
            1_007,
        )),
    );
    checked.notified().await;
    let duplicate = tokio::spawn(duplicate.send_plan(bind(
        ResponsePlan::command(response_head(98, 1_008)).expect("duplicate response plan"),
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
        send.await.expect("plan send task"),
        Err(ResponseError::Cancelled)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        duplicate.await.expect("duplicate plan task"),
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Cancelled
        })
    ));

    harness.shutdown().await;
}

#[tokio::test]
async fn network_plan_queue_rejection_is_not_started_and_drops_a_file_lease_once() {
    let harness = NetworkHarness::new(
        "network-plan-queue-reject",
        FrameLimits::default(),
        AdmissionLimits {
            queued: ResourceLimit { count: 64, bytes: 1 },
            ..AdmissionLimits::default()
        },
        None,
    )
    .await;
    let accesses = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(b"leased body").expect("write leased body");
    let lease = Arc::new(CountingLease {
        file,
        accesses: Arc::clone(&accesses),
        drops: Arc::clone(&drops),
    });
    let region = FileRegion::try_new(lease.clone(), 0, 11).expect("file region");
    let plan = ResponsePlan::file_regions(response_head(99, 1_009), FileRegionSequence::single(region))
        .expect("response plan");
    let (control, _parent) = harness.control("network-plan-queue-reject-control", None);
    let sink = ResponseSink::network_plan(harness.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();

    assert!(matches!(
        sink.send_plan(bind(plan, 809, 1_009)).await,
        Err(ResponseError::QueueSaturated)
    ));
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(100, 1_010)).expect("duplicate response plan"),
                810,
                1_010,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&lease), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn network_plan_preserves_session_close_before_and_after_encoding() {
    let pre_encode = NetworkHarness::new(
        "network-plan-pre-encode-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let encodes = Arc::new(AtomicUsize::new(0));
    let (control, _parent) = pre_encode.control("network-plan-pre-encode-close-control", None);
    let sink = ResponseSink::network_plan(pre_encode.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();
    pre_encode.session.abort();
    let plan = ResponsePlan::command(response_head(112, 1_112).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("response plan");
    assert!(matches!(
        sink.send_plan(bind(plan, 819, 1_112)).await,
        Err(ResponseError::SessionClosed)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 0);
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(113, 1_113)).expect("duplicate response plan"),
                820,
                1_113,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Closed
        })
    ));
    pre_encode.shutdown().await;

    let post_encode = NetworkHarness::new(
        "network-plan-post-encode-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        None,
    )
    .await;
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = post_encode.control("network-plan-post-encode-close-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_gate(
        post_encode.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&checked),
        resume,
    );
    let plan = ResponsePlan::command(response_head(114, 1_114).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    }))
    .expect("response plan");
    let send = tokio::spawn(sink.send_plan(bind(plan, 821, 1_114)));
    checked.notified().await;
    post_encode.session.abort();
    assert!(matches!(
        send.await.expect("plan send task"),
        Err(ResponseError::SessionClosed)
    ));
    assert_eq!(encodes.load(Ordering::SeqCst), 1);
    post_encode.shutdown().await;
}

#[tokio::test]
async fn dropping_a_waiting_network_send_cancels_before_start_and_writes_no_plan_bytes() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "network-plan-waiting-drop",
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
    let (control, _parent) = harness.control("network-plan-waiting-drop-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(sink.send_plan(bind(
        ResponsePlan::bytes(response_head(102, 1_102), Bytes::from_static(b"must-not-write")).expect("response plan"),
        811,
        1_102,
    )));
    enqueued.notified().await;
    send.abort();
    assert!(send.await.expect_err("aborted plan send should stop").is_cancelled());

    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(103, 1_103)).expect("duplicate response plan"),
                812,
                1_103,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));
    resume.notify_one();
    blocker.await.expect("blocker task should complete");
    let received = harness.receive().await;
    assert_eq!(received.opaque(), 1_101);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command())
            .await
            .is_err(),
        "cancelled waiting plan must not write a second frame"
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
        "network-plan-claimed-drop",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;

    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = harness.control("network-plan-claimed-drop-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(sink.send_plan(bind(
        ResponsePlan::bytes(response_head(104, 1_104), Bytes::from_static(b"write-once")).expect("response plan"),
        813,
        1_104,
    )));
    enqueued.notified().await;
    checked.notified().await;
    send.abort();
    assert!(send.await.expect_err("aborted plan send should stop").is_cancelled());

    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(105, 1_105)).expect("duplicate response plan"),
                814,
                1_105,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        })
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
    let mut harness = NetworkHarness::new(
        "network-plan-claimed-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let enqueued = Arc::new(tokio::sync::Notify::new());
    let (control, _parent) = harness.control("network-plan-claimed-close-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(sink.send_plan(bind(
        ResponsePlan::bytes(response_head(115, 1_115), Bytes::from_static(b"must-not-start")).expect("response plan"),
        822,
        1_115,
    )));
    enqueued.notified().await;
    checked.notified().await;
    harness.session.abort();
    assert!(matches!(
        send.await.expect("plan send task"),
        Err(ResponseError::SessionClosed)
    ));
    let duplicate_result = duplicate
        .send_plan(bind(
            ResponsePlan::command(response_head(116, 1_116)).expect("duplicate response plan"),
            823,
            1_116,
        ))
        .await;
    assert!(
        matches!(
            duplicate_result,
            Err(ResponseError::AlreadyCompleted {
                state: ResponseTerminalState::Closed
            })
        ),
        "unexpected duplicate result: {duplicate_result:?}"
    );
    resume.notify_one();
    let observed = tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command()).await;
    assert!(
        !matches!(observed, Ok(Some(Ok(_)))),
        "session close before writer start must not touch the socket"
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn session_close_after_writer_start_reports_possibly_partial_and_never_retries() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "network-plan-started-close",
        FrameLimits::default(),
        AdmissionLimits::default(),
        Some(barrier),
    )
    .await;
    let (control, _parent) = harness.control("network-plan-started-close-control", None);
    let sink = ResponseSink::network_plan(harness.session.clone(), AdmissionClass::Data, control);
    let duplicate = sink.clone();
    let send = tokio::spawn(
        sink.send_plan(bind(
            ResponsePlan::bytes(response_head(119, 1_119), Bytes::from(vec![0x5a; 2 * 1024 * 1024]))
                .expect("response plan"),
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
        send.await.expect("plan send task"),
        Err(ResponseError::Transport {
            progress: WriteProgress::PossiblyPartial,
            ..
        })
    ));
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(120, 1_120)).expect("duplicate response plan"),
                827,
                1_120,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::PossiblyPartial
            }
        })
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
    let mut harness = NetworkHarness::new(
        "network-plan-waiting-cancel",
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
    let (control, parent) = harness.control("network-plan-waiting-cancel-control", None);
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(sink.send_plan(bind(
        ResponsePlan::bytes(response_head(107, 1_107), Bytes::from_static(b"cancelled")).expect("response plan"),
        815,
        1_107,
    )));
    enqueued.notified().await;
    parent.cancel();
    assert!(matches!(
        send.await.expect("plan send task"),
        Err(ResponseError::Cancelled)
    ));
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(108, 1_108)).expect("duplicate response plan"),
                816,
                1_108,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Cancelled
        })
    ));

    resume.notify_one();
    blocker.await.expect("blocker task should complete");
    assert_eq!(harness.receive().await.opaque(), 1_106);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), harness.peer.receive_command())
            .await
            .is_err(),
        "cancelled waiting response must not reach the socket"
    );
    assert_eq!(harness.session.writer_snapshot().deadline_expired, 0);

    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn deadline_of_a_waiting_network_send_is_not_started_and_writes_no_plan_bytes() {
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let barrier = crate::write_strategy::WritePreflightBarrier::new(Arc::clone(&checked), Arc::clone(&resume));
    let mut harness = NetworkHarness::new(
        "network-plan-waiting-deadline",
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
    let (control, _parent) = harness.control("network-plan-waiting-deadline-control", Some(deadline));
    let sink = ResponseSink::network_plan_with_enqueue_observer(
        harness.session.clone(),
        AdmissionClass::Data,
        control,
        Arc::clone(&enqueued),
    );
    let duplicate = sink.clone();
    let send = tokio::spawn(sink.send_plan(bind(
        ResponsePlan::bytes(response_head(110, 1_110), Bytes::from_static(b"expired")).expect("response plan"),
        817,
        1_110,
    )));
    enqueued.notified().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        send.await.expect("plan send task"),
        Err(ResponseError::DeadlineExceeded)
    ));
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(111, 1_111)).expect("duplicate response plan"),
                818,
                1_111,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
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
