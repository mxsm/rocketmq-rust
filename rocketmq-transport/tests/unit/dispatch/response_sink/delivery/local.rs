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

#[tokio::test]
async fn local_hands_off_all_body_owners_without_encoding_or_copying_storage() {
    let (harness, control) = ControlHarness::new("local-response-four-bodies", None);

    let empty = RemotingResponse::command(response_head(71, 700)).expect("empty remoting response");
    let (sink, receiver) = ResponseSink::local(control.clone());
    let receipt = sink
        .send_response(bind(empty, 701, 701))
        .await
        .expect("empty response handoff");
    assert_eq!(receipt.request_id().owner_id(), 701);
    assert_eq!(receipt.disposition(), ResponseDisposition::InProcessAccepted);
    let received = receiver.receive().await.expect("receive empty response");
    assert_eq!(received.body_kind(), ResponseBodyKind::Empty);
    assert_eq!(received.response_code(), 71);

    let bytes = Bytes::from_static(b"local bytes");
    let bytes_pointer = bytes.as_ptr();
    let bytes_response = RemotingResponse::bytes(response_head(72, 710), bytes).expect("bytes remoting response");
    let (sink, receiver) = ResponseSink::local(control.clone());
    sink.send_response(bind(bytes_response, 702, 702))
        .await
        .expect("bytes response handoff");
    let received = receiver.receive().await.expect("receive bytes response");
    let ResponseBody::Bytes(bytes) = received.test_body() else {
        panic!("bytes owner should remain contiguous");
    };
    assert_eq!(bytes.as_ptr(), bytes_pointer);

    let first = Bytes::from_static(b"first");
    let second = Bytes::from_static(b"second");
    let first_pointer = first.as_ptr();
    let second_pointer = second.as_ptr();
    let segments_response =
        RemotingResponse::segments(response_head(73, 720), vec![first, second]).expect("segments remoting response");
    let (vector_pointer, vector_capacity) = match segments_response.test_body() {
        ResponseBody::Segments(segments) => (segments.as_ptr(), segments.capacity()),
        _ => panic!("segments response should own segments"),
    };
    let (sink, receiver) = ResponseSink::local(control.clone());
    sink.send_response(bind(segments_response, 703, 703))
        .await
        .expect("segments response handoff");
    let received = receiver.receive().await.expect("receive segments response");
    let ResponseBody::Segments(segments) = received.test_body() else {
        panic!("segments owner should remain segmented");
    };
    assert_eq!(segments.as_ptr(), vector_pointer);
    assert_eq!(segments.capacity(), vector_capacity);
    assert_eq!(segments[0].as_ptr(), first_pointer);
    assert_eq!(segments[1].as_ptr(), second_pointer);
    assert_eq!(
        segments.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
        vec![&b"first"[..], &b"second"[..]]
    );

    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(b"file-body").expect("write file body");
    let file = Arc::new(file);
    let region = FileRegion::try_new(file.clone(), 0, 9).expect("file region");
    let response = RemotingResponse::file_regions(response_head(74, 730), FileRegionSequence::single(region))
        .expect("file remoting response");
    let (sink, receiver) = ResponseSink::local(control);
    sink.send_response(bind(response, 704, 704))
        .await
        .expect("file response handoff");
    let received = receiver.receive().await.expect("receive file response");
    assert_eq!(received.body_kind(), ResponseBodyKind::FileRegions);
    assert_eq!(received.body_len(), 9);
    assert_eq!(Arc::strong_count(&file), 2);
    drop(received);
    assert_eq!(Arc::strong_count(&file), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn local_never_invokes_the_header_encoder() {
    let (harness, control) = ControlHarness::new("local-response-no-encoder", None);
    let encodes = Arc::new(AtomicUsize::new(0));
    let head = response_head(75, 740).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    });
    let response = RemotingResponse::bytes(head, Bytes::from_static(b"body")).expect("remoting response");
    let (sink, receiver) = ResponseSink::local(control);

    sink.send_response(bind(response, 705, 705))
        .await
        .expect("local response handoff");
    assert_eq!(encodes.load(Ordering::SeqCst), 0);
    drop(receiver.receive().await.expect("receive response"));
    assert_eq!(encodes.load(Ordering::SeqCst), 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn local_receiver_and_sender_drop_publish_closed_once() {
    let (harness, control) = ControlHarness::new("local-response-drop", None);
    let (sink, receiver) = ResponseSink::local(control.clone());
    let duplicate = sink.clone();
    drop(receiver);

    assert!(matches!(
        sink.send_response(bind(
            RemotingResponse::command(response_head(76, 750)).expect("remoting response"),
            706,
            706,
        ))
        .await,
        Err(ResponseError::SessionClosed)
    ));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(77, 751)).expect("duplicate remoting response"),
                707,
                707,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Closed
        })
    ));

    let (sink, receiver) = ResponseSink::local(control);
    let clone = sink.clone();
    drop(sink);
    drop(clone);
    assert!(matches!(receiver.receive().await, Err(ResponseError::SessionClosed)));

    harness.shutdown().await;
}

#[tokio::test]
async fn local_parent_cancellation_after_claim_prevents_handoff_and_resolves_duplicates() {
    let (harness, control) = ControlHarness::new("local-response-post-claim-cancel", None);
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let (sink, receiver, handoff_attempts) =
        ResponseSink::local_with_handoff_gate(control, Arc::clone(&checked), Arc::clone(&resume));
    let duplicate = sink.clone();
    let final_duplicate = sink.clone();
    let active = tokio::spawn(sink.send_response(bind(
        RemotingResponse::command(response_head(91, 830)).expect("active remoting response"),
        721,
        721,
    )));
    checked.notified().await;

    harness.parent.cancel();
    assert!(matches!(receiver.receive().await, Err(ResponseError::Cancelled)));
    let duplicate = tokio::spawn(duplicate.send_response(bind(
        RemotingResponse::command(response_head(92, 831)).expect("duplicate remoting response"),
        722,
        722,
    )));
    tokio::task::yield_now().await;
    assert!(!duplicate.is_finished(), "duplicate must wait for the active claimant");

    resume.notify_one();
    assert!(matches!(
        active.await.expect("active task"),
        Err(ResponseError::Cancelled)
    ));
    assert_eq!(handoff_attempts.load(Ordering::SeqCst), 0);
    assert!(matches!(
        duplicate.await.expect("duplicate task"),
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Cancelled
        })
    ));
    assert!(matches!(
        final_duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(93, 832)).expect("final duplicate remoting response"),
                723,
                723,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Cancelled
        })
    ));

    harness.shutdown().await;
}

#[tokio::test]
async fn local_receiver_drop_after_claim_prevents_handoff_without_primary_close_leakage() {
    let (harness, control) = ControlHarness::new("local-response-post-claim-receiver-drop", None);
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let (sink, receiver, handoff_attempts) =
        ResponseSink::local_with_handoff_gate(control, Arc::clone(&checked), Arc::clone(&resume));
    let duplicate = sink.clone();
    let final_duplicate = sink.clone();
    let active = tokio::spawn(sink.send_response(bind(
        RemotingResponse::command(response_head(94, 840)).expect("active remoting response"),
        724,
        724,
    )));
    checked.notified().await;

    drop(receiver);
    let duplicate = tokio::spawn(duplicate.send_response(bind(
        RemotingResponse::command(response_head(95, 841)).expect("duplicate remoting response"),
        725,
        725,
    )));
    tokio::task::yield_now().await;
    assert!(!duplicate.is_finished(), "duplicate must wait for the active claimant");

    resume.notify_one();
    assert!(matches!(
        active.await.expect("active task"),
        Err(ResponseError::SessionClosed)
    ));
    assert_eq!(handoff_attempts.load(Ordering::SeqCst), 0);
    assert!(matches!(
        duplicate.await.expect("duplicate task"),
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Closed
        })
    ));
    assert!(matches!(
        final_duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(96, 842)).expect("final duplicate remoting response"),
                726,
                726,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Closed
        })
    ));

    harness.shutdown().await;
}

#[tokio::test]
async fn local_sequential_and_concurrent_duplicates_observe_the_exact_terminal_state() {
    let (harness, control) = ControlHarness::new("local-response-duplicates", None);
    let (sink, receiver) = ResponseSink::local(control.clone());
    let duplicate = sink.clone();
    sink.send_response(bind(
        RemotingResponse::command(response_head(78, 760)).expect("remoting response"),
        708,
        708,
    ))
    .await
    .expect("first handoff");
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(79, 761)).expect("duplicate remoting response"),
                709,
                709,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Completed
        })
    ));
    drop(receiver);

    let (sink, receiver) = ResponseSink::local(control);
    let first = tokio::spawn(sink.clone().send_response(bind(
        RemotingResponse::command(response_head(80, 770)).expect("first remoting response"),
        710,
        710,
    )));
    let second = tokio::spawn(sink.send_response(bind(
        RemotingResponse::command(response_head(81, 771)).expect("second remoting response"),
        711,
        711,
    )));
    let first = first.await.expect("first task");
    let second = second.await.expect("second task");
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let duplicate = if first.is_err() { first } else { second };
    assert!(matches!(
        duplicate,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Completed
        })
    ));
    drop(receiver);

    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn local_preserves_cancel_session_deadline_priority_and_terminal_states() {
    let deadline = RequestDeadline::after(Duration::from_secs(1));
    let (deadline_harness, deadline_control) = ControlHarness::new("local-response-deadline", Some(deadline));
    tokio::time::advance(Duration::from_secs(1)).await;
    let (sink, _receiver) = ResponseSink::local(deadline_control);
    let duplicate = sink.clone();
    assert!(matches!(
        sink.send_response(bind(
            RemotingResponse::command(response_head(82, 780)).expect("remoting response"),
            712,
            712,
        ))
        .await,
        Err(ResponseError::DeadlineExceeded)
    ));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(83, 781)).expect("duplicate remoting response"),
                713,
                713,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));
    deadline_harness.shutdown().await;

    let (cancel_harness, cancel_control) = ControlHarness::new("local-response-cancel", None);
    cancel_harness.parent.cancel();
    let (sink, _receiver) = ResponseSink::local(cancel_control);
    assert!(matches!(
        sink.send_response(bind(
            RemotingResponse::command(response_head(84, 790)).expect("remoting response"),
            714,
            714,
        ))
        .await,
        Err(ResponseError::Cancelled)
    ));
    cancel_harness.shutdown().await;

    let (closed_harness, closed_control) = ControlHarness::new("local-response-close", Some(deadline));
    closed_harness
        .closed_tx
        .send(true)
        .expect("session close publisher should remain open");
    let (sink, _receiver) = ResponseSink::local(closed_control);
    assert!(matches!(
        sink.send_response(bind(
            RemotingResponse::command(response_head(85, 800)).expect("remoting response"),
            715,
            715,
        ))
        .await,
        Err(ResponseError::SessionClosed)
    ));
    closed_harness.shutdown().await;
}

#[tokio::test]
async fn local_receiver_is_cancellation_first_without_reopening_a_completed_handoff() {
    let (harness, control) = ControlHarness::new("local-response-receiver-cancel", None);
    let (sink, receiver) = ResponseSink::local(control);
    let duplicate = sink.clone();
    sink.send_response(bind(
        RemotingResponse::bytes(response_head(87, 820), Bytes::from_static(b"already handed off"))
            .expect("remoting response"),
        717,
        717,
    ))
    .await
    .expect("handoff should complete");
    harness.parent.cancel();

    assert!(matches!(receiver.receive().await, Err(ResponseError::Cancelled)));
    assert!(matches!(
        duplicate
            .send_response(bind(
                RemotingResponse::command(response_head(88, 821)).expect("duplicate remoting response"),
                718,
                718,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Completed
        })
    ));

    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn local_receiver_stop_reasons_publish_their_exact_terminal_states() {
    let deadline = RequestDeadline::after(Duration::from_secs(1));
    let (deadline_harness, deadline_control) = ControlHarness::new("local-receiver-deadline", Some(deadline));
    let (sink, receiver) = ResponseSink::local(deadline_control);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(receiver.receive().await, Err(ResponseError::DeadlineExceeded)));
    assert!(matches!(
        sink.send_response(bind(
            RemotingResponse::command(response_head(88, 821)).expect("remoting response"),
            718,
            718,
        ))
        .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Failed {
                progress: WriteProgress::NotStarted
            }
        })
    ));
    deadline_harness.shutdown().await;

    let (cancel_harness, cancel_control) = ControlHarness::new("local-receiver-cancel", None);
    let (sink, receiver) = ResponseSink::local(cancel_control);
    cancel_harness.parent.cancel();
    assert!(matches!(receiver.receive().await, Err(ResponseError::Cancelled)));
    assert!(matches!(
        sink.send_response(bind(
            RemotingResponse::command(response_head(89, 822)).expect("remoting response"),
            719,
            719,
        ))
        .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Cancelled
        })
    ));
    cancel_harness.shutdown().await;

    let (closed_harness, closed_control) = ControlHarness::new("local-receiver-close", None);
    let (sink, receiver) = ResponseSink::local(closed_control);
    closed_harness
        .closed_tx
        .send(true)
        .expect("session close publisher should remain open");
    assert!(matches!(receiver.receive().await, Err(ResponseError::SessionClosed)));
    assert!(matches!(
        sink.send_response(bind(
            RemotingResponse::command(response_head(90, 823)).expect("remoting response"),
            720,
            720,
        ))
        .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Closed
        })
    ));
    closed_harness.shutdown().await;
}

#[tokio::test]
async fn local_file_response_preserves_the_exact_lease_without_restatting_or_cloning() {
    let (harness, control) = ControlHarness::new("local-response-file-lease", None);
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
    let response = RemotingResponse::file_regions(response_head(86, 810), FileRegionSequence::single(region))
        .expect("file remoting response");
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&lease), 2);
    let (sink, receiver) = ResponseSink::local(control);

    sink.send_response(bind(response, 716, 716))
        .await
        .expect("file response handoff");
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&lease), 2);
    let received = receiver.receive().await.expect("receive file response");
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&lease), 2);
    drop(received);
    assert_eq!(Arc::strong_count(&lease), 1);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}
