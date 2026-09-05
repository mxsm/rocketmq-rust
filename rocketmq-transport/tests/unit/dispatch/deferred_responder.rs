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

use std::error::Error;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use super::*;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionScope;
use crate::connection::ConnectionState;
use crate::connection::SessionLifecycle;
use crate::connection::SessionWriterDiagnostics;
use crate::dispatch::InlineResponseSlot;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseBody;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseOperationalFailure;
use crate::dispatch::ResponseSendOutcome;
use crate::dispatch::ResponseStateOutcome;
use crate::dispatch::ResponseTerminalState;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionSequence;
use crate::session_view::EmbeddedSessionRecord;
use crate::session_view::SessionId;
use crate::writer_runtime::writer_lanes;
use crate::writer_runtime::WriterQueueConfig;

fn identity(owner: u64, opaque: i32, one_way: bool) -> OriginalRequestIdentity {
    identity_with_code(owner, opaque, one_way, 39)
}

fn identity_with_code(owner: u64, opaque: i32, one_way: bool, code: i32) -> OriginalRequestIdentity {
    let mut command = RemotingCommand::create_remoting_command(code).set_opaque(opaque);
    if one_way {
        command.mark_oneway_rpc_ref();
    }
    OriginalRequestIdentity::capture(owner, &AtomicU64::new(1), &command).expect("test identity should allocate")
}

fn remoting_response(opaque: i32) -> RemotingResponse {
    RemotingResponse::command(RemotingCommand::create_response_command_with_code(0).set_opaque(opaque))
        .expect("remoting response")
}

fn expect_applied<T>(result: Result<ResponseStateOutcome<T>, TransportContractViolation>) -> T {
    match result.expect("response state transition remains valid") {
        ResponseStateOutcome::Applied(value) => value,
        ResponseStateOutcome::AlreadyCompleted { .. } => panic!("response state unexpectedly terminal"),
    }
}

fn expect_taken(outcome: DeferredResponderOutcome) -> DeferredResponder {
    match outcome {
        DeferredResponderOutcome::Taken(responder) => responder,
        _ => panic!("deferred response ownership should be available"),
    }
}

fn expect_completed(outcome: DeferredResponseOutcome) -> ResponseReceipt {
    match outcome {
        DeferredResponseOutcome::Completed(receipt) => receipt,
        _ => panic!("deferred response should complete"),
    }
}

struct ControlHarness {
    runtime: RuntimeOwner,
    parent: rocketmq_runtime::TaskGroup,
    session: EmbeddedSessionRecord,
}

struct PanicOnWrite;

impl AsyncRead for PanicOnWrite {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for PanicOnWrite {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        panic!("a direct deferred response must fail before socket I/O")
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        panic!("a direct deferred response must fail before socket I/O")
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl ControlHarness {
    fn new(name: &'static str, owner: u64) -> (Self, crate::dispatch::RequestControlView) {
        let runtime = RuntimeOwner::plan(RuntimeConfig::server_default(name))
            .expect("test runtime configuration is valid")
            .build()
            .expect("deferred responder runtime owner");
        let parent = runtime.root_context().component(name).task_group().clone();
        let session = EmbeddedSessionRecord::new(owner);
        let control = crate::dispatch::RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), None),
            session.view().state().clone(),
            &parent,
        );
        (
            Self {
                runtime,
                parent,
                session,
            },
            control,
        )
    }

    fn session_id(&self) -> SessionId {
        self.session.view().id()
    }

    async fn shutdown(self) {
        let report = self.runtime.shutdown_tasks().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

#[tokio::test]
async fn take_failures_are_exact_and_only_a_success_allocates_deferred_state() {
    let (harness, control) = ControlHarness::new("deferred-take-errors", 71);
    let before = deferred_state_allocations();
    let ordinary = identity(71, 17, false);
    let one_way = identity(72, 18, true);
    let mut unavailable = InlineResponseSlot::disabled();
    assert!(matches!(
        unavailable.take_deferred_responder(ordinary),
        DeferredResponderOutcome::Unavailable
    ));
    assert_eq!(deferred_state_allocations(), before);

    let (sink, _receiver) = ResponseSink::local(control.clone());
    let seed = sink.deferred_seed_for_test(TransportTelemetry::noop(), harness.session_id(), control);
    let mut slot = InlineResponseSlot::with_deferred_seed(seed);
    assert!(matches!(
        slot.take_deferred_responder(one_way),
        DeferredResponderOutcome::OneWayRequest
    ));
    assert_eq!(deferred_state_allocations(), before);

    let responder = expect_taken(slot.take_deferred_responder(ordinary));
    assert_eq!(deferred_state_allocations(), before + 1);
    assert!(matches!(
        slot.take_deferred_responder(ordinary),
        DeferredResponderOutcome::AlreadyTaken
    ));
    assert_eq!(deferred_state_allocations(), before + 1);
    drop(responder);

    let mut completed = InlineResponseSlot::disabled();
    let _ = completed
        .resolve(
            ordinary,
            crate::dispatch::HandlerOutcome::Reply(remoting_response(ordinary.original_opaque())),
        )
        .expect("inline reply completes the slot");
    assert!(matches!(
        completed.take_deferred_responder(ordinary),
        DeferredResponderOutcome::OutcomeCompleted
    ));
    assert_eq!(deferred_state_allocations(), before + 1);
    harness.shutdown().await;
}

#[tokio::test]
async fn explicit_cancel_and_abandoned_drop_record_only_the_winning_cas() {
    let (harness, control) = ControlHarness::new("deferred-cancel-drop", 73);
    let original = identity(73, 19, false);
    let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let explicit = DeferredResponseSeed::new(sink.clone(), telemetry.clone(), harness.session_id(), control.clone())
        .into_responder(original);
    let explicit_state = Arc::clone(&explicit.state);
    assert_eq!(explicit.request_id(), original.request_id());
    assert_eq!(explicit.session_id(), harness.session_id());
    assert!(explicit
        .control()
        .same_lifecycle_owner(harness.session.view().state(), &harness.parent));
    expect_applied(explicit.register());
    expect_applied(explicit.claim());
    assert_eq!(
        explicit.cancel().expect("open responder cancels"),
        DeferredResponseOutcome::Cancelled
    );
    assert_eq!(explicit_state.terminal_state(), Some(ResponseTerminalState::Cancelled));
    assert_eq!(explicit_state.terminal_reason(), Some(DeferredTerminalReason::Explicit));

    let abandoned = DeferredResponseSeed::new(sink.clone(), telemetry.clone(), harness.session_id(), control.clone())
        .into_responder(original);
    let abandoned_state = Arc::clone(&abandoned.state);
    drop(abandoned);
    assert_eq!(abandoned_state.terminal_state(), Some(ResponseTerminalState::Cancelled));
    assert_eq!(
        abandoned_state.terminal_reason(),
        Some(DeferredTerminalReason::Abandoned)
    );

    let already_closed =
        DeferredResponseSeed::new(sink.clone(), telemetry.clone(), harness.session_id(), control.clone())
            .into_responder(original);
    already_closed.state.close().expect("close wins before explicit cancel");
    assert_eq!(
        already_closed.cancel().expect("prior terminal is a normal outcome"),
        DeferredResponseOutcome::AlreadyCompleted
    );

    let sending = DeferredResponseSeed::new(sink, telemetry, harness.session_id(), control).into_responder(original);
    let send_claim = expect_applied(sending.state.begin_sending());
    drop(sending);
    drop(send_claim);
    assert_eq!(
        terminals.lock().as_slice(),
        [
            ("other", "explicit"),
            ("other", "abandoned"),
            ("other", "session_closed"),
        ]
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn caller_receiver_drop_closes_once_and_reports_the_prior_reason() {
    let (harness, control) = ControlHarness::new("deferred-receiver-drop", 80);
    let original = identity_with_code(
        80,
        25,
        false,
        rocketmq_protocol::code::request_code::RequestCode::PullMessage.to_i32(),
    );
    let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let responder = DeferredResponseSeed::new(sink, telemetry, harness.session_id(), control).into_responder(original);
    let state = Arc::clone(&responder.state);

    assert_eq!(
        responder
            .cancel_with_reason(DeferredCancellationReason::ReceiverDropped)
            .expect("caller-owned receiver drop closes the response"),
        DeferredResponseOutcome::Cancelled
    );

    assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Closed));
    assert_eq!(state.terminal_reason(), Some(DeferredTerminalReason::ReceiverDropped));
    assert!(matches!(
        state.cancel().expect("prior terminal is a normal outcome"),
        ResponseStateOutcome::AlreadyCompleted {
            state: ResponseTerminalState::Closed,
            reason: Some(DeferredTerminalReason::ReceiverDropped),
        }
    ));
    assert_eq!(terminals.lock().as_slice(), [("pull_message", "receiver_dropped")]);
    harness.shutdown().await;
}

#[tokio::test]
async fn local_response_binds_original_opaque_and_moves_body_once() {
    let (harness, control) = ControlHarness::new("deferred-responder-local-response", 74);
    let (sink, receiver) = ResponseSink::local(control.clone());
    let original = identity(74, -712, false);
    let bytes = Bytes::from_static(b"deferred-body");
    let pointer = bytes.as_ptr();
    let responder = DeferredResponseSeed::new(sink, TransportTelemetry::noop(), harness.session_id(), control.clone())
        .into_responder(original);
    let state = Arc::clone(&responder.state);
    let receipt = expect_completed(
        responder
            .respond(
                RemotingResponse::bytes(
                    RemotingCommand::create_response_command_with_code(71).set_opaque(999),
                    bytes,
                )
                .expect("bytes plan"),
            )
            .await
            .expect("local trusted owner accepts deferred plan"),
    );
    assert_eq!(receipt.request_id(), original.request_id());
    assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Completed));

    let received = receiver.receive().await.expect("trusted local owner receives the plan");
    assert_eq!(received.test_head().opaque(), original.original_opaque());
    let ResponseBody::Bytes(received_bytes) = received.test_body() else {
        panic!("bytes body must retain its representation");
    };
    assert_eq!(received_bytes.as_ptr(), pointer);
    assert_eq!(received_bytes.as_ref(), b"deferred-body");

    let first = Bytes::from_static(b"first");
    let second = Bytes::from_static(b"second");
    let first_pointer = first.as_ptr();
    let second_pointer = second.as_ptr();
    let segments_plan = RemotingResponse::segments(
        RemotingCommand::create_response_command_with_code(72).set_opaque(1),
        vec![first, second],
    )
    .expect("segments plan");
    let (vector_pointer, vector_capacity) = match segments_plan.test_body() {
        ResponseBody::Segments(segments) => (segments.as_ptr(), segments.capacity()),
        _ => panic!("segments plan must retain its representation"),
    };
    let (sink, receiver) = ResponseSink::local(control.clone());
    let segments_outcome =
        DeferredResponseSeed::new(sink, TransportTelemetry::noop(), harness.session_id(), control.clone())
            .into_responder(original)
            .respond(segments_plan)
            .await
            .expect("segments plan handoff");
    assert!(matches!(segments_outcome, DeferredResponseOutcome::Completed(_)));
    let received = receiver.receive().await.expect("receive segments plan");
    let ResponseBody::Segments(segments) = received.test_body() else {
        panic!("deferred segments must retain their representation");
    };
    assert_eq!(segments.as_ptr(), vector_pointer);
    assert_eq!(segments.capacity(), vector_capacity);
    assert_eq!(segments[0].as_ptr(), first_pointer);
    assert_eq!(segments[1].as_ptr(), second_pointer);

    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(b"file-body").expect("write file body");
    let file = Arc::new(file);
    let region = FileRegion::try_new(file.clone(), 0, 9).expect("file region");
    let plan = RemotingResponse::file_regions(
        RemotingCommand::create_response_command_with_code(73).set_opaque(2),
        FileRegionSequence::try_new(vec![region]).expect("file region sequence"),
    )
    .expect("file remoting response");
    let (sink, receiver) = ResponseSink::local(control.clone());
    let file_outcome = DeferredResponseSeed::new(sink, TransportTelemetry::noop(), harness.session_id(), control)
        .into_responder(original)
        .respond(plan)
        .await
        .expect("file plan handoff");
    assert!(matches!(file_outcome, DeferredResponseOutcome::Completed(_)));
    assert_eq!(Arc::strong_count(&file), 2);
    let received = receiver.receive().await.expect("receive file plan");
    let ResponseBody::FileRegions(regions) = received.test_body() else {
        panic!("deferred file plan must retain its representation");
    };
    assert_eq!(regions.len(), 9);
    assert_eq!(Arc::strong_count(&file), 2);
    drop(received);
    assert_eq!(Arc::strong_count(&file), 1);
    harness.shutdown().await;
}

#[tokio::test]
async fn direct_remoting_response_fails_closed_before_io_and_finishes_not_started() {
    let (harness, control) = ControlHarness::new("deferred-direct-fail-closed", 78);
    let original = identity(78, 23, false);
    let bound = remoting_response(999).bind(original).expect("bound remoting response");
    let prepared = crate::codec::prepare_response(bound, crate::codec::remoting_command_codec::FrameLimits::default())
        .expect("prepared response");
    let state = Arc::new(ResponseState::open());
    let mut claim = expect_applied(state.begin_sending());
    let delegated = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let transport_drop = claim.observe_transport_drop(delegated);
    let mut connection = crate::connection::Connection::new_with_plaintext_stream(PanicOnWrite);

    let outcome = connection
        .send_prepared_deferred_response(prepared, &control, transport_drop)
        .await;
    assert!(matches!(
        outcome,
        ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed)
    ));
    expect_applied(claim.fail(WriteProgress::NotStarted));
    assert_eq!(
        state.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted
        })
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn queued_response_without_canonical_plan_context_fails_before_enqueue() {
    let (harness, control) = ControlHarness::new("deferred-queued-missing-context", 79);
    let original = identity(79, 24, false);
    let bound = remoting_response(1000).bind(original).expect("bound remoting response");
    let prepared = crate::codec::prepare_response(bound, crate::codec::remoting_command_codec::FrameLimits::default())
        .expect("prepared response");
    let state = Arc::new(ResponseState::open());
    let mut claim = expect_applied(state.begin_sending());
    let delegated = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let transport_drop = claim.observe_transport_drop(delegated);

    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare admission scope");
    let config = WriterQueueConfig::default();
    let (lanes, receivers) = writer_lanes(config);
    let diagnostics = Arc::new(SessionWriterDiagnostics::new(config.total_capacity()));
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let mut connection = crate::connection::Connection::new_queued(
        lanes,
        Arc::clone(&diagnostics),
        admission,
        state_tx,
        state_rx,
        CheetahString::from_static_str("deferred-missing-plan-context"),
        crate::codec::remoting_command_codec::FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );

    let outcome = connection
        .send_prepared_deferred_response(prepared, &control, transport_drop)
        .await;
    assert!(matches!(
        outcome,
        ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed)
    ));
    assert_eq!(diagnostics.snapshot().accepted, 0);
    expect_applied(claim.fail(WriteProgress::NotStarted));
    assert_eq!(
        state.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted
        })
    );

    drop(connection);
    drop(receivers);
    harness.shutdown().await;
}

#[tokio::test]
async fn immutable_binding_failure_terminates_not_started_and_preserves_its_source() {
    let (harness, control) = ControlHarness::new("deferred-binding-failure", 76);
    let original = identity(76, 22, true);
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let responder = DeferredResponseSeed::new(sink, TransportTelemetry::noop(), harness.session_id(), control)
        .into_responder(original);
    let state = Arc::clone(&responder.state);
    let error = responder
        .respond(remoting_response(1))
        .await
        .expect_err("immutable one-way identity must fail binding");
    assert_eq!(error.code(), rocketmq_error::TRANSPORT_RESPONSE_FAILED.code());
    assert!(error.source().is_some());
    assert_eq!(
        state.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: WriteProgress::NotStarted
        })
    );
    harness.shutdown().await;
}

#[test]
fn response_operational_failures_preserve_typed_sources_but_redact_source_text() {
    let secret = "opaque=991 principal=alice token=secret body=payload session=77";
    let error = ResponseOperationalFailure::Transport {
        progress: WriteProgress::PossiblyPartial,
        source: RocketMQError::network_connection_failed("deferred_test", secret),
    };
    assert_eq!(error.operation(), "transport");
    assert_eq!(error.write_progress(), WriteProgress::PossiblyPartial);
    assert!(!error.retryable());
    assert!(error.source().is_some());
    for rendered in [format!("{error}"), format!("{error:?}")] {
        assert!(!rendered.contains("991"));
        assert!(!rendered.contains("alice"));
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("payload"));
        assert!(!rendered.contains("77"));
    }
}
