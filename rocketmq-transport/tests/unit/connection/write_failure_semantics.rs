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
use std::io;
use std::io::IoSlice;
use std::io::Write as _;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeOwner;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio_util::sync::CancellationToken;

use super::classify_writer_failure;
use super::Connection;
use super::ConnectionState;
use super::FrameLimits;
use super::RequestStopPolicy;
use super::SessionLifecycle;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::AdmissionScopeHandle;
use crate::admission::ResourceLimit;
use crate::deadline::RequestDeadline;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseSendOutcome;
use crate::dispatch::WriteProgress;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionSequence;
use crate::file_region::FileTransferMode;
use crate::session_view::SessionStateView;
use crate::telemetry::TransportTelemetry;
use crate::write_strategy::OutboundPayload;
use crate::write_strategy::QueuedWrite;
use crate::write_strategy::QueuedWriteProgress;
use crate::writer_runtime::run_session_writer;
use crate::writer_runtime::writer_lanes;
use crate::writer_runtime::WriterEnqueueOutcome;
use crate::writer_runtime::WriterLanes;
use crate::writer_runtime::WriterQueueConfig;

fn expect_writer_enqueued(outcome: WriterEnqueueOutcome) {
    match outcome {
        WriterEnqueueOutcome::Enqueued => {}
        WriterEnqueueOutcome::Full(write) | WriterEnqueueOutcome::Closed(write) => {
            drop(write);
            panic!("writer lane accepts the blocker")
        }
    }
}

fn enqueue_raw_write(
    lanes: &WriterLanes,
    admission: &AdmissionScopeHandle,
    deadline: Option<RequestDeadline>,
    target: &'static str,
) -> tokio::sync::oneshot::Receiver<crate::write_result::WriterResult> {
    let permit = admission
        .try_acquire(AdmissionResource::Queued, 1, AdmissionClass::Data)
        .expect("reserve raw queued write");
    let (completion, result) = tokio::sync::oneshot::channel();
    expect_writer_enqueued(lanes.try_send(
        AdmissionClass::Data,
        QueuedWrite::data(
            OutboundPayload::Contiguous(Bytes::from_static(b"typed")),
            completion,
            permit,
            deadline,
            target.to_string(),
            Arc::new(QueuedWriteProgress::waiting()),
            std::time::Instant::now(),
        ),
    ));
    result
}

fn assert_operational_failure(outcome: ResponseSendOutcome, expected: WriteProgress, operation: &'static str) {
    match outcome {
        ResponseSendOutcome::OperationalFailure(error) => {
            assert_eq!(error.operation(), operation);
            assert_eq!(error.write_progress(), expected);
            let source = Error::source(&error).expect("operational response failure source");
            match operation {
                "encode" => assert!(source.downcast_ref::<RocketMQError>().is_some()),
                "transport" => assert!(source.downcast_ref::<rocketmq_error::Error>().is_some()),
                other => panic!("unexpected response operation {other}"),
            }
        }
        ResponseSendOutcome::Written => panic!("{operation} unexpectedly completed"),
        ResponseSendOutcome::Rejected(completion) => {
            panic!("{operation} unexpectedly returned normal completion {:?}", completion)
        }
    }
}

fn assert_normal_rejection(outcome: ResponseSendOutcome, expected: ResponseCompletionOutcome) {
    match outcome {
        ResponseSendOutcome::Rejected(actual) => assert_eq!(actual, expected),
        ResponseSendOutcome::Written => panic!("response unexpectedly completed"),
        ResponseSendOutcome::OperationalFailure(_) => panic!("response unexpectedly failed operationally"),
    }
}

fn assert_not_started_prewrite_operational(outcome: ResponseSendOutcome) {
    let ResponseSendOutcome::OperationalFailure(error) = outcome else {
        panic!("pre-write failure must remain operational")
    };
    assert_eq!(error.operation(), "transport");
    assert_eq!(error.write_progress(), WriteProgress::NotStarted);
    let source = Error::source(&error).expect("pre-write operational failure source");
    let canonical = source
        .downcast_ref::<rocketmq_error::Error>()
        .expect("pre-write failure retains the canonical writer error");
    assert_eq!(canonical.code(), rocketmq_error::TRANSPORT_WRITE_TIMEOUT.code());
    assert_eq!(canonical.context().to_string(), "phase=<redacted>, timeout_ms=10");
    assert!(canonical.source().is_none());
}

fn explicit_sendfile_payload() -> OutboundPayload {
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(&[0x5a]).expect("write leased byte");
    let body = FileRegionSequence::single(FileRegion::try_new(Arc::new(file), 0, 1).expect("leased file region"));
    let head = EncodedFrameHead::from_command_and_body_len(RemotingCommand::create_remoting_command(97), 1)
        .expect("file frame head");
    OutboundPayload::FileFrame { head, body }
}

#[derive(Clone, Copy)]
enum DirectFailurePhase {
    FirstWrite,
    PartialThenError,
    Flush,
}

struct FailingResponseTransport {
    writes: Arc<AtomicUsize>,
    phase: DirectFailurePhase,
}

impl AsyncRead for FailingResponseTransport {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for FailingResponseTransport {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let attempt = this.writes.fetch_add(1, Ordering::AcqRel);
        match this.phase {
            DirectFailurePhase::FirstWrite => Poll::Ready(Err(io::Error::other("injected direct write failure"))),
            DirectFailurePhase::PartialThenError if attempt == 0 => Poll::Ready(Ok(1)),
            DirectFailurePhase::PartialThenError => {
                Poll::Ready(Err(io::Error::other("injected partial write failure")))
            }
            DirectFailurePhase::Flush => Poll::Ready(Ok(buffer.len())),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let attempt = this.writes.fetch_add(1, Ordering::AcqRel);
        match this.phase {
            DirectFailurePhase::FirstWrite => Poll::Ready(Err(io::Error::other("injected direct write failure"))),
            DirectFailurePhase::PartialThenError if attempt == 0 => Poll::Ready(Ok(1)),
            DirectFailurePhase::PartialThenError => {
                Poll::Ready(Err(io::Error::other("injected partial write failure")))
            }
            DirectFailurePhase::Flush => Poll::Ready(Ok(buffers.iter().map(|buffer| buffer.len()).sum())),
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().phase {
            DirectFailurePhase::FirstWrite => Poll::Ready(Ok(())),
            DirectFailurePhase::PartialThenError => Poll::Ready(Ok(())),
            DirectFailurePhase::Flush => Poll::Ready(Err(io::Error::other("injected direct flush failure"))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct CountingTransport {
    writes: Arc<AtomicUsize>,
    flushes: Arc<AtomicUsize>,
}

impl AsyncRead for CountingTransport {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for CountingTransport {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.writes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.writes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(buffers.iter().map(|buffer| buffer.len()).sum()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().flushes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct PendingWriteTransport {
    writes: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
}

impl AsyncRead for PendingWriteTransport {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for PendingWriteTransport {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.writes.fetch_add(1, Ordering::AcqRel);
        this.entered.notify_one();
        Poll::Pending
    }

    fn poll_write_vectored(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.writes.fetch_add(1, Ordering::AcqRel);
        this.entered.notify_one();
        Poll::Pending
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn writer_diagnostics_use_the_retained_start_instant_for_latency() {
    let diagnostics = super::SessionWriterDiagnostics::new(1);
    let started_at = std::time::Instant::now();
    let finished_at = started_at
        .checked_add(std::time::Duration::from_millis(17))
        .expect("small duration remains representable");

    diagnostics.finish_write_at(started_at, finished_at, true, false);

    let snapshot = diagnostics.snapshot();
    assert_eq!(snapshot.last_write_latency_millis, 17);
    assert_eq!(snapshot.max_write_latency_millis, 17);
    assert_eq!(snapshot.completed, 1);
}

#[tokio::test]
async fn typed_response_encode_failure_is_not_started_and_keeps_direct_writer_healthy() {
    let limits = FrameLimits::try_new(1024, 1024, 0, 8).expect("valid test limits");
    let (transport, peer_transport) = tokio::io::duplex(4096);
    let mut connection = Connection::new_with_plaintext_stream_and_limits(transport, limits);

    assert_operational_failure(
        connection
            .send_response(RemotingCommand::create_remoting_command(1).set_body(vec![1]))
            .await,
        WriteProgress::NotStarted,
        "encode",
    );
    assert_eq!(connection.state(), ConnectionState::Healthy);

    assert!(matches!(
        connection
            .send_response(RemotingCommand::create_remoting_command(2))
            .await,
        ResponseSendOutcome::Written
    ));
    let mut peer = Connection::new_with_plaintext_stream_and_limits(peer_transport, limits);
    let response = peer
        .receive_command()
        .await
        .expect("peer response result")
        .expect("peer response command");
    assert_eq!(response.code(), 2);
}

#[tokio::test]
async fn typed_response_queue_saturation_is_not_started() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let config = WriterQueueConfig {
        data_capacity: NonZeroUsize::new(1).expect("non-zero"),
        ..WriterQueueConfig::default()
    };
    let (lanes, receivers) = writer_lanes(config);
    let permit = admission
        .try_acquire(AdmissionResource::Queued, 1, AdmissionClass::Data)
        .expect("reserve blocker");
    let (completion, _completion_result) = tokio::sync::oneshot::channel();
    expect_writer_enqueued(lanes.try_send(
        AdmissionClass::Data,
        QueuedWrite::data(
            OutboundPayload::Contiguous(Bytes::from_static(b"x")),
            completion,
            permit,
            None,
            "queued-blocker".to_string(),
            Arc::new(QueuedWriteProgress::waiting()),
            std::time::Instant::now(),
        ),
    ));
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let mut connection = Connection::new_queued(
        lanes.clone(),
        diagnostics,
        admission,
        state_tx,
        state_rx,
        CheetahString::from_string("typed-response-queue-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    assert_normal_rejection(
        connection
            .send_response(RemotingCommand::create_remoting_command(7))
            .await,
        ResponseCompletionOutcome::QueueSaturated,
    );
    drop(connection);
    drop(lanes);
    drop(receivers);
    assert_eq!(controller.snapshot().queued.current_count, 0);
}

#[tokio::test]
async fn typed_response_admission_saturation_is_not_started_before_lane_enqueue() {
    let limits = AdmissionLimits {
        queued: ResourceLimit { count: 1, bytes: 1024 },
        ..AdmissionLimits::default()
    };
    let controller = AdmissionController::new(limits);
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let blocker = admission
        .try_acquire(AdmissionResource::Queued, 1, AdmissionClass::Data)
        .expect("consume the admission budget without filling the lane");
    let config = WriterQueueConfig::default();
    let (lanes, receivers) = writer_lanes(config);
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let mut connection = Connection::new_queued(
        lanes.clone(),
        diagnostics,
        admission,
        state_tx,
        state_rx,
        CheetahString::from_string("typed-response-admission-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );

    assert_normal_rejection(
        connection
            .send_response(RemotingCommand::create_remoting_command(7))
            .await,
        ResponseCompletionOutcome::QueueSaturated,
    );
    drop(connection);
    drop(lanes);
    drop(receivers);
    drop(blocker);
    assert_eq!(controller.snapshot().queued.current_count, 0);
}

#[tokio::test]
async fn queued_response_api_records_queue_wait_but_generic_commands_do_not() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let physical_connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::new(AtomicUsize::new(0)),
        flushes: Arc::new(AtomicUsize::new(0)),
    });
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig::default();
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let (telemetry, capture) = TransportTelemetry::with_boundary_metric_capture();
    let mut connection = Connection::new_queued(
        lanes,
        Arc::clone(&diagnostics),
        admission,
        state_tx.clone(),
        state_rx,
        CheetahString::from_string("queued-response-observation-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        telemetry.clone(),
    );
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        telemetry,
    ));

    connection
        .send_command(RemotingCommand::create_remoting_command(7))
        .await
        .expect("generic command write");
    assert!(matches!(
        connection
            .send_response(RemotingCommand::create_response_command_with_code(0))
            .await,
        ResponseSendOutcome::Written
    ));

    assert_eq!(capture.response_queue_waits(), 1);
    drop(connection);
    writer.await.expect("writer exits after the last queue handle drops");
}

#[tokio::test]
async fn actual_dropped_queued_completion_before_start_maps_to_typed_not_started() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let config = WriterQueueConfig::default();
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, mut receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let connection = Connection::new_queued(
        lanes.clone(),
        diagnostics,
        admission,
        state_tx,
        state_rx,
        CheetahString::from_string("dropped-before-start-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    let send = tokio::spawn(async move {
        let mut connection = connection;
        let outcome = connection
            .send_payload_inner(
                OutboundPayload::Contiguous(Bytes::from_static(b"dropped-before-start")),
                AdmissionClass::Data,
                None,
                None,
                None,
                RequestStopPolicy::All,
                None,
                "dropped-before-start-target".to_string(),
            )
            .await;
        (connection, outcome)
    });

    receivers.drop_next_write_for_test().await;
    let (connection, outcome) = send.await.expect("queued send task");
    let response = outcome
        .expect_err("dropped completion must fail the waiting caller")
        .into_response();
    assert_operational_failure(response, WriteProgress::NotStarted, "transport");
    drop(connection);
    drop(lanes);
    assert_eq!(controller.snapshot().queued.current_count, 0);
}

#[tokio::test]
async fn actual_dropped_active_completion_maps_to_typed_possibly_partial() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let writes = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let physical_connection = Connection::new_with_plaintext_stream(PendingWriteTransport {
        writes: Arc::clone(&writes),
        entered: Arc::clone(&entered),
    });
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig::default();
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let connection = Connection::new_queued(
        lanes,
        Arc::clone(&diagnostics),
        admission,
        state_tx.clone(),
        state_rx,
        CheetahString::from_string("dropped-active-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));
    let send = tokio::spawn(async move {
        let mut connection = connection;
        let outcome = connection
            .send_payload_inner(
                OutboundPayload::Contiguous(Bytes::from_static(b"dropped-active")),
                AdmissionClass::Data,
                None,
                None,
                None,
                RequestStopPolicy::All,
                None,
                "dropped-active-target".to_string(),
            )
            .await;
        (connection, outcome)
    });

    entered.notified().await;
    writer.abort();
    assert!(writer.await.expect_err("aborted writer task").is_cancelled());
    let (connection, outcome) = send.await.expect("queued send task");
    let response = outcome
        .expect_err("dropped active completion must fail the waiting caller")
        .into_response();
    assert_operational_failure(response, WriteProgress::PossiblyPartial, "transport");
    assert_eq!(writes.load(Ordering::Acquire), 1);
    drop(connection);
    assert_eq!(controller.snapshot().queued.current_count, 0);
}

#[tokio::test]
async fn typed_response_write_and_flush_failures_are_possibly_partial_with_a_typed_source() {
    for phase in [
        DirectFailurePhase::FirstWrite,
        DirectFailurePhase::PartialThenError,
        DirectFailurePhase::Flush,
    ] {
        let writes = Arc::new(AtomicUsize::new(0));
        let mut connection = Connection::new_with_plaintext_stream(FailingResponseTransport {
            writes: Arc::clone(&writes),
            phase,
        });
        assert_operational_failure(
            connection
                .send_response(RemotingCommand::create_remoting_command(7))
                .await,
            WriteProgress::PossiblyPartial,
            "transport",
        );
        assert_eq!(connection.state(), ConnectionState::Closed);
        assert!(writes.load(Ordering::Acquire) >= 1);
        if matches!(phase, DirectFailurePhase::PartialThenError) {
            assert_eq!(writes.load(Ordering::Acquire), 2);
        }
    }
}

#[tokio::test]
async fn explicit_sendfile_tls_preflight_preserves_legacy_reason_without_poisoning_typed_send() {
    let owner = RuntimeOwner::new().expect("runtime owner");
    let blocking = owner
        .root_context()
        .component("typed-sendfile-preflight-test")
        .storage_io()
        .clone();
    let (transport, _peer) = tokio::io::duplex(4096);
    let mut connection =
        Connection::new_with_tls_stream(transport).with_file_region_io(blocking.clone(), FileTransferMode::Sendfile);

    let typed = connection
        .send_payload_inner(
            explicit_sendfile_payload(),
            AdmissionClass::Data,
            None,
            None,
            None,
            RequestStopPolicy::All,
            None,
            "typed-sendfile-target".to_string(),
        )
        .await
        .expect_err("TLS sendfile preflight must fail");
    assert_operational_failure(typed.into_response(), WriteProgress::NotStarted, "transport");
    assert_eq!(connection.state(), ConnectionState::Healthy);
    connection
        .send_command(RemotingCommand::create_remoting_command(98))
        .await
        .expect("preflight rejection must leave typed writer healthy");

    let (transport, _peer) = tokio::io::duplex(4096);
    let mut legacy_connection =
        Connection::new_with_tls_stream(transport).with_file_region_io(blocking, FileTransferMode::Sendfile);
    let canonical = legacy_connection
        .send_payload_inner(
            explicit_sendfile_payload(),
            AdmissionClass::Data,
            None,
            None,
            None,
            RequestStopPolicy::All,
            None,
            "legacy-sendfile-target".to_string(),
        )
        .await
        .expect_err("TLS sendfile preflight must fail")
        .into_error();
    let RocketMQError::Shared(source) = canonical else {
        panic!("sendfile preflight must use the canonical Shared carrier")
    };
    assert_eq!(source.code(), rocketmq_error::TRANSPORT_CONNECTION_FAILED.code());
    assert!(source.source().is_some());
    assert_eq!(legacy_connection.state(), ConnectionState::Healthy);
}

#[tokio::test(start_paused = true)]
async fn direct_preflight_deadline_is_not_started_and_keeps_the_writer_healthy() {
    let writes = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
    });
    connection.set_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&checked),
        Arc::clone(&resume),
    ));
    let send = tokio::spawn(async move {
        let outcome = connection
            .send_payload_inner(
                OutboundPayload::Contiguous(Bytes::from_static(b"typed")),
                AdmissionClass::Data,
                None,
                Some(crate::deadline::RequestDeadline::after(
                    std::time::Duration::from_millis(10),
                )),
                None,
                RequestStopPolicy::All,
                None,
                "typed-direct-target".to_string(),
            )
            .await;
        (connection, outcome)
    });

    checked.notified().await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    resume.notify_one();
    let (mut connection, outcome) = send.await.expect("direct send task");
    let response = outcome.expect_err("preflight deadline must fail").into_response();
    assert_normal_rejection(response, ResponseCompletionOutcome::DeadlineExpired);
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert_eq!(connection.state(), ConnectionState::Healthy);
    connection
        .send_command(RemotingCommand::create_remoting_command(11))
        .await
        .expect("healthy direct writer must remain usable");
    assert!(writes.load(Ordering::Acquire) > 0);
    assert!(flushes.load(Ordering::Acquire) > 0);

    let writes = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut legacy_connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
    });
    legacy_connection.set_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&checked),
        Arc::clone(&resume),
    ));
    let legacy_send = tokio::spawn(async move {
        let result = legacy_connection
            .send_command_with_deadline(
                RemotingCommand::create_remoting_command(10),
                crate::deadline::RequestDeadline::after(std::time::Duration::from_millis(10)),
                "legacy-direct-target",
            )
            .await;
        (legacy_connection, result)
    });

    checked.notified().await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    resume.notify_one();
    let (legacy_connection, result) = legacy_send.await.expect("legacy direct send task");
    assert!(matches!(
        result,
        Err(RocketMQError::Timeout {
            operation: "transport_before_send",
            timeout_ms: 10,
        })
    ));
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert_eq!(legacy_connection.state(), ConnectionState::Healthy);
}

#[tokio::test(start_paused = true)]
async fn queued_preflight_deadline_matches_direct_not_started_classification_without_socket_io() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let writes = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut physical_connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
    });
    physical_connection.set_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&checked),
        Arc::clone(&resume),
    ));
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig::default();
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let queued_connection = Connection::new_queued(
        lanes,
        Arc::clone(&diagnostics),
        admission,
        state_tx.clone(),
        state_rx,
        CheetahString::from_string("queued-preflight-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));
    let send = tokio::spawn(async move {
        let mut connection = queued_connection;
        let outcome = connection
            .send_payload_inner(
                OutboundPayload::Contiguous(Bytes::from_static(b"queued")),
                AdmissionClass::Data,
                None,
                Some(crate::deadline::RequestDeadline::after(
                    std::time::Duration::from_millis(10),
                )),
                None,
                RequestStopPolicy::All,
                None,
                "typed-queued-target".to_string(),
            )
            .await;
        (connection, outcome)
    });

    checked.notified().await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    resume.notify_one();
    let (mut queued_connection, outcome) = send.await.expect("queued send task");
    let response = outcome
        .expect_err("queued preflight deadline must fail")
        .into_response();
    assert_normal_rejection(response, ResponseCompletionOutcome::DeadlineExpired);
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert_eq!(queued_connection.state(), ConnectionState::Healthy);
    queued_connection
        .send_command(RemotingCommand::create_remoting_command(12))
        .await
        .expect("healthy queued writer must remain usable");
    assert!(writes.load(Ordering::Acquire) > 0);
    assert!(flushes.load(Ordering::Acquire) > 0);
    drop(queued_connection);
    writer.await.expect("writer must exit after queue handles drop");
}

#[tokio::test(start_paused = true)]
async fn micro_batch_preflight_deadline_only_rejects_the_member_that_caused_the_cutoff() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let writes = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut physical_connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
    });
    physical_connection.set_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&checked),
        Arc::clone(&resume),
    ));
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig {
        max_write_stall: std::time::Duration::from_secs(1),
        ..WriterQueueConfig::default()
    };
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let short_deadline = RequestDeadline::after(std::time::Duration::from_millis(10));
    let late_deadline = RequestDeadline::after(std::time::Duration::from_millis(80));
    let short_result = enqueue_raw_write(&lanes, &admission, Some(short_deadline), "short-member");
    let late_result = enqueue_raw_write(&lanes, &admission, Some(late_deadline), "late-member");
    let no_deadline_result = enqueue_raw_write(&lanes, &admission, None, "undeadlined-member");
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));

    checked.notified().await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    let short_failure = short_result
        .await
        .expect("short member completion")
        .expect_err("short deadline must fail the pre-write batch");
    let late_failure = late_result
        .await
        .expect("late member completion")
        .expect_err("late member shares the pre-write batch failure");
    let no_deadline_failure = no_deadline_result
        .await
        .expect("undeadlined member completion")
        .expect_err("undeadlined member shares the pre-write batch failure");
    assert!(Arc::ptr_eq(
        short_failure.error().expect("short operational failure"),
        late_failure.error().expect("late operational failure")
    ));
    assert!(Arc::ptr_eq(
        short_failure.error().expect("short operational failure"),
        no_deadline_failure.error().expect("undeadlined operational failure")
    ));

    tokio::time::advance(std::time::Duration::from_millis(80)).await;
    assert_normal_rejection(
        classify_writer_failure("short-member".to_string(), short_failure, Some(short_deadline)).into_response(),
        ResponseCompletionOutcome::DeadlineExpired,
    );
    assert_not_started_prewrite_operational(
        classify_writer_failure("late-member".to_string(), late_failure, Some(late_deadline)).into_response(),
    );
    assert_not_started_prewrite_operational(
        classify_writer_failure("undeadlined-member".to_string(), no_deadline_failure, None).into_response(),
    );
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert_eq!(*state_rx.borrow(), ConnectionState::Healthy);
    assert_eq!(controller.snapshot().queued.current_count, 0);

    let followup = enqueue_raw_write(&lanes, &admission, None, "followup-member");
    followup
        .await
        .expect("follow-up completion")
        .expect("healthy writer remains reusable");
    assert!(writes.load(Ordering::Acquire) > 0);
    assert!(flushes.load(Ordering::Acquire) > 0);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    drop(lanes);
    writer.await.expect("writer exits after queue handles drop");
}

#[tokio::test(start_paused = true)]
async fn prewrite_stall_without_an_owner_deadline_remains_a_typed_failure() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let writes = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut physical_connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
    });
    physical_connection.set_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&checked),
        Arc::clone(&resume),
    ));
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig {
        max_write_stall: std::time::Duration::from_millis(10),
        ..WriterQueueConfig::default()
    };
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let stalled_result = enqueue_raw_write(&lanes, &admission, None, "stalled-member");
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));

    checked.notified().await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    let failure = stalled_result
        .await
        .expect("stalled member completion")
        .expect_err("writer stall must fail before socket I/O");
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_not_started_prewrite_operational(
        classify_writer_failure("stalled-member".to_string(), failure, None).into_response(),
    );
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert_eq!(*state_rx.borrow(), ConnectionState::Healthy);
    assert_eq!(controller.snapshot().queued.current_count, 0);

    let followup = enqueue_raw_write(&lanes, &admission, None, "followup-member");
    followup
        .await
        .expect("follow-up completion")
        .expect("healthy writer remains reusable");
    assert!(writes.load(Ordering::Acquire) > 0);
    assert!(flushes.load(Ordering::Acquire) > 0);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    drop(lanes);
    writer.await.expect("writer exits after queue handles drop");
}

#[tokio::test(start_paused = true)]
async fn queued_control_does_not_reclassify_a_completed_stall_after_its_later_deadline() {
    let runtime = RuntimeContext::from_current("queued-completion-cause-test");
    let parent = runtime
        .service_context("queued-completion-cause-test")
        .task_group()
        .clone();
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let writes = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut physical_connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
    });
    physical_connection.set_write_preflight_barrier(crate::write_strategy::WritePreflightBarrier::new(
        Arc::clone(&checked),
        Arc::clone(&resume),
    ));
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig {
        max_write_stall: std::time::Duration::from_millis(10),
        ..WriterQueueConfig::default()
    };
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let mut connection = Connection::new_queued(
        lanes.clone(),
        Arc::clone(&diagnostics),
        admission,
        state_tx.clone(),
        state_rx,
        CheetahString::from_string("queued-completion-cause-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        Arc::clone(&diagnostics),
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));
    let owner_deadline = RequestDeadline::after(std::time::Duration::from_millis(80));
    let (control_state_tx, control_state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
    let control = RequestControlView::from_meta(
        &RequestMeta::new(std::time::Instant::now(), Some(owner_deadline)),
        SessionStateView::from_receivers(control_state_rx, closed_rx),
        &parent,
    );
    let mut send = Box::pin(connection.send_payload_inner(
        OutboundPayload::Contiguous(Bytes::from_static(b"delayed-observer")),
        AdmissionClass::Data,
        None,
        Some(owner_deadline),
        Some(&control),
        RequestStopPolicy::All,
        None,
        "delayed-observer".to_string(),
    ));
    assert!(futures_util::poll!(&mut send).is_pending());

    checked.notified().await;
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(diagnostics.snapshot().failed, 1);
    tokio::time::advance(std::time::Duration::from_millis(80)).await;
    assert!(control.deadline().is_some_and(RequestDeadline::is_expired));
    let response = send
        .await
        .expect_err("the completed writer stall must remain a typed failure")
        .into_response();
    assert_not_started_prewrite_operational(response);
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert_eq!(connection.state(), ConnectionState::Healthy);
    assert_eq!(controller.snapshot().queued.current_count, 0);

    connection
        .send_command(RemotingCommand::create_remoting_command(13))
        .await
        .expect("completed stall leaves the canonical writer reusable");
    assert!(writes.load(Ordering::Acquire) > 0);
    assert!(flushes.load(Ordering::Acquire) > 0);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    drop(connection);
    drop(lanes);
    writer.await.expect("writer exits after queue handles drop");
    drop(control);
    drop(control_state_tx);
    drop(closed_tx);
    drop(parent);
    let shutdown = runtime.shutdown_tasks(std::time::Duration::from_secs(1)).await;
    assert!(shutdown.is_healthy(), "{}", shutdown.to_json());
}

#[tokio::test(start_paused = true)]
async fn queued_control_does_not_reclassify_a_dropped_completion_after_its_later_deadline() {
    let runtime = RuntimeContext::from_current("queued-dropped-completion-cause-test");
    let parent = runtime
        .service_context("queued-dropped-completion-cause-test")
        .task_group()
        .clone();
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let writes = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let physical_connection = Connection::new_with_plaintext_stream(CountingTransport {
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
    });
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig::default();
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, mut receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let mut connection = Connection::new_queued(
        lanes.clone(),
        Arc::clone(&diagnostics),
        admission,
        state_tx.clone(),
        state_rx,
        CheetahString::from_string("queued-dropped-completion-cause-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    let owner_deadline = RequestDeadline::after(std::time::Duration::from_millis(80));
    let (control_state_tx, control_state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
    let control = RequestControlView::from_meta(
        &RequestMeta::new(std::time::Instant::now(), Some(owner_deadline)),
        SessionStateView::from_receivers(control_state_rx, closed_rx),
        &parent,
    );
    let mut send = Box::pin(connection.send_payload_inner(
        OutboundPayload::Contiguous(Bytes::from_static(b"dropped-observer")),
        AdmissionClass::Data,
        None,
        Some(owner_deadline),
        Some(&control),
        RequestStopPolicy::All,
        None,
        "dropped-observer".to_string(),
    ));
    assert!(futures_util::poll!(&mut send).is_pending());

    receivers.drop_next_write_for_test().await;
    tokio::time::advance(std::time::Duration::from_millis(80)).await;
    assert!(control.deadline().is_some_and(RequestDeadline::is_expired));
    let response = send
        .await
        .expect_err("a dropped writer completion must remain a typed failure")
        .into_response();
    let ResponseSendOutcome::OperationalFailure(error) = response else {
        panic!("a dropped writer completion must remain operational")
    };
    assert_eq!(error.operation(), "transport");
    assert_eq!(error.write_progress(), WriteProgress::NotStarted);
    let source = Error::source(&error).expect("dropped completion source");
    let canonical = source
        .downcast_ref::<rocketmq_error::Error>()
        .expect("dropped completion retains the canonical writer error");
    assert_eq!(canonical.code(), rocketmq_error::TRANSPORT_CONNECTION_FAILED.code());
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert_eq!(connection.state(), ConnectionState::Healthy);
    assert_eq!(controller.snapshot().queued.current_count, 0);

    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));
    connection
        .send_command(RemotingCommand::create_remoting_command(14))
        .await
        .expect("dropped completion leaves the canonical writer reusable");
    assert!(writes.load(Ordering::Acquire) > 0);
    assert!(flushes.load(Ordering::Acquire) > 0);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    drop(connection);
    drop(lanes);
    writer.await.expect("writer exits after queue handles drop");
    drop(control);
    drop(control_state_tx);
    drop(closed_tx);
    drop(parent);
    let shutdown = runtime.shutdown_tasks(std::time::Duration::from_secs(1)).await;
    assert!(shutdown.is_healthy(), "{}", shutdown.to_json());
}

#[tokio::test]
async fn direct_facade_preserves_the_canonical_writer_error_and_typed_source() {
    let writes = Arc::new(AtomicUsize::new(0));
    let mut connection = Connection::new_with_plaintext_stream(FailingResponseTransport {
        writes,
        phase: DirectFailurePhase::FirstWrite,
    });
    let error = connection
        .send_command_with_deadline(
            RemotingCommand::create_remoting_command(8),
            crate::deadline::RequestDeadline::after(std::time::Duration::from_secs(1)),
            "direct-caller-target",
        )
        .await
        .expect_err("injected direct write failure");

    let RocketMQError::Shared(source) = error else {
        panic!("direct writer failure must use the canonical Shared carrier")
    };
    assert_eq!(source.code(), rocketmq_error::TRANSPORT_CONNECTION_FAILED.code());
    assert!(source
        .source()
        .and_then(|source| source.downcast_ref::<io::Error>())
        .is_some());
}

#[tokio::test]
async fn queued_typed_transport_failure_matches_direct_possibly_partial_classification() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let physical_connection = Connection::new_with_plaintext_stream(FailingResponseTransport {
        writes: Arc::new(AtomicUsize::new(0)),
        phase: DirectFailurePhase::PartialThenError,
    });
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig::default();
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let mut connection = Connection::new_queued(
        lanes,
        Arc::clone(&diagnostics),
        admission,
        state_tx.clone(),
        state_rx,
        CheetahString::from_string("queued-typed-failure-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));

    let response = connection
        .send_payload_inner(
            OutboundPayload::Contiguous(Bytes::from_static(b"queued-typed")),
            AdmissionClass::Data,
            None,
            None,
            None,
            RequestStopPolicy::All,
            None,
            "typed-queued-target".to_string(),
        )
        .await
        .expect_err("partial queued write must fail")
        .into_response();
    writer.await.expect("poisoned writer task must exit");

    assert_operational_failure(response, WriteProgress::PossiblyPartial, "transport");
    assert_eq!(connection.state(), ConnectionState::Closed);
}

#[tokio::test]
async fn queued_facade_preserves_the_shared_canonical_writer_source() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let writes = Arc::new(AtomicUsize::new(0));
    let physical_connection = Connection::new_with_plaintext_stream(FailingResponseTransport {
        writes,
        phase: DirectFailurePhase::FirstWrite,
    });
    let (frame_writer, _reader) = physical_connection.into_session_io(admission.clone());
    let config = WriterQueueConfig::default();
    let diagnostics = Arc::new(super::SessionWriterDiagnostics::new(config.total_capacity()));
    let (lanes, receivers) = writer_lanes(config);
    let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
    let mut queued_connection = Connection::new_queued(
        lanes,
        Arc::clone(&diagnostics),
        admission,
        state_tx.clone(),
        state_rx,
        CheetahString::from_string("queued-legacy-test".to_string()),
        FrameLimits::default(),
        Some(AdmissionClass::Data),
        Arc::new(SessionLifecycle::new()),
        TransportTelemetry::noop(),
    );
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state_tx,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));

    let error = queued_connection
        .send_command_with_deadline(
            RemotingCommand::create_remoting_command(9),
            crate::deadline::RequestDeadline::after(std::time::Duration::from_secs(1)),
            "queued-caller-target",
        )
        .await
        .expect_err("injected queued write failure");
    writer.await.expect("writer task must exit after poisoning");

    let RocketMQError::Shared(source) = error else {
        panic!("queued writer failure must use the canonical Shared carrier")
    };
    assert_eq!(source.code(), rocketmq_error::TRANSPORT_CONNECTION_FAILED.code());
    assert!(source
        .source()
        .and_then(|source| source.downcast_ref::<io::Error>())
        .is_some());
}
