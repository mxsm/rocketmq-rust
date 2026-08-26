// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
use rocketmq_error::NetworkError;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio_util::sync::CancellationToken;

use super::Connection;
use super::ConnectionState;
use super::FrameLimits;
use super::SessionLifecycle;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::admission::ResourceLimit;
use crate::dispatch::ResponseError;
use crate::dispatch::WriteProgress;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionSequence;
use crate::file_region::FileTransferMode;
use crate::telemetry::TransportTelemetry;
use crate::write_strategy::OutboundPayload;
use crate::write_strategy::QueuedWrite;
use crate::write_strategy::QueuedWriteProgress;
use crate::writer_runtime::run_session_writer;
use crate::writer_runtime::writer_lanes;
use crate::writer_runtime::WriterQueueConfig;

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

    let error = connection
        .send_response(RemotingCommand::create_remoting_command(1).set_body(vec![1]))
        .await
        .expect_err("body above the zero-byte profile must fail encoding");
    let ResponseError::Encode { source } = &error else {
        panic!("oversized response must retain the typed encoding failure")
    };
    assert_eq!(error.write_progress(), Some(WriteProgress::NotStarted));
    let _ = source.kind();
    assert!(Error::source(&error).is_some());
    assert_eq!(connection.state(), ConnectionState::Healthy);

    connection
        .send_response(RemotingCommand::create_remoting_command(2))
        .await
        .expect("a deterministic preflight failure must not poison the writer");
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
    lanes
        .try_send(
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
        )
        .expect("fill data lane");
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
    let error = connection
        .send_response(RemotingCommand::create_remoting_command(7))
        .await
        .expect_err("a full data lane must reject the typed response");

    assert!(matches!(&error, ResponseError::QueueSaturated));
    assert_eq!(error.write_progress(), Some(WriteProgress::NotStarted));
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

    let error = connection
        .send_response(RemotingCommand::create_remoting_command(7))
        .await
        .expect_err("admission exhaustion must reject before lane enqueue");
    assert!(matches!(&error, ResponseError::QueueSaturated));
    assert_eq!(error.write_progress(), Some(WriteProgress::NotStarted));
    drop(connection);
    drop(lanes);
    drop(receivers);
    drop(blocker);
    assert_eq!(controller.snapshot().queued.current_count, 0);
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
    assert!(matches!(
        response,
        ResponseError::Transport {
            progress: WriteProgress::NotStarted,
            ..
        }
    ));
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
    assert!(matches!(
        response,
        ResponseError::Transport {
            progress: WriteProgress::PossiblyPartial,
            ..
        }
    ));
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
        let error = connection
            .send_response(RemotingCommand::create_remoting_command(7))
            .await
            .expect_err("injected transport failure");

        let ResponseError::Transport { progress, source } = &error else {
            panic!("transport failures must preserve typed response completion")
        };
        assert_eq!(*progress, WriteProgress::PossiblyPartial);
        let RocketMQError::Shared(shared) = source else {
            panic!("writer completion must preserve the shared typed source")
        };
        assert!(matches!(shared.as_error(), RocketMQError::IO(_)));
        assert!(Error::source(&error).is_some());
        assert_eq!(connection.state(), ConnectionState::Closed);
        assert!(writes.load(Ordering::Acquire) >= 1);
        if matches!(phase, DirectFailurePhase::PartialThenError) {
            assert_eq!(writes.load(Ordering::Acquire), 2);
        }
    }
}

#[tokio::test]
async fn typed_borrowed_response_late_transport_failure_preserves_source_after_body_take() {
    let writes = Arc::new(AtomicUsize::new(0));
    let mut connection = Connection::new_with_plaintext_stream(FailingResponseTransport {
        writes: Arc::clone(&writes),
        phase: DirectFailurePhase::PartialThenError,
    });
    let mut command = RemotingCommand::create_remoting_command(7).set_body(b"borrowed-body".to_vec());

    let error = connection
        .send_response_ref(&mut command)
        .await
        .expect_err("injected late transport failure");

    assert!(
        command.body().is_none(),
        "borrowed response body must be taken before the late write failure"
    );
    let ResponseError::Transport { progress, source } = &error else {
        panic!("late transport failure must retain typed response completion")
    };
    assert_eq!(*progress, WriteProgress::PossiblyPartial);
    let RocketMQError::Shared(shared) = source else {
        panic!("late transport failure must preserve the writer source")
    };
    assert!(matches!(shared.as_error(), RocketMQError::IO(_)));
    assert!(Error::source(&error).is_some());
    assert_eq!(writes.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn explicit_sendfile_tls_preflight_preserves_legacy_reason_without_poisoning_typed_send() {
    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
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
            "typed-sendfile-target".to_string(),
        )
        .await
        .expect_err("TLS sendfile preflight must fail");
    assert!(matches!(
        typed.into_response(),
        ResponseError::Transport {
            progress: WriteProgress::NotStarted,
            ..
        }
    ));
    assert_eq!(connection.state(), ConnectionState::Healthy);
    connection
        .send_command(RemotingCommand::create_remoting_command(98))
        .await
        .expect("preflight rejection must leave typed writer healthy");

    let (transport, _peer) = tokio::io::duplex(4096);
    let mut legacy_connection =
        Connection::new_with_tls_stream(transport).with_file_region_io(blocking, FileTransferMode::Sendfile);
    let legacy = legacy_connection
        .send_payload_inner(
            explicit_sendfile_payload(),
            AdmissionClass::Data,
            None,
            None,
            "legacy-sendfile-target".to_string(),
        )
        .await
        .expect_err("TLS sendfile preflight must fail")
        .into_legacy();
    assert!(matches!(
        legacy,
        RocketMQError::Network(NetworkError::ConnectionFailed { addr, reason })
            if addr == "legacy-sendfile-target" && reason.contains("sendfile")
    ));
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

    assert!(matches!(
        response,
        ResponseError::Transport {
            progress: WriteProgress::NotStarted,
            ..
        }
    ));
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
        Err(RocketMQError::Network(NetworkError::DeadlineExceededBeforeSend { addr }))
            if addr == "legacy-direct-target"
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

    assert!(matches!(
        response,
        ResponseError::Transport {
            progress: WriteProgress::NotStarted,
            ..
        }
    ));
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

#[tokio::test]
async fn direct_legacy_facade_retains_the_callers_target_and_network_failure_shape() {
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

    assert!(matches!(
        error,
        RocketMQError::Network(NetworkError::ConnectionFailed { addr, reason })
            if addr == "direct-caller-target" && reason == "canonical writer failure"
    ));
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
            "typed-queued-target".to_string(),
        )
        .await
        .expect_err("partial queued write must fail")
        .into_response();
    writer.await.expect("poisoned writer task must exit");

    assert!(matches!(
        response,
        ResponseError::Transport {
            progress: WriteProgress::PossiblyPartial,
            ..
        }
    ));
    assert_eq!(connection.state(), ConnectionState::Closed);
}

#[tokio::test]
async fn queued_legacy_facade_reprojects_the_shared_writer_source_for_its_caller_target() {
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

    assert!(matches!(
        error,
        RocketMQError::Network(NetworkError::ConnectionFailed { addr, reason })
            if addr == "queued-caller-target" && reason == "canonical writer failure"
    ));
}
