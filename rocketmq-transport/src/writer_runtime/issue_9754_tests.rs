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
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrameHead;
use rocketmq_protocol::protocol::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::run_session_writer;
use super::writer_lanes;
use super::MicroBatchConfig;
use super::WriterEvent;
use super::WriterQueueConfig;
use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::connection::Connection;
use crate::connection::ConnectionState;
use crate::connection::SessionWriterDiagnostics;
use crate::dispatch::WriteProgress;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionLease;
use crate::file_region::FileRegionSequence;
use crate::file_region::FileTransferMode;
use crate::telemetry::TransportTelemetry;
use crate::write_result::WriterFailure;
use crate::write_strategy::OutboundPayload;
use crate::write_strategy::QueuedWrite;
use crate::write_strategy::QueuedWriteProgress;

struct DropLease {
    file: std::fs::File,
    drops: Arc<AtomicUsize>,
}

impl FileRegionLease for DropLease {
    fn file(&self) -> &std::fs::File {
        &self.file
    }
}

impl Drop for DropLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn queued_write_with_completion(
    admission: &crate::admission::AdmissionScopeHandle,
    target: &'static str,
    bytes: usize,
    progress: Arc<QueuedWriteProgress>,
) -> (QueuedWrite, oneshot::Receiver<crate::write_result::WriterResult>) {
    let permit = admission
        .try_acquire(AdmissionResource::Queued, bytes, AdmissionClass::Data)
        .expect("queue admission");
    let (completion, result) = oneshot::channel();
    (
        QueuedWrite::data(
            OutboundPayload::Contiguous(Bytes::from(vec![0x5a; bytes])),
            completion,
            permit,
            None,
            target.to_string(),
            progress,
            Instant::now(),
        ),
        result,
    )
}

fn queued_file_write_with_completion(
    admission: &crate::admission::AdmissionScopeHandle,
    target: &'static str,
) -> (
    QueuedWrite,
    oneshot::Receiver<crate::write_result::WriterResult>,
    Arc<AtomicUsize>,
) {
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(&[0x5a; 16]).expect("write file body");
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = Arc::new(DropLease {
        file,
        drops: Arc::clone(&drops),
    });
    let region = FileRegion::try_new(lease.clone(), 0, 16).expect("leased file region");
    let head = EncodedFrameHead::from_command_and_body_len(
        RemotingCommand::create_remoting_command(91),
        region.len() as usize,
    )
    .expect("file frame head");
    let payload = OutboundPayload::FileFrame {
        head,
        body: FileRegionSequence::single(region),
    };
    let permit = admission
        .try_acquire(AdmissionResource::Queued, payload.encoded_len(), AdmissionClass::Data)
        .expect("queue admission");
    let (completion, result) = oneshot::channel();
    let write = QueuedWrite::data(
        payload,
        completion,
        permit,
        None,
        target.to_string(),
        Arc::new(QueuedWriteProgress::waiting()),
        Instant::now(),
    );
    drop(lease);
    (write, result, drops)
}

#[derive(Clone, Copy)]
enum WriteFailurePhase {
    FirstWrite,
    Flush,
}

struct FailingTransport {
    attempts: Arc<AtomicUsize>,
    flushes: Arc<AtomicUsize>,
    phase: WriteFailurePhase,
}

struct PendingTransport {
    attempts: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
}

struct SuccessfulTransport {
    attempts: Arc<AtomicUsize>,
}

impl AsyncRead for SuccessfulTransport {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for SuccessfulTransport {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(buffers.iter().map(|buffer| buffer.len()).sum()))
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

impl AsyncRead for PendingTransport {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for PendingTransport {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.attempts.fetch_add(1, Ordering::AcqRel);
        this.entered.notify_one();
        Poll::Pending
    }

    fn poll_write_vectored(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.attempts.fetch_add(1, Ordering::AcqRel);
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

impl AsyncRead for FailingTransport {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for FailingTransport {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.attempts.fetch_add(1, Ordering::AcqRel);
        match this.phase {
            WriteFailurePhase::FirstWrite => Poll::Ready(Err(io::Error::other("injected first write failure"))),
            WriteFailurePhase::Flush => Poll::Ready(Ok(buffer.len())),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.attempts.fetch_add(1, Ordering::AcqRel);
        match this.phase {
            WriteFailurePhase::FirstWrite => Poll::Ready(Err(io::Error::other("injected first write failure"))),
            WriteFailurePhase::Flush => Poll::Ready(Ok(buffers.iter().map(|buffer| buffer.len()).sum())),
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.flushes.fetch_add(1, Ordering::AcqRel);
        match this.phase {
            WriteFailurePhase::FirstWrite => Poll::Ready(Ok(())),
            WriteFailurePhase::Flush => Poll::Ready(Err(io::Error::other("injected flush failure"))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn queue_config() -> WriterQueueConfig {
    WriterQueueConfig {
        data_capacity: NonZeroUsize::new(4).expect("non-zero"),
        control_capacity: NonZeroUsize::new(4).expect("non-zero"),
        data_max_bytes: NonZeroUsize::new(256).expect("non-zero"),
        control_max_bytes: NonZeroUsize::new(256).expect("non-zero"),
        control_burst: NonZeroUsize::new(2).expect("non-zero"),
        max_write_stall: Duration::from_secs(1),
        batch: MicroBatchConfig {
            max_items: NonZeroUsize::new(3).expect("non-zero"),
            max_bytes: NonZeroUsize::new(128).expect("non-zero"),
            max_delay: Duration::ZERO,
            max_iov: NonZeroUsize::new(8).expect("non-zero"),
        },
    }
}

#[test]
fn queued_deadline_cancellation_cannot_race_a_later_writer_claim() {
    let progress = QueuedWriteProgress::waiting();
    assert!(progress.cancel_before_start());
    assert!(!progress.claim_for_writer());
    assert!(!progress.write_started());

    let progress = QueuedWriteProgress::waiting();
    assert!(progress.claim_for_writer());
    assert!(!progress.cancel_before_start());
    progress.start_write();
    assert!(progress.write_started());
}

#[tokio::test]
async fn active_batch_failure_shares_source_and_drains_followers_without_a_second_socket_attempt() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let attempts = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let connection = Connection::new_with_plaintext_stream(FailingTransport {
        attempts: Arc::clone(&attempts),
        flushes,
        phase: WriteFailurePhase::FirstWrite,
    });
    let (frame_writer, _reader) = connection.into_session_io(admission.clone());
    let mut config = queue_config();
    config.batch.max_items = NonZeroUsize::new(2).expect("non-zero");
    let (lanes, receivers) = writer_lanes(config);
    let (first, first_completion) =
        queued_write_with_completion(&admission, "first-target", 16, Arc::new(QueuedWriteProgress::waiting()));
    let (second, second_completion) = queued_write_with_completion(
        &admission,
        "second-target",
        16,
        Arc::new(QueuedWriteProgress::waiting()),
    );
    let (follower, follower_completion) = queued_write_with_completion(
        &admission,
        "follower-target",
        16,
        Arc::new(QueuedWriteProgress::waiting()),
    );
    lanes.try_send(AdmissionClass::Data, first).expect("first queued write");
    lanes
        .try_send(AdmissionClass::Data, second)
        .expect("second queued write");
    lanes
        .try_send(AdmissionClass::Data, follower)
        .expect("follower queued write");
    drop(lanes);

    let diagnostics = Arc::new(SessionWriterDiagnostics::new(config.total_capacity()));
    let (state, _) = watch::channel(ConnectionState::Healthy);
    run_session_writer(
        frame_writer,
        receivers,
        Arc::clone(&diagnostics),
        state,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    )
    .await;

    let first = first_completion
        .await
        .expect("first completion")
        .expect_err("first write must fail");
    let second = second_completion
        .await
        .expect("second completion")
        .expect_err("second write must fail");
    let follower = follower_completion
        .await
        .expect("follower completion")
        .expect_err("poison-drained follower must fail");

    assert_eq!(first.progress(), WriteProgress::PossiblyPartial);
    assert_eq!(second.progress(), WriteProgress::PossiblyPartial);
    assert_eq!(follower.progress(), WriteProgress::NotStarted);
    assert!(std::ptr::eq(first.source().as_error(), second.source().as_error()));
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    assert_eq!(controller.snapshot().queued.current_bytes, 0);
    assert_eq!(diagnostics.snapshot().failed, 3);
}

#[tokio::test]
async fn active_file_failure_and_poison_drain_release_each_file_lease_once() {
    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    let blocking = owner
        .root_context()
        .component("writer-file-lease-test")
        .storage_io()
        .clone();
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let attempts = Arc::new(AtomicUsize::new(0));
    let connection = Connection::new_with_plaintext_stream(FailingTransport {
        attempts: Arc::clone(&attempts),
        flushes: Arc::new(AtomicUsize::new(0)),
        phase: WriteFailurePhase::FirstWrite,
    })
    .with_file_region_io(blocking, FileTransferMode::Portable);
    let (frame_writer, _reader) = connection.into_session_io(admission.clone());
    let mut config = queue_config();
    config.batch.max_items = NonZeroUsize::new(1).expect("non-zero");
    config.data_max_bytes = NonZeroUsize::new(1024).expect("non-zero");
    let (lanes, receivers) = writer_lanes(config);
    let (active, active_completion, active_drops) = queued_file_write_with_completion(&admission, "active-file");
    let (follower, follower_completion, follower_drops) =
        queued_file_write_with_completion(&admission, "follower-file");
    lanes
        .try_send(AdmissionClass::Data, active)
        .expect("active file queued");
    lanes
        .try_send(AdmissionClass::Data, follower)
        .expect("follower file queued");
    drop(lanes);
    let diagnostics = Arc::new(SessionWriterDiagnostics::new(config.total_capacity()));
    let (state, _) = watch::channel(ConnectionState::Healthy);
    run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    )
    .await;

    assert_eq!(
        active_completion
            .await
            .expect("active completion")
            .expect_err("active file head write must fail")
            .progress(),
        WriteProgress::PossiblyPartial
    );
    assert_eq!(
        follower_completion
            .await
            .expect("follower completion")
            .expect_err("poison-drained file write must fail")
            .progress(),
        WriteProgress::NotStarted
    );
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    assert_eq!(active_drops.load(Ordering::SeqCst), 1);
    assert_eq!(follower_drops.load(Ordering::SeqCst), 1);
    assert_eq!(controller.snapshot().queued.current_count, 0);
}

#[tokio::test]
async fn successful_file_write_releases_its_lease_once_after_completion() {
    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    let blocking = owner
        .root_context()
        .component("writer-file-success-test")
        .storage_io()
        .clone();
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let attempts = Arc::new(AtomicUsize::new(0));
    let connection = Connection::new_with_plaintext_stream(SuccessfulTransport {
        attempts: Arc::clone(&attempts),
    })
    .with_file_region_io(blocking, FileTransferMode::Portable);
    let (frame_writer, _reader) = connection.into_session_io(admission.clone());
    let mut config = queue_config();
    config.batch.max_items = NonZeroUsize::new(1).expect("non-zero");
    config.data_max_bytes = NonZeroUsize::new(1024).expect("non-zero");
    let (lanes, receivers) = writer_lanes(config);
    let (write, completion, drops) = queued_file_write_with_completion(&admission, "successful-file");
    lanes
        .try_send(AdmissionClass::Data, write)
        .expect("successful file queued");
    drop(lanes);
    let diagnostics = Arc::new(SessionWriterDiagnostics::new(config.total_capacity()));
    let (state, _) = watch::channel(ConnectionState::Healthy);
    run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    )
    .await;

    completion.await.expect("file completion").expect("file write succeeds");
    assert!(
        attempts.load(Ordering::Acquire) >= 2,
        "head and body must both be written"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(controller.snapshot().queued.current_count, 0);
}

#[tokio::test]
async fn queued_file_cancellation_releases_its_lease_before_socket_progress() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let attempts = Arc::new(AtomicUsize::new(0));
    let connection = Connection::new_with_plaintext_stream(FailingTransport {
        attempts: Arc::clone(&attempts),
        flushes: Arc::new(AtomicUsize::new(0)),
        phase: WriteFailurePhase::Flush,
    });
    let (frame_writer, _reader) = connection.into_session_io(admission.clone());
    let mut config = queue_config();
    config.data_max_bytes = NonZeroUsize::new(1024).expect("non-zero");
    let (lanes, receivers) = writer_lanes(config);
    let (write, completion, drops) = queued_file_write_with_completion(&admission, "cancelled-file");
    let progress = Arc::clone(&write.progress);
    lanes.try_send(AdmissionClass::Data, write).expect("file queued");
    assert!(progress.cancel_before_start());
    drop(lanes);
    let diagnostics = Arc::new(SessionWriterDiagnostics::new(config.total_capacity()));
    let (state, _) = watch::channel(ConnectionState::Healthy);
    run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    )
    .await;

    let failure = completion
        .await
        .expect("cancelled file completion")
        .expect_err("cancelled file must fail");
    assert_eq!(failure.progress(), WriteProgress::NotStarted);
    assert_eq!(attempts.load(Ordering::Acquire), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_a_queued_file_envelope_releases_its_lease_once() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let mut config = queue_config();
    config.data_max_bytes = NonZeroUsize::new(1024).expect("non-zero");
    let (lanes, mut receivers) = writer_lanes(config);
    let (write, completion, drops) = queued_file_write_with_completion(&admission, "dropped-file");
    drop(completion);
    lanes.try_send(AdmissionClass::Data, write).expect("file queued");
    drop(lanes);

    let WriterEvent::Write(envelope) = receivers.recv().await else {
        panic!("queued file envelope")
    };
    drop(envelope);

    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    assert_eq!(controller.snapshot().queued.current_bytes, 0);
}

#[tokio::test]
async fn queued_cancellation_wins_before_writer_start_and_never_touches_the_socket() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let attempts = Arc::new(AtomicUsize::new(0));
    let flushes = Arc::new(AtomicUsize::new(0));
    let connection = Connection::new_with_plaintext_stream(FailingTransport {
        attempts: Arc::clone(&attempts),
        flushes,
        phase: WriteFailurePhase::Flush,
    });
    let (frame_writer, _reader) = connection.into_session_io(admission.clone());
    let config = queue_config();
    let (lanes, receivers) = writer_lanes(config);
    let progress = Arc::new(QueuedWriteProgress::waiting());
    let (write, completion) = queued_write_with_completion(&admission, "deadline-target", 16, Arc::clone(&progress));
    lanes.try_send(AdmissionClass::Data, write).expect("queued write");
    assert!(progress.cancel_before_start());
    drop(lanes);

    let diagnostics = Arc::new(SessionWriterDiagnostics::new(config.total_capacity()));
    let (state, _) = watch::channel(ConnectionState::Healthy);
    run_session_writer(
        frame_writer,
        receivers,
        Arc::clone(&diagnostics),
        state,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    )
    .await;

    let failure = completion
        .await
        .expect("deadline completion")
        .expect_err("cancelled queued write must fail");
    assert_eq!(failure.progress(), WriteProgress::NotStarted);
    assert_eq!(attempts.load(Ordering::Acquire), 0);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    assert_eq!(diagnostics.snapshot().failed, 1);
    assert_eq!(diagnostics.snapshot().deadline_expired, 1);
}

#[tokio::test]
async fn dropped_completion_before_writer_start_is_not_started_and_releases_its_permit() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let config = queue_config();
    let (lanes, mut receivers) = writer_lanes(config);
    let progress = Arc::new(QueuedWriteProgress::waiting());
    let (write, completion) =
        queued_write_with_completion(&admission, "dropped-before-start", 16, Arc::clone(&progress));
    drop(completion);
    lanes.try_send(AdmissionClass::Data, write).expect("queued write");
    drop(lanes);

    let WriterEvent::Write(envelope) = receivers.recv().await else {
        panic!("queued envelope")
    };
    drop(envelope);

    assert!(!progress.write_started());
    assert_eq!(
        WriterFailure::completion_dropped(WriteProgress::NotStarted).progress(),
        WriteProgress::NotStarted
    );
    assert_eq!(controller.snapshot().queued.current_count, 0);
    assert_eq!(controller.snapshot().queued.current_bytes, 0);
}

#[tokio::test]
async fn dropped_active_completion_is_possibly_partial_and_releases_its_permit() {
    let controller = AdmissionController::new(AdmissionLimits::default());
    let admission = controller
        .prepare_scope(AdmissionScope::new("127.0.0.1".parse().expect("loopback")))
        .expect("prepare scope");
    let attempts = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let connection = Connection::new_with_plaintext_stream(PendingTransport {
        attempts: Arc::clone(&attempts),
        entered: Arc::clone(&entered),
    });
    let (frame_writer, _reader) = connection.into_session_io(admission.clone());
    let config = queue_config();
    let (lanes, receivers) = writer_lanes(config);
    let progress = Arc::new(QueuedWriteProgress::waiting());
    let (write, completion) =
        queued_write_with_completion(&admission, "dropped-after-start", 16, Arc::clone(&progress));
    lanes.try_send(AdmissionClass::Data, write).expect("queued write");
    drop(lanes);
    let diagnostics = Arc::new(SessionWriterDiagnostics::new(config.total_capacity()));
    let (state, _) = watch::channel(ConnectionState::Healthy);
    let writer = tokio::spawn(run_session_writer(
        frame_writer,
        receivers,
        diagnostics,
        state,
        CancellationToken::new(),
        TransportTelemetry::noop(),
    ));

    entered.notified().await;
    writer.abort();
    assert!(writer.await.expect_err("aborted writer task").is_cancelled());
    assert!(completion.await.is_err());
    assert!(progress.write_started());
    assert_eq!(
        WriterFailure::completion_dropped(WriteProgress::PossiblyPartial).progress(),
        WriteProgress::PossiblyPartial
    );
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    assert_eq!(controller.snapshot().queued.current_count, 0);
    assert_eq!(controller.snapshot().queued.current_bytes, 0);
}
