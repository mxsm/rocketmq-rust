// Copyright 2023 The RocketMQ Rust Authors
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

#![cfg(feature = "test-support")]

use std::future::Future;
use std::io;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::TransportSecurity;
use rocketmq_transport::test_support::run_connected_session;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::ConnectionHandler;
use rocketmq_transport::test_support::FrameWriteMode;
use rocketmq_transport::test_support::FrameWriter;
use rocketmq_transport::test_support::SessionHandle;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::mpsc;
use tokio::sync::Notify;

#[derive(Clone, Copy)]
enum StopBehavior {
    Never,
    ErrorAfter(usize),
    WriteZeroAfter(usize),
}

#[derive(Default)]
struct WriteRecord {
    bytes: Vec<u8>,
    scalar_calls: usize,
    vectored_calls: usize,
    largest_vector: usize,
    flushes: usize,
    shutdowns: usize,
}

struct ScriptedWriter {
    record: Arc<Mutex<WriteRecord>>,
    max_chunk: usize,
    stop: StopBehavior,
}

impl ScriptedWriter {
    fn new(max_chunk: usize, stop: StopBehavior) -> (Self, Arc<Mutex<WriteRecord>>) {
        let record = Arc::new(Mutex::new(WriteRecord::default()));
        (
            Self {
                record: record.clone(),
                max_chunk,
                stop,
            },
            record,
        )
    }

    fn allowed_bytes(&self, already_written: usize, offered: usize) -> io::Result<usize> {
        let available = match self.stop {
            StopBehavior::Never => offered,
            StopBehavior::ErrorAfter(limit) if already_written >= limit => {
                return Err(io::Error::other("injected partial-write failure"));
            }
            StopBehavior::WriteZeroAfter(limit) if already_written >= limit => return Ok(0),
            StopBehavior::ErrorAfter(limit) | StopBehavior::WriteZeroAfter(limit) => {
                offered.min(limit - already_written)
            }
        };
        Ok(available.min(self.max_chunk))
    }
}

impl AsyncWrite for ScriptedWriter {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        let mut record = self.record.lock().expect("write record lock");
        record.scalar_calls += 1;
        let allowed = match self.allowed_bytes(record.bytes.len(), buffer.len()) {
            Ok(allowed) => allowed,
            Err(error) => return Poll::Ready(Err(error)),
        };
        record.bytes.extend_from_slice(&buffer[..allowed]);
        Poll::Ready(Ok(allowed))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let offered = buffers.iter().map(|buffer| buffer.len()).sum();
        let mut record = self.record.lock().expect("write record lock");
        record.vectored_calls += 1;
        record.largest_vector = record.largest_vector.max(buffers.len());
        let allowed = match self.allowed_bytes(record.bytes.len(), offered) {
            Ok(allowed) => allowed,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let mut remaining = allowed;
        for buffer in buffers {
            let copied = remaining.min(buffer.len());
            record.bytes.extend_from_slice(&buffer[..copied]);
            remaining -= copied;
            if remaining == 0 {
                break;
            }
        }
        Poll::Ready(Ok(allowed))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.record.lock().expect("write record lock").flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.record.lock().expect("write record lock").shutdowns += 1;
        Poll::Ready(Ok(()))
    }
}

fn encoded_frame(code: i32, body: &'static [u8]) -> EncodedFrame {
    EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(code)
            .set_opaque(code)
            .set_body(Bytes::from_static(body)),
    )
    .expect("test command should encode")
}

#[tokio::test]
async fn plaintext_advances_partial_vectored_writes_across_all_frame_segments() {
    let frame = encoded_frame(10_001, b"segmented-plaintext-body");
    let expected = frame.clone().into_bytes();
    let (io, record) = ScriptedWriter::new(5, StopBehavior::Never);
    let mut writer = FrameWriter::plaintext(io);

    writer
        .write_frame(&frame)
        .await
        .expect("vectored write should complete");

    let record = record.lock().expect("write record lock");
    assert_eq!(record.bytes, expected.as_ref());
    assert!(record.vectored_calls > 1);
    assert!(
        record.largest_vector >= 2,
        "prefix/header/body should reach one vectored call"
    );
    assert_eq!(record.scalar_calls, 0);
    assert_eq!(record.flushes, 1);
}

#[tokio::test]
async fn tls_coalescing_enforces_its_plaintext_bound_and_uses_scalar_writes() {
    let first = encoded_frame(10_002, b"first-tls-body");
    let second = encoded_frame(10_003, b"second-tls-body");
    let expected = [first.clone().into_bytes(), second.clone().into_bytes()].concat();
    let limit = first.encoded_len().max(second.encoded_len());
    let (io, record) = ScriptedWriter::new(7, StopBehavior::Never);
    let mut writer = FrameWriter::new(
        io,
        FrameWriteMode::TlsCoalesced {
            max_plaintext_frame_bytes: limit,
        },
    )
    .expect("non-zero TLS bound");

    writer.write_frame(&first).await.expect("first TLS frame");
    writer.write_frame(&second).await.expect("second TLS frame");

    {
        let record = record.lock().expect("write record lock");
        assert_eq!(record.bytes, expected);
        assert_eq!(record.vectored_calls, 0);
        assert!(record.scalar_calls > 1);
    }

    let (small_io, small_record) = ScriptedWriter::new(usize::MAX, StopBehavior::Never);
    let mut bounded = FrameWriter::new(
        small_io,
        FrameWriteMode::TlsCoalesced {
            max_plaintext_frame_bytes: first.encoded_len() - 1,
        },
    )
    .expect("non-zero TLS bound");
    let error = bounded
        .write_frame(&first)
        .await
        .expect_err("oversized TLS plaintext must be rejected before writing");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(!bounded.is_poisoned());
    assert!(small_record.lock().expect("write record lock").bytes.is_empty());
}

#[tokio::test]
async fn tls_auto_coalesces_small_frames_and_vectors_large_frames() {
    let small = encoded_frame(10_020, b"small-auto-frame");
    let large = EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(10_021)
            .set_opaque(10_021)
            .set_body(vec![0x5a; 64 * 1024]),
    )
    .expect("large auto frame");
    let mode = FrameWriteMode::TlsAuto {
        max_plaintext_frame_bytes: large.encoded_len(),
        coalesce_below_bytes: 16 * 1024,
    };

    let (small_io, small_record) = ScriptedWriter::new(usize::MAX, StopBehavior::Never);
    let mut small_writer = FrameWriter::new(small_io, mode).expect("small auto writer");
    small_writer.write_frame(&small).await.expect("small auto frame");
    {
        let small_record = small_record.lock().expect("small auto record");
        assert_eq!(small_record.scalar_calls, 1);
        assert_eq!(small_record.vectored_calls, 0);
    }

    let (large_io, large_record) = ScriptedWriter::new(usize::MAX, StopBehavior::Never);
    let mut large_writer = FrameWriter::new(large_io, mode).expect("large auto writer");
    large_writer.write_frame(&large).await.expect("large auto frame");
    let large_record = large_record.lock().expect("large auto record");
    assert_eq!(large_record.scalar_calls, 0);
    assert_eq!(large_record.vectored_calls, 1);
    assert!(large_record.largest_vector >= 2);
}

#[tokio::test]
async fn partial_io_error_poisons_writer_and_blocks_later_frames_without_socket_access() {
    let frame = encoded_frame(10_004, b"partial-error-body");
    let (io, record) = ScriptedWriter::new(4, StopBehavior::ErrorAfter(10));
    let mut writer = FrameWriter::plaintext(io);

    let first_error = writer
        .write_frame(&frame)
        .await
        .expect_err("injected error should fail the partial frame");
    assert_eq!(first_error.kind(), io::ErrorKind::Other);
    assert!(writer.is_poisoned());
    let bytes_after_failure = record.lock().expect("write record lock").bytes.len();
    assert_eq!(bytes_after_failure, 10);

    let second_error = writer
        .write_frame(&frame)
        .await
        .expect_err("poisoned writer must reject a later frame");
    assert_eq!(second_error.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(
        record.lock().expect("write record lock").bytes.len(),
        bytes_after_failure
    );
}

#[tokio::test]
async fn write_zero_after_partial_progress_poisons_writer() {
    let frame = encoded_frame(10_005, b"write-zero-body");
    let (io, record) = ScriptedWriter::new(3, StopBehavior::WriteZeroAfter(7));
    let mut writer = FrameWriter::plaintext(io);

    let error = writer
        .write_frame(&frame)
        .await
        .expect_err("WriteZero should fail the frame");

    assert_eq!(error.kind(), io::ErrorKind::WriteZero);
    assert!(writer.is_poisoned());
    assert_eq!(record.lock().expect("write record lock").bytes.len(), 7);
}

struct SuspendedWriter {
    started: Arc<Notify>,
    written: Arc<AtomicUsize>,
}

impl AsyncWrite for SuspendedWriter {
    fn poll_write(self: Pin<&mut Self>, _context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        if self.written.load(Ordering::Acquire) == 0 {
            let written = buffer.len().min(4);
            self.written.store(written, Ordering::Release);
            self.started.notify_one();
            Poll::Ready(Ok(written))
        } else {
            Poll::Pending
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let first = buffers
            .iter()
            .find(|buffer| !buffer.is_empty())
            .map_or(&[][..], |buffer| buffer.as_ref());
        self.poll_write(context, first)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn dropping_an_active_low_level_write_poisons_the_writer() {
    let frame = encoded_frame(10_009, b"cancelled-low-level-write");
    let started = Arc::new(Notify::new());
    let written = Arc::new(AtomicUsize::new(0));
    let mut writer = FrameWriter::plaintext(SuspendedWriter {
        started: started.clone(),
        written: written.clone(),
    });
    let mut active_write = Box::pin(writer.write_frame(&frame));

    tokio::select! {
        () = started.notified() => {}
        result = &mut active_write => panic!("write unexpectedly completed: {result:?}"),
    }
    drop(active_write);

    assert_eq!(written.load(Ordering::Acquire), 4);
    assert!(writer.is_poisoned());
    let error = writer
        .write_frame(&frame)
        .await
        .expect_err("cancelled low-level writer must reject later frames");
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

struct DeadlineResponseHandler {
    completion: mpsc::Sender<Result<(), String>>,
    response_bytes: usize,
}

impl ConnectionHandler for DeadlineResponseHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let completion = self.completion.clone();
        let response_bytes = self.response_bytes;
        Box::pin(async move {
            let mut connection = session.connection();
            let result = connection
                .send_command_with_deadline(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque())
                        .set_body(vec![0x5a; response_bytes]),
                    RequestDeadline::after(Duration::from_millis(20)),
                    "cancellation-test-peer",
                )
                .await
                .map_err(|error| error.to_string());
            let _ = completion.send(result).await;
        })
    }
}

#[tokio::test]
async fn caller_deadline_during_active_write_poisons_incomplete_frame_and_closes_session() {
    let runtime = RuntimeContext::from_current("frame-writer-caller-cancellation");
    let service = runtime.service_context("frame-writer-caller-cancellation");
    let (transport, peer_stream) = tokio::io::duplex(64);
    let (completion_tx, mut completion_rx) = mpsc::channel(1);
    let local_addr: SocketAddr = "127.0.0.1:19201".parse().expect("local address");
    let remote_addr: SocketAddr = "127.0.0.1:19202".parse().expect("remote address");
    let runner = tokio::spawn(run_connected_session(
        Connection::new_with_plaintext_stream(transport),
        local_addr,
        remote_addr,
        service.task_group().clone(),
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        None,
        Duration::from_secs(30),
        Arc::new(DeadlineResponseHandler {
            completion: completion_tx,
            response_bytes: 64 * 1024,
        }),
    ));
    let mut peer = Connection::new_with_plaintext_stream(peer_stream);
    peer.send_command(RemotingCommand::create_remoting_command(10_006).set_opaque(76))
        .await
        .expect("request should reach session");

    let completion = tokio::time::timeout(Duration::from_secs(1), completion_rx.recv())
        .await
        .expect("caller deadline should finish")
        .expect("handler should report its send result");
    assert!(completion.is_err(), "caller should stop waiting at its deadline");

    let response = tokio::time::timeout(Duration::from_secs(2), peer.receive_command())
        .await
        .expect("poisoned writer should close after the partial frame");
    assert!(
        matches!(response, None | Some(Err(_))),
        "a frame interrupted by its deadline must never decode as a valid response"
    );

    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .expect("poisoned session should close without waiting for its peer")
        .expect("session task should finish");
    drop(peer);
    drop(service);
    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

struct FailureControl {
    release_error: AtomicBool,
    partial_written: Notify,
    parked_writer: Mutex<Option<Waker>>,
    write_polls: AtomicUsize,
    failure_poll: AtomicUsize,
}

impl FailureControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            release_error: AtomicBool::new(false),
            partial_written: Notify::new(),
            parked_writer: Mutex::new(None),
            write_polls: AtomicUsize::new(0),
            failure_poll: AtomicUsize::new(0),
        })
    }

    fn release(&self) {
        self.release_error.store(true, Ordering::Release);
        if let Some(waker) = self.parked_writer.lock().expect("writer waker lock").take() {
            waker.wake();
        }
    }
}

struct PartialFailureTransport {
    inner: tokio::io::DuplexStream,
    control: Arc<FailureControl>,
    wrote_partial: bool,
}

impl AsyncRead for PartialFailureTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for PartialFailureTransport {
    fn poll_write(mut self: Pin<&mut Self>, context: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        let write_poll = self.control.write_polls.fetch_add(1, Ordering::AcqRel) + 1;
        if !self.wrote_partial {
            let limit = buffer.len().min(5);
            match Pin::new(&mut self.inner).poll_write(context, &buffer[..limit]) {
                Poll::Ready(Ok(written)) if written > 0 => {
                    self.wrote_partial = true;
                    self.control.partial_written.notify_one();
                    Poll::Ready(Ok(written))
                }
                result => result,
            }
        } else if self.control.release_error.load(Ordering::Acquire) {
            self.control.failure_poll.store(write_poll, Ordering::Release);
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "injected connection failure after partial frame",
            )))
        } else {
            *self.control.parked_writer.lock().expect("writer waker lock") = Some(context.waker().clone());
            Poll::Pending
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let first = buffers
            .iter()
            .find(|buffer| !buffer.is_empty())
            .map_or(&[][..], |buffer| buffer.as_ref());
        self.poll_write(context, first)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

struct ConcurrentResponseHandler {
    queued: mpsc::Sender<i32>,
    results: mpsc::Sender<(i32, Result<(), String>)>,
}

impl ConnectionHandler for ConcurrentResponseHandler {
    fn connected(&self, _session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn command(
        &self,
        session: SessionHandle,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let queued = self.queued.clone();
        let results = self.results.clone();
        Box::pin(async move {
            let opaque = request.opaque();
            let _ = queued.send(opaque).await;
            let mut connection = session.connection();
            let result = connection
                .send_command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(opaque)
                        .set_body(vec![opaque as u8; 128]),
                )
                .await
                .map_err(|error| error.to_string());
            let _ = results.send((opaque, result)).await;
        })
    }
}

#[tokio::test]
async fn partial_failure_closes_session_and_fails_every_already_queued_frame() {
    let runtime = RuntimeContext::from_current("frame-writer-poisoned-queue");
    let service = runtime.service_context("frame-writer-poisoned-queue");
    let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let (inner, peer_stream) = tokio::io::duplex(4096);
    let control = FailureControl::new();
    let transport = PartialFailureTransport {
        inner,
        control: control.clone(),
        wrote_partial: false,
    };
    let (queued_tx, mut queued_rx) = mpsc::channel(2);
    let (result_tx, mut result_rx) = mpsc::channel(2);
    let local_addr: SocketAddr = "127.0.0.1:19203".parse().expect("local address");
    let remote_addr: SocketAddr = "127.0.0.1:19204".parse().expect("remote address");
    let runner = tokio::spawn(run_connected_session(
        Connection::new_with_plaintext_stream(transport),
        local_addr,
        remote_addr,
        service.task_group().clone(),
        admission.clone(),
        Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
        None,
        Duration::from_secs(30),
        Arc::new(ConcurrentResponseHandler {
            queued: queued_tx,
            results: result_tx,
        }),
    ));
    let mut peer = Connection::new_with_plaintext_stream(peer_stream);
    peer.send_command(RemotingCommand::create_remoting_command(10_007).set_opaque(77))
        .await
        .expect("first request");
    peer.send_command(RemotingCommand::create_remoting_command(10_008).set_opaque(78))
        .await
        .expect("second request");

    tokio::time::timeout(Duration::from_secs(1), queued_rx.recv())
        .await
        .expect("first handler should enqueue")
        .expect("first queued signal");
    tokio::time::timeout(Duration::from_secs(1), queued_rx.recv())
        .await
        .expect("second handler should enqueue")
        .expect("second queued signal");
    tokio::time::timeout(Duration::from_secs(1), control.partial_written.notified())
        .await
        .expect("writer should make partial progress");
    tokio::time::timeout(Duration::from_secs(1), async {
        while admission.snapshot().queued.current_count < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both frames should own queued-byte permits");

    control.release();

    let first = tokio::time::timeout(Duration::from_secs(1), result_rx.recv())
        .await
        .expect("first send should finish")
        .expect("first send result");
    let second = tokio::time::timeout(Duration::from_secs(1), result_rx.recv())
        .await
        .expect("second send should finish")
        .expect("second send result");
    let errors = [
        first.1.expect_err("first queued frame should fail"),
        second.1.expect_err("second queued frame should fail"),
    ];
    assert!(errors.iter().all(|error| error.contains("canonical writer failure")));
    assert!(control.failure_poll.load(Ordering::Acquire) >= 2);
    assert_eq!(
        control.write_polls.load(Ordering::Acquire),
        control.failure_poll.load(Ordering::Acquire),
        "queued frames must not touch the socket after the poisoning failure"
    );

    drop(peer);
    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .expect("poisoned session should close")
        .expect("session task should finish");
    drop(service);
    let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}
