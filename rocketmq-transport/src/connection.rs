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

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures_util::StreamExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScopeHandle;
use crate::backend::ReadBackend;
use crate::backend::WriteBackend;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::codec::remoting_command_codec::RemotingCommandCodec;
use crate::codec::remoting_command_codec::SessionCommandDecoder;
use crate::codec::PreparedResponse;
use crate::deadline::RequestDeadline;
use crate::dispatch::DeferredTransportDropHandle;
use crate::dispatch::RequestControlView;
use crate::dispatch::ResponseCompletionOutcome;
use crate::dispatch::ResponseOperationalFailure;
use crate::dispatch::ResponseSendOutcome;
use crate::dispatch::ResponseTransportDropHandle;
use crate::dispatch::WriteProgress;
use crate::error_helpers::admission_queue_saturated;
use crate::error_helpers::connection_failed;
use crate::error_helpers::connection_failed_without_source;
use crate::error_helpers::TransportStage;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionSequence;
use crate::file_region::FileTransferMode;
use crate::telemetry::TransportTelemetry;
use crate::write_result::WriterFailure;
use crate::write_result::WriterRejection;
use crate::write_strategy::FrameWriteMode;
use crate::write_strategy::FrameWriter;
use crate::write_strategy::OutboundPayload;
use crate::write_strategy::QueuedWrite;
use crate::write_strategy::QueuedWriteCancellation;
use crate::write_strategy::QueuedWriteProgress;
use crate::write_strategy::ResponseQueueWaitObservation;
use crate::writer_runtime::WriterLanes;
use rocketmq_error::SharedError;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ResourcePermit;
use std::sync::Arc;

pub(crate) struct SessionLifecycle {
    accepting: AtomicBool,
    active_sends: AtomicUsize,
    sends_drained: tokio::sync::Notify,
}

pub(crate) struct SessionSendLease<'a> {
    lifecycle: &'a SessionLifecycle,
}

impl SessionLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            active_sends: AtomicUsize::new(0),
            sends_drained: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn begin_send(&self) -> Option<SessionSendLease<'_>> {
        if !self.accepting.load(Ordering::Acquire) {
            return None;
        }
        self.active_sends.fetch_add(1, Ordering::AcqRel);
        if self.accepting.load(Ordering::Acquire) {
            Some(SessionSendLease { lifecycle: self })
        } else {
            self.finish_send();
            None
        }
    }

    pub(crate) async fn begin_retirement(&self) {
        self.accepting.store(false, Ordering::Release);
        loop {
            let drained = self.sends_drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.active_sends.load(Ordering::Acquire) == 0 {
                break;
            }
            drained.await;
        }
    }

    fn finish_send(&self) {
        if self.active_sends.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.sends_drained.notify_waiters();
        }
    }
}

impl Drop for SessionSendLease<'_> {
    fn drop(&mut self) {
        self.lifecycle.finish_send();
    }
}

struct QueuedConnection {
    writer: WriterLanes,
    writer_diagnostics: Arc<SessionWriterDiagnostics>,
    admission: AdmissionScopeHandle,
    response_class: Option<AdmissionClass>,
    lifecycle: Arc<SessionLifecycle>,
}

pub type ConnectionId = CheetahString;

/// Async transport accepted by the RocketMQ framed connection.
pub trait ConnectionTransport: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> ConnectionTransport for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub type BoxedConnectionTransport = Box<dyn ConnectionTransport>;
type ConnectionReadHalf = FramedRead<ReadBackend, RemotingCommandCodec>;
pub(crate) type SessionConnectionReadHalf = FramedRead<ReadBackend, SessionCommandDecoder>;
pub(crate) type ConnectionFrameWriter = FrameWriter<WriteBackend>;

enum ConnectionWriter {
    Direct(ConnectionFrameWriter),
    Queued(QueuedConnection),
}

/// Private send mechanics shared by `RocketMQResult` facades and the
/// server's typed response completion path.
enum SendFailure {
    DeadlineExceeded {
        timeout_ms: u64,
    },
    SessionClosed,
    Cancelled,
    QueueSaturated {
        target: String,
    },
    Writer {
        progress: WriteProgress,
        error: SharedError,
    },
}

/// Crate-private completion used where transport owns the delivery decision.
///
/// This keeps normal admission and lifecycle outcomes distinct from a
/// canonical operational failure without exporting writer progress through a
/// public compatibility surface.
pub(crate) enum CommandSendOutcome {
    Written,
    DeadlineExpired,
    SessionClosed,
    Cancelled,
    QueueSaturated,
    EncodingFailed(rocketmq_error::RocketMQError),
    OperationalFailure {
        progress: WriteProgress,
        error: SharedError,
    },
}

impl SendFailure {
    fn into_error(self) -> rocketmq_error::RocketMQError {
        match self {
            Self::DeadlineExceeded { timeout_ms } => rocketmq_error::RocketMQError::Timeout {
                operation: "transport_before_send",
                timeout_ms,
            },
            Self::SessionClosed | Self::Cancelled => {
                rocketmq_error::RocketMQError::Shared(connection_failed_without_source(TransportStage::Closed))
            }
            Self::QueueSaturated { target } => rocketmq_error::RocketMQError::Shared(admission_queue_saturated(target)),
            Self::Writer { error, .. } => rocketmq_error::RocketMQError::Shared(error),
        }
    }

    fn into_response(self) -> ResponseSendOutcome {
        match self {
            Self::DeadlineExceeded { .. } => ResponseSendOutcome::Rejected(ResponseCompletionOutcome::DeadlineExpired),
            Self::SessionClosed => ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed),
            Self::Cancelled => ResponseSendOutcome::Rejected(ResponseCompletionOutcome::Cancelled),
            Self::QueueSaturated { .. } => ResponseSendOutcome::Rejected(ResponseCompletionOutcome::QueueSaturated),
            Self::Writer { progress, error } => {
                ResponseSendOutcome::OperationalFailure(ResponseOperationalFailure::transport(progress, error))
            }
        }
    }

    fn into_command_outcome(self) -> CommandSendOutcome {
        match self {
            Self::DeadlineExceeded { .. } => CommandSendOutcome::DeadlineExpired,
            Self::SessionClosed => CommandSendOutcome::SessionClosed,
            Self::Cancelled => CommandSendOutcome::Cancelled,
            Self::QueueSaturated { .. } => CommandSendOutcome::QueueSaturated,
            Self::Writer { progress, error } => CommandSendOutcome::OperationalFailure { progress, error },
        }
    }
}

fn classify_writer_failure(
    _target: String,
    failure: WriterFailure,
    owner_deadline: Option<RequestDeadline>,
) -> SendFailure {
    if owner_deadline.is_some_and(|deadline| failure.was_caused_by(deadline)) {
        return SendFailure::DeadlineExceeded {
            timeout_ms: owner_deadline.map_or(0, RequestDeadline::budget_millis),
        };
    }
    match failure.into_operational() {
        Ok((progress, error)) => SendFailure::Writer { progress, error },
        Err(WriterRejection::DeadlineExpired) => SendFailure::DeadlineExceeded {
            timeout_ms: owner_deadline.map_or(0, RequestDeadline::budget_millis),
        },
        Err(WriterRejection::Cancelled) => SendFailure::Cancelled,
        Err(WriterRejection::SessionClosed) => SendFailure::SessionClosed,
    }
}

#[derive(Clone, Copy)]
enum RequestStopPolicy {
    All,
}

fn current_request_stop(control: &RequestControlView, policy: RequestStopPolicy) -> Option<QueuedWriteCancellation> {
    if control.parent_is_cancelled() {
        Some(QueuedWriteCancellation::Request)
    } else if control.session_is_closed() {
        Some(QueuedWriteCancellation::SessionClosed)
    } else if matches!(policy, RequestStopPolicy::All) && control.deadline().is_some_and(RequestDeadline::is_expired) {
        Some(QueuedWriteCancellation::Deadline)
    } else {
        None
    }
}

async fn wait_for_control_stop(control: &RequestControlView, policy: RequestStopPolicy) {
    match policy {
        RequestStopPolicy::All => control.cancelled().await,
    }
}

fn stop_failure(reason: QueuedWriteCancellation, _target: String, deadline: Option<RequestDeadline>) -> SendFailure {
    match reason {
        QueuedWriteCancellation::Deadline => SendFailure::DeadlineExceeded {
            timeout_ms: deadline.map_or(0, RequestDeadline::budget_millis),
        },
        QueuedWriteCancellation::Request => SendFailure::Cancelled,
        QueuedWriteCancellation::SessionClosed => SendFailure::SessionClosed,
    }
}

struct InFlightQueuedSendDrop {
    handle: ResponseTransportDropHandle,
    deferred: Option<DeferredTransportDropHandle>,
    progress: Arc<QueuedWriteProgress>,
    armed: bool,
}

impl InFlightQueuedSendDrop {
    fn new(
        handle: ResponseTransportDropHandle,
        deferred: Option<DeferredTransportDropHandle>,
        progress: Arc<QueuedWriteProgress>,
    ) -> Self {
        handle.delegate();
        Self {
            handle,
            deferred,
            progress,
            armed: true,
        }
    }

    fn complete(mut self) {
        self.handle.resume_outer();
        self.armed = false;
    }
}

impl Drop for InFlightQueuedSendDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let progress = if self.progress.cancel_before_start_with(QueuedWriteCancellation::Request) {
            WriteProgress::NotStarted
        } else {
            WriteProgress::PossiblyPartial
        };
        self.handle.finish_dropped(progress);
        if let Some(deferred) = &self.deferred {
            deferred.finish_dropped(progress);
        }
    }
}

static TRANSPORT_ENCODED_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);

/// Monotonic process-wide transport I/O diagnostics.
///
/// The counter advances only after a complete encoded RocketMQ frame write
/// succeeds. It deliberately observes the transport framing boundary, before
/// optional TLS record encoding, so baseline and candidate measurements remain
/// comparable across plaintext and TLS variants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "the process-wide snapshot is exposed only by test_support and benchmark_support"
)]
pub struct TransportIoSnapshot {
    pub encoded_bytes_written: u64,
}

/// Low-cardinality diagnostics for one canonical session writer queue.
#[derive(Clone, Copy, Debug, Default, serde::Serialize, PartialEq, Eq)]
pub struct SessionWriterSnapshot {
    pub capacity: usize,
    pub queued_items: usize,
    pub queued_bytes: usize,
    pub control_capacity: usize,
    pub control_queued_items: usize,
    pub control_queued_bytes: usize,
    pub data_capacity: usize,
    pub data_queued_items: usize,
    pub data_queued_bytes: usize,
    pub oldest_queue_age_millis: Option<u64>,
    pub accepted: u64,
    pub rejected: u64,
    pub completed: u64,
    pub failed: u64,
    pub deadline_expired: u64,
    pub last_queue_age_millis: u64,
    pub max_queue_age_millis: u64,
    pub last_write_latency_millis: u64,
    pub max_write_latency_millis: u64,
}

#[derive(Debug)]
pub(crate) struct SessionWriterDiagnostics {
    capacity: usize,
    queued_items: AtomicUsize,
    queued_bytes: AtomicUsize,
    accepted: AtomicU64,
    rejected: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    deadline_expired: AtomicU64,
    last_queue_age_millis: AtomicU64,
    max_queue_age_millis: AtomicU64,
    last_write_latency_millis: AtomicU64,
    max_write_latency_millis: AtomicU64,
}

impl SessionWriterDiagnostics {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queued_items: AtomicUsize::new(0),
            queued_bytes: AtomicUsize::new(0),
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            deadline_expired: AtomicU64::new(0),
            last_queue_age_millis: AtomicU64::new(0),
            max_queue_age_millis: AtomicU64::new(0),
            last_write_latency_millis: AtomicU64::new(0),
            max_write_latency_millis: AtomicU64::new(0),
        }
    }

    pub(crate) fn prepare_enqueue(&self, bytes: usize) -> Instant {
        self.queued_items.fetch_add(1, Ordering::AcqRel);
        self.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
        Instant::now()
    }

    pub(crate) fn record_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rejected(&self, prepared_bytes: Option<usize>) {
        if let Some(bytes) = prepared_bytes {
            self.queued_items.fetch_sub(1, Ordering::AcqRel);
            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn start_write(&self, enqueued_at: Option<Instant>, bytes: usize) -> Instant {
        let now = Instant::now();
        if let Some(enqueued_at) = enqueued_at {
            self.queued_items.fetch_sub(1, Ordering::AcqRel);
            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
            let queue_age = duration_millis(now.saturating_duration_since(enqueued_at));
            self.last_queue_age_millis.store(queue_age, Ordering::Relaxed);
            self.max_queue_age_millis.fetch_max(queue_age, Ordering::Relaxed);
        }
        now
    }

    pub(crate) fn finish_write(&self, started_at: Instant, succeeded: bool, deadline_expired: bool) {
        self.finish_write_at(started_at, Instant::now(), succeeded, deadline_expired);
    }

    fn finish_write_at(&self, started_at: Instant, finished_at: Instant, succeeded: bool, deadline_expired: bool) {
        let write_latency = duration_millis(finished_at.saturating_duration_since(started_at));
        self.last_write_latency_millis.store(write_latency, Ordering::Relaxed);
        self.max_write_latency_millis
            .fetch_max(write_latency, Ordering::Relaxed);
        if succeeded {
            self.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        if deadline_expired {
            self.deadline_expired.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Completes an accepted envelope that never reached a socket attempt.
    ///
    /// This deliberately does not call `start_write`: poisoned-drain and
    /// deterministic preflight paths must not look like writer activity in the
    /// queue diagnostics.
    pub(crate) fn finish_not_started(&self, enqueued_at: Option<Instant>, bytes: usize, deadline_expired: bool) {
        if enqueued_at.is_some() {
            self.queued_items.fetch_sub(1, Ordering::AcqRel);
            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
        self.failed.fetch_add(1, Ordering::Relaxed);
        if deadline_expired {
            self.deadline_expired.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> SessionWriterSnapshot {
        SessionWriterSnapshot {
            capacity: self.capacity,
            queued_items: self.queued_items.load(Ordering::Acquire),
            queued_bytes: self.queued_bytes.load(Ordering::Acquire),
            control_capacity: 0,
            control_queued_items: 0,
            control_queued_bytes: 0,
            data_capacity: 0,
            data_queued_items: 0,
            data_queued_bytes: 0,
            oldest_queue_age_millis: None,
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            deadline_expired: self.deadline_expired.load(Ordering::Relaxed),
            last_queue_age_millis: self.last_queue_age_millis.load(Ordering::Relaxed),
            max_queue_age_millis: self.max_queue_age_millis.load(Ordering::Relaxed),
            last_write_latency_millis: self.last_write_latency_millis.load(Ordering::Relaxed),
            max_write_latency_millis: self.max_write_latency_millis.load(Ordering::Relaxed),
        }
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Returns a read-only snapshot of successful encoded transport writes.
#[must_use]
#[allow(
    dead_code,
    reason = "the process-wide snapshot is exposed only by test_support and benchmark_support"
)]
pub fn transport_io_snapshot() -> TransportIoSnapshot {
    TransportIoSnapshot {
        encoded_bytes_written: TRANSPORT_ENCODED_BYTES_WRITTEN.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_transport_write(bytes: usize) {
    TRANSPORT_ENCODED_BYTES_WRITTEN.fetch_add(bytes as u64, Ordering::Relaxed);
}

/// Connection health state
///
/// Represents the current health status of a connection.
/// This enum is used with `watch` channel to broadcast state changes
/// to all interested parties without explicit polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is healthy and ready for I/O operations
    Healthy,
    /// Connection has encountered an error and should not be used
    Degraded,
    /// Connection is explicitly closed
    Closed,
}

/// Cloneable connection lifecycle capability without access to socket I/O state.
///
/// The handle lets shared channel owners observe health, subscribe to lifecycle
/// changes, and signal closure without sharing the connection encoder, buffers,
/// or transport halves.
#[derive(Clone)]
pub struct ConnectionStateHandle {
    state_tx: watch::Sender<ConnectionState>,
    state_rx: watch::Receiver<ConnectionState>,
}

impl ConnectionStateHandle {
    /// Returns the most recently published connection state.
    #[inline]
    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Returns whether the connection is currently healthy.
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.state() == ConnectionState::Healthy
    }

    /// Subscribes to subsequent connection state changes.
    pub fn subscribe(&self) -> watch::Receiver<ConnectionState> {
        self.state_tx.subscribe()
    }

    /// Signals that the connection must no longer accept new work.
    pub fn close(&self) {
        let _ = self.state_tx.send(ConnectionState::Closed);
    }
}

/// Bidirectional TCP connection for RocketMQ protocol communication.
///
/// `Connection` handles low-level frame encoding/decoding and provides high-level
/// APIs for sending/receiving `RemotingCommand` messages. It manages I/O buffers
/// and broadcasts connection state changes via a watch channel.
///
/// ## Lifecycle & State Management
///
/// **Tokio Best Practice**: Connection health is determined by I/O operation results,
/// not by polling a boolean flag. State changes are broadcast via `watch` channel:
///
/// ```text
/// ┌──────────┐  I/O Success   ┌──────────┐
/// │ Healthy  │ ──────────────► │ Healthy  │
/// └──────────┘                 └──────────┘
///      │                            │
///      │ I/O Error                  │ I/O Error
///      ↓                            ↓
/// ┌──────────┐                 ┌──────────┐
/// │ Degraded │                 │ Degraded │
/// └──────────┘                 └──────────┘
///      │                            │
///      │ close()                    │
///      ↓                            ↓
/// ┌──────────┐                 ┌──────────┐
/// │  Closed  │                 │  Closed  │
/// └──────────┘                 └──────────┘
/// ```
///
/// 1. **Created**: New connection from `TcpStream` (Healthy)
/// 2. **Active**: Processing requests/responses (Healthy)
/// 3. **Degraded**: I/O error occurred, broadcast state change
/// 4. **Closed**: Stream ended or explicit shutdown
///
/// ## Threading
///
/// - Safe for concurrent sends (internal buffering)
/// - Receives must be sequential (single reader)
/// - State monitoring: Multiple tasks can watch state via `subscribe()`
///
/// ## Key Design Principles
///
/// - **No explicit `ok` flag**: Connection validity determined by I/O results
/// - **Broadcast state changes**: Using `watch` channel for reactive updates
/// - **Fail-fast**: I/O errors immediately update state and return error
/// - **Zero polling**: Subscribers notified automatically on state change
pub struct Connection {
    /// Inbound command stream. Session response capabilities have no read half.
    inbound: Option<ConnectionReadHalf>,

    /// Direct socket writer or a capability to the canonical session writer actor.
    outbound: ConnectionWriter,

    /// One endpoint-owned profile shared by inbound decoding and every outbound command path.
    limits: FrameLimits,

    // === State Management (Tokio Watch Channel) ===
    /// Broadcast channel for connection state changes
    ///
    /// **Design**: Uses `watch` channel to notify all subscribers of state changes.
    /// This is the Tokio-idiomatic way to share state without locks or polling.
    ///
    /// - **Sender**: Held by Connection to broadcast state changes
    /// - **Receivers**: Created via `subscribe()` for monitoring
    ///
    /// **Why not a boolean?**
    /// - Reactive: Subscribers notified immediately on change
    /// - Lock-free: No mutex/atomic overhead
    /// - Composable: Can use in `tokio::select!` for timeout/cancellation
    state_tx: watch::Sender<ConnectionState>,

    /// Cached current state receiver for quick local queries
    ///
    /// Used for fast `state()` queries without creating new receivers
    state_rx: watch::Receiver<ConnectionState>,

    // === Identification ===
    /// Unique identifier for this connection instance
    ///
    /// Generated via UUID, stable across the connection lifetime
    connection_id: ConnectionId,

    telemetry: TransportTelemetry,

    response_drop: Option<ResponseTransportDropHandle>,

    #[cfg(test)]
    enqueue_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,

    #[cfg(test)]
    enqueue_complete_signal: Option<Arc<tokio::sync::Notify>>,
}

impl Hash for Connection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.connection_id.hash(state);
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.connection_id == other.connection_id
    }
}

impl Eq for Connection {}

impl Connection {
    /// Creates a new `Connection` instance with initial Healthy state.
    ///
    /// # Arguments
    ///
    /// * `tcp_stream` - The `TcpStream` associated with the connection
    ///
    /// # Returns
    ///
    /// A new `Connection` instance with a watch channel for state monitoring
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stream = TcpStream::connect("127.0.0.1:9876").await?;
    /// let connection = Connection::new(stream);
    ///
    /// // Subscribe to state changes
    /// let mut state_watcher = connection.subscribe();
    /// tokio::spawn(async move {
    ///     while state_watcher.changed().await.is_ok() {
    ///         let state = *state_watcher.borrow();
    ///         println!("Connection state: {:?}", state);
    ///     }
    /// });
    /// ```
    pub fn new(tcp_stream: TcpStream) -> Connection {
        Self::new_with_limits(tcp_stream, FrameLimits::default())
    }

    /// Creates a TCP connection bound to one explicit transport telemetry instance.
    pub fn new_with_telemetry(tcp_stream: TcpStream, telemetry: TransportTelemetry) -> Connection {
        Self::new(tcp_stream).with_telemetry(telemetry)
    }

    pub fn new_with_limits(tcp_stream: TcpStream, limits: FrameLimits) -> Connection {
        let (read_half, write_half) = tcp_stream.into_split();
        Self::new_with_backends(
            ReadBackend::Tcp(read_half),
            WriteBackend::Tcp(write_half),
            limits,
            FrameWriteMode::PlainVectored,
        )
    }

    /// Creates a plaintext connection over any compatible async stream.
    pub fn new_with_plaintext_stream<S>(stream: S) -> Connection
    where
        S: ConnectionTransport + 'static,
    {
        Self::new_with_plaintext_stream_and_limits(stream, FrameLimits::default())
    }

    /// Creates a bounded plaintext connection over any compatible async stream.
    pub fn new_with_plaintext_stream_and_limits<S>(stream: S, limits: FrameLimits) -> Connection
    where
        S: ConnectionTransport + 'static,
    {
        Self::new_with_stream_limits_and_write_mode(stream, limits, FrameWriteMode::PlainVectored)
    }

    /// Creates a connection over a negotiated TLS stream with bounded plaintext aggregation.
    pub fn new_with_tls_stream<S>(stream: S) -> Connection
    where
        S: ConnectionTransport + 'static,
    {
        Self::new_with_tls_stream_and_limits(stream, FrameLimits::default())
    }

    /// Creates a TLS connection using the configured maximum wire frame as its aggregation bound.
    pub fn new_with_tls_stream_and_limits<S>(stream: S, limits: FrameLimits) -> Connection
    where
        S: ConnectionTransport + 'static,
    {
        let max_plaintext_frame_bytes = limits.max_frame_bytes;
        Self::new_with_stream_limits_and_write_mode(
            stream,
            limits,
            FrameWriteMode::TlsAuto {
                max_plaintext_frame_bytes,
                coalesce_below_bytes: 16 * 1024,
            },
        )
    }

    fn new_with_stream_limits_and_write_mode<S>(
        stream: S,
        limits: FrameLimits,
        write_mode: FrameWriteMode,
    ) -> Connection
    where
        S: ConnectionTransport + 'static,
    {
        let transport = Box::new(stream) as BoxedConnectionTransport;
        let (read_half, write_half) = tokio::io::split(transport);
        Self::new_with_backends(
            ReadBackend::Compat(read_half),
            WriteBackend::Compat(write_half),
            limits,
            write_mode,
        )
    }

    fn new_with_backends(
        read_half: ReadBackend,
        write_half: WriteBackend,
        limits: FrameLimits,
        write_mode: FrameWriteMode,
    ) -> Connection {
        let inbound = FramedRead::with_capacity(
            read_half,
            RemotingCommandCodec::with_limits(limits),
            limits.safe_initial_read_bytes(),
        );
        let outbound = FrameWriter::new(write_half, write_mode)
            .expect("constructing a frame writer does not perform fallible I/O");
        // Initialize watch channel with Healthy state
        let (state_tx, state_rx) = watch::channel(ConnectionState::Healthy);

        Self {
            inbound: Some(inbound),
            outbound: ConnectionWriter::Direct(outbound),
            limits,
            state_tx,
            state_rx,
            connection_id: CheetahString::from_string(Uuid::new_v4().to_string()),
            telemetry: TransportTelemetry::noop(),
            response_drop: None,
            #[cfg(test)]
            enqueue_gate: None,
            #[cfg(test)]
            enqueue_complete_signal: None,
        }
    }

    /// Replaces this connection's no-op recorder with an explicitly composed transport recorder.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Injects the runtime-owned storage I/O lane used by portable file-region writes.
    ///
    /// Queued session capabilities already share the writer configured when the physical
    /// connection was established, so calling this on a queued capability has no effect.
    #[must_use]
    pub fn with_file_region_io(mut self, blocking: BlockingExecutor, mode: FileTransferMode) -> Self {
        if let ConnectionWriter::Direct(writer) = &mut self.outbound {
            writer.configure_file_region_io(blocking, mode);
        }
        self
    }

    pub(crate) fn new_queued(
        writer: WriterLanes,
        writer_diagnostics: Arc<SessionWriterDiagnostics>,
        admission: AdmissionScopeHandle,
        state_tx: watch::Sender<ConnectionState>,
        state_rx: watch::Receiver<ConnectionState>,
        connection_id: ConnectionId,
        limits: FrameLimits,
        response_class: Option<AdmissionClass>,
        lifecycle: Arc<SessionLifecycle>,
        telemetry: TransportTelemetry,
    ) -> Self {
        Self {
            inbound: None,
            outbound: ConnectionWriter::Queued(QueuedConnection {
                writer,
                writer_diagnostics,
                admission,
                response_class,
                lifecycle,
            }),
            limits,
            state_tx,
            state_rx,
            connection_id,
            telemetry,
            response_drop: None,
            #[cfg(test)]
            enqueue_gate: None,
            #[cfg(test)]
            enqueue_complete_signal: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_enqueue_gate(&mut self, checked: Arc<tokio::sync::Notify>, resume: Arc<tokio::sync::Notify>) {
        self.enqueue_gate = Some((checked, resume));
    }

    #[cfg(test)]
    pub(crate) fn set_enqueue_complete_signal(&mut self, signal: Arc<tokio::sync::Notify>) {
        self.enqueue_complete_signal = Some(signal);
    }

    pub(crate) fn with_response_drop(mut self, drop_handle: Option<ResponseTransportDropHandle>) -> Self {
        self.response_drop = drop_handle;
        self
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn set_write_preflight_barrier(&mut self, barrier: crate::write_strategy::WritePreflightBarrier) {
        if let ConnectionWriter::Direct(writer) = &mut self.outbound {
            writer.set_write_preflight_barrier(barrier);
        }
    }

    pub(crate) fn into_session_io(
        self,
        admission: AdmissionScopeHandle,
    ) -> (ConnectionFrameWriter, SessionConnectionReadHalf) {
        let writer = match self.outbound {
            ConnectionWriter::Direct(writer) => writer,
            ConnectionWriter::Queued(_) => unreachable!("session runtime requires an owned transport writer"),
        };
        let reader = self
            .inbound
            .unwrap_or_else(|| unreachable!("session runtime requires an owned transport reader"))
            .map_decoder(|decoder| SessionCommandDecoder::new(decoder, admission));
        (writer, reader)
    }

    /// Returns the immutable frame policy shared by this connection's reader and writer.
    #[inline]
    pub const fn frame_limits(&self) -> FrameLimits {
        self.limits
    }

    pub(crate) fn telemetry(&self) -> TransportTelemetry {
        self.telemetry.clone()
    }

    /// Receives the next `RemotingCommand` from the peer.
    ///
    /// Blocks until a complete frame is available or the stream ends.
    ///
    /// # Returns
    ///
    /// - `Some(Ok(command))`: Successfully received and decoded a command
    /// - `Some(Err(e))`: Decoding error occurred
    /// - `None`: Stream ended (peer closed connection)
    ///
    /// # Example
    ///
    /// ```ignore
    /// while let Some(result) = connection.receive_command().await {
    ///     match result {
    ///         Ok(cmd) => handle_command(cmd),
    ///         Err(e) => eprintln!("Decode error: {}", e),
    ///     }
    /// }
    /// // Connection closed
    /// ```
    pub async fn receive_command(&mut self) -> Option<rocketmq_error::RocketMQResult<RemotingCommand>> {
        match self.inbound.as_mut() {
            Some(inbound) => inbound.next().await,
            None => None,
        }
    }

    fn queued_writer(&self) -> Option<&QueuedConnection> {
        match &self.outbound {
            ConnectionWriter::Queued(queued) => Some(queued),
            ConnectionWriter::Direct(_) => None,
        }
    }

    fn response_class(&self) -> Option<AdmissionClass> {
        self.queued_writer().and_then(|queued| queued.response_class)
    }

    async fn send_payload(
        &mut self,
        payload: OutboundPayload,
        class: AdmissionClass,
        reservation: Option<ResourcePermit>,
        deadline: Option<RequestDeadline>,
        target: String,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.send_payload_inner(
            payload,
            class,
            reservation,
            deadline,
            None,
            RequestStopPolicy::All,
            None,
            target,
        )
        .await
        .map_err(SendFailure::into_error)
    }

    async fn send_payload_inner(
        &mut self,
        payload: OutboundPayload,
        class: AdmissionClass,
        reservation: Option<ResourcePermit>,
        deadline: Option<RequestDeadline>,
        control: Option<&RequestControlView>,
        stop_policy: RequestStopPolicy,
        deferred_drop: Option<DeferredTransportDropHandle>,
        target: String,
    ) -> Result<(), SendFailure> {
        self.send_payload_inner_with_response_observation(
            payload,
            class,
            reservation,
            deadline,
            control,
            stop_policy,
            deferred_drop,
            ResponseQueueWaitObservation::Ineligible,
            target,
        )
        .await
    }

    async fn send_payload_inner_with_response_observation(
        &mut self,
        payload: OutboundPayload,
        class: AdmissionClass,
        reservation: Option<ResourcePermit>,
        deadline: Option<RequestDeadline>,
        control: Option<&RequestControlView>,
        stop_policy: RequestStopPolicy,
        deferred_drop: Option<DeferredTransportDropHandle>,
        response_queue_wait_observation: ResponseQueueWaitObservation,
        target: String,
    ) -> Result<(), SendFailure> {
        let response_drop = self.response_drop.clone();
        let encoded_len = payload.encoded_len();
        self.telemetry.record_outbound_attempted_plaintext_bytes(encoded_len);
        if let Some(reason) = control.and_then(|control| current_request_stop(control, stop_policy)) {
            return Err(stop_failure(reason, target, deadline));
        }
        if deadline.is_some_and(RequestDeadline::is_expired) {
            return Err(SendFailure::DeadlineExceeded {
                timeout_ms: deadline.map_or(0, RequestDeadline::budget_millis),
            });
        }
        if let Some(queued) = self.queued_writer() {
            let _send_lease = match queued.lifecycle.begin_send() {
                Some(lease) => lease,
                None => {
                    queued.writer_diagnostics.record_rejected(None);
                    return Err(SendFailure::SessionClosed);
                }
            };
            if self.state() == ConnectionState::Closed {
                queued.writer_diagnostics.record_rejected(None);
                return Err(SendFailure::SessionClosed);
            }
            #[cfg(test)]
            if let Some((checked, resume)) = self.enqueue_gate.as_ref() {
                checked.notify_one();
                if let Some(control) = control {
                    tokio::select! {
                        biased;
                        () = wait_for_control_stop(control, stop_policy) => {
                            let reason = current_request_stop(control, stop_policy)
                                .unwrap_or(QueuedWriteCancellation::Request);
                            return Err(stop_failure(reason, target, deadline));
                        }
                        () = resume.notified() => {}
                    }
                } else if let Some(deadline) = deadline {
                    deadline
                        .timeout(resume.notified())
                        .await
                        .map_err(|_| SendFailure::DeadlineExceeded {
                            timeout_ms: deadline.budget_millis(),
                        })?;
                } else {
                    resume.notified().await;
                }
            }
            let permit = match reservation {
                Some(reservation) => queued.admission.rebind_permit(AdmissionResource::Queued, reservation),
                None => queued
                    .admission
                    .try_acquire(AdmissionResource::Queued, encoded_len, class),
            }
            .map_err(|_| {
                queued.writer_diagnostics.record_rejected(None);
                SendFailure::QueueSaturated { target: target.clone() }
            })?;
            if let Some(reason) = control.and_then(|control| current_request_stop(control, stop_policy)) {
                return Err(stop_failure(reason, target, deadline));
            }
            if deadline.is_some_and(RequestDeadline::is_expired) {
                return Err(SendFailure::DeadlineExceeded {
                    timeout_ms: deadline.map_or(0, RequestDeadline::budget_millis),
                });
            }
            let (completion, result) = oneshot::channel();
            let progress = Arc::new(QueuedWriteProgress::waiting());
            let enqueued_at = queued.writer_diagnostics.prepare_enqueue(encoded_len);
            match queued.writer.try_send(
                class,
                QueuedWrite::data(
                    payload,
                    completion,
                    permit,
                    deadline,
                    target.clone(),
                    progress.clone(),
                    enqueued_at,
                )
                .with_response_queue_wait_observation(response_queue_wait_observation),
            ) {
                crate::writer_runtime::WriterEnqueueOutcome::Enqueued => {
                    queued.writer_diagnostics.record_accepted();
                    self.telemetry.record_outbound_accepted_plaintext_bytes(encoded_len);
                }
                rejection => {
                    queued.writer_diagnostics.record_rejected(Some(encoded_len));
                    return Err(match rejection {
                        crate::writer_runtime::WriterEnqueueOutcome::Full(write) => {
                            drop(write);
                            SendFailure::QueueSaturated { target: target.clone() }
                        }
                        crate::writer_runtime::WriterEnqueueOutcome::Closed(write) => {
                            drop(write);
                            SendFailure::SessionClosed
                        }
                        crate::writer_runtime::WriterEnqueueOutcome::Enqueued => unreachable!(),
                    });
                }
            }
            let mut in_flight_drop =
                response_drop.map(|handle| InFlightQueuedSendDrop::new(handle, deferred_drop, Arc::clone(&progress)));
            #[cfg(test)]
            if let Some(signal) = &self.enqueue_complete_signal {
                signal.notify_one();
            }
            let mut result = result;
            let outcome = if let Some(control) = control {
                tokio::select! {
                    biased;
                    outcome = &mut result => outcome,
                    () = wait_for_control_stop(control, stop_policy) => {
                        let reason = current_request_stop(control, stop_policy)
                            .unwrap_or(QueuedWriteCancellation::Request);
                        if progress.cancel_before_start_with(reason) {
                            if let Some(drop_guard) = in_flight_drop.take() {
                                drop_guard.complete();
                            }
                            return Err(stop_failure(reason, target, deadline));
                        }
                        result.await
                    }
                }
            } else if let Some(deadline) = deadline {
                match deadline.timeout(&mut result).await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        if progress.cancel_before_start() {
                            if let Some(drop_guard) = in_flight_drop.take() {
                                drop_guard.complete();
                            }
                            return Err(SendFailure::DeadlineExceeded {
                                timeout_ms: deadline.budget_millis(),
                            });
                        }
                        result.await
                    }
                }
            } else {
                result.await
            };
            let outcome = outcome.map_err(|_| {
                if let Some(reason) = progress.cancellation_reason() {
                    return stop_failure(reason, target.clone(), deadline);
                }
                let progress = if progress.write_started() {
                    WriteProgress::PossiblyPartial
                } else {
                    WriteProgress::NotStarted
                };
                classify_writer_failure(target.clone(), WriterFailure::completion_dropped(progress), deadline)
            });
            if let Some(drop_guard) = in_flight_drop.take() {
                drop_guard.complete();
            }
            let outcome = outcome?;
            return outcome.map_err(|failure| classify_writer_failure(target, failure, deadline));
        }
        if self.state() == ConnectionState::Closed {
            return Err(SendFailure::SessionClosed);
        }
        self.telemetry.record_outbound_accepted_plaintext_bytes(encoded_len);
        let writer = match &mut self.outbound {
            ConnectionWriter::Direct(writer) => writer,
            ConnectionWriter::Queued(_) => unreachable!("queued writer returned above"),
        };
        let write_started = Arc::new(AtomicBool::new(false));
        let write_started_marker = Arc::clone(&write_started);
        let mut mark_write_started = move || write_started_marker.store(true, Ordering::Release);
        let send_result = if let Some(deadline) = deadline {
            match deadline
                .timeout(writer.write_transport_payloads_with_start(&[&payload], 64, &mut mark_write_started))
                .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(WriterFailure::from_io(
                    if write_started.load(Ordering::Acquire) {
                        WriteProgress::PossiblyPartial
                    } else {
                        WriteProgress::NotStarted
                    },
                    error,
                )),
                Err(source) => Err(if write_started.load(Ordering::Acquire) {
                    WriterFailure::write_timeout_caused_by(
                        WriteProgress::PossiblyPartial,
                        deadline.budget_millis(),
                        source,
                    )
                } else {
                    WriterFailure::prewrite_timeout(deadline.budget_millis(), deadline.instant())
                }),
            }
        } else {
            writer
                .write_transport_payloads_with_start(&[&payload], 64, &mut mark_write_started)
                .await
                .map_err(|error| {
                    WriterFailure::from_io(
                        if write_started.load(Ordering::Acquire) {
                            WriteProgress::PossiblyPartial
                        } else {
                            WriteProgress::NotStarted
                        },
                        error,
                    )
                })
        };
        match send_result {
            Ok(()) => {
                record_transport_write(encoded_len);
                self.telemetry.record_outbound_written_plaintext_bytes(encoded_len);
                Ok(())
            }
            Err(failure) => {
                if failure.progress() == Some(WriteProgress::PossiblyPartial) || writer.is_poisoned() {
                    let _ = self.state_tx.send(ConnectionState::Degraded);
                    let _ = writer.shutdown().await;
                    let _ = self.state_tx.send(ConnectionState::Closed);
                }
                Err(classify_writer_failure(target, failure, deadline))
            }
        }
    }

    /// Sends a server response through the canonical writer with a stage-aware
    /// completion error. This remains crate-private because remoting responses own
    /// the public delivery contract.
    pub(crate) async fn send_prepared_response(
        &mut self,
        prepared: PreparedResponse,
        control: &RequestControlView,
    ) -> ResponseSendOutcome {
        if self.queued_writer().is_none() {
            return ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed);
        }
        let Some(class) = self.response_class() else {
            return ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed);
        };
        let (_, payload) = prepared.into_parts();
        self.send_payload_inner_with_response_observation(
            payload,
            class,
            None,
            control.deadline(),
            Some(control),
            RequestStopPolicy::All,
            None,
            ResponseQueueWaitObservation::Response,
            "transport-session-writer".to_string(),
        )
        .await
        .map_or_else(SendFailure::into_response, |_| ResponseSendOutcome::Written)
    }

    pub(crate) async fn send_prepared_deferred_response(
        &mut self,
        prepared: PreparedResponse,
        control: &RequestControlView,
        deferred_drop: DeferredTransportDropHandle,
    ) -> ResponseSendOutcome {
        if self.queued_writer().is_none() {
            return ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed);
        }
        if self.response_drop.is_none() {
            return ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed);
        }
        let Some(class) = self.response_class() else {
            return ResponseSendOutcome::Rejected(ResponseCompletionOutcome::SessionClosed);
        };
        let (_, payload) = prepared.into_parts();
        self.send_payload_inner_with_response_observation(
            payload,
            class,
            None,
            control.deadline(),
            Some(control),
            RequestStopPolicy::All,
            Some(deferred_drop),
            ResponseQueueWaitObservation::Response,
            "transport-session-writer".to_string(),
        )
        .await
        .map_or_else(SendFailure::into_response, |_| ResponseSendOutcome::Written)
    }

    /// Sends a server response through the canonical writer with a stage-aware
    /// completion error. This is intentionally crate-private while the public
    /// response receipt/context API is introduced separately.
    pub(crate) async fn send_response(&mut self, command: RemotingCommand) -> ResponseSendOutcome {
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let frame = match self.limits.encode_command(command) {
            Ok(frame) => frame,
            Err(source) => return ResponseSendOutcome::OperationalFailure(ResponseOperationalFailure::encode(source)),
        };
        self.send_payload_inner_with_response_observation(
            OutboundPayload::Frame(frame),
            class,
            None,
            None,
            None,
            RequestStopPolicy::All,
            None,
            ResponseQueueWaitObservation::Response,
            "transport-session-writer".to_string(),
        )
        .await
        .map_or_else(SendFailure::into_response, |_| ResponseSendOutcome::Written)
    }

    /// Sends a `RemotingCommand` to the peer (consumes command).
    ///
    /// Encodes the command into immutable prefix/header/body segments, then flushes it to the
    /// negotiated plaintext or TLS writer.
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `command` - The command to send (consumed)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Command successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # State Management
    ///
    /// On error, this method:
    /// 1. Marks connection as `Degraded` via watch channel
    /// 2. Broadcasts state change to all subscribers
    /// 3. Returns the error to caller
    ///
    /// **No need to explicitly check `is_healthy()` before calling** - just
    /// handle the `Result` and the connection state is automatically managed.
    ///
    /// # Lifecycle
    ///
    /// 1. Encode the prefix and header while preserving the existing body allocation.
    /// 2. Reserve the exact frame-byte cost before queued staging.
    /// 3. Transfer the immutable frame to the single writer owner.
    ///
    /// # Performance Optimization
    ///
    /// - Plaintext retains three segments through `write_vectored`.
    /// - TLS uses one writer-owned, bounded coalescing buffer.
    pub async fn send_command(&mut self, command: RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let frame = self.limits.encode_command(command)?;
        self.send_payload(
            OutboundPayload::Frame(frame),
            class,
            None,
            None,
            "transport-session-writer".to_string(),
        )
        .await
    }

    /// Sends one command under a caller-owned immutable request deadline.
    ///
    /// The deadline is checked before encoding, after encoding, during queue
    /// admission, immediately before socket write, and while the write is in
    /// progress.
    ///
    /// # Errors
    ///
    /// Returns a typed queue, before-send, write-timeout, or transport error.
    pub async fn send_command_with_deadline(
        &mut self,
        command: RemotingCommand,
        deadline: RequestDeadline,
        target: impl Into<String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let target = target.into();
        deadline.ensure_before_send()?;
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let frame = self.limits.encode_command(command)?;
        deadline.ensure_before_send()?;

        self.send_payload(OutboundPayload::Frame(frame), class, None, Some(deadline), target)
            .await
    }

    pub(crate) async fn send_command_outcome_with_deadline(
        &mut self,
        command: RemotingCommand,
        deadline: RequestDeadline,
        target: impl Into<String>,
    ) -> CommandSendOutcome {
        let target = target.into();
        if deadline.is_expired() {
            return CommandSendOutcome::DeadlineExpired;
        }
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let frame = match self.limits.encode_command(command) {
            Ok(frame) => frame,
            Err(source) => return CommandSendOutcome::EncodingFailed(source),
        };
        if deadline.is_expired() {
            return CommandSendOutcome::DeadlineExpired;
        }

        self.send_payload_inner(
            OutboundPayload::Frame(frame),
            class,
            None,
            Some(deadline),
            None,
            RequestStopPolicy::All,
            None,
            target,
        )
        .await
        .map_or_else(SendFailure::into_command_outcome, |_| CommandSendOutcome::Written)
    }

    /// Sends a RocketMQ command whose body is a validated, leased file region.
    ///
    /// The command must not contain an in-memory body. The complete head plus file length is
    /// admitted to the existing bounded writer queue, and the lease remains owned by the writer
    /// payload until completion. Plaintext Linux TCP may use `sendfile`; TLS always uses bounded
    /// portable reads so the bytes still pass through rustls encryption.
    ///
    /// # Errors
    ///
    /// Returns a typed encoding, admission, deadline, file-read, or socket-write error. After any
    /// prefix/header progress, failure poisons and closes the current connection.
    pub async fn send_file_region_command(
        &mut self,
        command_without_body: RemotingCommand,
        body: FileRegion,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.send_file_regions_command(command_without_body, FileRegionSequence::single(body), deadline)
            .await
    }

    /// Sends one command whose body is an ordered sequence of leased file regions.
    pub async fn send_file_regions_command(
        &mut self,
        command_without_body: RemotingCommand,
        body: FileRegionSequence,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.send_file_regions_inner(command_without_body, body, Some(deadline))
            .await
    }

    /// Sends a server response backed by ordered file regions under the writer's own stall bound.
    pub async fn send_file_regions_response(
        &mut self,
        command_without_body: RemotingCommand,
        body: FileRegionSequence,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.send_file_regions_inner(command_without_body, body, None).await
    }

    async fn send_file_regions_inner(
        &mut self,
        command_without_body: RemotingCommand,
        body: FileRegionSequence,
        deadline: Option<RequestDeadline>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let target = "transport-file-region-writer".to_string();
        if let Some(deadline) = deadline {
            deadline.ensure_before_send()?;
        }
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command_without_body.code()));
        let body_len = usize::try_from(body.len()).map_err(|_| {
            rocketmq_error::RocketMQError::illegal_argument("file region sequence length exceeds this platform's usize")
        })?;
        let head = self.limits.encode_frame_head(command_without_body, body_len)?;
        if let Some(deadline) = deadline {
            deadline.ensure_before_send()?;
        }
        self.send_payload(OutboundPayload::FileFrame { head, body }, class, None, deadline, target)
            .await
    }

    /// Sends one command while transferring an existing process-budget
    /// reservation into the session writer queue.
    ///
    /// # Errors
    ///
    /// Returns a typed queue, before-send, write-timeout, budget-rebind, or
    /// transport error.
    pub async fn send_command_with_deadline_and_permit(
        &mut self,
        command: RemotingCommand,
        deadline: RequestDeadline,
        target: impl Into<String>,
        permit: ResourcePermit,
    ) -> rocketmq_error::RocketMQResult<()> {
        let target = target.into();
        deadline.ensure_before_send()?;
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let frame = self.limits.encode_command(command)?;
        deadline.ensure_before_send()?;

        self.send_payload(
            OutboundPayload::Frame(frame),
            class,
            Some(permit),
            Some(deadline),
            target,
        )
        .await
    }

    /// Sends a `RemotingCommand` to the peer (borrows command).
    ///
    /// Similar to `send_command`, but borrows the command mutably instead of
    /// consuming it. Use when the caller needs to retain ownership.
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `command` - Mutable reference to the command to send
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Command successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # Note
    ///
    /// This method may consume the command's body (`take_body()`), modifying
    /// the original command.
    pub async fn send_command_ref(&mut self, command: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let owned = command.clone();
        let frame = self.limits.encode_command(owned)?;
        let _ = command.take_body();
        self.send_payload(
            OutboundPayload::Frame(frame),
            class,
            None,
            None,
            "transport-session-writer".to_string(),
        )
        .await
    }

    /// Sends multiple `RemotingCommand`s under one ordered writer-queue admission.
    ///
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// Each command remains an independently delimited immutable frame. This preserves plaintext
    /// vectored writes and TLS per-frame aggregation while avoiding a second full-batch copy.
    ///
    /// # Arguments
    ///
    /// * `commands` - Vector of commands to send (consumed for zero-copy)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: All commands sent successfully
    /// - `Err(e)`: Network I/O error (connection marked as Degraded)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let batch = vec![cmd1, cmd2, cmd3];
    /// connection.send_batch(batch).await?;
    /// ```
    pub async fn send_batch(&mut self, commands: Vec<RemotingCommand>) -> rocketmq_error::RocketMQResult<()> {
        if commands.is_empty() {
            return Ok(());
        }
        let limits = self.limits;
        let frames = commands
            .into_iter()
            .map(|command| limits.encode_command(command))
            .collect::<rocketmq_error::RocketMQResult<Vec<_>>>()?;
        let payload = OutboundPayload::batch(frames)?;
        self.send_payload(
            payload,
            AdmissionClass::Data,
            None,
            None,
            "transport-session-writer".to_string(),
        )
        .await
    }

    /// Sends one complete pre-encoded remoting frame directly to the peer (zero-copy).
    ///
    /// Bypasses command encoding but validates the serialized prefix, header, body, and complete
    /// wire length against the connection-owned profile before writing.
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to send (reference-counted, zero-copy)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Bytes successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # Performance
    ///
    /// This is the most efficient send method as it avoids intermediate buffering
    /// and serialization overhead.
    pub async fn send_bytes(&mut self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.limits.validate_frame_segments(std::slice::from_ref(&bytes))?;
        self.send_payload(
            OutboundPayload::Contiguous(bytes),
            AdmissionClass::Data,
            None,
            None,
            "transport-session-writer".to_string(),
        )
        .await
    }

    /// Sends a static byte slice to the peer (zero-copy).
    ///
    /// Converts a `&'static [u8]` to `Bytes` and sends. Use for compile-time
    /// known data (e.g., protocol constants).
    /// **Automatically marks connection as Degraded on I/O errors.**
    ///
    /// # Arguments
    ///
    /// * `slice` - Static byte slice with `'static` lifetime
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Slice successfully sent
    /// - `Err(e)`: Network I/O error occurred (connection marked as Degraded)
    ///
    /// # Example
    ///
    /// ```ignore
    /// const PING: &[u8] = b"PING\r\n";
    /// connection.send_slice(PING).await?;
    /// ```
    pub async fn send_slice(&mut self, slice: &'static [u8]) -> rocketmq_error::RocketMQResult<()> {
        let bytes = Bytes::from_static(slice);
        self.limits.validate_raw_payload(bytes.len())?;
        self.send_payload(
            OutboundPayload::Contiguous(bytes),
            AdmissionClass::Control,
            None,
            None,
            "transport-session-writer".to_string(),
        )
        .await
    }

    /// Sends one already encoded frame as immutable ordered segments.
    ///
    /// The complete sequence is validated against the connection-owned frame, header, and body
    /// limits before it enters the writer queue, preserving zero-copy plaintext output without
    /// allowing multipart writes to bypass the endpoint policy.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error before socket progress when the prefix, aggregate
    /// length, header length, body length, or endpoint profile is invalid.
    pub async fn send_frame_segments(&mut self, segments: Vec<Bytes>) -> rocketmq_error::RocketMQResult<()> {
        let encoded_len = self.limits.validate_frame_segments(&segments)?;
        self.send_payload(
            OutboundPayload::FrameSegments { segments, encoded_len },
            AdmissionClass::Data,
            None,
            None,
            "transport-session-writer".to_string(),
        )
        .await
    }

    /// Gets the unique identifier for this connection.
    ///
    /// # Returns
    ///
    /// Reference to the connection ID (UUID-based string)
    #[inline]
    pub fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    /// Gets the current connection state.
    ///
    /// # Returns
    ///
    /// Current `ConnectionState` (Healthy, Degraded, or Closed)
    ///
    /// # Performance
    ///
    /// This is a fast, lock-free read from the watch channel receiver.
    /// No system calls or network operations involved.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if connection.state() == ConnectionState::Healthy {
    ///     // Safe to send
    ///     connection.send_command(cmd).await?;
    /// }
    /// ```
    #[inline]
    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Checks if the connection is in a healthy state (convenience method).
    ///
    /// # Returns
    ///
    /// - `true`: Connection is `Healthy` and operational
    /// - `false`: Connection is `Degraded` or `Closed`
    ///
    /// # Note
    ///
    /// **Prefer using `send_*()` methods directly** rather than checking state first.
    /// This method is provided for backward compatibility and specific use cases
    /// like connection pool eviction.
    ///
    /// **Best practice (Tokio-idiomatic)**:
    /// ```rust,ignore
    /// // Don't do this:
    /// if connection.is_healthy() {
    ///     connection.send_command(cmd).await?;
    /// }
    ///
    /// // Do this instead:
    /// match connection.send_command(cmd).await {
    ///     Ok(()) => { /* success */ }
    ///     Err(e) => { /* connection automatically marked as degraded */ }
    /// }
    /// ```
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.state() == ConnectionState::Healthy
    }

    /// Subscribes to connection state changes.
    ///
    /// # Returns
    ///
    /// A `watch::Receiver` that notifies on state transitions
    ///
    /// # Example: Monitor state in background task
    ///
    /// ```rust,ignore
    /// let mut state_watcher = connection.subscribe();
    /// tokio::spawn(async move {
    ///     while state_watcher.changed().await.is_ok() {
    ///         match *state_watcher.borrow() {
    ///             ConnectionState::Healthy => println!("Connection restored"),
    ///             ConnectionState::Degraded => println!("Connection degraded"),
    ///             ConnectionState::Closed => {
    ///                 println!("Connection closed");
    ///                 break;
    ///             }
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// # Example: Wait for state change with timeout
    ///
    /// ```rust,ignore
    /// let mut state_watcher = connection.subscribe();
    /// tokio::select! {
    ///     _ = state_watcher.changed() => {
    ///         println!("State changed to: {:?}", *state_watcher.borrow());
    ///     }
    ///     _ = tokio::time::sleep(Duration::from_secs(5)) => {
    ///         println!("No state change within 5 seconds");
    ///     }
    /// }
    /// ```
    pub fn subscribe(&self) -> watch::Receiver<ConnectionState> {
        self.state_tx.subscribe()
    }

    /// Returns a cloneable lifecycle capability that does not expose transport mutation.
    pub fn state_handle(&self) -> ConnectionStateHandle {
        ConnectionStateHandle {
            state_tx: self.state_tx.clone(),
            state_rx: self.state_rx.clone(),
        }
    }

    /// Marks the connection as closed (internal use).
    ///
    /// Called when connection is explicitly closed. Broadcasts final state.
    #[inline]
    fn mark_closed(&self) {
        let _ = self.state_tx.send(ConnectionState::Closed);
    }

    /// Explicitly closes the connection and broadcasts Closed state.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// connection.close();
    /// assert_eq!(connection.state(), ConnectionState::Closed);
    /// ```
    pub fn close(&self) {
        self.mark_closed();
    }

    /// Flushes and actively shuts down the socket write half before marking the connection closed.
    pub async fn shutdown(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let result = match &mut self.outbound {
            ConnectionWriter::Queued(queued) => {
                let (completion, result) = oneshot::channel();
                queued.writer.close(completion).await.map_err(|_| {
                    rocketmq_error::RocketMQError::Shared(connection_failed_without_source(TransportStage::Closed))
                })?;
                result.await.map_err(|source| {
                    rocketmq_error::RocketMQError::Shared(connection_failed(TransportStage::Closed, source))
                })?
            }
            ConnectionWriter::Direct(writer) => writer.shutdown().await.map_err(|source| {
                rocketmq_error::RocketMQError::Shared(connection_failed(TransportStage::Closed, source))
            }),
        };
        self.mark_closed();
        result
    }

    /// Legacy alias for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Use `is_healthy()` or `state()` instead for clearer semantics.
    #[inline]
    #[deprecated(since = "0.7.0", note = "Use `is_healthy()` or `state()` instead")]
    pub fn connection_is_ok(&self) -> bool {
        self.is_healthy()
    }
}

#[cfg(test)]
#[path = "../tests/unit/connection/write_failure_semantics.rs"]
mod write_failure_semantics_tests;

#[cfg(test)]
mod lifecycle_regression_tests {
    use std::sync::Arc;

    use super::SessionLifecycle;

    #[tokio::test]
    async fn retirement_closes_admission_and_waits_for_lock_free_send_leases() {
        let lifecycle = Arc::new(SessionLifecycle::new());
        let lease = lifecycle.begin_send().expect("session should accept an initial send");
        let retiring = Arc::clone(&lifecycle);
        let (retired_tx, mut retired_rx) = tokio::sync::oneshot::channel();
        let retirement = tokio::spawn(async move {
            retiring.begin_retirement().await;
            let _ = retired_tx.send(());
        });

        while lifecycle.begin_send().is_some() {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            retired_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        drop(lease);
        retired_rx
            .await
            .expect("retirement should observe the final lease drop");
        retirement.await.expect("retirement task should not panic");
    }
}
