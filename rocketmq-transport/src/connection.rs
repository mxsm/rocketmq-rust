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

use std::collections::VecDeque;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures_util::StreamExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::codec::remoting_command_codec::RemotingCommandCodec;
use crate::codec::remoting_command_codec::SessionCommandDecoder;
use crate::deadline::RequestDeadline;
use crate::telemetry::TransportTelemetry;
use crate::write_strategy::FrameWriteMode;
use crate::write_strategy::FrameWriter;
use crate::write_strategy::OutboundPayload;
use crate::write_strategy::QueuedWrite;
use crate::write_strategy::QueuedWriteProgress;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
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
    writer: mpsc::Sender<QueuedWrite>,
    writer_diagnostics: Arc<SessionWriterDiagnostics>,
    admission: Arc<AdmissionController>,
    scope: AdmissionScope,
    response_class: Option<AdmissionClass>,
    lifecycle: Arc<SessionLifecycle>,
}

pub type ConnectionId = CheetahString;

/// Async transport accepted by the RocketMQ framed connection.
pub trait ConnectionTransport: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> ConnectionTransport for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub type BoxedConnectionTransport = Box<dyn ConnectionTransport>;
type ConnectionReadHalf = FramedRead<ReadHalf<BoxedConnectionTransport>, RemotingCommandCodec>;
pub(crate) type SessionConnectionReadHalf = FramedRead<ReadHalf<BoxedConnectionTransport>, SessionCommandDecoder>;
pub(crate) type ConnectionFrameWriter = FrameWriter<WriteHalf<BoxedConnectionTransport>>;

enum ConnectionWriter {
    Direct(ConnectionFrameWriter),
    Queued(QueuedConnection),
}

static TRANSPORT_ENCODED_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);

/// Monotonic process-wide transport I/O diagnostics.
///
/// The counter advances only after a complete encoded RocketMQ frame write
/// succeeds. It deliberately observes the transport framing boundary, before
/// optional TLS record encoding, so baseline and candidate measurements remain
/// comparable across plaintext and TLS variants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportIoSnapshot {
    pub encoded_bytes_written: u64,
}

/// Low-cardinality diagnostics for one canonical session writer queue.
#[derive(Clone, Copy, Debug, Default, serde::Serialize, PartialEq, Eq)]
pub struct SessionWriterSnapshot {
    pub capacity: usize,
    pub queued_items: usize,
    pub queued_bytes: usize,
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
struct WriterQueueEntry {
    id: u64,
    bytes: usize,
    enqueued_at: Instant,
}

#[derive(Debug, Default)]
struct SessionWriterDiagnosticsState {
    next_id: u64,
    queue: VecDeque<WriterQueueEntry>,
    accepted: u64,
    rejected: u64,
    completed: u64,
    failed: u64,
    deadline_expired: u64,
    last_queue_age: Duration,
    max_queue_age: Duration,
    last_write_latency: Duration,
    max_write_latency: Duration,
}

#[derive(Debug)]
pub(crate) struct SessionWriterDiagnostics {
    capacity: usize,
    state: Mutex<SessionWriterDiagnosticsState>,
}

impl SessionWriterDiagnostics {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(SessionWriterDiagnosticsState::default()),
        }
    }

    pub(crate) fn prepare_enqueue(&self, bytes: usize) -> u64 {
        let mut state = self.state();
        state.next_id = state.next_id.wrapping_add(1).max(1);
        let id = state.next_id;
        state.queue.push_back(WriterQueueEntry {
            id,
            bytes,
            enqueued_at: Instant::now(),
        });
        id
    }

    pub(crate) fn record_accepted(&self) {
        let mut state = self.state();
        state.accepted = state.accepted.saturating_add(1);
    }

    pub(crate) fn record_rejected(&self, prepared_id: Option<u64>) {
        let mut state = self.state();
        if let Some(id) = prepared_id {
            state.queue.retain(|entry| entry.id != id);
        }
        state.rejected = state.rejected.saturating_add(1);
    }

    pub(crate) fn start_write(&self, id: u64) -> Instant {
        let now = Instant::now();
        let mut state = self.state();
        if let Some(index) = state.queue.iter().position(|entry| entry.id == id) {
            let Some(entry) = state.queue.remove(index) else {
                return now;
            };
            let queue_age = now.saturating_duration_since(entry.enqueued_at);
            state.last_queue_age = queue_age;
            state.max_queue_age = state.max_queue_age.max(queue_age);
        }
        now
    }

    pub(crate) fn finish_write(&self, started_at: Instant, succeeded: bool, deadline_expired: bool) {
        let write_latency = started_at.elapsed();
        let mut state = self.state();
        state.last_write_latency = write_latency;
        state.max_write_latency = state.max_write_latency.max(write_latency);
        if succeeded {
            state.completed = state.completed.saturating_add(1);
        } else {
            state.failed = state.failed.saturating_add(1);
        }
        if deadline_expired {
            state.deadline_expired = state.deadline_expired.saturating_add(1);
        }
    }

    pub(crate) fn snapshot(&self) -> SessionWriterSnapshot {
        let state = self.state();
        let now = Instant::now();
        SessionWriterSnapshot {
            capacity: self.capacity,
            queued_items: state.queue.len(),
            queued_bytes: state
                .queue
                .iter()
                .fold(0_usize, |total, entry| total.saturating_add(entry.bytes)),
            oldest_queue_age_millis: state
                .queue
                .front()
                .map(|entry| duration_millis(now.saturating_duration_since(entry.enqueued_at))),
            accepted: state.accepted,
            rejected: state.rejected,
            completed: state.completed,
            failed: state.failed,
            deadline_expired: state.deadline_expired,
            last_queue_age_millis: duration_millis(state.last_queue_age),
            max_queue_age_millis: duration_millis(state.max_queue_age),
            last_write_latency_millis: duration_millis(state.last_write_latency),
            max_write_latency_millis: duration_millis(state.max_write_latency),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, SessionWriterDiagnosticsState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Returns a read-only snapshot of successful encoded transport writes.
#[must_use]
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

    #[cfg(test)]
    enqueue_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
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
        Self::new_with_plaintext_stream(tcp_stream)
    }

    /// Creates a TCP connection bound to one explicit transport telemetry instance.
    pub fn new_with_telemetry(tcp_stream: TcpStream, telemetry: TransportTelemetry) -> Connection {
        Self::new(tcp_stream).with_telemetry(telemetry)
    }

    pub fn new_with_limits(tcp_stream: TcpStream, limits: FrameLimits) -> Connection {
        Self::new_with_plaintext_stream_and_limits(tcp_stream, limits)
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
        let max_plaintext_frame_bytes = limits.max_frame_bytes.saturating_add(4);
        Self::new_with_stream_limits_and_write_mode(
            stream,
            limits,
            FrameWriteMode::TlsCoalesced {
                max_plaintext_frame_bytes,
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
        let inbound = FramedRead::with_capacity(
            read_half,
            RemotingCommandCodec::with_limits(limits),
            limits.initial_read_bytes.max(4),
        );
        let outbound = FrameWriter::new(write_half, write_mode)
            .unwrap_or_else(|_| unreachable!("connection frame limits always produce a non-zero writer bound"));
        // Initialize watch channel with Healthy state
        let (state_tx, state_rx) = watch::channel(ConnectionState::Healthy);

        Self {
            inbound: Some(inbound),
            outbound: ConnectionWriter::Direct(outbound),
            state_tx,
            state_rx,
            connection_id: CheetahString::from_string(Uuid::new_v4().to_string()),
            telemetry: TransportTelemetry::noop(),
            #[cfg(test)]
            enqueue_gate: None,
        }
    }

    /// Replaces this connection's no-op recorder with an explicitly composed transport recorder.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub(crate) fn new_queued(
        writer: mpsc::Sender<QueuedWrite>,
        writer_diagnostics: Arc<SessionWriterDiagnostics>,
        admission: Arc<AdmissionController>,
        scope: AdmissionScope,
        state_tx: watch::Sender<ConnectionState>,
        state_rx: watch::Receiver<ConnectionState>,
        connection_id: ConnectionId,
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
                scope,
                response_class,
                lifecycle,
            }),
            state_tx,
            state_rx,
            connection_id,
            telemetry,
            #[cfg(test)]
            enqueue_gate: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_enqueue_gate(&mut self, checked: Arc<tokio::sync::Notify>, resume: Arc<tokio::sync::Notify>) {
        self.enqueue_gate = Some((checked, resume));
    }

    pub(crate) fn into_session_io(self) -> (ConnectionFrameWriter, SessionConnectionReadHalf) {
        let writer = match self.outbound {
            ConnectionWriter::Direct(writer) => writer,
            ConnectionWriter::Queued(_) => unreachable!("session runtime requires an owned transport writer"),
        };
        let reader = self
            .inbound
            .unwrap_or_else(|| unreachable!("session runtime requires an owned transport reader"))
            .map_decoder(SessionCommandDecoder::from);
        (writer, reader)
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
        let encoded_len = payload.encoded_len();
        if let Some(deadline) = deadline {
            deadline.ensure_before_send(target.clone())?;
        }
        if let Some(queued) = self.queued_writer() {
            let _send_lease = match queued.lifecycle.begin_send() {
                Some(lease) => lease,
                None => {
                    queued.writer_diagnostics.record_rejected(None);
                    return Err(rocketmq_error::RocketMQError::network_connection_failed(
                        target,
                        "connection writer is retiring",
                    ));
                }
            };
            if self.state() == ConnectionState::Closed {
                queued.writer_diagnostics.record_rejected(None);
                return Err(rocketmq_error::RocketMQError::network_connection_failed(
                    target,
                    "connection is closed",
                ));
            }
            #[cfg(test)]
            if let Some((checked, resume)) = self.enqueue_gate.as_ref() {
                checked.notify_one();
                if let Some(deadline) = deadline {
                    deadline.timeout(resume.notified()).await.map_err(|_| {
                        rocketmq_error::RocketMQError::network_deadline_exceeded_before_send(target.clone())
                    })?;
                } else {
                    resume.notified().await;
                }
            }
            let permit = match reservation {
                Some(reservation) => {
                    queued
                        .admission
                        .rebind_permit(AdmissionResource::Queued, queued.scope, reservation)
                }
                None => queued
                    .admission
                    .try_acquire(AdmissionResource::Queued, queued.scope, encoded_len, class),
            }
            .map_err(|_| {
                queued.writer_diagnostics.record_rejected(None);
                rocketmq_error::RocketMQError::network_queue_full(target.clone())
            })?;
            if let Some(deadline) = deadline {
                deadline.ensure_before_send(target.clone())?;
            }
            let (completion, result) = oneshot::channel();
            let progress = deadline.map(|_| Arc::new(QueuedWriteProgress::waiting()));
            let queue_id = queued.writer_diagnostics.prepare_enqueue(encoded_len);
            match queued.writer.try_send(QueuedWrite::data(
                payload,
                completion,
                permit,
                deadline,
                target.clone(),
                progress.clone(),
                queue_id,
            )) {
                Ok(()) => queued.writer_diagnostics.record_accepted(),
                Err(error) => {
                    queued.writer_diagnostics.record_rejected(Some(queue_id));
                    return Err(match error {
                        tokio::sync::mpsc::error::TrySendError::Full(_) => {
                            rocketmq_error::RocketMQError::network_queue_full(target.clone())
                        }
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                            rocketmq_error::RocketMQError::network_connection_failed(
                                target.clone(),
                                "writer queue closed",
                            )
                        }
                    });
                }
            }
            let outcome = if let Some(deadline) = deadline {
                deadline.timeout(result).await.map_err(|_| {
                    if progress.as_ref().is_some_and(|progress| !progress.write_started()) {
                        rocketmq_error::RocketMQError::network_deadline_exceeded_before_send(target.clone())
                    } else {
                        rocketmq_error::RocketMQError::network_write_timeout(target.clone(), deadline.budget_millis())
                    }
                })?
            } else {
                result.await
            }
            .map_err(|_| {
                rocketmq_error::RocketMQError::network_connection_failed(target.clone(), "writer completion dropped")
            })?;
            return outcome;
        }
        if self.state() == ConnectionState::Closed {
            return Err(rocketmq_error::RocketMQError::network_connection_failed(
                target,
                "connection is closed",
            ));
        }
        let writer = match &mut self.outbound {
            ConnectionWriter::Direct(writer) => writer,
            ConnectionWriter::Queued(_) => unreachable!("queued writer returned above"),
        };
        let send_result = if let Some(deadline) = deadline {
            match deadline.timeout(payload.write_to(writer)).await {
                Ok(result) => result,
                Err(_) => {
                    let _ = self.state_tx.send(ConnectionState::Degraded);
                    let _ = writer.shutdown().await;
                    let _ = self.state_tx.send(ConnectionState::Closed);
                    return Err(rocketmq_error::RocketMQError::network_write_timeout(
                        target,
                        deadline.budget_millis(),
                    ));
                }
            }
        } else {
            payload.write_to(writer).await
        };
        match send_result {
            Ok(()) => {
                record_transport_write(encoded_len);
                Ok(())
            }
            Err(error) => {
                let _ = self.state_tx.send(ConnectionState::Degraded);
                let _ = writer.shutdown().await;
                let _ = self.state_tx.send(ConnectionState::Closed);
                Err(rocketmq_error::RocketMQError::network_connection_failed(
                    target,
                    error.to_string(),
                ))
            }
        }
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
        let frame = EncodedFrame::from_command(command)?;
        self.telemetry.record_network_bytes(frame.encoded_len());
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
        deadline.ensure_before_send(target.clone())?;
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let frame = EncodedFrame::from_command(command)?;
        deadline.ensure_before_send(target.clone())?;

        self.telemetry.record_network_bytes(frame.encoded_len());
        self.send_payload(OutboundPayload::Frame(frame), class, None, Some(deadline), target)
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
        deadline.ensure_before_send(target.clone())?;
        let class = self
            .response_class()
            .unwrap_or_else(|| AdmissionClass::for_request_code(command.code()));
        let frame = EncodedFrame::from_command(command)?;
        deadline.ensure_before_send(target.clone())?;

        self.telemetry.record_network_bytes(frame.encoded_len());
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
        let _ = command.take_body();
        let frame = EncodedFrame::from_command(owned)?;
        self.telemetry.record_network_bytes(frame.encoded_len());
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
        let frames = commands
            .into_iter()
            .map(EncodedFrame::from_command)
            .collect::<rocketmq_error::RocketMQResult<Vec<_>>>()?;
        let payload = OutboundPayload::batch(frames)?;
        self.telemetry.record_network_bytes(payload.encoded_len());
        self.send_payload(
            payload,
            AdmissionClass::Data,
            None,
            None,
            "transport-session-writer".to_string(),
        )
        .await
    }

    /// Sends raw `Bytes` directly to the peer (zero-copy).
    ///
    /// Bypasses command encoding and sends pre-serialized bytes directly.
    /// Use for forwarding or when bytes are already encoded.
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
        self.telemetry.record_network_bytes(bytes.len());
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
        self.telemetry.record_network_bytes(slice.len());
        let bytes = Bytes::from_static(slice);
        self.send_payload(
            OutboundPayload::Contiguous(bytes),
            AdmissionClass::Control,
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
                queued.writer.send(QueuedWrite::close(completion)).await.map_err(|_| {
                    rocketmq_error::RocketMQError::network_connection_failed(
                        "transport-session-writer",
                        "writer queue closed",
                    )
                })?;
                result.await.map_err(|_| {
                    rocketmq_error::RocketMQError::network_connection_failed(
                        "transport-session-writer",
                        "writer completion dropped",
                    )
                })?
            }
            ConnectionWriter::Direct(writer) => writer.shutdown().await.map_err(|error| {
                rocketmq_error::RocketMQError::network_connection_failed(
                    "transport-connection-writer",
                    error.to_string(),
                )
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
mod session_lifecycle_tests {
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

    #[test]
    fn queued_send_path_does_not_restore_a_lock_guard_across_network_await() {
        let source = include_str!("connection.rs").replace("\r\n", "\n");
        let production = source
            .split("#[cfg(test)]\nmod session_lifecycle_tests")
            .next()
            .expect("production connection source");

        assert!(!production.contains("RwLockReadGuard"));
        assert!(!production.contains("RwLockWriteGuard"));
        assert!(!production.contains("lifecycle.begin_send().await"));
    }
}
