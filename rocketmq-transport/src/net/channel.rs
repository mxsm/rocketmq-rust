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

use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
// Use flume for high-performance async channel (40-60% faster than tokio::mpsc)
// Lock-free design provides better throughput under high load
use flume::Receiver;
use flume::Sender;
use rocketmq_error::RocketMQError;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use tracing::error;
use uuid::Uuid;

use crate::base::pending_request_table::materialize_and_estimate_remoting_command_retained_bytes;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestToken;
use crate::connection::Connection;
use crate::connection::ConnectionStateHandle;
use crate::deadline::RequestDeadline;
use crate::dispatch::ResponseDisposition;
use crate::dispatch::ResponseError;
use crate::dispatch::ResponseReceipt;
use crate::dispatch::ResponseSink;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionSequence;
use crate::proxy_protocol::ProxyProtocolMetadata;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

pub type ChannelId = CheetahString;

pub type ArcChannel = Arc<Channel>;

/// High-level abstraction over a bidirectional network connection.
///
/// `Channel` represents a logical communication endpoint with identity,
/// address information, and access to the underlying connection and
/// response tracking infrastructure.
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │           Channel                       │
/// │  ┌─────────────────────────────────┐   │
/// │  │  Identity & Addressing          │   │
/// │  │  - channel_id (UUID)            │   │
/// │  │  - local_address (SocketAddr)   │   │
/// │  │  - remote_address (SocketAddr)  │   │
/// │  └─────────────────────────────────┘   │
/// │  ┌─────────────────────────────────┐   │
/// │  │  ChannelInner (shared state)    │   │
/// │  │  - Connection (I/O)             │   │
/// │  │  - ResponseTable (futures)      │   │
/// │  │  - Message queue (tx/rx)        │   │
/// │  └─────────────────────────────────┘   │
/// └─────────────────────────────────────────┘
/// ```
///
/// ## Design Rationale
///
/// - **Separation of concerns**: `Channel` handles identity/routing, `ChannelInner` handles I/O
/// - **Clone-friendly**: Lightweight outer type can be cloned, shares explicitly synchronized inner
///   state
/// - **Equality/Hash**: Based on identity (addresses + ID), not inner state
#[derive(Clone)]
pub struct Channel {
    // === Core State ===
    /// Shared access to channel internals with one serialized connection writer.
    inner: Arc<ChannelInner>,

    // === Identity & Addressing ===
    /// Local socket address (our end of the connection)
    local_address: SocketAddr,

    /// Remote peer socket address (their end of the connection)
    remote_address: SocketAddr,

    /// Socket peer before a trusted PROXY header supplies the effective source address.
    transport_peer_address: SocketAddr,

    /// Trusted ingress metadata retained for authorization and audit consumers.
    proxy_protocol: Option<Arc<ProxyProtocolMetadata>>,

    /// Unique identifier for this channel instance (UUID-based)
    ///
    /// Used for logging, routing, and distinguishing channels in maps/sets.
    channel_id: ChannelId,
}

impl Channel {
    /// Creates a new `Channel` with generated UUID identifier.
    ///
    /// # Arguments
    ///
    /// * `inner` - Shared channel state (connection, response table, etc.)
    /// * `local_address` - Our local socket address
    /// * `remote_address` - Remote peer socket address
    ///
    /// # Returns
    ///
    /// A new channel with a randomly generated UUID as its ID.
    pub(crate) fn new(inner: Arc<ChannelInner>, local_address: SocketAddr, remote_address: SocketAddr) -> Self {
        let channel_id = Uuid::new_v4().to_string().into();
        Self {
            inner,
            local_address,
            remote_address,
            transport_peer_address: remote_address,
            proxy_protocol: None,
            channel_id,
        }
    }

    pub(crate) fn new_with_proxy_protocol(
        inner: Arc<ChannelInner>,
        local_address: SocketAddr,
        remote_address: SocketAddr,
        transport_peer_address: SocketAddr,
        proxy_protocol: Option<Arc<ProxyProtocolMetadata>>,
    ) -> Self {
        let mut channel = Self::new(inner, local_address, remote_address);
        channel.transport_peer_address = transport_peer_address;
        channel.proxy_protocol = proxy_protocol;
        channel
    }

    // === Address Mutators ===

    /// Updates the local address of this channel.
    ///
    /// # Arguments
    ///
    /// * `local_address` - New local socket address
    #[inline]
    pub fn set_local_address(&mut self, local_address: SocketAddr) {
        self.local_address = local_address;
    }

    /// Updates the remote address of this channel.
    ///
    /// # Arguments
    ///
    /// * `remote_address` - New remote socket address
    #[inline]
    pub fn set_remote_address(&mut self, remote_address: SocketAddr) {
        self.remote_address = remote_address;
    }

    /// Updates the channel identifier.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - New channel ID (convertible to `CheetahString`)
    ///
    /// # Warning
    ///
    /// Changing the ID after insertion into a HashMap/HashSet will break lookup.
    #[inline]
    pub fn set_channel_id(&mut self, channel_id: impl Into<CheetahString>) {
        self.channel_id = channel_id.into();
    }

    // === Address Accessors ===

    /// Gets the local socket address.
    ///
    /// # Returns
    ///
    /// The local address of this channel
    #[inline]
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Gets the remote peer socket address.
    ///
    /// # Returns
    ///
    /// The remote address of this channel
    #[inline]
    pub fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }

    /// Gets the direct transport peer, which can differ from `remote_address()` behind a trusted
    /// PROXY protocol ingress.
    #[inline]
    pub fn transport_peer_address(&self) -> SocketAddr {
        self.transport_peer_address
    }

    /// Gets trusted PROXY source/destination/TLV metadata for this channel.
    #[inline]
    pub fn proxy_protocol(&self) -> Option<&ProxyProtocolMetadata> {
        self.proxy_protocol.as_deref()
    }

    /// Resolves one Java-compatible HAProxy channel attribute from typed metadata.
    #[must_use]
    pub fn proxy_protocol_attribute(&self, key: &str) -> Option<String> {
        self.proxy_protocol().and_then(|metadata| metadata.attribute(key))
    }

    /// Gets the channel identifier as a string slice.
    ///
    /// # Returns
    ///
    /// String slice of the channel ID
    #[inline]
    pub fn channel_id(&self) -> &str {
        self.channel_id.as_str()
    }

    /// Gets a cloned owned copy of the channel identifier.
    ///
    /// # Returns
    ///
    /// Owned `CheetahString` containing the channel ID
    pub fn channel_id_owned(&self) -> CheetahString {
        self.channel_id.clone()
    }

    // === Connection Access ===

    /// Gets the connection lifecycle capability.
    ///
    /// # Returns
    ///
    /// The handle exposes health and close signaling without socket mutation.
    #[inline]
    pub fn connection_ref(&self) -> &ConnectionStateHandle {
        self.inner.connection_ref()
    }

    // === Inner State Access ===

    /// Gets immutable access to the shared channel state.
    ///
    /// # Returns
    ///
    /// Immutable reference to `ChannelInner` (connection + response table)
    pub fn channel_inner(&self) -> &ChannelInner {
        self.inner.as_ref()
    }

    pub(crate) fn pending_request_owner(&self) -> Option<&PendingRequestOwner> {
        self.inner.pending_request_owner()
    }

    pub(crate) async fn send_response(&self, command: RemotingCommand) -> Result<ResponseReceipt, ResponseError> {
        self.inner.send_response(command).await
    }

    pub(crate) async fn send_response_ref(
        &self,
        command: &mut RemotingCommand,
    ) -> Result<ResponseReceipt, ResponseError> {
        self.inner.send_response_ref(command).await
    }

    /// Sends a command through the serialized connection writer.
    pub async fn send_command(&self, command: RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_command(command).await
    }

    /// Sends a command whose body is backed by a leased file region.
    ///
    /// # Errors
    ///
    /// Returns a deadline, frame-encoding, unsupported embedded-channel, or socket-write error.
    pub async fn send_file_region_command(
        &self,
        command_without_body: RemotingCommand,
        body: FileRegion,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.inner
            .send_file_region_command(command_without_body, body, deadline)
            .await
    }

    /// Sends a command whose external body is an ordered file-region sequence.
    pub async fn send_file_regions_command(
        &self,
        command_without_body: RemotingCommand,
        body: FileRegionSequence,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.inner
            .send_file_regions_command(command_without_body, body, deadline)
            .await
    }

    /// Sends a server response backed by ordered file regions.
    pub async fn send_file_regions_response(
        &self,
        command_without_body: RemotingCommand,
        body: FileRegionSequence,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_file_regions_response(command_without_body, body).await
    }

    /// Sends a borrowed command through the serialized connection writer.
    pub async fn send_command_ref(&self, command: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_command_ref(command).await
    }

    /// Sends pre-encoded bytes through the serialized connection writer.
    pub async fn send_bytes(&self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_bytes(bytes).await
    }

    /// Sends one pre-encoded frame as an atomically validated immutable segment sequence.
    pub async fn send_frame_segments(&self, segments: Vec<Bytes>) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_frame_segments(segments).await
    }

    /// Sends a request and waits for its correlated response.
    pub async fn send_wait_response(
        &self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        self.inner.send_wait_response(request, timeout_millis).await
    }

    /// Enqueues a one-way request on the bounded outbound path.
    pub async fn send_oneway(
        &self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_oneway(request, timeout_millis).await
    }

    /// Enqueues a request without waiting for a response.
    pub async fn send(
        &self,
        request: RemotingCommand,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send(request, timeout_millis).await
    }

    /// Closes the channel-owned writer and waits for its task report.
    pub async fn close_with_report(&self, timeout: Duration) -> ShutdownReport {
        self.inner.close_with_report(timeout).await
    }
}

impl crate::connection_context::ConnectionContext for Channel {
    fn local_address(&self) -> SocketAddr {
        Channel::local_address(self)
    }

    fn remote_address(&self) -> SocketAddr {
        Channel::remote_address(self)
    }

    fn connection_id(&self) -> &str {
        self.channel_id()
    }
}

impl PartialEq for Channel {
    fn eq(&self, other: &Self) -> bool {
        self.local_address == other.local_address
            && self.remote_address == other.remote_address
            && self.channel_id == other.channel_id
    }
}

impl Eq for Channel {}

impl Hash for Channel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.local_address.hash(state);
        self.remote_address.hash(state);
        self.channel_id.hash(state);
    }
}

impl Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel {{ local_address: {:?}, remote_address: {:?}, channel_id: {} }}",
            self.local_address, self.remote_address, self.channel_id
        )
    }
}

impl Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel {{ local_address: {}, remote_address: {}, channel_id: {} }}",
            self.local_address, self.remote_address, self.channel_id
        )
    }
}

/// Internal message type for the send queue.
///
/// Encapsulates a command to send along with optional response tracking.
enum ResponseReservation {
    Pending(PendingRequestToken),
}

const OUTBOUND_QUEUED: u8 = 0;
const OUTBOUND_WRITING: u8 = 1;
const OUTBOUND_SENT: u8 = 2;
const OUTBOUND_FAILED_BEFORE_SEND: u8 = 3;

struct OutboundProgress(AtomicU8);

impl OutboundProgress {
    fn queued() -> Self {
        Self(AtomicU8::new(OUTBOUND_QUEUED))
    }

    fn set(&self, stage: u8) {
        self.0.store(stage, Ordering::Release);
    }

    fn deadline_error(&self, deadline: RequestDeadline) -> RocketMQError {
        match self.0.load(Ordering::Acquire) {
            OUTBOUND_QUEUED | OUTBOUND_FAILED_BEFORE_SEND => {
                RocketMQError::network_deadline_exceeded_before_send("channel")
            }
            OUTBOUND_WRITING => RocketMQError::network_write_timeout("channel", deadline.budget_millis()),
            OUTBOUND_SENT => RocketMQError::network_response_timeout("channel", deadline.budget_millis()),
            _ => RocketMQError::network_response_timeout("channel", deadline.budget_millis()),
        }
    }
}

type ChannelMessage = (
    RemotingCommand,
    Option<ResponseReservation>,
    Option<RequestDeadline>,
    Option<Arc<OutboundProgress>>,
);
/// Shared state for a `Channel` - handles I/O, async message queueing, and response tracking.
///
/// `ChannelInner` is the "heavy" part of a channel shared through `Arc` across
/// multiple `Channel` clones. Mutable resources are owned by explicit synchronization.
///
/// - **Connection**: Low-level TCP I/O
/// - **Send Queue**: Async message queueing to decouple caller from I/O backpressure
/// - **Response Table**: Tracks pending request-response pairs (opaque ID → future)
///
/// ## Threading Model
///
/// - **Send Task**: Dedicated task (`handle_send`) pulls from queue and writes to connection
/// - **Response Tracking**: Shared map accessed by send task (insert) and receive task (remove)
///
/// ## Lifecycle
///
/// 1. **Created**: Spawns background `handle_send` task
/// 2. **Active**: Processes send queue, tracks responses
/// 3. **Shutdown**: Queue closed, pending responses canceled
pub struct ChannelInner {
    // === Message Queue ===
    /// Sender half of the high-performance message queue channel.
    ///
    /// Uses `flume` instead of `tokio::mpsc` for:
    /// - 40-60% better throughput (lock-free for most operations)
    /// - Lower latency under contention
    /// - Better backpressure handling
    ///
    /// Callers use this to enqueue commands for asynchronous sending.
    /// The receive half is owned by the background `handle_send` task.
    outbound_queue_tx: parking_lot::Mutex<Option<Sender<ChannelMessage>>>,

    // === I/O Transport ===
    /// Underlying network connection serialized by one async writer lock.
    connection: ChannelIo,

    /// Cloneable lifecycle state without connection mutation capability.
    connection_state: ConnectionStateHandle,

    // === Response Tracking ===
    /// Map of pending request opaque IDs to their response futures.
    ///
    /// - **Key**: Request opaque ID (unique per request)
    /// - **Value**: owned pending completion and its resource reservation
    ///
    /// Shared between:
    /// - Send task: Inserts entries when request is sent
    /// - Receive task: Removes and completes entries when response arrives
    pub(crate) response_table: PendingRequestTable,

    /// Correlation generation owned by this physical connection.
    pending_request_owner: Option<PendingRequestOwner>,

    /// Tracks the outbound send task for shutdown diagnostics.
    send_task_group: TaskGroup,
    send_task_id: Option<TaskId>,
}

enum ChannelIo {
    Network(Arc<tokio::sync::Mutex<Connection>>),
    Local(ResponseSink),
}

/// Background task that processes the outbound message queue.
///
/// # Performance Features
///
/// - Uses `flume` receiver for lock-free message reception
/// - Processes messages sequentially to maintain order
/// - Handles errors gracefully (marks connection as failed on I/O errors)
///
/// # Potential Optimization (TODO)
///
/// Consider implementing batch sending:
/// ```ignore
/// // Collect multiple pending messages
/// let mut batch = vec![first_msg];
/// while batch.len() < 32 {
///     match rx.try_recv() {
///         Ok(msg) => batch.push(msg),
///         Err(_) => break,
///     }
/// }
/// // Send batch together for better throughput
/// ```
///
/// This would reduce per-message overhead and improve throughput by ~20-40%
/// under high load, at the cost of slightly increased latency for small batches.
async fn handle_send(
    connection: Arc<tokio::sync::Mutex<Connection>>,
    rx: Receiver<ChannelMessage>,
    response_table: PendingRequestTable,
) {
    // Loop until channel is closed or connection fails
    loop {
        // flume receiver is async-compatible: recv_async() awaits message
        let msg = match rx.recv_async().await {
            Ok(msg) => msg,
            Err(_) => {
                // Channel closed, exit gracefully
                break;
            }
        };

        let (send, reservation, deadline, progress) = msg;

        // Send command via connection
        let send_result = match deadline {
            Some(deadline) => {
                let mut connection = match deadline.timeout(connection.lock()).await {
                    Ok(connection) => connection,
                    Err(_) => {
                        complete_send_error(
                            reservation,
                            &response_table,
                            RocketMQError::network_deadline_exceeded_before_send("channel"),
                        );
                        continue;
                    }
                };
                if let Err(error) = deadline.ensure_before_send("channel") {
                    if let Some(progress) = progress.as_ref() {
                        progress.set(OUTBOUND_FAILED_BEFORE_SEND);
                    }
                    complete_send_error(reservation, &response_table, error);
                    continue;
                }
                if let Some(progress) = progress.as_ref() {
                    progress.set(OUTBOUND_WRITING);
                }
                connection.send_command_with_deadline(send, deadline, "channel").await
            }
            None => connection.lock().await.send_command(send).await,
        };
        match send_result {
            Ok(()) => {
                if let Some(progress) = progress.as_ref() {
                    progress.set(OUTBOUND_SENT);
                }
            }
            Err(error) => {
                if matches!(
                    error,
                    RocketMQError::Network(rocketmq_error::NetworkError::DeadlineExceededBeforeSend { .. })
                ) {
                    if let Some(progress) = progress.as_ref() {
                        progress.set(OUTBOUND_FAILED_BEFORE_SEND);
                    }
                }
                let connection_broken = matches!(error, rocketmq_error::RocketMQError::IO(_));
                error!(error = %error, "send request failed");
                complete_send_error(reservation, &response_table, error);
                if connection_broken {
                    return;
                }
            }
        }
    }
}

fn complete_send_error(
    reservation: Option<ResponseReservation>,
    response_table: &PendingRequestTable,
    error: RocketMQError,
) {
    match reservation {
        Some(ResponseReservation::Pending(reservation)) => {
            response_table.complete_token(reservation, Err(error));
        }
        None => {}
    }
}

impl ChannelInner {
    pub(crate) fn new_local(response: ResponseSink, task_group: TaskGroup) -> Self {
        let (outbound_queue_tx, outbound_queue_rx) = flume::bounded(0);
        drop(outbound_queue_rx);
        Self {
            outbound_queue_tx: parking_lot::Mutex::new(Some(outbound_queue_tx)),
            connection: ChannelIo::Local(response),
            connection_state: ConnectionStateHandle::healthy(),
            response_table: PendingRequestTable::new(),
            pending_request_owner: None,
            send_task_group: task_group,
            send_task_id: None,
        }
    }

    /// Creates a new `ChannelInner` and spawns the background send task.
    ///
    /// # Arguments
    ///
    /// * `connection` - The underlying TCP connection
    /// * `response_table` - Shared response tracking map
    ///
    /// # Returns
    ///
    /// A new `ChannelInner` with an active background send task.
    ///
    /// # Implementation Note
    ///
    /// - Queue capacity: 1024 messages (adjust based on load)
    /// - Spawns `handle_send` task immediately
    /// - Task runs until channel is dropped or connection fails
    ///
    /// # Performance
    ///
    /// Uses `flume::bounded` channel for better performance:
    /// - Lock-free operations for most cases
    /// - ~40-60% higher throughput than tokio::mpsc
    /// - Better performance under contention
    pub fn try_new_with_pending_requests(
        connection: Connection,
        response_table: PendingRequestTable,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        Self::try_new_with_pending_requests_and_task_group(connection, response_table, parent_task_group)
    }

    pub fn try_new_with_pending_requests_and_task_group(
        connection: Connection,
        response_table: PendingRequestTable,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let owner = response_table.new_owner();
        Self::try_new_with_send_task_group(connection, response_table, Some(owner), parent_task_group, true)
    }

    pub(crate) fn new_transport_session(
        connection: Connection,
        response_table: PendingRequestTable,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        Self::new_transport_session_with_task_group(connection, response_table, parent_task_group)
    }

    /// Creates a transport-backed channel snapshot under an already-owned task group.
    ///
    /// Unlike `new_transport_session`, this does not register another fixed child. The caller
    /// chooses the registration lifetime and the returned `ChannelInner` keeps the supplied
    /// group alive for exactly as long as the snapshot remains reachable.
    pub(crate) fn new_transport_session_with_task_group(
        connection: Connection,
        response_table: PendingRequestTable,
        task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let pending_request_owner = Some(response_table.new_owner());
        Self::try_new_with_send_task_group(connection, response_table, pending_request_owner, task_group, false)
    }

    fn try_new_with_send_task_group(
        connection: Connection,
        response_table: PendingRequestTable,
        pending_request_owner: Option<PendingRequestOwner>,
        task_group: TaskGroup,
        start_send_task: bool,
    ) -> rocketmq_error::RocketMQResult<Self> {
        const QUEUE_CAPACITY: usize = 1024;

        // Use flume bounded channel for better performance
        // flume provides lock-free operations and better throughput than tokio::mpsc
        let (outbound_queue_tx, outbound_queue_rx) = flume::bounded(if start_send_task { QUEUE_CAPACITY } else { 0 });

        let connection_state = connection.state_handle();
        let connection = Arc::new(tokio::sync::Mutex::new(connection));
        let send_task_id = if start_send_task {
            Some(
                task_group
                    .spawn_service(
                        "remoting.channel.send",
                        handle_send(connection.clone(), outbound_queue_rx, response_table.clone()),
                    )
                    .map_err(|error| {
                        RocketMQError::network_connection_failed(
                            "channel",
                            format!("failed to spawn ChannelInner send task: {error}"),
                        )
                    })?,
            )
        } else {
            drop(outbound_queue_rx);
            None
        };
        Ok(Self {
            outbound_queue_tx: parking_lot::Mutex::new(Some(outbound_queue_tx)),
            connection: ChannelIo::Network(connection),
            connection_state,
            response_table,
            pending_request_owner,
            send_task_group: task_group,
            send_task_id,
        })
    }

    /// Closes the outbound queue and waits for the send task shutdown report.
    pub async fn close_with_report(&self, timeout: Duration) -> ShutdownReport {
        let started_at = Instant::now();
        self.outbound_queue_tx.lock().take();
        self.connection_state.close();

        let mut report = ShutdownReport::new("remoting.channel.send", Duration::ZERO);
        if let Some(task_id) = self.send_task_id {
            if self.send_task_group.wait_task(task_id, timeout).await {
                report.completed = 1;
            } else {
                report.aborted = usize::from(self.send_task_group.abort_task(task_id));
                report.timed_out = 1;
            }
        }
        report.elapsed = started_at.elapsed();
        if let Some(owner) = self.pending_request_owner.as_ref() {
            self.response_table.close_owner(owner, || {
                RocketMQError::network_connection_failed("channel", "connection closed")
            });
        }
        report.log_if_unhealthy();
        report
    }
}

impl Drop for ChannelInner {
    fn drop(&mut self) {
        self.outbound_queue_tx.get_mut().take();
        if let Some(task_id) = self.send_task_id {
            self.send_task_group.abort_task(task_id);
        }
        if let Some(owner) = self.pending_request_owner.as_ref() {
            self.response_table.close_owner(owner, || {
                RocketMQError::network_connection_failed("channel", "connection dropped")
            });
        }
    }
}

impl ChannelInner {
    pub(crate) fn pending_request_owner(&self) -> Option<&PendingRequestOwner> {
        self.pending_request_owner.as_ref()
    }

    fn outbound_queue_sender(&self) -> rocketmq_error::RocketMQResult<Sender<ChannelMessage>> {
        self.outbound_queue_tx
            .lock()
            .as_ref()
            .cloned()
            .ok_or_else(|| RocketMQError::network_connection_failed("channel", "outbound queue is closed"))
    }

    // === Connection Accessors ===

    /// Gets the connection lifecycle capability.
    ///
    /// # Returns
    ///
    /// The handle cannot access transport buffers or socket halves.
    #[inline]
    pub fn connection_ref(&self) -> &ConnectionStateHandle {
        &self.connection_state
    }

    /// Sends a command through the serialized writer capability.
    pub async fn send_command(&self, command: RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        match &self.connection {
            ChannelIo::Network(connection) => connection.lock().await.send_command(command).await,
            ChannelIo::Local(response) => response.send(command).await.map_err(response_sink_error),
        }
    }

    pub(crate) async fn send_response(&self, command: RemotingCommand) -> Result<ResponseReceipt, ResponseError> {
        match &self.connection {
            ChannelIo::Network(connection) => {
                let receipt = ResponseReceipt::legacy_v1(ResponseDisposition::TransportWritten)?;
                connection.lock().await.send_response(command).await?;
                Ok(receipt)
            }
            ChannelIo::Local(response) => {
                let receipt = response.reserve_legacy_v1_receipt()?;
                response.complete_legacy_v1_reserved(command, receipt).await
            }
        }
    }

    /// Sends one network command with a leased external file body.
    pub async fn send_file_region_command(
        &self,
        command_without_body: RemotingCommand,
        body: FileRegion,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<()> {
        match &self.connection {
            ChannelIo::Network(connection) => {
                connection
                    .lock()
                    .await
                    .send_file_region_command(command_without_body, body, deadline)
                    .await
            }
            ChannelIo::Local(_) => Err(RocketMQError::network_connection_failed(
                "embedded-response",
                "file-region transfer requires a network channel",
            )),
        }
    }

    /// Sends one network command with an ordered leased external body.
    pub async fn send_file_regions_command(
        &self,
        command_without_body: RemotingCommand,
        body: FileRegionSequence,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<()> {
        match &self.connection {
            ChannelIo::Network(connection) => {
                connection
                    .lock()
                    .await
                    .send_file_regions_command(command_without_body, body, deadline)
                    .await
            }
            ChannelIo::Local(_) => Err(RocketMQError::network_connection_failed(
                "embedded-response",
                "file-region transfer requires a network channel",
            )),
        }
    }

    /// Sends one network response with an ordered leased external body.
    pub async fn send_file_regions_response(
        &self,
        command_without_body: RemotingCommand,
        body: FileRegionSequence,
    ) -> rocketmq_error::RocketMQResult<()> {
        match &self.connection {
            ChannelIo::Network(connection) => {
                connection
                    .lock()
                    .await
                    .send_file_regions_response(command_without_body, body)
                    .await
            }
            ChannelIo::Local(_) => Err(RocketMQError::network_connection_failed(
                "embedded-response",
                "file-region transfer requires a network channel",
            )),
        }
    }

    /// Sends a borrowed command through the serialized writer capability.
    pub async fn send_command_ref(&self, command: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        match &self.connection {
            ChannelIo::Network(connection) => connection.lock().await.send_command_ref(command).await,
            ChannelIo::Local(response) => {
                let owned = command.clone();
                let _ = command.take_body();
                response.send(owned).await.map_err(response_sink_error)
            }
        }
    }

    pub(crate) async fn send_response_ref(
        &self,
        command: &mut RemotingCommand,
    ) -> Result<ResponseReceipt, ResponseError> {
        match &self.connection {
            ChannelIo::Network(connection) => {
                let receipt = ResponseReceipt::legacy_v1(ResponseDisposition::TransportWritten)?;
                connection.lock().await.send_response_ref(command).await?;
                Ok(receipt)
            }
            ChannelIo::Local(response) => {
                let receipt = response.reserve_legacy_v1_receipt()?;
                let owned = command.clone();
                let _ = command.take_body();
                response.complete_legacy_v1_reserved(owned, receipt).await
            }
        }
    }

    /// Sends pre-encoded bytes through the serialized writer capability.
    pub async fn send_bytes(&self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        match &self.connection {
            ChannelIo::Network(connection) => connection.lock().await.send_bytes(bytes).await,
            ChannelIo::Local(response) => response.send_bytes(bytes).await.map_err(response_sink_error),
        }
    }

    /// Sends one pre-encoded frame without allowing multipart output to bypass endpoint limits.
    pub async fn send_frame_segments(&self, segments: Vec<Bytes>) -> rocketmq_error::RocketMQResult<()> {
        match &self.connection {
            ChannelIo::Network(connection) => connection.lock().await.send_frame_segments(segments).await,
            ChannelIo::Local(response) => response
                .send_frame_segments(segments)
                .await
                .map_err(response_sink_error),
        }
    }

    // === High-Level Send Methods ===

    /// Sends a request and waits for the response (request-response pattern).
    ///
    /// Enqueues the request, tracks it via opaque ID, and blocks until the
    /// response arrives or timeout expires.
    ///
    /// # Arguments
    ///
    /// * `request` - The command to send
    /// * `timeout_millis` - Maximum wait time for response (milliseconds)
    ///
    /// # Returns
    ///
    /// - `Ok(response)`: Response received within timeout
    /// - `Err(ChannelSendRequestFailed)`: Failed to enqueue request
    /// - `Err(ChannelRecvRequestFailed)`: Response channel closed or timeout
    ///
    /// # Lifecycle
    ///
    /// 1. Create oneshot channel for response
    /// 2. Enqueue request with response channel
    /// 3. Wait (with timeout) for response on channel
    /// 4. Clean up response table on error
    ///
    /// # Example
    ///
    /// ```ignore
    /// let request = RemotingCommand::create_request_command(10, header).into();
    /// let response = channel_inner.send_wait_response(request, 3000).await?;
    /// println!("Got response: {:?}", response);
    /// ```
    pub async fn send_wait_response(
        &self,
        mut request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let deadline = RequestDeadline::from_timeout_millis(timeout_millis);
        let progress = Arc::new(OutboundProgress::queued());
        let (response_tx, mut response_rx) =
            tokio::sync::oneshot::channel::<rocketmq_error::RocketMQResult<RemotingCommand>>();
        let opaque = request.opaque();
        let owner = self.pending_request_owner.as_ref().ok_or_else(|| {
            RocketMQError::network_connection_failed("channel", "request-response requires a network session owner")
        })?;
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut request);
        let guard =
            self.response_table
                .register_for_owner_with_bytes(owner, opaque, deadline, retained_bytes, response_tx)?;
        let reservation = ResponseReservation::Pending(guard.token());

        // Enqueue request with response tracking
        let outbound_queue_tx = self.outbound_queue_sender()?;
        deadline.ensure_before_send("channel")?;
        outbound_queue_tx
            .try_send((request, Some(reservation), Some(deadline), Some(progress.clone())))
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => RocketMQError::network_queue_full("channel"),
                flume::TrySendError::Disconnected(_) => {
                    RocketMQError::network_connection_failed("channel", "outbound queue is closed")
                }
            })?;

        // Wait for response with timeout
        match deadline.timeout(&mut response_rx).await {
            Ok(result) => match result {
                Ok(response) => response,
                Err(e) => Err(RocketMQError::network_connection_failed(
                    "channel",
                    format!("connection dropped: {}", e),
                )),
            },
            Err(_) => {
                let stage_error = progress.deadline_error(deadline);
                // Timeout expired
                guard.expire("channel");
                Err(stage_error)
            }
        }
    }

    /// Sends a one-way request without waiting for response (fire-and-forget).
    ///
    /// Marks the request as oneway and enqueues it. Does not track response.
    ///
    /// # Arguments
    ///
    /// * `request` - The command to send
    /// * `timeout_millis` - Timeout for enqueuing (not for response)
    ///
    /// # Returns
    ///
    /// - `Ok(().into())`: Request successfully enqueued
    /// - `Err(ChannelSendRequestFailed)`: Failed to enqueue
    ///
    /// # Use Case
    ///
    /// Notifications, heartbeats, or any scenario where response is not needed.
    /// More efficient than `send_wait_response` as it avoids response tracking overhead.
    pub async fn send_oneway(
        &self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let deadline = RequestDeadline::from_timeout_millis(timeout_millis);
        let request = request.mark_oneway_rpc();
        if let ChannelIo::Local(response) = &self.connection {
            deadline.ensure_before_send("embedded-response")?;
            return response.send(request).await.map_err(response_sink_error);
        }

        let outbound_queue_tx = self.outbound_queue_sender()?;
        deadline.ensure_before_send("channel")?;
        outbound_queue_tx
            .try_send((request, None, Some(deadline), None))
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => RocketMQError::network_queue_full("channel"),
                flume::TrySendError::Disconnected(_) => {
                    RocketMQError::network_connection_failed("channel", "outbound queue is closed")
                }
            })
    }

    /// Sends a request without waiting for response (async enqueue only).
    ///
    /// Similar to `send_oneway`, but does not mark the request as oneway.
    /// Use when caller doesn't care about response but request is not marked as oneway protocol.
    ///
    /// # Arguments
    ///
    /// * `request` - The command to send
    /// * `timeout_millis` - Optional timeout for enqueuing
    ///
    /// # Returns
    ///
    /// - `Ok(())`: Request successfully enqueued
    /// - `Err(ChannelSendRequestFailed)`: Failed to enqueue
    pub async fn send(
        &self,
        request: RemotingCommand,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let deadline = timeout_millis.map(RequestDeadline::from_timeout_millis);
        if let Some(deadline) = deadline {
            deadline.ensure_before_send("channel")?;
        }
        if let ChannelIo::Local(response) = &self.connection {
            return response.send(request).await.map_err(response_sink_error);
        }
        let outbound_queue_tx = self.outbound_queue_sender()?;
        outbound_queue_tx
            .try_send((request, None, deadline, None))
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => RocketMQError::network_queue_full("channel"),
                flume::TrySendError::Disconnected(_) => {
                    RocketMQError::network_connection_failed("channel", "outbound queue is closed")
                }
            })
    }

    // === Health Check ===

    /// Checks if the underlying connection is healthy.
    ///
    /// # Returns
    ///
    /// - `true`: Connection is operational
    /// - `false`: Connection has failed, channel should be discarded
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.connection_state.is_healthy()
    }
}

fn response_sink_error(error: crate::dispatch::ResponseSinkError) -> RocketMQError {
    RocketMQError::response_process_failed("embedded_response_sink", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::pending_request_table::PendingRequestTable;
    use rocketmq_error::NetworkError;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    fn test_parent(name: &'static str) -> TaskGroup {
        rocketmq_runtime::RuntimeContext::from_current(name)
            .service_context("channel-inner-service")
            .task_group()
            .clone()
    }

    #[tokio::test]
    async fn channel_send_task_uses_fixed_owner_and_close_waits_for_task_id() {
        let context = rocketmq_runtime::RuntimeContext::from_current("channel-inner-parent-test");
        let service = context.service_context("channel-inner-service");
        let parent_group = service.task_group().clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client_stream = TcpStream::connect(addr).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();

        let response_table = PendingRequestTable::new();
        let channel_inner = ChannelInner::try_new_with_pending_requests(
            Connection::new(socket),
            response_table.clone(),
            parent_group.clone(),
        )
        .unwrap();
        let response_owner = channel_inner
            .pending_request_owner()
            .expect("network channel owner")
            .clone();
        assert_eq!(channel_inner.send_task_group.id(), parent_group.id());
        let request = RemotingCommand::create_remoting_command(1).set_opaque(73);

        let response_task = tokio::spawn(async move {
            let response = channel_inner.send_wait_response(request, 1_000).await;
            let report = channel_inner.close_with_report(Duration::from_secs(1)).await;
            (response, report)
        });
        tokio::time::timeout(Duration::from_millis(100), async {
            while response_table.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending table should receive the request");
        let response = RemotingCommand::create_response_command_with_code(0).set_opaque(73);
        assert!(response_table.complete_response_for_owner(&response_owner, 73, response));
        let (response, report) = response_task.await.unwrap();
        assert_eq!(response.unwrap().opaque(), 73);
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.name, "remoting.channel.send");
        assert_eq!(report.completed, 1);
        assert_eq!(
            parent_group.lifecycle_state(),
            rocketmq_runtime::TaskGroupLifecycleState::Open
        );
        assert_eq!(parent_group.task_count(), 0);

        let parent_report = parent_group.shutdown(Duration::from_secs(1)).await;
        assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
        assert!(parent_report.children.is_empty(), "{}", parent_report.to_json());
    }

    #[tokio::test]
    async fn close_completes_pending_requests_without_waiting_for_request_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client_stream = TcpStream::connect(addr).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let pending_requests = PendingRequestTable::new();
        let (sender, receiver) = tokio::sync::oneshot::channel();

        let channel_inner = ChannelInner::try_new_with_pending_requests(
            Connection::new(socket),
            pending_requests.clone(),
            test_parent("channel-close-test"),
        )
        .unwrap();
        let guard = pending_requests
            .register_for_owner(
                channel_inner.pending_request_owner().unwrap(),
                91,
                RequestDeadline::from_timeout_millis(30_000),
                sender,
            )
            .unwrap();
        let report = channel_inner.close_with_report(Duration::from_secs(1)).await;

        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), receiver)
                .await
                .expect("close must complete pending requests immediately")
                .expect("close must send a typed result"),
            Err(RocketMQError::Network(_))
        ));
        assert!(pending_requests.is_empty());
        drop(guard);
    }

    #[tokio::test]
    async fn closing_channel_only_completes_requests_owned_by_that_connection() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first_listener.local_addr().unwrap();
        let _first_client = TcpStream::connect(first_addr).await.unwrap();
        let (first_socket, _) = first_listener.accept().await.unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_addr = second_listener.local_addr().unwrap();
        let _second_client = TcpStream::connect(second_addr).await.unwrap();
        let (second_socket, _) = second_listener.accept().await.unwrap();
        let pending_requests = PendingRequestTable::new();
        let first = ChannelInner::try_new_with_pending_requests(
            Connection::new(first_socket),
            pending_requests.clone(),
            test_parent("channel-first-owner-test"),
        )
        .unwrap();
        let second = ChannelInner::try_new_with_pending_requests(
            Connection::new(second_socket),
            pending_requests.clone(),
            test_parent("channel-second-owner-test"),
        )
        .unwrap();
        let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
        let (second_sender, mut second_receiver) = tokio::sync::oneshot::channel();
        let first_guard = pending_requests
            .register_for_owner(
                first.pending_request_owner().unwrap(),
                51,
                RequestDeadline::from_timeout_millis(30_000),
                first_sender,
            )
            .unwrap();
        let second_guard = pending_requests
            .register_for_owner(
                second.pending_request_owner().unwrap(),
                51,
                RequestDeadline::from_timeout_millis(30_000),
                second_sender,
            )
            .unwrap();

        first.close_with_report(Duration::from_secs(1)).await;

        assert!(matches!(first_receiver.await.unwrap(), Err(RocketMQError::Network(_))));
        assert!(second_receiver.try_recv().is_err());
        assert_eq!(pending_requests.len(), 1);
        drop((first_guard, second_guard, second));
    }

    #[tokio::test(start_paused = true)]
    async fn writer_lock_wait_uses_the_request_deadline() {
        let (transport, mut peer) = tokio::io::duplex(4096);
        let pending_requests = PendingRequestTable::new();
        let channel = Arc::new(
            ChannelInner::try_new_with_pending_requests(
                Connection::new_with_plaintext_stream(transport),
                pending_requests.clone(),
                test_parent("channel-writer-deadline-test"),
            )
            .expect("create channel"),
        );
        let locked_connection = match &channel.connection {
            ChannelIo::Network(connection) => Arc::clone(connection),
            ChannelIo::Local(_) => panic!("test requires a network channel"),
        };
        let writer_lock = locked_connection.lock().await;
        let sending = channel.clone();
        let send = tokio::spawn(async move {
            sending
                .send_wait_response(RemotingCommand::create_remoting_command(5), 50)
                .await
        });
        while !channel.outbound_queue_sender().expect("queue").is_empty() || pending_requests.is_empty() {
            tokio::task::yield_now().await;
        }

        tokio::time::advance(Duration::from_millis(50)).await;
        let error = match send.await.expect("send task") {
            Ok(_) => panic!("writer lock wait must time out"),
            Err(error) => error,
        };

        assert!(
            matches!(
                error,
                RocketMQError::Network(NetworkError::DeadlineExceededBeforeSend { .. })
            ),
            "unexpected error: {error:?}"
        );
        let mut byte = [0_u8; 1];
        tokio::select! {
            biased;
            read = peer.read(&mut byte) => panic!("unexpected socket read after writer lock timeout: {read:?}"),
            () = tokio::task::yield_now() => {}
        }

        drop(writer_lock);
        let report = channel.close_with_report(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_oneway_request_is_never_sent_after_writer_lock_releases() {
        let (transport, mut peer) = tokio::io::duplex(4096);
        let channel = Arc::new(
            ChannelInner::try_new_with_pending_requests(
                Connection::new_with_plaintext_stream(transport),
                PendingRequestTable::new(),
                test_parent("channel-oneway-deadline-test"),
            )
            .expect("create channel"),
        );
        let locked_connection = match &channel.connection {
            ChannelIo::Network(connection) => Arc::clone(connection),
            ChannelIo::Local(_) => panic!("test requires a network channel"),
        };
        let writer_lock = locked_connection.lock().await;

        channel
            .send_oneway(RemotingCommand::create_remoting_command(6), 50)
            .await
            .expect("enqueue oneway request");
        while !channel.outbound_queue_sender().expect("queue").is_empty() {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(50)).await;
        drop(writer_lock);
        tokio::task::yield_now().await;

        let mut byte = [0_u8; 1];
        tokio::select! {
            biased;
            read = peer.read(&mut byte) => panic!("unexpected expired oneway socket read: {read:?}"),
            () = tokio::task::yield_now() => {}
        }

        let report = channel.close_with_report(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}
