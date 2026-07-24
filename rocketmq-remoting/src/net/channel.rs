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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
// Use flume for high-performance async channel (40-60% faster than tokio::mpsc)
// Lock-free design provides better throughput under high load
use flume::Receiver;
use flume::Sender;
use rocketmq_error::RocketMQError;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tracing::error;
use uuid::Uuid;

use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestToken;
use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
use crate::connection::ConnectionStateHandle;
use crate::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::deadline::RequestDeadline;

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
    pub fn new(inner: Arc<ChannelInner>, local_address: SocketAddr, remote_address: SocketAddr) -> Self {
        let channel_id = Uuid::new_v4().to_string().into();
        Self {
            inner,
            local_address,
            remote_address,
            channel_id,
        }
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

    /// Sends a command through the serialized connection writer.
    pub async fn send_command(&self, command: RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_command(command).await
    }

    /// Sends a borrowed command through the serialized connection writer.
    pub async fn send_command_ref(&self, command: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_command_ref(command).await
    }

    /// Sends pre-encoded bytes through the serialized connection writer.
    pub async fn send_bytes(&self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.inner.send_bytes(bytes).await
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

impl rocketmq_transport::connection_context::ConnectionContext for Channel {
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
    Legacy {
        opaque: i32,
        deadline: RequestDeadline,
        sender: tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<RemotingCommand>>,
    },
    LegacyRegistered {
        opaque: i32,
    },
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
pub type LegacyResponseTable = Arc<parking_lot::Mutex<HashMap<i32, ResponseFuture>>>;

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
    connection: Arc<tokio::sync::Mutex<Connection>>,

    /// Cloneable lifecycle state without connection mutation capability.
    connection_state: ConnectionStateHandle,

    // === Response Tracking ===
    /// Map of pending request opaque IDs to their response futures.
    ///
    /// - **Key**: Request opaque ID (unique per request)
    /// - **Value**: `ResponseFuture` containing timeout and oneshot channel
    ///
    /// Shared between:
    /// - Send task: Inserts entries when request is sent
    /// - Receive task: Removes and completes entries when response arrives
    pub(crate) response_table: PendingRequestTable,

    /// Correlation generation owned by this physical connection.
    pending_request_owner: Option<PendingRequestOwner>,

    /// Compatibility backend retained by the legacy constructors.
    legacy_response_table: Option<LegacyResponseTable>,

    /// Tracks the outbound send task for shutdown diagnostics.
    send_task_group: TaskGroup,
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
    legacy_response_table: Option<LegacyResponseTable>,
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

        let (send, mut reservation, deadline, progress) = msg;

        if let Some(ResponseReservation::Legacy {
            opaque,
            deadline,
            sender,
        }) = reservation.take()
        {
            let Some(table) = legacy_response_table.as_ref() else {
                let _ = sender.send(Err(RocketMQError::network_connection_failed(
                    "channel",
                    "legacy response table is unavailable",
                )));
                continue;
            };
            table.lock().insert(
                opaque,
                ResponseFuture::new(
                    opaque,
                    deadline.remaining().as_millis().min(u128::from(u64::MAX)) as u64,
                    true,
                    sender,
                ),
            );
            reservation = Some(ResponseReservation::LegacyRegistered { opaque });
        }

        // Send command via connection
        let send_result = match deadline {
            Some(deadline) => {
                let mut connection = match deadline.timeout(connection.lock()).await {
                    Ok(connection) => connection,
                    Err(_) => {
                        complete_send_error(
                            reservation,
                            &response_table,
                            legacy_response_table.as_ref(),
                            RocketMQError::network_deadline_exceeded_before_send("channel"),
                        );
                        continue;
                    }
                };
                if let Err(error) = deadline.ensure_before_send("channel") {
                    if let Some(progress) = progress.as_ref() {
                        progress.set(OUTBOUND_FAILED_BEFORE_SEND);
                    }
                    complete_send_error(reservation, &response_table, legacy_response_table.as_ref(), error);
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
                complete_send_error(reservation, &response_table, legacy_response_table.as_ref(), error);
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
    legacy_response_table: Option<&LegacyResponseTable>,
    error: RocketMQError,
) {
    match reservation {
        Some(ResponseReservation::Pending(reservation)) => {
            response_table.complete_token(reservation, Err(error));
        }
        Some(ResponseReservation::LegacyRegistered { opaque }) => {
            let future = legacy_response_table.and_then(|table| table.lock().remove(&opaque));
            if let Some(future) = future {
                let _ = future.tx.send(Err(error));
            }
        }
        Some(ResponseReservation::Legacy { sender, .. }) => {
            let _ = sender.send(Err(error));
        }
        None => {}
    }
}

impl ChannelInner {
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
    pub fn new(connection: Connection, response_table: LegacyResponseTable) -> Self {
        Self::try_new(connection, response_table).expect("ChannelInner send task requires a Tokio runtime")
    }

    /// Creates a new `ChannelInner` and reports runtime/task startup failures.
    pub fn try_new(
        connection: Connection,
        response_table: LegacyResponseTable,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            RocketMQError::network_connection_failed(
                "channel",
                format!("ChannelInner send task requires a Tokio runtime: {error}"),
            )
        })?;
        let task_group = TaskGroup::root("rocketmq-remoting.channel", RuntimeHandle::new(runtime));
        Self::try_new_with_send_task_group(
            connection,
            PendingRequestTable::new(),
            None,
            Some(response_table),
            task_group,
            true,
        )
    }

    pub fn try_new_with_pending_requests(
        connection: Connection,
        response_table: PendingRequestTable,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            RocketMQError::network_connection_failed(
                "channel",
                format!("ChannelInner send task requires a Tokio runtime: {error}"),
            )
        })?;
        let task_group = TaskGroup::root("rocketmq-remoting.channel", RuntimeHandle::new(runtime));
        let owner = response_table.new_owner();
        Self::try_new_with_send_task_group(connection, response_table, Some(owner), None, task_group, true)
    }

    /// Creates a new `ChannelInner` under the provided parent task group.
    pub fn try_new_with_task_group(
        connection: Connection,
        response_table: LegacyResponseTable,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let task_group = parent_task_group.child("rocketmq-remoting.channel");
        Self::try_new_with_send_task_group(
            connection,
            PendingRequestTable::new(),
            None,
            Some(response_table),
            task_group,
            true,
        )
    }

    pub fn try_new_with_pending_requests_and_task_group(
        connection: Connection,
        response_table: PendingRequestTable,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let task_group = parent_task_group.child("rocketmq-remoting.channel");
        let owner = response_table.new_owner();
        Self::try_new_with_send_task_group(connection, response_table, Some(owner), None, task_group, true)
    }

    pub(crate) fn new_transport_session(
        connection: Connection,
        response_table: PendingRequestTable,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        Self::new_transport_session_with_task_group(
            connection,
            response_table,
            parent_task_group.child("rocketmq-remoting.channel"),
        )
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
        Self::try_new_with_send_task_group(
            connection,
            response_table,
            pending_request_owner,
            None,
            task_group,
            false,
        )
    }

    fn try_new_with_send_task_group(
        connection: Connection,
        response_table: PendingRequestTable,
        pending_request_owner: Option<PendingRequestOwner>,
        legacy_response_table: Option<LegacyResponseTable>,
        task_group: TaskGroup,
        start_send_task: bool,
    ) -> rocketmq_error::RocketMQResult<Self> {
        const QUEUE_CAPACITY: usize = 1024;

        // Use flume bounded channel for better performance
        // flume provides lock-free operations and better throughput than tokio::mpsc
        let (outbound_queue_tx, outbound_queue_rx) = flume::bounded(if start_send_task { QUEUE_CAPACITY } else { 0 });

        let connection_state = connection.state_handle();
        let connection = Arc::new(tokio::sync::Mutex::new(connection));
        if start_send_task {
            task_group
                .spawn_service(
                    "remoting.channel.send",
                    handle_send(
                        connection.clone(),
                        outbound_queue_rx,
                        response_table.clone(),
                        legacy_response_table.clone(),
                    ),
                )
                .map_err(|error| {
                    RocketMQError::network_connection_failed(
                        "channel",
                        format!("failed to spawn ChannelInner send task: {error}"),
                    )
                })?;
        } else {
            drop(outbound_queue_rx);
        }
        Ok(Self {
            outbound_queue_tx: parking_lot::Mutex::new(Some(outbound_queue_tx)),
            connection,
            connection_state,
            response_table,
            pending_request_owner,
            legacy_response_table,
            send_task_group: task_group,
        })
    }

    /// Closes the outbound queue and waits for the send task shutdown report.
    pub async fn close_with_report(&self, timeout: Duration) -> ShutdownReport {
        self.outbound_queue_tx.lock().take();
        self.connection_state.close();

        let report = self.send_task_group.shutdown(timeout).await;
        if let Some(owner) = self.pending_request_owner.as_ref() {
            self.response_table.close_owner(owner, || {
                RocketMQError::network_connection_failed("channel", "connection closed")
            });
        }
        self.close_legacy_requests("connection closed");
        report.log_if_unhealthy();
        report
    }
}

impl Drop for ChannelInner {
    fn drop(&mut self) {
        self.outbound_queue_tx.get_mut().take();
        self.send_task_group.cancel();
        if let Some(owner) = self.pending_request_owner.as_ref() {
            self.response_table.close_owner(owner, || {
                RocketMQError::network_connection_failed("channel", "connection dropped")
            });
        }
        self.close_legacy_requests("connection dropped");
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

    fn close_legacy_requests(&self, reason: &'static str) {
        let Some(table) = self.legacy_response_table.as_ref() else {
            return;
        };
        let pending = std::mem::take(&mut *table.lock());
        for (_, future) in pending {
            let _ = future
                .tx
                .send(Err(RocketMQError::network_connection_failed("channel", reason)));
        }
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
        self.connection.lock().await.send_command(command).await
    }

    /// Sends a borrowed command through the serialized writer capability.
    pub async fn send_command_ref(&self, command: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<()> {
        self.connection.lock().await.send_command_ref(command).await
    }

    /// Sends pre-encoded bytes through the serialized writer capability.
    pub async fn send_bytes(&self, bytes: Bytes) -> rocketmq_error::RocketMQResult<()> {
        self.connection.lock().await.send_bytes(bytes).await
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
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let deadline = RequestDeadline::from_timeout_millis(timeout_millis);
        let progress = Arc::new(OutboundProgress::queued());
        let (response_tx, mut response_rx) =
            tokio::sync::oneshot::channel::<rocketmq_error::RocketMQResult<RemotingCommand>>();
        let opaque = request.opaque();
        let (guard, reservation) = if let Some(owner) = self.pending_request_owner.as_ref() {
            let retained_bytes = request.body().map_or(0, bytes::Bytes::len);
            let guard = self.response_table.register_for_owner_with_bytes(
                owner,
                opaque,
                deadline,
                retained_bytes,
                response_tx,
            )?;
            let reservation = ResponseReservation::Pending(guard.token());
            (Some(guard), reservation)
        } else {
            (
                None,
                ResponseReservation::Legacy {
                    opaque,
                    deadline,
                    sender: response_tx,
                },
            )
        };

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
                if let Some(guard) = guard {
                    guard.expire("channel");
                } else {
                    let future = self
                        .legacy_response_table
                        .as_ref()
                        .and_then(|table| table.lock().remove(&opaque));
                    if let Some(future) = future {
                        let _ = future.tx.send(Err(progress.deadline_error(deadline)));
                    }
                }
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

    /// Legacy alias for `is_healthy()` - kept for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Use `is_healthy()` instead for clearer semantics.
    #[inline]
    #[deprecated(since = "0.1.0", note = "Use `is_healthy()` instead")]
    pub fn is_ok(&self) -> bool {
        self.connection_state.is_healthy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::pending_request_table::PendingRequestTable;
    use rocketmq_error::NetworkError;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    #[test]
    fn try_new_without_tokio_runtime_returns_error() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (stream, _client_stream) = runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let client = TcpStream::connect(addr);
            let server = listener.accept();
            let (client_stream, server_stream) = tokio::join!(client, server);
            (server_stream.unwrap().0, client_stream.unwrap())
        });
        drop(runtime);

        let error = match ChannelInner::try_new(
            Connection::new(stream),
            Arc::new(parking_lot::Mutex::new(HashMap::<i32, ResponseFuture>::new())),
        ) {
            Ok(_) => panic!("try_new should report missing Tokio runtime instead of panicking"),
            Err(error) => error,
        };

        assert!(error
            .to_string()
            .contains("ChannelInner send task requires a Tokio runtime"));
    }

    #[tokio::test]
    async fn try_new_with_task_group_parents_send_task_and_close_returns_report() {
        let context = rocketmq_runtime::RuntimeContext::from_current("channel-inner-parent-test");
        let service = context.service_context("channel-inner-service");
        let parent_group = service.task_group().child("channel-inner-parent");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client_stream = TcpStream::connect(addr).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();

        let response_table = Arc::new(parking_lot::Mutex::new(HashMap::<i32, ResponseFuture>::new()));
        let channel_inner = ChannelInner::try_new_with_task_group(
            Connection::new(socket),
            response_table.clone(),
            parent_group.clone(),
        )
        .unwrap();
        assert_eq!(channel_inner.send_task_group.parent_id(), Some(parent_group.id()));
        let send_group_name = channel_inner.send_task_group.name().to_string();
        let request = RemotingCommand::create_remoting_command(1).set_opaque(73);

        let response_task = tokio::spawn(async move {
            let response = channel_inner.send_wait_response(request, 1_000).await;
            let report = channel_inner.close_with_report(Duration::from_secs(1)).await;
            (response, report)
        });
        tokio::time::timeout(Duration::from_millis(100), async {
            while !response_table.lock().contains_key(&73) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("legacy response table should receive the pending request");
        let response = RemotingCommand::create_response_command_with_code(0).set_opaque(73);
        assert!(response_table
            .lock()
            .remove(&73)
            .expect("legacy response future should remain registered")
            .tx
            .send(Ok(response))
            .is_ok());
        let (response, report) = response_task.await.unwrap();
        assert_eq!(response.unwrap().opaque(), 73);
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.name, send_group_name);

        let parent_report = parent_group.shutdown(Duration::from_secs(1)).await;
        assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
        assert!(
            parent_report.children.iter().any(|child| child.name == send_group_name),
            "{}",
            parent_report.to_json()
        );
    }

    #[tokio::test]
    async fn close_completes_pending_requests_without_waiting_for_request_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client_stream = TcpStream::connect(addr).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let pending_requests = PendingRequestTable::new();
        let (sender, receiver) = tokio::sync::oneshot::channel();

        let channel_inner =
            ChannelInner::try_new_with_pending_requests(Connection::new(socket), pending_requests.clone()).unwrap();
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
        let first =
            ChannelInner::try_new_with_pending_requests(Connection::new(first_socket), pending_requests.clone())
                .unwrap();
        let second =
            ChannelInner::try_new_with_pending_requests(Connection::new(second_socket), pending_requests.clone())
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
                Connection::new_with_stream(transport),
                pending_requests.clone(),
            )
            .expect("create channel"),
        );
        let locked_connection = channel.connection.clone();
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
                Connection::new_with_stream(transport),
                PendingRequestTable::new(),
            )
            .expect("create channel"),
        );
        let locked_connection = channel.connection.clone();
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
