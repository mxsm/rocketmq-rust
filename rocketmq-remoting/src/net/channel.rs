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
use std::time::Duration;

use cheetah_string::CheetahString;
// Use flume for high-performance async channel (40-60% faster than tokio::mpsc)
// Lock-free design provides better throughput under high load
use flume::Receiver;
use flume::Sender;
use rocketmq_error::RocketMQError;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_rust::ArcMut;
use tracing::error;
use uuid::Uuid;

use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestToken;
use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
use crate::protocol::remoting_command::RemotingCommand;

pub type ChannelId = CheetahString;

pub type ArcChannel = ArcMut<Channel>;

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
/// - **Clone-friendly**: Lightweight outer type can be cloned, shares inner state via `ArcMut`
/// - **Equality/Hash**: Based on identity (addresses + ID), not inner state
#[derive(Clone)]
pub struct Channel {
    // === Core State ===
    /// Shared mutable access to channel internals (connection, response tracking, etc.)
    inner: ArcMut<ChannelInner>,

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
    pub fn new(inner: ArcMut<ChannelInner>, local_address: SocketAddr, remote_address: SocketAddr) -> Self {
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

    /// Gets mutable access to the underlying connection.
    ///
    /// # Returns
    ///
    /// Mutable reference to the `Connection` for sending/receiving
    ///
    /// # Use Case
    ///
    /// Direct low-level I/O operations (receive_command, send_command)
    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        self.inner.connection.as_mut()
    }

    /// Gets immutable access to the underlying connection.
    ///
    /// # Returns
    ///
    /// Immutable reference to the `Connection` for inspection
    #[inline]
    pub fn connection_ref(&self) -> &Connection {
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

    /// Gets mutable access to the shared channel state.
    ///
    /// # Returns
    ///
    /// Mutable reference to `ChannelInner` for advanced operations
    pub fn channel_inner_mut(&mut self) -> &mut ChannelInner {
        self.inner.as_mut()
    }

    pub(crate) fn pending_request_owner(&self) -> Option<&PendingRequestOwner> {
        self.inner.pending_request_owner()
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
        timeout_millis: u64,
        sender: tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<RemotingCommand>>,
    },
    LegacyRegistered {
        opaque: i32,
    },
}

type ChannelMessage = (RemotingCommand, Option<ResponseReservation>);

/// Shared state for a `Channel` - handles I/O, async message queueing, and response tracking.
///
/// `ChannelInner` is the "heavy" part of a channel that is shared via `ArcMut` across
/// multiple `Channel` clones. It manages:
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
    outbound_queue_tx: Sender<ChannelMessage>,

    // === I/O Transport ===
    /// Underlying network connection (shared, mutable).
    ///
    /// Wrapped in `ArcMut` to allow concurrent access by the send task
    /// and potential direct access via `Channel::connection_mut()`.
    pub(crate) connection: ArcMut<Connection>,

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
    legacy_response_table: Option<ArcMut<HashMap<i32, ResponseFuture>>>,

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
    mut connection: ArcMut<Connection>,
    rx: Receiver<ChannelMessage>,
    response_table: PendingRequestTable,
    mut legacy_response_table: Option<ArcMut<HashMap<i32, ResponseFuture>>>,
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

        let (send, mut reservation) = msg;

        if let Some(ResponseReservation::Legacy {
            opaque,
            timeout_millis,
            sender,
        }) = reservation.take()
        {
            let Some(table) = legacy_response_table.as_mut() else {
                let _ = sender.send(Err(RocketMQError::network_connection_failed(
                    "channel",
                    "legacy response table is unavailable",
                )));
                continue;
            };
            table.insert(opaque, ResponseFuture::new(opaque, timeout_millis, true, sender));
            reservation = Some(ResponseReservation::LegacyRegistered { opaque });
        }

        // Send command via connection
        if let Err(error) = connection.send_command(send).await {
            let connection_broken = matches!(error, rocketmq_error::RocketMQError::IO(_));
            error!(error = %error, "send request failed");
            match reservation {
                Some(ResponseReservation::Pending(reservation)) => {
                    response_table.complete_token(reservation, Err(error));
                }
                Some(ResponseReservation::LegacyRegistered { opaque }) => {
                    if let Some(future) = legacy_response_table.as_mut().and_then(|table| table.remove(&opaque)) {
                        let _ = future.tx.send(Err(error));
                    }
                }
                Some(ResponseReservation::Legacy { sender, .. }) => {
                    let _ = sender.send(Err(error));
                }
                None => {}
            }
            if connection_broken {
                return;
            }
        }
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
    pub fn new(connection: Connection, response_table: ArcMut<HashMap<i32, ResponseFuture>>) -> Self {
        Self::try_new(connection, response_table).expect("ChannelInner send task requires a Tokio runtime")
    }

    /// Creates a new `ChannelInner` and reports runtime/task startup failures.
    pub fn try_new(
        connection: Connection,
        response_table: ArcMut<HashMap<i32, ResponseFuture>>,
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
        Self::try_new_with_send_task_group(connection, response_table, Some(owner), None, task_group)
    }

    /// Creates a new `ChannelInner` under the provided parent task group.
    pub fn try_new_with_task_group(
        connection: Connection,
        response_table: ArcMut<HashMap<i32, ResponseFuture>>,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let task_group = parent_task_group.child("rocketmq-remoting.channel");
        Self::try_new_with_send_task_group(
            connection,
            PendingRequestTable::new(),
            None,
            Some(response_table),
            task_group,
        )
    }

    pub fn try_new_with_pending_requests_and_task_group(
        connection: Connection,
        response_table: PendingRequestTable,
        parent_task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        let task_group = parent_task_group.child("rocketmq-remoting.channel");
        let owner = response_table.new_owner();
        Self::try_new_with_send_task_group(connection, response_table, Some(owner), None, task_group)
    }

    fn try_new_with_send_task_group(
        connection: Connection,
        response_table: PendingRequestTable,
        pending_request_owner: Option<PendingRequestOwner>,
        legacy_response_table: Option<ArcMut<HashMap<i32, ResponseFuture>>>,
        task_group: TaskGroup,
    ) -> rocketmq_error::RocketMQResult<Self> {
        const QUEUE_CAPACITY: usize = 1024;

        // Use flume bounded channel for better performance
        // flume provides lock-free operations and better throughput than tokio::mpsc
        let (outbound_queue_tx, outbound_queue_rx) = flume::bounded(QUEUE_CAPACITY);

        let connection = ArcMut::new(connection);
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
        Ok(Self {
            outbound_queue_tx,
            connection,
            response_table,
            pending_request_owner,
            legacy_response_table,
            send_task_group: task_group,
        })
    }

    /// Closes the outbound queue and waits for the send task shutdown report.
    pub async fn close_with_report(&mut self, timeout: Duration) -> ShutdownReport {
        let (replacement_tx, replacement_rx) = flume::bounded(0);
        drop(replacement_rx);
        let outbound_queue_tx = std::mem::replace(&mut self.outbound_queue_tx, replacement_tx);
        drop(outbound_queue_tx);

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

    fn close_legacy_requests(&mut self, reason: &'static str) {
        let Some(table) = self.legacy_response_table.as_mut() else {
            return;
        };
        for (_, future) in std::mem::take(table.as_mut()) {
            let _ = future
                .tx
                .send(Err(RocketMQError::network_connection_failed("channel", reason)));
        }
    }

    // === Connection Accessors ===

    /// Gets a cloned `ArcMut` handle to the connection.
    ///
    /// # Returns
    ///
    /// Shared mutable reference to the connection (cheap clone, increments refcount)
    #[inline]
    pub fn connection(&self) -> ArcMut<Connection> {
        self.connection.clone()
    }

    /// Gets an immutable reference to the connection.
    ///
    /// # Returns
    ///
    /// Immutable reference to the underlying `Connection`
    #[inline]
    pub fn connection_ref(&self) -> &Connection {
        self.connection.as_ref()
    }

    /// Gets a mutable reference to the connection.
    ///
    /// # Returns
    ///
    /// Mutable reference to the underlying `Connection`
    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        self.connection.as_mut()
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
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let (response_tx, response_rx) =
            tokio::sync::oneshot::channel::<rocketmq_error::RocketMQResult<RemotingCommand>>();
        let opaque = request.opaque();
        let (guard, deadline, reservation) = if let Some(owner) = self.pending_request_owner.as_ref() {
            let guard = self
                .response_table
                .register_for_owner(owner, opaque, timeout_millis, response_tx)?;
            let deadline = guard.deadline();
            let reservation = ResponseReservation::Pending(guard.token());
            (Some(guard), deadline, reservation)
        } else {
            (
                None,
                std::time::Instant::now() + Duration::from_millis(timeout_millis),
                ResponseReservation::Legacy {
                    opaque,
                    timeout_millis,
                    sender: response_tx,
                },
            )
        };

        // Enqueue request with response tracking
        // flume sender: use send_async() for async context
        if let Err(err) = self.outbound_queue_tx.send_async((request, Some(reservation))).await {
            return Err(RocketMQError::network_connection_failed(
                "channel",
                format!("send failed: {}", err),
            ));
        }

        // Wait for response with timeout
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), response_rx).await {
            Ok(result) => match result {
                Ok(response) => response,
                Err(e) => Err(RocketMQError::network_connection_failed(
                    "channel",
                    format!("connection dropped: {}", e),
                )),
            },
            Err(_) => {
                // Timeout expired
                if let Some(guard) = guard {
                    Err(guard.expire("channel_recv", timeout_millis))
                } else {
                    if let Some(future) = self
                        .legacy_response_table
                        .as_mut()
                        .and_then(|table| table.remove(&opaque))
                    {
                        let _ = future.tx.send(Err(RocketMQError::Timeout {
                            operation: "channel_recv",
                            timeout_ms: timeout_millis,
                        }));
                    }
                    Err(RocketMQError::Timeout {
                        operation: "channel_recv",
                        timeout_ms: timeout_millis,
                    })
                }
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
        _timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = request.mark_oneway_rpc();

        // flume sender: use send_async() for async context
        if let Err(err) = self.outbound_queue_tx.send_async((request, None)).await {
            error!("send oneway request failed: {}", err);
            return Err(RocketMQError::network_connection_failed(
                "channel",
                format!("send oneway failed: {}", err),
            ));
        }
        Ok(())
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
        // flume sender: use send_async() for async context
        let _ = timeout_millis;
        if let Err(err) = self.outbound_queue_tx.send_async((request, None)).await {
            error!("send request failed: {}", err);
            return Err(RocketMQError::network_connection_failed(
                "channel",
                format!("send failed: {}", err),
            ));
        }
        Ok(())
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
        self.connection.is_healthy()
    }

    /// Legacy alias for `is_healthy()` - kept for backward compatibility.
    ///
    /// # Deprecated
    ///
    /// Use `is_healthy()` instead for clearer semantics.
    #[inline]
    #[deprecated(since = "0.1.0", note = "Use `is_healthy()` instead")]
    pub fn is_ok(&self) -> bool {
        self.connection.is_healthy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::pending_request_table::PendingRequestTable;
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
            ArcMut::new(HashMap::<i32, ResponseFuture>::new()),
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

        let mut response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
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
            let mut channel_inner = channel_inner;
            let response = channel_inner.send_wait_response(request, 1_000).await;
            let report = channel_inner.close_with_report(Duration::from_secs(1)).await;
            (response, report)
        });
        tokio::time::timeout(Duration::from_millis(100), async {
            while !response_table.contains_key(&73) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("legacy response table should receive the pending request");
        let response = RemotingCommand::create_response_command_with_code(0).set_opaque(73);
        assert!(response_table
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

        let mut channel_inner =
            ChannelInner::try_new_with_pending_requests(Connection::new(socket), pending_requests.clone()).unwrap();
        let guard = pending_requests
            .register_for_owner(channel_inner.pending_request_owner().unwrap(), 91, 30_000, sender)
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
        let mut first =
            ChannelInner::try_new_with_pending_requests(Connection::new(first_socket), pending_requests.clone())
                .unwrap();
        let second =
            ChannelInner::try_new_with_pending_requests(Connection::new(second_socket), pending_requests.clone())
                .unwrap();
        let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
        let (second_sender, mut second_receiver) = tokio::sync::oneshot::channel();
        let first_guard = pending_requests
            .register_for_owner(first.pending_request_owner().unwrap(), 51, 30_000, first_sender)
            .unwrap();
        let second_guard = pending_requests
            .register_for_owner(second.pending_request_owner().unwrap(), 51, 30_000, second_sender)
            .unwrap();

        first.close_with_report(Duration::from_secs(1)).await;

        assert!(matches!(first_receiver.await.unwrap(), Err(RocketMQError::Network(_))));
        assert!(second_receiver.try_recv().is_err());
        assert_eq!(pending_requests.len(), 1);
        drop((first_guard, second_guard, second));
    }
}
