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
use rocketmq_rust::ArcMut;
use tokio::time::timeout;
use tracing::error;
use uuid::Uuid;

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
type ChannelMessage = (
    RemotingCommand,                                                                       /* command */
    Option<tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<RemotingCommand>>>, /* response_tx */
    Option<u64>,                                                                           /* timeout_millis */
);

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
    pub(crate) response_table: ArcMut<HashMap<i32, ResponseFuture>>,
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
pub(crate) async fn handle_send(
    mut connection: ArcMut<Connection>,
    rx: Receiver<ChannelMessage>,
    mut response_table: ArcMut<HashMap<i32, ResponseFuture>>,
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

        let (send, tx, timeout_millis) = msg;
        let opaque = send.opaque();

        // Register response future if this is a request-response operation
        if let Some(tx) = tx {
            response_table.insert(
                opaque,
                ResponseFuture::new(opaque, timeout_millis.unwrap_or(0), true, tx),
            );
        }

        // Send command via connection
        match connection.send_command(send).await {
            Ok(_) => {}
            Err(error) => match error {
                rocketmq_error::RocketMQError::IO(error) => {
                    // I/O error means connection is broken
                    // Connection state is automatically marked as degraded by send_command()
                    error!("send request failed: {}", error);
                    response_table.remove(&opaque);
                    return;
                }
                _ => {
                    // Other errors: remove response future but continue
                    response_table.remove(&opaque);
                }
            },
        };
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
        const QUEUE_CAPACITY: usize = 1024;

        // Use flume bounded channel for better performance
        // flume provides lock-free operations and better throughput than tokio::mpsc
        let (outbound_queue_tx, outbound_queue_rx) = flume::bounded(QUEUE_CAPACITY);

        let connection = ArcMut::new(connection);
        tokio::spawn(handle_send(
            connection.clone(),
            outbound_queue_rx,
            response_table.clone(),
        ));
        Self {
            outbound_queue_tx,
            connection,
            response_table,
        }
    }
}

impl ChannelInner {
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

        // Enqueue request with response tracking
        // flume sender: use send_async() for async context
        if let Err(err) = self
            .outbound_queue_tx
            .send_async((request, Some(response_tx), Some(timeout_millis)))
            .await
        {
            return Err(RocketMQError::network_connection_failed(
                "channel",
                format!("send failed: {}", err),
            ));
        }

        // Wait for response with timeout
        match timeout(Duration::from_millis(timeout_millis), response_rx).await {
            Ok(result) => match result {
                Ok(response) => response,
                Err(e) => {
                    // Response channel closed without sending (connection dropped?)
                    self.response_table.remove(&opaque);
                    Err(RocketMQError::network_connection_failed(
                        "channel",
                        format!("connection dropped: {}", e),
                    ))
                }
            },
            Err(_) => {
                // Timeout expired
                self.response_table.remove(&opaque);
                Err(RocketMQError::Timeout {
                    operation: "channel_recv",
                    timeout_ms: timeout_millis,
                })
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
        let request = request.mark_oneway_rpc();

        // flume sender: use send_async() for async context
        if let Err(err) = self
            .outbound_queue_tx
            .send_async((request, None, Some(timeout_millis)))
            .await
        {
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
        if let Err(err) = self.outbound_queue_tx.send_async((request, None, timeout_millis)).await {
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
