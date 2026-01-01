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
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_rust::wait_for_signal;
use rocketmq_rust::ArcMut;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::time;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::channel_event_listener::ChannelEventListener;
use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::tokio_event::TokioEvent;
use crate::connection::Connection;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;

/// Default limit the max number of connections.
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

/// Default idle timeout in seconds (aligned with Java version: 120s)
const DEFAULT_CHANNEL_IDLE_TIMEOUT_SECONDS: u64 = 120;

/// Per-connection handler managing the lifecycle of a single client connection.
///
/// # Performance Notes
/// - Uses reference-counted handler to avoid cloning heavyweight objects
/// - Shutdown signal via broadcast for efficient multi-connection coordination
/// - Connection context wrapped in ArcMut for safe concurrent access
///
/// # Lifecycle
/// 1. Created when TCP connection accepted
/// 2. Spawned into dedicated Tokio task
/// 3. Processes commands until shutdown or disconnection
/// 4. Notifies listeners on drop
pub struct ConnectionHandler<RP> {
    /// Connection-specific context (channel, state, metrics)
    ///
    /// Wrapped in ArcMut to allow sharing with async tasks without excessive cloning
    connection_handler_context: ConnectionHandlerContext,

    /// Shutdown coordination signal
    ///
    /// Receives broadcast when server initiates graceful shutdown
    shutdown: Shutdown,

    /// Completion notification sender
    ///
    /// Dropped when handler completes, signaling to shutdown coordinator
    _shutdown_complete: mpsc::Sender<()>,

    /// Optional disconnect event broadcaster
    ///
    /// If Some, sends `SocketAddr` when connection closes (for routing table cleanup)
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,

    /// Shared command processing handler
    ///
    /// Reference-counted to avoid cloning per-connection (contains processor + hooks)
    cmd_handler: ArcMut<RemotingGeneralHandler<RP>>,

    /// Event notification channel for ChannelEventListener
    ///
    /// Used to send IDLE and EXCEPTION events to the event dispatcher
    event_tx: Option<mpsc::UnboundedSender<TokioEvent>>,

    /// Idle timeout duration for this connection
    ///
    /// When no data received for this duration, connection is closed and IDLE event is triggered
    idle_timeout: Duration,
}

impl<RP> Drop for ConnectionHandler<RP> {
    fn drop(&mut self) {
        if let Some(ref sender) = self.conn_disconnect_notify {
            let socket_addr = self.connection_handler_context.remote_address();
            warn!("connection[{}] disconnected, Send notify message.", socket_addr);
            let _ = sender.send(socket_addr);
        }
    }
}

impl<RP: RequestProcessor + Sync + 'static> ConnectionHandler<RP> {
    /// Main event loop processing incoming commands until shutdown or disconnect.
    ///
    /// # Flow
    /// 1. Wait for next command or shutdown signal (via `tokio::select!`)
    /// 2. Decode and validate command
    /// 3. Dispatch to business logic processor
    /// 4. Repeat until connection closes or shutdown requested
    ///
    /// # Performance
    /// - Zero-copy command reception where possible
    /// - Early exit on shutdown reduces unnecessary work
    /// - Connection state checked once per loop iteration
    ///
    /// # Error Handling
    /// - Decode errors: logged, connection marked unhealthy
    /// - Processor errors: logged, connection continues (per-request isolation)
    /// - Connection closed: graceful return Ok(())
    #[inline]
    async fn handle(&mut self) -> rocketmq_error::RocketMQResult<()> {
        // Get idle timeout configuration from handler
        let idle_timeout = self.idle_timeout;
        let remote_addr = self.connection_handler_context.remote_address();

        // HOT PATH: Main server receive loop
        while !self.shutdown.is_shutdown {
            let channel = self.connection_handler_context.channel_mut();

            let frame = tokio::select! {
                // Branch 1: Receive next command from peer
                res = channel.connection_mut().receive_command() => res,

                // Branch 2: Shutdown signal received
                _ = self.shutdown.recv() => {
                    // Mark connection as closed to prevent further sends
                    channel.connection_mut().close();
                    return Ok(());
                }

                // Branch 3: Idle timeout - no data received for configured duration
                _ = tokio::time::sleep(idle_timeout) => {
                    warn!(
                        "Connection idle timeout ({}s), remote: {}",
                        idle_timeout.as_secs(),
                        remote_addr
                    );

                    // Clone channel before closing to avoid borrow conflicts
                    let channel_clone = channel.clone();

                    // Send IDLE event to listener
                    if let Some(ref event_tx) = self.event_tx {
                        let _ = event_tx.send(TokioEvent::new(
                            ConnectionNetEvent::IDLE,
                            remote_addr,
                            channel_clone,
                        ));
                    }

                    // Close connection due to idle timeout
                    channel.connection_mut().close();
                    return Ok(());
                }
            };

            // Extract command or handle end-of-stream
            let cmd = match frame {
                Some(Ok(frame)) => frame,
                Some(Err(e)) => {
                    // Decode error - log and close connection
                    error!("Failed to decode command: {:?}", e);

                    // Clone channel before closing to avoid borrow conflicts
                    let channel_clone = channel.clone();

                    // Send EXCEPTION event to listener
                    if let Some(ref event_tx) = self.event_tx {
                        let _ = event_tx.send(TokioEvent::new(
                            ConnectionNetEvent::EXCEPTION,
                            remote_addr,
                            channel_clone,
                        ));
                    }

                    channel.connection_mut().close();
                    return Err(e);
                }
                None => {
                    // Peer closed connection gracefully
                    return Ok(());
                }
            };

            // Dispatch command to business logic
            // Note: process_message_received handles errors internally
            self.cmd_handler
                .process_message_received(&mut self.connection_handler_context, cmd)
                .await;
        }
        Ok(())
    }
}

/// Server listener managing TCP connection acceptance and connection lifecycle.
///
/// # Architecture
/// ```text
/// TcpListener → ConnectionListener → ConnectionHandler (per-connection task)
///                      ↓
///               Event Dispatcher
/// ```
///
/// # Concurrency Control
/// - **Connection Limit**: Semaphore-based backpressure (DEFAULT_MAX_CONNECTIONS)
/// - **Graceful Shutdown**: Broadcast signal to all active handlers
/// - **Event Notification**: Optional async event dispatcher for connection lifecycle
///
/// # Performance Characteristics
/// - O(1) accept loop with backpressure
/// - Parallel connection handling via Tokio spawn
/// - Shared handler state (Arc) to avoid per-connection clones
struct ConnectionListener<RP> {
    /// TCP socket acceptor bound to server address
    listener: TcpListener,

    /// Semaphore controlling max concurrent connections
    ///
    /// Permits acquired before accept, released on handler drop.
    /// Provides backpressure when server reaches capacity.
    limit_connections: Arc<Semaphore>,

    /// Shutdown broadcast sender
    ///
    /// All connection handlers subscribe to this channel.
    /// Sending signal triggers graceful termination across all connections.
    notify_shutdown: broadcast::Sender<()>,

    /// Completion coordination channel
    ///
    /// Each handler holds a clone of this sender.
    /// When all handlers drop (server fully shutdown), receiver unblocks.
    shutdown_complete_tx: mpsc::Sender<()>,

    /// Optional connection disconnect broadcaster
    ///
    /// Used for routing table cleanup and metrics.
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,

    /// Optional lifecycle event listener
    ///
    /// Receives CONNECTED/DISCONNECTED/EXCEPTION events.
    /// Useful for external monitoring and orchestration.
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,

    /// Shared command processing handler
    ///
    /// Contains request processor, RPC hooks, and response routing table.
    /// Arc-wrapped to share across all connection handlers efficiently.
    cmd_handler: ArcMut<RemotingGeneralHandler<RP>>,
}

impl<RP: RequestProcessor + Sync + 'static + Clone> ConnectionListener<RP> {
    /// Main server event loop accepting and spawning connection handlers.
    ///
    /// # Architecture
    /// ```text
    /// ┌─────────────┐
    /// │TcpListener  │ ← accept()
    /// └──────┬──────┘
    ///        │ spawn for each connection
    ///        ↓
    /// ┌──────────────────┐      ┌─────────────────┐
    /// │ConnectionHandler │ ───► │Event Dispatcher │ ← optional
    /// └──────────────────┘      └─────────────────┘
    /// ```
    ///
    /// # Performance Optimizations
    /// 1. **Permit acquisition before accept**: Backpressure at OS level
    /// 2. **TCP_NODELAY**: Disable Nagle's algorithm for low latency
    /// 3. **Event channel buffering**: Prevent blocking on event dispatch
    /// 4. **Arc reuse**: cmd_handler cloned once per connection, not per message
    ///
    /// # Concurrency
    /// - Accept loop: Single-threaded (TcpListener)
    /// - Handler tasks: Multi-threaded (Tokio runtime)
    /// - Event dispatcher: Independent task (non-blocking)
    async fn run(&mut self) -> anyhow::Result<()> {
        info!("Server ready to accept connections");

        // Event notification channel (unbounded to prevent accept() blocking)
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<TokioEvent>();

        // Spawn event dispatcher task if listener configured
        if let Some(listener) = self.channel_event_listener.take() {
            tokio::spawn(async move {
                while let Some(event) = event_rx.recv().await {
                    let addr = event.remote_addr();
                    let addr_str = addr.to_string();

                    // HOT PATH: Match on event type and dispatch to listener
                    match event.type_() {
                        ConnectionNetEvent::CONNECTED(_) => {
                            listener.on_channel_connect(&addr_str, event.channel());
                        }
                        ConnectionNetEvent::DISCONNECTED => {
                            listener.on_channel_close(&addr_str, event.channel());
                        }
                        ConnectionNetEvent::EXCEPTION => {
                            listener.on_channel_exception(&addr_str, event.channel());
                        }
                        ConnectionNetEvent::IDLE => {
                            listener.on_channel_idle(&addr_str, event.channel());
                        }
                    }
                }
                info!("Event dispatcher task terminated");
            });
        }

        // Main accept loop
        loop {
            // OPTIMIZATION: Acquire permit BEFORE accept() to provide backpressure
            // If at capacity, accept() won't be called until a slot frees up
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .expect("Semaphore closed unexpectedly");

            // Accept next connection (with exponential backoff on errors)
            let (socket, remote_addr) = self.accept().await?;

            // OPTIMIZATION: Enable TCP_NODELAY for low-latency RPC
            // Disables Nagle's algorithm to send small packets immediately
            if let Err(e) = socket.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY for {}: {}", remote_addr, e);
            }

            let local_addr = socket.local_addr()?;
            info!("Accepted connection: {} → {}", remote_addr, local_addr);

            // Create connection channel wrapper
            let channel_inner = ArcMut::new(ChannelInner::new(
                Connection::new(socket),
                self.cmd_handler.response_table.clone(),
            ));
            let channel = Channel::new(channel_inner, local_addr, remote_addr);

            // Notify CONNECTED event
            let _ = event_tx.send(TokioEvent::new(
                ConnectionNetEvent::CONNECTED(remote_addr),
                remote_addr,
                channel.clone(),
            ));

            // Build connection handler
            let idle_timeout = Duration::from_secs(DEFAULT_CHANNEL_IDLE_TIMEOUT_SECONDS);
            let handler = ConnectionHandler {
                connection_handler_context: ArcMut::new(ConnectionHandlerContextWrapper {
                    channel: channel.clone(),
                }),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                conn_disconnect_notify: self.conn_disconnect_notify.clone(),
                cmd_handler: self.cmd_handler.clone(),
                event_tx: Some(event_tx.clone()),
                idle_timeout,
            };

            // Spawn dedicated task for this connection
            let event_tx_clone = event_tx.clone();
            tokio::spawn(async move {
                let mut handler = handler;

                // Run handler until completion
                if let Err(err) = handler.handle().await {
                    error!(
                        remote_addr = %remote_addr,
                        error = ?err,
                        "Connection handler terminated with error"
                    );
                }

                // Notify DISCONNECTED event
                let _ = event_tx_clone.send(TokioEvent::new(
                    ConnectionNetEvent::DISCONNECTED,
                    remote_addr,
                    handler.connection_handler_context.channel.clone(),
                ));

                info!("Client {} disconnected", remote_addr);

                // IMPORTANT: Permit released when `permit` drops here
                drop(permit);
            });
        }
    }

    /// Accept new TCP connection with exponential backoff on transient errors.
    ///
    /// # Error Handling Strategy
    /// - **Fatal errors** (e.g., listener closed): Return immediately
    /// - **Transient errors** (e.g., too many open files): Retry with backoff
    /// - **Max retries**: Give up after backoff reaches 64 seconds
    ///
    /// # Backoff Schedule
    /// ```text
    /// Attempt | Delay
    /// --------|-------
    /// 1       | 1s
    /// 2       | 2s
    /// 3       | 4s
    /// 4       | 8s
    /// 5       | 16s
    /// 6       | 32s
    /// 7       | 64s (final)
    /// ```
    ///
    /// # Performance
    /// - Fast path: Single syscall when no errors
    /// - Slow path: Exponential backoff prevents thundering herd
    async fn accept(&mut self) -> anyhow::Result<(TcpStream, SocketAddr)> {
        let mut backoff = 1;
        const MAX_BACKOFF: u64 = 64;

        loop {
            match self.listener.accept().await {
                Ok((socket, remote_addr)) => {
                    // Fast path: successful accept
                    return Ok((socket, remote_addr));
                }
                Err(err) => {
                    if backoff > MAX_BACKOFF {
                        // Exceeded retry limit - fatal error
                        error!("Accept failed after {} retries, last error: {}", MAX_BACKOFF, err);
                        return Err(err.into());
                    }

                    // Log transient error and retry
                    warn!("Accept error (will retry in {}s): {}", backoff, err);
                }
            }

            // Exponential backoff before retry
            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

pub struct RocketMQServer<RP> {
    config: Arc<ServerConfig>,
    rpc_hooks: Option<Vec<Arc<dyn RPCHook>>>,
    _phantom_data: std::marker::PhantomData<RP>,
}

impl<RP> RocketMQServer<RP> {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            config,
            rpc_hooks: Some(vec![]),
            _phantom_data: std::marker::PhantomData,
        }
    }

    pub fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>) {
        if let Some(ref mut hooks) = self.rpc_hooks {
            hooks.push(hook);
        } else {
            self.rpc_hooks = Some(vec![hook]);
        }
    }
}

impl<RP: RequestProcessor + Sync + 'static + Clone> RocketMQServer<RP> {
    pub async fn run(&mut self, request_processor: RP, channel_event_listener: Option<Arc<dyn ChannelEventListener>>) {
        let addr = format!("{}:{}", self.config.bind_address, self.config.listen_port);
        let listener = TcpListener::bind(&addr).await.unwrap();
        let rpc_hooks = self.rpc_hooks.take().unwrap_or_default();
        info!("Starting remoting_server at: {}", addr);
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        run(
            listener,
            wait_for_signal(),
            request_processor,
            Some(notify_conn_disconnect),
            rpc_hooks,
            channel_event_listener,
        )
        .await;
    }
}

pub async fn run<RP: RequestProcessor + Sync + 'static + Clone>(
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    // Initialize the connection listener state
    let handler = RemotingGeneralHandler {
        request_processor,
        //shutdown: Shutdown::new(notify_shutdown.subscribe()),
        rpc_hooks,
        response_table: ArcMut::new(HashMap::with_capacity(512)),
    };
    let mut listener = ConnectionListener {
        listener,
        notify_shutdown,
        shutdown_complete_tx,
        conn_disconnect_notify,
        limit_connections: Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
        channel_event_listener,
        cmd_handler: ArcMut::new(handler),
    };

    tokio::select! {
        res = listener.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the remoting_server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("Shutdown now.....");
        }
    }

    let ConnectionListener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = listener;
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received
    is_shutdown: bool,

    /// The receive half of the channel used to listen for shutdown.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.is_shutdown {
            return;
        }

        // Cannot receive a "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;

        // Remember that the signal has been received.
        self.is_shutdown = true;
    }
}
