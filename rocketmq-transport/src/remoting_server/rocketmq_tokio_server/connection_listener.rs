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

use super::connection_handler::ConnectionHandler;
use super::connection_handler::InterceptingConnectionHandler;
use super::connection_handler::SessionCommandInterceptor;
use super::lifecycle_events::run_lifecycle_event_dispatcher;
use super::lifecycle_events::LifecycleEventConfig;
use super::lifecycle_events::LifecycleEventPublisher;
use super::*;

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
pub(super) struct ConnectionListener<RP> {
    /// TCP socket acceptor bound to server address
    pub(super) listener: Option<TcpListener>,

    /// Semaphore controlling max concurrent connections
    ///
    /// Permits acquired before accept, released on handler drop.
    /// Provides backpressure when server reaches capacity.
    /// Completion coordination channel
    ///
    /// Each handler holds a clone of this sender.
    /// When all handlers drop (server fully shutdown), receiver unblocks.
    pub(super) shutdown_complete_tx: mpsc::Sender<()>,

    /// Optional connection disconnect broadcaster
    ///
    /// Used for routing table cleanup and metrics.
    pub(super) conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,

    /// Optional lifecycle event listener
    ///
    /// Receives CONNECTED/DISCONNECTED/EXCEPTION events.
    /// Useful for external monitoring and orchestration.
    pub(super) channel_event_listener: Option<Arc<dyn ChannelEventListener>>,

    /// Shared command processing handler
    ///
    /// Contains request processor, RPC hooks, and response routing table.
    /// Arc-wrapped to share across all connection handlers efficiently.
    pub(super) dispatcher: Arc<AuthorizedCommandDispatcher<RP>>,

    /// TLS mode and acceptor state for newly accepted connections.
    pub(super) tls_runtime: TlsServerRuntime,

    /// Tracks remoting event and connection tasks for shutdown diagnostics.
    pub(super) task_group: TaskGroup,

    pub(super) file_region_blocking: BlockingExecutor,
    pub(super) file_transfer_mode: FileTransferMode,
    pub(super) frame_limits: FrameLimits,
    pub(super) proxy_protocol: ProxyProtocolConfig,

    pub(super) transport_principal: Option<Principal>,
    pub(super) command_interceptor: Arc<dyn SessionCommandInterceptor>,
    pub(super) telemetry: TransportTelemetry,
    pub(super) lifecycle_event_config: LifecycleEventConfig,
    pub(super) lifecycle_shutdown: CancellationToken,
    pub(super) lifecycle_dispatcher_task: Option<TaskId>,
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
    pub(super) async fn run(&mut self) -> RocketMQResult<()> {
        info!("Server ready to accept connections");

        let event_publisher = if let Some(listener) = self.channel_event_listener.take() {
            let (sender, receiver) = mpsc::channel(self.lifecycle_event_config.queue_capacity);
            let cancellation = self.lifecycle_shutdown.clone();
            let publisher = LifecycleEventPublisher {
                sender,
                publish_timeout: self.lifecycle_event_config.publish_timeout,
                cancellation: cancellation.clone(),
                telemetry: self.telemetry.clone(),
            };
            let spawn_result = self.task_group.spawn_service(
                "rocketmq.remoting.event_dispatcher",
                run_lifecycle_event_dispatcher(
                    receiver,
                    listener,
                    cancellation,
                    self.lifecycle_event_config,
                    self.telemetry.clone(),
                ),
            );
            match spawn_result {
                Ok(task_id) => {
                    self.lifecycle_dispatcher_task = Some(task_id);
                    Some(publisher)
                }
                Err(error) => {
                    error!(%error, "Failed to spawn remoting lifecycle event dispatcher");
                    None
                }
            }
        } else {
            None
        };

        let listener = self.listener.take().ok_or_else(|| {
            RocketMQError::network_connection_failed("remoting-server", "transport listener already started")
        })?;
        let transport = TransportListener::new(
            listener,
            self.task_group.clone(),
            self.tls_runtime.clone(),
            self.dispatcher.boundary().admission_controller(),
            DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        )
        .with_authorized_dispatch(self.dispatcher.boundary(), self.transport_principal.clone())
        .with_file_region_io(self.file_region_blocking.clone(), self.file_transfer_mode)
        .try_with_frame_limits(self.frame_limits)?
        .try_with_proxy_protocol(self.proxy_protocol.clone())?
        .with_telemetry(self.telemetry.clone());
        transport
            .run(Arc::new(InterceptingConnectionHandler {
                inner: ConnectionHandler {
                    shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                    conn_disconnect_notify: self.conn_disconnect_notify.clone(),
                    dispatcher: self.dispatcher.clone(),
                    event_publisher,
                    sessions: dashmap::DashMap::new(),
                },
                command_interceptor: self.command_interceptor.clone(),
            }))
            .await
    }
}
