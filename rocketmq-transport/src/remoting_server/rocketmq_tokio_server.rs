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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::codec::remoting_command_codec::FrameLimits;
use crate::config::ServerConfig;
use crate::dispatch::AuthorizedCommandDispatcher;
use crate::file_region::FileTransferMode;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::wait_for_signal;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_security_api::Principal;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::admission::ResourceLimit;
use crate::base::channel_event_listener::ChannelEventListener;
use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::tokio_event::TokioEvent;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::security::TransportSecurity;
use crate::server::ConnectionHandler as TransportConnectionHandler;
use crate::server::TransportListener;
use crate::telemetry::TransportTelemetry;
use crate::tls::TlsServerRuntime;

/// Default limit the max number of connections.
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
struct LifecycleEventConfig {
    queue_capacity: usize,
    publish_timeout: Duration,
    drain_timeout: Duration,
    listener_warn_threshold: Duration,
}

impl Default for LifecycleEventConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            publish_timeout: Duration::from_millis(10),
            drain_timeout: Duration::from_millis(250),
            listener_warn_threshold: Duration::from_millis(50),
        }
    }
}

impl LifecycleEventConfig {
    fn validate(self) -> RocketMQResult<Self> {
        validate_positive_config("channelEventQueueCapacity", self.queue_capacity)?;
        validate_duration_config("channelEventPublishTimeoutMillis", self.publish_timeout)?;
        validate_duration_config("channelEventDrainTimeoutMillis", self.drain_timeout)?;
        validate_duration_config("channelEventListenerWarnMillis", self.listener_warn_threshold)?;
        Ok(self)
    }
}

fn validate_positive_config(
    key: &'static str,
    value: impl TryInto<u64> + Copy + std::fmt::Display,
) -> RocketMQResult<()> {
    if value.try_into().ok().is_some_and(|value| value > 0) {
        return Ok(());
    }
    Err(RocketMQError::ConfigInvalidValue {
        key,
        value: value.to_string(),
        reason: "must be greater than zero".to_owned(),
    })
}

fn validate_duration_config(key: &'static str, value: Duration) -> RocketMQResult<()> {
    if !value.is_zero() {
        return Ok(());
    }
    Err(RocketMQError::ConfigInvalidValue {
        key,
        value: format!("{value:?}"),
        reason: "must be greater than zero".to_owned(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
enum LifecycleEventPublishOutcome {
    Queued,
    DeadlineExpired,
    DispatcherClosed,
    ShuttingDown,
}

impl LifecycleEventPublishOutcome {
    const fn metric_label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::DeadlineExpired => "deadline_expired",
            Self::DispatcherClosed => "dropped_dispatcher_closed",
            Self::ShuttingDown => "dropped_shutdown",
        }
    }

    const fn is_queued(self) -> bool {
        matches!(self, Self::Queued)
    }
}

#[derive(Clone)]
struct LifecycleEventPublisher {
    sender: mpsc::Sender<TokioEvent>,
    publish_timeout: Duration,
    cancellation: CancellationToken,
    telemetry: TransportTelemetry,
}

impl LifecycleEventPublisher {
    async fn publish(&self, event: TokioEvent) -> LifecycleEventPublishOutcome {
        let event_name = lifecycle_event_name(event.type_());
        let outcome = enqueue_lifecycle_event(&self.sender, event, self.publish_timeout, &self.cancellation).await;
        self.telemetry
            .record_lifecycle_event(event_name, outcome.metric_label());
        outcome
    }
}

async fn enqueue_lifecycle_event<T>(
    sender: &mpsc::Sender<T>,
    event: T,
    publish_timeout: Duration,
    cancellation: &CancellationToken,
) -> LifecycleEventPublishOutcome {
    if cancellation.is_cancelled() {
        return LifecycleEventPublishOutcome::ShuttingDown;
    }

    tokio::select! {
        biased;
        _ = cancellation.cancelled() => LifecycleEventPublishOutcome::ShuttingDown,
        result = tokio::time::timeout(publish_timeout, sender.send(event)) => match result {
            Ok(Ok(())) => LifecycleEventPublishOutcome::Queued,
            Ok(Err(_)) => LifecycleEventPublishOutcome::DispatcherClosed,
            Err(_) => LifecycleEventPublishOutcome::DeadlineExpired,
        },
    }
}

const fn lifecycle_event_name(event: &ConnectionNetEvent) -> &'static str {
    match event {
        ConnectionNetEvent::CONNECTED(_) => "connected",
        ConnectionNetEvent::DISCONNECTED => "disconnected",
        ConnectionNetEvent::EXCEPTION => "exception",
        ConnectionNetEvent::IDLE => "idle",
    }
}

async fn run_lifecycle_event_dispatcher(
    mut receiver: mpsc::Receiver<TokioEvent>,
    listener: Arc<dyn ChannelEventListener>,
    cancellation: CancellationToken,
    config: LifecycleEventConfig,
    telemetry: TransportTelemetry,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            event = receiver.recv() => match event {
                Some(event) => dispatch_lifecycle_event(
                    listener.as_ref(),
                    event,
                    config.listener_warn_threshold,
                    &telemetry,
                ),
                None => {
                    info!("Remoting lifecycle event dispatcher closed");
                    return;
                }
            },
        }
    }

    let drain_deadline = Instant::now() + config.drain_timeout;
    while Instant::now() < drain_deadline {
        let Ok(event) = receiver.try_recv() else {
            break;
        };
        dispatch_lifecycle_event(listener.as_ref(), event, config.listener_warn_threshold, &telemetry);
    }

    let dropped = receiver.len();
    for _ in 0..dropped {
        telemetry.record_lifecycle_event("pending", "dropped_drain_deadline");
    }
    if dropped > 0 {
        warn!(dropped, "Remoting lifecycle event drain deadline expired");
    }
    receiver.close();
    info!("Remoting lifecycle event dispatcher terminated");
}

fn dispatch_lifecycle_event(
    listener: &dyn ChannelEventListener,
    event: TokioEvent,
    listener_warn_threshold: Duration,
    telemetry: &TransportTelemetry,
) {
    let event_name = lifecycle_event_name(event.type_());
    let addr = event.remote_addr().to_string();
    let started = Instant::now();
    match event.type_() {
        ConnectionNetEvent::CONNECTED(_) => listener.on_channel_connect(&addr, event.channel()),
        ConnectionNetEvent::DISCONNECTED => listener.on_channel_close(&addr, event.channel()),
        ConnectionNetEvent::EXCEPTION => listener.on_channel_exception(&addr, event.channel()),
        ConnectionNetEvent::IDLE => listener.on_channel_idle(&addr, event.channel()),
    }
    let elapsed = started.elapsed();
    telemetry.record_lifecycle_event(event_name, "delivered");
    telemetry.record_lifecycle_listener_latency(elapsed, event_name);
    if elapsed >= listener_warn_threshold {
        warn!(
            event = event_name,
            elapsed_ms = elapsed.as_millis(),
            "Slow remoting lifecycle listener callback"
        );
    }
}

#[cfg(all(test, not(doctest)))]
enum TestRequestHookResult {
    Continue,
    Intercept,
}

#[cfg(all(test, not(doctest)))]
type TestDeferredResponse = Box<
    dyn FnOnce(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send,
>;

#[cfg(all(test, not(doctest)))]
type TestRequestHook =
    Arc<dyn Fn(i32, i32, Channel, TaskGroup, TestDeferredResponse) -> TestRequestHookResult + Send + Sync>;

trait SessionCommandInterceptor: Send + Sync + 'static {
    fn intercept(&self, code: i32, opaque: i32, channel: Channel, request_executor_group: TaskGroup) -> bool;
}

impl SessionCommandInterceptor for () {
    fn intercept(&self, _code: i32, _opaque: i32, _channel: Channel, _request_executor_group: TaskGroup) -> bool {
        false
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
    listener: Option<TcpListener>,

    /// Semaphore controlling max concurrent connections
    ///
    /// Permits acquired before accept, released on handler drop.
    /// Provides backpressure when server reaches capacity.
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
    dispatcher: Arc<AuthorizedCommandDispatcher<RP>>,

    /// TLS mode and acceptor state for newly accepted connections.
    tls_runtime: TlsServerRuntime,

    /// Tracks remoting event and connection tasks for shutdown diagnostics.
    task_group: TaskGroup,

    file_region_blocking: BlockingExecutor,
    file_transfer_mode: FileTransferMode,
    frame_limits: FrameLimits,

    transport_principal: Option<Principal>,
    command_interceptor: Arc<dyn SessionCommandInterceptor>,
    telemetry: TransportTelemetry,
    lifecycle_event_config: LifecycleEventConfig,
    lifecycle_shutdown: CancellationToken,
    lifecycle_dispatcher_task: Option<TaskId>,
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
    async fn run(&mut self) -> RocketMQResult<()> {
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

struct ConnectionHandler<RP> {
    shutdown_complete_tx: mpsc::Sender<()>,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    dispatcher: Arc<AuthorizedCommandDispatcher<RP>>,
    event_publisher: Option<LifecycleEventPublisher>,
    sessions: dashmap::DashMap<u64, RemotingSession<ConnectionHandlerContext>>,
}

struct InterceptingConnectionHandler<RP> {
    inner: ConnectionHandler<RP>,
    command_interceptor: Arc<dyn SessionCommandInterceptor>,
}

struct RemotingSession<C> {
    context: C,
    _shutdown_complete: mpsc::Sender<()>,
}

enum RemotingSessionAction {
    Connect,
    Command(rocketmq_protocol::protocol::remoting_command::RemotingCommand),
}

impl<RP: RequestProcessor + Sync + Clone + 'static> ConnectionHandler<RP> {
    async fn run(
        &self,
        session: crate::server::SessionHandle,
        action: RemotingSessionAction,
        command_interceptor: Option<&dyn SessionCommandInterceptor>,
    ) {
        let channel_id = match &action {
            RemotingSessionAction::Connect => format!("transport-session-{}", session.session_id()),
            RemotingSessionAction::Command(_) => {
                let Some(channel_id) = self
                    .sessions
                    .get(&session.session_id())
                    .map(|remoting_session| remoting_session.context.channel().channel_id().to_owned())
                else {
                    return;
                };
                channel_id
            }
        };
        let channel_inner = match &action {
            RemotingSessionAction::Connect => ChannelInner::new_transport_session(
                session.connection(),
                self.dispatcher.response_table(),
                session.task_group().clone(),
            ),
            RemotingSessionAction::Command(_) => ChannelInner::new_transport_session_with_task_group(
                session.connection(),
                self.dispatcher.response_table(),
                session.task_group().clone(),
            ),
        };
        let Ok(channel_inner) = channel_inner else {
            return;
        };
        let mut channel = Channel::new(Arc::new(channel_inner), session.local_addr(), session.remote_addr());
        channel.set_channel_id(channel_id);
        let remoting_session = RemotingSession {
            context: Arc::new(ConnectionHandlerContextWrapper::new(channel)),
            _shutdown_complete: self.shutdown_complete_tx.clone(),
        };
        match action {
            RemotingSessionAction::Connect => {
                let event_channel = remoting_session.context.channel().clone();
                self.sessions.insert(session.session_id(), remoting_session);
                if let Some(publisher) = &self.event_publisher {
                    let outcome = publisher
                        .publish(TokioEvent::new(
                            ConnectionNetEvent::CONNECTED(session.remote_addr()),
                            session.remote_addr(),
                            event_channel,
                        ))
                        .await;
                    if !outcome.is_queued() {
                        warn!(?outcome, event = "connected", "Remoting lifecycle event was not queued");
                    }
                }
            }
            RemotingSessionAction::Command(command) => {
                if let Some(command_interceptor) = command_interceptor {
                    if command_interceptor.intercept(
                        command.code(),
                        command.opaque(),
                        remoting_session.context.channel().clone(),
                        session.request_executor_group().clone(),
                    ) {
                        return;
                    }
                }
                let dispatcher = self.dispatcher.clone();
                dispatcher.process_network(&remoting_session.context, command).await;
            }
        }
    }
}

impl<RP: RequestProcessor + Sync + Clone + 'static> TransportConnectionHandler for ConnectionHandler<RP> {
    fn connected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.run(session, RemotingSessionAction::Connect, None).await;
        })
    }

    fn request_ordering(
        &self,
        command: &rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> crate::request_ordering::RequestOrdering {
        self.dispatcher.request_ordering(command)
    }

    fn command(
        &self,
        session: crate::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.run(session, RemotingSessionAction::Command(command), None).await;
        })
    }

    fn disconnected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let event_publisher = self.event_publisher.clone();
        let conn_disconnect_notify = self.conn_disconnect_notify.clone();
        Box::pin(async move {
            let Some((_, remoting_session)) = self.sessions.remove(&session.session_id()) else {
                return;
            };
            let channel_report = remoting_session
                .context
                .channel()
                .close_with_report(Duration::from_secs(3))
                .await;
            channel_report.log_if_unhealthy();
            if let Some(notify) = conn_disconnect_notify {
                let _ = notify.send(session.remote_addr());
            }
            if let Some(publisher) = event_publisher {
                let outcome = publisher
                    .publish(TokioEvent::new(
                        ConnectionNetEvent::DISCONNECTED,
                        session.remote_addr(),
                        remoting_session.context.channel().clone(),
                    ))
                    .await;
                if !outcome.is_queued() {
                    warn!(
                        ?outcome,
                        event = "disconnected",
                        "Remoting lifecycle event was not queued"
                    );
                }
            }
        })
    }
}

impl<RP: RequestProcessor + Sync + Clone + 'static> TransportConnectionHandler for InterceptingConnectionHandler<RP> {
    fn connected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.connected(session)
    }

    fn request_ordering(
        &self,
        command: &rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> crate::request_ordering::RequestOrdering {
        self.inner.request_ordering(command)
    }

    fn command(
        &self,
        session: crate::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.inner
                .run(
                    session,
                    RemotingSessionAction::Command(command),
                    Some(self.command_interceptor.as_ref()),
                )
                .await;
        })
    }

    fn disconnected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.disconnected(session)
    }
}

pub struct TransportServer<RP> {
    config: Arc<ServerConfig>,
    rpc_hooks: Option<Vec<Arc<dyn RPCHook>>>,
    service_context: ChildServiceContext,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
    admission: Option<Arc<AdmissionController>>,
    authorized_dispatcher: Option<Arc<AuthorizedCommandDispatcher<RP>>>,
    telemetry: TransportTelemetry,
    lifecycle_event_config: LifecycleEventConfig,
    frame_limits: FrameLimits,
    #[cfg(all(test, not(doctest)))]
    test_request_hook: Option<TestRequestHook>,
    _phantom_data: std::marker::PhantomData<RP>,
}

impl<RP> TransportServer<RP> {
    pub fn new(config: Arc<ServerConfig>, service_context: ChildServiceContext) -> Self {
        Self {
            config,
            rpc_hooks: Some(vec![]),
            service_context,
            transport_security: None,
            transport_principal: None,
            admission: None,
            authorized_dispatcher: None,
            telemetry: TransportTelemetry::noop(),
            lifecycle_event_config: LifecycleEventConfig::default(),
            frame_limits: FrameLimits::java_compatibility(),
            #[cfg(all(test, not(doctest)))]
            test_request_hook: None,
            _phantom_data: std::marker::PhantomData,
        }
    }

    pub fn new_with_service_context(config: Arc<ServerConfig>, service_context: ChildServiceContext) -> Self {
        Self::new(config, service_context)
    }

    /// Creates a remoting server bound to one explicit transport telemetry instance.
    pub fn new_with_telemetry(
        config: Arc<ServerConfig>,
        service_context: ChildServiceContext,
        telemetry: TransportTelemetry,
    ) -> Self {
        Self {
            telemetry,
            ..Self::new(config, service_context)
        }
    }

    /// Replaces the no-op transport recorder before the server starts.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies one validated frame profile to every accepted connection.
    pub fn try_with_frame_limits(mut self, frame_limits: FrameLimits) -> RocketMQResult<Self> {
        frame_limits.validate()?;
        self.frame_limits = frame_limits;
        Ok(self)
    }

    pub fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>) {
        if let Some(ref mut hooks) = self.rpc_hooks {
            hooks.push(hook);
        } else {
            self.rpc_hooks = Some(vec![hook]);
        }
    }

    /// Installs transport authorization for accepted sessions.
    pub fn with_transport_security(
        mut self,
        transport_security: Arc<TransportSecurity>,
        principal: Option<Principal>,
    ) -> Self {
        self.transport_security = Some(transport_security);
        self.transport_principal = principal;
        self
    }

    #[doc(hidden)]
    pub fn with_admission_controller(mut self, admission: Arc<AdmissionController>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Installs one dispatcher shared with another trusted entry adapter.
    #[doc(hidden)]
    pub fn with_authorized_dispatcher(mut self, dispatcher: Arc<AuthorizedCommandDispatcher<RP>>) -> Self {
        self.authorized_dispatcher = Some(dispatcher);
        self
    }

    #[cfg(all(test, not(doctest)))]
    fn with_test_request_hook(mut self, hook: TestRequestHook) -> Self {
        self.test_request_hook = Some(hook);
        self
    }
}

impl<RP: RequestProcessor + Sync + 'static + Clone> TransportServer<RP> {
    pub async fn run(&mut self, request_processor: RP, channel_event_listener: Option<Arc<dyn ChannelEventListener>>) {
        self.run_with_shutdown(request_processor, channel_event_listener, wait_for_signal())
            .await;
    }

    pub async fn run_with_shutdown<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) where
        S: Future,
    {
        let _ = self
            .run_with_shutdown_report(request_processor, channel_event_listener, shutdown)
            .await;
    }

    /// Serves an already-bound listener through the canonical server runtime.
    ///
    /// This entry point is intended for composition roots that must bind the
    /// socket themselves in order to publish an exact readiness transition.
    pub async fn serve_bound_listener_until<S>(
        &mut self,
        listener: TcpListener,
        request_processor: RP,
        conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        let lifecycle_event_config = match self.lifecycle_event_config.validate() {
            Ok(config) => config,
            Err(error) => {
                error!(%error, "invalid remoting lifecycle event configuration");
                return None;
            }
        };
        let rpc_hooks = self.rpc_hooks.take().unwrap_or_default();
        let remoting_context = new_remoting_server_context(&self.service_context);
        let tls_runtime =
            match TlsServerRuntime::initialize_with_service_context(self.config.tls_config.clone(), &remoting_context)
                .await
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    error!(%error, "failed to initialize remoting server TLS runtime");
                    return None;
                }
            };
        #[cfg(all(test, not(doctest)))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(self.test_request_hook.clone());
        #[cfg(not(test))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(());
        run_with_tls_config_report(
            listener,
            shutdown,
            request_processor,
            conn_disconnect_notify,
            rpc_hooks,
            channel_event_listener,
            self.authorized_dispatcher.clone(),
            RemotingServerRunCapabilities {
                tls_runtime,
                task_group: remoting_context.task_group().clone(),
                file_region_blocking: remoting_context.storage_io().clone(),
                file_transfer_mode: self.config.file_transfer_mode,
                frame_limits: self.frame_limits,
                process_budget: self.service_context.process_budget(),
                transport_security: self.transport_security.clone(),
                transport_principal: self.transport_principal.clone(),
                admission: self.admission.clone(),
                command_interceptor,
                telemetry: self.telemetry.clone(),
                lifecycle_event_config,
            },
        )
        .await
    }

    #[doc(hidden)]
    pub async fn run_with_shutdown_report<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        self.run_with_shutdown_report_inner(request_processor, channel_event_listener, shutdown, None)
            .await
    }

    /// Runs the server and reports whether its listener is ready before entering the accept loop.
    ///
    /// The startup signal is sent only after the socket is bound and the remoting runtime and TLS
    /// state have been initialized. This prevents lifecycle owners from treating a spawned server
    /// task as a bound, production-ready listener.
    #[doc(hidden)]
    pub async fn run_with_shutdown_report_and_startup<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        startup: oneshot::Sender<RocketMQResult<SocketAddr>>,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        self.run_with_shutdown_report_inner(request_processor, channel_event_listener, shutdown, Some(startup))
            .await
    }

    async fn run_with_shutdown_report_inner<S>(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
        shutdown: S,
        mut startup: Option<oneshot::Sender<RocketMQResult<SocketAddr>>>,
    ) -> Option<ShutdownReport>
    where
        S: Future,
    {
        let lifecycle_event_config = match self.lifecycle_event_config.validate() {
            Ok(config) => config,
            Err(error) => {
                error!(%error, "Invalid remoting lifecycle event configuration");
                notify_server_startup(&mut startup, Err(error));
                return None;
            }
        };
        let addr = format!("{}:{}", self.config.bind_address, self.config.listen_port);
        let listener = match TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(err) => {
                error!(addr = %addr, error = %err, "failed to bind remoting_server");
                notify_server_startup(
                    &mut startup,
                    Err(RocketMQError::network_connection_failed(
                        "remoting-server-bind",
                        format!("{addr}: {err}"),
                    )),
                );
                return None;
            }
        };
        let local_addr = match listener.local_addr() {
            Ok(local_addr) => local_addr,
            Err(error) => {
                error!(addr = %addr, %error, "failed to read bound remoting server address");
                notify_server_startup(
                    &mut startup,
                    Err(RocketMQError::network_connection_failed(
                        "remoting-server-local-address",
                        format!("{addr}: {error}"),
                    )),
                );
                return None;
            }
        };
        let rpc_hooks = self.rpc_hooks.take().unwrap_or_default();
        let remoting_context = new_remoting_server_context(&self.service_context);
        let task_group = remoting_context.task_group().clone();
        let tls_runtime =
            match TlsServerRuntime::initialize_with_service_context(self.config.tls_config.clone(), &remoting_context)
                .await
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    error!(%error, "failed to initialize remoting server TLS runtime");
                    notify_server_startup(
                        &mut startup,
                        Err(RocketMQError::network_connection_failed(
                            "remoting-server-tls",
                            error.to_string(),
                        )),
                    );
                    return None;
                }
            };
        info!("Starting remoting_server at: {}", addr);
        notify_server_startup(&mut startup, Ok(local_addr));
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        #[cfg(all(test, not(doctest)))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(self.test_request_hook.clone());
        #[cfg(not(test))]
        let command_interceptor: Arc<dyn SessionCommandInterceptor> = Arc::new(());
        run_with_tls_config_report(
            listener,
            shutdown,
            request_processor,
            Some(notify_conn_disconnect),
            rpc_hooks,
            channel_event_listener,
            self.authorized_dispatcher.clone(),
            RemotingServerRunCapabilities {
                tls_runtime,
                task_group,
                file_region_blocking: remoting_context.storage_io().clone(),
                file_transfer_mode: self.config.file_transfer_mode,
                frame_limits: self.frame_limits,
                process_budget: self.service_context.process_budget(),
                transport_security: self.transport_security.clone(),
                transport_principal: self.transport_principal.clone(),
                admission: self.admission.clone(),
                command_interceptor,
                telemetry: self.telemetry.clone(),
                lifecycle_event_config,
            },
        )
        .await
    }
}

fn notify_server_startup(
    startup: &mut Option<oneshot::Sender<RocketMQResult<SocketAddr>>>,
    result: RocketMQResult<SocketAddr>,
) {
    if let Some(startup) = startup.take() {
        let _ = startup.send(result);
    }
}

#[cfg(test)]
async fn run_with_report<RP: RequestProcessor + Sync + 'static + Clone>(
    service_context: ChildServiceContext,
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
) -> Option<ShutdownReport> {
    run_with_report_with_service_context(
        service_context,
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
    )
    .await
}

#[cfg(test)]
async fn run_with_report_with_service_context<RP: RequestProcessor + Sync + 'static + Clone>(
    service_context: ChildServiceContext,
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
) -> Option<ShutdownReport> {
    run_with_report_with_service_context_and_telemetry(
        service_context,
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
        TransportTelemetry::noop(),
    )
    .await
}

/// Runs a remoting server under an explicit service context and transport telemetry instance.
///
/// The supplied telemetry capability is propagated to accepted connections, derived channels,
/// request metrics guards, and request tracing spans.
// These arguments are independent composition capabilities owned by the remoting server runtime.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn run_with_report_with_service_context_and_telemetry<RP: RequestProcessor + Sync + 'static + Clone>(
    service_context: ChildServiceContext,
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    telemetry: TransportTelemetry,
) -> Option<ShutdownReport> {
    let remoting_context = new_remoting_server_context(&service_context);
    let tls_runtime =
        match TlsServerRuntime::initialize_with_service_context(Default::default(), &remoting_context).await {
            Ok(runtime) => runtime,
            Err(error) => {
                error!(%error, "failed to initialize remoting server TLS runtime");
                return None;
            }
        };
    run_with_tls_config_report(
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
        None,
        RemotingServerRunCapabilities {
            tls_runtime,
            task_group: remoting_context.task_group().clone(),
            file_region_blocking: remoting_context.storage_io().clone(),
            file_transfer_mode: FileTransferMode::Auto,
            frame_limits: FrameLimits::java_compatibility(),
            process_budget: service_context.process_budget(),
            transport_security: None,
            transport_principal: None,
            admission: None,
            command_interceptor: Arc::new(()),
            telemetry,
            lifecycle_event_config: LifecycleEventConfig::default(),
        },
    )
    .await
}

struct RemotingServerRunCapabilities {
    tls_runtime: TlsServerRuntime,
    task_group: TaskGroup,
    file_region_blocking: BlockingExecutor,
    file_transfer_mode: FileTransferMode,
    frame_limits: FrameLimits,
    process_budget: rocketmq_runtime::ResourceBudget,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
    admission: Option<Arc<AdmissionController>>,
    command_interceptor: Arc<dyn SessionCommandInterceptor>,
    telemetry: TransportTelemetry,
    lifecycle_event_config: LifecycleEventConfig,
}

async fn run_with_tls_config_report<RP: RequestProcessor + Sync + 'static + Clone>(
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    authorized_dispatcher: Option<Arc<AuthorizedCommandDispatcher<RP>>>,
    capabilities: RemotingServerRunCapabilities,
) -> Option<ShutdownReport> {
    let RemotingServerRunCapabilities {
        tls_runtime,
        task_group,
        file_region_blocking,
        file_transfer_mode,
        frame_limits,
        process_budget,
        transport_security,
        transport_principal,
        admission,
        command_interceptor,
        telemetry,
        lifecycle_event_config,
    } = capabilities;
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let lifecycle_shutdown = CancellationToken::new();
    let mut admission_limits = AdmissionLimits::default();
    admission_limits.connections = ResourceLimit {
        count: DEFAULT_MAX_CONNECTIONS,
        ..admission_limits.connections
    };
    admission_limits.handshakes = ResourceLimit {
        count: DEFAULT_MAX_CONNECTIONS,
        ..admission_limits.handshakes
    };
    let admission = match authorized_dispatcher.as_ref() {
        Some(dispatcher) => dispatcher.boundary().admission_controller(),
        None => match admission {
            Some(admission) => admission,
            None => match AdmissionController::try_new_with_budget(admission_limits, &process_budget) {
                Ok(admission) => Arc::new(admission),
                Err(error) => {
                    error!(%error, "failed to initialize transport admission budgets");
                    return None;
                }
            },
        },
    };
    let dispatcher = match authorized_dispatcher {
        Some(dispatcher) => dispatcher,
        None => {
            let security = transport_security
                .unwrap_or_else(|| Arc::new(TransportSecurity::development_insecure_loopback(None, None)));
            match AuthorizedCommandDispatcher::try_new(
                request_processor,
                rpc_hooks,
                &process_budget,
                telemetry.clone(),
                security,
                admission,
            ) {
                Ok(dispatcher) => Arc::new(dispatcher),
                Err(error) => {
                    error!(%error, "failed to initialize authorized command dispatcher");
                    return None;
                }
            }
        }
    };
    let mut listener = ConnectionListener {
        listener: Some(listener),
        shutdown_complete_tx,
        conn_disconnect_notify,
        channel_event_listener,
        dispatcher,
        tls_runtime,
        task_group: task_group.clone(),
        file_region_blocking,
        file_transfer_mode,
        frame_limits,
        transport_principal,
        command_interceptor,
        telemetry,
        lifecycle_event_config,
        lifecycle_shutdown: lifecycle_shutdown.clone(),
        lifecycle_dispatcher_task: None,
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
        tls_runtime,
        lifecycle_dispatcher_task,
        ..
    } = listener;
    let deadline = task_group
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(30)));
    task_group.cancel();
    drop(shutdown_complete_tx);
    let _ = tokio::time::timeout(deadline.remaining(), shutdown_complete_rx.recv()).await;

    lifecycle_shutdown.cancel();
    if let Some(task_id) = lifecycle_dispatcher_task {
        if !task_group.wait_task(task_id, deadline.remaining()).await {
            warn!(
                task_id = task_id.as_u64(),
                "Remoting lifecycle event dispatcher did not drain before shutdown deadline"
            );
        }
    }

    let tls_report = tls_runtime
        .shutdown_gracefully(deadline.remaining().min(Duration::from_secs(3)))
        .await;
    if let Some(report) = tls_report.as_ref() {
        report.log_if_unhealthy();
    }
    let mut report = task_group.shutdown_until(deadline).await;
    if let Some(tls_report) = tls_report {
        report.children.push(tls_report);
    }
    report.log_if_unhealthy();
    Some(report)
}

fn new_remoting_server_context(context: &ChildServiceContext) -> ChildServiceContext {
    context.component("rocketmq.remoting.server")
}

#[cfg(test)]
fn new_remoting_server_task_group_with_service_context(context: &ChildServiceContext) -> TaskGroup {
    new_remoting_server_context(context).task_group().clone()
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::Arc;

    use crate::config::ServerConfig;
    #[cfg(feature = "tls")]
    use crate::config::TlsConfig;
    #[cfg(feature = "tls")]
    use crate::config::TlsMode;
    #[cfg(feature = "tls")]
    use crate::config::TlsServerConfig;
    use rocketmq_runtime::RuntimeContext;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;
    use crate::clients::rocketmq_tokio_client::TransportClient;
    use crate::request_processor::default_request_processor::DefaultRequestProcessor;
    use crate::runtime::config::client_config::TransportClientConfig;

    impl SessionCommandInterceptor for Option<TestRequestHook> {
        fn intercept(&self, code: i32, opaque: i32, channel: Channel, request_executor_group: TaskGroup) -> bool {
            let Some(hook) = self.as_ref() else {
                return false;
            };
            let response_channel = channel.clone();
            let deferred_response: TestDeferredResponse = Box::new(move |response| {
                Box::pin(async move {
                    let _ = response_channel.send_command(response.set_opaque(opaque)).await;
                })
            });
            matches!(
                hook(code, opaque, channel, request_executor_group, deferred_response),
                TestRequestHookResult::Intercept
            )
        }
    }

    struct ConnectSignalListener {
        connected: std::sync::Mutex<Option<oneshot::Sender<()>>>,
        disconnected: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    }

    struct SlowLifecycleListener {
        first_delivery: std::sync::Mutex<Option<oneshot::Sender<()>>>,
        release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
        deliveries: std::sync::atomic::AtomicUsize,
    }

    struct RequireTransportSignature {
        calls: std::sync::atomic::AtomicUsize,
    }

    impl rocketmq_security_api::RequestPolicy for RequireTransportSignature {
        fn evaluate_authenticated(
            &self,
            context: rocketmq_security_api::AuthenticatedRequestContext<'_>,
        ) -> rocketmq_security_api::Decision {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if context.request().fields().contains_key("TransportSignature") {
                rocketmq_security_api::Decision::Allow
            } else {
                rocketmq_security_api::Decision::deny("missing transport signature")
            }
        }
    }

    struct RemotingMarkerSigner;

    type DelayedResponse = (String, TestDeferredResponse);

    fn test_service_context(name: &'static str) -> ChildServiceContext {
        RuntimeContext::from_current(name).service_context("remoting-server-service")
    }

    async fn await_server_startup(startup: oneshot::Receiver<RocketMQResult<SocketAddr>>) -> SocketAddr {
        tokio::time::timeout(Duration::from_secs(1), startup)
            .await
            .expect("remoting server startup acknowledgement deadline")
            .expect("remoting server startup sender")
            .expect("remoting server should bind")
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_event_queue_reports_capacity_overload_and_shutdown() {
        let (sender, receiver) = mpsc::channel(1);
        let cancellation = CancellationToken::new();

        let first = enqueue_lifecycle_event(&sender, 1_u8, Duration::from_millis(5), &cancellation).await;
        assert_eq!(first, LifecycleEventPublishOutcome::Queued);

        let overloaded = enqueue_lifecycle_event(&sender, 2_u8, Duration::from_millis(5), &cancellation).await;
        assert_eq!(overloaded, LifecycleEventPublishOutcome::DeadlineExpired);

        cancellation.cancel();
        let shutting_down = enqueue_lifecycle_event(&sender, 3_u8, Duration::from_millis(5), &cancellation).await;
        assert_eq!(shutting_down, LifecycleEventPublishOutcome::ShuttingDown);

        drop(receiver);
    }

    #[tokio::test]
    async fn lifecycle_event_queue_reports_closed_dispatcher() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);

        let outcome = enqueue_lifecycle_event(&sender, 1_u8, Duration::from_millis(5), &CancellationToken::new()).await;

        assert_eq!(outcome, LifecycleEventPublishOutcome::DispatcherClosed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_one_slow_listener_makes_overload_explicit_and_drains_queued_event() {
        let context = RuntimeContext::from_current("remoting-lifecycle-overload-test");
        let service = context.service_context("remoting-lifecycle-overload");
        let task_group = service.task_group().clone();
        let harness = crate::local::LocalRequestHarness::new(task_group.clone())
            .await
            .expect("local channel harness");
        let channel = harness.channel();
        let remote_addr = harness.remote_address();
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (first_delivery_tx, first_delivery_rx) = oneshot::channel();
        let listener = Arc::new(SlowLifecycleListener {
            first_delivery: std::sync::Mutex::new(Some(first_delivery_tx)),
            release: Arc::clone(&release),
            deliveries: std::sync::atomic::AtomicUsize::new(0),
        });
        let (sender, receiver) = mpsc::channel(1);
        let cancellation = task_group.cancellation_token();
        let config = LifecycleEventConfig {
            queue_capacity: 1,
            publish_timeout: Duration::from_millis(5),
            drain_timeout: Duration::from_millis(100),
            listener_warn_threshold: Duration::from_millis(1),
        };
        let telemetry = TransportTelemetry::noop();
        task_group
            .spawn_service(
                "remoting-lifecycle-overload-dispatcher",
                run_lifecycle_event_dispatcher(
                    receiver,
                    listener.clone(),
                    cancellation.clone(),
                    config,
                    telemetry.clone(),
                ),
            )
            .expect("event dispatcher should spawn");
        let publisher = LifecycleEventPublisher {
            sender,
            publish_timeout: config.publish_timeout,
            cancellation,
            telemetry,
        };

        assert_eq!(
            publisher
                .publish(TokioEvent::new(
                    ConnectionNetEvent::CONNECTED(remote_addr),
                    remote_addr,
                    channel.clone(),
                ))
                .await,
            LifecycleEventPublishOutcome::Queued
        );
        first_delivery_rx
            .await
            .expect("slow listener should receive the first event");
        assert_eq!(
            publisher
                .publish(TokioEvent::new(
                    ConnectionNetEvent::DISCONNECTED,
                    remote_addr,
                    channel.clone(),
                ))
                .await,
            LifecycleEventPublishOutcome::Queued
        );
        assert_eq!(
            publisher
                .publish(TokioEvent::new(
                    ConnectionNetEvent::DISCONNECTED,
                    remote_addr,
                    channel.clone(),
                ))
                .await,
            LifecycleEventPublishOutcome::DeadlineExpired
        );

        {
            let (released, condition) = release.as_ref();
            *released.lock().expect("slow listener release lock") = true;
            condition.notify_all();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while listener.deliveries.load(std::sync::atomic::Ordering::Acquire) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued lifecycle event should be delivered");

        let channel_report = channel.close_with_report(Duration::from_secs(1)).await;
        assert!(channel_report.is_healthy(), "{}", channel_report.to_json());

        task_group.cancel();
        drop(publisher);
        let report = task_group.shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(listener.deliveries.load(std::sync::atomic::Ordering::Acquire), 2);
    }

    impl rocketmq_security_api::OutboundSigner for RemotingMarkerSigner {
        fn sign(
            &self,
            _request: rocketmq_security_api::SecurityRequestView<'_>,
        ) -> Result<rocketmq_security_api::Signature, rocketmq_security_api::SigningError> {
            Ok(rocketmq_security_api::Signature::new(vec![(
                cheetah_string::CheetahString::from_static_str("TransportSignature"),
                rocketmq_security_api::Secret::new(cheetah_string::CheetahString::from_static_str("signed")),
            )]))
        }
    }

    impl ChannelEventListener for ConnectSignalListener {
        fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
            if let Some(sender) = self.connected.lock().expect("connect signal lock").take() {
                let _ = sender.send(());
            }
        }

        fn on_channel_close(&self, _remote_addr: &str, _channel: &Channel) {
            if let Some(sender) = self.disconnected.lock().expect("disconnect signal lock").take() {
                let _ = sender.send(());
            }
        }

        fn on_channel_exception(&self, _remote_addr: &str, _channel: &Channel) {}

        fn on_channel_idle(&self, _remote_addr: &str, _channel: &Channel) {}

        fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {}
    }

    impl ChannelEventListener for SlowLifecycleListener {
        fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
            let delivery = self.deliveries.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            if delivery != 0 {
                return;
            }
            if let Some(sender) = self.first_delivery.lock().expect("first delivery lock").take() {
                let _ = sender.send(());
            }
            let (released, condition) = self.release.as_ref();
            let mut released = released.lock().expect("slow listener release lock");
            while !*released {
                released = condition.wait(released).expect("slow listener release wait");
            }
        }

        fn on_channel_close(&self, _remote_addr: &str, _channel: &Channel) {
            self.deliveries.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }

        fn on_channel_exception(&self, _remote_addr: &str, _channel: &Channel) {}

        fn on_channel_idle(&self, _remote_addr: &str, _channel: &Channel) {}

        fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {}
    }

    #[tokio::test]
    async fn remoting_server_task_group_from_service_context_is_parented() {
        let context = RuntimeContext::from_current("remoting-server-context-test");
        let service = context.service_context("remoting-server-service");

        let task_group = new_remoting_server_task_group_with_service_context(&service);

        assert_eq!(task_group.parent_id(), Some(service.task_group().id()));
        assert_eq!(task_group.name(), "rocketmq.remoting.server");

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn run_with_report_returns_component_report_without_retaining_completed_child() {
        let context = RuntimeContext::from_current("remoting-server-parent-report-test");
        let service = context.service_context("remoting-server-parent");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server_task = tokio::spawn(run_with_report_with_service_context(
            service.clone(),
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRequestProcessor,
            None,
            Vec::new(),
            None,
        ));

        let _ = shutdown_tx.send(());
        let report = tokio::time::timeout(Duration::from_secs(3), server_task)
            .await
            .expect("server should shut down before timeout")
            .expect("server task should not panic")
            .expect("server should return shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.name, "rocketmq.remoting.server");

        let parent_report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
        assert!(parent_report.children.is_empty(), "{}", parent_report.to_json());
    }

    #[tokio::test]
    async fn run_with_shutdown_bind_error_returns_without_panicking() {
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: 70000,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-bind-error-test"),
        );

        server
            .run_with_shutdown(DefaultRequestProcessor, None, future::pending::<()>())
            .await;
    }

    #[tokio::test]
    async fn startup_rejects_zero_lifecycle_event_queue_capacity() {
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-invalid-event-config-test"),
        );
        server.lifecycle_event_config.queue_capacity = 0;
        let (startup_tx, startup_rx) = oneshot::channel();

        let report = server
            .run_with_shutdown_report_and_startup(DefaultRequestProcessor, None, future::pending::<()>(), startup_tx)
            .await;

        assert!(report.is_none());
        let error = startup_rx
            .await
            .expect("startup error should be reported")
            .expect_err("zero event queue capacity must be rejected");
        assert!(matches!(
            error,
            RocketMQError::ConfigInvalidValue {
                key: "channelEventQueueCapacity",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn run_with_shutdown_report_bind_error_returns_none() {
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: 70000,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-report-bind-error-test"),
        );

        let report = server
            .run_with_shutdown_report(DefaultRequestProcessor, None, future::pending::<()>())
            .await;

        assert!(report.is_none());
    }

    #[tokio::test]
    async fn startup_acknowledgement_reports_bind_failure() {
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: 70000,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-startup-failure-test"),
        );
        let (startup_tx, startup_rx) = oneshot::channel();

        let report = server
            .run_with_shutdown_report_and_startup(DefaultRequestProcessor, None, future::pending::<()>(), startup_tx)
            .await;

        assert!(report.is_none());
        let error = startup_rx
            .await
            .expect("startup acknowledgement should be sent")
            .expect_err("invalid port must fail startup");
        assert!(error.to_string().contains("remoting-server-bind"));
    }

    #[tokio::test]
    async fn startup_acknowledgement_is_sent_after_listener_binding() {
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: 0,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-startup-test"),
        );
        let (startup_tx, startup_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown_report_and_startup(
                    DefaultRequestProcessor,
                    None,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
        });

        let bound_address = startup_rx
            .await
            .expect("startup acknowledgement should be sent")
            .expect("listener should become ready");
        assert_ne!(bound_address.port(), 0);
        TcpStream::connect(bound_address)
            .await
            .expect("acknowledged listener should accept connections");

        let _ = shutdown_tx.send(());
        let report = server_task
            .await
            .expect("server task should not panic")
            .expect("server should report shutdown");
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn run_shutdown_drains_connection_tasks() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(run_with_report(
            test_service_context("remoting-server-drain-test"),
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRequestProcessor,
            None,
            Vec::new(),
            None,
        ));

        let mut clients = Vec::new();
        for _ in 0..4 {
            clients.push(TcpStream::connect(addr).await.expect("client should connect"));
        }
        drop(clients);

        let _ = shutdown_tx.send(());
        let report = tokio::time::timeout(Duration::from_secs(3), server_task)
            .await
            .expect("server should shut down before timeout")
            .expect("server task should not panic")
            .expect("server should return shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn public_client_and_server_exchange_through_canonical_transport_session() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let service = test_service_context("remoting-server-exchange-test");
        let server_task = tokio::spawn(run_with_report(
            service.clone(),
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRequestProcessor,
            None,
            Vec::new(),
            None,
        ));
        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            service.component("client"),
        );
        let remote_addr = cheetah_string::CheetahString::from_string(addr.to_string());
        let request = rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(105);
        let opaque = request.opaque();
        let response = client
            .invoke_request(Some(&remote_addr), request, 1_000)
            .await
            .expect("echo response");
        assert_eq!(response.code(), 105);
        assert_eq!(response.opaque(), opaque);

        let client_report = client.shutdown_with_report(Duration::from_secs(1)).await;
        assert!(client_report.is_healthy());
        let _ = shutdown_tx.send(());
        let report = server_task.await.unwrap().unwrap();
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn remoting_control_response_uses_reserve_when_data_writer_budget_is_full() {
        let limits = AdmissionLimits {
            queued: ResourceLimit { count: 4, bytes: 4096 },
            control_reserve: ResourceLimit { count: 2, bytes: 2048 },
            ..AdmissionLimits::default()
        };
        let admission = Arc::new(AdmissionController::new(limits));
        let scope = crate::admission::AdmissionScope::new("127.0.0.1".parse().expect("loopback address"));
        let _data_one = admission
            .try_acquire(
                crate::admission::AdmissionResource::Queued,
                scope,
                1,
                crate::admission::AdmissionClass::Data,
            )
            .unwrap();
        let _data_two = admission
            .try_acquire(
                crate::admission::AdmissionResource::Queued,
                scope,
                1,
                crate::admission::AdmissionClass::Data,
            )
            .unwrap();
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-control-reserve-test"),
        )
        .with_admission_controller(admission);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (startup_tx, startup_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown_report_and_startup(
                    DefaultRequestProcessor,
                    None,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
        });
        let addr = await_server_startup(startup_rx).await;

        let mut client = crate::connection::Connection::new(TcpStream::connect(addr).await.unwrap());
        client
            .send_command(
                rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                    rocketmq_protocol::code::request_code::RequestCode::HeartBeat,
                )
                .set_opaque(71),
            )
            .await
            .unwrap();
        let response = tokio::time::timeout(Duration::from_millis(250), client.receive_command())
            .await
            .expect("control response should consume the reserved writer budget")
            .unwrap()
            .unwrap();
        assert_eq!(
            response.code(),
            rocketmq_protocol::code::request_code::RequestCode::HeartBeat as i32
        );
        assert_eq!(response.opaque(), 71);

        let _ = shutdown_tx.send(());
        let report = server_task.await.unwrap().unwrap();
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn command_snapshots_reuse_the_fixed_request_executor_owner() {
        const COMMAND_COUNT: usize = 128;

        let request_executor_group = Arc::new(std::sync::Mutex::new(None::<TaskGroup>));
        let request_executor_group_for_hook = request_executor_group.clone();
        let hook: TestRequestHook = Arc::new(move |_code, _opaque, _channel, task_group, _deferred_response| {
            let mut captured = request_executor_group_for_hook
                .lock()
                .expect("request executor group lock");
            if captured.is_none() {
                *captured = Some(task_group);
            }
            TestRequestHookResult::Continue
        });
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-snapshot-test"),
        )
        .with_test_request_hook(hook);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (startup_tx, startup_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown_report_and_startup(
                    DefaultRequestProcessor,
                    None,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
        });
        let addr = await_server_startup(startup_rx).await;

        let mut client = crate::connection::Connection::new(TcpStream::connect(addr).await.unwrap());
        for index in 0..COMMAND_COUNT {
            let opaque = 1_000 + i32::try_from(index).unwrap();
            client
                .send_command(
                    rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                        rocketmq_protocol::code::request_code::RequestCode::SendMessage,
                    )
                    .set_opaque(opaque),
                )
                .await
                .unwrap();
            let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
                .await
                .expect("snapshot response deadline")
                .expect("snapshot response frame")
                .expect("snapshot response command");
            assert_eq!(response.opaque(), opaque);
        }

        let request_executor_group = request_executor_group
            .lock()
            .expect("request executor group lock")
            .clone()
            .expect("request executor group");
        assert_eq!(request_executor_group.component_count(), 0);

        let _ = shutdown_tx.send(());
        let report = server_task.await.unwrap().unwrap();
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn delayed_data_response_keeps_its_request_admission_class_after_control_request() {
        let limits = AdmissionLimits {
            queued: ResourceLimit { count: 4, bytes: 4096 },
            control_reserve: ResourceLimit { count: 2, bytes: 2048 },
            ..AdmissionLimits::default()
        };
        let admission = Arc::new(AdmissionController::new(limits));
        let scope = crate::admission::AdmissionScope::new("127.0.0.1".parse().expect("loopback address"));
        let delayed: Arc<std::sync::Mutex<Option<DelayedResponse>>> = Arc::new(std::sync::Mutex::new(None));
        let first_seen = Arc::new(tokio::sync::Notify::new());
        let second_seen = Arc::new(tokio::sync::Notify::new());
        let request_executor_group = Arc::new(std::sync::Mutex::new(None::<TaskGroup>));
        let delayed_for_hook = delayed.clone();
        let first_seen_for_hook = first_seen.clone();
        let second_seen_for_hook = second_seen.clone();
        let request_executor_group_for_hook = request_executor_group.clone();
        let hook: TestRequestHook = Arc::new(
            move |code, opaque, channel, task_group, deferred_response| match opaque {
                81 => {
                    assert_eq!(
                        code,
                        rocketmq_protocol::code::request_code::RequestCode::SendMessage as i32
                    );
                    *request_executor_group_for_hook
                        .lock()
                        .expect("request executor group lock") = Some(task_group);
                    *delayed_for_hook.lock().expect("delayed response lock") =
                        Some((channel.channel_id().to_owned(), deferred_response));
                    first_seen_for_hook.notify_one();
                    TestRequestHookResult::Intercept
                }
                82 => {
                    assert_eq!(
                        code,
                        rocketmq_protocol::code::request_code::RequestCode::HeartBeat as i32
                    );
                    let delayed_channel_id = delayed_for_hook
                        .lock()
                        .expect("delayed response lock")
                        .as_ref()
                        .map(|(channel_id, _)| channel_id.clone())
                        .expect("first request snapshot");
                    assert_eq!(channel.channel_id(), delayed_channel_id);
                    second_seen_for_hook.notify_one();
                    TestRequestHookResult::Continue
                }
                _ => TestRequestHookResult::Continue,
            },
        );
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        });
        let mut server = TransportServer::<DefaultRequestProcessor>::new(
            config,
            test_service_context("remoting-server-admission-snapshot-test"),
        )
        .with_admission_controller(admission.clone())
        .with_test_request_hook(hook);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (startup_tx, startup_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown_report_and_startup(
                    DefaultRequestProcessor,
                    None,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
        });
        let addr = await_server_startup(startup_rx).await;

        let mut client = crate::connection::Connection::new(TcpStream::connect(addr).await.unwrap());
        client
            .send_command(
                rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                    rocketmq_protocol::code::request_code::RequestCode::SendMessage,
                )
                .set_opaque(81),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), first_seen.notified())
            .await
            .expect("first data request should reach the processor");
        client
            .send_command(
                rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(
                    rocketmq_protocol::code::request_code::RequestCode::HeartBeat,
                )
                .set_opaque(82),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), second_seen.notified())
            .await
            .expect("second control request should reach the processor");
        let control_response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
            .await
            .expect("control response should use its request snapshot")
            .expect("control response frame")
            .expect("control response command");
        assert_eq!(control_response.opaque(), 82);
        let retained_request_executor_group = request_executor_group
            .lock()
            .expect("request executor group lock")
            .clone()
            .expect("request executor group");
        assert_eq!(retained_request_executor_group.component_count(), 0);

        let _data_one = admission
            .try_acquire(
                crate::admission::AdmissionResource::Queued,
                scope,
                1,
                crate::admission::AdmissionClass::Data,
            )
            .unwrap();
        let _data_two = admission
            .try_acquire(
                crate::admission::AdmissionResource::Queued,
                scope,
                1,
                crate::admission::AdmissionClass::Data,
            )
            .unwrap();

        let (_channel_id, delayed_write) = delayed
            .lock()
            .expect("delayed response lock")
            .take()
            .expect("first request snapshot");
        delayed_write(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_response_command_with_code(
                rocketmq_protocol::code::response_code::ResponseCode::Success,
            ),
        )
        .await;
        assert_eq!(retained_request_executor_group.component_count(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), client.receive_command())
                .await
                .is_err(),
            "the delayed data response must not borrow the later control request reserve"
        );

        let _ = shutdown_tx.send(());
        let report = server_task.await.unwrap().unwrap();
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn production_remoting_client_and_server_use_injected_transport_security() {
        let policy = Arc::new(RequireTransportSignature {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let security = Arc::new(crate::security::TransportSecurity::development_insecure_loopback(
            Some(policy.clone()),
            None,
        ));
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        });
        let service = test_service_context("remoting-server-security-test");
        let mut server = TransportServer::<DefaultRequestProcessor>::new(config, service.clone())
            .with_transport_security(security, Some(rocketmq_security_api::Principal::new("remoting-test")));
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (startup_tx, startup_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown_report_and_startup(
                    DefaultRequestProcessor,
                    None,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
        });
        let addr = await_server_startup(startup_rx).await;

        let client = TransportClient::build_for_test(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            service.component("client"),
        )
        .with_transport_security(Arc::new(
            crate::security::TransportSecurity::development_insecure_loopback(
                None,
                Some(Arc::new(RemotingMarkerSigner)),
            ),
        ));
        let remote = cheetah_string::CheetahString::from_string(addr.to_string());
        let response = client
            .invoke_request(
                Some(&remote),
                rocketmq_protocol::protocol::remoting_command::RemotingCommand::create_remoting_command(105),
                1_000,
            )
            .await
            .expect("signed high-level request");
        assert_eq!(response.code(), 105);
        assert_eq!(policy.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let client_report = client.shutdown_with_report(Duration::from_secs(1)).await;
        assert!(client_report.is_healthy());
        let _ = shutdown_tx.send(());
        let report = server_task.await.unwrap().expect("server report");
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn run_shutdown_cancels_connection_before_tls_peek_completes() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(run_with_report(
            test_service_context("remoting-server-tls-peek-test"),
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRequestProcessor,
            None,
            Vec::new(),
            None,
        ));

        let client = TcpStream::connect(addr).await.expect("client should connect");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _ = shutdown_tx.send(());
        let report = tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("server should shut down even when a connection has not sent its first byte")
            .expect("server task should not panic")
            .expect("server should return shutdown report");
        drop(client);

        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn run_shutdown_delivers_disconnect_before_lifecycle_dispatcher_stops() {
        let context = RuntimeContext::from_current("remoting-server-channel-report-test");
        let service = context.service_context("remoting-server-channel-report");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (connected_tx, connected_rx) = oneshot::channel::<()>();
        let (disconnected_tx, disconnected_rx) = oneshot::channel::<()>();
        let channel_listener = std::sync::Arc::new(ConnectSignalListener {
            connected: std::sync::Mutex::new(Some(connected_tx)),
            disconnected: std::sync::Mutex::new(Some(disconnected_tx)),
        });
        let server_task = tokio::spawn(run_with_report_with_service_context(
            service,
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRequestProcessor,
            None,
            Vec::new(),
            Some(channel_listener),
        ));

        let mut client = TcpStream::connect(addr).await.expect("client should connect");
        client
            .write_all(&[0])
            .await
            .expect("client should send first byte for TLS/plaintext detection");
        tokio::time::timeout(Duration::from_secs(3), connected_rx)
            .await
            .expect("server should accept connection before timeout")
            .expect("connect signal should be sent");
        let _ = shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(3), disconnected_rx)
            .await
            .expect("disconnect event should be delivered before shutdown")
            .expect("disconnect signal should be sent");
        let report = tokio::time::timeout(Duration::from_secs(3), server_task)
            .await
            .expect("server should shut down before timeout")
            .expect("server task should not panic")
            .expect("server should return shutdown report");
        drop(client);

        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.leaked, 0, "{}", report.to_json());
        assert_eq!(report.detached_still_running, 0, "{}", report.to_json());
        assert!(report.remaining_tasks.is_empty(), "{}", report.to_json());
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn run_shutdown_report_includes_tls_reload_task() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let context = RuntimeContext::from_current("remoting-server-tls-report-test");
        let service = context.service_context("remoting-server-tls-report");
        let tls_runtime = TlsServerRuntime::initialize_with_service_context(
            TlsConfig {
                test_mode_enable: true,
                server: TlsServerConfig {
                    mode: TlsMode::Permissive,
                    ..Default::default()
                },
                ..Default::default()
            },
            &service,
        )
        .await
        .expect("TLS runtime should initialize");

        let report = run_with_tls_config_report(
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRequestProcessor,
            None,
            Vec::new(),
            None,
            None,
            RemotingServerRunCapabilities {
                tls_runtime,
                task_group: service.component("remoting-server").task_group().clone(),
                file_region_blocking: service.storage_io().clone(),
                file_transfer_mode: FileTransferMode::Auto,
                frame_limits: FrameLimits::default(),
                process_budget: service.process_budget(),
                transport_security: None,
                transport_principal: None,
                admission: None,
                command_interceptor: Arc::new(()),
                telemetry: TransportTelemetry::noop(),
                lifecycle_event_config: LifecycleEventConfig::default(),
            },
        );
        let server_task = tokio::spawn(report);

        let _ = shutdown_tx.send(());
        let report = tokio::time::timeout(Duration::from_secs(3), server_task)
            .await
            .expect("server should shut down before timeout")
            .expect("server task should not panic")
            .expect("server should return shutdown report");

        assert!(report.is_healthy(), "{}", report.to_json());
        let tls_report = report
            .children
            .iter()
            .find(|child| child.name == "rocketmq-transport.tls")
            .expect("remoting shutdown report should include tls reload task group");
        assert!(tls_report.is_healthy(), "{}", tls_report.to_json());
        assert_eq!(tls_report.leaked, 0, "{}", tls_report.to_json());
    }
}
