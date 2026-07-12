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

use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::security::Principal;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_rust::wait_for_signal;
use rocketmq_rust::ArcMut;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::error;
use tracing::info;

use crate::base::channel_event_listener::ChannelEventListener;
use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::tokio_event::TokioEvent;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;
use crate::tls::TlsServerRuntime;
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::admission::ResourceLimit;
use rocketmq_transport::security::TransportSecurity;
use rocketmq_transport::server::ConnectionHandler as TransportConnectionHandler;
use rocketmq_transport::server::TransportListener;

/// Default limit the max number of connections.
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const EVENT_QUEUE_CAPACITY: usize = 1024;

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

    /// TLS mode and acceptor state for newly accepted connections.
    tls_runtime: TlsServerRuntime,

    /// Tracks remoting event and connection tasks for shutdown diagnostics.
    task_group: TaskGroup,

    admission: Arc<AdmissionController>,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
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

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<TokioEvent>(EVENT_QUEUE_CAPACITY);

        // Spawn event dispatcher task if listener configured
        if let Some(listener) = self.channel_event_listener.take() {
            let spawn_result =
                self.task_group
                    .spawn("rocketmq.remoting.event_dispatcher", TaskKind::Service, async move {
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
            if let Err(error) = spawn_result {
                error!("Failed to spawn remoting event dispatcher: {}", error);
            }
        }

        let listener = self.listener.take().ok_or_else(|| {
            RocketMQError::network_connection_failed("remoting-server", "transport listener already started")
        })?;
        let mut transport = TransportListener::new(
            listener,
            self.task_group.clone(),
            self.tls_runtime.transport_runtime(),
            self.admission.clone(),
            DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        );
        if let Some(security) = self.transport_security.clone() {
            transport = transport.with_security(security, self.transport_principal.clone());
        }
        transport
            .run(Arc::new(ConnectionHandler {
                shutdown_complete_tx: self.shutdown_complete_tx.clone(),
                conn_disconnect_notify: self.conn_disconnect_notify.clone(),
                cmd_handler: self.cmd_handler.clone(),
                event_tx,
                sessions: dashmap::DashMap::new(),
            }))
            .await
    }
}

struct ConnectionHandler<RP> {
    shutdown_complete_tx: mpsc::Sender<()>,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    cmd_handler: ArcMut<RemotingGeneralHandler<RP>>,
    event_tx: mpsc::Sender<TokioEvent>,
    sessions: dashmap::DashMap<u64, RemotingSession<ConnectionHandlerContext>>,
}

struct RemotingSession<C> {
    context: C,
    _shutdown_complete: mpsc::Sender<()>,
}

impl<RP> ConnectionHandler<RP> {
    fn run(&self, session: rocketmq_transport::server::SessionHandle) {
        let Ok(channel_inner) = ChannelInner::new_transport_session(
            session.connection(),
            self.cmd_handler.response_table.clone(),
            session.task_group().child("rocketmq.remoting.channel"),
        ) else {
            return;
        };
        let mut channel = Channel::new(ArcMut::new(channel_inner), session.local_addr(), session.remote_addr());
        channel.set_channel_id(format!("transport-session-{}", session.session_id()));
        let _ = self.event_tx.try_send(TokioEvent::new(
            ConnectionNetEvent::CONNECTED(session.remote_addr()),
            session.remote_addr(),
            channel.clone(),
        ));
        self.sessions.insert(
            session.session_id(),
            RemotingSession {
                context: ArcMut::new(ConnectionHandlerContextWrapper::new(channel)),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            },
        );
    }
}

impl<RP: RequestProcessor + Sync + 'static> TransportConnectionHandler for ConnectionHandler<RP> {
    fn connected(
        &self,
        session: rocketmq_transport::server::SessionHandle,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.run(session);
        })
    }

    fn command(
        &self,
        session: rocketmq_transport::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let mut cmd_handler = self.cmd_handler.clone();
        Box::pin(async move {
            let Some((_, mut remoting_session)) = self.sessions.remove(&session.session_id()) else {
                return;
            };
            cmd_handler
                .process_message_received(&mut remoting_session.context, command)
                .await;
            self.sessions.insert(session.session_id(), remoting_session);
        })
    }

    fn disconnected(
        &self,
        session: rocketmq_transport::server::SessionHandle,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let event_tx = self.event_tx.clone();
        let conn_disconnect_notify = self.conn_disconnect_notify.clone();
        Box::pin(async move {
            let Some((_, mut remoting_session)) = self.sessions.remove(&session.session_id()) else {
                return;
            };
            let channel_report = remoting_session
                .context
                .channel_mut()
                .channel_inner_mut()
                .close_with_report(Duration::from_secs(3))
                .await;
            channel_report.log_if_unhealthy();
            if let Some(notify) = conn_disconnect_notify {
                let _ = notify.send(session.remote_addr());
            }
            let _ = event_tx.try_send(TokioEvent::new(
                ConnectionNetEvent::DISCONNECTED,
                session.remote_addr(),
                remoting_session.context.channel.clone(),
            ));
        })
    }
}

pub struct RocketMQServer<RP> {
    config: Arc<ServerConfig>,
    rpc_hooks: Option<Vec<Arc<dyn RPCHook>>>,
    service_context: Option<ServiceContext>,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
    _phantom_data: std::marker::PhantomData<RP>,
}

impl<RP> RocketMQServer<RP> {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            config,
            rpc_hooks: Some(vec![]),
            service_context: None,
            transport_security: None,
            transport_principal: None,
            _phantom_data: std::marker::PhantomData,
        }
    }

    pub fn new_with_service_context(config: Arc<ServerConfig>, service_context: ServiceContext) -> Self {
        Self {
            config,
            rpc_hooks: Some(vec![]),
            service_context: Some(service_context),
            transport_security: None,
            transport_principal: None,
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
}

impl<RP: RequestProcessor + Sync + 'static + Clone> RocketMQServer<RP> {
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
        let addr = format!("{}:{}", self.config.bind_address, self.config.listen_port);
        let listener = match TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(err) => {
                error!(addr = %addr, error = %err, "failed to bind remoting_server");
                return None;
            }
        };
        let rpc_hooks = self.rpc_hooks.take().unwrap_or_default();
        let remoting_context = match self.service_context.as_ref() {
            Some(context) => new_remoting_server_context(context),
            None => match standalone_remoting_server_context() {
                Ok(context) => context,
                Err(error) => {
                    error!(%error, "failed to initialize remoting server runtime context");
                    return None;
                }
            },
        };
        let task_group = Some(remoting_context.task_group().clone());
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
        info!("Starting remoting_server at: {}", addr);
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        run_with_tls_config_report(
            listener,
            shutdown,
            request_processor,
            Some(notify_conn_disconnect),
            rpc_hooks,
            channel_event_listener,
            tls_runtime,
            task_group,
            self.transport_security.clone(),
            self.transport_principal.clone(),
        )
        .await
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
    let _ = run_with_report(
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
    )
    .await;
}

#[doc(hidden)]
pub async fn run_with_report<RP: RequestProcessor + Sync + 'static + Clone>(
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
) -> Option<ShutdownReport> {
    let service_context = match standalone_remoting_server_context() {
        Ok(context) => context,
        Err(error) => {
            error!(%error, "failed to initialize remoting server runtime context");
            return None;
        }
    };
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

#[doc(hidden)]
pub async fn run_with_report_with_service_context<RP: RequestProcessor + Sync + 'static + Clone>(
    service_context: ServiceContext,
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
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
        tls_runtime,
        Some(remoting_context.task_group().clone()),
        None,
        None,
    )
    .await
}

async fn run_with_tls_config<RP: RequestProcessor + Sync + 'static + Clone>(
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    tls_runtime: TlsServerRuntime,
) {
    let _ = run_with_tls_config_report(
        listener,
        shutdown,
        request_processor,
        conn_disconnect_notify,
        rpc_hooks,
        channel_event_listener,
        tls_runtime,
        None,
        None,
        None,
    )
    .await;
}

async fn run_with_tls_config_report<RP: RequestProcessor + Sync + 'static + Clone>(
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Vec<Arc<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    tls_runtime: TlsServerRuntime,
    parented_task_group: Option<TaskGroup>,
    transport_security: Option<Arc<TransportSecurity>>,
    transport_principal: Option<Principal>,
) -> Option<ShutdownReport> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let task_group = if let Some(parented_task_group) = parented_task_group {
        parented_task_group
    } else {
        match new_remoting_server_task_group() {
            Ok(task_group) => task_group,
            Err(error) => {
                error!(%error, "failed to start remoting server task group");
                return None;
            }
        }
    };
    // Initialize the connection listener state
    let handler = RemotingGeneralHandler {
        request_processor,
        //shutdown: Shutdown::new(notify_shutdown.subscribe()),
        rpc_hooks,
        response_table: PendingRequestTable::with_capacity(512),
    };
    let mut admission_limits = AdmissionLimits::default();
    admission_limits.connections = ResourceLimit {
        count: DEFAULT_MAX_CONNECTIONS,
        ..admission_limits.connections
    };
    admission_limits.handshakes = ResourceLimit {
        count: DEFAULT_MAX_CONNECTIONS,
        ..admission_limits.handshakes
    };
    let mut listener = ConnectionListener {
        listener: Some(listener),
        notify_shutdown,
        shutdown_complete_tx,
        conn_disconnect_notify,
        channel_event_listener,
        cmd_handler: ArcMut::new(handler),
        tls_runtime,
        task_group: task_group.clone(),
        admission: Arc::new(AdmissionController::new(admission_limits)),
        transport_security,
        transport_principal,
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
        tls_runtime,
        ..
    } = listener;
    let deadline = task_group
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(Duration::from_secs(30)));
    let tls_report = tls_runtime
        .shutdown_gracefully(deadline.remaining().min(Duration::from_secs(3)))
        .await;
    if let Some(report) = tls_report.as_ref() {
        report.log_if_unhealthy();
    }
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    task_group.cancel();
    let _ = tokio::time::timeout(deadline.remaining(), shutdown_complete_rx.recv()).await;
    let mut report = task_group.shutdown_until(deadline).await;
    if let Some(tls_report) = tls_report {
        report.children.push(tls_report);
    }
    report.log_if_unhealthy();
    Some(report)
}

fn new_remoting_server_task_group() -> rocketmq_error::RocketMQResult<TaskGroup> {
    let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
        rocketmq_error::RocketMQError::network_connection_failed(
            "remoting-server",
            format!("remoting server task group requires a Tokio runtime: {error}"),
        )
    })?;
    Ok(TaskGroup::root("rocketmq.remoting.server", RuntimeHandle::new(runtime)))
}

fn new_remoting_server_context(context: &ServiceContext) -> ServiceContext {
    context.child("rocketmq.remoting.server")
}

fn standalone_remoting_server_context() -> rocketmq_error::RocketMQResult<ServiceContext> {
    let runtime = RuntimeContext::try_from_current("rocketmq.remoting.server")
        .map_err(|error| RocketMQError::network_connection_failed("remoting-server", error.to_string()))?;
    Ok(runtime.service_context("rocketmq.remoting.server.service"))
}

fn new_remoting_server_task_group_with_service_context(context: &ServiceContext) -> TaskGroup {
    new_remoting_server_context(context).task_group().clone()
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

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::Arc;

    use rocketmq_common::common::server::config::ServerConfig;
    #[cfg(feature = "tls")]
    use rocketmq_common::common::tls_config::TlsConfig;
    #[cfg(feature = "tls")]
    use rocketmq_common::common::tls_config::TlsMode;
    #[cfg(feature = "tls")]
    use rocketmq_common::common::tls_config::TlsServerConfig;
    use rocketmq_runtime::RuntimeContext;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;
    use crate::clients::rocketmq_tokio_client::RocketmqDefaultClient;
    use crate::clients::RemotingClient;
    use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
    use crate::runtime::config::client_config::TokioClientConfig;

    struct ConnectSignalListener {
        connected: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    }

    struct RequireTransportSignature {
        calls: std::sync::atomic::AtomicUsize,
    }

    impl rocketmq_common::security::RequestPolicy for RequireTransportSignature {
        fn evaluate_authenticated(
            &self,
            context: rocketmq_common::security::AuthenticatedRequestContext<'_>,
        ) -> rocketmq_common::security::Decision {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if context.request().fields().contains_key("TransportSignature") {
                rocketmq_common::security::Decision::Allow
            } else {
                rocketmq_common::security::Decision::deny("missing transport signature")
            }
        }
    }

    struct RemotingMarkerSigner;

    impl rocketmq_common::security::OutboundSigner for RemotingMarkerSigner {
        fn sign(
            &self,
            _request: rocketmq_common::security::SecurityRequestView<'_>,
        ) -> Result<rocketmq_common::security::Signature, rocketmq_common::security::SigningError> {
            Ok(rocketmq_common::security::Signature::new(vec![(
                cheetah_string::CheetahString::from_static_str("TransportSignature"),
                rocketmq_common::security::Secret::new(cheetah_string::CheetahString::from_static_str("signed")),
            )]))
        }
    }

    impl ChannelEventListener for ConnectSignalListener {
        fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
            if let Some(sender) = self.connected.lock().expect("connect signal lock").take() {
                let _ = sender.send(());
            }
        }

        fn on_channel_close(&self, _remote_addr: &str, _channel: &Channel) {}

        fn on_channel_exception(&self, _remote_addr: &str, _channel: &Channel) {}

        fn on_channel_idle(&self, _remote_addr: &str, _channel: &Channel) {}

        fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {}
    }

    #[test]
    fn remoting_server_task_group_without_tokio_runtime_returns_error() {
        let error = new_remoting_server_task_group()
            .expect_err("remoting server task group should require an ambient Tokio runtime");

        assert!(error
            .to_string()
            .contains("remoting server task group requires a Tokio runtime"));
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
    async fn run_with_report_with_service_context_adds_remoting_child_to_parent_report() {
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
            DefaultRemotingRequestProcessor,
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
        assert!(
            parent_report
                .children
                .iter()
                .any(|child| child.name == "rocketmq.remoting.server"),
            "{}",
            parent_report.to_json()
        );
    }

    #[tokio::test]
    async fn run_with_shutdown_bind_error_returns_without_panicking() {
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: 70000,
            ..ServerConfig::default()
        });
        let mut server = RocketMQServer::<DefaultRemotingRequestProcessor>::new(config);

        server
            .run_with_shutdown(DefaultRemotingRequestProcessor, None, future::pending::<()>())
            .await;
    }

    #[tokio::test]
    async fn run_with_shutdown_report_bind_error_returns_none() {
        let config = Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: 70000,
            ..ServerConfig::default()
        });
        let mut server = RocketMQServer::<DefaultRemotingRequestProcessor>::new(config);

        let report = server
            .run_with_shutdown_report(DefaultRemotingRequestProcessor, None, future::pending::<()>())
            .await;

        assert!(report.is_none());
    }

    #[tokio::test]
    async fn run_shutdown_drains_connection_tasks() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(run_with_report(
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRemotingRequestProcessor,
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
        let server_task = tokio::spawn(run_with_report(
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRemotingRequestProcessor,
            None,
            Vec::new(),
            None,
        ));
        let mut client =
            RocketmqDefaultClient::new(Arc::new(TokioClientConfig::default()), DefaultRemotingRequestProcessor);
        let remote_addr = cheetah_string::CheetahString::from_string(addr.to_string());
        let request = crate::protocol::remoting_command::RemotingCommand::create_remoting_command(105);
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
    async fn production_remoting_client_and_server_use_injected_transport_security() {
        let reserved = TcpListener::bind("127.0.0.1:0").await.expect("reserve port");
        let addr = reserved.local_addr().unwrap();
        drop(reserved);
        let policy = Arc::new(RequireTransportSignature {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let security = Arc::new(rocketmq_transport::security::TransportSecurity::new(
            Some(policy.clone()),
            None,
        ));
        let config = Arc::new(ServerConfig {
            bind_address: addr.ip().to_string(),
            listen_port: u32::from(addr.port()),
            ..ServerConfig::default()
        });
        let mut server = RocketMQServer::<DefaultRemotingRequestProcessor>::new(config).with_transport_security(
            security,
            Some(rocketmq_common::security::Principal::new("remoting-test")),
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown_report(DefaultRemotingRequestProcessor, None, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut client =
            RocketmqDefaultClient::new(Arc::new(TokioClientConfig::default()), DefaultRemotingRequestProcessor)
                .with_transport_security(Arc::new(rocketmq_transport::security::TransportSecurity::new(
                    None,
                    Some(Arc::new(RemotingMarkerSigner)),
                )));
        let remote = cheetah_string::CheetahString::from_string(addr.to_string());
        let response = client
            .invoke_request(
                Some(&remote),
                crate::protocol::remoting_command::RemotingCommand::create_remoting_command(105),
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
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRemotingRequestProcessor,
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
    async fn run_shutdown_report_is_healthy_after_dynamic_connection_child_prunes() {
        let context = RuntimeContext::from_current("remoting-server-channel-report-test");
        let service = context.service_context("remoting-server-channel-report");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (connected_tx, connected_rx) = oneshot::channel::<()>();
        let channel_listener = std::sync::Arc::new(ConnectSignalListener {
            connected: std::sync::Mutex::new(Some(connected_tx)),
        });
        let server_task = tokio::spawn(run_with_report_with_service_context(
            service,
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRemotingRequestProcessor,
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
        let tls_runtime = TlsServerRuntime::new(TlsConfig {
            test_mode_enable: true,
            server: TlsServerConfig {
                mode: TlsMode::Permissive,
                ..Default::default()
            },
            ..Default::default()
        });

        let report = run_with_tls_config_report(
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRemotingRequestProcessor,
            None,
            Vec::new(),
            None,
            tls_runtime,
            None,
            None,
            None,
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
            .find(|child| child.name == "rocketmq-remoting.tls")
            .expect("remoting shutdown report should include tls reload task group");
        assert!(tls_report.is_healthy(), "{}", tls_report.to_json());
        assert_eq!(tls_report.leaked, 0, "{}", tls_report.to_json());
    }
}
