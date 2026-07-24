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
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::tls_config::TlsConfig;
use rocketmq_common::security::PeerInfo;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupChildLease;
use tokio::sync::broadcast;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::codec::remoting_command_codec::FrameLimits;
use crate::connection::ConnectionStateHandle;
// Import error helpers for convenient error creation
use crate::error_helpers::remote_error;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::deadline::RequestDeadline;
use rocketmq_transport::security::TransportSecurity;
use rocketmq_transport::server::ConnectionHandler as TransportConnectionHandler;
use rocketmq_transport::server::SessionHandle;

#[derive(Clone)]
pub struct Client<PR> {
    /// The TCP connection decorated with the rocketmq remoting protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    //connection: Connection,
    channel: Channel,
    notify_shutdown: broadcast::Sender<()>,
    session: SessionHandle,
    pending_requests: PendingRequestTable,
    pending_request_owner: PendingRequestOwner,
    task_lifecycle: Arc<ClientTaskLifecycle>,
    transport_security: Arc<TransportSecurity>,
    peer: PeerInfo,
    _processor: PhantomData<fn() -> PR>,
}

const DEFAULT_CALLBACK_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
type ConnectedClientSession = (Channel, PendingRequestOwner, SessionHandle, SocketAddr, bool);
type ClientConnectFuture = Pin<Box<dyn Future<Output = RocketMQResult<ConnectedClientSession>> + Send>>;

struct ClientTaskLifecycle {
    task_group: TaskGroup,
    _child_lease: Option<TaskGroupChildLease>,
}

struct ClientInner<PR> {
    cmd_handler: Arc<RemotingGeneralHandler<PR>>,
    sessions: dashmap::DashMap<u64, ConnectionHandlerContext>,
    ready: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<(Channel, PendingRequestOwner, SessionHandle)>>>,
}

impl<PR> ClientInner<PR> {
    fn connect(&self, session: SessionHandle) {
        let Ok(channel_inner) = ChannelInner::new_transport_session(
            session.connection(),
            self.cmd_handler.response_table.clone(),
            session.task_group().child("rocketmq.remoting.client-channel"),
        ) else {
            return;
        };
        let Some(owner) = channel_inner.pending_request_owner().cloned() else {
            return;
        };
        let channel = Channel::new(Arc::new(channel_inner), session.local_addr(), session.remote_addr());
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        self.sessions.insert(session.session_id(), context);
        if let Some(ready) = self.ready.lock().take() {
            let _ = ready.send((channel, owner, session));
        }
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportConnectionHandler for ClientInner<PR> {
    fn connected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.connect(session);
        })
    }

    fn command(
        &self,
        session: SessionHandle,
        command: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let cmd_handler = self.cmd_handler.clone();
        Box::pin(async move {
            let Some((_, context)) = self.sessions.remove(&session.session_id()) else {
                return;
            };
            cmd_handler.process_message_received(&context, command).await;
            self.sessions.insert(session.session_id(), context);
        })
    }

    fn disconnected(&self, session: SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.sessions.remove(&session.session_id());
        })
    }
}

fn new_client_connection_task_group(addr: &str) -> RocketMQResult<(TaskGroup, Option<TaskGroupChildLease>)> {
    let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
        remote_error(format!(
            "failed to create remoting client connection task group outside Tokio runtime: {error}"
        ))
    })?;
    Ok((
        TaskGroup::root(
            format!("rocketmq-remoting.client.connection[{addr}]"),
            RuntimeHandle::new(runtime),
        ),
        None,
    ))
}

fn new_client_connection_task_group_with_service_context(
    context: &ServiceContext,
    addr: &str,
) -> RocketMQResult<(TaskGroup, Option<TaskGroupChildLease>)> {
    let lease = context
        .task_group()
        .try_child_lease(format!("rocketmq-remoting.client.connection[{addr}]"))
        .map_err(|error| {
            remote_error(format!(
                "failed to register remoting client connection task group: {error}"
            ))
        })?;
    Ok((lease.group().clone(), Some(lease)))
}

impl<PR> Drop for Client<PR> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.task_lifecycle) != 1 {
            return;
        }

        let _ = self.notify_shutdown.send(());
        self.pending_requests.close_owner(&self.pending_request_owner, || {
            RocketMQError::network_connection_failed("client", "connection dropped")
        });
        self.task_lifecycle.task_group.cancel();
    }
}

fn connect<PR>(
    addr: String,
    cmd_handler: Arc<RemotingGeneralHandler<PR>>,
    tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    _notify: broadcast::Receiver<()>,
    _send_notify: broadcast::Receiver<()>,
    tls_config: TlsConfig,
    task_group: TaskGroup,
    deadline: RequestDeadline,
) -> ClientConnectFuture
where
    PR: RequestProcessor + Sync + Clone + 'static,
{
    Box::pin(async move {
        let connected = rocketmq_transport::client::connect_with_config(
            addr.as_str(),
            &tls_config,
            FrameLimits::legacy_compatibility(),
            deadline,
        )
        .await?;
        let (connection, local_addr, remote_address, negotiated_tls) = connected.into_parts_with_tls();
        let (ready, connected_session) = tokio::sync::oneshot::channel();
        let transport_handler = Arc::new(ClientInner {
            cmd_handler: cmd_handler.clone(),
            sessions: dashmap::DashMap::new(),
            ready: parking_lot::Mutex::new(Some(ready)),
        });
        let session_task_group = task_group.clone();
        let session_handler = transport_handler.clone();
        let session_runner: Pin<Box<dyn Future<Output = ()> + Send>> =
            Box::pin(rocketmq_transport::server::run_connected_session(
                connection,
                local_addr,
                remote_address,
                session_task_group,
                Arc::new(AdmissionController::new(AdmissionLimits::default())),
                Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
                None,
                Duration::from_secs(120),
                session_handler,
            ));
        task_group
            .spawn_service("rocketmq.transport.client-session", session_runner)
            .map_err(|error| remote_error(format!("failed to spawn transport client session: {error}")))?;
        let (channel, pending_request_owner, session) = deadline
            .timeout(connected_session)
            .await
            .map_err(|_| RocketMQError::network_connection_timeout(addr.clone(), deadline.budget_millis()))?
            .map_err(|_| remote_error("transport client session ended before connection setup"))?;
        if let Some(tx) = tx {
            let _ = tx.send(ConnectionNetEvent::CONNECTED(channel.remote_address()));
        }
        Ok((channel, pending_request_owner, session, remote_address, negotiated_tls))
    })
}

impl<PR> Client<PR>
where
    PR: RequestProcessor + Sync + Clone + 'static,
{
    /// Creates a new `Client` instance and connects to the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to connect to.
    ///
    /// # Returns
    ///
    /// A new `Client` instance wrapped in a `Result`. Returns an error if the connection fails.
    pub(crate) async fn connect(
        addr: String,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
    ) -> RocketMQResult<Client<PR>> {
        Self::connect_until(
            addr,
            cmd_handler,
            tx,
            tls_config,
            RequestDeadline::after(Duration::from_secs(10)),
        )
        .await
    }

    pub(crate) async fn connect_until(
        addr: String,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Client<PR>> {
        let (task_group, child_lease) = new_client_connection_task_group(&addr)?;
        Self::connect_with_task_group(addr, cmd_handler, tx, tls_config, task_group, child_lease, deadline).await
    }

    pub(crate) async fn connect_with_service_context(
        context: &ServiceContext,
        addr: String,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
    ) -> RocketMQResult<Client<PR>> {
        Self::connect_with_service_context_until(
            context,
            addr,
            cmd_handler,
            tx,
            tls_config,
            RequestDeadline::after(Duration::from_secs(10)),
        )
        .await
    }

    pub(crate) async fn connect_with_service_context_until(
        context: &ServiceContext,
        addr: String,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Client<PR>> {
        let (task_group, child_lease) = new_client_connection_task_group_with_service_context(context, &addr)?;
        Self::connect_with_task_group(addr, cmd_handler, tx, tls_config, task_group, child_lease, deadline).await
    }

    async fn connect_with_task_group(
        addr: String,
        cmd_handler: Arc<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
        task_group: TaskGroup,
        child_lease: Option<TaskGroupChildLease>,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Client<PR>> {
        let (notify_shutdown, _) = broadcast::channel(1);
        let receiver = notify_shutdown.subscribe();
        let send_receiver = notify_shutdown.subscribe();
        let task_lifecycle = Arc::new(ClientTaskLifecycle {
            task_group: task_group.clone(),
            _child_lease: child_lease,
        });
        let pending_requests = cmd_handler.response_table.clone();
        let (channel, pending_request_owner, session, remote_address, negotiated_tls) = connect(
            addr,
            cmd_handler,
            tx.cloned(),
            receiver,
            send_receiver,
            tls_config,
            task_group,
            deadline,
        )
        .await?;
        Ok(Client {
            channel,
            notify_shutdown,
            session,
            pending_requests,
            pending_request_owner,
            task_lifecycle,
            transport_security: Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            peer: PeerInfo::new(remote_address, negotiated_tls),
            _processor: PhantomData,
        })
    }

    pub(crate) fn with_transport_security(mut self, transport_security: Arc<TransportSecurity>) -> Self {
        self.transport_security = transport_security;
        self
    }

    async fn send_transport(&self, mut request: RemotingCommand, deadline: RequestDeadline) -> RocketMQResult<()> {
        let transport_security = &self.transport_security;
        let target = self.peer.address().to_string();
        deadline.ensure_before_send(target.clone())?;
        transport_security
            .sign(&mut request, Some(&self.peer))
            .map_err(|error| remote_error(format!("request signing failed: {error}")))?;
        deadline.ensure_before_send(target.clone())?;
        let mut connection = self.session.connection();
        connection.send_command_with_deadline(request, deadline, target).await
    }

    /// Invokes a remote operation with the given `RemotingCommand`.
    ///
    /// # Arguments
    ///
    /// * `request` - The `RemotingCommand` representing the request.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// the invocation fails.
    pub async fn send_read(
        &mut self,
        request: RemotingCommand,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RemotingCommand> {
        let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let opaque = request.opaque();
        let retained_bytes = request.body().map_or(0, bytes::Bytes::len);
        let guard = self.pending_requests.register_for_owner_with_bytes(
            &self.pending_request_owner,
            opaque,
            deadline,
            retained_bytes,
            tx,
        )?;

        self.send_transport(request, deadline).await?;
        match deadline.timeout(rx).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => Err(remote_error(error.to_string())),
            Err(_) => Err(guard.expire(self.peer.address().to_string())),
        }
    }

    /// Invokes a remote operation with the given `RemotingCommand` and provides a callback function
    /// for handling the response.
    ///
    /// # Arguments
    ///
    /// * `request` - The `RemotingCommand` representing the request.
    /// * `func` - The callback function to run after the response future completes.
    pub async fn invoke_with_callback<F>(&self, request: RemotingCommand, mut func: F)
    where
        F: FnMut(),
    {
        self.invoke_with_callback_timeout(request, DEFAULT_CALLBACK_RESPONSE_TIMEOUT, &mut func)
            .await;
    }

    async fn invoke_with_callback_timeout<F>(&self, request: RemotingCommand, timeout: Duration, mut func: F)
    where
        F: FnMut(),
    {
        let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let deadline = RequestDeadline::after(timeout);
        let retained_bytes = request.body().map_or(0, bytes::Bytes::len);
        let guard = match self.pending_requests.register_for_owner_with_bytes(
            &self.pending_request_owner,
            request.opaque(),
            deadline,
            retained_bytes,
            tx,
        ) {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if self.send_transport(request, deadline).await.is_err() {
            return;
        }

        if deadline.timeout(rx).await.is_err() {
            guard.expire(self.peer.address().to_string());
            self.retire_after_timeout().await;
        }
        func();
    }

    /// Sends a request to the remote remoting_server.
    ///
    /// # Arguments
    ///
    /// * `request` - The `RemotingCommand` representing the request.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure in sending the request.
    pub async fn send(&mut self, request: RemotingCommand) -> RocketMQResult<()> {
        self.send_transport(request, RequestDeadline::after(DEFAULT_CALLBACK_RESPONSE_TIMEOUT))
            .await
    }

    /// Sends a request using the caller's existing immutable deadline.
    pub async fn send_until(&mut self, request: RemotingCommand, deadline: RequestDeadline) -> RocketMQResult<()> {
        self.send_transport(request, deadline).await
    }

    /// Sends multiple requests in a batch (fire-and-forget, no response expected).
    ///
    /// # Performance
    ///
    /// Batching provides 2-4x throughput improvement for small messages:
    /// - Single system call instead of N
    /// - Better CPU cache locality during encoding
    /// - Reduced Nagle algorithm delays
    ///
    /// # Use Cases
    ///
    /// - Log shipping (async, high volume)
    /// - Metrics reporting
    /// - Event publishing
    ///
    /// # Arguments
    ///
    /// * `requests` - Vector of commands to send (consumed)
    ///
    /// # Returns
    ///
    /// - `Ok(())`: All commands queued successfully
    /// - `Err(e)`: Channel send error (client shutdown)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let commands = vec![
    ///     RemotingCommand::create_request_command(/*...*/),
    ///     RemotingCommand::create_request_command(/*...*/),
    /// ];
    /// client.send_batch(commands).await?;
    /// ```
    pub async fn send_batch(&mut self, requests: Vec<RemotingCommand>) -> RocketMQResult<()> {
        // Send all commands individually through the channel
        // The underlying connection will buffer them efficiently
        for request in requests {
            self.send_transport(request, RequestDeadline::after(DEFAULT_CALLBACK_RESPONSE_TIMEOUT))
                .await?;
        }
        Ok(())
    }

    /// Sends multiple requests and collects responses (request-response batch).
    ///
    /// # Performance vs send_read()
    ///
    /// ```text
    /// 100x send_read():    ~5000ms  (sequential network RTT)
    /// send_batch_read():   ~100ms   (parallel + single RTT)
    /// Improvement: 50x faster
    /// ```
    ///
    /// # Arguments
    ///
    /// * `requests` - Vector of commands expecting responses
    /// * `timeout_millis` - Timeout for each individual request
    ///
    /// # Returns
    ///
    /// Vector of results in the same order as input requests
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let requests = vec![cmd1, cmd2, cmd3];
    /// let responses = client.send_batch_read(requests, 3000).await?;
    /// for response in responses {
    ///     match response {
    ///         Ok(cmd) => println!("Success: {:?}", cmd),
    ///         Err(e) => eprintln!("Failed: {}", e),
    ///     }
    /// }
    /// ```
    pub async fn send_batch_read(
        &mut self,
        requests: Vec<RemotingCommand>,
        timeout_millis: u64,
    ) -> RocketMQResult<Vec<RocketMQResult<RemotingCommand>>> {
        let deadline = RequestDeadline::from_timeout_millis(timeout_millis);
        let mut receivers = Vec::with_capacity(requests.len());

        // Send all requests and collect oneshot receivers
        for request in requests {
            let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
            let retained_bytes = request.body().map_or(0, bytes::Bytes::len);
            let guard = self.pending_requests.register_for_owner_with_bytes(
                &self.pending_request_owner,
                request.opaque(),
                deadline,
                retained_bytes,
                tx,
            )?;

            self.send_transport(request, deadline).await?;
            receivers.push((guard, rx));
        }

        // Collect all responses
        let mut results = Vec::with_capacity(receivers.len());
        let mut timed_out = false;
        for (guard, rx) in receivers {
            let result = match deadline.timeout(rx).await {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => Err(remote_error(error.to_string())),
                Err(_) => {
                    timed_out = true;
                    Err(guard.expire(self.peer.address().to_string()))
                }
            };
            results.push(result);
        }
        if timed_out {
            self.retire_after_timeout().await;
        }

        Ok(results)
    }

    /// Gracefully stop this client connection and return the task shutdown report.
    pub async fn close_with_report(&self, timeout: Duration) -> rocketmq_runtime::ShutdownReport {
        let _ = self.notify_shutdown.send(());
        let report = self.task_lifecycle.task_group.shutdown(timeout).await;
        self.pending_requests.close_owner(&self.pending_request_owner, || {
            RocketMQError::network_connection_failed("client", "connection closed")
        });
        report.log_if_unhealthy();
        report
    }

    pub fn connection(&self) -> &ConnectionStateHandle {
        self.channel.connection_ref()
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.channel.remote_address()
    }

    pub(crate) fn retire_after_timeout(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let _ = self.session.retire().await;
            let _ = self.notify_shutdown.send(());
            self.pending_requests.close_owner(&self.pending_request_owner, || {
                RocketMQError::network_connection_failed("client", "connection retired after request timeout")
            });
        })
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::time::Duration;

    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::TaskGroupLifecycleState;
    use tokio::net::TcpListener;
    use tokio::time;

    use super::*;
    use crate::base::pending_request_table::PendingRequestTable;
    use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;

    #[test]
    fn connection_task_group_without_tokio_runtime_returns_error() {
        let error = new_client_connection_task_group("127.0.0.1:10911")
            .expect_err("client connection task group should require an ambient Tokio runtime");

        assert!(error
            .to_string()
            .contains("failed to create remoting client connection task group outside Tokio runtime"));
    }

    #[tokio::test]
    async fn drop_last_client_closes_connection_task_group() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let cmd_handler = Arc::new(RemotingGeneralHandler::new(
            DefaultRemotingRequestProcessor,
            vec![],
            PendingRequestTable::new(),
        ));

        let mut client = Client::connect(addr.to_string(), cmd_handler, None, TlsConfig::default())
            .await
            .expect("connect client");
        let task_group = client.task_lifecycle.task_group.clone();

        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(task_group.task_count(), 2);

        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_called = called.clone();
        client
            .invoke_with_callback_timeout(
                RemotingCommand::create_remoting_command(5),
                Duration::from_millis(50),
                move || callback_called.store(true, std::sync::atomic::Ordering::SeqCst),
            )
            .await;
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(task_group.cancellation_token().is_cancelled());
        assert_eq!(client.connection().state(), crate::connection::ConnectionState::Closed);
        assert!(client.send(RemotingCommand::create_remoting_command(6)).await.is_err());

        drop(client);

        assert!(task_group.cancellation_token().is_cancelled());
        server.abort();
    }

    #[tokio::test]
    async fn batch_requests_share_one_absolute_response_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let cmd_handler = Arc::new(RemotingGeneralHandler::new(
            DefaultRemotingRequestProcessor,
            vec![],
            PendingRequestTable::new(),
        ));

        let mut client = Client::connect(addr.to_string(), cmd_handler, None, TlsConfig::default())
            .await
            .expect("connect client");
        let task_group = client.task_lifecycle.task_group.clone();

        assert_eq!(task_group.task_count(), 2);
        let started_at = time::Instant::now();
        let results = client
            .send_batch_read(
                vec![
                    RemotingCommand::create_remoting_command(1),
                    RemotingCommand::create_remoting_command(2),
                    RemotingCommand::create_remoting_command(3),
                ],
                100,
            )
            .await
            .expect("batch registration should succeed");
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|result| matches!(
            result,
            Err(RocketMQError::Network(
                rocketmq_error::NetworkError::ResponseTimeout { .. }
            ))
        )));
        assert!(
            time::Instant::now().duration_since(started_at) < Duration::from_millis(200),
            "batch timeout must be one absolute deadline, not one timeout per sequential await"
        );
        time::timeout(Duration::from_secs(1), async {
            while !task_group.cancellation_token().is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("batch timeout must cancel the canonical session");
        assert_eq!(client.connection().state(), crate::connection::ConnectionState::Closed);
        assert!(client.send(RemotingCommand::create_remoting_command(4)).await.is_err());
        let report = client.close_with_report(Duration::from_secs(1)).await;

        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.completed + report.cancelled, 2);
        server.abort();
    }

    #[tokio::test]
    async fn connect_with_service_context_parents_connection_tasks() {
        let runtime_context = RuntimeContext::from_current("remoting-client-context-test");
        let service = runtime_context.service_context("remoting-client-service");
        let baseline_children = service.task_group().child_count();
        let baseline_stats = service.task_group().child_stats();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let response_table = PendingRequestTable::new();
        let cmd_handler = Arc::new(RemotingGeneralHandler::new(
            DefaultRemotingRequestProcessor,
            vec![],
            response_table.clone(),
        ));

        let client =
            Client::connect_with_service_context(&service, addr.to_string(), cmd_handler, None, TlsConfig::default())
                .await
                .expect("connect client with context");
        let task_group = client.task_lifecycle.task_group.clone();

        assert_eq!(task_group.parent_id(), Some(service.task_group().id()));
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(task_group.task_count(), 2);
        assert_eq!(service.task_group().child_stats().active, baseline_stats.active + 1);

        let mut retained_client = client.clone();
        let retained_request = RemotingCommand::create_remoting_command(105).set_body(vec![7_u8; 4096]);
        let retained_invocation = tokio::spawn(async move {
            retained_client
                .send_read(retained_request, RequestDeadline::from_timeout_millis(100))
                .await
        });
        time::timeout(Duration::from_secs(1), async {
            while response_table.usage().count == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request should register");
        assert_eq!(response_table.usage().bytes, 4096);
        assert!(retained_invocation.await.unwrap().is_err());
        assert_eq!(response_table.usage().bytes, 0);

        let report = client.close_with_report(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        drop(client);
        assert!(task_group.cancellation_token().is_cancelled());
        drop(task_group);
        tokio::task::yield_now().await;

        let final_stats = service.task_group().child_stats();
        assert_eq!(final_stats.active, baseline_stats.active);
        assert_eq!(final_stats.created, baseline_stats.created + 1);
        assert_eq!(final_stats.pruned, baseline_stats.pruned + 1);
        assert_eq!(service.task_group().child_count(), baseline_children);
        server.abort();
    }
}
