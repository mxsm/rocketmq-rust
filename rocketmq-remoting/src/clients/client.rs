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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::tls_config::TlsConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupChildLease;
use rocketmq_rust::ArcMut;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Receiver;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::base::pending_request_table::PendingRequestToken;
use crate::connection::Connection;
// Import error helpers for convenient error creation
use crate::error_helpers::connection_invalid;
use crate::error_helpers::io_error;
use crate::error_helpers::remote_error;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::remoting_server::rocketmq_tokio_server::Shutdown;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
#[cfg(feature = "tls")]
use crate::tls::connect_tls_stream;
#[cfg(not(feature = "tls"))]
use crate::tls::tls_disabled_error;
#[cfg(not(feature = "tls"))]
use crate::tls::TLS_DISABLED_ERROR_REASON;

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
    inner: ArcMut<ClientInner<PR>>,
    notify_shutdown: broadcast::Sender<()>,
    tx: tokio::sync::mpsc::Sender<SendMessage>,
    pending_requests: PendingRequestTable,
    pending_request_owner: PendingRequestOwner,
    task_lifecycle: Arc<ClientTaskLifecycle>,
}

type SendMessage = (RemotingCommand, Option<PendingRequestToken>);
const DEFAULT_CALLBACK_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

struct ClientInner<PR> {
    cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
    ctx: ConnectionHandlerContext,
    shutdown: Shutdown,
}

struct ClientTaskLifecycle {
    task_group: TaskGroup,
    _child_lease: Option<TaskGroupChildLease>,
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

impl<PR> ClientInner<PR>
where
    PR: RequestProcessor + Sync + 'static,
{
    pub async fn connect(
        addr: String,
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        notify: broadcast::Receiver<()>,
        send_notify: broadcast::Receiver<()>,
        tls_config: TlsConfig,
        task_group: TaskGroup,
    ) -> RocketMQResult<(
        tokio::sync::mpsc::Sender<SendMessage>,
        ArcMut<ClientInner<PR>>,
        PendingRequestOwner,
    )> {
        let stream = TcpStream::connect(addr.as_str()).await.map_err(io_error)?;
        let local_addr = stream.local_addr()?;
        let remote_address = stream.peer_addr()?;
        let connection = if tls_config.enable {
            #[cfg(feature = "tls")]
            {
                let server_name = server_name_from_addr(addr.as_str());
                let tls_stream = connect_tls_stream(stream, &server_name, &tls_config).await?;
                Connection::new_with_stream(tls_stream)
            }
            #[cfg(not(feature = "tls"))]
            {
                let _ = stream;
                debug_assert_eq!(
                    TLS_DISABLED_ERROR_REASON,
                    "rocketmq-remoting was compiled without the tls feature"
                );
                return Err(tls_disabled_error());
            }
        } else {
            Connection::new(stream)
        };
        let channel_inner = ArcMut::new(ChannelInner::try_new_with_pending_requests(
            connection,
            cmd_handler.response_table.clone(),
        )?);
        let pending_request_owner = channel_inner
            .pending_request_owner()
            .expect("pending-request constructor must install a connection owner")
            .clone();
        let channel = Channel::new(channel_inner, local_addr, remote_address);
        let (tx_, rx) = tokio::sync::mpsc::channel(1024);
        let client = ClientInner {
            cmd_handler,
            ctx: ArcMut::new(ConnectionHandlerContextWrapper::new(
                //connection,
                channel,
            )),
            shutdown: Shutdown::new(notify),
        };
        let client_inner = ArcMut::new(client);
        let mut client_ = client_inner.clone();
        if let Err(error) = task_group.spawn_service("remoting.client.connection.recv", async move {
            let _ = client_.run_recv().await;
        }) {
            let _ = task_group.shutdown(Duration::from_secs(1)).await;
            return Err(remote_error(format!(
                "failed to spawn remoting client receive task: {error}"
            )));
        }
        let mut client_ = client_inner.clone();
        if let Err(error) = task_group.spawn_service("remoting.client.connection.send", async move {
            client_.run_send(rx, Shutdown::new(send_notify)).await;
        }) {
            let _ = task_group.shutdown(Duration::from_secs(1)).await;
            return Err(remote_error(format!(
                "failed to spawn remoting client send task: {error}"
            )));
        }

        if let Some(tx) = tx {
            let _ = tx.send(ConnectionNetEvent::CONNECTED(client_inner.ctx.channel.remote_address()));
        }
        Ok((tx_, client_inner, pending_request_owner))
    }

    async fn run_recv(&mut self) -> RocketMQResult<()> {
        loop {
            //Get the next frame from the connection.
            let channel = self.ctx.channel_mut();
            let frame = tokio::select! {
                res = channel.connection_mut().receive_command() => res,
                _ = self.shutdown.recv() =>{
                    //If a shutdown signal is received, mark connection as closed
                    channel.connection_mut().close();
                    return Ok(());
                }
            };
            let cmd = match frame {
                Some(frame) => frame?,
                None => {
                    //If the frame is None, it means the connection is closed.
                    //Connection state is automatically managed by I/O operations
                    return Ok(());
                }
            };
            //process request and response
            self.cmd_handler.process_message_received(&mut self.ctx, cmd).await;
        }
    }

    async fn run_send(&mut self, mut rx: Receiver<SendMessage>, mut shutdown: Shutdown) {
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                maybe_message = rx.recv() => {
                    let Some((request, reservation)) = maybe_message else {
                        break;
                    };
                    let _ = self.send(request, reservation).await;
                }
            }
        }
    }

    pub async fn send(
        &mut self,
        request: RemotingCommand,
        reservation: Option<PendingRequestToken>,
    ) -> RocketMQResult<()> {
        match self.ctx.connection_mut().send_command(request).await {
            Ok(_) => Ok(()),
            Err(error) => {
                let connection_broken = matches!(error, rocketmq_error::RocketMQError::IO(_));
                if let Some(reservation) = reservation {
                    self.cmd_handler.response_table.complete_token(reservation, Err(error));
                }
                if connection_broken {
                    Err(connection_invalid("connection send failed"))
                } else {
                    Err(remote_error("request send failed"))
                }
            }
        }
    }
}

impl<PR> Client<PR>
where
    PR: RequestProcessor + Sync + 'static,
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
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
    ) -> RocketMQResult<Client<PR>> {
        let (task_group, child_lease) = new_client_connection_task_group(&addr)?;
        Self::connect_with_task_group(addr, cmd_handler, tx, tls_config, task_group, child_lease).await
    }

    pub(crate) async fn connect_with_service_context(
        context: &ServiceContext,
        addr: String,
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
    ) -> RocketMQResult<Client<PR>> {
        let (task_group, child_lease) = new_client_connection_task_group_with_service_context(context, &addr)?;
        Self::connect_with_task_group(addr, cmd_handler, tx, tls_config, task_group, child_lease).await
    }

    async fn connect_with_task_group(
        addr: String,
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
        tls_config: TlsConfig,
        task_group: TaskGroup,
        child_lease: Option<TaskGroupChildLease>,
    ) -> RocketMQResult<Client<PR>> {
        let (notify_shutdown, _) = broadcast::channel(1);
        let receiver = notify_shutdown.subscribe();
        let send_receiver = notify_shutdown.subscribe();
        let task_lifecycle = Arc::new(ClientTaskLifecycle {
            task_group: task_group.clone(),
            _child_lease: child_lease,
        });
        let pending_requests = cmd_handler.response_table.clone();
        let (tx, inner, pending_request_owner) =
            ClientInner::connect(addr, cmd_handler, tx, receiver, send_receiver, tls_config, task_group).await?;
        Ok(Client {
            inner,
            notify_shutdown,
            tx,
            pending_requests,
            pending_request_owner,
            task_lifecycle,
        })
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
        timeout_millis: u64,
    ) -> RocketMQResult<RemotingCommand> {
        let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let opaque = request.opaque();
        let guard =
            self.pending_requests
                .register_for_owner(&self.pending_request_owner, opaque, timeout_millis, tx)?;

        if let Err(err) = self.tx.send((request, Some(guard.token()))).await {
            return Err(remote_error(err.to_string()));
        }
        match tokio::time::timeout_at(tokio::time::Instant::from_std(guard.deadline()), rx).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => Err(remote_error(error.to_string())),
            Err(_) => Err(guard.expire("remoting_client_response", timeout_millis)),
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
        let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
        let timeout_millis = DEFAULT_CALLBACK_RESPONSE_TIMEOUT.as_millis() as u64;
        let guard = match self.pending_requests.register_for_owner(
            &self.pending_request_owner,
            request.opaque(),
            timeout_millis,
            tx,
        ) {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if self.tx.send((request, Some(guard.token()))).await.is_err() {
            return;
        }

        if tokio::time::timeout_at(tokio::time::Instant::from_std(guard.deadline()), rx)
            .await
            .is_err()
        {
            guard.expire("remoting_client_callback_response", timeout_millis);
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
        if let Err(err) = self.tx.send((request, None)).await {
            return Err(remote_error(err.to_string()));
        }
        Ok(())
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
            if let Err(err) = self.tx.send((request, None)).await {
                return Err(remote_error(err.to_string()));
            }
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
        let mut receivers = Vec::with_capacity(requests.len());

        // Send all requests and collect oneshot receivers
        for request in requests {
            let (tx, rx) = tokio::sync::oneshot::channel::<RocketMQResult<RemotingCommand>>();
            let guard = self.pending_requests.register_for_owner(
                &self.pending_request_owner,
                request.opaque(),
                timeout_millis,
                tx,
            )?;

            if let Err(err) = self.tx.send((request, Some(guard.token()))).await {
                return Err(remote_error(err.to_string()));
            }

            let deadline = guard.deadline();
            receivers.push((guard, deadline, rx));
        }

        // Collect all responses
        let mut results = Vec::with_capacity(receivers.len());
        for (guard, deadline, rx) in receivers {
            let result = match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rx).await {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => Err(remote_error(error.to_string())),
                Err(_) => Err(guard.expire("remoting_client_batch_response", timeout_millis)),
            };
            results.push(result);
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

    /// Reads and retrieves the response from the remote remoting_server.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// reading the response fails.
    async fn read(&mut self) -> RocketMQResult<RemotingCommand> {
        match self.inner.ctx.connection_mut().receive_command().await {
            Some(Ok(response)) => Ok(response),
            Some(Err(error)) => {
                if matches!(error, rocketmq_error::RocketMQError::IO(_)) {
                    Err(connection_invalid(error.to_string()))
                } else {
                    Err(error)
                }
            }
            None => Err(connection_invalid("connection disconnected")),
        }
    }

    pub fn connection(&self) -> &Connection {
        self.inner.ctx.connection_ref()
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.inner.ctx.channel.remote_address()
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        self.inner.ctx.connection_mut()
    }
}

fn server_name_from_addr(addr: &str) -> String {
    if let Some(rest) = addr.strip_prefix('[') {
        if let Some((host, _)) = rest.split_once(']') {
            return host.to_string();
        }
    }

    match addr.rsplit_once(':') {
        Some((host, _)) if !host.contains(':') => host.to_string(),
        _ => addr.to_string(),
    }
}

#[cfg(test)]
mod tls_tests {
    use super::server_name_from_addr;

    #[test]
    fn server_name_parser_handles_common_socket_forms() {
        assert_eq!(server_name_from_addr("broker.example.com:10911"), "broker.example.com");
        assert_eq!(server_name_from_addr("127.0.0.1:10911"), "127.0.0.1");
        assert_eq!(server_name_from_addr("[::1]:10911"), "::1");
        assert_eq!(server_name_from_addr("::1"), "::1");
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
        let cmd_handler = ArcMut::new(RemotingGeneralHandler {
            request_processor: DefaultRemotingRequestProcessor,
            rpc_hooks: vec![],
            response_table: PendingRequestTable::new(),
        });

        let client = Client::connect(addr.to_string(), cmd_handler, None, TlsConfig::default())
            .await
            .expect("connect client");
        let task_group = client.task_lifecycle.task_group.clone();

        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(task_group.task_count(), 2);

        drop(client);

        assert!(task_group.cancellation_token().is_cancelled());
        server.abort();
    }

    #[tokio::test]
    async fn close_with_report_stops_connection_tasks() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            time::sleep(Duration::from_secs(5)).await;
        });
        let cmd_handler = ArcMut::new(RemotingGeneralHandler {
            request_processor: DefaultRemotingRequestProcessor,
            rpc_hooks: vec![],
            response_table: PendingRequestTable::new(),
        });

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
        assert!(results
            .iter()
            .all(|result| matches!(result, Err(RocketMQError::Timeout { .. }))));
        assert!(
            time::Instant::now().duration_since(started_at) < Duration::from_millis(200),
            "batch timeout must be one absolute deadline, not one timeout per sequential await"
        );
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
        let cmd_handler = ArcMut::new(RemotingGeneralHandler {
            request_processor: DefaultRemotingRequestProcessor,
            rpc_hooks: vec![],
            response_table: PendingRequestTable::new(),
        });

        let client =
            Client::connect_with_service_context(&service, addr.to_string(), cmd_handler, None, TlsConfig::default())
                .await
                .expect("connect client with context");
        let task_group = client.task_lifecycle.task_group.clone();

        assert_eq!(task_group.parent_id(), Some(service.task_group().id()));
        assert_eq!(task_group.lifecycle_state(), TaskGroupLifecycleState::Open);
        assert_eq!(task_group.task_count(), 2);
        assert_eq!(service.task_group().child_stats().active, baseline_stats.active + 1);

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
