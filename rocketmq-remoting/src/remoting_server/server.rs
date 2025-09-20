/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_error::RocketmqError;
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
use crate::base::response_future::ResponseFuture;
use crate::base::tokio_event::TokioEvent;
use crate::code::response_code::ResponseCode;
use crate::connection::Connection;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::RemotingCommandType;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::RPCHook;

/// Default limit the max number of connections.
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

/// Shorthand for the transmit half of the message channel.
type Tx = mpsc::UnboundedSender<RemotingCommand>;

/// Shorthand for the receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<RemotingCommand>;

pub struct ConnectionHandler<RP> {
    request_processor: RP,
    connection_handler_context: ConnectionHandlerContext,
    channel_inner: (ArcMut<ChannelInner>, Channel),
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    rpc_hooks: Arc<Vec<Box<dyn RPCHook>>>,
    response_table: ArcMut<HashMap<i32, ResponseFuture>>,
}

impl<RP> Drop for ConnectionHandler<RP> {
    fn drop(&mut self) {
        if let Some(ref sender) = self.conn_disconnect_notify {
            let socket_addr = self.channel_inner.1.remote_address();
            warn!(
                "connection[{}] disconnected, Send notify message.",
                socket_addr
            );
            let _ = sender.send(socket_addr);
        }
    }
}

impl<RP> ConnectionHandler<RP> {
    pub fn do_before_rpc_hooks(
        &self,
        channel: &Channel,
        request: Option<&mut RemotingCommand>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(request) = request {
            for hook in self.rpc_hooks.iter() {
                hook.do_before_request(channel.remote_address(), request)?;
            }
        }
        Ok(())
    }

    pub fn do_after_rpc_hooks(
        &self,
        channel: &Channel,
        request: &RemotingCommand,
        response: Option<&mut RemotingCommand>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(response) = response {
            for hook in self.rpc_hooks.iter() {
                hook.do_after_response(channel.remote_address(), request, response)?;
            }
        }
        Ok(())
    }
}

impl<RP: RequestProcessor + Sync + 'static> ConnectionHandler<RP> {
    async fn handle(&mut self) -> rocketmq_error::RocketMQResult<()> {
        while !self.shutdown.is_shutdown {
            //Get the next frame from the connection.
            let frame = tokio::select! {
                res = self.channel_inner.0.connection.receive_command() => res,
                _ = self.shutdown.recv() =>{
                    //If a shutdown signal is received, return from `handle`.
                    self.channel_inner.0.connection_mut().ok = false;
                    return Ok(());
                }
            };

            let mut cmd = match frame {
                Some(frame) => frame?,
                None => {
                    //If the frame is None, it means the connection is closed.
                    return Ok(());
                }
            };
            // process response command
            if cmd.get_type() == RemotingCommandType::RESPONSE {
                let future_response = self.response_table.remove(&cmd.opaque());
                if let Some(future_response) = future_response {
                    let _ = future_response.tx.send(Ok(cmd));
                } else {
                    warn!(
                        "receive response, cmd={}, but not matched any request, address={}",
                        cmd,
                        self.channel_inner.1.remote_address()
                    )
                }
                continue;
            }

            let opaque = cmd.opaque();
            let reject_request = self.request_processor.reject_request(cmd.code());
            const REJECT_REQUEST_MSG: &str =
                "[REJECT REQUEST]system busy, start flow control for a while";
            if reject_request.0 {
                let response = if let Some(response) = reject_request.1 {
                    response
                } else {
                    RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SystemBusy,
                        REJECT_REQUEST_MSG,
                    )
                };
                self.channel_inner
                    .0
                    .connection
                    .send_command(response.set_opaque(opaque))
                    .await?;
                continue;
            }
            let oneway_rpc = cmd.is_oneway_rpc();
            //before handle request hooks

            let exception = self
                .do_before_rpc_hooks(&(self.channel_inner.1), Some(&mut cmd))
                .err();
            //handle error if return have
            match self.handle_error(oneway_rpc, opaque, exception).await {
                HandleErrorResult::Continue => continue,
                HandleErrorResult::ReturnMethod => return Ok(()),
                HandleErrorResult::GoHead => {}
            }

            let mut response = {
                let channel = self.channel_inner.1.clone();
                let ctx = self.connection_handler_context.clone();
                let result = self
                    .request_processor
                    .process_request(channel, ctx, &mut cmd)
                    .await
                    .unwrap_or_else(|_err| {
                        Some(RemotingCommand::create_response_command_with_code(
                            ResponseCode::SystemError,
                        ))
                    });
                result
            };

            let exception = self
                .do_after_rpc_hooks(&self.channel_inner.1, &cmd, response.as_mut())
                .err();

            match self.handle_error(oneway_rpc, opaque, exception).await {
                HandleErrorResult::Continue => continue,
                HandleErrorResult::ReturnMethod => return Ok(()),
                HandleErrorResult::GoHead => {}
            }
            if response.is_none() || oneway_rpc {
                continue;
            }
            let response = response.unwrap();
            let result = self
                .channel_inner
                .0
                .connection
                .send_command(response.set_opaque(opaque))
                .await;
            match result {
                Ok(_) => {}
                Err(err) => match err {
                    RocketmqError::Io(io_error) => {
                        error!("connection disconnect: {}", io_error);
                        return Ok(());
                    }
                    _ => {
                        error!("send response failed: {}", err);
                    }
                },
            };
        }
        Ok(())
    }

    async fn handle_error(
        &mut self,
        oneway_rpc: bool,
        opaque: i32,
        exception: Option<RocketmqError>,
    ) -> HandleErrorResult {
        if let Some(exception_inner) = exception {
            match exception_inner {
                RocketmqError::AbortProcessError(code, message) => {
                    if oneway_rpc {
                        return HandleErrorResult::Continue;
                    }
                    let response =
                        RemotingCommand::create_response_command_with_code_remark(code, message);
                    tokio::select! {
                        result =self.channel_inner.0.connection.send_command(response.set_opaque(opaque)) => match result{
                            Ok(_) =>{},
                            Err(err) => {
                                match err {
                                    RocketmqError::Io(io_error) => {
                                        error!("send response failed: {}", io_error);
                                        return HandleErrorResult::ReturnMethod;
                                    }
                                    _ => { error!("send response failed: {}", err);}
                                }
                            },
                        },
                    }
                }
                _ => {
                    if !oneway_rpc {
                        let response = RemotingCommand::create_response_command_with_code_remark(
                            ResponseCode::SystemError,
                            exception_inner.to_string(),
                        );
                        tokio::select! {
                            result =self.channel_inner.0.connection.send_command(response.set_opaque(opaque)) => match result{
                                Ok(_) =>{},
                                Err(err) => {
                                    match err {
                                        RocketmqError::Io(io_error) => {
                                            error!("send response failed: {}", io_error);
                                            return HandleErrorResult::ReturnMethod;
                                        }
                                        _ => { error!("send response failed: {}", err);}
                                    }
                                },
                            },
                        }
                    }
                }
            }
            HandleErrorResult::Continue
        } else {
            HandleErrorResult::GoHead
        }
    }
}

enum HandleErrorResult {
    Continue,
    ReturnMethod,
    GoHead,
}

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
struct ConnectionListener<RP> {
    /// The TCP listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,

    notify_shutdown: broadcast::Sender<()>,

    shutdown_complete_tx: mpsc::Sender<()>,

    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,

    request_processor: RP,

    rpc_hooks: Arc<Vec<Box<dyn RPCHook>>>,

    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
}

impl<RP: RequestProcessor + Sync + 'static + Clone> ConnectionListener<RP> {
    async fn run(&mut self) -> anyhow::Result<()> {
        info!("Prepare accepting connection");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TokioEvent>();

        if let Some(listener) = self.channel_event_listener.take() {
            tokio::spawn(async move {
                loop {
                    if let Some(event) = rx.recv().await {
                        info!("Accepting connection event: {:?}", event);
                        let addr = event.remote_addr();
                        match event.type_() {
                            ConnectionNetEvent::CONNECTED(_) => {
                                listener
                                    .on_channel_connect(addr.to_string().as_str(), event.channel());
                            }
                            ConnectionNetEvent::DISCONNECTED => {
                                listener
                                    .on_channel_close(addr.to_string().as_str(), event.channel());
                            }
                            ConnectionNetEvent::EXCEPTION => {
                                listener.on_channel_exception(
                                    addr.to_string().as_str(),
                                    event.channel(),
                                );
                            }
                        }
                    }
                }
            });
        }

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let (socket, remote_addr) = self.accept().await?;
            info!("Accepted connection, client ip:{}", remote_addr);
            socket.set_nodelay(true).expect("set nodelay failed");
            let local_addr = socket.local_addr()?;
            let response_table = ArcMut::new(HashMap::with_capacity(128));
            let channel_inner = ArcMut::new(ChannelInner::new(
                Connection::new(socket),
                response_table.clone(),
            ));
            //create per connection handler state
            let weak_channel = ArcMut::downgrade(&channel_inner);
            let channel = Channel::new(weak_channel, local_addr, remote_addr);
            let _ = tx.send(TokioEvent::new(
                ConnectionNetEvent::CONNECTED(remote_addr),
                remote_addr,
                channel.clone(),
            ));
            let mut handler = ConnectionHandler {
                request_processor: self.request_processor.clone(),
                connection_handler_context: ArcMut::new(ConnectionHandlerContextWrapper {
                    channel: channel.clone(),
                }),
                channel_inner: (channel_inner, channel),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                conn_disconnect_notify: self.conn_disconnect_notify.clone(),
                rpc_hooks: self.rpc_hooks.clone(),
                response_table,
            };
            let sender = tx.clone();
            tokio::spawn(async move {
                if let Err(err) = handler.handle().await {
                    error!(cause = ?err, "connection error");
                }
                let _ = sender.send(TokioEvent::new(
                    ConnectionNetEvent::DISCONNECTED,
                    remote_addr,
                    handler.channel_inner.1.clone(),
                ));
                warn!(
                    "The client[IP={}] disconnected from the remoting_server.",
                    remote_addr
                );
                /*  if let Some(ref sender) = handler.conn_disconnect_notify {
                    let _ = sender.send(remote_addr);
                }*/
                drop(permit);
                drop(handler);
            });
        }
    }

    async fn accept(&mut self) -> anyhow::Result<(TcpStream, SocketAddr)> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, remote_addr)) => return Ok((socket, remote_addr)),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

pub struct RocketMQServer<RP> {
    config: Arc<ServerConfig>,
    rpc_hooks: Option<Vec<Box<dyn RPCHook>>>,
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

    pub fn register_rpc_hook(&mut self, hook: Box<dyn RPCHook>) {
        if let Some(ref mut hooks) = self.rpc_hooks {
            hooks.push(hook);
        } else {
            self.rpc_hooks = Some(vec![hook]);
        }
    }
}

impl<RP: RequestProcessor + Sync + 'static + Clone> RocketMQServer<RP> {
    pub async fn run(
        &mut self,
        request_processor: RP,
        channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
    ) {
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
    rpc_hooks: Vec<Box<dyn RPCHook>>,
    channel_event_listener: Option<Arc<dyn ChannelEventListener>>,
) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    // Initialize the connection listener state
    let mut listener = ConnectionListener {
        listener,
        notify_shutdown,
        shutdown_complete_tx,
        conn_disconnect_notify,
        limit_connections: Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
        request_processor,
        rpc_hooks: Arc::new(rpc_hooks),
        channel_event_listener,
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
