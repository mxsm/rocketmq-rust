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

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use rocketmq_common::common::server::config::ServerConfig;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::time;
use tokio_stream::StreamExt;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::connection::Connection;
use crate::protocol::remoting_command::RemotingCommand;
use crate::runtime::processor::RequestProcessor;

/// Default limit the max number of connections.
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

/// Shorthand for the transmit half of the message channel.
type Tx = mpsc::UnboundedSender<RemotingCommand>;

/// Shorthand for the receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<RemotingCommand>;

pub struct ConnectionHandler<RP> {
    request_processor: RP,
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
}

impl<RP> Drop for ConnectionHandler<RP> {
    fn drop(&mut self) {
        if let Some(ref sender) = self.conn_disconnect_notify {
            let socket_addr = self.connection.remote_addr;
            warn!(
                "connection[{}] disconnected, Send notify message.",
                socket_addr
            );
            let _ = sender.send(socket_addr);
        }
    }
}

impl<RP: RequestProcessor + Sync + 'static> ConnectionHandler<RP> {
    async fn handle(&mut self) -> anyhow::Result<()> {
        let _remote_addr = self.connection.remote_addr;
        while !self.shutdown.is_shutdown {
            let frame = tokio::select! {
                res = self.connection.framed.next() => res,
                _ = self.shutdown.recv() =>{
                    //If a shutdown signal is received, return from `handle`.
                    return Ok(());
                }
            };

            let cmd = match frame {
                Some(frame) => match frame {
                    Ok(cmd) => cmd,
                    Err(err) => {
                        error!("read frame failed: {}", err);
                        return Ok(());
                    }
                },
                None => {
                    //If the frame is None, it means the connection is closed.
                    return Ok(());
                }
            };
            let ctx = ConnectionHandlerContext::new(&self.connection);
            let opaque = cmd.opaque();
            let response = tokio::select! {
                result = self.request_processor.process_request( ctx,cmd) =>  result,
            };
            if response.is_none() {
                continue;
            }
            let response = response.unwrap();
            tokio::select! {
                result = self.connection.framed.send(response.set_opaque(opaque)) => match result{
                    Ok(_) =>{},
                    Err(err) => {
                        error!("send response failed: {}", err);
                        return Ok(())
                    },
                },
            }
        }
        Ok(())
    }
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
}

impl<RP: RequestProcessor + Sync + 'static + Clone> ConnectionListener<RP> {
    async fn run(&mut self) -> anyhow::Result<()> {
        info!("Prepare accepting connection");
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
            //create per connection handler state
            let mut handler = ConnectionHandler {
                request_processor: self.request_processor.clone(),
                connection: Connection::new(socket, remote_addr),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                conn_disconnect_notify: self.conn_disconnect_notify.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.handle().await {
                    error!(cause = ?err, "connection error");
                }
                warn!(
                    "The client[IP={}] disconnected from the server.",
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
    _phantom_data: std::marker::PhantomData<RP>,
}

impl<RP> RocketMQServer<RP> {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            config,
            _phantom_data: std::marker::PhantomData,
        }
    }
}

impl<RP: RequestProcessor + Sync + 'static> RocketMQServer<RP> {
    pub async fn run(&self, request_processor: RP) {
        let listener = TcpListener::bind(&format!(
            "{}:{}",
            self.config.bind_address, self.config.listen_port
        ))
        .await
        .unwrap();
        info!(
            "Bind local address: {}",
            format!("{}:{}", self.config.bind_address, self.config.listen_port)
        );
        let (notify_conn_disconnect, _) = broadcast::channel::<SocketAddr>(100);
        run(
            listener,
            tokio::signal::ctrl_c(),
            request_processor,
            Some(notify_conn_disconnect),
        )
        .await;
    }
}

pub async fn run<RP: RequestProcessor + Sync + 'static>(
    listener: TcpListener,
    shutdown: impl Future,
    request_processor: RP,
    conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
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
    };

    tokio::select! {
        res = listener.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
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

pub struct ConnectionHandlerContext<'a> {
    pub(crate) connection: &'a Connection,
}

impl<'a> ConnectionHandlerContext<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }

    pub fn remoting_address(&self) -> SocketAddr {
        self.connection.remote_addr
    }
}

impl<'a> AsRef<ConnectionHandlerContext<'a>> for ConnectionHandlerContext<'a> {
    fn as_ref(&self) -> &ConnectionHandlerContext<'a> {
        self
    }
}

impl<'a> AsMut<ConnectionHandlerContext<'a>> for ConnectionHandlerContext<'a> {
    fn as_mut(&mut self) -> &mut ConnectionHandlerContext<'a> {
        self
    }
}
