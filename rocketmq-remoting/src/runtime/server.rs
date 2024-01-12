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

use std::{collections::HashMap, future::Future, net::SocketAddr, sync::Arc, time::Duration};

use futures::SinkExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
    time,
};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use crate::{
    connection::Connection, protocol::remoting_command::RemotingCommand,
    runtime::processor::RequestProcessor,
};

/// Default limit the max number of connections.
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

/// Shorthand for the transmit half of the message channel.
type Tx = mpsc::UnboundedSender<RemotingCommand>;

/// Shorthand for the receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<RemotingCommand>;

pub struct ConnectionHandler {
    default_request_processor:
        Arc<tokio::sync::RwLock<Box<dyn RequestProcessor + Send + Sync + 'static>>>,
    processor_table:
        Arc<tokio::sync::RwLock<HashMap<i32, Box<dyn RequestProcessor + Send + Sync + 'static>>>>,
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl ConnectionHandler {
    async fn handle(&mut self) -> anyhow::Result<()> {
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
            let opaque = cmd.opaque();
            let response = match self.processor_table.write().await.get_mut(&cmd.code()) {
                None => self
                    .default_request_processor
                    .write()
                    .await
                    .process_request(cmd),
                Some(pr) => pr.process_request(cmd),
            };
            tokio::select! {
                result = self.connection.framed.send(response.set_opaque(opaque)) => match result{
                    Ok(_) =>{},
                    Err(err) => {
                        error!("send response failed: {}", err);
                        return Ok(())
                    },
                },
            };
        }
        Ok(())
    }
}

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
struct ConnectionListener {
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

    default_request_processor:
        Arc<tokio::sync::RwLock<Box<dyn RequestProcessor + Send + Sync + 'static>>>,
    processor_table:
        Arc<tokio::sync::RwLock<HashMap<i32, Box<dyn RequestProcessor + Send + Sync + 'static>>>>,
}

impl ConnectionListener {
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
                default_request_processor: self.default_request_processor.clone(),
                processor_table: self.processor_table.clone(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.handle().await {
                    error!(cause = ?err, "connection error");
                }
                warn!(
                    "The client[IP={}] disconnected from the server.",
                    remote_addr
                );
                drop(permit);
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

pub async fn run(
    listener: TcpListener,
    shutdown: impl Future,
    default_request_processor: impl RequestProcessor + Sync + Send + 'static,
    processor_table: HashMap<i32, Box<dyn RequestProcessor + Sync + Send + 'static>>,
) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    // Initialize the connection listener state
    let mut server = ConnectionListener {
        listener,
        notify_shutdown,
        default_request_processor: Arc::new(tokio::sync::RwLock::new(Box::new(
            default_request_processor,
        ))),
        shutdown_complete_tx,
        processor_table: Arc::new(tokio::sync::RwLock::new(processor_table)),
        limit_connections: Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
    };

    tokio::select! {
        res = server.run() => {
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
            info!("shutdown now");
        }
    }

    let ConnectionListener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;
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
