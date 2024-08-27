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

use futures_util::SinkExt;
use futures_util::StreamExt;
use rocketmq_common::ArcRefCellWrapper;
use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
use crate::error::Error::ConnectionInvalid;
use crate::error::Error::Io;
use crate::error::Error::RemoteException;
use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;
use crate::runtime::processor::RequestProcessor;
use crate::runtime::server::ConnectionHandlerContextWrapper;
use crate::Result;

#[derive(Clone)]
pub struct Client {
    /// The TCP connection decorated with the rocketmq remoting protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    //connection: Connection,
    inner: ArcRefCellWrapper<ClientInner>,
    tx: tokio::sync::mpsc::Sender<SendMessage>,
}

struct ClientInner {
    response_table: HashMap<i32, ResponseFuture>,
    channel: Channel,
    ctx: ArcRefCellWrapper<ConnectionHandlerContextWrapper>,
}

type SendMessage = (
    RemotingCommand,
    Option<tokio::sync::oneshot::Sender<Result<RemotingCommand>>>,
    Option<u64>,
);

async fn run_send(mut client: ArcRefCellWrapper<ClientInner>, mut rx: Receiver<SendMessage>) {
    while let Some((request, tx, timeout)) = rx.recv().await {
        let _ = client.send(request, tx, timeout).await;
    }
}

async fn run_recv<PR: RequestProcessor>(
    mut client: ArcRefCellWrapper<ClientInner>,
    mut processor: PR,
) {
    while let Some(response) = client.ctx.connection.reader.next().await {
        match response {
            Ok(response) => {
                let opaque = response.opaque();
                let process_result = processor
                    .process_request(
                        client.channel.clone(),
                        ArcRefCellWrapper::downgrade(&client.ctx),
                        response,
                    )
                    .await;
                match process_result {
                    Ok(result) => {
                        if let Some(command) = result {
                            if let Some(response_future) = client.response_table.remove(&opaque) {
                                let _ = response_future.tx.send(Ok(command));
                            }
                        }
                    }
                    Err(err) => {
                        if let Some(response_future) = client.response_table.remove(&opaque) {
                            let _ = response_future
                                .tx
                                .send(Err(RemoteException(err.to_string())));
                        }
                    }
                }
            }
            Err(error) => match error {
                Io(value) => {
                    client.ctx.connection.ok = false;
                    error!("error: {:?}", value);
                    return;
                }
                _ => {
                    error!("error: {:?}", error);
                }
            },
        }
    }
}

impl ClientInner {
    pub async fn connect<T, PR>(
        addr: T,
        processor: PR,
    ) -> Result<(
        tokio::sync::mpsc::Sender<SendMessage>,
        ArcRefCellWrapper<ClientInner>,
    )>
    where
        T: tokio::net::ToSocketAddrs,
        PR: RequestProcessor + 'static,
    {
        let tcp_stream = tokio::net::TcpStream::connect(addr).await;
        if tcp_stream.is_err() {
            return Err(Io(tcp_stream.err().unwrap()));
        }
        let stream = tcp_stream?;
        let channel = Channel::new(stream.local_addr()?, stream.peer_addr()?);
        let connection = Connection::new(stream);
        let client = ClientInner {
            ctx: ArcRefCellWrapper::new(ConnectionHandlerContextWrapper::new(connection)),
            response_table: HashMap::new(),
            channel,
        };
        let client = ArcRefCellWrapper::new(client);
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(run_recv(client.clone(), processor));
        tokio::spawn(run_send(client.clone(), rx));
        Ok((tx, client))
    }

    pub async fn send(
        &mut self,
        request: RemotingCommand,
        tx: Option<tokio::sync::oneshot::Sender<Result<RemotingCommand>>>,
        timeout_millis: Option<u64>,
    ) -> Result<()> {
        let opaque = request.opaque();
        if let Some(tx) = tx {
            self.response_table.insert(
                opaque,
                ResponseFuture::new(opaque, timeout_millis.unwrap_or(0), false, tx),
            );
        }
        match self.ctx.connection.writer.send(request).await {
            Ok(_) => Ok(()),
            Err(error) => match error {
                Io(value) => {
                    self.response_table.remove(&opaque);
                    self.ctx.connection.ok = false;
                    Err(ConnectionInvalid(value.to_string()))
                }
                _ => {
                    self.response_table.remove(&opaque);
                    Err(error)
                }
            },
        }
    }
}

impl Client {
    /// Creates a new `Client` instance and connects to the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to connect to.
    ///
    /// # Returns
    ///
    /// A new `Client` instance wrapped in a `Result`. Returns an error if the connection fails.
    pub async fn connect<T, PR>(addr: T, processor: PR) -> Result<Client>
    where
        T: tokio::net::ToSocketAddrs,
        PR: RequestProcessor + 'static,
    {
        /*let tcp_stream = tokio::net::TcpStream::connect(addr).await;
        if tcp_stream.is_err() {
            return Err(Io(tcp_stream.err().unwrap()));
        }
        Ok(Client {
            connection: Connection::new(tcp_stream?),
        })*/
        let (tx, inner) = ClientInner::connect(addr, processor).await?;
        Ok(Client {
            //connection: inner.connection.clone(),
            inner,
            tx,
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
    ) -> Result<RemotingCommand> {
        /*self.send(request).await?;
        let response = self.read().await?;
        Ok(response)*/

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<RemotingCommand>>();

        if let Err(err) = self
            .tx
            .send((request, Some(tx), Some(timeout_millis)))
            .await
        {
            return Err(RemoteException(err.to_string()));
        }
        match rx.await {
            Ok(value) => value,
            Err(error) => Err(RemoteException(error.to_string())),
        }
    }

    /// Invokes a remote operation with the given `RemotingCommand` and provides a callback function
    /// for handling the response.
    ///
    /// # Arguments
    ///
    /// * `_request` - The `RemotingCommand` representing the request.
    /// * `_func` - The callback function to handle the response.
    ///
    /// This method is a placeholder and currently does not perform any functionality.
    pub async fn invoke_with_callback<F>(&self, _request: RemotingCommand, _func: F)
    where
        F: FnMut(),
    {
    }

    /// Sends a request to the remote server.
    ///
    /// # Arguments
    ///
    /// * `request` - The `RemotingCommand` representing the request.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure in sending the request.
    pub async fn send(&mut self, request: RemotingCommand) -> Result<()> {
        /*match self.inner.ctx.connection.writer.send(request).await {
            Ok(_) => Ok(()),
            Err(error) => match error {
                Io(value) => {
                    self.inner.ctx.connection.ok = false;
                    Err(ConnectionInvalid(value.to_string()))
                }
                _ => Err(error),
            },
        }*/
        if let Err(err) = self.tx.send((request, None, None)).await {
            return Err(RemoteException(err.to_string()));
        }
        Ok(())
    }

    /// Reads and retrieves the response from the remote server.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// reading the response fails.
    async fn read(&mut self) -> Result<RemotingCommand> {
        match self.inner.ctx.connection.reader.next().await {
            None => {
                self.inner.ctx.connection.ok = false;
                Err(ConnectionInvalid("connection disconnection".to_string()))
            }
            Some(result) => match result {
                Ok(response) => Ok(response),
                Err(error) => match error {
                    Io(value) => {
                        self.inner.ctx.connection.ok = false;
                        Err(ConnectionInvalid(value.to_string()))
                    }
                    _ => Err(error),
                },
            },
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.inner.ctx.connection
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.inner.ctx.connection
    }
}
