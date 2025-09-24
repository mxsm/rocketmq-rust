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

use rocketmq_error::RocketMQResult;
use rocketmq_error::RocketmqError::ConnectionInvalid;
use rocketmq_error::RocketmqError::Io;
use rocketmq_error::RocketmqError::RemoteError;
use rocketmq_rust::ArcMut;
use tokio::sync::mpsc::Receiver;
use tracing::error;
use tracing::warn;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::base::response_future::ResponseFuture;
use crate::code::response_code::ResponseCode;
use crate::connection::Connection;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::protocol::remoting_command::RemotingCommand;
use crate::protocol::RemotingCommandType;
use crate::remoting::inner::RemotingGeneralHandler;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;

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
}

struct ClientInner<PR> {
    cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
    ctx: ConnectionHandlerContext,
}

impl<PR> ClientInner<PR> {
    pub async fn connect<T, PR>(
        addr: T,
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> RocketMQResult<ArcMut<ClientInner<PR>>>
    where
        T: tokio::net::ToSocketAddrs,
        PR: RequestProcessor + 'static,
    {
        let tcp_stream = tokio::net::TcpStream::connect(addr).await;
        if tcp_stream.is_err() {
            return Err(Io(tcp_stream.err().unwrap()));
        }
        let stream = tcp_stream?;
        let local_addr = stream.local_addr()?;
        let remote_address = stream.peer_addr()?;
        let connection = Connection::new(stream);
        let channel = Channel::new(connection, local_addr, remote_address);
        let client = ClientInner {
            cmd_handler,
            ctx: ArcMut::new(ConnectionHandlerContextWrapper::new(
                //connection,
                channel.clone(),
            )),
        };
        let client = ArcMut::new(client);
        let client_inner = client.clone();
        tokio::task::spawn(async move {
            loop {
                //Get the next frame from the connection.
                let channel = client_inner.ctx.channel_mut();
                let frame = tokio::select! {
                    res = channel.connection_mut().receive_command() => res,
                    _ = client_inner.shutdown.recv() =>{
                        //If a shutdown signal is received, return from `handle`.
                        channel.connection_mut().ok = false;
                        return Ok(());
                    }
                };
                let cmd = match frame {
                    Some(frame) => frame?,
                    None => {
                        //If the frame is None, it means the connection is closed.
                        return Ok(());
                    }
                };
                //process request and response
                client_inner
                    .cmd_handler
                    .process_message_received(&client_inner.ctx, cmd)
                    .await;
            }
        });

        Ok(client)
    }

    pub async fn send(
        &mut self,
        request: RemotingCommand,
        tx: Option<tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>>,
        timeout_millis: Option<u64>,
    ) -> RocketMQResult<()> {
        let opaque = request.opaque();
        if let Some(tx) = tx {
            self.response_table.insert(
                opaque,
                ResponseFuture::new(opaque, timeout_millis.unwrap_or(0), true, tx),
            );
        }
        match self.channel.0.connection.send_command(request).await {
            Ok(_) => Ok(()),
            Err(error) => match error {
                Io(value) => {
                    self.response_table.remove(&opaque);
                    self.channel.0.connection.ok = false;
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

impl<PR> Client<PR>
where
    PR: RequestProcessor + 'static,
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
    pub async fn connect<T>(
        addr: T,
        cmd_handler: ArcMut<RemotingGeneralHandler<PR>>,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> RocketMQResult<Client<PR>>
    where
        T: tokio::net::ToSocketAddrs,
    {
        let inner = ClientInner::connect(addr, cmd_handler, tx).await?;
        Ok(Client { inner })
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

        if let Err(err) = self
            .tx
            .send((request, Some(tx), Some(timeout_millis)))
            .await
        {
            return Err(RemoteError(err.to_string()));
        }
        match rx.await {
            Ok(value) => value,
            Err(error) => Err(RemoteError(error.to_string())),
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
        if let Err(err) = self.tx.send((request, None, None)).await {
            return Err(RemoteError(err.to_string()));
        }
        Ok(())
    }

    /// Reads and retrieves the response from the remote remoting_server.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// reading the response fails.
    async fn read(&mut self) -> RocketMQResult<RemotingCommand> {
        match self.inner.channel.0.connection.receive_command().await {
            None => {
                self.inner.channel.0.connection.ok = false;
                Err(ConnectionInvalid("connection disconnection".to_string()))
            }
            Some(result) => match result {
                Ok(response) => Ok(response),
                Err(error) => match error {
                    Io(value) => {
                        self.inner.channel.0.connection.ok = false;
                        Err(ConnectionInvalid(value.to_string()))
                    }
                    _ => Err(error),
                },
            },
        }
    }

    pub fn connection(&self) -> &Connection {
        self.inner.channel.0.connection_ref()
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        self.inner.channel.0.connection_mut()
    }
}
