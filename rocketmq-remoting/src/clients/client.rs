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
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use crate::runtime::processor::RequestProcessor;

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
    inner: ArcMut<ClientInner>,
    tx: tokio::sync::mpsc::Sender<SendMessage>,
}

struct ClientInner {
    response_table: ArcMut<HashMap<i32, ResponseFuture>>,
    channel: (ArcMut<ChannelInner>, Channel),
    ctx: ConnectionHandlerContext,
    tx: tokio::sync::mpsc::Sender<SendMessage>,
}

type SendMessage = (
    RemotingCommand,
    Option<tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>>,
    Option<u64>,
);

async fn run_send(mut client: ArcMut<ClientInner>, mut rx: Receiver<SendMessage>) {
    while let Some((request, tx, timeout)) = rx.recv().await {
        let _ = client.send(request, tx, timeout).await;
    }
}

async fn run_recv<PR: RequestProcessor>(mut client: ArcMut<ClientInner>, mut processor: PR) {
    while let Some(response) = client.channel.0.connection.receive_command().await {
        match response {
            Ok(mut msg) => match msg.get_type() {
                // handle request
                RemotingCommandType::REQUEST => {
                    let opaque = msg.opaque();
                    let process_result = processor
                        .process_request(client.channel.1.clone(), client.ctx.clone(), &mut msg)
                        .await;
                    match process_result {
                        Ok(response) => {
                            if let Some(response) = response {
                                let _ = client
                                    .tx
                                    .send((response.set_opaque(opaque), None, None))
                                    .await;
                            }
                        }
                        Err(err) => {
                            error!("process request error: {:?}", err);
                            let command = RemotingCommand::create_response_command()
                                .set_opaque(opaque)
                                .set_code(ResponseCode::SystemBusy)
                                .set_remark_option(Some("System busy".to_string()));
                            client.tx.send((command, None, None)).await.unwrap();
                        }
                    }
                }
                // handle response
                RemotingCommandType::RESPONSE => {
                    let opaque = msg.opaque();
                    if let Some(response_future) = client.response_table.remove(&opaque) {
                        let _ = response_future.tx.send(Ok(msg));
                    } else {
                        warn!(
                            "receive response, cmd={}, but not matched any request, address={}",
                            msg,
                            client.channel.1.remote_address()
                        )
                    }
                }
            },
            Err(error) => match error {
                Io(value) => {
                    client.channel.0.connection.ok = false;
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
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> RocketMQResult<(tokio::sync::mpsc::Sender<SendMessage>, ArcMut<ClientInner>)>
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
        let response_table = ArcMut::new(HashMap::with_capacity(128));
        let channel_inner = ArcMut::new(ChannelInner::new(connection, response_table.clone()));
        let weak_channel = ArcMut::downgrade(&channel_inner);
        let channel = Channel::new(weak_channel, local_addr, remote_address);
        let (tx_, rx) = tokio::sync::mpsc::channel(1024);
        let client = ClientInner {
            ctx: ArcMut::new(ConnectionHandlerContextWrapper::new(
                //connection,
                channel.clone(),
            )),
            response_table,
            channel: (channel_inner, channel),
            tx: tx_.clone(),
        };
        let client = ArcMut::new(client);

        tokio::spawn(run_recv(client.clone(), processor));
        tokio::spawn(run_send(client.clone(), rx));
        if let Some(tx) = tx {
            let _ = tx.send(ConnectionNetEvent::CONNECTED(
                client.channel.1.remote_address(),
                //client.channel.clone(),
            ));
        }
        Ok((tx_, client))
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
    pub async fn connect<T, PR>(
        addr: T,
        processor: PR,
        tx: Option<&tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> RocketMQResult<Client>
    where
        T: tokio::net::ToSocketAddrs,
        PR: RequestProcessor + 'static,
    {
        let (tx, inner) = ClientInner::connect(addr, processor, tx).await?;
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
    ) -> RocketMQResult<RemotingCommand> {
        /*self.send(request).await?;
        let response = self.read().await?;
        Ok(response)*/

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
