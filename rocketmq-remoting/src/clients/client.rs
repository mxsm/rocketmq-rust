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

use futures_util::SinkExt;
use tokio_stream::StreamExt;

use crate::connection::Connection;
use crate::error::Error::ConnectionInvalid;
use crate::error::Error::Io;
use crate::protocol::remoting_command::RemotingCommand;
use crate::Result;

pub struct Client {
    /// The TCP connection decorated with the rocketmq remoting protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,
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
    pub async fn connect<T: tokio::net::ToSocketAddrs>(addr: T) -> Result<Client> {
        let tcp_stream = tokio::net::TcpStream::connect(addr).await;
        if tcp_stream.is_err() {
            return Err(Io(tcp_stream.err().unwrap()));
        }
        Ok(Client {
            connection: Connection::new(tcp_stream.unwrap()),
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
    pub async fn send_read(&mut self, request: RemotingCommand) -> Result<RemotingCommand> {
        self.send(request).await?;
        let response = self.read().await?;
        Ok(response)
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
        match self.connection.framed.send(request).await {
            Ok(_) => Ok(()),
            Err(error) => match error {
                Io(value) => {
                    self.connection.ok = false;
                    Err(ConnectionInvalid(value.to_string()))
                }
                _ => Err(error),
            },
        }
    }

    /// Reads and retrieves the response from the remote server.
    ///
    /// # Returns
    ///
    /// The `RemotingCommand` representing the response, wrapped in a `Result`. Returns an error if
    /// reading the response fails.
    async fn read(&mut self) -> Result<RemotingCommand> {
        match self.connection.framed.next().await {
            None => {
                self.connection.ok = false;
                Err(ConnectionInvalid("connection disconnection".to_string()))
            }
            Some(result) => match result {
                Ok(response) => Ok(response),
                Err(error) => match error {
                    Io(value) => {
                        self.connection.ok = false;
                        Err(ConnectionInvalid(value.to_string()))
                    }
                    _ => Err(error),
                },
            },
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }
}
