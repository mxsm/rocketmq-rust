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
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use rocketmq_rust::ArcMut;
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;
use tracing::error;
use uuid::Uuid;

use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting_error::RemotingError;
use crate::remoting_error::RemotingError::ChannelSendRequestFailed;
use crate::remoting_error::RemotingError::Io;
use crate::Result;

#[derive(Clone)]
pub struct Channel {
    local_address: SocketAddr,
    remote_address: SocketAddr,
    channel_id: String,
    tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    pub(crate) connection: ArcMut<Connection>,
    pub(crate) response_table: ArcMut<HashMap<i32, ResponseFuture>>,
}

type ChannelMessage = (
    RemotingCommand,
    Option<tokio::sync::oneshot::Sender<Result<RemotingCommand>>>,
    Option<u64>,
);

pub(crate) async fn run_send(
    mut connection: ArcMut<Connection>,
    mut rx: Receiver<ChannelMessage>,
    mut response_table: ArcMut<HashMap<i32, ResponseFuture>>,
) {
    while let Some((request, tx, timeout_millis)) = rx.recv().await {
        let opaque = request.opaque();
        if let Some(tx) = tx {
            response_table.insert(
                opaque,
                ResponseFuture::new(opaque, timeout_millis.unwrap_or(0), true, tx),
            );
        }
        match connection.writer.send(request).await {
            Ok(_) => {}
            Err(error) => match error {
                Io(error) => {
                    error!("send request failed: {}", error);
                    response_table.remove(&opaque);
                    connection.ok = false;
                    return;
                }
                _ => {
                    response_table.remove(&opaque);
                }
            },
        };
    }
}

impl PartialEq for Channel {
    fn eq(&self, other: &Self) -> bool {
        self.local_address == other.local_address
            && self.remote_address == other.remote_address
            && self.channel_id == other.channel_id
            && Arc::ptr_eq(self.connection.get_inner(), other.connection.get_inner())
            && Arc::ptr_eq(
                self.response_table.get_inner(),
                other.response_table.get_inner(),
            )
    }
}

impl Eq for Channel {}

impl Hash for Channel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.local_address.hash(state);
        self.remote_address.hash(state);
        self.channel_id.hash(state);
        Arc::as_ptr(self.connection.get_inner()).hash(state);
        Arc::as_ptr(self.response_table.get_inner()).hash(state);
    }
}

impl Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel {{ local_address: {:?}, remote_address: {:?}, channel_id: {} }}",
            self.local_address, self.remote_address, self.channel_id
        )
    }
}

impl Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel {{ local_address: {}, remote_address: {}, channel_id: {} }}",
            self.local_address, self.remote_address, self.channel_id
        )
    }
}

impl Channel {
    pub fn new(
        local_address: SocketAddr,
        remote_address: SocketAddr,
        connection: Connection,
        response_table: ArcMut<HashMap<i32, ResponseFuture>>,
    ) -> Self {
        let channel_id = Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        //let response_table = ArcMut::new(HashMap::with_capacity(32));
        let connection = ArcMut::new(connection);
        tokio::spawn(run_send(connection.clone(), rx, response_table.clone()));
        Self {
            local_address,
            remote_address,
            channel_id,
            tx,
            connection,
            response_table,
        }
    }
}

impl Channel {
    pub fn set_local_address(&mut self, local_address: SocketAddr) {
        self.local_address = local_address;
    }
    pub fn set_remote_address(&mut self, remote_address: SocketAddr) {
        self.remote_address = remote_address;
    }
    pub fn set_channel_id(&mut self, channel_id: String) {
        self.channel_id = channel_id;
    }

    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }
    pub fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }
    pub fn channel_id(&self) -> &str {
        self.channel_id.as_str()
    }

    pub fn connection(&self) -> ArcMut<Connection> {
        self.connection.clone()
    }
    pub fn connection_ref(&self) -> &Connection {
        self.connection.as_ref()
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        self.connection.as_mut()
    }

    pub fn connection_mut_from_ref(&self) -> &mut Connection {
        self.connection.mut_from_ref()
    }

    pub async fn send_wait_response(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<RemotingCommand>>();
        let opaque = request.opaque();
        if let Err(err) = self
            .tx
            .send((request, Some(tx), Some(timeout_millis)))
            .await
        {
            return Err(ChannelSendRequestFailed(err.to_string()));
        }

        match timeout(Duration::from_millis(timeout_millis), rx).await {
            Ok(result) => match result {
                Ok(response) => response,
                Err(e) => {
                    self.response_table.remove(&opaque);
                    Err(RemotingError::ChannelRecvRequestFailed(e.to_string()))
                }
            },
            Err(e) => {
                self.response_table.remove(&opaque);
                Err(RemotingError::ChannelRecvRequestFailed(e.to_string()))
            }
        }
    }

    pub async fn send_one_way(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<()> {
        let request = request.mark_oneway_rpc();
        if let Err(err) = self.tx.send((request, None, Some(timeout_millis))).await {
            error!("send one way request failed: {}", err);
            return Err(ChannelSendRequestFailed(err.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn channel_creation_with_new() {
        /*let local_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let remote_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)), 8080);
        let channel = Channel::new(local_address, remote_address);

        assert_eq!(channel.local_address(), local_address);
        assert_eq!(channel.remote_address(), remote_address);
        assert!(Uuid::parse_str(channel.channel_id()).is_ok());*/
    }

    #[test]
    fn channel_setters_work_correctly() {
        /* let local_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let remote_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)), 8080);
        let new_local_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080);
        let new_remote_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)), 8080);
        let new_channel_id = Uuid::new_v4().to_string();

        let mut channel = Channel::new(local_address, remote_address);
        channel.set_local_address(new_local_address);
        channel.set_remote_address(new_remote_address);
        channel.set_channel_id(new_channel_id.clone());

        assert_eq!(channel.local_address(), new_local_address);
        assert_eq!(channel.remote_address(), new_remote_address);
        assert_eq!(channel.channel_id(), new_channel_id);*/
    }
}
