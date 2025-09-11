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
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketmqError;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;
use tracing::error;
use uuid::Uuid;

use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
use crate::protocol::remoting_command::RemotingCommand;

pub type ChannelId = CheetahString;

#[derive(Clone)]
pub struct Channel {
    inner: WeakArcMut<ChannelInner>,
    local_address: SocketAddr,
    remote_address: SocketAddr,
    channel_id: ChannelId,
}

impl Channel {
    pub fn new(
        inner: WeakArcMut<ChannelInner>,
        local_address: SocketAddr,
        remote_address: SocketAddr,
    ) -> Self {
        let channel_id = Uuid::new_v4().to_string().into();
        Self {
            inner,
            local_address,
            remote_address,
            channel_id,
        }
    }

    #[inline]
    pub fn set_local_address(&mut self, local_address: SocketAddr) {
        self.local_address = local_address;
    }

    #[inline]
    pub fn set_remote_address(&mut self, remote_address: SocketAddr) {
        self.remote_address = remote_address;
    }

    #[inline]
    pub fn set_channel_id(&mut self, channel_id: impl Into<CheetahString>) {
        self.channel_id = channel_id.into();
    }

    #[inline]
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    #[inline]
    pub fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }

    #[inline]
    pub fn channel_id(&self) -> &str {
        self.channel_id.as_str()
    }

    #[inline]
    pub fn upgrade(&self) -> Option<ArcMut<ChannelInner>> {
        self.inner.upgrade()
    }
}

impl PartialEq for Channel {
    fn eq(&self, other: &Self) -> bool {
        self.local_address == other.local_address
            && self.remote_address == other.remote_address
            && self.channel_id == other.channel_id
    }
}

impl Eq for Channel {}

impl Hash for Channel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.local_address.hash(state);
        self.remote_address.hash(state);
        self.channel_id.hash(state);
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

pub struct ChannelInner {
    tx: tokio::sync::mpsc::Sender<ChannelMessage>,
    pub(crate) connection: ArcMut<Connection>,
    pub(crate) response_table: ArcMut<HashMap<i32, ResponseFuture>>,
}

type ChannelMessage = (
    RemotingCommand, /* command */
    Option<tokio::sync::oneshot::Sender<rocketmq_error::RocketMQResult<RemotingCommand>>>, /* tx */
    Option<u64>,     /* timeout_millis */
);

pub(crate) async fn handle_send(
    mut connection: ArcMut<Connection>,
    mut rx: Receiver<ChannelMessage>,
    mut response_table: ArcMut<HashMap<i32, ResponseFuture>>,
) {
    while let Some((send, tx, timeout_millis)) = rx.recv().await {
        let opaque = send.opaque();
        if let Some(tx) = tx {
            response_table.insert(
                opaque,
                ResponseFuture::new(opaque, timeout_millis.unwrap_or(0), true, tx),
            );
        }
        match connection.send_command(send).await {
            Ok(_) => {}
            Err(error) => match error {
                RocketmqError::Io(error) => {
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

impl ChannelInner {
    pub fn new(
        connection: Connection,
        response_table: ArcMut<HashMap<i32, ResponseFuture>>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        //let response_table = ArcMut::new(HashMap::with_capacity(32));
        let connection = ArcMut::new(connection);
        tokio::spawn(handle_send(connection.clone(), rx, response_table.clone()));
        Self {
            tx,
            connection,
            response_table,
        }
    }
}

impl ChannelInner {
    #[inline]
    pub fn connection(&self) -> ArcMut<Connection> {
        self.connection.clone()
    }

    #[inline]
    pub fn connection_ref(&self) -> &Connection {
        self.connection.as_ref()
    }

    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        self.connection.as_mut()
    }

    #[inline]
    pub fn connection_mut_from_ref(&self) -> &mut Connection {
        self.connection.mut_from_ref()
    }

    pub async fn send_wait_response(
        &mut self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let (tx, rx) =
            tokio::sync::oneshot::channel::<rocketmq_error::RocketMQResult<RemotingCommand>>();
        let opaque = request.opaque();
        if let Err(err) = self
            .tx
            .send((request, Some(tx), Some(timeout_millis)))
            .await
        {
            return Err(RocketmqError::ChannelSendRequestFailed(err.to_string()));
        }

        match timeout(Duration::from_millis(timeout_millis), rx).await {
            Ok(result) => match result {
                Ok(response) => response,
                Err(e) => {
                    self.response_table.remove(&opaque);
                    Err(RocketmqError::ChannelRecvRequestFailed(e.to_string()))
                }
            },
            Err(e) => {
                self.response_table.remove(&opaque);
                Err(RocketmqError::ChannelRecvRequestFailed(e.to_string()))
            }
        }
    }

    pub async fn send_one_way(
        &self,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = request.mark_oneway_rpc();
        if let Err(err) = self.tx.send((request, None, Some(timeout_millis))).await {
            error!("send one way request failed: {}", err);
            return Err(RocketmqError::ChannelSendRequestFailed(err.to_string()));
        }
        Ok(())
    }

    pub async fn send(
        &self,
        request: RemotingCommand,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        // let request = request.mark_oneway_rpc();
        if let Err(err) = self.tx.send((request, None, timeout_millis)).await {
            error!("send request failed: {}", err);
            return Err(RocketmqError::ChannelSendRequestFailed(err.to_string()));
        }
        Ok(())
    }

    #[inline]
    pub fn is_ok(&self) -> bool {
        self.connection.ok
    }
}
