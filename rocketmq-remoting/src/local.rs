//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::net::SocketAddr;

use rocketmq_rust::ArcMut;
use tokio::net::TcpListener;

use crate::base::response_future::ResponseFuture;
use crate::connection::Connection;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;
use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;

pub struct LocalRequestHarness {
    channel: Channel,
    context: ConnectionHandlerContext,
    peer: Connection,
}

impl LocalRequestHarness {
    pub async fn new() -> rocketmq_error::RocketMQResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let server_addr = listener.local_addr()?;

        let client_task = tokio::spawn(async move { tokio::net::TcpStream::connect(server_addr).await });
        let (server_stream, _) = listener.accept().await?;
        let client_stream = client_task.await.map_err(|error| {
            rocketmq_error::RocketMQError::Internal(format!("failed to join local remoting client task: {error}"))
        })??;

        let local_address = server_stream.local_addr()?;
        let remote_address = server_stream.peer_addr()?;
        let peer_local_address = client_stream.local_addr()?;
        let peer_remote_address = client_stream.peer_addr()?;

        debug_assert_eq!(local_address, peer_remote_address);
        debug_assert_eq!(remote_address, peer_local_address);

        let channel_inner = ArcMut::new(ChannelInner::new(
            Connection::new(server_stream),
            ArcMut::new(HashMap::<i32, ResponseFuture>::new()),
        ));
        let channel = Channel::new(channel_inner, local_address, remote_address);
        let context = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));

        Ok(Self {
            channel,
            context,
            peer: Connection::new(client_stream),
        })
    }

    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    pub fn context(&self) -> ConnectionHandlerContext {
        self.context.clone()
    }

    pub fn local_address(&self) -> SocketAddr {
        self.channel.local_address()
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.channel.remote_address()
    }

    pub async fn receive_response(
        &mut self,
    ) -> rocketmq_error::RocketMQResult<Option<crate::protocol::remoting_command::RemotingCommand>> {
        match self.peer.receive_command().await {
            Some(result) => result.map(Some),
            None => Ok(None),
        }
    }
}
