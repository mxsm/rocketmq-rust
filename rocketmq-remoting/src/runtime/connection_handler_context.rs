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
use rocketmq_rust::WeakArcMut;
use tracing::error;

use crate::connection::Connection;
use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;

pub type ConnectionHandlerContext = WeakArcMut<ConnectionHandlerContextWrapper>;

#[derive(Hash, Eq, PartialEq)]
pub struct ConnectionHandlerContextWrapper {
    // pub(crate) connection: Connection,
    pub(crate) channel: Channel,
}

impl ConnectionHandlerContextWrapper {
    // pub fn new(connection: Connection, channel: Channel) -> Self {
    pub fn new(channel: Channel) -> Self {
        Self {
            //connection,
            channel,
        }
    }

    pub fn connection(&self) -> &Connection {
        self.channel.connection_ref()
    }

    pub async fn write(&mut self, cmd: RemotingCommand) {
        match self.channel.connection_mut().writer.send(cmd).await {
            Ok(_) => {}
            Err(error) => {
                error!("send response failed: {}", error);
            }
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }
}

impl AsRef<ConnectionHandlerContextWrapper> for ConnectionHandlerContextWrapper {
    fn as_ref(&self) -> &ConnectionHandlerContextWrapper {
        self
    }
}

impl AsMut<ConnectionHandlerContextWrapper> for ConnectionHandlerContextWrapper {
    fn as_mut(&mut self) -> &mut ConnectionHandlerContextWrapper {
        self
    }
}
