// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::net::SocketAddr;

use crate::base::connection_net_event::ConnectionNetEvent;
use crate::net::channel::Channel;

#[derive(Debug, Clone)]
pub struct TokioEvent {
    type_: ConnectionNetEvent,
    remote_addr: SocketAddr,
    channel: Channel,
}

impl TokioEvent {
    pub fn new(type_: ConnectionNetEvent, remote_addr: SocketAddr, channel: Channel) -> Self {
        Self {
            type_,
            remote_addr,
            channel,
        }
    }

    #[inline]
    pub fn type_(&self) -> &ConnectionNetEvent {
        &self.type_
    }

    #[inline]
    pub fn remote_addr(&self) -> &SocketAddr {
        &self.remote_addr
    }

    #[inline]
    pub fn channel(&self) -> &Channel {
        &self.channel
    }
}

impl Display for TokioEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TokioEvent {{ type_: {:?}, remote_addr: {:?}, channel: {:?} }}",
            self.type_, self.remote_addr, self.channel
        )
    }
}
