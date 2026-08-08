// Copyright 2026 The RocketMQ Rust Authors
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

use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

use socket2::SockRef;
use socket2::TcpKeepalive;
use tokio::net::TcpStream;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpKeepaliveConfig {
    pub idle: Duration,
    pub interval: Option<Duration>,
    pub retries: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SocketOptions {
    pub tcp_nodelay: bool,
    pub send_buffer_bytes: Option<NonZeroUsize>,
    pub receive_buffer_bytes: Option<NonZeroUsize>,
    pub keepalive: Option<TcpKeepaliveConfig>,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            send_buffer_bytes: None,
            receive_buffer_bytes: None,
            keepalive: None,
        }
    }
}

impl SocketOptions {
    pub(crate) fn apply(self, stream: &TcpStream) -> io::Result<()> {
        stream.set_nodelay(self.tcp_nodelay)?;
        let socket = SockRef::from(stream);
        if let Some(bytes) = self.send_buffer_bytes {
            socket.set_send_buffer_size(bytes.get())?;
        }
        if let Some(bytes) = self.receive_buffer_bytes {
            socket.set_recv_buffer_size(bytes.get())?;
        }
        if let Some(config) = self.keepalive {
            let mut keepalive = TcpKeepalive::new().with_time(config.idle);
            if let Some(interval) = config.interval {
                keepalive = keepalive.with_interval(interval);
            }
            if let Some(retries) = config.retries {
                keepalive = keepalive.with_retries(retries);
            }
            socket.set_tcp_keepalive(&keepalive)?;
        }
        Ok(())
    }
}
