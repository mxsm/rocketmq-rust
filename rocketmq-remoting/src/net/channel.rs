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

use std::net::SocketAddr;

use uuid::Uuid;

#[derive(Clone, PartialEq, Hash)]
pub struct Channel {
    local_address: SocketAddr,
    remote_address: SocketAddr,
    channel_id: String,
}

impl Channel {
    pub fn new(local_address: SocketAddr, remote_address: SocketAddr) -> Self {
        let channel_id = Uuid::new_v4().to_string();
        Self {
            local_address,
            remote_address,
            channel_id,
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
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn channel_creation_with_new() {
        let local_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let remote_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)), 8080);
        let channel = Channel::new(local_address, remote_address);

        assert_eq!(channel.local_address(), local_address);
        assert_eq!(channel.remote_address(), remote_address);
        assert!(Uuid::parse_str(channel.channel_id()).is_ok());
    }

    #[test]
    fn channel_setters_work_correctly() {
        let local_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
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
        assert_eq!(channel.channel_id(), new_channel_id);
    }
}
