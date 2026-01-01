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

use std::fmt;

use cheetah_string::CheetahString;
use rocketmq_remoting::net::channel::Channel;

pub trait RemotingChannel: Send + Sync {}

#[derive(Clone)]
pub struct BrokerLiveInfo {
    broker_name: CheetahString,

    broker_addr: CheetahString,

    heartbeat_timeout_millis: u64,

    channel: Channel,

    broker_id: i64,
    last_update_timestamp: u64,
    epoch: i32,

    max_offset: i64,
    confirm_offset: i64,

    election_priority: Option<i32>,
}

impl BrokerLiveInfo {
    pub fn new(
        broker_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        broker_id: i64,
        last_update_timestamp: u64,
        heartbeat_timeout_millis: u64,
        channel: Channel,
        epoch: i32,
        max_offset: i64,
        election_priority: Option<i32>,
        confirm_offset: Option<i64>,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_addr: broker_addr.into(),
            broker_id,
            last_update_timestamp,
            heartbeat_timeout_millis,
            channel,
            epoch,
            max_offset,
            confirm_offset: confirm_offset.unwrap_or(0),
            election_priority,
        }
    }
}

impl BrokerLiveInfo {
    // getters
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn broker_addr(&self) -> &str {
        &self.broker_addr
    }

    pub fn broker_id(&self) -> i64 {
        self.broker_id
    }

    pub fn heartbeat_timeout_millis(&self) -> u64 {
        self.heartbeat_timeout_millis
    }

    pub fn last_update_timestamp(&self) -> u64 {
        self.last_update_timestamp
    }

    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    pub fn max_offset(&self) -> i64 {
        self.max_offset
    }

    pub fn confirm_offset(&self) -> i64 {
        self.confirm_offset
    }

    pub fn election_priority(&self) -> Option<i32> {
        self.election_priority
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    // setters
    pub fn set_last_update_timestamp(&mut self, ts: u64) {
        self.last_update_timestamp = ts;
    }

    pub fn set_heartbeat_timeout_millis(&mut self, timeout_millis: u64) {
        self.heartbeat_timeout_millis = timeout_millis;
    }

    pub fn set_epoch(&mut self, epoch: i32) {
        self.epoch = epoch;
    }

    pub fn set_max_offset(&mut self, offset: i64) {
        self.max_offset = offset;
    }

    pub fn set_confirm_offset(&mut self, offset: i64) {
        self.confirm_offset = offset;
    }

    pub fn set_election_priority(&mut self, priority: Option<i32>) {
        self.election_priority = priority;
    }

    pub fn set_broker_addr(&mut self, addr: impl Into<CheetahString>) {
        self.broker_addr = addr.into();
    }

    pub fn set_channel(&mut self, channel: Channel) {
        self.channel = channel;
    }
}

impl fmt::Debug for BrokerLiveInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerLiveInfo")
            .field("broker_name", &self.broker_name)
            .field("broker_addr", &self.broker_addr)
            .field("heartbeat_timeout_millis", &self.heartbeat_timeout_millis)
            .field("broker_id", &self.broker_id)
            .field("last_update_timestamp", &self.last_update_timestamp)
            .field("epoch", &self.epoch)
            .field("max_offset", &self.max_offset)
            .field("confirm_offset", &self.confirm_offset)
            .field("election_priority", &self.election_priority)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use rocketmq_remoting::connection::Connection;
    use rocketmq_rust::ArcMut;

    use super::*;

    async fn create_test_channel() -> Channel {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = std::net::TcpListener::bind(addr).unwrap();
        let local_addr = listener.local_addr().unwrap();
        let std_stream = std::net::TcpStream::connect(local_addr).unwrap();
        std_stream.set_nonblocking(true).unwrap();
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).unwrap();
        let connection = Connection::new(tcp_stream);
        let response_table = ArcMut::new(HashMap::new());
        let inner = ArcMut::new(rocketmq_remoting::net::channel::ChannelInner::new(
            connection,
            response_table,
        ));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn broker_live_info_new() {
        let channel = create_test_channel().await;
        let info = BrokerLiveInfo::new(
            "test_broker",
            "127.0.0.1:10911",
            0,
            1000,
            3000,
            channel,
            1,
            100,
            Some(10),
            Some(50),
        );

        assert_eq!(info.broker_name(), "test_broker");
        assert_eq!(info.broker_addr(), "127.0.0.1:10911");
        assert_eq!(info.broker_id(), 0);
        assert_eq!(info.last_update_timestamp(), 1000);
        assert_eq!(info.heartbeat_timeout_millis(), 3000);
        assert_eq!(info.epoch(), 1);
        assert_eq!(info.max_offset(), 100);
        assert_eq!(info.confirm_offset(), 50);
        assert_eq!(info.election_priority(), Some(10));
    }

    #[tokio::test]
    async fn broker_live_info_setters_and_getters() {
        let channel = create_test_channel().await;
        let mut info = BrokerLiveInfo::new(
            "test_broker",
            "127.0.0.1:10911",
            0,
            1000,
            3000,
            channel.clone(),
            1,
            100,
            Some(10),
            Some(50),
        );

        assert_eq!(info.broker_name(), "test_broker");
        assert_eq!(info.broker_id(), 0);

        info.set_last_update_timestamp(2000);
        assert_eq!(info.last_update_timestamp(), 2000);

        info.set_heartbeat_timeout_millis(4000);
        assert_eq!(info.heartbeat_timeout_millis(), 4000);

        info.set_epoch(5);
        assert_eq!(info.epoch(), 5);

        info.set_max_offset(500);
        assert_eq!(info.max_offset(), 500);

        info.set_confirm_offset(300);
        assert_eq!(info.confirm_offset(), 300);

        info.set_election_priority(Some(20));
        assert_eq!(info.election_priority(), Some(20));

        info.set_broker_addr("192.168.1.100:10911");
        assert_eq!(info.broker_addr(), "192.168.1.100:10911");

        let new_channel = create_test_channel().await;
        let expected_channel = new_channel.clone();
        info.set_channel(new_channel);
        assert_eq!(info.channel(), &expected_channel);
    }

    #[tokio::test]
    async fn broker_live_info_clone() {
        let channel = create_test_channel().await;
        let info = BrokerLiveInfo::new(
            "test_broker",
            "127.0.0.1:10911",
            0,
            1000,
            3000,
            channel,
            1,
            100,
            Some(10),
            Some(50),
        );

        let cloned = info.clone();
        assert_eq!(cloned.broker_name(), info.broker_name());
        assert_eq!(cloned.broker_addr(), info.broker_addr());
        assert_eq!(cloned.broker_id(), info.broker_id());
        assert_eq!(cloned.epoch(), info.epoch());
        assert_eq!(cloned.max_offset(), info.max_offset());
    }

    #[tokio::test]
    async fn broker_live_info_debug_format() {
        let channel = create_test_channel().await;
        let info = BrokerLiveInfo::new(
            "test_broker",
            "127.0.0.1:10911",
            0,
            1000,
            3000,
            channel,
            1,
            100,
            Some(10),
            Some(50),
        );

        let debug_str = format!("{:?}", info);
        assert!(debug_str.contains("BrokerLiveInfo"));
        assert!(debug_str.contains("test_broker"));
        assert!(debug_str.contains("127.0.0.1:10911"));
    }
}
