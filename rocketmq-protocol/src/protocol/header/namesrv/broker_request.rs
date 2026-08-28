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

use rocketmq_macros::RequestHeaderCodecV3;
use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader"
)]
pub struct UnRegisterBrokerRequestHeader {
    #[header(required)]
    pub broker_name: CheetahString,

    #[header(required)]
    pub broker_addr: CheetahString,

    #[header(required)]
    pub cluster_name: CheetahString,

    #[header(required, range = "i64")]
    pub broker_id: u64,
}

impl UnRegisterBrokerRequestHeader {
    pub fn new(
        broker_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        cluster_name: impl Into<CheetahString>,
        broker_id: u64,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_addr: broker_addr.into(),
            cluster_name: cluster_name.into(),
            broker_id,
        }
    }
}

impl Display for UnRegisterBrokerRequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnRegisterBrokerRequestHeader {{ brokerName: {}, brokerAddr: {}, clusterName: {}, brokerId: {} }}",
            self.broker_name, self.broker_addr, self.cluster_name, self.broker_id
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.BrokerHeartbeatRequestHeader"
)]
pub struct BrokerHeartbeatRequestHeader {
    #[header(required)]
    pub cluster_name: CheetahString,

    #[header(required)]
    pub broker_addr: CheetahString,

    #[header(required)]
    pub broker_name: CheetahString,
    pub broker_id: Option<i64>,
    pub epoch: Option<i32>,
    pub max_offset: Option<i64>,
    pub confirm_offset: Option<i64>,
    /// Rust-native Controller extension; Java peers and NameServers ignore it.
    pub store_ready: Option<bool>,
    pub heartbeat_timeout_mills: Option<i64>,
    pub election_priority: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.GetBrokerMemberGroupRequestHeader"
)]
pub struct GetBrokerMemberGroupRequestHeader {
    #[header(required)]
    pub cluster_name: CheetahString,
    #[header(required)]
    pub broker_name: CheetahString,
}

impl GetBrokerMemberGroupRequestHeader {
    pub fn new(cluster_name: impl Into<CheetahString>, broker_name: impl Into<CheetahString>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_instance_with_correct_values() {
        let header = GetBrokerMemberGroupRequestHeader::new("testCluster", "testBroker");
        assert_eq!(header.cluster_name, CheetahString::from("testCluster"));
        assert_eq!(header.broker_name, CheetahString::from("testBroker"));
    }

    #[test]
    fn unregister_broker_request_header_display() {
        let header = UnRegisterBrokerRequestHeader::new("name", "addr", "cluster", 1);
        let display = format!("{}", header);
        let expected =
            "UnRegisterBrokerRequestHeader { brokerName: name, brokerAddr: addr, clusterName: cluster, brokerId: 1 }";
        assert_eq!(display, expected);
    }
}
