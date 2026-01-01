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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
pub struct BrokerHeartbeatRequestHeader {
    #[serde(rename = "clusterName")]
    #[required]
    pub cluster_name: CheetahString,

    #[serde(rename = "brokerAddr")]
    #[required]
    pub broker_addr: CheetahString,

    #[serde(rename = "brokerName")]
    #[required]
    pub broker_name: CheetahString,

    #[serde(rename = "brokerId")]
    pub broker_id: Option<i64>,

    pub epoch: Option<i32>,

    #[serde(rename = "maxOffset")]
    pub max_offset: Option<i64>,

    #[serde(rename = "confirmOffset")]
    pub confirm_offset: Option<i64>,

    #[serde(rename = "heartbeatTimeoutMills")]
    pub heartbeat_timeout_mills: Option<i64>,

    #[serde(rename = "electionPriority")]
    pub election_priority: Option<i32>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn broker_heartbeat_request_header_with_required_fields() {
        let header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from("testCluster"),
            broker_addr: CheetahString::from("testAddr"),
            broker_name: CheetahString::from("testBroker"),
            broker_id: Some(1),
            epoch: Some(1),
            max_offset: Some(100),
            confirm_offset: Some(50),
            heartbeat_timeout_mills: Some(3000),
            election_priority: Some(1),
        };
        assert_eq!(header.cluster_name, CheetahString::from("testCluster"));
        assert_eq!(header.broker_addr, CheetahString::from("testAddr"));
        assert_eq!(header.broker_name, CheetahString::from("testBroker"));
        assert_eq!(header.broker_id, Some(1));
        assert_eq!(header.epoch, Some(1));
        assert_eq!(header.max_offset, Some(100));
        assert_eq!(header.confirm_offset, Some(50));
        assert_eq!(header.heartbeat_timeout_mills, Some(3000));
        assert_eq!(header.election_priority, Some(1));
    }

    #[test]
    fn broker_heartbeat_request_header_with_optional_fields() {
        let header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from("testCluster"),
            broker_addr: CheetahString::from("testAddr"),
            broker_name: CheetahString::from("testBroker"),
            broker_id: None,
            epoch: None,
            max_offset: None,
            confirm_offset: None,
            heartbeat_timeout_mills: None,
            election_priority: None,
        };
        assert_eq!(header.cluster_name, CheetahString::from("testCluster"));
        assert_eq!(header.broker_addr, CheetahString::from("testAddr"));
        assert_eq!(header.broker_name, CheetahString::from("testBroker"));
        assert!(header.broker_id.is_none());
        assert!(header.epoch.is_none());
        assert!(header.max_offset.is_none());
        assert!(header.confirm_offset.is_none());
        assert!(header.heartbeat_timeout_mills.is_none());
        assert!(header.election_priority.is_none());
    }

    #[test]
    fn broker_heartbeat_request_header_with_empty_values() {
        let header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from(""),
            broker_addr: CheetahString::from(""),
            broker_name: CheetahString::from(""),
            broker_id: None,
            epoch: None,
            max_offset: None,
            confirm_offset: None,
            heartbeat_timeout_mills: None,
            election_priority: None,
        };
        assert_eq!(header.cluster_name, CheetahString::from(""));
        assert_eq!(header.broker_addr, CheetahString::from(""));
        assert_eq!(header.broker_name, CheetahString::from(""));
        assert!(header.broker_id.is_none());
        assert!(header.epoch.is_none());
        assert!(header.max_offset.is_none());
        assert!(header.confirm_offset.is_none());
        assert!(header.heartbeat_timeout_mills.is_none());
        assert!(header.election_priority.is_none());
    }

    #[test]
    fn broker_heartbeat_request_header_with_long_values() {
        let long_string = "a".repeat(1000);
        let header = BrokerHeartbeatRequestHeader {
            cluster_name: CheetahString::from(&long_string),
            broker_addr: CheetahString::from(&long_string),
            broker_name: CheetahString::from(&long_string),
            broker_id: Some(1),
            epoch: Some(1),
            max_offset: Some(100),
            confirm_offset: Some(50),
            heartbeat_timeout_mills: Some(3000),
            election_priority: Some(1),
        };
        assert_eq!(header.cluster_name, CheetahString::from(&long_string));
        assert_eq!(header.broker_addr, CheetahString::from(&long_string));
        assert_eq!(header.broker_name, CheetahString::from(&long_string));
        assert_eq!(header.broker_id, Some(1));
        assert_eq!(header.epoch, Some(1));
        assert_eq!(header.max_offset, Some(100));
        assert_eq!(header.confirm_offset, Some(50));
        assert_eq!(header.heartbeat_timeout_mills, Some(3000));
        assert_eq!(header.election_priority, Some(1));
    }
}
