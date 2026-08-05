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

/// Represents the header for a broker registration request.
#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct RegisterBrokerRequestHeader {
    /// The name of the broker.
    #[serde(rename = "brokerName")]
    #[required]
    pub broker_name: CheetahString,

    /// The address of the broker.
    #[serde(rename = "brokerAddr")]
    #[required]
    pub broker_addr: CheetahString,

    /// The name of the cluster to which the broker belongs.
    #[serde(rename = "clusterName")]
    #[required]
    pub cluster_name: CheetahString,

    /// The address of the highly available (HA) remoting_server associated with the broker.
    #[serde(rename = "haServerAddr")]
    #[required]
    pub ha_server_addr: CheetahString,

    /// The unique identifier for the broker.
    #[serde(rename = "brokerId")]
    #[required]
    pub broker_id: u64,

    /// The optional heartbeat timeout in milliseconds.
    #[serde(rename = "heartbeatTimeoutMillis")]
    pub heartbeat_timeout_millis: Option<i64>,

    /// The optional flag indicating whether acting as the master is enabled.
    #[serde(rename = "enableActingMaster")]
    pub enable_acting_master: Option<bool>,

    /// Indicates whether the data is compressed.
    #[required]
    pub compressed: bool,

    /// The CRC32 checksum for the message body.
    #[serde(rename = "bodyCrc32")]
    #[required]
    pub body_crc32: u32,
}

impl RegisterBrokerRequestHeader {
    /// Creates a new instance of `RegisterBrokerRequestHeader`.
    ///
    /// # Arguments
    ///
    /// * `broker_name` - The name of the broker.
    /// * `broker_addr` - The address of the broker.
    /// * `cluster_name` - The name of the cluster.
    /// * `ha_server_addr` - The address of the HA remoting_server.
    /// * `broker_id` - The unique identifier for the broker.
    /// * `heartbeat_timeout_millis` - The optional heartbeat timeout in milliseconds.
    /// * `enable_acting_master` - The optional flag indicating whether acting as the master is
    ///   enabled.
    /// * `compressed` - Indicates whether the data is compressed.
    /// * `body_crc32` - The CRC32 checksum for the message body.
    ///
    /// # Returns
    ///
    /// A new `RegisterBrokerRequestHeader` instance.
    pub fn new(
        broker_name: CheetahString,
        broker_addr: CheetahString,
        cluster_name: CheetahString,
        ha_server_addr: CheetahString,
        broker_id: u64,
        heartbeat_timeout_millis: Option<i64>,
        enable_acting_master: Option<bool>,
        compressed: bool,
        body_crc32: u32,
    ) -> Self {
        RegisterBrokerRequestHeader {
            broker_name,
            broker_addr,
            cluster_name,
            ha_server_addr,
            broker_id,
            heartbeat_timeout_millis,
            enable_acting_master,
            compressed,
            body_crc32,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct RegisterBrokerResponseHeader {
    pub ha_server_addr: Option<CheetahString>,
    pub master_addr: Option<CheetahString>,
}

impl RegisterBrokerResponseHeader {
    pub fn new(ha_server_addr: Option<CheetahString>, master_addr: Option<CheetahString>) -> Self {
        RegisterBrokerResponseHeader {
            ha_server_addr,
            master_addr,
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn register_broker_request_header_new() {
        let header = RegisterBrokerRequestHeader::new(
            CheetahString::from("broker1"),
            CheetahString::from("127.0.0.1"),
            CheetahString::from("cluster1"),
            CheetahString::from("127.0.0.2"),
            1,
            Some(3000),
            Some(true),
            true,
            12345,
        );
        assert_eq!(header.broker_name, CheetahString::from("broker1"));
        assert_eq!(header.broker_addr, CheetahString::from("127.0.0.1"));
        assert_eq!(header.cluster_name, CheetahString::from("cluster1"));
        assert_eq!(header.ha_server_addr, CheetahString::from("127.0.0.2"));
        assert_eq!(header.broker_id, 1);
        assert_eq!(header.heartbeat_timeout_millis, Some(3000));
        assert_eq!(header.enable_acting_master, Some(true));
        assert!(header.compressed);
        assert_eq!(header.body_crc32, 12345);
    }

    #[test]
    fn register_broker_request_header_serialization() {
        let header = RegisterBrokerRequestHeader::new(
            CheetahString::from("broker1"),
            CheetahString::from("127.0.0.1"),
            CheetahString::from("cluster1"),
            CheetahString::from("127.0.0.2"),
            1,
            Some(3000),
            Some(true),
            true,
            12345,
        );
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(
            serialized,
            r#"{"brokerName":"broker1","brokerAddr":"127.0.0.1","clusterName":"cluster1","haServerAddr":"127.0.0.2","brokerId":1,"heartbeatTimeoutMillis":3000,"enableActingMaster":true,"compressed":true,"bodyCrc32":12345}"#
        );
    }

    #[test]
    fn register_broker_request_header_deserialization() {
        let json = r#"{"brokerName":"broker1","brokerAddr":"127.0.0.1","clusterName":"cluster1","haServerAddr":"127.0.0.2","brokerId":1,"heartbeatTimeoutMillis":3000,"enableActingMaster":true,"compressed":true,"bodyCrc32":12345}"#;
        let deserialized: RegisterBrokerRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.broker_name, CheetahString::from("broker1"));
        assert_eq!(deserialized.broker_addr, CheetahString::from("127.0.0.1"));
        assert_eq!(deserialized.cluster_name, CheetahString::from("cluster1"));
        assert_eq!(deserialized.ha_server_addr, CheetahString::from("127.0.0.2"));
        assert_eq!(deserialized.broker_id, 1);
        assert_eq!(deserialized.heartbeat_timeout_millis, Some(3000));
        assert_eq!(deserialized.enable_acting_master, Some(true));
        assert!(deserialized.compressed);
        assert_eq!(deserialized.body_crc32, 12345);
    }

    #[test]
    fn register_broker_request_header_deserialization_missing_fields() {
        let json = r#"{"brokerName":"broker1","brokerAddr":"127.0.0.1","clusterName":"cluster1","haServerAddr":"127.0.0.2","brokerId":1,"compressed":true,"bodyCrc32":12345}"#;
        let deserialized: RegisterBrokerRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.broker_name, CheetahString::from("broker1"));
        assert_eq!(deserialized.broker_addr, CheetahString::from("127.0.0.1"));
        assert_eq!(deserialized.cluster_name, CheetahString::from("cluster1"));
        assert_eq!(deserialized.ha_server_addr, CheetahString::from("127.0.0.2"));
        assert_eq!(deserialized.broker_id, 1);
        assert_eq!(deserialized.heartbeat_timeout_millis, None);
        assert_eq!(deserialized.enable_acting_master, None);
        assert!(deserialized.compressed);
        assert_eq!(deserialized.body_crc32, 12345);
    }

    #[test]
    fn register_broker_response_header_new() {
        let header = RegisterBrokerResponseHeader::new(
            Some(CheetahString::from("127.0.0.2")),
            Some(CheetahString::from("127.0.0.3")),
        );
        assert_eq!(header.ha_server_addr, Some(CheetahString::from("127.0.0.2")));
        assert_eq!(header.master_addr, Some(CheetahString::from("127.0.0.3")));
    }

    #[test]
    fn register_broker_response_header_serialization() {
        let header = RegisterBrokerResponseHeader::new(
            Some(CheetahString::from("127.0.0.2")),
            Some(CheetahString::from("127.0.0.3")),
        );
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"haServerAddr":"127.0.0.2","masterAddr":"127.0.0.3"}"#);
    }

    #[test]
    fn register_broker_response_header_deserialization() {
        let json = r#"{"haServerAddr":"127.0.0.2","masterAddr":"127.0.0.3"}"#;
        let deserialized: RegisterBrokerResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.ha_server_addr, Some(CheetahString::from("127.0.0.2")));
        assert_eq!(deserialized.master_addr, Some(CheetahString::from("127.0.0.3")));
    }

    #[test]
    fn register_broker_response_header_deserialization_missing_fields() {
        let json = r#"{"haServerAddr":"127.0.0.2"}"#;
        let deserialized: RegisterBrokerResponseHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.ha_server_addr, Some(CheetahString::from("127.0.0.2")));
        assert_eq!(deserialized.master_addr, None);
    }
}
