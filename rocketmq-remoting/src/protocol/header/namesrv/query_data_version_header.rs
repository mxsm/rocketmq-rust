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

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct QueryDataVersionRequestHeader {
    #[required]
    pub broker_name: CheetahString,

    #[required]
    pub broker_addr: CheetahString,

    #[required]
    pub cluster_name: CheetahString,

    #[required]
    pub broker_id: u64,
}

impl QueryDataVersionRequestHeader {
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

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct QueryDataVersionResponseHeader {
    changed: bool,
}

impl QueryDataVersionResponseHeader {
    pub fn new(changed: bool) -> Self {
        Self { changed }
    }

    pub fn changed(&self) -> bool {
        self.changed
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn query_data_version_request_header_new() {
        let header = QueryDataVersionRequestHeader::new("broker1", "127.0.0.1", "cluster1", 1);
        assert_eq!(header.broker_name, CheetahString::from("broker1"));
        assert_eq!(header.broker_addr, CheetahString::from("127.0.0.1"));
        assert_eq!(header.cluster_name, CheetahString::from("cluster1"));
        assert_eq!(header.broker_id, 1);
    }

    #[test]
    fn query_data_version_request_header_serialization() {
        let header = QueryDataVersionRequestHeader::new("broker1", "127.0.0.1", "cluster1", 1);
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(
            serialized,
            r#"{"brokerName":"broker1","brokerAddr":"127.0.0.1","clusterName":"cluster1","brokerId":1}"#
        );
    }

    #[test]
    fn query_data_version_request_header_deserialization() {
        let json = r#"{"brokerName":"broker1","brokerAddr":"127.0.0.1","clusterName":"cluster1","brokerId":1}"#;
        let deserialized: QueryDataVersionRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.broker_name, CheetahString::from("broker1"));
        assert_eq!(deserialized.broker_addr, CheetahString::from("127.0.0.1"));
        assert_eq!(deserialized.cluster_name, CheetahString::from("cluster1"));
        assert_eq!(deserialized.broker_id, 1);
    }

    #[test]
    fn query_data_version_response_header_new() {
        let header = QueryDataVersionResponseHeader::new(true);
        assert!(header.changed);
    }

    #[test]
    fn query_data_version_response_header_serialization() {
        let header = QueryDataVersionResponseHeader::new(true);
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"changed":true}"#);
    }

    #[test]
    fn query_data_version_response_header_deserialization() {
        let json = r#"{"changed":true}"#;
        let deserialized: QueryDataVersionResponseHeader = serde_json::from_str(json).unwrap();
        assert!(deserialized.changed);
    }
}
