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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::controller::node_info::NodeInfo;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ControllerMetadataInfo {
    pub controller_leader_id: Option<u64>,
    pub controller_leader_address: Option<String>,
    pub is_leader: bool,
    pub raft_peers: Vec<NodeInfo>,
}

#[cfg(test)]
mod tests {
    use crate::protocol::body::controller::controller_metadata_info::ControllerMetadataInfo;

    #[test]
    fn test_create_controller_metadata_info_with_default_values() {
        let controller_metadata_info = ControllerMetadataInfo::default();

        assert!(controller_metadata_info.controller_leader_id.is_none());
        assert!(controller_metadata_info.controller_leader_address.is_none());
        assert!(!controller_metadata_info.is_leader);
        assert!(controller_metadata_info.raft_peers.is_empty());
    }

    #[test]
    fn controller_metadata_roundtrip() {
        let json = r#"
    {
        "controllerLeaderId": 12,
        "controllerLeaderAddress": "XY",
        "isLeader": true,
        "raftPeers": [
            {   
                "nodeId": 0,
                "addr": "127.0.0.1"
            },
            {
                "nodeId": 1,
                "addr": "127.0.0.2"
            }
        ]
    }
    "#;

        let parsed: ControllerMetadataInfo = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&parsed).unwrap();

        let reparsed: ControllerMetadataInfo = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed, reparsed);
    }
}
