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

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct NodeInfo {
    #[serde(rename = "nodeId")]
    pub node_id: u64,
    pub addr: String,
}

#[cfg(test)]
mod tests {
    use crate::protocol::body::controller::node_info::NodeInfo;

    #[test]
    fn test_create_node_info_with_default_values() {
        let node_info = NodeInfo::default();

        assert_eq!(0, node_info.node_id);
        assert_eq!("", node_info.addr);
    }

    #[test]
    fn test_node_info_roundtrip() {
        let json = r#"
    {
        "nodeId": 12,
        "addr": "127.0.0.1"
    }
    "#;

        let parsed: NodeInfo = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&parsed).unwrap();

        let reparsed: NodeInfo = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed, reparsed);
    }
}
