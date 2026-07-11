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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::kv_table::KVTable;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegisterBrokerResult {
    #[serde(rename = "haServerAddr")]
    pub ha_server_addr: CheetahString,

    #[serde(rename = "masterAddr")]
    pub master_addr: CheetahString,

    #[serde(rename = "kvTable")]
    pub kv_table: KVTable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_values() {
        let result = RegisterBrokerResult::default();
        assert!(result.ha_server_addr.is_empty());
        assert!(result.master_addr.is_empty());
        assert!(result.kv_table.table.is_empty());
    }

    #[test]
    fn test_clone_and_debug() {
        let mut table = HashMap::new();
        table.insert(CheetahString::from("key"), CheetahString::from("value"));

        let original = RegisterBrokerResult {
            ha_server_addr: CheetahString::from("127.0.0.1:10912"),
            master_addr: CheetahString::from("127.0.0.1:10911"),
            kv_table: KVTable { table },
        };

        let cloned = original.clone();
        assert_eq!(original.ha_server_addr, cloned.ha_server_addr);

        let debug_str = format!("{:?}", cloned);
        assert!(debug_str.contains("ha_server_addr"));
    }

    #[test]
    fn test_serialization() {
        let mut table = HashMap::new();
        table.insert(CheetahString::from("version"), CheetahString::from("1.0"));

        let result = RegisterBrokerResult {
            ha_server_addr: CheetahString::from("addr1"),
            master_addr: CheetahString::from("addr2"),
            kv_table: KVTable { table },
        };

        let json = serde_json::to_string(&result).unwrap();

        assert!(json.contains("\"haServerAddr\":\"addr1\""));
        assert!(json.contains("\"masterAddr\":\"addr2\""));
        assert!(json.contains("\"kvTable\""));
    }

    #[test]
    fn test_deserialization() {
        let json = r#"{
            "haServerAddr": "10.0.0.1:1001",
            "masterAddr": "10.0.0.1:1002",
            "kvTable": {
                "table": {
                    "brokerId": "0"
                }
            }
        }"#;

        let decoded: RegisterBrokerResult = serde_json::from_str(json).expect("Should deserialize");

        assert_eq!(decoded.ha_server_addr, "10.0.0.1:1001");
        assert_eq!(decoded.master_addr, "10.0.0.1:1002");
        assert_eq!(
            decoded.kv_table.table.get(&CheetahString::from("brokerId")).unwrap(),
            "0"
        );
    }
}
