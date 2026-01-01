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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::ha_client_runtime_info::HAClientRuntimeInfo;
use crate::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HARuntimeInfo {
    pub master: bool,
    pub master_commit_log_max_offset: u64,
    pub in_sync_slave_nums: i32,
    pub ha_connection_info: Vec<HAConnectionRuntimeInfo>,
    pub ha_client_runtime_info: HAClientRuntimeInfo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_runtime_info_initializes_correctly() {
        let info = HARuntimeInfo {
            master: true,
            master_commit_log_max_offset: 1000,
            in_sync_slave_nums: 3,
            ha_connection_info: vec![HAConnectionRuntimeInfo {
                addr: "127.0.0.1:10912".to_string(),
                slave_ack_offset: 500,
                diff: 100,
                in_sync: true,
                transferred_byte_in_second: 2048,
                transfer_from_where: 300,
            }],
            ha_client_runtime_info: HAClientRuntimeInfo {
                master_addr: "127.0.0.1:10911".to_string(),
                transferred_byte_in_second: 1024,
                max_offset: 1000,
                last_read_timestamp: 1627849200,
                last_write_timestamp: 1627849300,
                master_flush_offset: 500,
                is_activated: true,
            },
        };
        assert!(info.master);
        assert_eq!(info.master_commit_log_max_offset, 1000);
        assert_eq!(info.in_sync_slave_nums, 3);
        assert_eq!(info.ha_connection_info.len(), 1);
        assert_eq!(info.ha_connection_info[0].addr, "127.0.0.1:10912");
        assert_eq!(info.ha_client_runtime_info.master_addr, "127.0.0.1:10911");
    }

    #[test]
    fn ha_runtime_info_default_values() {
        let info = HARuntimeInfo::default();
        assert!(!info.master);
        assert_eq!(info.master_commit_log_max_offset, 0);
        assert_eq!(info.in_sync_slave_nums, 0);
        assert!(info.ha_connection_info.is_empty());
        assert_eq!(info.ha_client_runtime_info.master_addr, "");
    }

    #[test]
    fn ha_runtime_info_display_formats_correctly() {
        let info = HARuntimeInfo {
            master: true,
            master_commit_log_max_offset: 1000,
            in_sync_slave_nums: 3,
            ha_connection_info: vec![HAConnectionRuntimeInfo {
                addr: "127.0.0.1:10912".to_string(),
                slave_ack_offset: 500,
                diff: 100,
                in_sync: true,
                transferred_byte_in_second: 2048,
                transfer_from_where: 300,
            }],
            ha_client_runtime_info: HAClientRuntimeInfo {
                master_addr: "127.0.0.1:10911".to_string(),
                transferred_byte_in_second: 1024,
                max_offset: 1000,
                last_read_timestamp: 1627849200,
                last_write_timestamp: 1627849300,
                master_flush_offset: 500,
                is_activated: true,
            },
        };
        let display = format!("{info:?}");
        assert!(display.contains("HARuntimeInfo"));
        assert!(display.contains("master: true"));
        assert!(display.contains("master_commit_log_max_offset: 1000"));
        assert!(display.contains("in_sync_slave_nums: 3"));
        assert!(display.contains("ha_connection_info"));
        assert!(display.contains("ha_client_runtime_info"));
    }
}
