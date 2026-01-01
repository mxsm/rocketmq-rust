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

use std::fmt::Display;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HAClientRuntimeInfo {
    pub master_addr: String,
    pub transferred_byte_in_second: u64,
    pub max_offset: u64,
    pub last_read_timestamp: u64,
    pub last_write_timestamp: u64,
    pub master_flush_offset: u64,
    pub is_activated: bool,
}

impl Display for HAClientRuntimeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HAClientRuntimeInfo [masterAddr={}, transferredBytesInSecond={}, maxOffset={}, lastReadTimestamp={}, \
             lastWriteTimestamp={}, masterFlushOffset={}, isActivated={}]",
            self.master_addr,
            self.transferred_byte_in_second,
            self.max_offset,
            self.last_read_timestamp,
            self.last_write_timestamp,
            self.master_flush_offset,
            self.is_activated
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_client_runtime_info_initializes_correctly() {
        let info = HAClientRuntimeInfo {
            master_addr: "127.0.0.1:10911".to_string(),
            transferred_byte_in_second: 1024,
            max_offset: 1000,
            last_read_timestamp: 1627849200,
            last_write_timestamp: 1627849300,
            master_flush_offset: 500,
            is_activated: true,
        };
        assert_eq!(info.master_addr, "127.0.0.1:10911");
        assert_eq!(info.transferred_byte_in_second, 1024);
        assert_eq!(info.max_offset, 1000);
        assert_eq!(info.last_read_timestamp, 1627849200);
        assert_eq!(info.last_write_timestamp, 1627849300);
        assert_eq!(info.master_flush_offset, 500);
        assert!(info.is_activated);
    }

    #[test]
    fn ha_client_runtime_info_default_values() {
        let info = HAClientRuntimeInfo::default();
        assert_eq!(info.master_addr, "");
        assert_eq!(info.transferred_byte_in_second, 0);
        assert_eq!(info.max_offset, 0);
        assert_eq!(info.last_read_timestamp, 0);
        assert_eq!(info.last_write_timestamp, 0);
        assert_eq!(info.master_flush_offset, 0);
        assert!(!info.is_activated);
    }

    #[test]
    fn ha_client_runtime_info_display_formats_correctly() {
        let info = HAClientRuntimeInfo {
            master_addr: "127.0.0.1:10911".to_string(),
            transferred_byte_in_second: 1024,
            max_offset: 1000,
            last_read_timestamp: 1627849200,
            last_write_timestamp: 1627849300,
            master_flush_offset: 500,
            is_activated: true,
        };
        let display = format!("{}", info);
        assert!(display.contains(
            "HAClientRuntimeInfo [masterAddr=127.0.0.1:10911, transferredBytesInSecond=1024, maxOffset=1000, \
             lastReadTimestamp=1627849200, lastWriteTimestamp=1627849300, masterFlushOffset=500, isActivated=true]"
        ));
    }
}
