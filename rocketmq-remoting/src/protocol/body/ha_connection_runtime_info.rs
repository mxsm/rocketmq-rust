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
pub struct HAConnectionRuntimeInfo {
    pub addr: String,
    pub slave_ack_offset: u64,
    pub diff: i64,
    pub in_sync: bool,
    pub transferred_byte_in_second: u64,
    pub transfer_from_where: u64,
}

impl Display for HAConnectionRuntimeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HAConnectionRuntimeInfo [addr={}, slaveAckOffset={}, diff={}, inSync={}, transferredBytesInSecond={}, \
             transferFromWhere={}]",
            self.addr,
            self.slave_ack_offset,
            self.diff,
            self.in_sync,
            self.transferred_byte_in_second,
            self.transfer_from_where
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_connection_runtime_info_initializes_correctly() {
        let info = HAConnectionRuntimeInfo {
            addr: "127.0.0.1:10911".to_string(),
            slave_ack_offset: 100,
            diff: 50,
            in_sync: true,
            transferred_byte_in_second: 1024,
            transfer_from_where: 200,
        };
        assert_eq!(info.addr, "127.0.0.1:10911");
        assert_eq!(info.slave_ack_offset, 100);
        assert_eq!(info.diff, 50);
        assert!(info.in_sync);
        assert_eq!(info.transferred_byte_in_second, 1024);
        assert_eq!(info.transfer_from_where, 200);
    }

    #[test]
    fn ha_connection_runtime_info_default_values() {
        let info = HAConnectionRuntimeInfo::default();
        assert_eq!(info.addr, "");
        assert_eq!(info.slave_ack_offset, 0);
        assert_eq!(info.diff, 0);
        assert!(!info.in_sync);
        assert_eq!(info.transferred_byte_in_second, 0);
        assert_eq!(info.transfer_from_where, 0);
    }

    #[test]
    fn ha_connection_runtime_info_display_formats_correctly() {
        let info = HAConnectionRuntimeInfo {
            addr: "127.0.0.1:10911".to_string(),
            slave_ack_offset: 100,
            diff: 50,
            in_sync: true,
            transferred_byte_in_second: 1024,
            transfer_from_where: 200,
        };
        let display = format!("{}", info);
        assert!(display.contains(
            "HAConnectionRuntimeInfo [addr=127.0.0.1:10911, slaveAckOffset=100, diff=50, inSync=true, \
             transferredBytesInSecond=1024, transferFromWhere=200]"
        ));
    }
}
