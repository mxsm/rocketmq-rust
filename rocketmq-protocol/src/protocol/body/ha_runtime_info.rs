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
    #[serde(default)]
    pub pending_group_transfer_request_count: u64,
    #[serde(default)]
    pub pending_group_transfer_oldest_wait_millis: u64,
    #[serde(default)]
    pub group_transfer_ack_notify_count: u64,
    pub ha_connection_info: Vec<HAConnectionRuntimeInfo>,
    pub ha_client_runtime_info: HAClientRuntimeInfo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_json_defaults_new_group_transfer_metrics() {
        let value = serde_json::json!({
            "master": true,
            "masterCommitLogMaxOffset": 1000,
            "inSyncSlaveNums": 1,
            "haConnectionInfo": [],
            "haClientRuntimeInfo": {
                "masterAddr": "127.0.0.1:10911",
                "transferredByteInSecond": 1024,
                "maxOffset": 1000,
                "lastReadTimestamp": 10,
                "lastWriteTimestamp": 20,
                "masterFlushOffset": 900,
                "isActivated": true
            }
        });

        let info: HARuntimeInfo = serde_json::from_value(value).expect("deserialize legacy HA runtime info");
        assert_eq!(info.pending_group_transfer_request_count, 0);
        assert_eq!(info.pending_group_transfer_oldest_wait_millis, 0);
        assert_eq!(info.group_transfer_ack_notify_count, 0);

        let encoded = serde_json::to_value(info).expect("serialize HA runtime info");
        assert_eq!(encoded["pendingGroupTransferRequestCount"], 0);
        assert_eq!(encoded["pendingGroupTransferOldestWaitMillis"], 0);
        assert_eq!(encoded["groupTransferAckNotifyCount"], 0);
    }
}
