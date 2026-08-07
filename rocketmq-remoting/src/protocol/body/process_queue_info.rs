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

/// Java RocketMQ 4.7.1 compatible `ProcessQueueInfo`.
/// Field names match the Java client's JSON serialization exactly.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessQueueInfo {
    pub commit_offset: u64,
    pub cached_msg_min_offset: u64,
    pub cached_msg_max_offset: u64,
    pub cached_msg_count: u32,
    /// Size in bytes (Java field name is `cachedMsgSizeInMiB` but carries bytes in practice).
    #[serde(rename = "cachedMsgSizeInMiB")]
    pub cached_msg_size_in_mib: u32,

    pub transaction_msg_min_offset: u64,
    pub transaction_msg_max_offset: u64,
    pub transaction_msg_count: u32,

    pub locked: bool,
    pub try_unlock_times: u64,
    pub last_lock_timestamp: u64,

    pub droped: bool,
    pub last_pull_timestamp: u64,
    pub last_consume_timestamp: u64,
}

impl std::fmt::Display for ProcessQueueInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProcessQueueInfo [commit_offset: {}, cached_msg_min_offset: {}, \
             cached_msg_max_offset: {}, cached_msg_count: {}, cached_msg_size_in_mib: {}, \
             transaction_msg_min_offset: {}, transaction_msg_max_offset: {}, \
             transaction_msg_count: {}, locked: {}, try_unlock_times: {}, \
             last_lock_timestamp: {}, droped: {}, last_pull_timestamp: {}, \
             last_consume_timestamp: {}]",
            self.commit_offset,
            self.cached_msg_min_offset,
            self.cached_msg_max_offset,
            self.cached_msg_count,
            self.cached_msg_size_in_mib,
            self.transaction_msg_min_offset,
            self.transaction_msg_max_offset,
            self.transaction_msg_count,
            self.locked,
            self.try_unlock_times,
            self.last_lock_timestamp,
            self.droped,
            self.last_pull_timestamp,
            self.last_consume_timestamp
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_queue_info_roundtrip_json() {
        let info = ProcessQueueInfo {
            commit_offset: 10,
            cached_msg_min_offset: 11,
            cached_msg_max_offset: 20,
            cached_msg_count: 9,
            cached_msg_size_in_mib: 1024,
            locked: true,
            droped: false,
            last_pull_timestamp: 1_000_000,
            last_consume_timestamp: 999_000,
            ..Default::default()
        };
        let json = serde_json::to_string(&info).unwrap();
        // Verify Java-compatible field names
        assert!(json.contains("\"commitOffset\""));
        assert!(json.contains("\"cachedMsgMinOffset\""));
        assert!(json.contains("\"cachedMsgMaxOffset\""));
        assert!(json.contains("\"cachedMsgCount\""));
        assert!(json.contains("\"cachedMsgSizeInMiB\""));
        assert!(json.contains("\"lastPullTimestamp\""));
        assert!(json.contains("\"lastConsumeTimestamp\""));
        assert!(json.contains("\"droped\""));

        let decoded: ProcessQueueInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.commit_offset, 10);
        assert_eq!(decoded.cached_msg_count, 9);
        assert!(decoded.locked);
    }
}
