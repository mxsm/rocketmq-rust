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

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieredIndexEntry {
    pub topic: String,
    pub key: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub commit_log_offset: u64,
    pub message_size: usize,
    pub store_timestamp: i64,
}

impl TieredIndexEntry {
    #[inline]
    pub fn in_time_range(&self, begin: i64, end: i64) -> bool {
        self.store_timestamp >= begin && self.store_timestamp <= end
    }
}

#[derive(Debug, Clone)]
pub struct IndexFileSegment {
    pub path: String,
    pub base_offset: u64,
}
