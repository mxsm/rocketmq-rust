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

use crate::file::FileSegmentStatus;
use crate::file::FileSegmentType;

#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSegmentMetadata {
    pub path: String,
    pub segment_type: FileSegmentType,
    pub base_offset: u64,
    pub status: FileSegmentStatus,
    pub size: u64,
    pub create_timestamp: i64,
    pub begin_timestamp: i64,
    pub end_timestamp: i64,
    pub seal_timestamp: i64,
}

impl FileSegmentMetadata {
    pub fn new(path: String, segment_type: FileSegmentType, base_offset: u64) -> Self {
        Self {
            path,
            segment_type,
            base_offset,
            status: FileSegmentStatus::New,
            size: 0,
            create_timestamp: current_time_millis(),
            begin_timestamp: i64::MAX,
            end_timestamp: i64::MIN,
            seal_timestamp: 0,
        }
    }
}

fn current_time_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}
