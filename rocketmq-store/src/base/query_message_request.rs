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

/// Complete query-message contract passed from the Broker to a Store backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMessageRequest {
    pub topic: CheetahString,
    pub key: CheetahString,
    pub index_type: Option<CheetahString>,
    pub max_num: i32,
    pub begin: i64,
    pub end: i64,
    pub last_key: Option<CheetahString>,
}

impl QueryMessageRequest {
    pub fn legacy(topic: &CheetahString, key: &CheetahString, max_num: i32, begin: i64, end: i64) -> Self {
        Self {
            topic: topic.clone(),
            key: key.clone(),
            index_type: None,
            max_num,
            begin,
            end,
            last_key: None,
        }
    }

    /// Returns the legacy LocalFile lookup key used before index metadata was explicit.
    pub fn legacy_backend_key(&self) -> CheetahString {
        match self.index_type.as_deref() {
            Some("U" | "T") => CheetahString::from_string(format!(
                "{}#{}",
                self.index_type.as_deref().unwrap_or_default(),
                self.key
            )),
            _ => self.key.clone(),
        }
    }
}
