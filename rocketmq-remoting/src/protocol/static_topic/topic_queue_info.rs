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

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all::METADATA_SCOPE_GLOBAL;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct TopicQueueMappingInfo {
    pub topic: Option<CheetahString>,
    pub scope: Option<CheetahString>,
    #[serde(rename = "totalQueues")]
    pub total_queues: i32,
    pub bname: Option<CheetahString>,
    pub epoch: i64,
    pub dirty: bool,
    #[serde(rename = "currIdMap")]
    pub curr_id_map: Option<HashMap<i32, i32>>,
}

impl Default for TopicQueueMappingInfo {
    fn default() -> Self {
        Self {
            topic: None,
            scope: Some(CheetahString::from_static_str(METADATA_SCOPE_GLOBAL)),
            total_queues: 0,
            bname: None,
            epoch: 0,
            dirty: false,
            curr_id_map: None,
        }
    }
}

impl TopicQueueMappingInfo {
    pub fn new(topic: CheetahString, total_queues: i32, bname: CheetahString, epoch: i64) -> Self {
        Self {
            topic: Some(topic),
            scope: Some(CheetahString::from_static_str(METADATA_SCOPE_GLOBAL)),
            total_queues,
            bname: Some(bname),
            epoch,
            dirty: false,
            curr_id_map: None,
        }
    }
}
