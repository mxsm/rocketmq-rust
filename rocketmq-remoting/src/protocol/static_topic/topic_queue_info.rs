/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

use rocketmq_common::common::mix_all::METADATA_SCOPE_GLOBAL;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct TopicQueueMappingInfo {
    pub topic: Option<String>,
    pub scope: Option<String>,
    #[serde(rename = "totalQueues")]
    pub total_queues: i32,
    pub bname: Option<String>,
    pub epoch: i64,
    pub dirty: bool,
    #[serde(rename = "currIdMap")]
    pub curr_id_map: Option<HashMap<i32, i32>>,
}

impl Default for TopicQueueMappingInfo {
    fn default() -> Self {
        Self {
            topic: None,
            scope: Some(METADATA_SCOPE_GLOBAL.to_string()),
            total_queues: 0,
            bname: None,
            epoch: 0,
            dirty: false,
            curr_id_map: None,
        }
    }
}

impl TopicQueueMappingInfo {
    pub fn new(topic: String, total_queues: i32, bname: String, epoch: i64) -> Self {
        Self {
            topic: Some(topic),
            scope: Some(METADATA_SCOPE_GLOBAL.to_string()),
            total_queues,
            bname: Some(bname),
            epoch,
            dirty: false,
            curr_id_map: None,
        }
    }
}
