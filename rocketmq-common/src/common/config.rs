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

use serde::{Deserialize, Serialize};

use super::TopicFilterType;
use crate::common::constant::PermName;

const SEPARATOR: &str = " ";
const DEFAULT_READ_QUEUE_NUMS: u32 = 16;
const DEFAULT_WRITE_QUEUE_NUMS: u32 = 16;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TopicConfig {
    #[serde(rename = "topicName")]
    pub topic_name: String,
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: u32,
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: u32,
    pub perm: u32,
    #[serde(rename = "topicFilterType")]
    pub topic_filter_type: TopicFilterType,
    #[serde(rename = "topicSysFlag")]
    pub topic_sys_flag: u32,
    pub order: bool,
    pub attributes: HashMap<String, String>,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            topic_name: "".to_string(),
            read_queue_nums: DEFAULT_READ_QUEUE_NUMS,
            write_queue_nums: DEFAULT_WRITE_QUEUE_NUMS,
            perm: (PermName::PERM_READ | PermName::PERM_WRITE) as u32,
            topic_filter_type: TopicFilterType::SingleTag,
            topic_sys_flag: 0,
            order: false,
            attributes: HashMap::new(),
        }
    }
}

impl TopicConfig {
    pub fn new(topic_name: impl Into<String>) -> Self {
        TopicConfig {
            topic_name: topic_name.into(),
            ..Self::default()
        }
    }

    pub fn new_with(
        topic_name: impl Into<String>,
        read_queue_nums: u32,
        write_queue_nums: u32,
    ) -> Self {
        TopicConfig {
            topic_name: topic_name.into(),
            read_queue_nums,
            write_queue_nums,
            ..Default::default()
        }
    }

    pub fn new_with_perm(
        topic_name: impl Into<String>,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
    ) -> Self {
        TopicConfig {
            topic_name: topic_name.into(),
            read_queue_nums,
            write_queue_nums,
            perm,
            ..Default::default()
        }
    }
}
