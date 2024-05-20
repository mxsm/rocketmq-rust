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

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfig {
    topic_name: Option<String>,
    read_queue_nums: u32,
    write_queue_nums: u32,
    perm: u32,
    topic_filter_type: TopicFilterType,
    topic_sys_flag: u32,
    order: bool,
    attributes: HashMap<String, String>,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            topic_name: None,
            read_queue_nums: Self::DEFAULT_READ_QUEUE_NUMS,
            write_queue_nums: Self::DEFAULT_WRITE_QUEUE_NUMS,
            perm: PermName::PERM_READ | PermName::PERM_WRITE,
            topic_filter_type: TopicFilterType::SingleTag,
            topic_sys_flag: 0,
            order: false,
            attributes: HashMap::new(),
        }
    }
}

impl TopicConfig {
    const SEPARATOR: &'static str = " ";
    const DEFAULT_READ_QUEUE_NUMS: u32 = 16;
    const DEFAULT_WRITE_QUEUE_NUMS: u32 = 16;
    pub fn new(topic_name: impl Into<String>) -> Self {
        TopicConfig {
            topic_name: Some(topic_name.into()),
            ..Self::default()
        }
    }

    pub fn with_queues(
        topic_name: impl Into<String>,
        read_queue_nums: u32,
        write_queue_nums: u32,
    ) -> Self {
        Self {
            read_queue_nums,
            write_queue_nums,
            ..Self::new(topic_name)
        }
    }

    pub fn with_perm(
        topic_name: impl Into<String>,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
    ) -> Self {
        Self {
            read_queue_nums,
            write_queue_nums,
            perm,
            ..Self::new(topic_name)
        }
    }

    pub fn with_sys_flag(
        topic_name: impl Into<String>,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
        topic_sys_flag: u32,
    ) -> Self {
        Self {
            read_queue_nums,
            write_queue_nums,
            perm,
            topic_sys_flag,
            ..Self::new(topic_name)
        }
    }

    pub fn encode(&self) -> String {
        let mut sb = String::new();
        sb.push_str(self.topic_name.as_deref().unwrap_or(""));
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&self.read_queue_nums.to_string());
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&self.write_queue_nums.to_string());
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&self.perm.to_string());
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&format!("{:?}", self.topic_filter_type));
        if !self.attributes.is_empty() {
            sb.push_str(Self::SEPARATOR);
            sb.push_str(&serde_json::to_string(&self.attributes).unwrap());
        }
        sb
    }

    pub fn decode(&mut self, input: &str) -> bool {
        let parts: Vec<&str> = input.split(Self::SEPARATOR).collect();
        if parts.len() >= 5 {
            self.topic_name = Some(parts[0].to_string());
            self.read_queue_nums = parts[1].parse().unwrap_or(Self::DEFAULT_READ_QUEUE_NUMS);
            self.write_queue_nums = parts[2].parse().unwrap_or(Self::DEFAULT_WRITE_QUEUE_NUMS);
            self.perm = parts[3]
                .parse()
                .unwrap_or(PermName::PERM_READ | PermName::PERM_WRITE);
            self.topic_filter_type = From::from(parts[4]);
            if parts.len() >= 6 {
                match serde_json::from_str(parts[5]) {
                    Ok(attrs) => self.attributes = attrs,
                    Err(_) => {}
                }
            }
            true
        } else {
            false
        }
    }

    pub fn topic_name(&self) -> Option<&String> {
        self.topic_name.as_ref()
    }
    pub fn read_queue_nums(&self) -> u32 {
        self.read_queue_nums
    }
    pub fn write_queue_nums(&self) -> u32 {
        self.write_queue_nums
    }
    pub fn perm(&self) -> u32 {
        self.perm
    }
    pub fn topic_filter_type(&self) -> TopicFilterType {
        self.topic_filter_type
    }
    pub fn topic_sys_flag(&self) -> u32 {
        self.topic_sys_flag
    }
    pub fn order(&self) -> bool {
        self.order
    }
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    pub fn set_topic_name(&mut self, topic_name: Option<String>) {
        self.topic_name = topic_name;
    }
    pub fn set_read_queue_nums(&mut self, read_queue_nums: u32) {
        self.read_queue_nums = read_queue_nums;
    }
    pub fn set_write_queue_nums(&mut self, write_queue_nums: u32) {
        self.write_queue_nums = write_queue_nums;
    }
    pub fn set_perm(&mut self, perm: u32) {
        self.perm = perm;
    }
    pub fn set_topic_filter_type(&mut self, topic_filter_type: TopicFilterType) {
        self.topic_filter_type = topic_filter_type;
    }
    pub fn set_topic_sys_flag(&mut self, topic_sys_flag: u32) {
        self.topic_sys_flag = topic_sys_flag;
    }
    pub fn set_order(&mut self, order: bool) {
        self.order = order;
    }
    pub fn set_attributes(&mut self, attributes: HashMap<String, String>) {
        self.attributes = attributes;
    }
}
