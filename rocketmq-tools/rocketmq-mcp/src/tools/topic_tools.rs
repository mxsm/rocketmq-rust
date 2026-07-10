// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::model::contract::Page;
use crate::model::contract::PageRequest;

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListTopicsArgs {
    #[serde(default)]
    pub cluster: Option<String>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicListEntry {
    pub topic: String,
    pub cluster: Option<String>,
    pub consumer_group: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ListTopicsOutput {
    pub cluster: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub namesrv_addr: String,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<TopicListEntry>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DescribeTopicArgs {
    pub cluster: String,
    pub topic: String,
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryTopicRouteArgs {
    pub cluster: String,
    pub topic: String,
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicRouteBroker {
    pub cluster: String,
    pub broker_name: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub broker_addrs: BTreeMap<String, String>,
    pub zone_name: Option<String>,
    pub enable_acting_master: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicRouteQueue {
    pub broker_name: String,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub topic_sys_flag: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct QueryTopicRouteOutput {
    pub cluster: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub namesrv_addr: String,
    pub topic: String,
    pub brokers: Vec<TopicRouteBroker>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<TopicRouteQueue>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct DescribeTopicOutput {
    pub cluster: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub namesrv_addr: String,
    pub topic: String,
    pub broker_names: Vec<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub brokers: Vec<TopicRouteBroker>,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<TopicRouteQueue>,
    pub generated_at: String,
}
