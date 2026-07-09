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

pub const LIST_TOPICS_TOOL: &str = "mq_list_topics";
pub const DESCRIBE_TOPIC_TOOL: &str = "mq_describe_topic";
pub const QUERY_TOPIC_ROUTE_TOOL: &str = "mq_query_topic_route";

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ListTopicsArgs {
    #[serde(default)]
    pub cluster: Option<String>,
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
    pub namesrv_addr: String,
    pub topic_count: usize,
    pub topics: Vec<TopicListEntry>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct DescribeTopicArgs {
    pub cluster: String,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct QueryTopicRouteArgs {
    pub cluster: String,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TopicRouteBroker {
    pub cluster: String,
    pub broker_name: String,
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
    pub namesrv_addr: String,
    pub topic: String,
    pub brokers: Vec<TopicRouteBroker>,
    pub queues: Vec<TopicRouteQueue>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct DescribeTopicOutput {
    pub cluster: String,
    pub namesrv_addr: String,
    pub topic: String,
    pub broker_names: Vec<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub brokers: Vec<TopicRouteBroker>,
    pub queues: Vec<TopicRouteQueue>,
    pub generated_at: String,
}
