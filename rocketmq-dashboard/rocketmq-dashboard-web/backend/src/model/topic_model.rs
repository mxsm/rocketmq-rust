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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicInfo {
    pub topic: String,
    pub broker_name: Option<String>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: u32,
    pub category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicListView {
    pub items: Vec<TopicInfo>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteInfo {
    pub topic: String,
    pub brokers: Vec<TopicRouteBroker>,
    pub queues: Vec<TopicRouteQueue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteBroker {
    pub broker_name: String,
    pub broker_addrs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteQueue {
    pub broker_name: String,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatsInfo {
    pub topic: String,
    pub queue_count: usize,
    pub total_min_offset: i64,
    pub total_max_offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicMutationRequest {
    pub topic: String,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub perm: u32,
    pub broker_name_list: Vec<String>,
    pub cluster_name_list: Vec<String>,
    pub order: Option<bool>,
    pub message_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationResult {
    pub message: String,
}
