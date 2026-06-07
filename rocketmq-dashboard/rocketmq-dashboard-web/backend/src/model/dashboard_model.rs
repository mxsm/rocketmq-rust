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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardOverview {
    pub current_namesrv: Option<String>,
    pub broker_count: usize,
    pub topic_count: usize,
    pub consumer_group_count: usize,
    pub producer_count: usize,
    pub message_backlog: i64,
    pub system_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardTopicCurrent {
    pub total_topics: usize,
    pub top_topics: Vec<TopicCurrentMetric>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopicCurrentMetric {
    pub topic: String,
    pub total_msg: i64,
    pub in_tps: f64,
    pub out_tps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardHistoryQuery {
    pub date: String,
    pub topic_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardHistorySeries {
    pub date: String,
    pub metric: String,
    pub topic_name: Option<String>,
    pub collected: bool,
    pub points: Vec<DashboardHistoryPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardHistoryPoint {
    pub timestamp: i64,
    pub value: f64,
}
