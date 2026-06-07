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
pub struct ConsumerGroupInfo {
    pub group: String,
    pub consume_type: String,
    pub message_model: String,
    pub client_count: usize,
    pub diff_total: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerListView {
    pub items: Vec<ConsumerGroupInfo>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProgress {
    pub group: String,
    pub topic_count: usize,
    pub diff_total: i64,
    pub queues: Vec<ConsumerQueueProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerQueueProgress {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub diff: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerResetOffsetRequest {
    pub topic: String,
    pub reset_timestamp: i64,
    pub force: bool,
}
