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
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageQuery {
    pub topic: Option<String>,
    pub key: Option<String>,
    pub message_id: Option<String>,
    pub begin: Option<i64>,
    pub end: Option<i64>,
    pub page_num: Option<u32>,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageView {
    pub topic: String,
    pub message_id: String,
    pub keys: Option<String>,
    pub tags: Option<String>,
    pub born_timestamp: i64,
    pub store_timestamp: i64,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub body: String,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageListView {
    pub items: Vec<MessageView>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageResendRequest {
    pub topic: String,
    pub consumer_group: String,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DlqMessageQuery {
    pub consumer_group: String,
    pub key: Option<String>,
    #[serde(default, alias = "msgId")]
    pub message_id: Option<String>,
    pub begin: Option<i64>,
    pub end: Option<i64>,
    pub page_num: Option<u32>,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DlqMessageRef {
    pub topic_name: Option<String>,
    pub consumer_group: String,
    #[serde(alias = "messageId")]
    pub msg_id: String,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DlqBatchResendRequest {
    pub messages: Vec<DlqMessageRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DlqMessageResendResult {
    pub msg_id: String,
    pub consume_result: String,
    pub remark: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DlqExportView {
    pub file_name: String,
    pub rows: Vec<MessageView>,
    pub csv: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageTraceView {
    pub message_id: String,
    pub trace_topic: String,
    pub nodes: Vec<MessageTraceNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageTraceNode {
    pub node_type: String,
    pub name: String,
    pub status: String,
    pub timestamp: i64,
}
