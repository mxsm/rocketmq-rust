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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclQuery {
    pub broker_name: Option<String>,
    pub cluster_name: Option<String>,
    pub filter: Option<String>,
    pub search_param: Option<String>,
    pub resource: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclUserView {
    pub broker_name: String,
    pub broker_addr: String,
    pub username: String,
    pub password: Option<String>,
    pub user_type: Option<String>,
    pub user_status: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclUserUpsertRequest {
    pub broker_name: Option<String>,
    pub cluster_name: Option<String>,
    pub username: Option<String>,
    pub password: String,
    pub user_type: String,
    pub user_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclMutationResult {
    pub message: String,
    pub target_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclPolicyEntryView {
    pub resource: Option<String>,
    pub actions: Vec<String>,
    pub source_ips: Vec<String>,
    pub decision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclPolicyView {
    pub broker_name: String,
    pub broker_addr: String,
    pub subject: Option<String>,
    pub policy_type: Option<String>,
    pub entries: Vec<AclPolicyEntryView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclPolicyRequest {
    pub broker_name: Option<String>,
    pub cluster_name: Option<String>,
    pub subject: String,
    pub policies: Vec<AclPolicyRequestPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclPolicyRequestPolicy {
    pub policy_type: String,
    pub entries: Vec<AclPolicyEntryRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclPolicyEntryRequest {
    pub resource: Vec<String>,
    pub actions: Vec<String>,
    pub source_ips: Vec<String>,
    pub decision: String,
}
