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
use std::fmt;

use rocketmq_error::REDACTED;
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclUserView {
    pub broker_name: String,
    pub broker_addr: String,
    pub username: String,
    pub password: Option<String>,
    pub user_type: Option<String>,
    pub user_status: Option<String>,
}

impl fmt::Debug for AclUserView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AclUserView")
            .field("broker_name", &self.broker_name)
            .field("broker_addr", &self.broker_addr)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| REDACTED))
            .field("user_type", &self.user_type)
            .field("user_status", &self.user_status)
            .finish()
    }
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclUserUpsertRequest {
    pub broker_name: Option<String>,
    pub cluster_name: Option<String>,
    pub username: Option<String>,
    pub password: String,
    pub user_type: String,
    pub user_status: Option<String>,
}

impl fmt::Debug for AclUserUpsertRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AclUserUpsertRequest")
            .field("broker_name", &self.broker_name)
            .field("cluster_name", &self.cluster_name)
            .field("username", &self.username)
            .field("password", &REDACTED)
            .field("user_type", &self.user_type)
            .field("user_status", &self.user_status)
            .finish()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_user_debug_redacts_passwords() {
        let view = AclUserView {
            broker_name: "broker-a".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            username: "alice".to_string(),
            password: Some("view-secret".to_string()),
            user_type: Some("Normal".to_string()),
            user_status: Some("enable".to_string()),
        };
        let request = AclUserUpsertRequest {
            broker_name: Some("broker-a".to_string()),
            cluster_name: None,
            username: Some("alice".to_string()),
            password: "request-secret".to_string(),
            user_type: "Normal".to_string(),
            user_status: Some("enable".to_string()),
        };

        let view_debug = format!("{view:?}");
        let request_debug = format!("{request:?}");

        assert!(view_debug.contains(REDACTED));
        assert!(request_debug.contains(REDACTED));
        assert!(!view_debug.contains("view-secret"));
        assert!(!request_debug.contains("request-secret"));
    }
}
