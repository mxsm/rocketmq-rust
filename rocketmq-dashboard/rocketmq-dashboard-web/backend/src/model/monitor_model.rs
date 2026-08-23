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
use crate::model::EnvironmentId;
use crate::persistence::Revision;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerMonitorRule {
    pub environment_id: EnvironmentId,
    pub consumer_group: String,
    pub min_count: i32,
    pub max_diff_total: i64,
    pub revision: Revision,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl ConsumerMonitorRule {
    pub fn validate(&self) -> Result<(), String> {
        if self.environment_id.0.trim().is_empty() || self.consumer_group.trim().is_empty() {
            return Err("environment ID and consumer group are required".to_string());
        }
        if self.consumer_group.len() > 255 || self.min_count < 0 || self.max_diff_total < 0 {
            return Err("consumer monitor rule is invalid".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerMonitorView {
    pub environment_id: EnvironmentId,
    pub consumer_group: String,
    pub min_count: i32,
    pub max_diff_total: i64,
    pub revision: Revision,
}

impl From<ConsumerMonitorRule> for ConsumerMonitorView {
    fn from(value: ConsumerMonitorRule) -> Self {
        Self {
            environment_id: value.environment_id,
            consumer_group: value.consumer_group,
            min_count: value.min_count,
            max_diff_total: value.max_diff_total,
            revision: value.revision,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerMonitorUpsertRequest {
    pub environment_id: EnvironmentId,
    pub consumer_group: String,
    pub min_count: i32,
    pub max_diff_total: i64,
    /// Zero creates a rule; a non-zero revision updates an existing rule.
    pub expected_revision: Revision,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerMonitorMutationResult {
    pub message: String,
    pub item: Option<ConsumerMonitorView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MonitorEnvironmentQuery {
    pub environment_id: EnvironmentId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MonitorDeleteQuery {
    pub environment_id: EnvironmentId,
    pub expected_revision: Revision,
}
