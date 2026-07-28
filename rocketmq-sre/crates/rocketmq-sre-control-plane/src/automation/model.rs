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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::AutomationArtifact;
use rocketmq_sre_contracts::AutomationFeedbackSubject;
use rocketmq_sre_contracts::AutomationFeedbackVerdict;
use rocketmq_sre_contracts::AutomationRunStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::NoSideEffectAutomationKind;
use rocketmq_sre_contracts::NoSideEffectAutomationRun;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompleteAutomationRunRequest {
    pub(super) status: AutomationRunStatus,
    pub(super) result_code: String,
    pub(super) sanitized_summary: String,
    #[serde(default)]
    pub(super) artifacts: Vec<AutomationArtifact>,
    pub(super) model_invocation_id: Option<ModelInvocationId>,
    pub(super) completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AutomationRunListQuery {
    pub(super) cluster_id: Option<ClusterId>,
    pub(super) incident_id: Option<IncidentId>,
    pub(super) kind: Option<NoSideEffectAutomationKind>,
    pub(super) status: Option<AutomationRunStatus>,
    #[serde(default = "default_limit")]
    pub(super) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct AutomationRunPage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<NoSideEffectAutomationRun>,
    pub(super) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecordAutomationFeedbackRequest {
    pub(super) cluster_id: Option<ClusterId>,
    pub(super) incident_id: Option<IncidentId>,
    pub(super) subject: AutomationFeedbackSubject,
    pub(super) subject_id: Option<Uuid>,
    pub(super) verdict: AutomationFeedbackVerdict,
    pub(super) comment: Option<String>,
}

const fn default_limit() -> u16 {
    50
}
