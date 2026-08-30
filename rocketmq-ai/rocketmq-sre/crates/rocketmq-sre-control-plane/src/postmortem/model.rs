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
use rocketmq_sre_contracts::ActionItem;
use rocketmq_sre_contracts::ActionItemStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::PostmortemConclusion;
use rocketmq_sre_contracts::PostmortemDraft;
use rocketmq_sre_contracts::PostmortemId;
use rocketmq_sre_contracts::PostmortemRevision;
use rocketmq_sre_contracts::TimelineEvent;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct CreatePostmortemRequest {
    #[serde(default)]
    pub operator_notes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PostmortemPatchRequest {
    pub summary: Option<String>,
    pub impact: Option<String>,
    pub detection: Option<String>,
    pub timeline: Option<Vec<TimelineEvent>>,
    pub root_causes: Option<Vec<PostmortemConclusion>>,
    pub contributing_factors: Option<Vec<PostmortemConclusion>>,
    pub conclusions: Option<Vec<PostmortemConclusion>>,
    pub recovery: Option<String>,
    pub effective_actions: Option<Vec<String>>,
    pub ineffective_actions: Option<Vec<String>>,
    pub evidence_ids: Option<Vec<EvidenceId>>,
    #[serde(default)]
    pub human_confirmed: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PostmortemPublishRequest {
    pub human_confirmed: bool,
    pub owner: String,
    pub component: String,
    #[serde(default = "default_version_range")]
    pub rocketmq_version_range: String,
    pub review_due_at: DateTime<Utc>,
}

fn default_version_range() -> String {
    "*".to_owned()
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ActionItemPatchRequest {
    pub status: ActionItemStatus,
    pub owner: Option<String>,
    pub due_at: Option<DateTime<Utc>>,
    pub verification: Option<String>,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ActionItemListQuery {
    pub cluster_id: ClusterId,
    pub status: Option<ActionItemStatus>,
    pub owner: Option<String>,
    pub limit: Option<u32>,
}

impl ActionItemListQuery {
    pub(crate) fn bounded_limit(&self) -> u32 {
        self.limit.unwrap_or(100).clamp(1, 200)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ActionItemPage {
    pub items: Vec<ActionItem>,
    pub partial: bool,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct IncidentRecurrenceView {
    pub incident_id: IncidentId,
    pub previous_incident_id: IncidentId,
    pub postmortem_id: PostmortemId,
    pub fingerprint: String,
    pub root_cause_code: String,
    pub affected_component: String,
    pub matched_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OperatorTodo {
    pub id: uuid::Uuid,
    pub tenant_id: rocketmq_sre_contracts::TenantId,
    pub cluster_id: Option<ClusterId>,
    pub kind: String,
    pub aggregate_id: uuid::Uuid,
    pub title: String,
    pub due_at: DateTime<Utc>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PostmortemView {
    pub postmortem: PostmortemDraft,
    pub revisions: Vec<PostmortemRevision>,
    pub action_items: Vec<ActionItem>,
    pub recurrences: Vec<IncidentRecurrenceView>,
    pub todos: Vec<OperatorTodo>,
    pub knowledge_item: Option<KnowledgeItem>,
    pub execution_journal_empty: bool,
}
