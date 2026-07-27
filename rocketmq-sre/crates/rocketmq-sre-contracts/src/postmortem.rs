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
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ActionItemId;
use crate::ClusterId;
use crate::EvidenceId;
use crate::IncidentId;
use crate::KnowledgeItemId;
use crate::PostmortemId;
use crate::PostmortemRevisionId;
use crate::TenantId;

/// Postmortem lifecycle. Publication always requires an explicit human actor.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostmortemStatus {
    Draft,
    InReview,
    Confirmed,
    Published,
    Archived,
}

/// Mutable postmortem head pointing at immutable revisions.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct PostmortemDraft {
    pub id: PostmortemId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub status: PostmortemStatus,
    pub current_revision: u32,
    pub confirmed_by: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub published_knowledge_item_id: Option<KnowledgeItemId>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Immutable content revision with explicit Evidence citations.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct PostmortemRevision {
    pub id: PostmortemRevisionId,
    pub postmortem_id: PostmortemId,
    pub revision: u32,
    pub summary: String,
    pub impact: String,
    pub detection: String,
    pub timeline: Value,
    pub root_causes: Vec<String>,
    pub contributing_factors: Vec<String>,
    pub recovery: String,
    pub effective_actions: Vec<String>,
    pub ineffective_actions: Vec<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub model_invocation_id: Option<crate::ModelInvocationId>,
    pub edited_by: String,
    pub human_confirmed: bool,
    pub created_at: DateTime<Utc>,
}

/// Action-item lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionItemStatus {
    Open,
    Assigned,
    InProgress,
    Blocked,
    Completed,
    Reopened,
    Cancelled,
}

/// Independently tracked postmortem follow-up.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ActionItem {
    pub id: ActionItemId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub postmortem_id: PostmortemId,
    pub incident_id: IncidentId,
    pub title: String,
    pub owner: Option<String>,
    pub due_at: Option<DateTime<Utc>>,
    pub status: ActionItemStatus,
    pub verification: Option<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub execution_journal: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postmortem_revision_round_trip_preserves_history_fields() {
        let revision = PostmortemRevision {
            id: PostmortemRevisionId::new(),
            postmortem_id: PostmortemId::new(),
            revision: 2,
            summary: "summary".into(),
            impact: "impact".into(),
            detection: "alert".into(),
            timeline: serde_json::json!([]),
            root_causes: vec!["store_pressure".into()],
            contributing_factors: Vec::new(),
            recovery: "manual recovery".into(),
            effective_actions: Vec::new(),
            ineffective_actions: Vec::new(),
            evidence_ids: vec![EvidenceId::new()],
            model_invocation_id: None,
            edited_by: "operator@example.test".into(),
            human_confirmed: true,
            created_at: Utc::now(),
        };
        let value = serde_json::to_value(&revision).expect("revision should encode");
        let decoded: PostmortemRevision = serde_json::from_value(value).expect("revision should decode");
        assert_eq!(decoded, revision);
    }
}
