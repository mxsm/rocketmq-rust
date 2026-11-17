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

//! Reviewed retrieval is knowledge context, never execution or physical-version authority.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ClusterId, IncidentId, KnowledgeChunkId, KnowledgeFeedbackKind, KnowledgeItem, KnowledgeItemId,
    KnowledgeReviewStatus, PostmortemId, Sensitivity, TenantId,
};

/// Direct import declaration; human/AI flags are caller declarations, not identity proof.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ImportKnowledgeRequest {
    pub cluster_id: Option<ClusterId>,
    pub title: String,
    pub component: String,
    pub rocketmq_version_range: String,
    pub source_uri: String,
    pub source_version: String,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub owner: String,
    pub review_due_at: DateTime<Utc>,
    pub sensitivity: Sensitivity,
    #[serde(default = "draft_status")]
    pub review_status: KnowledgeReviewStatus,
    #[serde(default)]
    pub human_validated: bool,
    #[serde(default)]
    pub ai_generated: bool,
    pub markdown: String,
}

fn draft_status() -> KnowledgeReviewStatus {
    KnowledgeReviewStatus::Draft
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeImportResult {
    pub item: KnowledgeItem,
    pub chunk_count: usize,
    pub deduplicated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgePage {
    pub items: Vec<KnowledgeItem>,
    pub next_cursor: Option<String>,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeSearchPage {
    pub items: Vec<KnowledgeChunkView>,
    /// Compatibility: one or more returned rows are ineligible, not a completeness claim.
    pub partial: bool,
    pub observed_at: DateTime<Utc>,
    pub binding: KnowledgeSearchBinding,
    pub bounds: KnowledgeSearchBounds,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeSearchBinding {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub q: String,
    pub component: Option<String>,
    /// Explicit knowledge applicability context; not an observed physical version.
    pub rocketmq_version: String,
    pub limit: u32,
    pub include_unvalidated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeSearchBounds {
    pub candidate_limit: u32,
    /// Materialized candidates evaluated; does not bound PostgreSQL's underlying scan.
    pub scanned_candidates: u32,
    pub candidate_limit_reached: bool,
    /// Additional matching rows existed inside the materialized candidate set.
    pub results_truncated: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeValidity {
    Current,
    NotYetValid,
    ReviewOverdue,
    ValidityEnded,
    StatusExpired,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeChunkView {
    pub id: KnowledgeChunkId,
    pub knowledge_item_id: KnowledgeItemId,
    pub title: String,
    pub component: String,
    pub heading: Option<String>,
    pub content: String,
    /// Original declared URI, retained for compatibility. Use source_uri_display in UI.
    pub source_uri: String,
    pub source_uri_display: Option<String>,
    pub source_version: String,
    pub rocketmq_version_range: String,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub review_due_at: DateTime<Utc>,
    pub validity: Vec<KnowledgeValidity>,
    pub sensitivity: Sensitivity,
    pub item_hash: String,
    pub chunk_hash: String,
    pub review_status: KnowledgeReviewStatus,
    pub conflict: bool,
    /// Legacy aggregate also includes a future validity start; consult validity for detail.
    pub expired: bool,
    pub content_truncated: bool,
    pub metadata_truncated: bool,
    pub eligible_for_diagnosis: bool,
    pub exclusion_reasons: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeReviewRequest {
    pub status: KnowledgeReviewStatus,
    #[serde(default)]
    pub human_confirmed: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeFeedbackRequest {
    pub kind: KnowledgeFeedbackKind,
    pub comment: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeReviewEventKind {
    Import,
    Review,
    Feedback,
}

/// Null typed provenance is explicitly unknown on legacy rows.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeReviewRecord {
    pub id: Uuid,
    pub event_kind: Option<KnowledgeReviewEventKind>,
    pub actor_subject: Option<String>,
    pub from_status: Option<KnowledgeReviewStatus>,
    pub to_status: Option<KnowledgeReviewStatus>,
    pub item_content_hash: Option<String>,
    pub human_confirmed: Option<bool>,
    pub ai_generated: Option<bool>,
    pub reason: String,
    pub task_status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub details_truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeFeedbackRecord {
    pub id: Uuid,
    pub cluster_id: Option<ClusterId>,
    pub kind: KnowledgeFeedbackKind,
    pub comment: Option<String>,
    pub actor_subject: Option<String>,
    pub created_at: DateTime<Utc>,
    pub details_truncated: bool,
}

/// Stored publication relation and immutable confirmed revision, not URI inference.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgePostmortemLineage {
    pub postmortem_id: PostmortemId,
    pub incident_id: IncidentId,
    pub revision: u32,
    pub confirmed_by: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct KnowledgeHistoryPage {
    pub knowledge_item_id: KnowledgeItemId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub item_content_hash: String,
    pub source_uri_display: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub reviews: Vec<KnowledgeReviewRecord>,
    pub feedback: Vec<KnowledgeFeedbackRecord>,
    pub reviews_truncated: bool,
    pub feedback_truncated: bool,
    pub postmortem: Option<KnowledgePostmortemLineage>,
}
