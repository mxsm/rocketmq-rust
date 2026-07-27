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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::KnowledgeChunkId;
use rocketmq_sre_contracts::KnowledgeFeedbackKind;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::KnowledgeItemId;
use rocketmq_sre_contracts::KnowledgeReviewStatus;
use rocketmq_sre_contracts::Sensitivity;
use serde::Deserialize;
use serde::Serialize;

use crate::ControlPlaneError;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ImportKnowledgeRequest {
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

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KnowledgeImportResult {
    pub item: KnowledgeItem,
    pub chunk_count: usize,
    pub deduplicated: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct KnowledgeSearchQuery {
    pub q: String,
    pub cluster_id: ClusterId,
    pub component: Option<String>,
    pub rocketmq_version: String,
    pub limit: Option<u32>,
    #[serde(default)]
    pub include_unvalidated: bool,
}

impl KnowledgeSearchQuery {
    pub(crate) fn bounded_limit(&self) -> u32 {
        self.limit.unwrap_or(20).clamp(1, 50)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KnowledgeSearchPage {
    pub items: Vec<KnowledgeChunkView>,
    pub partial: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct KnowledgeListQuery {
    pub cluster_id: ClusterId,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl KnowledgeListQuery {
    pub(crate) fn bounded_limit(&self) -> Result<u32, ControlPlaneError> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=200).contains(&limit) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "knowledge page limit must be between 1 and 200",
            ));
        }
        Ok(limit)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KnowledgePage {
    pub items: Vec<KnowledgeItem>,
    pub next_cursor: Option<String>,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct KnowledgeChunkView {
    pub id: KnowledgeChunkId,
    pub knowledge_item_id: KnowledgeItemId,
    pub title: String,
    pub component: String,
    pub heading: Option<String>,
    pub content: String,
    pub source_uri: String,
    pub source_version: String,
    pub sensitivity: Sensitivity,
    pub item_hash: String,
    pub chunk_hash: String,
    pub review_status: KnowledgeReviewStatus,
    pub conflict: bool,
    pub expired: bool,
    pub eligible_for_diagnosis: bool,
    pub exclusion_reasons: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct KnowledgeReviewRequest {
    pub status: KnowledgeReviewStatus,
    #[serde(default)]
    pub human_confirmed: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct KnowledgeFeedbackRequest {
    pub kind: KnowledgeFeedbackKind,
    pub comment: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct KnowledgeChunkDraft {
    pub id: KnowledgeChunkId,
    pub ordinal: i32,
    pub heading: Option<String>,
    pub content: String,
    pub content_hash: String,
}

#[derive(Clone, Debug)]
pub(super) struct KnowledgeImport {
    pub item: KnowledgeItem,
    pub chunks: Vec<KnowledgeChunkDraft>,
}
