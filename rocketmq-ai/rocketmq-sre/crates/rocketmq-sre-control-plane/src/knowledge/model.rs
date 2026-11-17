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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::KnowledgeChunkId;
use rocketmq_sre_contracts::KnowledgeItem;
use serde::Deserialize;

pub(crate) use rocketmq_sre_contracts::{
    ImportKnowledgeRequest, KnowledgeFeedbackRequest, KnowledgeImportResult, KnowledgePage, KnowledgeReviewRequest,
    KnowledgeSearchPage,
};

use crate::ControlPlaneRequestFailure;

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

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct KnowledgeListQuery {
    pub cluster_id: ClusterId,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl KnowledgeListQuery {
    pub(crate) fn bounded_limit(&self) -> Result<u32, ControlPlaneRequestFailure> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=200).contains(&limit) {
            return Err(ControlPlaneRequestFailure::validation(
                "invalid_request",
                "knowledge page limit must be between 1 and 200",
            ));
        }
        Ok(limit)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KnowledgeChunkDraft {
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
    pub human_validated: bool,
    pub ai_generated: bool,
}
