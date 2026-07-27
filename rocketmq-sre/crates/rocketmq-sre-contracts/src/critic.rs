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

use crate::ActionPlanId;
use crate::CriticReviewId;
use crate::EvidenceId;
use crate::ModelInvocationId;

/// Critic review availability and validation state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriticReviewStatus {
    Pending,
    Valid,
    Invalid,
    Unavailable,
    Conflict,
}

/// Bounded deterministic Critic conclusion.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriticConclusion {
    Accept,
    NeedsRevision,
    Reject,
}

/// One structured finding returned by the Critic.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CriticFinding {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
}

/// Immutable heterogeneous model review bound to actual invocation identity.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CriticReview {
    pub id: CriticReviewId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub primary_invocation_id: ModelInvocationId,
    pub critic_invocation_id: ModelInvocationId,
    pub primary_model_family: String,
    pub critic_model_family: String,
    pub critic_provider: String,
    pub critic_profile: String,
    pub critic_model_revision: String,
    pub endpoint_instance: String,
    #[serde(default)]
    pub fallback_chain: Vec<String>,
    pub prompt_version: String,
    pub schema_version: String,
    pub payload_hash: String,
    pub status: CriticReviewStatus,
    pub conclusion: CriticConclusion,
    #[serde(default)]
    pub findings: Vec<CriticFinding>,
    pub created_at: DateTime<Utc>,
}
