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

use crate::EvidenceId;
use crate::ExecutionStepId;

/// Deterministic verification outcome.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationOutcome {
    Succeeded,
    Failed,
    Inconclusive,
}

/// Bounded verification result with before/after evidence.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationResult {
    pub step_id: ExecutionStepId,
    pub outcome: VerificationOutcome,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    #[serde(default)]
    pub pre_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub during_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub post_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub satisfied_conditions: Vec<String>,
    #[serde(default)]
    pub failed_conditions: Vec<String>,
    pub stable_window_seconds: u64,
}
