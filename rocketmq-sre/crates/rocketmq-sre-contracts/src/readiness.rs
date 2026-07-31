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

use crate::ClusterId;
use crate::EvidenceId;
use crate::ReadinessReportId;
use crate::TenantId;

/// Aggregate readiness result.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessStatus {
    Ready,
    ReadyWithWarnings,
    Blocked,
    InsufficientData,
}

/// Severity of one deterministic readiness finding.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessFindingSeverity {
    Info,
    Warning,
    Blocker,
}

/// Explainable finding shared by upgrade and disaster-recovery reports.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ReadinessFinding {
    pub code: String,
    pub severity: ReadinessFindingSeverity,
    pub component: String,
    pub summary: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub remediation_hint: Option<String>,
}

/// Immutable upgrade-readiness assessment.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct UpgradeReadinessReport {
    pub id: ReadinessReportId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub target_version: String,
    pub status: ReadinessStatus,
    pub findings: Vec<ReadinessFinding>,
    pub pack_versions: Vec<String>,
    pub execution_eligible: bool,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Immutable disaster-recovery readiness assessment.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DrReadinessReport {
    pub id: ReadinessReportId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub target_region: Option<String>,
    pub requested_rto_seconds: u64,
    pub requested_rpo_seconds: u64,
    pub status: ReadinessStatus,
    pub findings: Vec<ReadinessFinding>,
    pub execution_eligible: bool,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
