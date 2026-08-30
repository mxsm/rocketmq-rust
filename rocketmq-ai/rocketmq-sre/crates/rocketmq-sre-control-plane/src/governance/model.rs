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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::GovernanceAccessPath;
use rocketmq_sre_contracts::GovernanceAdmission;
use rocketmq_sre_contracts::GovernanceArtifact;
use rocketmq_sre_contracts::GovernanceDependency;
use rocketmq_sre_contracts::GovernanceEvent;
use rocketmq_sre_contracts::GovernanceImpact;
use rocketmq_sre_contracts::GovernanceImpactKind;
use rocketmq_sre_contracts::GovernanceLifecycleState;
use rocketmq_sre_contracts::GovernanceObjectKind;
use rocketmq_sre_contracts::GovernanceVersion;
use rocketmq_sre_contracts::GovernanceVersionId;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const GOVERNANCE_API_SCHEMA_VERSION: &str = "rocketmq-sre.governance-api.v1";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateGovernanceArtifactRequest {
    pub(crate) kind: GovernanceObjectKind,
    pub(crate) logical_key: String,
    pub(crate) owner: String,
    pub(crate) reviewer: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GovernanceArtifactQuery {
    pub(crate) kind: Option<GovernanceObjectKind>,
    pub(crate) logical_key: Option<String>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GovernanceArtifactPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<GovernanceArtifact>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateGovernanceVersionRequest {
    pub(crate) version: String,
    pub(crate) content_digest: String,
    #[serde(default)]
    pub(crate) applicable_components: BTreeSet<String>,
    pub(crate) applicable_version_range: String,
    #[serde(default)]
    pub(crate) dependencies: BTreeSet<GovernanceDependency>,
    pub(crate) review_due_at: DateTime<Utc>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) rollback_version_id: Option<GovernanceVersionId>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransitionGovernanceVersionRequest {
    pub(crate) state: GovernanceLifecycleState,
    pub(crate) reason: String,
    pub(crate) replacement_version_id: Option<GovernanceVersionId>,
    pub(crate) rollback_version_id: Option<GovernanceVersionId>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GovernanceVersionQuery {
    pub(crate) state: Option<GovernanceLifecycleState>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GovernanceVersionPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<GovernanceVersion>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordGovernanceImpactRequest {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) kind: GovernanceImpactKind,
    pub(crate) reference_id: String,
    pub(crate) label: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GovernanceImpactQuery {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) kind: Option<GovernanceImpactKind>,
    #[serde(default = "default_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GovernanceImpactPage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<GovernanceImpact>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvaluateGovernanceAdmissionRequest {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) access_path: GovernanceAccessPath,
    pub(crate) required_version_ids: Vec<GovernanceVersionId>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GovernanceAdmissionView {
    pub(crate) schema_version: &'static str,
    pub(crate) decision: GovernanceAdmission,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GovernanceAuditQuery {
    pub(crate) artifact_id: Option<rocketmq_sre_contracts::GovernanceArtifactId>,
    pub(crate) version_id: Option<GovernanceVersionId>,
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) to: Option<DateTime<Utc>>,
    #[serde(default = "default_export_limit")]
    pub(crate) limit: u16,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GovernanceAuditExport {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<GovernanceEvent>,
    pub(crate) truncated: bool,
    pub(crate) exported_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GovernanceComplianceReport {
    pub(crate) schema_version: &'static str,
    pub(crate) state_counts: BTreeMap<String, u64>,
    pub(crate) unsigned_active: u64,
    pub(crate) expired_active: u64,
    pub(crate) overdue_review: u64,
    pub(crate) quarantined: u64,
    pub(crate) compliant: bool,
    pub(crate) observed_at: DateTime<Utc>,
}

pub(crate) fn bounded_limit(limit: u16) -> i64 {
    i64::from(limit.clamp(1, 200))
}

pub(crate) fn bounded_export_limit(limit: u16) -> i64 {
    i64::from(limit.clamp(1, 1_000))
}

const fn default_limit() -> u16 {
    100
}

const fn default_export_limit() -> u16 {
    500
}
