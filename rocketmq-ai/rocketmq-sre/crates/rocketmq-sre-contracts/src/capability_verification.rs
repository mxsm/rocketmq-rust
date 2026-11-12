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

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    CapabilityVerificationId, ClusterId, DiagnosisRevisionId, EvidenceId, InvestigationId, TenantId, TimeRange,
};

pub const CAPABILITY_VERIFICATION_SCHEMA_VERSION: &str = "rocketmq-sre.capability-verification.v1";

/// Independent validation environments, without any execution permission.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityVerificationLevel {
    Test,
    Production,
}

/// How the verifier obtained the report; fixtures never establish production validation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityVerificationMethod {
    DeterministicFixture,
    LiveReadOnly,
}

/// The latest source observation supersedes an earlier assessment in the same
/// scope. A non-passing outcome wins a tie; delivery order cannot restore a pass.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityVerificationOutcome {
    Passed,
    Failed,
    Inconclusive,
    Revoked,
}

/// Bounded attestation accepted only from a dedicated verifier workload identity.
///
/// `resource` is an exact canonical Evidence resource, not an operator navigation
/// resource. Resource-scoped records cannot certify a cluster overview. The
/// investigation and revision identifiers must either both be present or absent.
/// `source_ref` identifies a report; the server never fetches the reference.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityVerificationRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub pack_id: String,
    pub pack_version: String,
    pub level: CapabilityVerificationLevel,
    pub method: CapabilityVerificationMethod,
    pub outcome: CapabilityVerificationOutcome,
    pub resource: Option<String>,
    pub investigation_id: Option<InvestigationId>,
    pub revision_id: Option<DiagnosisRevisionId>,
    pub time_range: TimeRange,
    pub observed_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub source_ref: String,
    pub report_digest: String,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
}

/// Immutable provenance. The authenticated issuer and recording time are server-owned.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CapabilityVerificationRecord {
    pub record_id: CapabilityVerificationId,
    pub verified_by: String,
    pub recorded_at: DateTime<Utc>,
    pub assessment: CapabilityVerificationRequest,
}
