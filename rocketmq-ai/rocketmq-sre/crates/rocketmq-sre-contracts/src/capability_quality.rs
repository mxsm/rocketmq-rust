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

use crate::{ClusterId, DiagnosisRevisionId, EvidenceId, InvestigationId, TenantId};

/// Primitive shape required by a compiled diagnostic predicate.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceFieldKind {
    Boolean,
    Number,
    Text,
}

/// A field contract describes evidence sufficiency; it grants no execution authority.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct EvidenceFieldContract {
    pub path: String,
    pub kind: EvidenceFieldKind,
}

/// Whether a particular capability fact has actually been established.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityFactStatus {
    Present,
    Absent,
    Unknown,
    NotApplicable,
}

/// One independently sourced fact, rather than an aggregate authorization decision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CapabilityFact {
    pub status: CapabilityFactStatus,
    pub reason_code: String,
    pub source_ref: String,
    pub observed_at: Option<DateTime<Utc>>,
}

/// Registration, implementation, deployment, observation and verification stay independent.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CapabilityLayers {
    pub registered: CapabilityFact,
    pub handler: CapabilityFact,
    pub configured: CapabilityFact,
    pub target_observed: CapabilityFact,
    pub test_verified: CapabilityFact,
    pub production_verified: CapabilityFact,
}

/// Quality of the bounded stored sample selected for one diagnostic requirement.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticEvidenceQuality {
    Available,
    Partial,
    Missing,
    Stale,
    Unknown,
    Unsupported,
}

/// IDs identify the exact stored sample that produced this assessment.
///
/// This cluster projection does not bind a diagnosis revision. A consumer
/// showing revision quality must intersect these IDs with that revision's
/// saved references and verify its resource and time scope.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticRequirementQuality {
    pub id: String,
    pub source: String,
    pub resource_prefix: String,
    pub required: bool,
    pub max_age_seconds: u64,
    pub fields: Vec<EvidenceFieldContract>,
    pub quality: DiagnosticEvidenceQuality,
    pub reason_code: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub evidence_ids: Vec<EvidenceId>,
    pub missing_fields: Vec<String>,
}

/// Read-only evidence sufficiency for a compiled diagnostic pack.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticPackQuality {
    pub pack_id: String,
    pub layers: CapabilityLayers,
    pub requirements: Vec<DiagnosticRequirementQuality>,
}

/// A documented comparison of packs, never a claimed causal relationship between samples.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticScenarioCoverage {
    pub id: String,
    pub pack_ids: Vec<String>,
    pub required_signal_ids: Vec<String>,
}

/// Cluster-scoped capability facts evaluated against bounded, already saved evidence.
///
/// `observed_at` is the evaluation instant. Evidence freshness is compared with
/// each requirement's compiled age bound. This response never authorizes an action;
/// `execution_eligible` is always false. Verification remains unknown without an
/// explicit deployment verification record.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CapabilityQualityProjection {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub investigation_id: Option<InvestigationId>,
    pub revision_id: Option<DiagnosisRevisionId>,
    pub observed_at: DateTime<Utc>,
    pub execution_eligible: bool,
    pub partial: bool,
    pub packs: Vec<DiagnosticPackQuality>,
    pub scenarios: Vec<DiagnosticScenarioCoverage>,
}
