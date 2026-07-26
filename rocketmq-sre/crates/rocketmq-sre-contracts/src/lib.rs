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

//! Stable, serializable contracts shared by RocketMQ AI SRE components.
//!
//! This crate intentionally contains no networking, async runtime, database,
//! model SDK, or RocketMQ implementation dependency.

mod descriptor;
mod error;
mod evidence;
mod ids;
mod incident;
mod version;

pub use descriptor::ActionDescriptor;
pub use descriptor::ActionRisk;
pub use descriptor::Deprecation;
pub use descriptor::Descriptor;
pub use descriptor::DescriptorKind;
pub use descriptor::DescriptorStatus;
pub use descriptor::DiagnosticPackDescriptor;
pub use descriptor::EvidenceSourceDescriptor;
pub use descriptor::IntegrationDescriptor;
pub use descriptor::ProviderDescriptor;
pub use error::ContractError;
pub use error::ErrorCode;
pub use error::SreError;
pub use evidence::CoverageStatus;
pub use evidence::DiagnosticEvidence;
pub use evidence::EvidenceContent;
pub use evidence::EvidenceQuery;
pub use evidence::EvidenceReference;
pub use evidence::EvidenceRelation;
pub use evidence::EvidenceSnapshot;
pub use evidence::Hypothesis;
pub use evidence::HypothesisStatus;
pub use evidence::Sensitivity;
pub use evidence::TimeRange;
pub use ids::ClusterId;
pub use ids::CorrelationId;
pub use ids::EvidenceId;
pub use ids::IncidentId;
pub use ids::QueryId;
pub use ids::TenantId;
pub use incident::Incident;
pub use incident::IncidentStatus;
pub use incident::IncidentTransition;
/// Parsed semantic version used to order descriptor revisions.
pub use semver::Version as DescriptorVersion;
pub use version::SchemaVersion;

/// Business schema family for canonical evidence.
pub const EVIDENCE_SCHEMA_FAMILY: &str = "rocketmq-sre.evidence";

/// Current canonical evidence schema major.
pub const EVIDENCE_SCHEMA_MAJOR: u16 = 1;

/// Current canonical evidence schema minor.
pub const EVIDENCE_SCHEMA_MINOR: u16 = 0;

/// Returns the schema version emitted by Phase 00 evidence producers.
#[must_use]
pub fn current_evidence_schema() -> SchemaVersion {
    SchemaVersion::new(EVIDENCE_SCHEMA_FAMILY, EVIDENCE_SCHEMA_MAJOR, EVIDENCE_SCHEMA_MINOR)
}
