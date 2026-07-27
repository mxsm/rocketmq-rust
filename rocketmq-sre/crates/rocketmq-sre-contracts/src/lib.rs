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

mod alert;
mod api;
mod connector;
mod correlation;
mod descriptor;
mod error;
mod evidence;
mod ids;
mod incident;
mod notification;
mod operations;
mod postmortem;
mod prediction;
mod readiness;
mod simulation;
mod topology;
mod version;

pub use alert::AlertEvent;
pub use alert::AlertSeverity;
pub use alert::AlertSource;
pub use alert::AlertStatus;
pub use alert::CorrelationKey;
pub use alert::ResourceKind;
pub use alert::ResourceRef;
pub use alert::SymptomFamily;
pub use api::v1::ApiPage;
pub use api::v1::Phase2ContractManifest;
pub use api::v1::ReadOnlyOperation;
pub use connector::ConnectorCapabilityState;
pub use connector::ConnectorHeartbeat;
pub use connector::ConnectorQueryEnvelope;
pub use connector::ConnectorRegister;
pub use connector::ConnectorResponseEnvelope;
pub use connector::ConnectorSourceCapability;
pub use connector::ConnectorSourceStatus;
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
pub use evidence::EvidenceExposure;
pub use evidence::EvidenceQuery;
pub use evidence::EvidenceReference;
pub use evidence::EvidenceRelation;
pub use evidence::EvidenceSnapshot;
pub use evidence::Hypothesis;
pub use evidence::HypothesisStatus;
pub use evidence::Sensitivity;
pub use evidence::TimeRange;
pub use ids::ActionItemId;
pub use ids::AlertEventId;
pub use ids::AssetSnapshotId;
pub use ids::BaselineId;
pub use ids::ChangePointId;
pub use ids::ClusterId;
pub use ids::ConnectorSessionId;
pub use ids::ConversationId;
pub use ids::CorrelationId;
pub use ids::DiagnosisRevisionId;
pub use ids::EvidenceId;
pub use ids::ForecastId;
pub use ids::IncidentId;
pub use ids::IncidentRelationId;
pub use ids::InspectionRunId;
pub use ids::InvestigationId;
pub use ids::KnowledgeChunkId;
pub use ids::KnowledgeItemId;
pub use ids::ModelInvocationId;
pub use ids::ModelProfileId;
pub use ids::NotificationDeliveryId;
pub use ids::NotificationTargetId;
pub use ids::OnCallOwnerId;
pub use ids::PostmortemId;
pub use ids::PostmortemRevisionId;
pub use ids::QueryId;
pub use ids::ReadinessReportId;
pub use ids::RecommendationId;
pub use ids::SimulationId;
pub use ids::TenantId;
pub use ids::TimelineEventId;
pub use ids::TopologyEdgeId;
pub use ids::TopologySnapshotId;
pub use incident::Incident;
pub use incident::IncidentStatus;
pub use incident::IncidentTransition;
pub use notification::NotificationChannel;
pub use notification::NotificationDelivery;
pub use notification::NotificationDeliveryStatus;
pub use notification::NotificationTarget;
pub use notification::OnCallOwner;
pub use operations::AssetKind;
pub use operations::AssetSnapshot;
pub use operations::Conversation;
pub use operations::ConversationStatus;
pub use operations::DiagnosisRevision;
pub use operations::InspectionRun;
pub use operations::InspectionStatus;
pub use operations::InspectionTemplate;
pub use operations::Investigation;
pub use operations::InvestigationStatus;
pub use operations::KnowledgeFeedbackKind;
pub use operations::KnowledgeItem;
pub use operations::KnowledgeReviewStatus;
pub use operations::ModelInvocationPurpose;
pub use operations::ModelInvocationRecord;
pub use operations::Recommendation;
pub use operations::RecommendationStatus;
pub use operations::TimelineEvent;
pub use operations::TopologyEdge;
pub use operations::TopologyRelation;
pub use operations::WorkflowActor;
pub use postmortem::ActionItem;
pub use postmortem::ActionItemStatus;
pub use postmortem::PostmortemDraft;
pub use postmortem::PostmortemRevision;
pub use postmortem::PostmortemStatus;
pub use prediction::AnomalyBaseline;
pub use prediction::BacklogEta;
pub use prediction::CapacityForecast;
pub use prediction::ChangePoint;
pub use prediction::ForecastPoint;
pub use prediction::ForecastQuality;
pub use prediction::ForecastStatus;
pub use readiness::DrReadinessReport;
pub use readiness::ReadinessFinding;
pub use readiness::ReadinessFindingSeverity;
pub use readiness::ReadinessStatus;
pub use readiness::UpgradeReadinessReport;
/// Parsed semantic version used to order descriptor revisions.
pub use semver::Version as DescriptorVersion;
pub use simulation::SimulationKind;
pub use simulation::SimulationStatus;
pub use simulation::WhatIfSimulation;
pub use topology::TopologyNode;
pub use topology::TopologySnapshot;
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
pub use correlation::IncidentRelation;
pub use correlation::IncidentRelationKind;
pub use correlation::TimelineEventKind;
