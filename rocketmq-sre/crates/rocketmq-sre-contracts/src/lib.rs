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

mod action;
mod agent;
mod alert;
mod api;
mod approval;
mod audit;
mod canonical;
mod connector;
mod correlation;
mod critic;
mod descriptor;
mod error;
mod evidence;
mod execution;
mod fencing;
mod ids;
mod incident;
mod notification;
mod operations;
mod operator_workbench;
mod plan;
mod policy;
mod postmortem;
mod prediction;
mod readiness;
mod resource;
mod simulation;
mod slo;
mod topology;
mod verification;
mod version;

pub use action::CompensationMode;
pub use action::CompensationSpec;
pub use action::ExecutionAction;
pub use action::ImpactScope;
pub use action::VerificationSpec;
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
pub use approval::ApprovalDecision;
pub use approval::ApprovalGrant;
pub use approval::ApprovalRecord;
pub use audit::AuditEvent;
pub use audit::AuditEventKind;
pub use canonical::canonical_evidence_hash;
pub use canonical::canonical_precondition_hash;
pub use canonical::canonical_sha256;
pub use canonical::is_sha256_digest;
pub use connector::ConnectorCapabilityState;
pub use connector::ConnectorHeartbeat;
pub use connector::ConnectorQueryEnvelope;
pub use connector::ConnectorRegister;
pub use connector::ConnectorResponseEnvelope;
pub use connector::ConnectorSourceCapability;
pub use connector::ConnectorSourceStatus;
pub use critic::CriticAssessment;
pub use critic::CriticConclusion;
pub use critic::CriticFinding;
pub use critic::CriticFindingCode;
pub use critic::CriticGateState;
pub use critic::CriticReview;
pub use critic::CriticReviewStatus;
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
pub use execution::AgentStepRequest;
pub use execution::AgentStepResult;
pub use execution::EffectState;
pub use execution::ExecutionRequest;
pub use execution::ExecutionResult;
pub use execution::ExecutionState;
pub use execution::ExecutionTransition;
pub use execution::StepIntent;
pub use execution::StepResult;
pub use fencing::ActivateLeaseRequest;
pub use fencing::BeginLeaseTakeoverRequest;
pub use fencing::BeginLeaseTakeoverResponse;
pub use fencing::ExecutorLease;
pub use fencing::FenceAck;
pub use fencing::GrantVerification;
pub use fencing::IssueFenceGrantRequest;
pub use fencing::LEASE_AUTHORITY_SCHEMA_VERSION;
pub use fencing::LeaseEpoch;
pub use fencing::LeaseFenceGrant;
pub use fencing::LeaseState;
pub use fencing::ReconcileGrant;
pub use fencing::VerifyExecutionRequest;
pub use fencing::VerifyFenceGrantRequest;
pub use fencing::VerifyReconcileGrantRequest;
pub use ids::ActionItemId;
pub use ids::ActionPlanId;
pub use ids::AlertEventId;
pub use ids::ApprovalId;
pub use ids::AssetSnapshotId;
pub use ids::AuditEventId;
pub use ids::BaselineId;
pub use ids::ChangePointId;
pub use ids::ClusterId;
pub use ids::ConnectorSessionId;
pub use ids::ConversationId;
pub use ids::CorrelationId;
pub use ids::CriticReviewId;
pub use ids::DiagnosisRevisionId;
pub use ids::EvidenceId;
pub use ids::ExecutionId;
pub use ids::ExecutionStepId;
pub use ids::ForecastId;
pub use ids::HealthSnapshotId;
pub use ids::IncidentId;
pub use ids::IncidentRelationId;
pub use ids::InspectionRunId;
pub use ids::InvestigationId;
pub use ids::KnowledgeChunkId;
pub use ids::KnowledgeItemId;
pub use ids::LeaseId;
pub use ids::ModelInvocationId;
pub use ids::ModelProfileId;
pub use ids::NotificationDeliveryId;
pub use ids::NotificationTargetId;
pub use ids::OnCallOwnerId;
pub use ids::PlanStepId;
pub use ids::PolicyDecisionId;
pub use ids::PostmortemId;
pub use ids::PostmortemRevisionId;
pub use ids::QueryId;
pub use ids::ReadinessReportId;
pub use ids::RecommendationId;
pub use ids::ResourceLockId;
pub use ids::ResourceQuarantineId;
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
pub use operator_workbench::IncidentOperationRequest;
pub use operator_workbench::IncidentOperationResult;
pub use operator_workbench::IncidentOperationsState;
pub use operator_workbench::IncidentSlaState;
pub use operator_workbench::OperationsFinding;
pub use operator_workbench::OperationsReport;
pub use operator_workbench::OperationsReportWindow;
pub use operator_workbench::ShiftHandoffSummary;
pub use plan::ActionPlan;
pub use plan::ActionPlanDraft;
pub use plan::ManualRunbookDraft;
pub use plan::PlanStatus;
pub use plan::PlanStep;
pub use policy::PolicyDecision;
pub use policy::PolicyEffect;
pub use postmortem::ActionItem;
pub use postmortem::ActionItemStatus;
pub use postmortem::PostmortemConclusion;
pub use postmortem::PostmortemDraft;
pub use postmortem::PostmortemRevision;
pub use postmortem::PostmortemStatus;
pub use prediction::AnomalyAssessment;
pub use prediction::AnomalyBaseline;
pub use prediction::BacklogEta;
pub use prediction::CapacityForecast;
pub use prediction::ChangePoint;
pub use prediction::ClusterForecastReport;
pub use prediction::ForecastAccuracy;
pub use prediction::ForecastBacktest;
pub use prediction::ForecastPoint;
pub use prediction::ForecastQuality;
pub use prediction::ForecastStatus;
pub use prediction::ForecastTrend;
pub use prediction::ForecastWindow;
pub use prediction::Seasonality;
pub use readiness::DrReadinessReport;
pub use readiness::ReadinessFinding;
pub use readiness::ReadinessFindingSeverity;
pub use readiness::ReadinessStatus;
pub use readiness::UpgradeReadinessReport;
pub use resource::ResourceQuarantine;
/// Parsed semantic version used to order descriptor revisions.
pub use semver::Version as DescriptorVersion;
pub use simulation::SimulationKind;
pub use simulation::SimulationStatus;
pub use simulation::WhatIfSimulation;
pub use simulation::WhatIfSimulationRequest;
pub use slo::BurnRateSeverity;
pub use slo::BurnRateWindowResult;
pub use slo::ClusterHealthReport;
pub use slo::FleetClusterHealth;
pub use slo::FleetHealthReport;
pub use slo::HealthDataQuality;
pub use slo::HealthDimensionScore;
pub use slo::HealthOperationalState;
pub use slo::HealthRecentChange;
pub use slo::HealthStatus;
pub use slo::IncidentHealthSummary;
pub use slo::SliHealth;
pub use slo::SloDimension;
pub use topology::TopologyNode;
pub use topology::TopologySnapshot;
pub use verification::EXECUTION_VERIFICATION_SCHEMA_VERSION;
pub use verification::ExecutionSliObservation;
pub use verification::ExecutionSliQuery;
pub use verification::VerificationOutcome;
pub use verification::VerificationResult;
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
pub use agent::AdvanceFenceRequest;
pub use agent::AdvanceFenceResponse;
pub use agent::AgentDispatchRequest;
pub use agent::AgentDispatchResponse;
pub use agent::AgentReadRequest;
pub use agent::AgentReadResult;
pub use agent::EXECUTION_AGENT_SCHEMA_VERSION;
pub use agent::ExecutionAgentCapabilities;
pub use agent::ReconcileEffectRequest;
pub use agent::ReconcileEffectResponse;
pub use agent::ReconcileEffectState;
pub use correlation::IncidentRelation;
pub use correlation::IncidentRelationKind;
pub use correlation::TimelineEventKind;
