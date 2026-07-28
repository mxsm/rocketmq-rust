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

//! Deterministic schema export and signal-coverage manifest loading.

pub mod assertions;
pub mod phase1_shadow;
pub mod phase2;
pub mod replay;

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionItem;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanDraft;
use rocketmq_sre_contracts::ActivateLeaseRequest;
use rocketmq_sre_contracts::AdvanceFenceRequest;
use rocketmq_sre_contracts::AdvanceFenceResponse;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentDispatchResponse;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::AgentStepResult;
use rocketmq_sre_contracts::AlertEvent;
use rocketmq_sre_contracts::ApprovalGrant;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::BacklogEta;
use rocketmq_sre_contracts::BeginLeaseTakeoverRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverResponse;
use rocketmq_sre_contracts::CapacityForecast;
use rocketmq_sre_contracts::ChangeConflict;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ClusterForecastReport;
use rocketmq_sre_contracts::ClusterHealthReport;
use rocketmq_sre_contracts::CompensationEdge;
use rocketmq_sre_contracts::CriticAssessment;
use rocketmq_sre_contracts::CriticFinding;
use rocketmq_sre_contracts::CriticGateState;
use rocketmq_sre_contracts::CriticReview;
use rocketmq_sre_contracts::Descriptor;
use rocketmq_sre_contracts::DrReadinessReport;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionAgentCapabilities;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionResult;
use rocketmq_sre_contracts::ExecutionSliObservation;
use rocketmq_sre_contracts::ExecutionSliQuery;
use rocketmq_sre_contracts::ExecutionTransition;
use rocketmq_sre_contracts::ExecutorLease;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::FleetHealthReport;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::Incident;
use rocketmq_sre_contracts::IncidentOperationRequest;
use rocketmq_sre_contracts::IncidentOperationResult;
use rocketmq_sre_contracts::IncidentOperationsState;
use rocketmq_sre_contracts::IssueFenceGrantRequest;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::ManualGate;
use rocketmq_sre_contracts::ManualRunbookDraft;
use rocketmq_sre_contracts::NotificationDelivery;
use rocketmq_sre_contracts::OperationsReport;
use rocketmq_sre_contracts::Phase2ContractManifest;
use rocketmq_sre_contracts::PlanStep;
use rocketmq_sre_contracts::PolicyDecision;
use rocketmq_sre_contracts::PostmortemDraft;
use rocketmq_sre_contracts::PostmortemRevision;
use rocketmq_sre_contracts::ReconcileEffectRequest;
use rocketmq_sre_contracts::ReconcileEffectResponse;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookStep;
use rocketmq_sre_contracts::ShiftHandoffSummary;
use rocketmq_sre_contracts::StepIntent;
use rocketmq_sre_contracts::StepResult;
use rocketmq_sre_contracts::TopologySnapshot;
use rocketmq_sre_contracts::UpgradeReadinessReport;
use rocketmq_sre_contracts::VerificationResult;
use rocketmq_sre_contracts::VerifyExecutionRequest;
use rocketmq_sre_contracts::VerifyFenceGrantRequest;
use rocketmq_sre_contracts::VerifyReconcileGrantRequest;
use rocketmq_sre_contracts::WhatIfSimulation;
use rocketmq_sre_contracts::WhatIfSimulationRequest;
use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::CanonicalModelResponse;
use schemars::JsonSchema;
use schemars::schema_for;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Required-signal manifest schema implemented by Phase 00.
pub const REQUIRED_SIGNALS_SCHEMA_VERSION: &str = "rocketmq.sre.required-signals.v1";

/// Schema-only mirror of the Connector-owned MCP capability wire contract.
///
/// Keeping this type local preserves the black-box MCP boundary while still
/// publishing the exact Phase 00 JSON shape as a standalone artifact.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct CapabilityManifestWire {
    mcp_protocol_version: String,
    business_schema_version: String,
    server_version: String,
    cluster: String,
    tools: Vec<CapabilityToolWire>,
    resources: Vec<String>,
    tool_surface_digest: String,
    mutation_supported: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct CapabilityToolWire {
    name: String,
    risk_level: String,
    schema_digest: String,
    read_only: bool,
    destructive: bool,
    task_support: String,
    mutates_cluster: bool,
}

/// Manifest status without inventing unavailable signal values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalImplementationStatus {
    Existing,
    MissingInstrumentation,
    InProcessOnly,
    Queryable,
}

/// One required signal and its evidence mapping.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequiredSignal {
    pub requirement_id: String,
    pub purpose: String,
    pub owner: String,
    pub source_symbol: String,
    pub signal_type: String,
    pub registry_reference: String,
    pub status: SignalImplementationStatus,
    pub query: Option<String>,
    pub freshness_seconds: Option<u64>,
    #[serde(default)]
    pub expected_attributes: BTreeSet<String>,
    pub sensitivity: String,
    pub missing_behavior: String,
    pub evidence_field: String,
}

/// Required signals for one RocketMQ component.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequiredSignalManifest {
    pub schema_version: String,
    pub component: String,
    pub signals: Vec<RequiredSignal>,
}

/// Coverage or schema utility failure.
#[derive(Debug, Error)]
pub enum EvalError {
    #[error("failed to access `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid YAML coverage manifest: {0}")]
    InvalidYaml(#[from] serde_yaml::Error),
    #[error("duplicate requirement id `{0}`")]
    DuplicateRequirement(String),
    #[error("unsupported required-signal schema `{actual}`; expected `{expected}`")]
    UnsupportedRequiredSignalSchema { expected: &'static str, actual: String },
    #[error("failed to encode JSON schema: {0}")]
    SchemaEncoding(#[from] serde_json::Error),
}

/// Loads and validates a required-signal YAML manifest.
///
/// # Errors
///
/// Returns an I/O, YAML, or duplicate identifier error.
pub fn load_required_signals(path: &Path) -> Result<RequiredSignalManifest, EvalError> {
    let yaml = fs::read_to_string(path).map_err(|source| EvalError::Io {
        path: path.display().to_string(),
        source,
    })?;
    parse_required_signals(&yaml)
}

/// Parses a manifest while enforcing unique requirement IDs.
///
/// # Errors
///
/// Returns an invalid YAML or duplicate identifier error.
pub fn parse_required_signals(yaml: &str) -> Result<RequiredSignalManifest, EvalError> {
    let manifest: RequiredSignalManifest = serde_yaml::from_str(yaml)?;
    if manifest.schema_version != REQUIRED_SIGNALS_SCHEMA_VERSION {
        return Err(EvalError::UnsupportedRequiredSignalSchema {
            expected: REQUIRED_SIGNALS_SCHEMA_VERSION,
            actual: manifest.schema_version,
        });
    }
    let mut seen = BTreeSet::new();
    for signal in &manifest.signals {
        if !seen.insert(&signal.requirement_id) {
            return Err(EvalError::DuplicateRequirement(signal.requirement_id.clone()));
        }
    }
    Ok(manifest)
}

/// Writes stable, pretty JSON schemas for Phase 00 public contracts.
///
/// # Errors
///
/// Returns an I/O or JSON encoding error.
pub fn export_schemas(output_dir: &Path) -> Result<(), EvalError> {
    fs::create_dir_all(output_dir).map_err(|source| EvalError::Io {
        path: output_dir.display().to_string(),
        source,
    })?;
    for (name, schema) in generated_schemas()? {
        let path = output_dir.join(name);
        let mut bytes = serde_json::to_vec_pretty(&schema)?;
        bytes.push(b'\n');
        fs::write(&path, bytes).map_err(|source| EvalError::Io {
            path: path.display().to_string(),
            source,
        })?;
    }
    Ok(())
}

/// Writes only the Phase 3 supervised-execution schemas.
///
/// # Errors
///
/// Returns an I/O or JSON encoding error.
pub fn export_phase3_schemas(output_dir: &Path) -> Result<(), EvalError> {
    fs::create_dir_all(output_dir).map_err(|source| EvalError::Io {
        path: output_dir.display().to_string(),
        source,
    })?;
    for (name, schema) in phase3_generated_schemas()? {
        let path = output_dir.join(name);
        let mut bytes = serde_json::to_vec_pretty(&schema)?;
        bytes.push(b'\n');
        fs::write(&path, bytes).map_err(|source| EvalError::Io {
            path: path.display().to_string(),
            source,
        })?;
    }
    Ok(())
}

/// Returns every generated Phase 3 schema and its stable artifact name.
///
/// # Errors
///
/// Returns a JSON encoding error if a public schema cannot be represented.
pub fn phase3_generated_schemas() -> Result<Vec<(&'static str, serde_json::Value)>, EvalError> {
    Ok(vec![
        (
            "execution-action.schema.json",
            serde_json::to_value(schema_for!(ExecutionAction))?,
        ),
        (
            "action-descriptor.schema.json",
            serde_json::to_value(schema_for!(ActionDescriptor))?,
        ),
        ("plan-step.schema.json", serde_json::to_value(schema_for!(PlanStep))?),
        (
            "action-plan-draft.schema.json",
            serde_json::to_value(schema_for!(ActionPlanDraft))?,
        ),
        (
            "action-plan.schema.json",
            serde_json::to_value(schema_for!(ActionPlan))?,
        ),
        (
            "manual-runbook-draft.schema.json",
            serde_json::to_value(schema_for!(ManualRunbookDraft))?,
        ),
        (
            "runbook-definition.schema.json",
            serde_json::to_value(schema_for!(RunbookDefinition))?,
        ),
        (
            "runbook-step.schema.json",
            serde_json::to_value(schema_for!(RunbookStep))?,
        ),
        (
            "manual-gate.schema.json",
            serde_json::to_value(schema_for!(ManualGate))?,
        ),
        (
            "compensation-edge.schema.json",
            serde_json::to_value(schema_for!(CompensationEdge))?,
        ),
        (
            "change-window.schema.json",
            serde_json::to_value(schema_for!(ChangeWindow))?,
        ),
        (
            "change-schedule.schema.json",
            serde_json::to_value(schema_for!(ChangeSchedule))?,
        ),
        (
            "change-conflict.schema.json",
            serde_json::to_value(schema_for!(ChangeConflict))?,
        ),
        (
            "approval-record.schema.json",
            serde_json::to_value(schema_for!(ApprovalRecord))?,
        ),
        (
            "policy-decision.schema.json",
            serde_json::to_value(schema_for!(PolicyDecision))?,
        ),
        (
            "approval-grant.schema.json",
            serde_json::to_value(schema_for!(ApprovalGrant))?,
        ),
        (
            "critic-assessment.schema.json",
            serde_json::to_value(schema_for!(CriticAssessment))?,
        ),
        (
            "critic-finding.schema.json",
            serde_json::to_value(schema_for!(CriticFinding))?,
        ),
        (
            "critic-gate-state.schema.json",
            serde_json::to_value(schema_for!(CriticGateState))?,
        ),
        (
            "critic-review.schema.json",
            serde_json::to_value(schema_for!(CriticReview))?,
        ),
        (
            "execution-transition.schema.json",
            serde_json::to_value(schema_for!(ExecutionTransition))?,
        ),
        (
            "execution-request.schema.json",
            serde_json::to_value(schema_for!(ExecutionRequest))?,
        ),
        (
            "execution-sli-query.schema.json",
            serde_json::to_value(schema_for!(ExecutionSliQuery))?,
        ),
        (
            "execution-sli-observation.schema.json",
            serde_json::to_value(schema_for!(ExecutionSliObservation))?,
        ),
        (
            "step-intent.schema.json",
            serde_json::to_value(schema_for!(StepIntent))?,
        ),
        (
            "agent-step-request.schema.json",
            serde_json::to_value(schema_for!(AgentStepRequest))?,
        ),
        (
            "agent-step-result.schema.json",
            serde_json::to_value(schema_for!(AgentStepResult))?,
        ),
        (
            "agent-read-request.schema.json",
            serde_json::to_value(schema_for!(AgentReadRequest))?,
        ),
        (
            "agent-read-result.schema.json",
            serde_json::to_value(schema_for!(AgentReadResult))?,
        ),
        (
            "agent-dispatch-request.schema.json",
            serde_json::to_value(schema_for!(AgentDispatchRequest))?,
        ),
        (
            "agent-dispatch-response.schema.json",
            serde_json::to_value(schema_for!(AgentDispatchResponse))?,
        ),
        (
            "execution-agent-capabilities.schema.json",
            serde_json::to_value(schema_for!(ExecutionAgentCapabilities))?,
        ),
        (
            "reconcile-effect-request.schema.json",
            serde_json::to_value(schema_for!(ReconcileEffectRequest))?,
        ),
        (
            "reconcile-effect-response.schema.json",
            serde_json::to_value(schema_for!(ReconcileEffectResponse))?,
        ),
        (
            "advance-fence-request.schema.json",
            serde_json::to_value(schema_for!(AdvanceFenceRequest))?,
        ),
        (
            "advance-fence-response.schema.json",
            serde_json::to_value(schema_for!(AdvanceFenceResponse))?,
        ),
        (
            "step-result.schema.json",
            serde_json::to_value(schema_for!(StepResult))?,
        ),
        (
            "execution-result.schema.json",
            serde_json::to_value(schema_for!(ExecutionResult))?,
        ),
        (
            "verification-result.schema.json",
            serde_json::to_value(schema_for!(VerificationResult))?,
        ),
        (
            "lease-fence-grant.schema.json",
            serde_json::to_value(schema_for!(LeaseFenceGrant))?,
        ),
        (
            "reconcile-grant.schema.json",
            serde_json::to_value(schema_for!(ReconcileGrant))?,
        ),
        ("fence-ack.schema.json", serde_json::to_value(schema_for!(FenceAck))?),
        (
            "executor-lease.schema.json",
            serde_json::to_value(schema_for!(ExecutorLease))?,
        ),
        (
            "begin-lease-takeover-request.schema.json",
            serde_json::to_value(schema_for!(BeginLeaseTakeoverRequest))?,
        ),
        (
            "begin-lease-takeover-response.schema.json",
            serde_json::to_value(schema_for!(BeginLeaseTakeoverResponse))?,
        ),
        (
            "issue-fence-grant-request.schema.json",
            serde_json::to_value(schema_for!(IssueFenceGrantRequest))?,
        ),
        (
            "activate-lease-request.schema.json",
            serde_json::to_value(schema_for!(ActivateLeaseRequest))?,
        ),
        (
            "verify-execution-request.schema.json",
            serde_json::to_value(schema_for!(VerifyExecutionRequest))?,
        ),
        (
            "verify-fence-grant-request.schema.json",
            serde_json::to_value(schema_for!(VerifyFenceGrantRequest))?,
        ),
        (
            "verify-reconcile-grant-request.schema.json",
            serde_json::to_value(schema_for!(VerifyReconcileGrantRequest))?,
        ),
        (
            "grant-verification.schema.json",
            serde_json::to_value(schema_for!(GrantVerification))?,
        ),
        (
            "audit-event.schema.json",
            serde_json::to_value(schema_for!(AuditEvent))?,
        ),
        (
            "resource-quarantine.schema.json",
            serde_json::to_value(schema_for!(ResourceQuarantine))?,
        ),
    ])
}

fn generated_schemas() -> Result<Vec<(&'static str, serde_json::Value)>, EvalError> {
    Ok(vec![
        (
            "evidence-query.schema.json",
            serde_json::to_value(schema_for!(EvidenceQuery))?,
        ),
        (
            "evidence-snapshot.schema.json",
            serde_json::to_value(schema_for!(EvidenceSnapshot))?,
        ),
        ("incident.schema.json", serde_json::to_value(schema_for!(Incident))?),
        ("descriptor.schema.json", serde_json::to_value(schema_for!(Descriptor))?),
        (
            "action-descriptor.schema.json",
            serde_json::to_value(schema_for!(ActionDescriptor))?,
        ),
        (
            "capability-manifest.schema.json",
            serde_json::to_value(schema_for!(CapabilityManifestWire))?,
        ),
        (
            "model-request.schema.json",
            serde_json::to_value(schema_for!(CanonicalModelRequest))?,
        ),
        (
            "model-response.schema.json",
            serde_json::to_value(schema_for!(CanonicalModelResponse))?,
        ),
        (
            "alert-event.schema.json",
            serde_json::to_value(schema_for!(AlertEvent))?,
        ),
        (
            "topology-snapshot.schema.json",
            serde_json::to_value(schema_for!(TopologySnapshot))?,
        ),
        (
            "cluster-health-report.schema.json",
            serde_json::to_value(schema_for!(ClusterHealthReport))?,
        ),
        (
            "fleet-health-report.schema.json",
            serde_json::to_value(schema_for!(FleetHealthReport))?,
        ),
        (
            "capacity-forecast.schema.json",
            serde_json::to_value(schema_for!(CapacityForecast))?,
        ),
        (
            "cluster-forecast-report.schema.json",
            serde_json::to_value(schema_for!(ClusterForecastReport))?,
        ),
        (
            "backlog-eta.schema.json",
            serde_json::to_value(schema_for!(BacklogEta))?,
        ),
        (
            "what-if-simulation.schema.json",
            serde_json::to_value(schema_for!(WhatIfSimulation))?,
        ),
        (
            "what-if-simulation-request.schema.json",
            serde_json::to_value(schema_for!(WhatIfSimulationRequest))?,
        ),
        (
            "upgrade-readiness-report.schema.json",
            serde_json::to_value(schema_for!(UpgradeReadinessReport))?,
        ),
        (
            "dr-readiness-report.schema.json",
            serde_json::to_value(schema_for!(DrReadinessReport))?,
        ),
        (
            "notification-delivery.schema.json",
            serde_json::to_value(schema_for!(NotificationDelivery))?,
        ),
        (
            "postmortem-draft.schema.json",
            serde_json::to_value(schema_for!(PostmortemDraft))?,
        ),
        (
            "postmortem-revision.schema.json",
            serde_json::to_value(schema_for!(PostmortemRevision))?,
        ),
        (
            "action-item.schema.json",
            serde_json::to_value(schema_for!(ActionItem))?,
        ),
        (
            "phase2-contract-manifest.schema.json",
            serde_json::to_value(schema_for!(Phase2ContractManifest))?,
        ),
        (
            "incident-operation-request.schema.json",
            serde_json::to_value(schema_for!(IncidentOperationRequest))?,
        ),
        (
            "incident-operation-result.schema.json",
            serde_json::to_value(schema_for!(IncidentOperationResult))?,
        ),
        (
            "incident-operations-state.schema.json",
            serde_json::to_value(schema_for!(IncidentOperationsState))?,
        ),
        (
            "shift-handoff-summary.schema.json",
            serde_json::to_value(schema_for!(ShiftHandoffSummary))?,
        ),
        (
            "operations-report.schema.json",
            serde_json::to_value(schema_for!(OperationsReport))?,
        ),
    ])
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::Deprecation;
    use rocketmq_sre_contracts::DescriptorKind;
    use rocketmq_sre_contracts::DescriptorStatus;
    use rocketmq_sre_contracts::DiagnosticPackDescriptor;
    use rocketmq_sre_contracts::EvidenceSourceDescriptor;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_core::DescriptorRegistry;
    use serde_json::json;

    use super::*;

    fn evidence_source(version: &str) -> Descriptor {
        Descriptor::EvidenceSource(EvidenceSourceDescriptor {
            id: "fake-broker-source".to_owned(),
            version: version.to_owned(),
            owner: "eval".to_owned(),
            supported_versions: vec![SchemaVersion::new("rocketmq-sre.evidence", 1, 0)],
            required_capabilities: BTreeSet::from(["evidence.query".to_owned()]),
            config_schema: json!({"type": "object"}),
            status: DescriptorStatus::Active,
            deprecation: None,
            source_kind: "fixture".to_owned(),
            query_schema: json!({"type": "object"}),
            result_schema: json!({"type": "object"}),
        })
    }

    fn diagnostic_pack() -> Descriptor {
        Descriptor::DiagnosticPack(DiagnosticPackDescriptor {
            id: "fake-cluster-health".to_owned(),
            version: "1.0.0".to_owned(),
            owner: "eval".to_owned(),
            supported_versions: vec![SchemaVersion::new("rocketmq-sre.evidence", 1, 0)],
            required_capabilities: BTreeSet::from(["evidence.query".to_owned()]),
            config_schema: json!({"type": "object"}),
            status: DescriptorStatus::Active,
            deprecation: None,
            required_sources: BTreeSet::from(["fake-broker-source".to_owned()]),
            produced_hypotheses: BTreeSet::from(["broker_unavailable".to_owned()]),
        })
    }

    #[test]
    fn fake_source_and_diagnostic_pack_follow_registry_lifecycle() {
        let mut registry = DescriptorRegistry::new([("rocketmq-sre.evidence", 1)], ["evidence.query"]);
        registry
            .register(evidence_source("1.0.0"))
            .expect("fake source should register");
        registry
            .upgrade(evidence_source("1.1.0"))
            .expect("fake source should upgrade");
        registry
            .disable(DescriptorKind::EvidenceSource, "fake-broker-source")
            .expect("active source should disable");
        assert_eq!(
            registry
                .get(DescriptorKind::EvidenceSource, "fake-broker-source")
                .expect("source should remain registered")
                .status(),
            DescriptorStatus::Disabled
        );
        registry
            .rollback(DescriptorKind::EvidenceSource, "fake-broker-source", "1.0.0")
            .expect("source should roll back without losing history");
        assert_eq!(
            registry
                .get(DescriptorKind::EvidenceSource, "fake-broker-source")
                .expect("rolled-back source should exist")
                .version(),
            "1.0.0"
        );

        registry
            .register(diagnostic_pack())
            .expect("fake diagnostic pack should register");
        registry
            .deprecate(
                DescriptorKind::DiagnosticPack,
                "fake-cluster-health",
                Deprecation {
                    since: "1.1.0".to_owned(),
                    replacement: Some("cluster-health-v2".to_owned()),
                    message: "fixture migration".to_owned(),
                },
            )
            .expect("fake diagnostic pack should deprecate");
        assert_eq!(
            registry
                .get(DescriptorKind::DiagnosticPack, "fake-cluster-health")
                .expect("diagnostic pack should remain registered")
                .status(),
            DescriptorStatus::Deprecated
        );
    }

    #[test]
    fn rejects_duplicate_requirements() {
        let yaml = r#"
schema_version: rocketmq.sre.required-signals.v1
component: broker
signals:
  -
    requirement_id: broker.up
    purpose: availability
    owner: broker
    source_symbol: broker::start
    signal_type: metric
    registry_reference: rocketmq_broker_up
    status: existing
    query: null
    freshness_seconds: 30
    expected_attributes: []
    sensitivity: internal
    missing_behavior: missing
    evidence_field: availability.up
  -
    requirement_id: broker.up
    purpose: availability duplicate
    owner: broker
    source_symbol: broker::start
    signal_type: metric
    registry_reference: rocketmq_broker_up
    status: existing
    query: null
    freshness_seconds: 30
    expected_attributes: []
    sensitivity: internal
    missing_behavior: missing
    evidence_field: availability.up
"#;

        assert!(matches!(
            parse_required_signals(yaml),
            Err(EvalError::DuplicateRequirement(id)) if id == "broker.up"
        ));
    }

    #[test]
    fn parses_explicit_missing_instrumentation() {
        let yaml = r#"
schema_version: rocketmq.sre.required-signals.v1
component: nameserver
signals:
  - requirement_id: nameserver.route_freshness
    purpose: route freshness
    owner: name-server
    source_symbol: namesrv::route
    signal_type: metric
    registry_reference: rocketmq_namesrv_route_age
    status: missing_instrumentation
    query: null
    freshness_seconds: null
    expected_attributes: [cluster]
    sensitivity: internal
    missing_behavior: not_production_verified
    evidence_field: routing.freshness
"#;

        let manifest = parse_required_signals(yaml).expect("fixture should parse");
        assert_eq!(
            manifest.signals[0].status,
            SignalImplementationStatus::MissingInstrumentation
        );
    }

    #[test]
    fn rejects_legacy_required_signal_schema_spelling() {
        let yaml = r#"
schema_version: rocketmq-sre.required-signals.v1
component: broker
signals:
  - requirement_id: broker.up
    purpose: availability
    owner: broker
    source_symbol: broker::start
    signal_type: metric
    registry_reference: rocketmq_broker_up
    status: existing
    query: null
    freshness_seconds: 30
    expected_attributes: []
    sensitivity: internal
    missing_behavior: missing
    evidence_field: availability.up
"#;

        assert!(matches!(
            parse_required_signals(yaml),
            Err(EvalError::UnsupportedRequiredSignalSchema { actual, .. })
                if actual == "rocketmq-sre.required-signals.v1"
        ));
    }

    #[test]
    fn committed_schema_fixtures_match_public_contracts() {
        let committed = [
            (
                "evidence-query.schema.json",
                include_str!("../../../schemas/evidence-query.schema.json"),
            ),
            (
                "evidence-snapshot.schema.json",
                include_str!("../../../schemas/evidence-snapshot.schema.json"),
            ),
            (
                "incident.schema.json",
                include_str!("../../../schemas/incident.schema.json"),
            ),
            (
                "descriptor.schema.json",
                include_str!("../../../schemas/descriptor.schema.json"),
            ),
            (
                "action-descriptor.schema.json",
                include_str!("../../../schemas/action-descriptor.schema.json"),
            ),
            (
                "capability-manifest.schema.json",
                include_str!("../../../schemas/capability-manifest.schema.json"),
            ),
            (
                "model-request.schema.json",
                include_str!("../../../schemas/model-request.schema.json"),
            ),
            (
                "model-response.schema.json",
                include_str!("../../../schemas/model-response.schema.json"),
            ),
            (
                "alert-event.schema.json",
                include_str!("../../../schemas/alert-event.schema.json"),
            ),
            (
                "topology-snapshot.schema.json",
                include_str!("../../../schemas/topology-snapshot.schema.json"),
            ),
            (
                "cluster-health-report.schema.json",
                include_str!("../../../schemas/cluster-health-report.schema.json"),
            ),
            (
                "fleet-health-report.schema.json",
                include_str!("../../../schemas/fleet-health-report.schema.json"),
            ),
            (
                "capacity-forecast.schema.json",
                include_str!("../../../schemas/capacity-forecast.schema.json"),
            ),
            (
                "cluster-forecast-report.schema.json",
                include_str!("../../../schemas/cluster-forecast-report.schema.json"),
            ),
            (
                "backlog-eta.schema.json",
                include_str!("../../../schemas/backlog-eta.schema.json"),
            ),
            (
                "what-if-simulation.schema.json",
                include_str!("../../../schemas/what-if-simulation.schema.json"),
            ),
            (
                "what-if-simulation-request.schema.json",
                include_str!("../../../schemas/what-if-simulation-request.schema.json"),
            ),
            (
                "upgrade-readiness-report.schema.json",
                include_str!("../../../schemas/upgrade-readiness-report.schema.json"),
            ),
            (
                "dr-readiness-report.schema.json",
                include_str!("../../../schemas/dr-readiness-report.schema.json"),
            ),
            (
                "notification-delivery.schema.json",
                include_str!("../../../schemas/notification-delivery.schema.json"),
            ),
            (
                "postmortem-draft.schema.json",
                include_str!("../../../schemas/postmortem-draft.schema.json"),
            ),
            (
                "postmortem-revision.schema.json",
                include_str!("../../../schemas/postmortem-revision.schema.json"),
            ),
            (
                "action-item.schema.json",
                include_str!("../../../schemas/action-item.schema.json"),
            ),
            (
                "phase2-contract-manifest.schema.json",
                include_str!("../../../schemas/phase2-contract-manifest.schema.json"),
            ),
        ];
        let generated = generated_schemas().expect("schemas should encode");

        for ((generated_name, generated_schema), (committed_name, committed_schema)) in
            generated.into_iter().zip(committed)
        {
            assert_eq!(generated_name, committed_name);
            let committed_value: serde_json::Value =
                serde_json::from_str(committed_schema).expect("fixture should be JSON");
            assert_eq!(generated_schema, committed_value, "{generated_name} drifted");
        }
    }
}
