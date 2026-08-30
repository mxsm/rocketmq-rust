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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::AssetSnapshotId;
use crate::ClusterId;
use crate::ConversationAnswerRevisionId;
use crate::ConversationId;
use crate::ConversationTurnId;
use crate::CorrelationId;
use crate::DiagnosisRevisionId;
use crate::EvidenceId;
use crate::IncidentId;
use crate::IncidentStatus;
use crate::InspectionRunId;
use crate::InvestigationId;
use crate::KnowledgeItemId;
use crate::ModelInvocationId;
use crate::ModelProfileId;
use crate::RecommendationId;
use crate::TenantId;
use crate::TimelineEventId;
use crate::TopologyEdgeId;

/// Identity that caused a workflow event.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct WorkflowActor {
    pub subject: String,
    pub display_name: Option<String>,
}

/// Conversation lifecycle before an investigation is required.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationStatus {
    Active,
    Promoted,
    Closed,
}

/// Operator question scoped to one tenant and cluster.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Conversation {
    pub id: ConversationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub question: String,
    pub resource: Option<String>,
    pub status: ConversationStatus,
    pub investigation_id: Option<InvestigationId>,
    pub created_by: WorkflowActor,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle of one bounded conversational read operation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationTurnStatus {
    Collecting,
    Answered,
    NeedsScope,
    NeedsEvidence,
    Cancelled,
    Failed,
}

/// Closed set of read-only query intents available to a conversation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationQueryKind {
    ClusterOverview,
    TopicList,
    TopicDescribe,
    ConsumerLag,
    BrokerRuntime,
    MetricInstant,
    MetricRange,
}

/// Server-validated query selected for one conversation turn.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConversationQueryIntent {
    pub schema_version: String,
    pub kind: ConversationQueryKind,
    pub source: String,
    pub resource: String,
    pub window_seconds: u32,
}

/// Evidence projection exposed alongside a factual answer.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConversationCitation {
    pub evidence_id: EvidenceId,
    pub source: String,
    pub resource: String,
    pub content_hash: String,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
}

/// One immutable operator question within a conversation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConversationTurn {
    pub id: ConversationTurnId,
    pub conversation_id: ConversationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub sequence: u32,
    pub question: String,
    pub resource: Option<String>,
    pub status: ConversationTurnStatus,
    pub query_intent: Option<ConversationQueryIntent>,
    pub correlation_id: CorrelationId,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Whether a persisted answer used a model or deterministic rules only.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationAnswerMode {
    ModelAssisted,
    RulesOnly,
}

/// Immutable, evidence-cited answer for one conversation turn.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConversationAnswerRevision {
    pub id: ConversationAnswerRevisionId,
    pub conversation_id: ConversationId,
    pub turn_id: ConversationTurnId,
    pub revision: u32,
    pub answer: String,
    pub mode: ConversationAnswerMode,
    pub citations: Vec<ConversationCitation>,
    pub evidence_ids: Vec<EvidenceId>,
    pub model_invocation_id: Option<ModelInvocationId>,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub created_at: DateTime<Utc>,
}

/// Multi-step read-only investigation lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationStatus {
    Open,
    Collecting,
    Diagnosing,
    NeedsEvidence,
    Monitoring,
    Promoted,
    Closed,
}

/// Persistent investigation aggregate.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Investigation {
    pub id: InvestigationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub conversation_id: Option<ConversationId>,
    pub incident_id: Option<IncidentId>,
    pub title: String,
    pub resource: Option<String>,
    pub symptom_family: String,
    pub fingerprint: String,
    pub status: InvestigationStatus,
    pub created_by: WorkflowActor,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Immutable timeline entry shared by investigations and incidents.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub id: TimelineEventId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub investigation_id: Option<InvestigationId>,
    pub incident_id: Option<IncidentId>,
    pub event_type: String,
    pub summary: String,
    pub details: Value,
    pub correlation_id: CorrelationId,
    pub actor: WorkflowActor,
    pub occurred_at: DateTime<Utc>,
}

/// Immutable diagnostic result revision.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DiagnosisRevision {
    pub id: DiagnosisRevisionId,
    pub incident_id: IncidentId,
    pub revision: u32,
    pub status: IncidentStatus,
    pub rule_result: Value,
    pub hypotheses: Value,
    pub evidence_ids: Vec<EvidenceId>,
    pub primary_model_invocation_id: Option<ModelInvocationId>,
    pub execution_eligible: bool,
    pub partial: bool,
    pub created_at: DateTime<Utc>,
}

/// Outcome of a deterministic diagnostic pack evaluated for an investigation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationDiagnosisStatus {
    Healthy,
    Fault,
    Inconclusive,
    Unsupported,
}

/// Immutable, evidence-bound diagnostic result created by a conversation turn.
///
/// Conversation diagnostics are advisory. They can be promoted to an incident,
/// but never grant execution authority by themselves.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct InvestigationDiagnosisRevision {
    pub id: DiagnosisRevisionId,
    pub investigation_id: InvestigationId,
    pub conversation_id: ConversationId,
    pub turn_id: ConversationTurnId,
    pub answer_revision_id: ConversationAnswerRevisionId,
    pub revision: u32,
    pub pack_id: String,
    pub pack_version: String,
    pub status: InvestigationDiagnosisStatus,
    pub rule_result: Value,
    pub hypotheses: Value,
    pub evidence_ids: Vec<EvidenceId>,
    pub primary_model_invocation_id: Option<ModelInvocationId>,
    pub execution_eligible: bool,
    pub partial: bool,
    pub correlation_id: CorrelationId,
    pub created_at: DateTime<Utc>,
}

/// Supported read-only inspection templates.
///
/// The original Phase 1 variants remain wire-compatible. Phase 2 adds
/// domain-complete templates used by preventive operations.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionTemplate {
    ClusterHealth,
    Consumer,
    Broker,
    Telemetry,
    FullCluster,
    ProducerConsumer,
    StoreHa,
    RoutingProxy,
    Security,
    Upgrade,
    DisasterRecovery,
}

/// Inspection lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionStatus {
    Scheduled,
    Running,
    NeedsEvidence,
    Completed,
    Failed,
    Cancelled,
}

/// Persisted read-only inspection run.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct InspectionRun {
    pub id: InspectionRunId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub template: InspectionTemplate,
    pub status: InspectionStatus,
    pub schedule: Option<String>,
    pub finding_count: u32,
    pub partial: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Operator disposition of an inspection recommendation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationStatus {
    Open,
    Acknowledged,
    Assigned,
    Dismissed,
    Resolved,
    Promoted,
}

/// Read-only recommendation emitted by an inspection.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Recommendation {
    pub id: RecommendationId,
    pub inspection_run_id: InspectionRunId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub severity: String,
    pub title: String,
    pub rationale: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub status: RecommendationStatus,
    pub assignee: Option<String>,
    pub investigation_id: Option<InvestigationId>,
    pub incident_id: Option<IncidentId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Asset classes normalized into the topology graph.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetKind {
    NameServer,
    Controller,
    Broker,
    Proxy,
    Store,
    Pod,
    Node,
    PersistentVolumeClaim,
    PodDisruptionBudget,
    Topic,
    Queue,
    Producer,
    Consumer,
    Connection,
}

/// Point-in-time normalized asset record.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct AssetSnapshot {
    pub id: AssetSnapshotId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub kind: AssetKind,
    pub external_key: String,
    pub display_name: String,
    pub source: String,
    pub attributes: Value,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    pub content_hash: String,
}

/// Versioned topology relationship.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyRelation {
    Contains,
    RoutesTo,
    StoresOn,
    RunsOn,
    ConnectsTo,
    ConsumesFrom,
    ProducesTo,
    ReplicatesTo,
}

/// Directed edge between normalized assets.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct TopologyEdge {
    pub id: TopologyEdgeId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub from_key: String,
    pub to_key: String,
    pub relation: TopologyRelation,
    pub source: String,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    pub content_hash: String,
}

/// Knowledge review lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeReviewStatus {
    Draft,
    InReview,
    Validated,
    Deprecated,
    Expired,
}

/// Searchable, source-backed knowledge item.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeItem {
    pub id: KnowledgeItemId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub title: String,
    pub component: String,
    pub rocketmq_version_range: String,
    pub source_uri: String,
    pub source_version: String,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub owner: String,
    pub review_status: KnowledgeReviewStatus,
    pub review_due_at: DateTime<Utc>,
    pub sensitivity: String,
    pub content_hash: String,
    pub conflict: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Feedback classifications that create a knowledge review task.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeFeedbackKind {
    Useful,
    Incorrect,
    Outdated,
}

/// Purpose of one model invocation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelInvocationPurpose {
    PrimaryDiagnosis,
    Critic,
    Planner,
    Summary,
    Eval,
    ConversationToolSelection,
    ConversationAnswer,
}

/// Persisted model identity, usage and fallback record. Hidden reasoning and
/// full prompts are intentionally absent.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ModelInvocationRecord {
    pub id: ModelInvocationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: Option<IncidentId>,
    pub conversation_id: Option<ConversationId>,
    pub investigation_id: Option<InvestigationId>,
    pub diagnosis_revision_id: Option<DiagnosisRevisionId>,
    pub parent_invocation_id: Option<ModelInvocationId>,
    pub purpose: ModelInvocationPurpose,
    pub requested_profile_id: ModelProfileId,
    pub actual_profile_id: ModelProfileId,
    pub provider_family: String,
    pub model_family: String,
    pub model_revision: String,
    pub endpoint_instance: String,
    pub fallback_chain: Vec<ModelProfileId>,
    pub prompt_version: String,
    pub schema_version: String,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cost_micros: Option<u64>,
    pub rationale: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_only_revision_has_no_primary_model() {
        let revision = DiagnosisRevision {
            id: DiagnosisRevisionId::new(),
            incident_id: IncidentId::new(),
            revision: 1,
            status: IncidentStatus::Monitoring,
            rule_result: Value::Null,
            hypotheses: Value::Array(Vec::new()),
            evidence_ids: Vec::new(),
            primary_model_invocation_id: None,
            execution_eligible: false,
            partial: false,
            created_at: Utc::now(),
        };
        assert!(revision.primary_model_invocation_id.is_none());
        assert!(!revision.execution_eligible);
    }

    #[test]
    fn investigation_diagnosis_is_read_only_and_round_trips() {
        let revision = InvestigationDiagnosisRevision {
            id: DiagnosisRevisionId::new(),
            investigation_id: InvestigationId::new(),
            conversation_id: ConversationId::new(),
            turn_id: ConversationTurnId::new(),
            answer_revision_id: ConversationAnswerRevisionId::new(),
            revision: 1,
            pack_id: "consumer-lag.v2".to_owned(),
            pack_version: "2.0.0".to_owned(),
            status: InvestigationDiagnosisStatus::Inconclusive,
            rule_result: serde_json::json!({"missing_required_evidence": ["consumer-lag"]}),
            hypotheses: Value::Array(Vec::new()),
            evidence_ids: vec![EvidenceId::new()],
            primary_model_invocation_id: None,
            execution_eligible: false,
            partial: true,
            correlation_id: CorrelationId::new(),
            created_at: Utc::now(),
        };
        let encoded = serde_json::to_value(&revision).expect("revision should serialize");
        let decoded: InvestigationDiagnosisRevision =
            serde_json::from_value(encoded).expect("revision should deserialize");
        assert_eq!(decoded, revision);
        assert!(!decoded.execution_eligible);
    }
}
