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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::AutomationFeedbackId;
use crate::AutomationRunId;
use crate::ClusterId;
use crate::ContractError;
use crate::CorrelationId;
use crate::EvidenceId;
use crate::IncidentId;
use crate::InspectionRunId;
use crate::ModelInvocationId;
use crate::RecommendationId;
use crate::TenantId;

pub const AUTOMATION_SCHEMA_VERSION: &str = "rocketmq-sre.automation.v1";

/// Closed catalog of no-side-effect automation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoSideEffectAutomationKind {
    AlertCorrelation,
    SeverityOwnerSuggestion,
    EvidenceCollection,
    ShiftSummary,
    Notification,
    PostmortemDraft,
}

/// Closed preventive inspection families.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreventiveRiskFamily {
    Capacity,
    Certificate,
    Config,
    Route,
    Ha,
    Upgrade,
}

/// Durable lifecycle shared by no-side-effect and preventive runs.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutomationRunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Denied,
}

impl AutomationRunStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Denied)
    }
}

/// Hard resource limits for one automation run.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutomationBudget {
    pub max_model_calls: u8,
    pub max_output_bytes: u32,
    pub timeout_seconds: u16,
}

impl AutomationBudget {
    /// Validates the small, fail-closed automation budget.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive call, output, and runtime bounds.
    pub fn validate(self) -> Result<(), ContractError> {
        if self.max_model_calls > 4
            || !(1_024..=262_144).contains(&self.max_output_bytes)
            || !(1..=300).contains(&self.timeout_seconds)
        {
            return Err(invalid("automation budget is outside the bounded service limits"));
        }
        Ok(())
    }
}

/// Idempotent request for one bounded no-side-effect operation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSideEffectAutomationRequest {
    pub schema_version: String,
    pub id: AutomationRunId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub incident_id: Option<IncidentId>,
    pub correlation_id: CorrelationId,
    pub kind: NoSideEffectAutomationKind,
    pub idempotency_key: String,
    pub budget: AutomationBudget,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
    pub requested_by: String,
    pub requested_at: DateTime<Utc>,
}

impl NoSideEffectAutomationRequest {
    /// Validates scope, bounded inputs, and kind-specific requirements.
    ///
    /// # Errors
    ///
    /// Rejects unknown schema, missing scope, duplicate Evidence, unsafe
    /// identity text, and unbounded execution budgets.
    pub fn validate(&self) -> Result<(), ContractError> {
        let unique_evidence = self.evidence_ids.iter().collect::<BTreeSet<_>>();
        let incident_required = matches!(
            self.kind,
            NoSideEffectAutomationKind::SeverityOwnerSuggestion
                | NoSideEffectAutomationKind::EvidenceCollection
                | NoSideEffectAutomationKind::Notification
                | NoSideEffectAutomationKind::PostmortemDraft
        );
        let cluster_required = self.kind != NoSideEffectAutomationKind::ShiftSummary;
        if self.schema_version != AUTOMATION_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || self.tenant_id.as_uuid().is_nil()
            || self.correlation_id.as_uuid().is_nil()
            || (cluster_required && self.cluster_id.is_none())
            || (incident_required && self.incident_id.is_none())
            || !bounded_key(&self.idempotency_key)
            || !bounded_text(&self.requested_by, 256)
            || self.evidence_ids.len() > 64
            || unique_evidence.len() != self.evidence_ids.len()
        {
            return Err(invalid("no-side-effect automation request is incomplete or unbounded"));
        }
        self.budget.validate()
    }
}

/// Typed reference to one immutable output produced by an automation run.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutomationArtifact {
    pub kind: String,
    pub id: Uuid,
}

impl AutomationArtifact {
    fn validate(&self) -> bool {
        self.id != Uuid::nil() && bounded_text(&self.kind, 64)
    }
}

/// Durable no-side-effect outcome with no execution or approval surface.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSideEffectAutomationRun {
    pub schema_version: String,
    pub id: AutomationRunId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub incident_id: Option<IncidentId>,
    pub correlation_id: CorrelationId,
    pub kind: NoSideEffectAutomationKind,
    pub status: AutomationRunStatus,
    pub idempotency_key: String,
    pub result_code: String,
    pub sanitized_summary: String,
    #[serde(default)]
    pub artifacts: Vec<AutomationArtifact>,
    pub model_invocation_id: Option<ModelInvocationId>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl NoSideEffectAutomationRun {
    /// Validates the immutable operator-visible result.
    ///
    /// # Errors
    ///
    /// Rejects mismatched terminal timestamps, sensitive summaries, duplicate
    /// artifacts, and unbounded result fields.
    pub fn validate(&self) -> Result<(), ContractError> {
        let artifacts = self
            .artifacts
            .iter()
            .map(|artifact| artifact.id)
            .collect::<BTreeSet<_>>();
        let terminal_time_valid = self.status.is_terminal() == self.completed_at.is_some()
            && self
                .completed_at
                .is_none_or(|completed_at| completed_at >= self.started_at);
        if self.schema_version != AUTOMATION_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || self.tenant_id.as_uuid().is_nil()
            || self.correlation_id.as_uuid().is_nil()
            || !bounded_key(&self.idempotency_key)
            || !bounded_text(&self.result_code, 128)
            || !bounded_text(&self.sanitized_summary, 2_048)
            || contains_sensitive_marker(&self.sanitized_summary)
            || self.artifacts.len() > 64
            || artifacts.len() != self.artifacts.len()
            || self.artifacts.iter().any(|artifact| !artifact.validate())
            || !terminal_time_valid
        {
            return Err(invalid("no-side-effect automation result is incomplete or unsafe"));
        }
        Ok(())
    }
}

/// Idempotent preventive inspection request.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreventiveAutomationRequest {
    pub schema_version: String,
    pub id: AutomationRunId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub correlation_id: CorrelationId,
    pub risk_family: PreventiveRiskFamily,
    pub idempotency_key: String,
    pub budget: AutomationBudget,
    pub requested_by: String,
    pub requested_at: DateTime<Utc>,
}

impl PreventiveAutomationRequest {
    /// Validates the cluster-scoped preventive request.
    ///
    /// # Errors
    ///
    /// Rejects unknown schema, missing identities, or unbounded request data.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.schema_version != AUTOMATION_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || self.correlation_id.as_uuid().is_nil()
            || !bounded_key(&self.idempotency_key)
            || !bounded_text(&self.requested_by, 256)
        {
            return Err(invalid("preventive automation request is incomplete or unbounded"));
        }
        self.budget.validate()
    }
}

/// Durable outcome of one bounded preventive inspection.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreventiveAutomationRun {
    pub schema_version: String,
    pub id: AutomationRunId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub correlation_id: CorrelationId,
    pub risk_family: PreventiveRiskFamily,
    pub status: AutomationRunStatus,
    pub idempotency_key: String,
    pub inspection_run_id: Option<InspectionRunId>,
    #[serde(default)]
    pub recommendation_ids: Vec<RecommendationId>,
    pub freeze_id: Option<Uuid>,
    pub kill_switch_suggested: bool,
    pub result_code: String,
    pub sanitized_summary: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl PreventiveAutomationRun {
    /// Validates one bounded, operator-visible preventive result.
    ///
    /// # Errors
    ///
    /// Rejects invalid identities, duplicate recommendations, sensitive or
    /// unbounded summaries, and inconsistent lifecycle timestamps.
    pub fn validate(&self) -> Result<(), ContractError> {
        let recommendations = self.recommendation_ids.iter().collect::<BTreeSet<_>>();
        let terminal_time_valid = self.status.is_terminal() == self.completed_at.is_some()
            && self
                .completed_at
                .is_none_or(|completed_at| completed_at >= self.started_at);
        let succeeded_has_inspection =
            self.status != AutomationRunStatus::Succeeded || self.inspection_run_id.is_some();
        if self.schema_version != AUTOMATION_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || self.correlation_id.as_uuid().is_nil()
            || self.inspection_run_id.is_some_and(|id| id.as_uuid().is_nil())
            || self.freeze_id.is_some_and(|id| id.is_nil())
            || !bounded_key(&self.idempotency_key)
            || !bounded_text(&self.result_code, 128)
            || !bounded_text(&self.sanitized_summary, 2_048)
            || contains_sensitive_marker(&self.sanitized_summary)
            || self.recommendation_ids.len() > 256
            || recommendations.len() != self.recommendation_ids.len()
            || self.recommendation_ids.iter().any(|id| id.as_uuid().is_nil())
            || !terminal_time_valid
            || !succeeded_has_inspection
        {
            return Err(invalid("preventive automation result is incomplete or unsafe"));
        }
        Ok(())
    }
}

/// Subjects that can receive immutable operator feedback.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutomationFeedbackSubject {
    Severity,
    Owner,
    Summary,
    Recommendation,
    Plan,
}

/// Closed feedback verdicts used only by offline evaluation.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutomationFeedbackVerdict {
    Correct,
    Incorrect,
    Useful,
    NotUseful,
}

/// Append-only feedback that never changes production policy directly.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutomationOperatorFeedback {
    pub schema_version: String,
    pub id: AutomationFeedbackId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub incident_id: Option<IncidentId>,
    pub subject: AutomationFeedbackSubject,
    pub subject_id: Option<Uuid>,
    pub verdict: AutomationFeedbackVerdict,
    pub comment: Option<String>,
    pub actor_subject: String,
    pub created_at: DateTime<Utc>,
}

impl AutomationOperatorFeedback {
    /// Validates bounded, non-sensitive offline feedback.
    ///
    /// # Errors
    ///
    /// Rejects unknown schema, nil identities, sensitive comments, and
    /// unbounded actor-controlled text.
    pub fn validate(&self) -> Result<(), ContractError> {
        let comment_valid = self
            .comment
            .as_ref()
            .is_none_or(|comment| bounded_text(comment, 2_000) && !contains_sensitive_marker(comment));
        if self.schema_version != AUTOMATION_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || self.tenant_id.as_uuid().is_nil()
            || self.subject_id.is_some_and(|id| id == Uuid::nil())
            || !comment_valid
            || !bounded_text(&self.actor_subject, 256)
        {
            return Err(invalid("automation feedback is incomplete, sensitive, or unbounded"));
        }
        Ok(())
    }
}

fn bounded_key(value: &str) -> bool {
    (16..=200).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
}

fn bounded_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.chars().count() <= maximum && !value.chars().any(char::is_control)
}

fn contains_sensitive_marker(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    [
        "authorization:",
        "bearer ",
        "token=",
        "secret=",
        "password=",
        "private key",
        "message body",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn invalid(reason: &'static str) -> ContractError {
    ContractError::InvalidDescriptor {
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_side_effect_request_has_no_mutation_authority() {
        let request = NoSideEffectAutomationRequest {
            schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
            id: AutomationRunId::new(),
            tenant_id: TenantId::new(),
            cluster_id: Some(ClusterId::new()),
            incident_id: Some(IncidentId::new()),
            correlation_id: CorrelationId::new(),
            kind: NoSideEffectAutomationKind::EvidenceCollection,
            idempotency_key: "automation:evidence:0001".to_owned(),
            budget: AutomationBudget {
                max_model_calls: 0,
                max_output_bytes: 16_384,
                timeout_seconds: 30,
            },
            evidence_ids: vec![EvidenceId::new()],
            requested_by: "automation-service".to_owned(),
            requested_at: Utc::now(),
        };

        assert!(request.validate().is_ok());
        let encoded = serde_json::to_value(request).expect("automation request");
        assert!(encoded.get("approval").is_none());
        assert!(encoded.get("execution").is_none());
        assert!(encoded.get("close_incident").is_none());
    }

    #[test]
    fn automation_result_rejects_sensitive_summary() {
        let now = Utc::now();
        let mut run = NoSideEffectAutomationRun {
            schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
            id: AutomationRunId::new(),
            tenant_id: TenantId::new(),
            cluster_id: Some(ClusterId::new()),
            incident_id: Some(IncidentId::new()),
            correlation_id: CorrelationId::new(),
            kind: NoSideEffectAutomationKind::Notification,
            status: AutomationRunStatus::Succeeded,
            idempotency_key: "automation:notification:0001".to_owned(),
            result_code: "notification_enqueued".to_owned(),
            sanitized_summary: "Notification contains a deep link and bounded summary".to_owned(),
            artifacts: Vec::new(),
            model_invocation_id: None,
            started_at: now,
            completed_at: Some(now),
        };
        assert!(run.validate().is_ok());

        run.sanitized_summary = "authorization: bearer sensitive".to_owned();
        assert!(run.validate().is_err());
    }

    #[test]
    fn successful_preventive_result_requires_an_inspection_and_has_no_execution_surface() {
        let now = Utc::now();
        let mut run = PreventiveAutomationRun {
            schema_version: AUTOMATION_SCHEMA_VERSION.to_owned(),
            id: AutomationRunId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            correlation_id: CorrelationId::new(),
            risk_family: PreventiveRiskFamily::Capacity,
            status: AutomationRunStatus::Succeeded,
            idempotency_key: "preventive:capacity:0001".to_owned(),
            inspection_run_id: None,
            recommendation_ids: Vec::new(),
            freeze_id: None,
            kill_switch_suggested: false,
            result_code: "inspection_completed".to_owned(),
            sanitized_summary: "Capacity inspection completed with no critical recommendations".to_owned(),
            started_at: now,
            completed_at: Some(now),
        };
        assert!(run.validate().is_err());

        run.inspection_run_id = Some(InspectionRunId::new());
        assert!(run.validate().is_ok());
        let encoded = serde_json::to_value(run).expect("preventive run");
        assert!(encoded.get("approval").is_none());
        assert!(encoded.get("execution").is_none());
        assert!(encoded.get("clear_freeze").is_none());
    }
}
