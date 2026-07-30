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

use std::sync::Arc;
use std::time::Instant;

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::DiagnosisRevision;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceRelation;
use rocketmq_sre_contracts::HypothesisStatus;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentStatus;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_core::diagnostics::ConfidenceBand;
use rocketmq_sre_core::diagnostics::DiagnosticEngine;
use rocketmq_sre_core::diagnostics::DiagnosticReport;
use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use rocketmq_sre_core::diagnostics::EvidenceRequirement;
use rocketmq_sre_core::diagnostics::FindingOutcome;
use rocketmq_sre_core::diagnostics::Severity;
use rocketmq_sre_core::diagnostics::full_registry;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use tracing::Instrument;

use super::citation::validate_report_citations;
use super::limits::BudgetUsage;
use super::limits::OrchestratorLimits;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceListQuery;
use crate::evidence::EvidenceService;
use crate::evidence::PersistEvidenceRequest;
use crate::models::ModelDiagnosisDecision;
use crate::models::ModelGatewayService;
use crate::observability::CorrelationContext;
use crate::observability::DiagnosticPackLabel;
use crate::observability::EvidenceSourceLabel;
use crate::observability::IncidentOutcome;
use crate::observability::ResultClass;
use crate::observability::SreObservability;
use crate::workflow::IncidentView;
use crate::workflow::WorkflowService;

const RULES_ONLY_REASON: &str = "RulesOnlyDiagnosisNotExecutable";

/// Stable response for a completed, bounded read-only diagnosis.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct DiagnosisResponse {
    pub(crate) schema_version: &'static str,
    pub(crate) incident_id: IncidentId,
    pub(crate) revision: DiagnosisRevision,
    pub(crate) pack_id: String,
    pub(crate) mode: &'static str,
    pub(crate) reason: &'static str,
    pub(crate) budget: BudgetUsage,
    pub(crate) execution_eligible: bool,
}

/// Deterministic Phase 1 orchestrator.
///
/// The service owns no RocketMQ credentials and cannot call mutation tools.
/// Evidence is read through the scoped Evidence Service. A model provider can
/// be added behind the Model Gateway in later phases; the default is an
/// explicit, persisted rules-only revision.
#[derive(Clone)]
pub(crate) struct OrchestratorService {
    workflow: WorkflowService,
    evidence: EvidenceService,
    diagnostics: Arc<DiagnosticEngine>,
    limits: OrchestratorLimits,
    observability: SreObservability,
    connector_channel: Option<PostgresConnectorChannelService>,
    model_gateway: Option<ModelGatewayService>,
}

impl OrchestratorService {
    pub(crate) fn new(
        workflow: WorkflowService,
        evidence: EvidenceService,
        observability: SreObservability,
    ) -> Result<Self, ControlPlaneError> {
        let registry = full_registry().map_err(|error| {
            ControlPlaneError::configuration(format!("built-in diagnostic registry is invalid: {error}"))
        })?;
        Ok(Self {
            workflow,
            evidence,
            diagnostics: Arc::new(DiagnosticEngine::new(registry)),
            limits: OrchestratorLimits::default(),
            observability,
            connector_channel: None,
            model_gateway: None,
        })
    }

    #[must_use]
    pub(crate) fn with_connector_channel(mut self, connector_channel: PostgresConnectorChannelService) -> Self {
        self.connector_channel = Some(connector_channel);
        self
    }

    #[must_use]
    pub(crate) fn with_model_gateway(mut self, model_gateway: ModelGatewayService) -> Self {
        self.model_gateway = Some(model_gateway);
        self
    }

    pub(crate) async fn diagnose(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        correlation_id: CorrelationId,
    ) -> Result<DiagnosisResponse, ControlPlaneError> {
        let correlation = CorrelationContext::from_id(correlation_id);
        self.observability.record_incident(IncidentOutcome::Started);
        let result = self
            .diagnose_inner(auth, incident_id, correlation_id)
            .instrument(self.observability.incident_run_span(correlation))
            .await;
        self.observability.record_incident(match &result {
            Ok(response) => successful_incident_outcome(response.mode),
            Err(_) => IncidentOutcome::Failed,
        });
        result
    }

    async fn diagnose_inner(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        correlation_id: CorrelationId,
    ) -> Result<DiagnosisResponse, ControlPlaneError> {
        self.workflow.ensure_operator(auth)?;
        let mut incident = self.workflow.incident(auth, incident_id).await?;
        if incident.incident.status.is_terminal() {
            return Err(ControlPlaneError::conflict(
                "terminal incidents cannot be diagnosed again",
            ));
        }

        if matches!(
            incident.incident.status,
            IncidentStatus::New | IncidentStatus::NeedsEvidence | IncidentStatus::Monitoring
        ) {
            incident = self
                .workflow
                .transition_incident(
                    auth,
                    incident_id,
                    IncidentStatus::Collecting,
                    "evidence_collection_started",
                    correlation_id,
                )
                .await?;
        }

        let pack_id = select_pack(&incident);
        let correlation = CorrelationContext::from_id(correlation_id);
        let (evidence, tool_calls_used, query_retries_used) = self
            .collect_evidence(auth, &incident, pack_id, correlation_id, correlation)
            .await?;
        if incident.incident.status == IncidentStatus::Collecting {
            self.workflow
                .transition_incident(
                    auth,
                    incident_id,
                    IncidentStatus::Diagnosing,
                    "deterministic_diagnosis_started",
                    correlation_id,
                )
                .await?;
        }

        let report = self.evaluate(pack_id, &evidence.items, correlation)?;
        let evidence_ids = validate_report_citations(&report, &evidence.items)?;
        let next_status = terminal_status(&report);
        let partial = evidence.partial
            || !report.missing_required_evidence.is_empty()
            || matches!(
                report.status,
                DiagnosticStatus::Inconclusive | DiagnosticStatus::Unsupported
            );
        let mut rule_result = report_json(&report);
        let hypotheses = hypotheses_json(&report);
        let model_decision = match &self.model_gateway {
            Some(model_gateway) => match model_gateway
                .diagnose(
                    auth,
                    incident_id,
                    incident.incident.cluster_id,
                    &incident.incident.title,
                    &report.pack_id,
                    &rule_result,
                    &evidence.items,
                    correlation_id,
                )
                .await
            {
                Ok(decision) => decision,
                Err(error) => {
                    tracing::warn!(
                        code = control_plane_error_code(&error),
                        "model-assisted diagnosis was unavailable; retaining deterministic result"
                    );
                    ModelDiagnosisDecision::rules_only()
                }
            },
            None => ModelDiagnosisDecision::rules_only(),
        };
        if let Some(conclusion) = &model_decision.conclusion
            && let Some(object) = rule_result.as_object_mut()
        {
            object.insert("model_assessment".to_owned(), conclusion.clone());
            object.insert(
                "diagnosis_mode".to_owned(),
                Value::String(model_decision.mode.to_owned()),
            );
        }
        let revision = self
            .workflow
            .persist_diagnosis_revision(
                auth,
                incident_id,
                next_status,
                rule_result,
                hypotheses,
                evidence_ids,
                partial,
                model_decision.invocation_id,
                model_decision.mode,
                correlation_id,
            )
            .await?;

        Ok(DiagnosisResponse {
            schema_version: "rocketmq-sre.diagnosis.v1",
            incident_id,
            revision,
            pack_id: report.pack_id,
            mode: model_decision.mode,
            reason: model_decision.reason,
            budget: BudgetUsage::with_model_usage(
                self.limits,
                tool_calls_used,
                query_retries_used,
                model_decision.input_tokens,
                model_decision.output_tokens,
                model_decision.schema_repairs_used,
            ),
            execution_eligible: false,
        })
    }

    async fn collect_evidence(
        &self,
        auth: &AuthContext,
        incident: &IncidentView,
        pack_id: &str,
        correlation_id: CorrelationId,
        correlation: CorrelationContext,
    ) -> Result<(crate::evidence::EvidencePage, u8, u8), ControlPlaneError> {
        let query = EvidenceListQuery {
            cluster_id: incident.incident.cluster_id,
            incident_id: Some(incident.incident.id),
            source: None,
            limit: Some(self.limits.max_evidence_items),
            cursor: None,
        };
        let started = Instant::now();
        let result = tokio::time::timeout(self.limits.evidence_timeout, self.evidence.list(auth, &query))
            .instrument(
                self.observability
                    .evidence_collect_span(correlation, EvidenceSourceLabel::Other),
            )
            .await
            .map_err(|_| {
                ControlPlaneError::validation(
                    "source_unavailable",
                    "evidence collection exceeded the diagnosis deadline",
                )
            })
            .and_then(|result| result);
        self.observability
            .record_evidence_query(EvidenceSourceLabel::Other, result_class(&result), started.elapsed());
        let mut evidence = result?;
        let Some(connector_channel) = &self.connector_channel else {
            return Ok((evidence, 1, 0));
        };
        let registry = full_registry().map_err(|error| {
            ControlPlaneError::configuration(format!("built-in diagnostic registry is invalid: {error}"))
        })?;
        let pack = registry.resolve(pack_id).ok_or_else(|| {
            ControlPlaneError::configuration(format!("diagnostic pack `{pack_id}` is not registered"))
        })?;
        let requirements = pack
            .required_evidence()
            .iter()
            .chain(pack.optional_evidence())
            .copied()
            .filter(|requirement| {
                !has_complete_evidence(&evidence.items, requirement, pack.max_evidence_freshness_seconds())
            })
            .take(usize::from(self.limits.max_tool_calls.saturating_sub(1)))
            .collect::<Vec<_>>();
        let mut tool_calls_used = 1_u8;
        let mut query_retries_used = 0_u8;
        let mut source_partial = false;
        for requirement in requirements {
            let now = chrono::Utc::now();
            let query_deadline = now
                + chrono::Duration::from_std(per_query_timeout(
                    self.limits.evidence_timeout,
                    self.limits.max_tool_calls,
                ))
                .unwrap_or_else(|_| chrono::Duration::seconds(1));
            let time_range = TimeRange::new(incident.incident.created_at.max(now - chrono::Duration::hours(1)), now)
                .map_err(|_| {
                    ControlPlaneError::validation("invalid_request", "incident evidence time range is invalid")
                })?;
            let connector_query = EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id,
                tenant_id: auth.tenant_id,
                cluster_id: incident.incident.cluster_id,
                source: requirement.source.to_owned(),
                resource: requirement_resource(requirement, incident),
                time_range,
            };
            let response = loop {
                if tool_calls_used >= self.limits.max_tool_calls {
                    source_partial = true;
                    break None;
                }
                tool_calls_used = tool_calls_used.saturating_add(1);
                match connector_channel
                    .query_and_wait(
                        auth.tenant_id,
                        incident.incident.cluster_id,
                        connector_query.clone(),
                        query_deadline,
                    )
                    .await
                {
                    Err(error)
                        if query_retries_used < self.limits.max_query_retries
                            && retryable_evidence_query(&error)
                            && tool_calls_used < self.limits.max_tool_calls =>
                    {
                        query_retries_used = query_retries_used.saturating_add(1);
                        continue;
                    }
                    result => break Some(result),
                }
            };
            match response {
                Some(Ok(response)) => {
                    if let Some(snapshot) = response.evidence {
                        match self
                            .evidence
                            .persist(
                                auth,
                                PersistEvidenceRequest {
                                    investigation_id: incident.investigation.as_ref().map(|value| value.id),
                                    incident_id: Some(incident.incident.id),
                                    evidence: snapshot,
                                },
                            )
                            .await
                        {
                            Ok(snapshot) => evidence.items.push(snapshot),
                            Err(error) => {
                                source_partial = true;
                                tracing::warn!(
                                    code = control_plane_error_code(&error),
                                    source = requirement.source,
                                    "connector evidence could not be persisted"
                                );
                            }
                        }
                    } else {
                        source_partial = true;
                    }
                }
                Some(Err(error)) => {
                    source_partial = true;
                    tracing::warn!(
                        code = control_plane_error_code(&error),
                        source = requirement.source,
                        "connector evidence query was unavailable"
                    );
                }
                None => {}
            }
        }
        evidence.partial |= source_partial;
        Ok((evidence, tool_calls_used, query_retries_used))
    }

    fn evaluate(
        &self,
        pack_id: &str,
        evidence: &[rocketmq_sre_contracts::EvidenceSnapshot],
        correlation: CorrelationContext,
    ) -> Result<DiagnosticReport, ControlPlaneError> {
        let label = DiagnosticPackLabel::from_pack_id(pack_id);
        let span = self.observability.diagnostic_evaluate_span(correlation, label);
        let _guard = span.enter();
        let started = Instant::now();
        let result = self.diagnostics.evaluate(pack_id, evidence).map_err(|error| {
            ControlPlaneError::validation(
                "diagnostic_evaluation_failed",
                format!("deterministic diagnostic evaluation failed: {error}"),
            )
        });
        self.observability
            .record_diagnostic(label, result_class(&result), started.elapsed());
        result
    }
}

fn result_class<T>(result: &Result<T, ControlPlaneError>) -> ResultClass {
    match result {
        Ok(_) => ResultClass::Success,
        Err(ControlPlaneError::Unauthorized | ControlPlaneError::Forbidden { .. }) => ResultClass::Unauthorized,
        Err(
            ControlPlaneError::Database(_)
            | ControlPlaneError::IdentityProvider(_)
            | ControlPlaneError::ObjectStore
            | ControlPlaneError::Io(_),
        ) => ResultClass::Unavailable,
        Err(_) => ResultClass::OtherError,
    }
}

fn retryable_evidence_query(error: &ControlPlaneError) -> bool {
    matches!(
        error,
        ControlPlaneError::Database(_)
            | ControlPlaneError::IdentityProvider(_)
            | ControlPlaneError::ObjectStore
            | ControlPlaneError::Io(_)
            | ControlPlaneError::NotFound
    ) || matches!(
        error,
        ControlPlaneError::Validation {
            code: "source_unavailable",
            ..
        }
    )
}

fn select_pack(incident: &IncidentView) -> &'static str {
    let symptom = incident
        .investigation
        .as_ref()
        .map(|value| value.symptom_family.as_str())
        .unwrap_or_else(|| incident.incident.title.as_str())
        .to_ascii_lowercase()
        .replace(['_', ' '], "-");

    if symptom.contains("retry") || symptom.contains("dlq") {
        "retry-dlq.v1"
    } else if symptom.contains("transaction") || symptom.contains("half-message") {
        "transaction-message.v1"
    } else if symptom.contains("pop") || symptom.contains("revive") {
        "pop-revive.v1"
    } else if symptom.contains("timer") {
        "timer-backlog.v1"
    } else if symptom.contains("queue-hotspot") || symptom.contains("skew") {
        "queue-hotspot.v1"
    } else if symptom.contains("consumer-lag") || symptom.contains("lag") {
        "consumer-lag.v2"
    } else if symptom.contains("consumer-runtime") || symptom.contains("rebalance") {
        "consumer-runtime.v1"
    } else if symptom.contains("send-latency") || symptom.contains("latency") {
        "send-latency.v1"
    } else if symptom.contains("producer") || symptom.contains("send") {
        "producer-connectivity.v1"
    } else if symptom.contains("rocksdb") {
        "rocksdb-health.v1"
    } else if symptom.contains("cold-data") {
        "cold-data-flow.v1"
    } else if symptom.contains("tiered") {
        "tiered-store.v1"
    } else if symptom.contains("store-integrity") || symptom.contains("recovery") {
        "store-integrity.v1"
    } else if symptom.contains("store-pressure") || symptom.contains("disk") {
        "store-pressure.v1"
    } else if symptom.contains("broker-ha") || symptom.contains("replica") {
        "broker-ha.v1"
    } else if symptom.contains("controller") || symptom.contains("quorum") {
        "controller-ha.v1"
    } else if symptom.contains("static-topic") || symptom.contains("mapping-epoch") {
        "static-topic-route.v1"
    } else if symptom.contains("subscription-config") || symptom.contains("filter-drift") {
        "topic-subscription-config.v1"
    } else if symptom.contains("namesrv") || symptom.contains("route") {
        "namesrv-route.v1"
    } else if symptom.contains("proxy") || symptom.contains("grpc") {
        "proxy-connectivity.v1"
    } else if symptom.contains("auth") || symptom.contains("certificate") {
        "auth-failure.v1"
    } else if symptom.contains("runtime") || symptom.contains("blocking") {
        "runtime-saturation.v1"
    } else if symptom.contains("upgrade") {
        "upgrade-readiness.v1"
    } else if symptom.contains("capacity") || symptom.contains("runway") {
        "capacity-runway.v1"
    } else if symptom.contains("dr-readiness") || symptom.contains("disaster-recovery") {
        "dr-readiness.v1"
    } else if symptom.contains("security-posture") {
        "security-posture.v1"
    } else if symptom.contains("change-regression") {
        "change-regression.v1"
    } else if symptom.contains("broker") || symptom.contains("store") {
        "broker-health.v1"
    } else if symptom.contains("message-path") || symptom.contains("journey") {
        "message-path.v1"
    } else if symptom.contains("telemetry") || symptom.contains("metric") {
        "telemetry-pipeline.v1"
    } else if symptom.contains("deployment") || symptom.contains("drift") {
        "deployment-drift.v1"
    } else {
        "cluster-topology.v1"
    }
}

fn has_complete_evidence(
    evidence: &[rocketmq_sre_contracts::EvidenceSnapshot],
    requirement: &EvidenceRequirement,
    max_freshness_seconds: u64,
) -> bool {
    evidence.iter().any(|snapshot| {
        snapshot.source == requirement.source
            && snapshot.resource.starts_with(requirement.resource_prefix)
            && snapshot.freshness_seconds <= max_freshness_seconds
            && !snapshot.partial
            && snapshot.coverage == CoverageStatus::Available
            && matches!(snapshot.content, EvidenceContent::Inline(_))
    })
}

fn requirement_resource(requirement: EvidenceRequirement, incident: &IncidentView) -> String {
    let context = incident
        .investigation
        .as_ref()
        .and_then(|value| value.resource.as_deref())
        .or(incident.incident.resource.as_deref())
        .unwrap_or("cluster");
    if context.starts_with(requirement.resource_prefix) {
        context.to_owned()
    } else {
        let context = context.split_once(':').map_or(context, |(kind, value)| {
            if kind.bytes().all(|byte| byte.is_ascii_lowercase() || byte == b'_') {
                value
            } else {
                context
            }
        });
        format!("{}{}", requirement.resource_prefix, context.trim_matches('/'))
    }
}

fn per_query_timeout(total: std::time::Duration, max_tool_calls: u8) -> std::time::Duration {
    let query_slots = u32::from(max_tool_calls.saturating_sub(1).max(1));
    (total / query_slots).max(std::time::Duration::from_secs(1))
}

fn control_plane_error_code(error: &ControlPlaneError) -> &'static str {
    match error {
        ControlPlaneError::Validation { code, .. } | ControlPlaneError::Forbidden { code, .. } => code,
        ControlPlaneError::Unauthorized => "unauthorized_scope",
        ControlPlaneError::Conflict { .. } => "capability_mismatch",
        ControlPlaneError::Configuration { .. }
        | ControlPlaneError::NotFound
        | ControlPlaneError::Database(_)
        | ControlPlaneError::IdentityProvider(_)
        | ControlPlaneError::Executor(_)
        | ControlPlaneError::ObjectStore
        | ControlPlaneError::CapabilityDocument { .. }
        | ControlPlaneError::Io(_) => "source_unavailable",
    }
}

fn terminal_status(report: &DiagnosticReport) -> IncidentStatus {
    if !report.missing_required_evidence.is_empty()
        || matches!(
            report.status,
            DiagnosticStatus::Inconclusive | DiagnosticStatus::Unsupported
        )
    {
        IncidentStatus::NeedsEvidence
    } else {
        IncidentStatus::Monitoring
    }
}

fn report_json(report: &DiagnosticReport) -> Value {
    json!({
        "schema": {
            "family": report.output_schema.family,
            "major": report.output_schema.major,
            "minor": report.output_schema.minor,
            "required_features": report.output_schema.required_features,
        },
        "pack_id": report.pack_id,
        "pack_version": report.pack_version.to_string(),
        "status": diagnostic_status_name(report.status),
        "findings": report.findings.iter().map(finding_json).collect::<Vec<_>>(),
        "missing_required_evidence": report.missing_required_evidence,
        "missing_optional_evidence": report.missing_optional_evidence,
        "follow_up_queries": report.follow_up_queries.iter().map(|query| json!({
            "source": query.source,
            "resource_template": query.resource_template,
            "reason": query.reason,
        })).collect::<Vec<_>>(),
        "mode": "rules_only",
        "reason": RULES_ONLY_REASON,
        "execution_eligible": false,
    })
}

fn hypotheses_json(report: &DiagnosticReport) -> Value {
    Value::Array(
        report
            .findings
            .iter()
            .enumerate()
            .map(|(index, finding)| {
                let mut evidence = finding.supporting_evidence.clone();
                evidence.extend(finding.counter_evidence.clone());
                json!({
                    "id": format!("{}-{}", finding.reason_code, index + 1),
                    "statement": finding.root_cause,
                    "status": match finding.outcome {
                        FindingOutcome::Fault => HypothesisStatus::Supported,
                        FindingOutcome::Healthy => HypothesisStatus::Rejected,
                        FindingOutcome::Inconclusive => HypothesisStatus::Inconclusive,
                    },
                    "evidence": evidence,
                    "missing_evidence": finding.missing_evidence,
                    "confidence": {
                        "percent": finding.confidence.percent,
                        "band": confidence_band_name(finding.confidence.band),
                        "explanation": finding.confidence.explanation,
                    },
                    "read_only_recommendation": "Review the cited evidence and follow the validated runbook; no cluster change was generated.",
                    "execution_eligible": false,
                })
            })
            .collect(),
    )
}

fn finding_json(finding: &rocketmq_sre_core::diagnostics::DiagnosticFinding) -> Value {
    json!({
        "reason_code": finding.reason_code,
        "root_cause": finding.root_cause,
        "severity": severity_name(finding.severity),
        "outcome": finding_outcome_name(finding.outcome),
        "confidence": {
            "percent": finding.confidence.percent,
            "band": confidence_band_name(finding.confidence.band),
            "explanation": finding.confidence.explanation,
        },
        "supporting_evidence": finding.supporting_evidence.iter().map(|citation| json!({
            "evidence_id": citation.evidence_id,
            "relation": evidence_relation_name(citation.relation),
            "rationale": citation.rationale,
            "confidence_percent": citation.confidence_percent,
        })).collect::<Vec<_>>(),
        "counter_evidence": finding.counter_evidence.iter().map(|citation| json!({
            "evidence_id": citation.evidence_id,
            "relation": evidence_relation_name(citation.relation),
            "rationale": citation.rationale,
            "confidence_percent": citation.confidence_percent,
        })).collect::<Vec<_>>(),
        "missing_evidence": finding.missing_evidence,
    })
}

const fn diagnostic_status_name(status: DiagnosticStatus) -> &'static str {
    match status {
        DiagnosticStatus::Healthy => "healthy",
        DiagnosticStatus::Fault => "fault",
        DiagnosticStatus::Inconclusive => "inconclusive",
        DiagnosticStatus::Unsupported => "unsupported",
    }
}

fn successful_incident_outcome(mode: &str) -> IncidentOutcome {
    if mode == "rules_only" {
        IncidentOutcome::RulesOnly
    } else {
        IncidentOutcome::Completed
    }
}

const fn finding_outcome_name(outcome: FindingOutcome) -> &'static str {
    match outcome {
        FindingOutcome::Healthy => "healthy",
        FindingOutcome::Fault => "fault",
        FindingOutcome::Inconclusive => "inconclusive",
    }
}

const fn severity_name(severity: Severity) -> &'static str {
    match severity {
        Severity::Info => "info",
        Severity::Warning => "warning",
        Severity::Critical => "critical",
    }
}

const fn confidence_band_name(band: ConfidenceBand) -> &'static str {
    match band {
        ConfidenceBand::Low => "low",
        ConfidenceBand::Medium => "medium",
        ConfidenceBand::High => "high",
    }
}

const fn evidence_relation_name(relation: EvidenceRelation) -> &'static str {
    match relation {
        EvidenceRelation::Supports => "supports",
        EvidenceRelation::Contradicts => "contradicts",
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::EvidenceSnapshot;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::current_evidence_schema;
    use serde_json::json;

    use super::*;

    fn evidence_for_requirement(
        requirement: EvidenceRequirement,
        freshness_seconds: u64,
        partial: bool,
    ) -> EvidenceSnapshot {
        let at = chrono::Utc::now();
        let mut snapshot = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: TenantId::new(),
                cluster_id: ClusterId::new(),
                source: requirement.source.to_owned(),
                resource: format!("{}group/topic", requirement.resource_prefix),
                time_range: TimeRange::new(at, at).expect("time range"),
            },
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(json!({"total_lag": 10})),
        )
        .expect("evidence");
        snapshot.freshness_seconds = freshness_seconds;
        snapshot.partial = partial;
        snapshot.coverage = if partial {
            CoverageStatus::Partial
        } else {
            CoverageStatus::Available
        };
        snapshot
    }

    #[test]
    fn pack_selection_is_deterministic_and_read_only() {
        for (symptom, expected) in [
            ("consumer lag is rising", "consumer-lag.v2"),
            ("rocksdb read amplification", "rocksdb-health.v1"),
            ("controller quorum lost", "controller-ha.v1"),
            ("POP revive delay", "pop-revive.v1"),
            ("static topic mapping epoch", "static-topic-route.v1"),
            ("capacity runway", "capacity-runway.v1"),
            ("change regression", "change-regression.v1"),
        ] {
            let pack = select_pack(&IncidentView {
                incident: rocketmq_sre_contracts::Incident::new(
                    rocketmq_sre_contracts::TenantId::new(),
                    rocketmq_sre_contracts::ClusterId::new(),
                    symptom,
                    chrono::Utc::now(),
                ),
                investigation: None,
                timeline: Vec::new(),
                diagnosis_revisions: Vec::new(),
            });
            assert_eq!(pack, expected, "{symptom}");
        }
    }

    #[test]
    fn alert_resource_is_normalized_for_evidence_queries() {
        let mut incident = rocketmq_sre_contracts::Incident::new(
            rocketmq_sre_contracts::TenantId::new(),
            rocketmq_sre_contracts::ClusterId::new(),
            "consumer lag is rising",
            chrono::Utc::now(),
        );
        incident.resource = Some("consumer_group:group-a/topic-a".to_owned());
        let requirement = full_registry()
            .expect("built-in registry")
            .resolve("consumer-lag.v2")
            .expect("consumer lag pack")
            .required_evidence()[0];
        let view = IncidentView {
            incident,
            investigation: None,
            timeline: Vec::new(),
            diagnosis_revisions: Vec::new(),
        };

        assert_eq!(requirement_resource(requirement, &view), "consumer-lag/group-a/topic-a");
    }

    #[test]
    fn partial_or_stale_evidence_is_refreshed_before_rediagnosis() {
        let requirement = full_registry()
            .expect("built-in registry")
            .resolve("consumer-lag.v2")
            .expect("consumer lag pack")
            .required_evidence()[0];
        let partial = evidence_for_requirement(requirement, 0, true);
        let stale = evidence_for_requirement(requirement, 301, false);
        let complete = evidence_for_requirement(requirement, 0, false);

        assert!(!has_complete_evidence(&[partial], &requirement, 300));
        assert!(!has_complete_evidence(&[stale], &requirement, 300));
        assert!(has_complete_evidence(&[complete], &requirement, 300));
    }

    #[test]
    fn rules_only_budget_never_allocates_model_tokens() {
        let usage = BudgetUsage::rules_only(OrchestratorLimits::default(), 1, 0);
        assert_eq!(usage.tool_calls_used, 1);
        assert_eq!(usage.model_input_tokens, 0);
        assert_eq!(usage.model_output_tokens, 0);
    }

    #[test]
    fn model_budget_reports_the_single_schema_repair() {
        let usage = BudgetUsage::with_model_usage(OrchestratorLimits::default(), 1, 0, 14, 5, 1);

        assert_eq!(usage.schema_repairs_used, 1);
        assert_eq!(usage.model_input_tokens, 14);
        assert_eq!(usage.model_output_tokens, 5);
    }

    #[test]
    fn snake_case_rules_only_mode_is_recorded_as_rules_only() {
        assert_eq!(successful_incident_outcome("rules_only"), IncidentOutcome::RulesOnly);
        assert_eq!(
            successful_incident_outcome("model_assisted"),
            IncidentOutcome::Completed
        );
    }
}
