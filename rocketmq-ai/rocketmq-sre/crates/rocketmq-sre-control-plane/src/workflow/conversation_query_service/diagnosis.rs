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

use rocketmq_sre_contracts::ConversationQueryIntent;
use rocketmq_sre_contracts::DiagnosticEvidence;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::InvestigationDiagnosisStatus;
use rocketmq_sre_contracts::InvestigationId;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_core::diagnostics::ConfidenceBand;
use rocketmq_sre_core::diagnostics::DiagnosticFinding;
use rocketmq_sre_core::diagnostics::DiagnosticReport;
use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use rocketmq_sre_core::diagnostics::FindingOutcome;
use rocketmq_sre_core::diagnostics::Severity;
use serde_json::Value;
use serde_json::json;

use super::evidence_answer;
use crate::workflow::conversation_repository::InvestigationDiagnosisDraft;

pub(super) fn diagnosis_is_partial(report: &DiagnosticReport, evidence_partial: bool) -> bool {
    evidence_partial
        || matches!(
            report.status,
            DiagnosticStatus::Inconclusive | DiagnosticStatus::Unsupported
        )
        || !report.missing_required_evidence.is_empty()
}

pub(super) fn investigation_diagnosis_draft(
    investigation_id: InvestigationId,
    report: &DiagnosticReport,
    evidence_id: EvidenceId,
    primary_model_invocation_id: Option<ModelInvocationId>,
    partial: bool,
) -> InvestigationDiagnosisDraft {
    let findings = report.findings.iter().map(finding_projection).collect::<Vec<_>>();
    let hypotheses = report
        .findings
        .iter()
        .map(|finding| {
            json!({
                "reason_code": finding.reason_code,
                "root_cause": finding.root_cause,
                "confidence_percent": finding.confidence.percent,
                "confidence_band": confidence_band_name(finding.confidence.band),
                "supporting_evidence": evidence_projection(&finding.supporting_evidence),
                "counter_evidence": evidence_projection(&finding.counter_evidence),
                "missing_evidence": finding.missing_evidence,
            })
        })
        .collect::<Vec<_>>();
    InvestigationDiagnosisDraft {
        investigation_id,
        pack_id: report.pack_id.clone(),
        pack_version: report.pack_version.to_string(),
        status: investigation_status(report.status),
        rule_result: json!({
            "schema_version": report.output_schema,
            "status": diagnostic_status_name(report.status),
            "findings": findings,
            "missing_required_evidence": report.missing_required_evidence,
            "missing_optional_evidence": report.missing_optional_evidence,
            "follow_up_queries": report.follow_up_queries.iter().map(|query| json!({
                "source": query.source,
                "resource_template": query.resource_template,
                "reason": query.reason,
            })).collect::<Vec<_>>(),
        }),
        hypotheses: Value::Array(hypotheses),
        evidence_ids: vec![evidence_id],
        primary_model_invocation_id,
        partial,
    }
}

fn finding_projection(finding: &DiagnosticFinding) -> Value {
    json!({
        "reason_code": finding.reason_code,
        "root_cause": finding.root_cause,
        "severity": severity_name(finding.severity),
        "outcome": finding_outcome_name(finding.outcome),
        "confidence_percent": finding.confidence.percent,
        "confidence_band": confidence_band_name(finding.confidence.band),
        "confidence_explanation": finding.confidence.explanation,
        "supporting_evidence": evidence_projection(&finding.supporting_evidence),
        "counter_evidence": evidence_projection(&finding.counter_evidence),
        "missing_evidence": finding.missing_evidence,
    })
}

fn evidence_projection(items: &[DiagnosticEvidence]) -> Vec<Value> {
    items
        .iter()
        .map(|item| {
            json!({
                "evidence_id": item.evidence_id,
                "relation": item.relation,
                "rationale": item.rationale,
                "confidence_percent": item.confidence_percent,
            })
        })
        .collect()
}

const fn investigation_status(status: DiagnosticStatus) -> InvestigationDiagnosisStatus {
    match status {
        DiagnosticStatus::Healthy => InvestigationDiagnosisStatus::Healthy,
        DiagnosticStatus::Fault => InvestigationDiagnosisStatus::Fault,
        DiagnosticStatus::Inconclusive => InvestigationDiagnosisStatus::Inconclusive,
        DiagnosticStatus::Unsupported => InvestigationDiagnosisStatus::Unsupported,
    }
}

const fn diagnostic_status_name(status: DiagnosticStatus) -> &'static str {
    match status {
        DiagnosticStatus::Healthy => "healthy",
        DiagnosticStatus::Fault => "fault",
        DiagnosticStatus::Inconclusive => "inconclusive",
        DiagnosticStatus::Unsupported => "unsupported",
    }
}

const fn severity_name(severity: Severity) -> &'static str {
    match severity {
        Severity::Info => "info",
        Severity::Warning => "warning",
        Severity::Critical => "critical",
    }
}

const fn finding_outcome_name(outcome: FindingOutcome) -> &'static str {
    match outcome {
        FindingOutcome::Healthy => "healthy",
        FindingOutcome::Fault => "fault",
        FindingOutcome::Inconclusive => "inconclusive",
    }
}

const fn confidence_band_name(band: ConfidenceBand) -> &'static str {
    match band {
        ConfidenceBand::Low => "low",
        ConfidenceBand::Medium => "medium",
        ConfidenceBand::High => "high",
    }
}

pub(super) fn diagnostic_evidence_answer(
    intent: &ConversationQueryIntent,
    evidence: &EvidenceSnapshot,
    report: &DiagnosticReport,
) -> String {
    let findings = if report.findings.is_empty() {
        "No deterministic finding was emitted.".to_owned()
    } else {
        report
            .findings
            .iter()
            .take(8)
            .map(|finding| {
                format!(
                    "{}: {} ({}%, {})",
                    finding.reason_code,
                    finding.root_cause,
                    finding.confidence.percent,
                    severity_name(finding.severity)
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    format!(
        "Diagnostic pack {}@{} concluded {}. {} Missing required evidence: [{}]. {}",
        report.pack_id,
        report.pack_version,
        diagnostic_status_name(report.status),
        findings,
        report.missing_required_evidence.join(", "),
        evidence_answer(intent, evidence)
    )
}
