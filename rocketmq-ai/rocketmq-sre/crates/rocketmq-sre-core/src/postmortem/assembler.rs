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

use rocketmq_sre_contracts::DiagnosisRevision;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::Incident;
use rocketmq_sre_contracts::IncidentStatus;
use rocketmq_sre_contracts::PostmortemConclusion;
use rocketmq_sre_contracts::TimelineEvent;

const MAX_TIMELINE_EVENTS: usize = 256;
const MAX_NOTES: usize = 32;

/// Deterministic source material for a postmortem draft.
#[derive(Clone, Copy)]
pub struct PostmortemAssemblyInput<'a> {
    pub incident: &'a Incident,
    pub evidence: &'a [EvidenceSnapshot],
    pub diagnosis_revisions: &'a [DiagnosisRevision],
    pub timeline: &'a [TimelineEvent],
    pub operator_notes: &'a [String],
}

/// Suggested metadata-only follow-up. It never contains an executable command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostmortemActionProposal {
    pub title: String,
    pub evidence_ids: Vec<EvidenceId>,
}

/// Provider-neutral postmortem content before persistence identity is added.
#[derive(Clone, Debug, PartialEq)]
pub struct PostmortemAssembly {
    pub summary: String,
    pub impact: String,
    pub detection: String,
    pub timeline: Vec<TimelineEvent>,
    pub root_causes: Vec<PostmortemConclusion>,
    pub contributing_factors: Vec<PostmortemConclusion>,
    pub conclusions: Vec<PostmortemConclusion>,
    pub recovery: String,
    pub effective_actions: Vec<String>,
    pub ineffective_actions: Vec<String>,
    pub evidence_ids: Vec<EvidenceId>,
    pub action_items: Vec<PostmortemActionProposal>,
}

/// Builds a bounded deterministic draft from durable Incident state.
#[must_use]
pub fn assemble(input: PostmortemAssemblyInput<'_>) -> PostmortemAssembly {
    let allowed = input
        .evidence
        .iter()
        .map(|snapshot| snapshot.evidence_id)
        .collect::<BTreeSet<_>>();
    let latest = input.diagnosis_revisions.last();
    let mut root_causes = latest
        .map(|revision| conclusions_from_findings(revision, &allowed))
        .unwrap_or_default();
    if root_causes.is_empty() {
        root_causes = latest
            .map(|revision| conclusions_from_hypotheses(revision, &allowed))
            .unwrap_or_default();
    }
    let evidence_ids = root_causes
        .iter()
        .flat_map(|conclusion| conclusion.evidence_ids.iter().copied())
        .chain(
            input
                .diagnosis_revisions
                .iter()
                .flat_map(|revision| revision.evidence_ids.iter().copied())
                .filter(|id| allowed.contains(id)),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let conclusions = (!evidence_ids.is_empty())
        .then(|| PostmortemConclusion {
            code: "incident_scope_confirmed".to_owned(),
            statement: format!(
                "Incident affected {} in cluster {}",
                input.incident.resource.as_deref().unwrap_or("an unclassified resource"),
                input.incident.cluster_id
            ),
            evidence_ids: evidence_ids.clone(),
        })
        .into_iter()
        .collect();
    let effective_actions = input
        .operator_notes
        .iter()
        .take(MAX_NOTES)
        .filter_map(|note| {
            let trimmed = note.trim();
            (!trimmed.is_empty()).then(|| trimmed.chars().take(1_024).collect())
        })
        .collect::<Vec<_>>();
    let action_items = missing_evidence(latest)
        .into_iter()
        .take(16)
        .map(|missing| PostmortemActionProposal {
            title: format!("补齐诊断证据：{missing}"),
            evidence_ids: Vec::new(),
        })
        .collect();

    PostmortemAssembly {
        summary: format!(
            "{}；当前状态为 {:?}，合并告警 {} 次。",
            input.incident.title, input.incident.status, input.incident.occurrence_count
        ),
        impact: format!(
            "影响资源：{}；严重度：{}。",
            input.incident.resource.as_deref().unwrap_or("未分类"),
            input.incident.severity.map_or_else(
                || "未分类".to_owned(),
                |severity| format!("{severity:?}").to_lowercase()
            )
        ),
        detection: format!(
            "检测症状：{}；首次 Incident 时间：{}。",
            input.incident.symptom_family.as_deref().unwrap_or("未分类"),
            input.incident.created_at
        ),
        timeline: input
            .timeline
            .iter()
            .rev()
            .take(MAX_TIMELINE_EVENTS)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect(),
        root_causes,
        contributing_factors: Vec::new(),
        conclusions,
        recovery: if input.incident.status == IncidentStatus::Resolved {
            "Incident 已进入 resolved；恢复步骤仍需操作员确认。".to_owned()
        } else {
            "恢复尚未由操作员确认。".to_owned()
        },
        effective_actions,
        ineffective_actions: Vec::new(),
        evidence_ids,
        action_items,
    }
}

fn conclusions_from_findings(
    revision: &DiagnosisRevision,
    allowed: &BTreeSet<EvidenceId>,
) -> Vec<PostmortemConclusion> {
    revision
        .rule_result
        .get("findings")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|finding| {
            let statement = finding.get("root_cause")?.as_str()?.trim();
            let code = finding
                .get("reason_code")
                .and_then(|value| value.as_str())
                .unwrap_or("diagnostic_finding")
                .trim();
            let evidence_ids = finding
                .get("supporting_evidence")
                .and_then(|value| value.as_array())
                .into_iter()
                .flatten()
                .filter_map(|value| {
                    value
                        .get("evidence_id")
                        .and_then(|value| value.as_str())
                        .or_else(|| value.as_str())
                        .and_then(|value| value.parse().ok())
                })
                .filter(|id| allowed.contains(id))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            (!statement.is_empty() && !evidence_ids.is_empty()).then(|| PostmortemConclusion {
                code: bounded(code, 128),
                statement: bounded(statement, 2_000),
                evidence_ids,
            })
        })
        .take(16)
        .collect()
}

fn conclusions_from_hypotheses(
    revision: &DiagnosisRevision,
    allowed: &BTreeSet<EvidenceId>,
) -> Vec<PostmortemConclusion> {
    revision
        .hypotheses
        .as_array()
        .into_iter()
        .flatten()
        .filter(|hypothesis| {
            hypothesis
                .get("status")
                .and_then(|value| value.as_str())
                .is_some_and(|status| status == "supported")
        })
        .filter_map(|hypothesis| {
            let statement = hypothesis.get("statement")?.as_str()?.trim();
            let code = hypothesis
                .get("id")
                .and_then(|value| value.as_str())
                .unwrap_or("supported_hypothesis");
            let evidence_ids = hypothesis
                .get("evidence")
                .and_then(|value| value.as_array())
                .into_iter()
                .flatten()
                .filter_map(|value| {
                    value
                        .get("evidence_id")
                        .and_then(|value| value.as_str())
                        .or_else(|| value.as_str())
                        .and_then(|value| value.parse().ok())
                })
                .filter(|id| allowed.contains(id))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            (!statement.is_empty() && !evidence_ids.is_empty()).then(|| PostmortemConclusion {
                code: bounded(code, 128),
                statement: bounded(statement, 2_000),
                evidence_ids,
            })
        })
        .take(16)
        .collect()
}

fn missing_evidence(revision: Option<&DiagnosisRevision>) -> Vec<String> {
    let Some(result) = revision.map(|revision| &revision.rule_result) else {
        return Vec::new();
    };
    ["missing_required_evidence", "missing_optional_evidence"]
        .into_iter()
        .flat_map(|field| {
            result
                .get(field)
                .and_then(|value| value.as_array())
                .into_iter()
                .flatten()
                .filter_map(|value| value.as_str())
                .map(|value| bounded(value, 256))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn bounded(value: &str, max_chars: usize) -> String {
    value.trim().chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::DiagnosisRevisionId;
    use rocketmq_sre_contracts::EvidenceContent;
    use rocketmq_sre_contracts::EvidenceExposure;
    use rocketmq_sre_contracts::IncidentId;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_contracts::Sensitivity;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::TimeRange;

    use super::*;

    #[test]
    fn assembler_keeps_only_real_evidence_citations() {
        let at = Utc.with_ymd_and_hms(2026, 7, 27, 1, 0, 0).single().expect("timestamp");
        let valid = EvidenceId::new();
        let false_id = EvidenceId::new();
        let incident = Incident::new(TenantId::new(), ClusterId::new(), "lag", at);
        let evidence = EvidenceSnapshot {
            schema: SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
            evidence_id: valid,
            query_id: QueryId::new(),
            correlation_id: rocketmq_sre_contracts::CorrelationId::new(),
            tenant_id: incident.tenant_id,
            cluster_id: incident.cluster_id,
            source: "prometheus".to_owned(),
            resource: "consumer:orders".to_owned(),
            time_range: TimeRange { start: at, end: at },
            observed_at: at,
            freshness_seconds: 0,
            partial: false,
            warnings: Vec::new(),
            sensitivity: Sensitivity::Internal,
            coverage: rocketmq_sre_contracts::CoverageStatus::Available,
            exposure: EvidenceExposure::default(),
            content: EvidenceContent::Inline(serde_json::json!({"lag": 42})),
            content_hash: "sha256:fixture".to_owned(),
        };
        let revision = DiagnosisRevision {
            id: DiagnosisRevisionId::new(),
            incident_id: IncidentId::new(),
            revision: 1,
            status: IncidentStatus::Diagnosing,
            rule_result: serde_json::json!({
                "findings": [{
                    "reason_code": "consumer_slow",
                    "root_cause": "consumer throughput fell",
                    "supporting_evidence": [
                        {"evidence_id": valid},
                        {"evidence_id": false_id}
                    ]
                }]
            }),
            hypotheses: serde_json::json!([]),
            evidence_ids: vec![valid, false_id],
            primary_model_invocation_id: None,
            execution_eligible: false,
            partial: false,
            created_at: at,
        };

        let result = assemble(PostmortemAssemblyInput {
            incident: &incident,
            evidence: &[evidence],
            diagnosis_revisions: &[revision],
            timeline: &[],
            operator_notes: &[],
        });

        assert_eq!(result.root_causes[0].evidence_ids, vec![valid]);
        assert_eq!(result.evidence_ids, vec![valid]);
    }
}
