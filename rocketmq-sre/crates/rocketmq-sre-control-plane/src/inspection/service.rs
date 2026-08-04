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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::InspectionTemplate;
use rocketmq_sre_core::diagnostics::DiagnosticEngine;
use rocketmq_sre_core::diagnostics::DiagnosticFinding;
use rocketmq_sre_core::diagnostics::DiagnosticReport;
use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use rocketmq_sre_core::diagnostics::FindingOutcome;
use rocketmq_sre_core::diagnostics::full_registry;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

use super::InspectionPackRun;
use super::NewRecommendation;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::evidence::EvidenceListQuery;
use crate::evidence::EvidenceService;
use crate::workflow::InspectionCreateRequest;
use crate::workflow::InspectionView;
use crate::workflow::WorkflowService;

const EVIDENCE_TIMEOUT: Duration = Duration::from_secs(20);
/// Downloadable inspection summary.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct InspectionReport {
    pub(crate) schema_version: &'static str,
    pub(crate) media_type: &'static str,
    pub(crate) file_name: String,
    pub(crate) content: String,
}

/// Executes the four Phase 1 inspection templates using only deterministic
/// diagnostic packs and scoped evidence.
#[derive(Clone)]
pub(crate) struct InspectionService {
    repository: PostgresRepository,
    workflow: WorkflowService,
    evidence: EvidenceService,
    diagnostics: Arc<DiagnosticEngine>,
}

impl InspectionService {
    pub(crate) fn new(
        repository: PostgresRepository,
        workflow: WorkflowService,
        evidence: EvidenceService,
    ) -> Result<Self, ControlPlaneError> {
        let registry = full_registry().map_err(|error| {
            ControlPlaneError::configuration(format!("built-in diagnostic registry is invalid: {error}"))
        })?;
        Ok(Self {
            repository,
            workflow,
            evidence,
            diagnostics: Arc::new(DiagnosticEngine::new(registry)),
        })
    }

    pub(crate) async fn create(
        &self,
        auth: &AuthContext,
        request: &InspectionCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<InspectionView, ControlPlaneError> {
        let view = self.create_persisted(auth, request, correlation_id).await?;
        if request.schedule.is_none() {
            self.execute(auth, view.run.id, correlation_id).await
        } else {
            Ok(view)
        }
    }

    pub(crate) async fn create_persisted(
        &self,
        auth: &AuthContext,
        request: &InspectionCreateRequest,
        correlation_id: CorrelationId,
    ) -> Result<InspectionView, ControlPlaneError> {
        self.workflow.create_inspection(auth, request, correlation_id).await
    }

    #[tracing::instrument(
        name = "sre.inspection.run",
        skip_all,
        fields(inspection_id = %id, correlation_id = %correlation_id, access = "read_only")
    )]
    pub(crate) async fn execute(
        &self,
        auth: &AuthContext,
        id: InspectionRunId,
        correlation_id: CorrelationId,
    ) -> Result<InspectionView, ControlPlaneError> {
        self.workflow.ensure_operator(auth)?;
        let run = self.repository.claim_inspection(auth, id).await?;
        let query = EvidenceListQuery {
            cluster_id: run.cluster_id,
            incident_id: None,
            source: None,
            limit: Some(200),
            cursor: None,
        };
        let page = tokio::time::timeout(EVIDENCE_TIMEOUT, self.evidence.list(auth, &query))
            .await
            .map_err(|_| {
                ControlPlaneError::validation(
                    "source_unavailable",
                    "inspection evidence collection exceeded its deadline",
                )
            })??;

        let mut pack_runs = Vec::new();
        let mut recommendations = Vec::new();
        let mut partial = page.partial;
        if run.template == InspectionTemplate::ClusterHealth
            && let Some(health) = self.repository.latest_health_snapshot(auth, run.cluster_id).await?
        {
            let at = Utc::now();
            let health_partial = health.data_quality != HealthDataQuality::Complete;
            partial |= health_partial;
            if health.status != HealthStatus::Healthy {
                recommendations.push(NewRecommendation {
                    severity: match health.status {
                        HealthStatus::Critical => "critical",
                        HealthStatus::Degraded => "warning",
                        HealthStatus::Unknown => "info",
                        HealthStatus::Healthy => "info",
                    }
                    .to_owned(),
                    title: "Review deterministic cluster health score".to_owned(),
                    rationale: format!(
                        "Cluster health is {:?} with score {}; inspect triggered SLIs and cited evidence before any \
                         operator action",
                        health.status,
                        health
                            .score
                            .map_or_else(|| "unknown".to_owned(), |score| score.to_string())
                    ),
                    evidence_ids: health.evidence_ids.clone(),
                });
            }
            pack_runs.push(InspectionPackRun {
                pack_id: "cluster-health-score.v1".to_owned(),
                pack_version: health.algorithm_version.clone(),
                input_evidence_ids: health.evidence_ids.clone(),
                output: json!({
                    "schema_version": "rocketmq-sre.inspection-health-result.v1",
                    "health": health,
                    "execution_eligible": false,
                }),
                partial: health_partial,
                started_at: at,
                completed_at: at,
            });
        }
        for pack_id in template_packs(run.template) {
            let started_at = Utc::now();
            let report = self.diagnostics.evaluate(pack_id, &page.items).map_err(|error| {
                ControlPlaneError::validation(
                    "diagnostic_evaluation_failed",
                    format!("inspection diagnostic evaluation failed: {error}"),
                )
            })?;
            let completed_at = Utc::now();
            let pack_partial = diagnostic_pack_is_partial(
                page.partial,
                !report.missing_required_evidence.is_empty(),
                report.status,
            );
            partial |= pack_partial;
            recommendations.extend(recommendations_from_report(&report));
            pack_runs.push(InspectionPackRun {
                pack_id: report.pack_id.clone(),
                pack_version: report.pack_version.to_string(),
                input_evidence_ids: page.items.iter().map(|snapshot| snapshot.evidence_id).collect(),
                output: report_json(&report),
                partial: pack_partial,
                started_at,
                completed_at,
            });
        }
        self.repository
            .complete_inspection(auth, id, pack_runs, recommendations, partial, correlation_id)
            .await
    }

    pub(crate) async fn report(
        &self,
        auth: &AuthContext,
        id: InspectionRunId,
        format: &str,
    ) -> Result<InspectionReport, ControlPlaneError> {
        let view = self.workflow.inspection(auth, id).await?;
        match format {
            "markdown" | "md" => Ok(InspectionReport {
                schema_version: "rocketmq-sre.inspection-report.v1",
                media_type: "text/markdown; charset=utf-8",
                file_name: format!("inspection-{id}.md"),
                content: markdown_report(&view),
            }),
            "html" => Ok(InspectionReport {
                schema_version: "rocketmq-sre.inspection-report.v1",
                media_type: "text/html; charset=utf-8",
                file_name: format!("inspection-{id}.html"),
                content: html_report(&view),
            }),
            _ => Err(ControlPlaneError::validation(
                "invalid_request",
                "inspection report format must be markdown or html",
            )),
        }
    }
}

const fn diagnostic_pack_is_partial(
    input_partial: bool,
    missing_required_evidence: bool,
    status: DiagnosticStatus,
) -> bool {
    input_partial
        || missing_required_evidence
        || matches!(status, DiagnosticStatus::Inconclusive | DiagnosticStatus::Unsupported)
}

fn template_packs(template: InspectionTemplate) -> &'static [&'static str] {
    match template {
        InspectionTemplate::ClusterHealth => &[
            "cluster-topology.v1",
            "deployment-drift.v1",
            "namesrv-route.v1",
            "controller-ha.v1",
            "upgrade-readiness.v1",
            "capacity-runway.v1",
            "security-posture.v1",
            "change-regression.v1",
        ],
        InspectionTemplate::Consumer => &[
            "consumer-lag.v2",
            "consumer-runtime.v1",
            "retry-dlq.v1",
            "transaction-message.v1",
            "pop-revive.v1",
            "timer-backlog.v1",
            "queue-hotspot.v1",
            "topic-subscription-config.v1",
        ],
        InspectionTemplate::Broker => &[
            "broker-health.v1",
            "store-pressure.v1",
            "store-integrity.v1",
            "rocksdb-health.v1",
            "tiered-store.v1",
            "broker-ha.v1",
            "static-topic-route.v1",
            "cold-data-flow.v1",
            "dr-readiness.v1",
        ],
        InspectionTemplate::Telemetry => &[
            "telemetry-pipeline.v1",
            "runtime-saturation.v1",
            "send-latency.v1",
            "proxy-connectivity.v1",
            "auth-failure.v1",
        ],
        InspectionTemplate::FullCluster => &[
            "cluster-topology.v1",
            "broker-health.v1",
            "consumer-lag.v2",
            "consumer-runtime.v1",
            "store-pressure.v1",
            "store-integrity.v1",
            "broker-ha.v1",
            "controller-ha.v1",
            "namesrv-route.v1",
            "proxy-connectivity.v1",
            "security-posture.v1",
            "upgrade-readiness.v1",
            "capacity-runway.v1",
            "dr-readiness.v1",
            "telemetry-pipeline.v1",
            "change-regression.v1",
        ],
        InspectionTemplate::ProducerConsumer => &[
            "producer-connectivity.v1",
            "message-path.v1",
            "send-latency.v1",
            "consumer-lag.v2",
            "consumer-runtime.v1",
            "retry-dlq.v1",
            "transaction-message.v1",
            "pop-revive.v1",
            "timer-backlog.v1",
            "queue-hotspot.v1",
            "topic-subscription-config.v1",
        ],
        InspectionTemplate::StoreHa => &[
            "broker-health.v1",
            "store-pressure.v1",
            "store-integrity.v1",
            "rocksdb-health.v1",
            "tiered-store.v1",
            "broker-ha.v1",
            "controller-ha.v1",
            "cold-data-flow.v1",
        ],
        InspectionTemplate::RoutingProxy => &[
            "cluster-topology.v1",
            "namesrv-route.v1",
            "static-topic-route.v1",
            "proxy-connectivity.v1",
            "send-latency.v1",
            "deployment-drift.v1",
        ],
        InspectionTemplate::Security => &[
            "security-posture.v1",
            "auth-failure.v1",
            "proxy-connectivity.v1",
            "runtime-saturation.v1",
        ],
        InspectionTemplate::Upgrade => &[
            "upgrade-readiness.v1",
            "deployment-drift.v1",
            "change-regression.v1",
            "capacity-runway.v1",
            "broker-ha.v1",
            "controller-ha.v1",
            "store-integrity.v1",
        ],
        InspectionTemplate::DisasterRecovery => &[
            "dr-readiness.v1",
            "broker-ha.v1",
            "controller-ha.v1",
            "store-integrity.v1",
            "capacity-runway.v1",
            "cold-data-flow.v1",
            "security-posture.v1",
        ],
    }
}

fn recommendations_from_report(report: &DiagnosticReport) -> Vec<NewRecommendation> {
    report
        .findings
        .iter()
        .filter(|finding| finding.outcome == FindingOutcome::Fault)
        .map(|finding| NewRecommendation {
            severity: severity_name(finding),
            title: finding.root_cause.clone(),
            rationale: format!(
                "{} (reason code {}, confidence {}%)",
                finding.confidence.explanation, finding.reason_code, finding.confidence.percent
            ),
            evidence_ids: cited_ids(finding),
        })
        .collect()
}

fn cited_ids(finding: &DiagnosticFinding) -> Vec<EvidenceId> {
    finding
        .supporting_evidence
        .iter()
        .chain(&finding.counter_evidence)
        .map(|citation| citation.evidence_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn severity_name(finding: &DiagnosticFinding) -> String {
    format!("{:?}", finding.severity).to_ascii_lowercase()
}

fn report_json(report: &DiagnosticReport) -> Value {
    json!({
        "schema_version": "rocketmq-sre.inspection-pack-result.v1",
        "pack_id": report.pack_id,
        "pack_version": report.pack_version.to_string(),
        "status": format!("{:?}", report.status).to_ascii_lowercase(),
        "missing_required_evidence": report.missing_required_evidence,
        "missing_optional_evidence": report.missing_optional_evidence,
        "findings": report.findings.iter().map(|finding| json!({
            "reason_code": finding.reason_code,
            "root_cause": finding.root_cause,
            "severity": severity_name(finding),
            "outcome": format!("{:?}", finding.outcome).to_ascii_lowercase(),
            "confidence_percent": finding.confidence.percent,
            "supporting_evidence": finding.supporting_evidence,
            "counter_evidence": finding.counter_evidence,
            "missing_evidence": finding.missing_evidence,
        })).collect::<Vec<_>>(),
        "execution_eligible": false,
    })
}

fn markdown_report(view: &InspectionView) -> String {
    let mut output = format!(
        "# RocketMQ AI SRE Inspection\n\n- Run: `{}`\n- Template: `{:?}`\n- Status: `{:?}`\n- Partial: `{}`\n- \
         Findings: `{}`\n\n## Recommendations\n",
        view.run.id, view.run.template, view.run.status, view.run.partial, view.run.finding_count
    );
    if view.recommendations.is_empty() {
        output.push_str("\nNo read-only recommendations were generated.\n");
    } else {
        for recommendation in &view.recommendations {
            output.push_str(&format!(
                "\n### {}\n\n- Severity: `{}`\n- Status: `{:?}`\n- Evidence: `{}`\n\n{}\n",
                recommendation.title,
                recommendation.severity,
                recommendation.status,
                recommendation.evidence_ids.len(),
                recommendation.rationale
            ));
        }
    }
    output.push_str("\n## Changes since previous inspection\n");
    if view.pack_diffs.is_empty() {
        output.push_str("\nNo previous comparable pack result is available.\n");
    } else {
        for pack in &view.pack_diffs {
            output.push_str(&format!(
                "\n- `{}`: added `{}`, resolved `{}`, unchanged `{}`\n",
                markdown_code(value_text(pack, "pack_id", "unknown")),
                diff_count(pack, "added_reason_codes"),
                diff_count(pack, "resolved_reason_codes"),
                diff_count(pack, "unchanged_reason_codes"),
            ));
        }
    }
    output
}

fn html_report(view: &InspectionView) -> String {
    let recommendations = view
        .recommendations
        .iter()
        .map(|recommendation| {
            format!(
                "<article><h2>{}</h2><p><strong>Severity:</strong> {}</p><p>{}</p></article>",
                escape_html(&recommendation.title),
                escape_html(&recommendation.severity),
                escape_html(&recommendation.rationale)
            )
        })
        .collect::<String>();
    let diffs = if view.pack_diffs.is_empty() {
        "<p>No previous comparable pack result is available.</p>".to_owned()
    } else {
        view.pack_diffs
            .iter()
            .map(|pack| {
                format!(
                    "<li><code>{}</code>: added {}, resolved {}, unchanged {}</li>",
                    escape_html(value_text(pack, "pack_id", "unknown")),
                    diff_count(pack, "added_reason_codes"),
                    diff_count(pack, "resolved_reason_codes"),
                    diff_count(pack, "unchanged_reason_codes"),
                )
            })
            .collect::<String>()
    };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Inspection \
         {}</title></head><body><main><h1>RocketMQ AI SRE \
         Inspection</h1><dl><dt>Template</dt><dd>{:?}</dd><dt>Status</dt><dd>{:?}</dd><dt>Partial</dt><dd>{}</\
         dd><dt>Findings</dt><dd>{}</dd></dl>{}<h2>Changes since previous \
         inspection</h2><ul>{}</ul></main></body></html>",
        view.run.id,
        view.run.template,
        view.run.status,
        view.run.partial,
        view.run.finding_count,
        recommendations,
        diffs
    )
}

fn value_text<'a>(value: &'a Value, key: &str, fallback: &'a str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or(fallback)
}

fn diff_count(pack: &Value, key: &str) -> usize {
    pack.get("diff")
        .and_then(|diff| diff.get(key))
        .and_then(Value::as_array)
        .map_or(0, Vec::len)
}

fn markdown_code(value: &str) -> String {
    value.replace('`', "'")
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_supported_templates_have_read_only_packs() {
        for template in [
            InspectionTemplate::ClusterHealth,
            InspectionTemplate::Consumer,
            InspectionTemplate::Broker,
            InspectionTemplate::Telemetry,
            InspectionTemplate::FullCluster,
            InspectionTemplate::ProducerConsumer,
            InspectionTemplate::StoreHa,
            InspectionTemplate::RoutingProxy,
            InspectionTemplate::Security,
            InspectionTemplate::Upgrade,
            InspectionTemplate::DisasterRecovery,
        ] {
            assert!(!template_packs(template).is_empty());
        }
    }

    #[test]
    fn operational_templates_cover_the_complete_diagnostic_catalog() {
        let covered = [
            InspectionTemplate::ClusterHealth,
            InspectionTemplate::Consumer,
            InspectionTemplate::Broker,
            InspectionTemplate::Telemetry,
            InspectionTemplate::ProducerConsumer,
        ]
        .into_iter()
        .flat_map(template_packs)
        .copied()
        .collect::<BTreeSet<_>>();
        let expected = rocketmq_sre_core::diagnostics::full_pack_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(
            covered,
            expected.iter().map(String::as_str).collect(),
            "the bounded operational inspection surface must reach every built-in pack"
        );
    }

    #[test]
    fn pack_partial_status_is_scoped_to_the_current_pack() {
        assert!(!diagnostic_pack_is_partial(false, false, DiagnosticStatus::Healthy));
        assert!(!diagnostic_pack_is_partial(false, false, DiagnosticStatus::Fault));
        assert!(diagnostic_pack_is_partial(false, true, DiagnosticStatus::Healthy));
        assert!(diagnostic_pack_is_partial(false, false, DiagnosticStatus::Inconclusive));
    }

    #[test]
    fn html_report_escaping_rejects_markup_in_findings() {
        assert_eq!(
            escape_html("<script>alert('x')</script>"),
            "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"
        );
    }
}
