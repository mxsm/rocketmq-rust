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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentOperationRequest;
use rocketmq_sre_contracts::IncidentOperationResult;
use rocketmq_sre_contracts::IncidentOperationsState;
use rocketmq_sre_contracts::OperationsFinding;
use rocketmq_sre_contracts::OperationsReport;
use rocketmq_sre_contracts::OperationsReportWindow;
use rocketmq_sre_contracts::ShiftHandoffSummary;

use super::report_repository::OperationsReportRepository;
use super::repository::OperatorWorkbenchRepository;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[derive(Clone)]
pub(crate) struct OperatorWorkbenchService {
    incidents: OperatorWorkbenchRepository,
    reports: OperationsReportRepository,
}

impl OperatorWorkbenchService {
    pub(crate) fn new(repository: PostgresRepository) -> Self {
        Self {
            incidents: OperatorWorkbenchRepository::new(repository.pool.clone()),
            reports: OperationsReportRepository::new(repository.pool),
        }
    }

    pub(crate) async fn incident_state(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
    ) -> Result<IncidentOperationsState, ControlPlaneError> {
        self.incidents.state(auth, incident_id).await
    }

    pub(crate) async fn apply_incident_operation(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        request: &IncidentOperationRequest,
        correlation_id: CorrelationId,
    ) -> Result<IncidentOperationResult, ControlPlaneError> {
        ensure_operator(auth)?;
        self.incidents.apply(auth, incident_id, request, correlation_id).await
    }

    pub(crate) async fn shift_handoff(
        &self,
        auth: &AuthContext,
        cluster_id: Option<ClusterId>,
    ) -> Result<ShiftHandoffSummary, ControlPlaneError> {
        self.reports.shift_handoff(auth, cluster_id).await
    }

    pub(crate) async fn report(
        &self,
        auth: &AuthContext,
        cluster_id: Option<ClusterId>,
        window: OperationsReportWindow,
    ) -> Result<OperationsReport, ControlPlaneError> {
        self.reports.report(auth, cluster_id, window).await
    }
}

pub(super) fn render_report_markdown(report: &OperationsReport) -> String {
    let mut output = format!(
        "# RocketMQ AI SRE {} operations report\n\n- Window: {} → {}\n- Generated: {}\n- Partial: {}\n- RocketMQ \
         mutations: {}\n",
        report_window_name(report.window),
        report.window_start.to_rfc3339(),
        report.window_end.to_rfc3339(),
        report.generated_at.to_rfc3339(),
        report.partial,
        report.cluster_mutation_count
    );
    append_markdown_section(&mut output, "Worst clusters", &report.worst_clusters);
    append_markdown_section(&mut output, "SLO burns", &report.slo_burns);
    append_markdown_section(
        &mut output,
        "Diagnostic pack findings",
        &report.diagnostic_pack_findings,
    );
    append_markdown_section(&mut output, "Repeat incidents", &report.repeat_incidents);
    output.push_str(&format!(
        "\n## Forecast accuracy\n\nMean absolute error: {}\n",
        report
            .forecast_mean_absolute_error
            .map_or_else(|| "not available".to_owned(), |value| format!("{value:.3}"))
    ));
    append_markdown_section(&mut output, "Forecast errors", &report.forecast_errors);
    append_markdown_section(&mut output, "Data-source gaps", &report.source_gaps);
    output
}

pub(super) fn render_report_html(report: &OperationsReport) -> String {
    let mut sections = String::new();
    append_html_section(&mut sections, "Worst clusters", &report.worst_clusters);
    append_html_section(&mut sections, "SLO burns", &report.slo_burns);
    append_html_section(
        &mut sections,
        "Diagnostic pack findings",
        &report.diagnostic_pack_findings,
    );
    append_html_section(&mut sections, "Repeat incidents", &report.repeat_incidents);
    append_html_section(&mut sections, "Forecast errors", &report.forecast_errors);
    append_html_section(&mut sections, "Data-source gaps", &report.source_gaps);
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width,initial-scale=1\"><title>RocketMQ AI SRE operations \
         report</title><style>body{{font-family:system-ui,sans-serif;max-width:1120px;margin:40px auto;padding:0 \
         24px;color:#182230}}h1,h2{{color:#0f172a}}table{{width:100%;border-collapse:collapse;margin-bottom:28px}}th,\
         td{{text-align:left;border-bottom:1px solid \
         #dbe2ea;padding:9px;vertical-align:top}}code{{background:#eef2f6;padding:2px \
         4px;border-radius:4px}}</style></head><body><h1>RocketMQ AI SRE {} operations report</h1><p>Window: \
         <code>{}</code> → <code>{}</code><br>Generated: <code>{}</code><br>Partial: {} · RocketMQ mutations: \
         {}</p>{sections}</body></html>",
        report_window_name(report.window),
        report.window_start.to_rfc3339(),
        report.window_end.to_rfc3339(),
        report.generated_at.to_rfc3339(),
        report.partial,
        report.cluster_mutation_count
    )
}

fn ensure_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "diagnose" | "operator" | "sre-admin" | "rocketmq:diagnose" | "rocketmq:sre"
        )
    }) {
        return Ok(());
    }
    Err(ControlPlaneError::forbidden(
        "unauthorized_scope",
        "operator role is required for incident metadata operations",
    ))
}

const fn report_window_name(window: OperationsReportWindow) -> &'static str {
    match window {
        OperationsReportWindow::Daily => "daily",
        OperationsReportWindow::Weekly => "weekly",
    }
}

fn append_markdown_section(output: &mut String, title: &str, findings: &[OperationsFinding]) {
    output.push_str(&format!("\n## {}\n", markdown_text(title)));
    if findings.is_empty() {
        output.push_str("\nNo findings.\n");
        return;
    }
    for finding in findings {
        output.push_str(&format!(
            "\n- **{}** [{}] {} — {} (owner: `{}`; [open]({}))\n",
            markdown_text(&finding.severity),
            markdown_text(&finding.cluster_id.to_string()),
            markdown_text(&finding.title),
            markdown_text(&finding.detail),
            markdown_text(&finding.suggested_owner),
            markdown_link(&finding.deep_link)
        ));
    }
}

fn append_html_section(output: &mut String, title: &str, findings: &[OperationsFinding]) {
    output.push_str(&format!("<h2>{}</h2>", escape_html(title)));
    if findings.is_empty() {
        output.push_str("<p>No findings.</p>");
        return;
    }
    output.push_str(
        "<table><thead><tr><th>Severity</th><th>Cluster</th><th>Finding</th><th>Owner</th></tr></thead><tbody>",
    );
    for finding in findings {
        output.push_str(&format!(
            "<tr><td>{}</td><td><code>{}</code></td><td><strong>{}</strong><br>{}</td><td>{}</td></tr>",
            escape_html(&finding.severity),
            escape_html(&finding.cluster_id.to_string()),
            escape_html(&finding.title),
            escape_html(&finding.detail),
            escape_html(&finding.suggested_owner)
        ));
    }
    output.push_str("</tbody></table>");
}

fn markdown_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('`', "'")
        .replace('*', "\\*")
        .replace('[', "\\[")
        .replace(']', "\\]")
}

fn markdown_link(value: &str) -> String {
    value.replace(['(', ')', ' ', '\r', '\n'], "")
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
    use chrono::Duration;
    use chrono::Utc;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    fn report_with_untrusted_text() -> OperationsReport {
        let now = Utc::now();
        OperationsReport {
            schema_version: "rocketmq-sre.operations-report.v1".to_owned(),
            tenant_id: TenantId::new(),
            window: OperationsReportWindow::Daily,
            window_start: now - Duration::days(1),
            window_end: now,
            generated_at: now,
            worst_clusters: vec![OperationsFinding {
                category: "health".to_owned(),
                severity: "warning".to_owned(),
                title: "<script>alert(1)</script>".to_owned(),
                cluster_id: ClusterId::new(),
                incident_id: None,
                resource: None,
                detail: "score=*42*".to_owned(),
                suggested_owner: "ops".to_owned(),
                observed_at: now,
                deep_link: "/clusters/one".to_owned(),
            }],
            slo_burns: Vec::new(),
            diagnostic_pack_findings: Vec::new(),
            repeat_incidents: Vec::new(),
            forecast_mean_absolute_error: None,
            forecast_errors: Vec::new(),
            source_gaps: Vec::new(),
            partial: false,
            warnings: Vec::new(),
            cluster_mutation_count: 0,
        }
    }

    #[test]
    fn html_download_escapes_operator_visible_text() {
        let html = render_report_html(&report_with_untrusted_text());

        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn markdown_download_preserves_read_only_marker() {
        let markdown = render_report_markdown(&report_with_untrusted_text());

        assert!(markdown.contains("RocketMQ mutations: 0"));
        assert!(markdown.contains("\\*42\\*"));
    }
}
