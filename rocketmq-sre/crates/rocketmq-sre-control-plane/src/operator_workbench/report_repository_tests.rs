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

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::OperationsFinding;
use rocketmq_sre_contracts::OperationsReport;
use rocketmq_sre_contracts::OperationsReportWindow;
use rocketmq_sre_contracts::TenantId;

use super::report_repository::REPORT_SCHEMA;
use super::report_support::mean_error;

fn finding(error: f64) -> OperationsFinding {
    OperationsFinding {
        category: "forecast_error".to_owned(),
        severity: "info".to_owned(),
        title: "forecast".to_owned(),
        cluster_id: ClusterId::new(),
        incident_id: None,
        resource: None,
        detail: format!("window=daily; mae_sample={error}"),
        suggested_owner: "sre".to_owned(),
        observed_at: Utc::now(),
        deep_link: "/forecasts".to_owned(),
    }
}

#[test]
fn computes_report_mae_from_bounded_findings() {
    assert_eq!(mean_error(&[finding(1.0), finding(3.0)]), Some(2.0));
}

#[test]
fn report_schema_is_tenant_scoped() {
    let tenant = TenantId::new();
    let now = Utc::now();
    let report = OperationsReport {
        schema_version: REPORT_SCHEMA.to_owned(),
        tenant_id: tenant,
        window: OperationsReportWindow::Daily,
        window_start: now - Duration::days(1),
        window_end: now,
        generated_at: now,
        worst_clusters: Vec::new(),
        slo_burns: Vec::new(),
        diagnostic_pack_findings: Vec::new(),
        repeat_incidents: Vec::new(),
        forecast_mean_absolute_error: None,
        forecast_errors: Vec::new(),
        source_gaps: Vec::new(),
        partial: false,
        warnings: Vec::new(),
        cluster_mutation_count: 0,
    };

    assert_eq!(report.tenant_id, tenant);
    assert_eq!(report.cluster_mutation_count, 0);
}
