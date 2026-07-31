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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::IncidentId;
use crate::TenantId;
use crate::TimelineEvent;

/// Supported operations-report time windows.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationsReportWindow {
    Daily,
    Weekly,
}

/// Bounded operator action applied to incident metadata.
///
/// These actions never call RocketMQ mutation APIs. Reopening or splitting
/// creates a linked incident instead of mutating a terminal incident.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum IncidentOperationRequest {
    Acknowledge {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    Assign {
        owner: String,
        reason: String,
    },
    Merge {
        target_incident_id: IncidentId,
        reason: String,
    },
    Split {
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resource: Option<String>,
        symptom_family: String,
        reason: String,
    },
    Suppress {
        until: DateTime<Utc>,
        reason: String,
    },
    Reopen {
        reason: String,
    },
}

/// Current SLA clocks associated with one incident.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct IncidentSlaState {
    pub acknowledgement_due_at: DateTime<Utc>,
    pub resolution_due_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledgement_breached: bool,
    pub resolution_breached: bool,
}

/// Read model for incident ownership and operator state.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct IncidentOperationsState {
    pub schema_version: String,
    pub incident_id: IncidentId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppressed_until: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppression_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merged_into_incident_id: Option<IncidentId>,
    pub split_incident_ids: Vec<IncidentId>,
    pub sla: IncidentSlaState,
    pub updated_at: DateTime<Utc>,
}

/// Result returned after an incident operator action.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct IncidentOperationResult {
    pub schema_version: String,
    pub state: IncidentOperationsState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_incident_id: Option<IncidentId>,
    pub timeline_event: TimelineEvent,
    pub cluster_mutation_performed: bool,
}

/// One bounded, sanitized item in a handoff or operations report.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct OperationsFinding {
    pub category: String,
    pub severity: String,
    pub title: String,
    pub cluster_id: ClusterId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incident_id: Option<IncidentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    pub detail: String,
    pub suggested_owner: String,
    pub observed_at: DateTime<Utc>,
    pub deep_link: String,
}

/// Shift-change read model assembled from current operational state.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ShiftHandoffSummary {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub window_start: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub new_incidents: Vec<OperationsFinding>,
    pub unresolved_incidents: Vec<OperationsFinding>,
    pub risk_trends: Vec<OperationsFinding>,
    pub recent_changes: Vec<OperationsFinding>,
    pub expiring_certificates: Vec<OperationsFinding>,
    pub capacity_risks: Vec<OperationsFinding>,
    pub overdue_action_items: Vec<OperationsFinding>,
    pub source_gaps: Vec<OperationsFinding>,
    pub partial: bool,
    pub warnings: Vec<String>,
}

/// Daily or weekly operational report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OperationsReport {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub window: OperationsReportWindow,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub worst_clusters: Vec<OperationsFinding>,
    pub slo_burns: Vec<OperationsFinding>,
    pub diagnostic_pack_findings: Vec<OperationsFinding>,
    pub repeat_incidents: Vec<OperationsFinding>,
    pub forecast_mean_absolute_error: Option<f64>,
    pub forecast_errors: Vec<OperationsFinding>,
    pub source_gaps: Vec<OperationsFinding>,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub cluster_mutation_count: u32,
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;

    #[test]
    fn operation_requests_are_explicitly_tagged() {
        let request = IncidentOperationRequest::Suppress {
            until: Utc::now() + Duration::hours(1),
            reason: "planned maintenance".to_owned(),
        };
        let encoded = serde_json::to_value(request).expect("request should serialize");

        assert_eq!(encoded["action"], "suppress");
        assert_eq!(encoded["reason"], "planned maintenance");
    }

    #[test]
    fn report_contract_keeps_cluster_mutation_explicitly_zero() {
        let now = Utc::now();
        let report = OperationsReport {
            schema_version: "rocketmq-sre.operations-report.v1".to_owned(),
            tenant_id: TenantId::new(),
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

        assert_eq!(report.cluster_mutation_count, 0);
    }
}
