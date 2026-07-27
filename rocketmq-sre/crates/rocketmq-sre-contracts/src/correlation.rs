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
use crate::EvidenceId;
use crate::IncidentId;
use crate::IncidentRelationId;
use crate::TenantId;

/// Reason that two incidents are linked without crossing tenant boundaries.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentRelationKind {
    Duplicate,
    SameRootCause,
    Parent,
    Child,
    Recurrence,
    ChangeRegression,
}

/// Immutable relation between two tenant- and cluster-scoped incidents.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct IncidentRelation {
    pub id: IncidentRelationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub from_incident_id: IncidentId,
    pub to_incident_id: IncidentId,
    pub kind: IncidentRelationKind,
    pub reason_code: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

/// Typed event categories written to the existing append-only timeline.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimelineEventKind {
    Alert,
    KubernetesEvent,
    ConfigurationChange,
    DeploymentChange,
    CertificateChange,
    DiagnosticPackResult,
    HealthSnapshot,
    ModelResult,
    OperatorNote,
    IncidentStatusChange,
    NotificationDelivery,
    PostmortemRevision,
}

impl TimelineEventKind {
    /// Returns the stable value stored in the existing `event_type` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Alert => "alert",
            Self::KubernetesEvent => "kubernetes_event",
            Self::ConfigurationChange => "configuration_change",
            Self::DeploymentChange => "deployment_change",
            Self::CertificateChange => "certificate_change",
            Self::DiagnosticPackResult => "diagnostic_pack_result",
            Self::HealthSnapshot => "health_snapshot",
            Self::ModelResult => "model_result",
            Self::OperatorNote => "operator_note",
            Self::IncidentStatusChange => "incident_status_change",
            Self::NotificationDelivery => "notification_delivery",
            Self::PostmortemRevision => "postmortem_revision",
        }
    }
}
