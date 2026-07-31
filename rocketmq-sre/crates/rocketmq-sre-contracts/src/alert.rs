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

use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::AlertEventId;
use crate::ClusterId;
use crate::EvidenceId;
use crate::TenantId;

/// Source that supplied an alert-like event.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSource {
    Alertmanager,
    KubernetesEvent,
    HealthProbe,
    OperatorQuery,
    Inspection,
    Deployment,
    SyntheticProbe,
}

/// Stable alert severity independent of an upstream provider's spelling.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Normalized alert lifecycle.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertStatus {
    Firing,
    Resolved,
}

/// Resource classes accepted by the Phase 2 correlation engine.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Cluster,
    NameServer,
    Controller,
    Broker,
    Proxy,
    Store,
    Topic,
    Queue,
    ConsumerGroup,
    ProducerGroup,
    Pod,
    Node,
    PersistentVolumeClaim,
    Certificate,
    Runtime,
    Telemetry,
}

/// Tenant-scoped resource identity used for correlation and topology lookup.
#[derive(Clone, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub key: String,
    pub display_name: Option<String>,
}

/// Extensible symptom classifier. The value is a stable, lower snake-case ID.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SymptomFamily(pub String);

impl SymptomFamily {
    /// Creates an extensible symptom identifier.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable symptom identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Deterministic first-pass key for bounded-window incident correlation.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CorrelationKey {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource_kind: ResourceKind,
    pub resource_key: String,
    pub symptom_family: SymptomFamily,
    pub window_start: DateTime<Utc>,
    pub window_seconds: u32,
}

/// Sanitized alert event persisted before correlation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct AlertEvent {
    pub id: AlertEventId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub source: AlertSource,
    pub source_event_id: String,
    pub fingerprint: String,
    pub correlation_key: CorrelationKey,
    pub affected_resource: ResourceRef,
    pub symptom_family: SymptomFamily,
    pub severity: AlertSeverity,
    pub status: AlertStatus,
    pub summary: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    pub occurrence_count: u32,
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_event_round_trips_without_weakly_typed_resource_identity() {
        let now = Utc::now();
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let event = AlertEvent {
            id: AlertEventId::new(),
            tenant_id,
            cluster_id,
            source: AlertSource::Alertmanager,
            source_event_id: "alert-42".into(),
            fingerprint: "sha256:fixture".into(),
            correlation_key: CorrelationKey {
                tenant_id,
                cluster_id,
                resource_kind: ResourceKind::Broker,
                resource_key: "broker-a".into(),
                symptom_family: SymptomFamily::new("broker_unavailable"),
                window_start: now,
                window_seconds: 300,
            },
            affected_resource: ResourceRef {
                kind: ResourceKind::Broker,
                key: "broker-a".into(),
                display_name: Some("Broker A".into()),
            },
            symptom_family: SymptomFamily::new("broker_unavailable"),
            severity: AlertSeverity::Critical,
            status: AlertStatus::Firing,
            summary: "broker readiness failed".into(),
            labels: BTreeMap::new(),
            evidence_ids: Vec::new(),
            occurrence_count: 1,
            sequence: 1,
            occurred_at: now,
            received_at: now,
        };

        let encoded = serde_json::to_value(&event).expect("alert should encode");
        let decoded: AlertEvent = serde_json::from_value(encoded).expect("alert should decode");
        assert_eq!(decoded, event);
    }
}
