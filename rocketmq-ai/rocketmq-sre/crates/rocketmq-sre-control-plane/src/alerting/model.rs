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
use rocketmq_sre_contracts::AlertEvent;
use rocketmq_sre_contracts::AlertSeverity;
use rocketmq_sre_contracts::AlertSource;
use rocketmq_sre_contracts::AlertStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::NotificationDeliveryId;
use rocketmq_sre_contracts::NotificationTargetId;
use rocketmq_sre_contracts::ResourceKind;
use serde::Deserialize;
use serde::Serialize;

use crate::ControlPlaneError;

pub(crate) const MAX_ALERTS_PER_WEBHOOK: usize = 128;
pub(crate) const MAX_LABELS_PER_ALERT: usize = 64;
pub(crate) const MAX_LABEL_VALUE_CHARS: usize = 512;
pub(crate) const MAX_SUMMARY_CHARS: usize = 2_048;

/// Alertmanager v4 webhook plus the explicit cluster scope required by SRE.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AlertmanagerWebhook {
    pub version: String,
    pub cluster_id: ClusterId,
    pub status: String,
    #[serde(default)]
    pub receiver: String,
    #[serde(default)]
    pub group_key: String,
    #[serde(default)]
    pub common_labels: BTreeMap<String, String>,
    pub alerts: Vec<AlertmanagerAlert>,
}

impl AlertmanagerWebhook {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        if self.version != "4" {
            return Err(ControlPlaneError::validation(
                "unsupported_schema_major",
                "Alertmanager webhook version must be 4",
            ));
        }
        if !matches!(self.status.as_str(), "firing" | "resolved") {
            return Err(ControlPlaneError::validation(
                "invalid_alert_schema",
                "Alertmanager group status must be firing or resolved",
            ));
        }
        if self.alerts.is_empty() || self.alerts.len() > MAX_ALERTS_PER_WEBHOOK {
            return Err(ControlPlaneError::validation(
                "invalid_alert_schema",
                "Alertmanager webhook must contain between 1 and 128 alerts",
            ));
        }
        validate_bounded_text("receiver", &self.receiver, 256, true)?;
        validate_bounded_text("group_key", &self.group_key, 512, true)?;
        validate_labels(&self.common_labels)?;
        for alert in &self.alerts {
            alert.validate()?;
        }
        Ok(())
    }
}

/// One bounded Alertmanager alert.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AlertmanagerAlert {
    pub status: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    pub starts_at: DateTime<Utc>,
    pub ends_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub fingerprint: String,
}

impl AlertmanagerAlert {
    fn validate(&self) -> Result<(), ControlPlaneError> {
        if !matches!(self.status.as_str(), "firing" | "resolved") {
            return Err(ControlPlaneError::validation(
                "invalid_alert_schema",
                "alert status must be firing or resolved",
            ));
        }
        if self.ends_at.is_some_and(|ends_at| ends_at < self.starts_at) {
            return Err(ControlPlaneError::validation(
                "invalid_alert_schema",
                "alert end time cannot precede its start time",
            ));
        }
        validate_bounded_text("fingerprint", &self.fingerprint, 512, true)?;
        validate_labels(&self.labels)?;
        validate_labels(&self.annotations)?;
        if !self.labels.contains_key("alertname") {
            return Err(ControlPlaneError::validation(
                "invalid_alert_schema",
                "Alertmanager alert requires an alertname label",
            ));
        }
        Ok(())
    }
}

/// Provider-neutral authenticated event accepted from health probes,
/// Kubernetes, operator queries, inspections and deployment integrations.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct IntegrationEventRequest {
    pub cluster_id: ClusterId,
    pub source: AlertSource,
    pub source_event_id: String,
    pub resource_kind: ResourceKind,
    pub resource_key: String,
    pub display_name: Option<String>,
    pub symptom_family: String,
    pub severity: AlertSeverity,
    pub status: AlertStatus,
    pub summary: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub evidence_ids: Vec<rocketmq_sre_contracts::EvidenceId>,
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
}

impl IntegrationEventRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        self.validate_common()?;
        if self.source == AlertSource::Alertmanager {
            return Err(ControlPlaneError::validation(
                "invalid_alert_source",
                "Alertmanager events must use the Alertmanager webhook endpoint",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_unified_alert(&self) -> Result<(), ControlPlaneError> {
        self.validate_common()?;
        if matches!(
            self.source,
            AlertSource::OperatorQuery | AlertSource::Inspection | AlertSource::Deployment
        ) {
            return Err(ControlPlaneError::validation(
                "invalid_alert_source",
                "operator, inspection, and deployment events must use their dedicated unified entry kind",
            ));
        }
        Ok(())
    }

    fn validate_common(&self) -> Result<(), ControlPlaneError> {
        validate_bounded_text("source_event_id", &self.source_event_id, 512, false)?;
        validate_bounded_text("resource_key", &self.resource_key, 512, false)?;
        validate_bounded_text("symptom_family", &self.symptom_family, 128, false)?;
        validate_bounded_text("summary", &self.summary, MAX_SUMMARY_CHARS, false)?;
        if let Some(display_name) = &self.display_name {
            validate_bounded_text("display_name", display_name, 512, false)?;
        }
        validate_labels(&self.labels)?;
        if self.evidence_ids.len() > 64 {
            return Err(ControlPlaneError::validation(
                "output_too_large",
                "integration event contains more than 64 evidence references",
            ));
        }
        Ok(())
    }
}

/// Result of an idempotent event ingestion and correlation pass.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AlertIngestionOutcome {
    pub schema_version: &'static str,
    pub incident_id: IncidentId,
    pub alert_ids: Vec<rocketmq_sre_contracts::AlertEventId>,
    pub created: bool,
    pub recurrence: bool,
    pub occurrence_count: u32,
    pub owner: String,
    pub severity: AlertSeverity,
    pub partial: bool,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct IncidentTopologyView {
    pub schema_version: &'static str,
    pub incident_id: IncidentId,
    pub nodes: Vec<IncidentTopologyNode>,
    pub edges: Vec<IncidentTopologyEdge>,
    pub partial: bool,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct IncidentTopologyNode {
    pub key: String,
    pub kind: String,
    pub display_name: String,
    pub alert_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct IncidentTopologyEdge {
    pub from: String,
    pub to: String,
    pub relation: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ClusterIncidentHealth {
    pub schema_version: &'static str,
    pub cluster_id: ClusterId,
    pub status: &'static str,
    pub active_incidents: u32,
    pub critical_incidents: u32,
    pub unassigned_incidents: u32,
    pub last_alert_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct NotificationTestRequest {
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub target_id: NotificationTargetId,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct IncidentNoteRequest {
    pub note: String,
}

impl IncidentNoteRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        validate_bounded_text("incident note", &self.note, MAX_SUMMARY_CHARS, false)?;
        let normalized = self.note.to_ascii_lowercase();
        if [
            "token=",
            "secret=",
            "password=",
            "authorization:",
            "private key",
            "message body",
        ]
        .iter()
        .any(|marker| normalized.contains(marker))
        {
            return Err(ControlPlaneError::validation(
                "sensitive_data_rejected",
                "incident note contains prohibited sensitive material",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct NotificationTestResponse {
    pub schema_version: &'static str,
    pub delivery_id: NotificationDeliveryId,
    pub queued: bool,
    pub sanitized_summary: String,
    pub deep_link: String,
}

#[derive(Clone, Debug)]
pub(super) struct CorrelationResult {
    pub incident_id: IncidentId,
    pub created: bool,
    pub recurrence: bool,
    pub occurrence_count: u32,
    pub owner: String,
    pub severity: AlertSeverity,
}

#[derive(Clone, Debug)]
pub(super) struct StoredAlert {
    pub event: AlertEvent,
    pub persisted_id: rocketmq_sre_contracts::AlertEventId,
}

#[derive(Clone, Debug)]
pub(super) struct NotificationClaim {
    pub delivery_id: NotificationDeliveryId,
    pub claim_token: uuid::Uuid,
    pub channel: rocketmq_sre_contracts::NotificationChannel,
    pub endpoint: String,
    pub secret_reference: Option<String>,
    pub sanitized_summary: String,
    pub deep_link: String,
    pub attempt_count: u16,
    pub incident_id: IncidentId,
}

pub(super) fn validate_bounded_text(
    name: &'static str,
    value: &str,
    max_chars: usize,
    allow_empty: bool,
) -> Result<(), ControlPlaneError> {
    let trimmed = value.trim();
    if (!allow_empty && trimmed.is_empty())
        || trimmed.chars().count() > max_chars
        || trimmed.chars().any(char::is_control)
    {
        return Err(ControlPlaneError::validation(
            "invalid_alert_schema",
            format!("{name} is empty, contains control characters, or exceeds {max_chars} characters"),
        ));
    }
    Ok(())
}

fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), ControlPlaneError> {
    if labels.len() > MAX_LABELS_PER_ALERT {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "alert labels exceed the supported bound",
        ));
    }
    for (key, value) in labels {
        validate_bounded_text("label name", key, 128, false)?;
        validate_bounded_text("label value", value, MAX_LABEL_VALUE_CHARS, true)?;
        if is_sensitive_key(key) {
            return Err(ControlPlaneError::validation(
                "sensitive_data_rejected",
                "alert labels or annotations contain a prohibited sensitive field",
            ));
        }
    }
    Ok(())
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase().replace(['-', '.'], "_");
    matches!(
        key.as_str(),
        "authorization"
            | "password"
            | "passwd"
            | "secret"
            | "token"
            | "access_key"
            | "secret_key"
            | "private_key"
            | "message_body"
            | "acl"
            | "tls_material"
    ) || key.ends_with("_password")
        || key.ends_with("_secret")
        || key.ends_with("_token")
        || key.ends_with("_private_key")
}

/// Returns a bounded notification-safe summary. Arbitrary annotation bodies,
/// endpoints, credentials and message content are never included.
pub(super) fn notification_summary(event: &AlertEvent) -> String {
    let resource = format!(
        "{}:{}",
        resource_kind_name(event.affected_resource.kind),
        event.affected_resource.key
    );
    let summary = format!(
        "{} alert for {} ({})",
        severity_name(event.severity),
        resource,
        event.symptom_family.as_str()
    );
    summary.chars().take(MAX_SUMMARY_CHARS).collect()
}

pub(super) const fn severity_name(severity: AlertSeverity) -> &'static str {
    match severity {
        AlertSeverity::Info => "info",
        AlertSeverity::Warning => "warning",
        AlertSeverity::Error => "error",
        AlertSeverity::Critical => "critical",
    }
}

pub(super) const fn resource_kind_name(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Cluster => "cluster",
        ResourceKind::NameServer => "name_server",
        ResourceKind::Controller => "controller",
        ResourceKind::Broker => "broker",
        ResourceKind::Proxy => "proxy",
        ResourceKind::Store => "store",
        ResourceKind::Topic => "topic",
        ResourceKind::Queue => "queue",
        ResourceKind::ConsumerGroup => "consumer_group",
        ResourceKind::ProducerGroup => "producer_group",
        ResourceKind::Pod => "pod",
        ResourceKind::Node => "node",
        ResourceKind::PersistentVolumeClaim => "persistent_volume_claim",
        ResourceKind::Certificate => "certificate",
        ResourceKind::Runtime => "runtime",
        ResourceKind::Telemetry => "telemetry",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_rejects_sensitive_labels_and_unknown_schema() {
        let cluster_id = ClusterId::new();
        let mut labels = BTreeMap::from([("alertname".to_owned(), "BrokerDown".to_owned())]);
        let webhook = AlertmanagerWebhook {
            version: "3".into(),
            cluster_id,
            status: "firing".into(),
            receiver: "sre".into(),
            group_key: "broker".into(),
            common_labels: BTreeMap::new(),
            alerts: vec![AlertmanagerAlert {
                status: "firing".into(),
                labels: labels.clone(),
                annotations: BTreeMap::new(),
                starts_at: Utc::now(),
                ends_at: None,
                fingerprint: "upstream".into(),
            }],
        };
        assert!(webhook.validate().is_err());

        labels.insert("token".into(), "must-not-persist".into());
        let invalid = AlertmanagerWebhook {
            version: "4".into(),
            alerts: vec![AlertmanagerAlert {
                status: "firing".into(),
                labels,
                annotations: BTreeMap::new(),
                starts_at: Utc::now(),
                ends_at: None,
                fingerprint: "upstream".into(),
            }],
            ..webhook
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn notification_summary_uses_only_allowlisted_identity_fields() {
        let now = Utc::now();
        let tenant_id = rocketmq_sre_contracts::TenantId::new();
        let cluster_id = ClusterId::new();
        let event = AlertEvent {
            id: rocketmq_sre_contracts::AlertEventId::new(),
            tenant_id,
            cluster_id,
            source: AlertSource::Alertmanager,
            source_event_id: "fixture".into(),
            fingerprint: "sha256:fixture".into(),
            correlation_key: rocketmq_sre_contracts::CorrelationKey {
                tenant_id,
                cluster_id,
                resource_kind: ResourceKind::Broker,
                resource_key: "broker-a".into(),
                symptom_family: rocketmq_sre_contracts::SymptomFamily::new("broker_unavailable"),
                window_start: now,
                window_seconds: 300,
            },
            affected_resource: rocketmq_sre_contracts::ResourceRef {
                kind: ResourceKind::Broker,
                key: "broker-a".into(),
                display_name: None,
            },
            symptom_family: rocketmq_sre_contracts::SymptomFamily::new("broker_unavailable"),
            severity: AlertSeverity::Critical,
            status: AlertStatus::Firing,
            summary: "token=secret http://10.0.0.1".into(),
            labels: BTreeMap::new(),
            evidence_ids: Vec::new(),
            occurrence_count: 1,
            sequence: 1,
            occurred_at: now,
            received_at: now,
        };
        let summary = notification_summary(&event);
        assert_eq!(summary, "critical alert for broker:broker-a (broker_unavailable)");
        assert!(!summary.contains("secret"));
        assert!(!summary.contains("10.0.0.1"));
    }

    #[test]
    fn incident_note_is_bounded_and_rejects_sensitive_material() {
        assert!(
            IncidentNoteRequest {
                note: "Consumer capacity is recovering after the deployment rollback.".into(),
            }
            .validate()
            .is_ok()
        );
        assert!(
            IncidentNoteRequest {
                note: "token=must-not-persist".into(),
            }
            .validate()
            .is_err()
        );
        assert!(
            IncidentNoteRequest {
                note: "x".repeat(MAX_SUMMARY_CHARS + 1),
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn unified_alert_accepts_alertmanager_but_rejects_other_entry_kinds() {
        let mut request = IntegrationEventRequest {
            cluster_id: ClusterId::new(),
            source: AlertSource::Alertmanager,
            source_event_id: "alertmanager:broker-down".into(),
            resource_kind: ResourceKind::Broker,
            resource_key: "broker-a".into(),
            display_name: None,
            symptom_family: "broker_unavailable".into(),
            severity: AlertSeverity::Critical,
            status: AlertStatus::Firing,
            summary: "Broker unavailable".into(),
            labels: BTreeMap::new(),
            evidence_ids: Vec::new(),
            sequence: 1,
            occurred_at: Utc::now(),
        };

        assert!(request.validate().is_err());
        assert!(request.validate_unified_alert().is_ok());
        request.source = AlertSource::Deployment;
        assert!(request.validate_unified_alert().is_err());
    }
}
