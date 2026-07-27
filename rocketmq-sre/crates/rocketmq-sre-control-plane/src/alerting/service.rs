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

use chrono::TimeZone;
use chrono::Utc;
use rocketmq_sre_contracts::AlertEvent;
use rocketmq_sre_contracts::AlertEventId;
use rocketmq_sre_contracts::AlertSeverity;
use rocketmq_sre_contracts::AlertSource;
use rocketmq_sre_contracts::AlertStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CorrelationKey;
use rocketmq_sre_contracts::ResourceKind;
use rocketmq_sre_contracts::ResourceRef;
use rocketmq_sre_contracts::SymptomFamily;
use rocketmq_sre_core::correlation::CorrelationFingerprintMaterial;
use rocketmq_sre_core::correlation::DEFAULT_CORRELATION_WINDOW_SECONDS;
use rocketmq_sre_core::correlation::bounded_window_start_epoch;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

use super::model::AlertIngestionOutcome;
use super::model::AlertmanagerAlert;
use super::model::AlertmanagerWebhook;
use super::model::ClusterIncidentHealth;
use super::model::IncidentNoteRequest;
use super::model::IncidentTopologyView;
use super::model::IntegrationEventRequest;
use super::model::NotificationTestRequest;
use super::model::NotificationTestResponse;
use super::model::StoredAlert;
use super::model::resource_kind_name;
use crate::ControlPlaneError;
use crate::Phase2Repository;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::workflow::WorkflowService;
use crate::workflow::WorkflowStreamEvent;

const DEFAULT_PUBLIC_BASE_URL: &str = "http://localhost:3004";

/// Authenticated alert ingestion, deterministic correlation and read-only
/// incident projection facade.
#[derive(Clone)]
pub(crate) struct AlertingService {
    repository: PostgresRepository,
    workflow: WorkflowService,
    public_base_url: String,
}

impl AlertingService {
    pub(crate) fn new(repository: PostgresRepository, workflow: WorkflowService) -> Result<Self, ControlPlaneError> {
        let public_base_url =
            std::env::var("ROCKETMQ_SRE_PUBLIC_URL").unwrap_or_else(|_| DEFAULT_PUBLIC_BASE_URL.to_owned());
        let parsed = url::Url::parse(&public_base_url)
            .map_err(|_| ControlPlaneError::configuration("ROCKETMQ_SRE_PUBLIC_URL is invalid"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_PUBLIC_URL must be an HTTP(S) origin without credentials",
            ));
        }
        Ok(Self {
            repository,
            workflow,
            public_base_url: public_base_url.trim_end_matches('/').to_owned(),
        })
    }

    pub(crate) async fn ingest_alertmanager(
        &self,
        auth: &AuthContext,
        webhook: &AlertmanagerWebhook,
        correlation_id: CorrelationId,
    ) -> Result<Vec<AlertIngestionOutcome>, ControlPlaneError> {
        webhook.validate()?;
        if !auth.clusters.contains(&webhook.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "Alertmanager cluster is outside the authenticated scope",
            ));
        }
        let received_at = Utc::now();
        let mut outcomes = Vec::with_capacity(webhook.alerts.len());
        for alert in &webhook.alerts {
            let event = normalize_alertmanager(auth, webhook, alert, received_at)?;
            outcomes.push(self.persist_and_correlate(auth, event, correlation_id).await?);
        }
        Ok(outcomes)
    }

    pub(crate) async fn ingest_integration_event(
        &self,
        auth: &AuthContext,
        request: &IntegrationEventRequest,
        correlation_id: CorrelationId,
    ) -> Result<AlertIngestionOutcome, ControlPlaneError> {
        request.validate()?;
        if !auth.clusters.contains(&request.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "integration event cluster is outside the authenticated scope",
            ));
        }
        let event = normalize_integration_event(auth, request)?;
        self.persist_and_correlate(auth, event, correlation_id).await
    }

    pub(crate) async fn timeline(
        &self,
        auth: &AuthContext,
        incident_id: rocketmq_sre_contracts::IncidentId,
    ) -> Result<Vec<rocketmq_sre_contracts::TimelineEvent>, ControlPlaneError> {
        self.repository.incident_timeline_for_alerting(auth, incident_id).await
    }

    pub(crate) async fn topology(
        &self,
        auth: &AuthContext,
        incident_id: rocketmq_sre_contracts::IncidentId,
    ) -> Result<IncidentTopologyView, ControlPlaneError> {
        self.repository.incident_topology_for_alerting(auth, incident_id).await
    }

    pub(crate) async fn add_note(
        &self,
        auth: &AuthContext,
        incident_id: rocketmq_sre_contracts::IncidentId,
        request: &IncidentNoteRequest,
        correlation_id: CorrelationId,
    ) -> Result<rocketmq_sre_contracts::TimelineEvent, ControlPlaneError> {
        request.validate()?;
        let timeline = self
            .repository
            .append_incident_note(auth, incident_id, request.note.trim(), correlation_id)
            .await?;
        self.workflow.publish_external(WorkflowStreamEvent {
            tenant_id: timeline.tenant_id,
            cluster_id: timeline.cluster_id,
            aggregate_type: "incident",
            aggregate_id: incident_id.to_string(),
            event_type: "incident_note_added",
            payload: json!({"timeline_event_id": timeline.id}),
            correlation_id,
            occurred_at: timeline.occurred_at,
        });
        Ok(timeline)
    }

    pub(crate) async fn cluster_health(
        &self,
        auth: &AuthContext,
        cluster_id: rocketmq_sre_contracts::ClusterId,
    ) -> Result<ClusterIncidentHealth, ControlPlaneError> {
        self.repository.cluster_incident_health(auth, cluster_id).await
    }

    pub(crate) async fn test_notification(
        &self,
        auth: &AuthContext,
        request: &NotificationTestRequest,
    ) -> Result<NotificationTestResponse, ControlPlaneError> {
        let (delivery_id, queued, sanitized_summary, deep_link) = self
            .repository
            .enqueue_notification_test(
                auth,
                request.cluster_id,
                request.incident_id,
                request.target_id,
                &self.public_base_url,
            )
            .await?;
        Ok(NotificationTestResponse {
            schema_version: "rocketmq-sre.notification-test.v1",
            delivery_id,
            queued,
            sanitized_summary,
            deep_link,
        })
    }

    async fn persist_and_correlate(
        &self,
        auth: &AuthContext,
        event: AlertEvent,
        correlation_id: CorrelationId,
    ) -> Result<AlertIngestionOutcome, ControlPlaneError> {
        let alert_id = self.repository.store_alert(&event).await?;
        let stored = StoredAlert {
            event,
            persisted_id: AlertEventId::from_uuid(alert_id),
        };
        let result = self
            .repository
            .correlate_alert(
                auth,
                &stored.event,
                stored.persisted_id,
                correlation_id,
                &self.public_base_url,
            )
            .await?;
        self.repository
            .append_latest_health_to_incident(auth, stored.event.cluster_id, result.incident_id, correlation_id)
            .await?;
        self.workflow.publish_external(WorkflowStreamEvent {
            tenant_id: stored.event.tenant_id,
            cluster_id: stored.event.cluster_id,
            aggregate_type: "incident",
            aggregate_id: result.incident_id.to_string(),
            event_type: "incident_alert_correlated",
            payload: json!({
                "incident_id": result.incident_id,
                "alert_id": stored.persisted_id,
                "created": result.created,
                "recurrence": result.recurrence,
                "occurrence_count": result.occurrence_count,
                "owner": result.owner,
                "severity": result.severity,
            }),
            correlation_id,
            occurred_at: stored.event.received_at,
        });
        Ok(AlertIngestionOutcome {
            schema_version: "rocketmq-sre.alert-ingestion.v1",
            incident_id: result.incident_id,
            alert_ids: vec![stored.persisted_id],
            created: result.created,
            recurrence: result.recurrence,
            occurrence_count: result.occurrence_count,
            owner: result.owner,
            severity: result.severity,
            partial: false,
            warnings: Vec::new(),
        })
    }
}

fn normalize_alertmanager(
    auth: &AuthContext,
    webhook: &AlertmanagerWebhook,
    alert: &AlertmanagerAlert,
    received_at: chrono::DateTime<Utc>,
) -> Result<AlertEvent, ControlPlaneError> {
    let mut labels = webhook.common_labels.clone();
    labels.extend(alert.labels.clone());
    let alert_name = labels
        .get("alertname")
        .ok_or_else(|| ControlPlaneError::validation("invalid_alert_schema", "alertname is required"))?;
    let resource_kind = labels
        .get("rocketmq_resource_kind")
        .or_else(|| labels.get("resource_kind"))
        .map(|value| parse_resource_kind(value))
        .transpose()?
        .or_else(|| infer_resource_kind(&labels))
        .unwrap_or(ResourceKind::Cluster);
    let resource_key = labels
        .get("rocketmq_resource_key")
        .or_else(|| labels.get("resource_key"))
        .or_else(|| inferred_resource_key(resource_kind, &labels))
        .cloned()
        .unwrap_or_else(|| webhook.cluster_id.to_string());
    validate_identity("resource key", &resource_key, 512)?;
    let symptom_family = labels
        .get("symptom_family")
        .map_or_else(|| normalize_identifier(alert_name), |value| normalize_identifier(value));
    validate_identity("symptom family", &symptom_family, 128)?;
    let source_event_id = if alert.fingerprint.trim().is_empty() {
        format!(
            "sha256:{:x}",
            Sha256::digest(
                format!(
                    "{}|{}|{}|{}",
                    webhook.cluster_id,
                    alert_name,
                    resource_key,
                    alert.starts_at.timestamp_millis()
                )
                .as_bytes()
            )
        )
    } else {
        alert.fingerprint.clone()
    };
    let status = parse_alert_status(&alert.status)?;
    let occurred_at = match status {
        AlertStatus::Firing => alert.starts_at,
        AlertStatus::Resolved => alert.ends_at.unwrap_or(alert.starts_at),
    };
    let sequence = labels
        .get("rocketmq_sequence")
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| occurrence_sequence(status, alert.starts_at, alert.ends_at));
    let event_labels = allowlisted_labels(&labels);
    build_event(
        auth,
        webhook.cluster_id,
        AlertSource::Alertmanager,
        source_event_id,
        resource_kind,
        resource_key,
        labels.get("display_name").cloned(),
        symptom_family,
        parse_severity(labels.get("severity").map(String::as_str))?,
        status,
        format!(
            "{} on {}:{}",
            normalize_display(alert_name),
            resource_kind_name(resource_kind),
            labels
                .get("rocketmq_resource_key")
                .or_else(|| labels.get("resource_key"))
                .or_else(|| inferred_resource_key(resource_kind, &labels))
                .map_or("cluster", String::as_str)
        ),
        event_labels,
        Vec::new(),
        sequence,
        occurred_at,
        received_at,
    )
}

fn normalize_integration_event(
    auth: &AuthContext,
    request: &IntegrationEventRequest,
) -> Result<AlertEvent, ControlPlaneError> {
    let summary = if safe_summary(&request.summary) {
        request.summary.trim().to_owned()
    } else {
        format!(
            "{} on {}:{}",
            normalize_display(&request.symptom_family),
            resource_kind_name(request.resource_kind),
            request.resource_key
        )
    };
    build_event(
        auth,
        request.cluster_id,
        request.source,
        request.source_event_id.clone(),
        request.resource_kind,
        request.resource_key.clone(),
        request.display_name.clone(),
        normalize_identifier(&request.symptom_family),
        request.severity,
        request.status,
        summary,
        allowlisted_labels(&request.labels),
        request.evidence_ids.clone(),
        request.sequence,
        request.occurred_at,
        Utc::now(),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "normalization creates the complete canonical alert envelope in one audited boundary"
)]
fn build_event(
    auth: &AuthContext,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    source: AlertSource,
    source_event_id: String,
    resource_kind: ResourceKind,
    resource_key: String,
    display_name: Option<String>,
    symptom_family: String,
    severity: AlertSeverity,
    status: AlertStatus,
    summary: String,
    labels: BTreeMap<String, String>,
    evidence_ids: Vec<rocketmq_sre_contracts::EvidenceId>,
    sequence: u64,
    occurred_at: chrono::DateTime<Utc>,
    received_at: chrono::DateTime<Utc>,
) -> Result<AlertEvent, ControlPlaneError> {
    validate_identity("source event identifier", &source_event_id, 512)?;
    validate_identity("resource key", &resource_key, 512)?;
    validate_identity("symptom family", &symptom_family, 128)?;
    let window_epoch = bounded_window_start_epoch(occurred_at.timestamp(), DEFAULT_CORRELATION_WINDOW_SECONDS)
        .ok_or_else(|| {
            ControlPlaneError::validation(
                "invalid_alert_schema",
                "alert timestamp cannot be represented in a correlation window",
            )
        })?;
    let window_start = Utc
        .timestamp_opt(window_epoch, 0)
        .single()
        .ok_or_else(|| ControlPlaneError::validation("invalid_alert_schema", "alert timestamp is invalid"))?;
    let material = CorrelationFingerprintMaterial::new(
        auth.tenant_id,
        cluster_id,
        source,
        &source_event_id,
        resource_kind,
        &resource_key,
        &symptom_family,
    );
    let fingerprint = format!("sha256:{:x}", Sha256::digest(material.canonical_bytes()));
    let event_id = AlertEventId::from_uuid(deterministic_uuid(&format!(
        "{}:{}:{}:{}",
        auth.tenant_id,
        cluster_id,
        source_name(source),
        source_event_id
    )));
    let symptom_family = SymptomFamily::new(symptom_family);
    Ok(AlertEvent {
        id: event_id,
        tenant_id: auth.tenant_id,
        cluster_id,
        source,
        source_event_id,
        fingerprint,
        correlation_key: CorrelationKey {
            tenant_id: auth.tenant_id,
            cluster_id,
            resource_kind,
            resource_key: resource_key.clone(),
            symptom_family: symptom_family.clone(),
            window_start,
            window_seconds: DEFAULT_CORRELATION_WINDOW_SECONDS,
        },
        affected_resource: ResourceRef {
            kind: resource_kind,
            key: resource_key,
            display_name,
        },
        symptom_family,
        severity,
        status,
        summary: summary.chars().take(2_048).collect(),
        labels,
        evidence_ids,
        occurrence_count: 1,
        sequence,
        occurred_at,
        received_at,
    })
}

fn parse_resource_kind(value: &str) -> Result<ResourceKind, ControlPlaneError> {
    match normalize_identifier(value).as_str() {
        "cluster" => Ok(ResourceKind::Cluster),
        "name_server" | "nameserver" => Ok(ResourceKind::NameServer),
        "controller" => Ok(ResourceKind::Controller),
        "broker" => Ok(ResourceKind::Broker),
        "proxy" => Ok(ResourceKind::Proxy),
        "store" => Ok(ResourceKind::Store),
        "topic" => Ok(ResourceKind::Topic),
        "queue" => Ok(ResourceKind::Queue),
        "consumer_group" | "consumergroup" => Ok(ResourceKind::ConsumerGroup),
        "producer_group" | "producergroup" => Ok(ResourceKind::ProducerGroup),
        "pod" => Ok(ResourceKind::Pod),
        "node" => Ok(ResourceKind::Node),
        "persistent_volume_claim" | "pvc" => Ok(ResourceKind::PersistentVolumeClaim),
        "certificate" | "cert" => Ok(ResourceKind::Certificate),
        "runtime" => Ok(ResourceKind::Runtime),
        "telemetry" => Ok(ResourceKind::Telemetry),
        _ => Err(ControlPlaneError::validation(
            "invalid_alert_schema",
            "unsupported alert resource kind",
        )),
    }
}

fn infer_resource_kind(labels: &BTreeMap<String, String>) -> Option<ResourceKind> {
    [
        ("broker", ResourceKind::Broker),
        ("topic", ResourceKind::Topic),
        ("queue", ResourceKind::Queue),
        ("consumer_group", ResourceKind::ConsumerGroup),
        ("controller", ResourceKind::Controller),
        ("pod", ResourceKind::Pod),
        ("node", ResourceKind::Node),
        ("proxy", ResourceKind::Proxy),
    ]
    .into_iter()
    .find_map(|(label, kind)| labels.contains_key(label).then_some(kind))
}

fn inferred_resource_key(kind: ResourceKind, labels: &BTreeMap<String, String>) -> Option<&String> {
    let key = match kind {
        ResourceKind::NameServer => "nameserver",
        ResourceKind::ConsumerGroup => "consumer_group",
        ResourceKind::ProducerGroup => "producer_group",
        ResourceKind::PersistentVolumeClaim => "pvc",
        _ => resource_kind_name(kind),
    };
    labels.get(key)
}

fn parse_severity(value: Option<&str>) -> Result<AlertSeverity, ControlPlaneError> {
    match value.map(normalize_identifier).as_deref() {
        None | Some("") | Some("warning") | Some("warn") => Ok(AlertSeverity::Warning),
        Some("info") | Some("information") => Ok(AlertSeverity::Info),
        Some("error") | Some("high") => Ok(AlertSeverity::Error),
        Some("critical") | Some("fatal") | Some("page") => Ok(AlertSeverity::Critical),
        Some(_) => Err(ControlPlaneError::validation(
            "invalid_alert_schema",
            "unsupported alert severity",
        )),
    }
}

fn parse_alert_status(value: &str) -> Result<AlertStatus, ControlPlaneError> {
    match value {
        "firing" => Ok(AlertStatus::Firing),
        "resolved" => Ok(AlertStatus::Resolved),
        _ => Err(ControlPlaneError::validation(
            "invalid_alert_schema",
            "unsupported alert status",
        )),
    }
}

fn allowlisted_labels(labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    const ALLOWED: [&str; 8] = [
        "owner",
        "team",
        "on_call",
        "namespace",
        "region",
        "environment",
        "service",
        "workload",
    ];
    labels
        .iter()
        .filter(|(key, value)| {
            ALLOWED.contains(&key.as_str()) && value.chars().count() <= 512 && !value.chars().any(char::is_control)
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn normalize_identifier(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut previous_separator = false;
    for character in value.trim().chars() {
        if character.is_ascii_alphanumeric() {
            output.push(character.to_ascii_lowercase());
            previous_separator = false;
        } else if !previous_separator && !output.is_empty() {
            output.push('_');
            previous_separator = true;
        }
    }
    while output.ends_with('_') {
        output.pop();
    }
    output
}

fn normalize_display(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|character| !character.is_control())
        .take(256)
        .collect()
}

fn validate_identity(name: &'static str, value: &str, max: usize) -> Result<(), ControlPlaneError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max || value.chars().any(char::is_control) {
        return Err(ControlPlaneError::validation(
            "invalid_alert_schema",
            format!("{name} is empty, contains control characters, or exceeds {max} characters"),
        ));
    }
    Ok(())
}

fn safe_summary(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    !value.trim().is_empty()
        && value.chars().count() <= 2_048
        && !value.chars().any(char::is_control)
        && ![
            "token=",
            "secret=",
            "password",
            "authorization:",
            "private key",
            "message body",
            "http://",
            "https://",
        ]
        .iter()
        .any(|marker| normalized.contains(marker))
}

fn occurrence_sequence(
    status: AlertStatus,
    starts_at: chrono::DateTime<Utc>,
    ends_at: Option<chrono::DateTime<Utc>>,
) -> u64 {
    let material = format!(
        "{}|{}|{}",
        alert_status_name(status),
        starts_at,
        ends_at.unwrap_or(starts_at)
    );
    let digest = Sha256::digest(material.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes)
}

fn deterministic_uuid(material: &str) -> Uuid {
    let digest = Sha256::digest(material.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

const fn source_name(source: AlertSource) -> &'static str {
    match source {
        AlertSource::Alertmanager => "alertmanager",
        AlertSource::KubernetesEvent => "kubernetes_event",
        AlertSource::HealthProbe => "health_probe",
        AlertSource::OperatorQuery => "operator_query",
        AlertSource::Inspection => "inspection",
        AlertSource::Deployment => "deployment",
        AlertSource::SyntheticProbe => "synthetic_probe",
    }
}

const fn alert_status_name(status: AlertStatus) -> &'static str {
    match status {
        AlertStatus::Firing => "firing",
        AlertStatus::Resolved => "resolved",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[test]
    fn all_non_alertmanager_sources_normalize_to_canonical_events() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let auth = AuthContext {
            tenant_id,
            subject: "integration".into(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::new(),
        };
        for source in [
            AlertSource::KubernetesEvent,
            AlertSource::HealthProbe,
            AlertSource::OperatorQuery,
            AlertSource::Inspection,
            AlertSource::Deployment,
            AlertSource::SyntheticProbe,
        ] {
            let request = IntegrationEventRequest {
                cluster_id,
                source,
                source_event_id: format!("event-{}", source_name(source)),
                resource_kind: ResourceKind::Broker,
                resource_key: "broker-a".into(),
                display_name: None,
                symptom_family: "Broker Unavailable".into(),
                severity: AlertSeverity::Error,
                status: AlertStatus::Firing,
                summary: "broker unavailable".into(),
                labels: BTreeMap::new(),
                evidence_ids: Vec::new(),
                sequence: 1,
                occurred_at: Utc::now(),
            };
            let event = normalize_integration_event(&auth, &request).expect("canonical event");
            assert_eq!(event.source, source);
            assert_eq!(event.symptom_family.as_str(), "broker_unavailable");
            assert_eq!(event.correlation_key.tenant_id, tenant_id);
            assert_eq!(event.correlation_key.cluster_id, cluster_id);
        }
    }

    #[test]
    fn alertmanager_retry_produces_stable_event_and_occurrence_identity() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let auth = AuthContext {
            tenant_id,
            subject: "alertmanager".into(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::new(),
        };
        let starts_at = Utc::now();
        let webhook = AlertmanagerWebhook {
            version: "4".into(),
            cluster_id,
            status: "firing".into(),
            receiver: "sre".into(),
            group_key: "broker".into(),
            common_labels: BTreeMap::new(),
            alerts: Vec::new(),
        };
        let alert = AlertmanagerAlert {
            status: "firing".into(),
            labels: BTreeMap::from([
                ("alertname".into(), "BrokerDown".into()),
                ("broker".into(), "broker-a".into()),
                ("severity".into(), "critical".into()),
            ]),
            annotations: BTreeMap::new(),
            starts_at,
            ends_at: None,
            fingerprint: "upstream-fingerprint".into(),
        };
        let first = normalize_alertmanager(&auth, &webhook, &alert, starts_at).expect("first event");
        let retry = normalize_alertmanager(&auth, &webhook, &alert, starts_at).expect("retried event");
        assert_eq!(first.id, retry.id);
        assert_eq!(first.sequence, retry.sequence);
        assert_eq!(first.fingerprint, retry.fingerprint);
    }

    #[test]
    fn unsafe_operator_summary_is_not_persisted() {
        assert!(!safe_summary("token=secret"));
        assert!(!safe_summary("https://10.0.0.1/internal"));
        assert!(safe_summary("broker unavailable"));
    }
}
