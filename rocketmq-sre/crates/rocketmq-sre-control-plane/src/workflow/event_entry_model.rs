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
use rocketmq_sre_contracts::AlertSeverity;
use rocketmq_sre_contracts::AlertSource;
use rocketmq_sre_contracts::AlertStatus;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::InspectionTemplate;
use rocketmq_sre_contracts::ResourceKind;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use uuid::Uuid;

use super::InspectionCreateRequest;
use super::InvestigationCreateRequest;
use crate::ControlPlaneError;
use crate::alerting::IntegrationEventRequest;

pub(super) const EVENT_ENTRY_SCHEMA: &str = "rocketmq-sre.event-entry.v1";
pub(super) const EVENT_ENTRY_RESULT_SCHEMA: &str = "rocketmq-sre.event-entry-result.v1";

/// One authenticated entry into the SRE investigation workflow. The tagged
/// payload keeps all five sources explicit and prevents arbitrary aggregate
/// or execution creation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct UnifiedEventEntryRequest {
    pub schema_version: String,
    pub cluster_id: ClusterId,
    pub idempotency_key: String,
    pub occurred_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub(super) payload: UnifiedEventPayload,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "source_kind", rename_all = "snake_case")]
pub(super) enum UnifiedEventPayload {
    Alert {
        source: AlertSource,
        source_event_id: String,
        resource_kind: ResourceKind,
        resource_key: String,
        display_name: Option<String>,
        symptom_family: String,
        severity: AlertSeverity,
        status: AlertStatus,
        summary: String,
        #[serde(default)]
        labels: BTreeMap<String, String>,
        #[serde(default)]
        evidence_ids: Vec<EvidenceId>,
        sequence: u64,
    },
    ManualIssue {
        title: String,
        resource: Option<String>,
        symptom_family: String,
    },
    ScheduledInspection {
        template: InspectionTemplate,
        schedule: Option<String>,
    },
    ChangeEvent {
        change_kind: ChangeEventKind,
        #[serde(default)]
        target: EventEntryWorkflowTarget,
        title: String,
        resource: Option<String>,
        symptom_family: String,
    },
    ExternalIntegration {
        channel: ExternalEventChannel,
        target: EventEntryWorkflowTarget,
        title: String,
        resource: Option<String>,
        symptom_family: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ChangeEventKind {
    Release,
    Deployment,
    Configuration,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum EventEntryWorkflowTarget {
    #[default]
    Investigation,
    Incident,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExternalEventChannel {
    Itsm,
    ChatOps,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventEntrySourceKind {
    Alert,
    ManualIssue,
    ScheduledInspection,
    ChangeEvent,
    ExternalIntegration,
}

impl EventEntrySourceKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Alert => "alert",
            Self::ManualIssue => "manual_issue",
            Self::ScheduledInspection => "scheduled_inspection",
            Self::ChangeEvent => "change_event",
            Self::ExternalIntegration => "external_integration",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, ControlPlaneError> {
        match value {
            "alert" => Ok(Self::Alert),
            "manual_issue" => Ok(Self::ManualIssue),
            "scheduled_inspection" => Ok(Self::ScheduledInspection),
            "change_event" => Ok(Self::ChangeEvent),
            "external_integration" => Ok(Self::ExternalIntegration),
            _ => Err(ControlPlaneError::validation(
                "source_unavailable",
                "stored event entry source kind is unsupported",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventEntryTargetKind {
    Investigation,
    Incident,
    InspectionRun,
}

impl EventEntryTargetKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Investigation => "investigation",
            Self::Incident => "incident",
            Self::InspectionRun => "inspection_run",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, ControlPlaneError> {
        match value {
            "investigation" => Ok(Self::Investigation),
            "incident" => Ok(Self::Incident),
            "inspection_run" => Ok(Self::InspectionRun),
            _ => Err(ControlPlaneError::validation(
                "source_unavailable",
                "stored event entry target kind is unsupported",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct UnifiedEventEntryResult {
    pub schema_version: &'static str,
    pub entry_id: Uuid,
    pub source_kind: EventEntrySourceKind,
    pub target_kind: EventEntryTargetKind,
    pub target_id: Uuid,
    pub created: bool,
    pub replayed: bool,
    pub correlation_id: CorrelationId,
    pub accepted_at: DateTime<Utc>,
}

impl UnifiedEventEntryRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        if self.schema_version != EVENT_ENTRY_SCHEMA {
            return Err(ControlPlaneError::validation(
                "unsupported_schema_major",
                "event entry schema must be rocketmq-sre.event-entry.v1",
            ));
        }
        validate_idempotency_key(&self.idempotency_key)?;
        match &self.payload {
            UnifiedEventPayload::Alert {
                source,
                source_event_id,
                resource_kind,
                resource_key,
                display_name,
                symptom_family,
                severity,
                status,
                summary,
                labels,
                evidence_ids,
                sequence,
            } => IntegrationEventRequest {
                cluster_id: self.cluster_id,
                source: *source,
                source_event_id: source_event_id.clone(),
                resource_kind: *resource_kind,
                resource_key: resource_key.clone(),
                display_name: display_name.clone(),
                symptom_family: symptom_family.clone(),
                severity: *severity,
                status: *status,
                summary: summary.clone(),
                labels: labels.clone(),
                evidence_ids: evidence_ids.clone(),
                sequence: *sequence,
                occurred_at: self.effective_occurred_at(),
            }
            .validate_unified_alert(),
            UnifiedEventPayload::ManualIssue {
                title,
                resource,
                symptom_family,
            }
            | UnifiedEventPayload::ChangeEvent {
                title,
                resource,
                symptom_family,
                ..
            }
            | UnifiedEventPayload::ExternalIntegration {
                title,
                resource,
                symptom_family,
                ..
            } => InvestigationCreateRequest {
                cluster_id: self.cluster_id,
                conversation_id: None,
                title: title.clone(),
                resource: resource.clone(),
                symptom_family: symptom_family.clone(),
            }
            .validate(),
            UnifiedEventPayload::ScheduledInspection { template, schedule } => InspectionCreateRequest {
                cluster_id: self.cluster_id,
                template: *template,
                schedule: schedule.clone(),
            }
            .validate(),
        }
    }

    pub(super) const fn source_kind(&self) -> EventEntrySourceKind {
        match &self.payload {
            UnifiedEventPayload::Alert { .. } => EventEntrySourceKind::Alert,
            UnifiedEventPayload::ManualIssue { .. } => EventEntrySourceKind::ManualIssue,
            UnifiedEventPayload::ScheduledInspection { .. } => EventEntrySourceKind::ScheduledInspection,
            UnifiedEventPayload::ChangeEvent { .. } => EventEntrySourceKind::ChangeEvent,
            UnifiedEventPayload::ExternalIntegration { .. } => EventEntrySourceKind::ExternalIntegration,
        }
    }

    pub(super) const fn target_kind(&self) -> EventEntryTargetKind {
        match &self.payload {
            UnifiedEventPayload::Alert { .. } => EventEntryTargetKind::Incident,
            UnifiedEventPayload::ManualIssue { .. } => EventEntryTargetKind::Investigation,
            UnifiedEventPayload::ScheduledInspection { .. } => EventEntryTargetKind::InspectionRun,
            UnifiedEventPayload::ChangeEvent { target, .. }
            | UnifiedEventPayload::ExternalIntegration { target, .. } => match target {
                EventEntryWorkflowTarget::Investigation => EventEntryTargetKind::Investigation,
                EventEntryWorkflowTarget::Incident => EventEntryTargetKind::Incident,
            },
        }
    }

    pub(super) fn effective_occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at.unwrap_or_else(Utc::now)
    }

    pub(super) fn request_hash(&self) -> Result<String, ControlPlaneError> {
        let canonical = serde_jcs::to_vec(self).map_err(|_| {
            ControlPlaneError::validation("invalid_request", "event entry request cannot be canonicalized")
        })?;
        Ok(format!("sha256:{:x}", Sha256::digest(canonical)))
    }

    pub(super) fn alert_request(&self) -> Option<IntegrationEventRequest> {
        let UnifiedEventPayload::Alert {
            source,
            source_event_id,
            resource_kind,
            resource_key,
            display_name,
            symptom_family,
            severity,
            status,
            summary,
            labels,
            evidence_ids,
            sequence,
        } = &self.payload
        else {
            return None;
        };
        Some(IntegrationEventRequest {
            cluster_id: self.cluster_id,
            source: *source,
            source_event_id: source_event_id.clone(),
            resource_kind: *resource_kind,
            resource_key: resource_key.clone(),
            display_name: display_name.clone(),
            symptom_family: symptom_family.clone(),
            severity: *severity,
            status: *status,
            summary: summary.clone(),
            labels: labels.clone(),
            evidence_ids: evidence_ids.clone(),
            sequence: *sequence,
            occurred_at: self.effective_occurred_at(),
        })
    }
}

fn validate_idempotency_key(value: &str) -> Result<(), ControlPlaneError> {
    let valid = (1..=256).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'#'));
    if !valid {
        return Err(ControlPlaneError::validation(
            "invalid_idempotency_key",
            "event entry idempotency key must contain 1 to 256 allowlisted ASCII characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(payload: UnifiedEventPayload) -> UnifiedEventEntryRequest {
        UnifiedEventEntryRequest {
            schema_version: EVENT_ENTRY_SCHEMA.to_owned(),
            cluster_id: ClusterId::new(),
            idempotency_key: "entry:fixture:2026-07-30".to_owned(),
            occurred_at: Some(Utc::now()),
            payload,
        }
    }

    #[test]
    fn all_five_sources_have_a_fixed_workflow_target() {
        let alert = request(UnifiedEventPayload::Alert {
            source: AlertSource::HealthProbe,
            source_event_id: "broker-down-1".to_owned(),
            resource_kind: ResourceKind::Broker,
            resource_key: "broker-a".to_owned(),
            display_name: None,
            symptom_family: "broker_unavailable".to_owned(),
            severity: AlertSeverity::Critical,
            status: AlertStatus::Firing,
            summary: "Broker unavailable".to_owned(),
            labels: BTreeMap::new(),
            evidence_ids: Vec::new(),
            sequence: 1,
        });
        let manual = request(UnifiedEventPayload::ManualIssue {
            title: "Investigate intermittent consumer lag".to_owned(),
            resource: Some("consumer-group:orders".to_owned()),
            symptom_family: "consumer_lag".to_owned(),
        });
        let inspection = request(UnifiedEventPayload::ScheduledInspection {
            template: InspectionTemplate::ClusterHealth,
            schedule: Some("every 15m".to_owned()),
        });
        let change = request(UnifiedEventPayload::ChangeEvent {
            change_kind: ChangeEventKind::Release,
            target: EventEntryWorkflowTarget::Investigation,
            title: "Release readiness observation".to_owned(),
            resource: Some("release:2026.07".to_owned()),
            symptom_family: "release_change".to_owned(),
        });
        let external = request(UnifiedEventPayload::ExternalIntegration {
            channel: ExternalEventChannel::Itsm,
            target: EventEntryWorkflowTarget::Incident,
            title: "ITSM incident RMQ-42".to_owned(),
            resource: Some("broker:broker-a".to_owned()),
            symptom_family: "broker_unavailable".to_owned(),
        });

        for value in [&alert, &manual, &inspection, &change, &external] {
            value.validate().expect("representative event entry must validate");
        }
        assert_eq!(alert.target_kind(), EventEntryTargetKind::Incident);
        assert_eq!(manual.target_kind(), EventEntryTargetKind::Investigation);
        assert_eq!(inspection.target_kind(), EventEntryTargetKind::InspectionRun);
        assert_eq!(change.target_kind(), EventEntryTargetKind::Investigation);
        assert_eq!(external.target_kind(), EventEntryTargetKind::Incident);
    }

    #[test]
    fn request_hash_is_canonical_and_sensitive_to_payload_changes() {
        let mut labels = BTreeMap::new();
        labels.insert("zone".to_owned(), "cn-east-1".to_owned());
        labels.insert("owner".to_owned(), "messaging".to_owned());
        let first = request(UnifiedEventPayload::Alert {
            source: AlertSource::Alertmanager,
            source_event_id: "alert-42".to_owned(),
            resource_kind: ResourceKind::Broker,
            resource_key: "broker-a".to_owned(),
            display_name: None,
            symptom_family: "broker_unavailable".to_owned(),
            severity: AlertSeverity::Error,
            status: AlertStatus::Firing,
            summary: "Broker unavailable".to_owned(),
            labels,
            evidence_ids: Vec::new(),
            sequence: 42,
        });
        let encoded = serde_json::to_value(&first).expect("request JSON");
        let replay: UnifiedEventEntryRequest = serde_json::from_value(encoded).expect("request round trip");
        assert_eq!(
            first.request_hash().expect("first hash"),
            replay.request_hash().expect("replay hash")
        );

        let mut changed = replay;
        if let UnifiedEventPayload::Alert { sequence, .. } = &mut changed.payload {
            *sequence += 1;
        }
        assert_ne!(
            first.request_hash().expect("first hash"),
            changed.request_hash().expect("changed hash")
        );
    }

    #[test]
    fn unknown_schema_and_unsafe_idempotency_key_fail_closed() {
        let mut invalid = request(UnifiedEventPayload::ManualIssue {
            title: "Manual investigation".to_owned(),
            resource: None,
            symptom_family: "unknown_health".to_owned(),
        });
        invalid.schema_version = "rocketmq-sre.event-entry.v2".to_owned();
        assert!(invalid.validate().is_err());

        invalid.schema_version = EVENT_ENTRY_SCHEMA.to_owned();
        invalid.idempotency_key = "contains whitespace".to_owned();
        assert!(invalid.validate().is_err());
    }
}
