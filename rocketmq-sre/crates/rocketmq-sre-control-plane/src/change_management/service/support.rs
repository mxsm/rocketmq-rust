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

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::AuditEvent;
use rocketmq_sre_contracts::AuditEventId;
use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleStatus;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookStepBody;
use rocketmq_sre_contracts::TenantId;
use uuid::Uuid;

use super::super::model::ScheduleEvent;
use crate::ControlPlaneError;
use crate::auth::AuthContext;

const MAX_PAGE_SIZE: u32 = 256;
const MAX_SCHEDULE_HORIZON_DAYS: i64 = 90;
const MAX_SCHEDULE_DURATION_DAYS: i64 = 7;

pub(super) fn runbook_resources(definition: &RunbookDefinition) -> BTreeSet<String> {
    definition
        .steps
        .iter()
        .filter_map(|step| match &step.body {
            RunbookStepBody::Action { resource, .. } => Some(resource.clone()),
            RunbookStepBody::ManualGate { .. } => None,
        })
        .collect()
}

pub(super) fn effective_parallelism(
    definition: &RunbookDefinition,
    schedule: &ChangeSchedule,
    windows: &[ChangeWindow],
) -> u16 {
    windows
        .iter()
        .filter(|window| {
            window.kind == rocketmq_sre_contracts::ChangeWindowKind::Maintenance
                && window.starts_at <= schedule.scheduled_start
                && window.ends_at >= schedule.scheduled_end
                && (window.resource_keys.is_empty() || schedule.resource_keys.is_subset(&window.resource_keys))
        })
        .map(|window| window.max_parallelism)
        .min()
        .unwrap_or(1)
        .min(definition.max_parallelism)
}

pub(super) fn validate_schedule_window(
    now: DateTime<Utc>,
    starts_at: DateTime<Utc>,
    ends_at: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    if starts_at < now
        || ends_at <= starts_at
        || starts_at > now + Duration::days(MAX_SCHEDULE_HORIZON_DAYS)
        || ends_at - starts_at > Duration::days(MAX_SCHEDULE_DURATION_DAYS)
    {
        return Err(ControlPlaneError::validation(
            "invalid_change_schedule",
            "schedule must start within 90 days and have a positive duration no longer than 7 days",
        ));
    }
    Ok(())
}

pub(super) fn validate_version(value: &str) -> Result<(), ControlPlaneError> {
    rocketmq_sre_contracts::DescriptorVersion::parse(value)
        .map(|_| ())
        .map_err(|_| ControlPlaneError::validation("invalid_runbook_version", "runbook version must be semantic"))
}

pub(super) fn bounded_limit(limit: Option<u32>) -> Result<(i64, usize), ControlPlaneError> {
    let page_limit = limit.unwrap_or(100);
    if page_limit == 0 || page_limit > MAX_PAGE_SIZE {
        return Err(ControlPlaneError::validation(
            "invalid_limit",
            "page limit must be between 1 and 256",
        ));
    }
    let query_limit = i64::from(page_limit) + 1;
    let page_limit = usize::try_from(page_limit)
        .map_err(|_| ControlPlaneError::validation("invalid_limit", "page limit is unsupported on this platform"))?;
    Ok((query_limit, page_limit))
}

pub(super) fn require_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if auth.clusters.contains(&cluster_id) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "cluster is outside the authenticated scope",
        ))
    }
}

pub(super) fn require_role(auth: &AuthContext, role: &str) -> Result<(), ControlPlaneError> {
    if auth.roles.contains(role) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            format!("{role} role is required"),
        ))
    }
}

pub(super) fn validate_reason(reason: &str) -> Result<(), ControlPlaneError> {
    let trimmed = reason.trim();
    if trimmed.is_empty() || trimmed.chars().count() > 2048 || trimmed.chars().any(char::is_control) {
        return Err(ControlPlaneError::validation(
            "invalid_reason",
            "reason must contain 1 to 2048 visible characters",
        ));
    }
    let normalized = trimmed.to_ascii_lowercase();
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
            "reason contains prohibited sensitive material",
        ));
    }
    Ok(())
}

pub(in crate::change_management) fn next_timestamp(previous: DateTime<Utc>, now: DateTime<Utc>) -> DateTime<Utc> {
    if now > previous {
        now
    } else {
        previous + Duration::microseconds(1)
    }
}

pub(in crate::change_management) fn schedule_event(
    schedule: &ChangeSchedule,
    from_status: Option<ChangeScheduleStatus>,
    reason_code: impl Into<String>,
    actor_subject: &str,
    details: serde_json::Value,
) -> ScheduleEvent {
    ScheduleEvent {
        id: Uuid::new_v4(),
        schedule_id: schedule.id,
        correlation_id: schedule.correlation_id,
        from_status,
        to_status: schedule.status,
        reason_code: reason_code.into(),
        actor_subject: actor_subject.to_owned(),
        details,
        occurred_at: schedule.updated_at,
    }
}

pub(in crate::change_management) fn audit_event(
    auth: &AuthContext,
    cluster_id: ClusterId,
    correlation_id: CorrelationId,
    event_kind: AuditEventKind,
    actor_role: &str,
    resource_kind: &str,
    resource_id: String,
    reason_code: impl Into<String>,
    details: serde_json::Value,
    occurred_at: DateTime<Utc>,
) -> AuditEvent {
    AuditEvent {
        id: AuditEventId::new(),
        tenant_id: auth.tenant_id,
        cluster_id,
        correlation_id,
        event_kind,
        actor_subject: auth.subject.clone(),
        actor_role: actor_role.to_owned(),
        resource_kind: resource_kind.to_owned(),
        resource_id,
        reason_code: reason_code.into(),
        details,
        occurred_at,
    }
}

pub(in crate::change_management) fn scheduler_auth(tenant_id: TenantId, cluster_id: ClusterId) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "rocketmq-sre-change-scheduler".to_owned(),
        clusters: [cluster_id].into_iter().collect(),
        roles: ["operator".to_owned()].into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::validate_reason;

    #[test]
    fn audit_reason_rejects_sensitive_material() {
        assert!(validate_reason("operator confirmed the maintenance window").is_ok());
        for reason in [
            "token=abc",
            "SECRET=value",
            "password=hunter2",
            "Authorization: Bearer hidden",
            "contains a PRIVATE KEY",
            "copied the message body",
        ] {
            assert!(validate_reason(reason).is_err(), "{reason}");
        }
    }
}
