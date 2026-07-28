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

use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::DescriptorStatus;
use rocketmq_sre_contracts::DescriptorVersion;
use rocketmq_sre_contracts::ExternalApprovalInput;
use rocketmq_sre_contracts::INTEGRATION_DELIVERY_SCHEMA_VERSION;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationDelivery;
use rocketmq_sre_contracts::IntegrationDescriptor;
use rocketmq_sre_contracts::IntegrationEventKind;
use rocketmq_sre_contracts::IntegrationTarget;
use rocketmq_sre_contracts::SreTimestamp;
use rocketmq_sre_contracts::is_sha256_digest;

/// Fail-closed integration contract error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntegrationError {
    InvalidDescriptor(String),
    InvalidTarget(String),
    InvalidDelivery(String),
    InvalidApproval(String),
    SensitiveDataRejected,
    ScopeMismatch,
}

impl fmt::Display for IntegrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDescriptor(reason) => write!(formatter, "invalid integration descriptor: {reason}"),
            Self::InvalidTarget(reason) => write!(formatter, "invalid integration target: {reason}"),
            Self::InvalidDelivery(reason) => write!(formatter, "invalid integration delivery: {reason}"),
            Self::InvalidApproval(reason) => write!(formatter, "invalid external approval: {reason}"),
            Self::SensitiveDataRejected => formatter.write_str("sensitive integration content was rejected"),
            Self::ScopeMismatch => formatter.write_str("integration tenant or cluster scope does not match"),
        }
    }
}

impl Error for IntegrationError {}

/// Validates external integration descriptors and their bounded wire records.
pub struct IntegrationValidator;

impl IntegrationValidator {
    /// Validates a tenant-scoped target against the exact descriptor version.
    ///
    /// # Errors
    ///
    /// Rejects disabled, mismatched, unversioned, unscoped, credential-bearing,
    /// or unsupported adapter configurations.
    pub fn validate_target(
        target: &IntegrationTarget,
        descriptor: &IntegrationDescriptor,
    ) -> Result<(), IntegrationError> {
        if target.id.as_uuid().is_nil()
            || target.tenant_id.as_uuid().is_nil()
            || target.descriptor_id != descriptor.id
            || target.descriptor_version != descriptor.version
            || DescriptorVersion::parse(&descriptor.version).is_err()
            || descriptor.status != DescriptorStatus::Active
            || !target.enabled
            || target.name.trim().is_empty()
            || target.name.chars().count() > 128
            || adapter_kind_name(target.adapter_kind) != descriptor.integration_kind
            || target.updated_at < target.created_at
        {
            return Err(IntegrationError::InvalidTarget(
                "identity, descriptor version, lifecycle, name, or adapter kind is invalid".to_owned(),
            ));
        }
        if target.inbound_approval && !descriptor.inbound {
            return Err(IntegrationError::InvalidDescriptor(
                "target enables inbound approval but descriptor is outbound-only".to_owned(),
            ));
        }
        if !target.outbound_events.is_empty() && !descriptor.outbound {
            return Err(IntegrationError::InvalidDescriptor(
                "target enables outbound events but descriptor is inbound-only".to_owned(),
            ));
        }
        if descriptor.outbound && target.outbound_events.is_empty() {
            return Err(IntegrationError::InvalidTarget(
                "outbound integration requires at least one allowlisted event".to_owned(),
            ));
        }
        validate_endpoint(target.adapter_kind, &target.endpoint)?;
        if let Some(reference) = target.secret_reference.as_deref() {
            validate_secret_reference(reference)?;
        } else if !matches!(target.adapter_kind, IntegrationAdapterKind::MockItsm) {
            return Err(IntegrationError::InvalidTarget(
                "non-mock integration requires a secret reference".to_owned(),
            ));
        }
        Ok(())
    }

    /// Validates one delivery against its target and descriptor.
    ///
    /// # Errors
    ///
    /// Rejects schema, version, scope, event, idempotency, deep-link, or
    /// redaction mismatches.
    pub fn validate_delivery(
        delivery: &IntegrationDelivery,
        target: &IntegrationTarget,
        descriptor: &IntegrationDescriptor,
    ) -> Result<(), IntegrationError> {
        Self::validate_target(target, descriptor)?;
        if delivery.schema_version != INTEGRATION_DELIVERY_SCHEMA_VERSION
            || delivery.id.as_uuid().is_nil()
            || delivery.target_id != target.id
            || delivery.descriptor_id != descriptor.id
            || delivery.descriptor_version != descriptor.version
            || delivery.tenant_id != target.tenant_id
            || target
                .cluster_id
                .is_some_and(|cluster_id| delivery.cluster_id != cluster_id)
        {
            return Err(IntegrationError::ScopeMismatch);
        }
        if !target.outbound_events.contains(&delivery.event_kind) {
            return Err(IntegrationError::InvalidDelivery(
                "event kind is not enabled for this target".to_owned(),
            ));
        }
        if delivery.idempotency_key.trim().is_empty()
            || delivery.idempotency_key.chars().count() > 256
            || delivery.idempotency_key.chars().any(char::is_control)
            || delivery.sanitized_summary.trim().is_empty()
            || delivery.sanitized_summary.chars().count() > 2_048
            || delivery.deep_link.trim().is_empty()
            || delivery.deep_link.chars().count() > 2_048
            || !delivery.deep_link.starts_with('/')
            || delivery.deep_link.starts_with("//")
        {
            return Err(IntegrationError::InvalidDelivery(
                "delivery key, summary, or SRE deep link is invalid".to_owned(),
            ));
        }
        reject_sensitive(&delivery.sanitized_summary)?;
        reject_sensitive(&delivery.deep_link)?;
        if is_release_event(delivery.event_kind) && delivery.release_id.is_none() {
            return Err(IntegrationError::InvalidDelivery(
                "release event requires a release identifier".to_owned(),
            ));
        }
        Ok(())
    }

    /// Validates an inbound approval before it enters the normal approval
    /// service.
    ///
    /// # Errors
    ///
    /// Rejects unscoped targets, stale/malformed input, missing approver role,
    /// absent MFA/step-up, or plan-hash drift.
    pub fn validate_external_approval(
        input: &ExternalApprovalInput,
        target: &IntegrationTarget,
        descriptor: &IntegrationDescriptor,
        expected_plan_hash: &str,
        now: SreTimestamp,
    ) -> Result<(), IntegrationError> {
        Self::validate_target(target, descriptor)?;
        if !target.inbound_approval || !descriptor.inbound {
            return Err(IntegrationError::InvalidApproval(
                "integration is not enabled for inbound approvals".to_owned(),
            ));
        }
        let approval_valid_for = input.expires_at.signed_duration_since(now).num_seconds();
        let occurred_ahead_by = input.occurred_at.signed_duration_since(now).num_seconds();
        if input.schema_version != "rocketmq-sre.external-approval.v1"
            || input.target_id != target.id
            || input.plan_id.as_uuid().is_nil()
            || input.external_event_id.trim().is_empty()
            || input.external_event_id.chars().count() > 256
            || input.external_ticket_key.trim().is_empty()
            || input.external_ticket_key.chars().count() > 256
            || input.subject.trim().is_empty()
            || input.subject.chars().count() > 256
            || input.plan_hash != expected_plan_hash
            || !is_sha256_digest(&input.plan_hash)
            || !input.roles.contains("approver")
            || !input.mfa_verified
            || !input.step_up_verified
            || !(1..=86_400).contains(&approval_valid_for)
            || occurred_ahead_by > 300
        {
            return Err(IntegrationError::InvalidApproval(
                "schema, identity, role, step-up, plan hash, or expiry is invalid".to_owned(),
            ));
        }
        reject_sensitive(&input.external_ticket_key)?;
        reject_sensitive(&input.subject)?;
        Ok(())
    }
}

const fn adapter_kind_name(kind: IntegrationAdapterKind) -> &'static str {
    match kind {
        IntegrationAdapterKind::MockItsm => "mock_itsm",
        IntegrationAdapterKind::SignedWebhookItsm => "signed_webhook_itsm",
        IntegrationAdapterKind::ChatOpsWebhook => "chatops_webhook",
        IntegrationAdapterKind::Pager => "pager",
        IntegrationAdapterKind::Email => "email",
    }
}

fn validate_endpoint(kind: IntegrationAdapterKind, endpoint: &str) -> Result<(), IntegrationError> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty()
        || endpoint.chars().count() > 2_048
        || endpoint.contains('@')
        || endpoint.contains('#')
        || endpoint.chars().any(char::is_control)
    {
        return Err(IntegrationError::InvalidTarget(
            "endpoint is empty, credential-bearing, fragmented, or too large".to_owned(),
        ));
    }
    match kind {
        IntegrationAdapterKind::MockItsm if endpoint.starts_with("mock://") => Ok(()),
        IntegrationAdapterKind::SignedWebhookItsm
        | IntegrationAdapterKind::ChatOpsWebhook
        | IntegrationAdapterKind::Pager
        | IntegrationAdapterKind::Email
            if endpoint.starts_with("https://")
                || endpoint.starts_with("http://127.0.0.1/")
                || endpoint.starts_with("http://127.0.0.1:")
                || endpoint.starts_with("http://localhost/")
                || endpoint.starts_with("http://localhost:") =>
        {
            Ok(())
        }
        _ => Err(IntegrationError::InvalidTarget(
            "adapter endpoint scheme or host is not allowed".to_owned(),
        )),
    }
}

fn validate_secret_reference(reference: &str) -> Result<(), IntegrationError> {
    let Some(name) = reference.strip_prefix("env:") else {
        return Err(IntegrationError::InvalidTarget(
            "only env secret references are supported".to_owned(),
        ));
    };
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(IntegrationError::InvalidTarget(
            "secret reference name is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn reject_sensitive(value: &str) -> Result<(), IntegrationError> {
    let normalized = value.to_ascii_lowercase();
    if [
        "token=",
        "secret=",
        "password=",
        "authorization:",
        "private key",
        "message body",
        "message_body",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return Err(IntegrationError::SensitiveDataRejected);
    }
    Ok(())
}

const fn is_release_event(event: IntegrationEventKind) -> bool {
    matches!(
        event,
        IntegrationEventKind::ReleaseStarted
            | IntegrationEventKind::ReleasePaused
            | IntegrationEventKind::ReleaseRollingBack
            | IntegrationEventKind::ReleaseCompleted
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::Duration;
    use chrono::Utc;
    use rocketmq_sre_contracts::ActionPlanId;
    use rocketmq_sre_contracts::ApprovalDecision;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::IntegrationDeliveryId;
    use rocketmq_sre_contracts::IntegrationDeliveryStatus;
    use rocketmq_sre_contracts::IntegrationTargetId;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_contracts::TenantId;
    use serde_json::json;

    use super::*;

    fn descriptor() -> IntegrationDescriptor {
        IntegrationDescriptor {
            id: "itsm.mock.v1".to_owned(),
            version: "1.0.0".to_owned(),
            owner: "sre".to_owned(),
            supported_versions: vec![SchemaVersion::new("rocketmq-sre.integration-delivery", 1, 0)],
            required_capabilities: BTreeSet::new(),
            config_schema: json!({"type": "object"}),
            status: DescriptorStatus::Active,
            deprecation: None,
            integration_kind: "mock_itsm".to_owned(),
            inbound: true,
            outbound: true,
        }
    }

    fn target() -> IntegrationTarget {
        IntegrationTarget {
            id: IntegrationTargetId::new(),
            tenant_id: TenantId::new(),
            cluster_id: Some(ClusterId::new()),
            descriptor_id: "itsm.mock.v1".to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            name: "Mock ITSM".to_owned(),
            adapter_kind: IntegrationAdapterKind::MockItsm,
            endpoint: "mock://itsm/change".to_owned(),
            secret_reference: None,
            enabled: true,
            inbound_approval: true,
            outbound_events: BTreeSet::from([IntegrationEventKind::PlanSubmitted]),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn delivery_rejects_sensitive_summary_and_unregistered_event() {
        let descriptor = descriptor();
        let target = target();
        let delivery = IntegrationDelivery {
            schema_version: INTEGRATION_DELIVERY_SCHEMA_VERSION.to_owned(),
            id: IntegrationDeliveryId::new(),
            target_id: target.id,
            descriptor_id: descriptor.id.clone(),
            descriptor_version: descriptor.version.clone(),
            tenant_id: target.tenant_id,
            cluster_id: target.cluster_id.expect("cluster"),
            incident_id: rocketmq_sre_contracts::IncidentId::new(),
            plan_id: Some(ActionPlanId::new()),
            release_id: None,
            event_kind: IntegrationEventKind::PlanSubmitted,
            idempotency_key: "plan-submitted:fixture".to_owned(),
            sanitized_summary: "R2 Proxy canary plan awaiting approval".to_owned(),
            deep_link: "/changes/plans/fixture".to_owned(),
            status: IntegrationDeliveryStatus::Pending,
            attempt_count: 0,
            next_attempt_at: None,
            last_error_code: None,
            delivered_at: None,
            created_at: Utc::now(),
        };
        assert!(IntegrationValidator::validate_delivery(&delivery, &target, &descriptor).is_ok());

        let sensitive = IntegrationDelivery {
            sanitized_summary: "token=must-not-leave".to_owned(),
            ..delivery.clone()
        };
        assert_eq!(
            IntegrationValidator::validate_delivery(&sensitive, &target, &descriptor),
            Err(IntegrationError::SensitiveDataRejected)
        );

        let unsupported = IntegrationDelivery {
            event_kind: IntegrationEventKind::ReleaseStarted,
            release_id: Some(rocketmq_sre_contracts::ReleaseId::new()),
            ..delivery
        };
        assert!(IntegrationValidator::validate_delivery(&unsupported, &target, &descriptor).is_err());
    }

    #[test]
    fn external_approval_requires_step_up_and_exact_plan_hash() {
        let descriptor = descriptor();
        let target = target();
        let now = Utc::now();
        let hash = format!("sha256:{}", "a".repeat(64));
        let input = ExternalApprovalInput {
            schema_version: "rocketmq-sre.external-approval.v1".to_owned(),
            target_id: target.id,
            external_event_id: "event-1".to_owned(),
            external_ticket_key: "CHG-1001".to_owned(),
            plan_id: ActionPlanId::new(),
            plan_hash: hash.clone(),
            decision: ApprovalDecision::Approved,
            subject: "external-approver".to_owned(),
            roles: BTreeSet::from(["approver".to_owned()]),
            mfa_verified: true,
            step_up_verified: true,
            expires_at: now + Duration::hours(1),
            occurred_at: now,
        };
        assert!(IntegrationValidator::validate_external_approval(&input, &target, &descriptor, &hash, now).is_ok());
        let no_step_up = ExternalApprovalInput {
            step_up_verified: false,
            ..input
        };
        assert!(
            IntegrationValidator::validate_external_approval(&no_step_up, &target, &descriptor, &hash, now).is_err()
        );
    }
}
