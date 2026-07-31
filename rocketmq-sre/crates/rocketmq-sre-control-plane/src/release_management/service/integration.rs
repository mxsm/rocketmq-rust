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

use rocketmq_sre_contracts::AuditEventKind;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationDeliveryId;
use rocketmq_sre_contracts::IntegrationDescriptor;
use rocketmq_sre_contracts::IntegrationTarget;
use rocketmq_sre_contracts::IntegrationTargetId;
use rocketmq_sre_core::IntegrationValidator;
use serde_json::json;

use super::ReleaseManagementService;
use super::support::audit_event;
use super::support::reject_sensitive;
use super::support::require_approver;
use super::support::require_cluster;
use super::support::require_operator;
use super::support::validate_bounded_text;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::release_management::descriptors::descriptor_for;
use crate::release_management::descriptors::resolve_descriptor;
use crate::release_management::model::ExternalApprovalRequest;
use crate::release_management::model::ExternalApprovalView;
use crate::release_management::model::IntegrationDeliveryListQuery;
use crate::release_management::model::IntegrationDeliveryPage;
use crate::release_management::model::IntegrationTargetListQuery;
use crate::release_management::model::IntegrationTargetPage;
use crate::release_management::model::IntegrationTargetView;
use crate::release_management::model::RegisterIntegrationTargetRequest;
use crate::release_management::model::ReplayIntegrationDeliveryRequest;
use crate::release_management::model::ReplayIntegrationDeliveryView;
use crate::release_management::model::RotateIntegrationSecretRequest;
use crate::release_management::model::SetIntegrationTargetStateRequest;
use crate::release_management::secret_provider::valid_secret_reference;
use crate::supervised_execution::ApprovalDecisionRequest;
use crate::supervised_execution::ExternalApprovalSource;

const DEFAULT_PAGE_SIZE: u32 = 50;
const MAX_PAGE_SIZE: u32 = 200;

impl ReleaseManagementService {
    pub(in crate::release_management) fn integration_descriptors() -> Vec<IntegrationDescriptor> {
        [
            IntegrationAdapterKind::MockItsm,
            IntegrationAdapterKind::SignedWebhookItsm,
            IntegrationAdapterKind::ChatOpsWebhook,
            IntegrationAdapterKind::Pager,
            IntegrationAdapterKind::Email,
            IntegrationAdapterKind::MockCmdb,
            IntegrationAdapterKind::MockGitOps,
            IntegrationAdapterKind::SignedReleaseWebhook,
        ]
        .into_iter()
        .map(descriptor_for)
        .collect()
    }

    pub(in crate::release_management) async fn register_integration_target(
        &self,
        auth: &AuthContext,
        request: &RegisterIntegrationTargetRequest,
        correlation_id: CorrelationId,
    ) -> Result<IntegrationTargetView, ControlPlaneError> {
        require_operator(auth)?;
        require_cluster(auth, request.cluster_id)?;
        validate_bounded_text("integration target name", &request.name, 128)?;
        validate_bounded_text("integration endpoint", &request.endpoint, 2_048)?;
        reject_sensitive(&request.name)?;
        reject_sensitive(&request.endpoint)?;
        if !request.enabled {
            return Err(ControlPlaneError::validation(
                "integration_target_disabled",
                "new integration targets must be enabled and may be disabled after registration",
            ));
        }
        let descriptor = resolve_descriptor(
            &request.descriptor_id,
            &request.descriptor_version,
            request.adapter_kind,
        )
        .ok_or_else(|| {
            ControlPlaneError::validation(
                "integration_descriptor_mismatch",
                "integration descriptor identity, version, or adapter kind is unsupported",
            )
        })?;
        let now = self.now();
        let view = IntegrationTargetView {
            target: IntegrationTarget {
                id: IntegrationTargetId::new(),
                tenant_id: auth.tenant_id,
                cluster_id: Some(request.cluster_id),
                descriptor_id: descriptor.id.clone(),
                descriptor_version: descriptor.version.clone(),
                name: request.name.trim().to_owned(),
                adapter_kind: request.adapter_kind,
                endpoint: request.endpoint.trim().to_owned(),
                secret_reference: request.secret_reference.clone(),
                enabled: true,
                inbound_approval: request.inbound_approval,
                outbound_events: request.outbound_events.clone(),
                created_at: now,
                updated_at: now,
            },
            notification_target_id: request.notification_target_id,
        };
        IntegrationValidator::validate_target(&view.target, &descriptor)
            .map_err(|error| ControlPlaneError::validation("integration_target_invalid", error.to_string()))?;
        let audit = audit_event(
            auth,
            request.cluster_id,
            correlation_id,
            AuditEventKind::IntegrationTargetRegistered,
            "integration_target",
            view.target.id.to_string(),
            "IntegrationTargetRegistered",
            json!({
                "descriptor_id": &view.target.descriptor_id,
                "descriptor_version": &view.target.descriptor_version,
                "adapter_kind": view.target.adapter_kind,
                "inbound_approval": view.target.inbound_approval,
                "outbound_events": &view.target.outbound_events,
                "notification_target_id": view.notification_target_id,
            }),
            now,
        );
        self.repository.insert_integration_target(&view, &audit).await?;
        Ok(view)
    }

    pub(in crate::release_management) async fn integration_target(
        &self,
        auth: &AuthContext,
        target_id: IntegrationTargetId,
    ) -> Result<IntegrationTargetView, ControlPlaneError> {
        let target = self.repository.integration_target(auth.tenant_id, target_id).await?;
        let cluster_id = target.target.cluster_id.ok_or_else(|| {
            ControlPlaneError::forbidden(
                "integration_scope_mismatch",
                "integration target does not have a cluster scope",
            )
        })?;
        require_cluster(auth, cluster_id)?;
        Ok(target)
    }

    pub(in crate::release_management) async fn integration_targets(
        &self,
        auth: &AuthContext,
        query: &IntegrationTargetListQuery,
    ) -> Result<IntegrationTargetPage, ControlPlaneError> {
        require_cluster(auth, query.cluster_id)?;
        let limit = bounded_page_size(query.limit);
        let mut items = self
            .repository
            .integration_targets(
                auth.tenant_id,
                query.cluster_id,
                query.adapter_kind,
                query.enabled,
                i64::from(limit + 1),
            )
            .await?;
        let partial = items.len() > limit as usize;
        items.truncate(limit as usize);
        Ok(IntegrationTargetPage {
            schema_version: "rocketmq-sre.integration-target-page.v1",
            items,
            partial,
        })
    }

    pub(in crate::release_management) async fn set_integration_target_state(
        &self,
        auth: &AuthContext,
        target_id: IntegrationTargetId,
        request: &SetIntegrationTargetStateRequest,
        correlation_id: CorrelationId,
    ) -> Result<IntegrationTargetView, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.integration_target(auth, target_id).await?;
        if current.target.enabled == request.enabled {
            return Ok(current);
        }
        if request.enabled {
            let descriptor = resolve_descriptor(
                &current.target.descriptor_id,
                &current.target.descriptor_version,
                current.target.adapter_kind,
            )
            .ok_or_else(|| {
                ControlPlaneError::conflict_code(
                    "integration_descriptor_mismatch",
                    "integration target references an unsupported descriptor version",
                )
            })?;
            let mut enabled = current.target.clone();
            enabled.enabled = true;
            IntegrationValidator::validate_target(&enabled, &descriptor)
                .map_err(|error| ControlPlaneError::validation("integration_target_invalid", error.to_string()))?;
        }
        let cluster_id = current.target.cluster_id.ok_or(ControlPlaneError::NotFound)?;
        let now = self.now();
        let audit = audit_event(
            auth,
            cluster_id,
            correlation_id,
            AuditEventKind::StateChanged,
            "integration_target",
            current.target.id.to_string(),
            if request.enabled {
                "IntegrationTargetEnabled"
            } else {
                "IntegrationTargetDisabled"
            },
            json!({
                "enabled": request.enabled,
                "descriptor_id": &current.target.descriptor_id,
                "descriptor_version": &current.target.descriptor_version,
            }),
            now,
        );
        self.repository
            .set_integration_target_state(&current, request.enabled, now, &audit)
            .await
    }

    pub(in crate::release_management) async fn rotate_integration_secret(
        &self,
        auth: &AuthContext,
        target_id: IntegrationTargetId,
        request: &RotateIntegrationSecretRequest,
        correlation_id: CorrelationId,
    ) -> Result<IntegrationTargetView, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.integration_target(auth, target_id).await?;
        if !valid_secret_reference(&request.secret_reference) {
            return Err(ControlPlaneError::validation(
                "integration_secret_reference_invalid",
                "integration secret reference is invalid",
            ));
        }
        if !self.secrets.available(&request.secret_reference) {
            return Err(ControlPlaneError::conflict_code(
                "integration_secret_unavailable",
                "rotated integration secret is unavailable",
            ));
        }
        let cluster_id = current.target.cluster_id.ok_or(ControlPlaneError::NotFound)?;
        let now = self.now();
        let audit = audit_event(
            auth,
            cluster_id,
            correlation_id,
            AuditEventKind::StateChanged,
            "integration_target",
            target_id.to_string(),
            "IntegrationSecretReferenceRotated",
            json!({
                "descriptor_id": &current.target.descriptor_id,
                "descriptor_version": &current.target.descriptor_version,
            }),
            now,
        );
        self.repository
            .rotate_integration_secret(&current, request.secret_reference.trim(), now, &audit)
            .await
    }

    pub(in crate::release_management) async fn integration_deliveries(
        &self,
        auth: &AuthContext,
        query: &IntegrationDeliveryListQuery,
    ) -> Result<IntegrationDeliveryPage, ControlPlaneError> {
        require_cluster(auth, query.cluster_id)?;
        if let Some(target_id) = query.target_id {
            let target = self.integration_target(auth, target_id).await?;
            if target.target.cluster_id != Some(query.cluster_id) {
                return Err(ControlPlaneError::forbidden(
                    "integration_scope_mismatch",
                    "integration target does not match the requested cluster",
                ));
            }
        }
        let limit = bounded_page_size(query.limit);
        let mut items = self
            .repository
            .integration_deliveries(auth.tenant_id, query.cluster_id, query.target_id, i64::from(limit + 1))
            .await?;
        let partial = items.len() > limit as usize;
        items.truncate(limit as usize);
        Ok(IntegrationDeliveryPage {
            schema_version: "rocketmq-sre.integration-delivery-page.v1",
            items,
            partial,
        })
    }

    pub(in crate::release_management) async fn replay_integration_delivery(
        &self,
        auth: &AuthContext,
        delivery_id: IntegrationDeliveryId,
        request: &ReplayIntegrationDeliveryRequest,
        correlation_id: CorrelationId,
    ) -> Result<ReplayIntegrationDeliveryView, ControlPlaneError> {
        require_operator(auth)?;
        validate_bounded_text("integration replay reason", &request.reason, 1_024)?;
        reject_sensitive(&request.reason)?;
        let delivery = self
            .repository
            .integration_delivery(auth.tenant_id, delivery_id)
            .await?;
        require_cluster(auth, delivery.cluster_id)?;
        let target = self.integration_target(auth, delivery.target_id).await?;
        if !target.target.enabled {
            return Err(ControlPlaneError::conflict_code(
                "integration_target_disabled",
                "disabled integration target cannot replay deliveries",
            ));
        }
        let audit = audit_event(
            auth,
            delivery.cluster_id,
            correlation_id,
            AuditEventKind::IntegrationDeliveryQueued,
            "integration_delivery",
            delivery.id.to_string(),
            "IntegrationDeliveryManualReplay",
            json!({
                "target_id": delivery.target_id,
                "event_kind": delivery.event_kind,
                "reason": request.reason.trim(),
            }),
            self.now(),
        );
        let delivery = self.repository.replay_integration_delivery(&delivery, &audit).await?;
        Ok(ReplayIntegrationDeliveryView {
            schema_version: "rocketmq-sre.integration-delivery.v1",
            delivery,
        })
    }

    pub(in crate::release_management) async fn apply_external_approval(
        &self,
        auth: &AuthContext,
        request: &ExternalApprovalRequest,
        correlation_id: CorrelationId,
    ) -> Result<ExternalApprovalView, ControlPlaneError> {
        require_approver(auth)?;
        let input = &request.input;
        if input.subject != auth.subject || !input.roles.is_subset(&auth.roles) {
            return Err(ControlPlaneError::forbidden(
                "external_approval_identity_mismatch",
                "external approval subject and roles must match the authenticated identity",
            ));
        }
        let target = self.integration_target(auth, input.target_id).await?;
        if let Some(existing) = self
            .repository
            .external_approval_result(auth.tenant_id, input.target_id, &input.external_event_id)
            .await?
        {
            validate_duplicate_approval(input, &existing)?;
            return Ok(existing);
        }
        let descriptor = resolve_descriptor(
            &target.target.descriptor_id,
            &target.target.descriptor_version,
            target.target.adapter_kind,
        )
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "integration_descriptor_mismatch",
                "integration target references an unsupported descriptor version",
            )
        })?;
        let plan = self.supervised.plan(auth, input.plan_id).await?;
        if target.target.cluster_id != Some(plan.plan.cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "integration_scope_mismatch",
                "integration target and action plan belong to different clusters",
            ));
        }
        let now = self.now();
        IntegrationValidator::validate_external_approval(input, &target.target, &descriptor, &plan.plan.plan_hash, now)
            .map_err(|error| ControlPlaneError::validation("external_approval_invalid", error.to_string()))?;
        let precondition_hash = plan
            .plan
            .compute_precondition_hash()
            .map_err(|error| ControlPlaneError::validation("invalid_precondition_hash", error.to_string()))?;
        let validity_seconds =
            u64::try_from(input.expires_at.signed_duration_since(now).num_seconds()).map_err(|_| {
                ControlPlaneError::validation("invalid_approval_window", "external approval expiry is invalid")
            })?;
        let decision_request = ApprovalDecisionRequest {
            plan_hash: input.plan_hash.clone(),
            precondition_hash,
            reason: format!("External approval received through {}", descriptor.id),
            validity_seconds: Some(validity_seconds),
        };
        let source = ExternalApprovalSource {
            target_id: input.target_id,
            input: input.clone(),
            received_at: now,
        };
        match self
            .supervised
            .decide_external(auth, input.plan_id, &decision_request, &source, correlation_id)
            .await
        {
            Ok(response) => Ok(ExternalApprovalView {
                schema_version: "rocketmq-sre.external-approval-result.v1",
                duplicate: false,
                approval: response.approval,
                plan_status: response.plan.status,
            }),
            Err(ControlPlaneError::Conflict {
                code: "external_approval_duplicate",
                ..
            }) => {
                let existing = self
                    .repository
                    .external_approval_result(auth.tenant_id, input.target_id, &input.external_event_id)
                    .await?
                    .ok_or_else(|| {
                        ControlPlaneError::conflict_code(
                            "external_approval_duplicate",
                            "external approval event was already applied",
                        )
                    })?;
                validate_duplicate_approval(input, &existing)?;
                Ok(existing)
            }
            Err(error) => Err(error),
        }
    }
}

fn validate_duplicate_approval(
    input: &rocketmq_sre_contracts::ExternalApprovalInput,
    existing: &ExternalApprovalView,
) -> Result<(), ControlPlaneError> {
    if existing.approval.plan_id != input.plan_id
        || existing.approval.plan_hash != input.plan_hash
        || existing.approval.approver_subject != input.subject
        || existing.approval.decision != input.decision
    {
        return Err(ControlPlaneError::conflict_code(
            "external_approval_event_mismatch",
            "external approval event identity was reused with different content",
        ));
    }
    Ok(())
}

pub(super) fn bounded_page_size(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, MAX_PAGE_SIZE)
}
