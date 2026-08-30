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
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::EnterpriseIntegrationEvent;
use rocketmq_sre_contracts::EnterpriseIntegrationEventId;
use rocketmq_sre_contracts::EnterpriseIntegrationEventKind;
use rocketmq_sre_contracts::EnterpriseIntegrationPayload;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use rocketmq_sre_contracts::IntegrationHealth;
use rocketmq_sre_contracts::IntegrationHealthStatus;
use rocketmq_sre_contracts::IntegrationTargetId;
use rocketmq_sre_contracts::canonical_sha256;
use rocketmq_sre_contracts::is_sha256_digest;
use uuid::Uuid;

use super::ReleaseManagementService;
use super::integration::bounded_page_size;
use super::support::reject_sensitive;
use super::support::require_cluster;
use super::support::require_operator;
use super::support::validate_bounded_text;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::release_management::descriptors::resolve_descriptor;
use crate::release_management::model::EnterpriseEventListQuery;
use crate::release_management::model::EnterpriseEventPage;
use crate::release_management::model::EnterpriseIngressAuthorization;
use crate::release_management::model::EnterpriseIngressRequest;
use crate::release_management::model::EnterpriseIngressView;
use crate::release_management::model::IntegrationHealthView;
use crate::release_management::secret_provider::hmac_sha256;
use crate::release_management::secret_provider::signature_matches;
use crate::release_management::secret_provider::valid_secret_reference;

const SIGNATURE_WINDOW_SECONDS: i64 = 300;

impl ReleaseManagementService {
    pub(in crate::release_management) async fn ingest_enterprise_event(
        &self,
        auth: &AuthContext,
        target_id: IntegrationTargetId,
        authorization: &EnterpriseIngressAuthorization,
        request: &EnterpriseIngressRequest,
    ) -> Result<EnterpriseIngressView, ControlPlaneError> {
        let target = self.integration_target(auth, target_id).await?;
        let cluster_id = target.target.cluster_id.ok_or(ControlPlaneError::NotFound)?;
        require_cluster(auth, cluster_id)?;
        if !target.target.enabled {
            return Err(ControlPlaneError::conflict_code(
                "integration_target_disabled",
                "integration target is disabled",
            ));
        }
        validate_enterprise_payload(request, cluster_id, target.target.adapter_kind)?;
        validate_bounded_text("external event id", &request.external_event_id, 256)?;
        validate_bounded_text("integration source version", &request.source_version, 128)?;
        reject_sensitive(&request.external_event_id)?;
        reject_sensitive(&request.source_version)?;
        validate_nonce(&authorization.nonce)?;
        let signed_at = DateTime::parse_from_rfc3339(authorization.timestamp.trim())
            .map_err(|_| {
                ControlPlaneError::validation(
                    "integration_signature_invalid",
                    "integration timestamp must be RFC 3339",
                )
            })?
            .with_timezone(&Utc);
        let now = self.now();
        if now.signed_duration_since(signed_at).abs() > Duration::seconds(SIGNATURE_WINDOW_SECONDS)
            || now.signed_duration_since(request.occurred_at).abs() > Duration::seconds(SIGNATURE_WINDOW_SECONDS)
        {
            return Err(ControlPlaneError::validation(
                "integration_event_expired",
                "integration event is outside the accepted signature window",
            ));
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
        let recent = self.repository.recent_enterprise_event_count(target_id).await?;
        if recent >= u64::from(descriptor.operational.rate_limit_per_minute) {
            return Err(ControlPlaneError::conflict_code(
                "integration_rate_limited",
                "integration target exceeded its bounded ingress rate",
            ));
        }
        let secret_reference = target.target.secret_reference.as_deref().ok_or_else(|| {
            ControlPlaneError::validation(
                "integration_secret_unavailable",
                "integration target does not have a secret reference",
            )
        })?;
        let secret = self.secrets.resolve(secret_reference).map_err(|_| {
            ControlPlaneError::conflict_code(
                "integration_secret_unavailable",
                "integration target secret is unavailable",
            )
        })?;
        let payload_digest = canonical_sha256(request).map_err(|_| {
            ControlPlaneError::validation(
                "integration_payload_invalid",
                "integration payload cannot be canonicalized",
            )
        })?;
        let signature_material = format!(
            "{}\n{}\n{}",
            authorization.timestamp.trim(),
            authorization.nonce,
            payload_digest
        );
        let expected_signature = hmac_sha256(&secret, signature_material.as_bytes()).map_err(|_| {
            ControlPlaneError::validation(
                "integration_signature_invalid",
                "integration signature cannot be verified",
            )
        })?;
        if !signature_matches(&expected_signature, &authorization.signature) {
            return Err(ControlPlaneError::forbidden(
                "integration_signature_invalid",
                "integration signature verification failed",
            ));
        }
        let event = EnterpriseIntegrationEvent {
            schema_version: ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION.to_owned(),
            id: EnterpriseIntegrationEventId::new(),
            target_id,
            tenant_id: auth.tenant_id,
            cluster_id,
            event_kind: request.event_kind,
            external_event_id: request.external_event_id.trim().to_owned(),
            source_version: request.source_version.trim().to_owned(),
            payload_digest,
            payload: request.payload.clone(),
            signature_verified: true,
            occurred_at: request.occurred_at,
            received_at: now,
        };
        let (event, duplicate, followup_id) = self
            .repository
            .store_enterprise_integration_event(&event, &authorization.nonce)
            .await?;
        Ok(EnterpriseIngressView {
            schema_version: ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION,
            event,
            duplicate,
            followup_id,
        })
    }

    pub(in crate::release_management) async fn enterprise_events(
        &self,
        auth: &AuthContext,
        target_id: IntegrationTargetId,
        query: &EnterpriseEventListQuery,
    ) -> Result<EnterpriseEventPage, ControlPlaneError> {
        self.integration_target(auth, target_id).await?;
        let limit = bounded_page_size(query.limit);
        let mut items = self
            .repository
            .enterprise_events(auth.tenant_id, target_id, query.event_kind, i64::from(limit + 1))
            .await?;
        let partial = items.len() > limit as usize;
        items.truncate(limit as usize);
        Ok(EnterpriseEventPage {
            schema_version: ENTERPRISE_INTEGRATION_EVENT_SCHEMA_VERSION,
            items,
            partial,
        })
    }

    pub(in crate::release_management) async fn test_integration_config(
        &self,
        auth: &AuthContext,
        target_id: IntegrationTargetId,
    ) -> Result<IntegrationHealthView, ControlPlaneError> {
        require_operator(auth)?;
        let target = self.integration_target(auth, target_id).await?;
        let descriptor = resolve_descriptor(
            &target.target.descriptor_id,
            &target.target.descriptor_version,
            target.target.adapter_kind,
        );
        let config_valid = descriptor.is_some();
        let endpoint_valid = valid_endpoint(&target.target.endpoint);
        let secret_available = match target.target.secret_reference.as_deref() {
            Some(reference) => valid_secret_reference(reference) && self.secrets.available(reference),
            None => descriptor
                .as_ref()
                .is_some_and(|descriptor| !descriptor.operational.secret_required),
        };
        let (last_delivery_at, delivery_error) = self.repository.latest_integration_delivery_health(target_id).await?;
        let status = if !target.target.enabled {
            IntegrationHealthStatus::Disabled
        } else if config_valid && endpoint_valid && secret_available {
            if delivery_error.is_some() {
                IntegrationHealthStatus::Degraded
            } else {
                IntegrationHealthStatus::Healthy
            }
        } else if config_valid {
            IntegrationHealthStatus::Degraded
        } else {
            IntegrationHealthStatus::Unavailable
        };
        let last_error_code = match (config_valid, endpoint_valid, secret_available) {
            (false, _, _) => Some("integration_descriptor_mismatch".to_owned()),
            (_, false, _) => Some("integration_endpoint_invalid".to_owned()),
            (_, _, false) => Some("integration_secret_unavailable".to_owned()),
            _ => delivery_error,
        };
        let health = IntegrationHealth {
            target_id,
            status,
            config_valid,
            secret_available,
            endpoint_valid,
            last_delivery_at,
            last_error_code,
            observed_at: self.now(),
        };
        self.repository.store_integration_health(&health).await?;
        Ok(IntegrationHealthView {
            schema_version: "rocketmq-sre.integration-health.v1",
            health,
        })
    }

    pub(in crate::release_management) async fn integration_health(
        &self,
        auth: &AuthContext,
        target_id: IntegrationTargetId,
    ) -> Result<IntegrationHealthView, ControlPlaneError> {
        self.integration_target(auth, target_id).await?;
        let health = self.repository.integration_health(auth.tenant_id, target_id).await?;
        Ok(IntegrationHealthView {
            schema_version: "rocketmq-sre.integration-health.v1",
            health,
        })
    }

    pub(in crate::release_management) async fn record_enterprise_followup(
        &self,
        auth: &AuthContext,
        event_id: EnterpriseIntegrationEventId,
        followup_id: Uuid,
    ) -> Result<(), ControlPlaneError> {
        self.repository
            .record_enterprise_followup(auth.tenant_id, event_id, followup_id)
            .await
    }
}

fn validate_enterprise_payload(
    request: &EnterpriseIngressRequest,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    adapter_kind: IntegrationAdapterKind,
) -> Result<(), ControlPlaneError> {
    if request.payload.cluster_id() != cluster_id || !event_matches_payload(request, adapter_kind) {
        return Err(ControlPlaneError::forbidden(
            "integration_scope_mismatch",
            "integration event kind, payload, adapter, and cluster scope must match",
        ));
    }
    match &request.payload {
        EnterpriseIntegrationPayload::Cmdb(payload) => {
            validate_bounded_text("CMDB owner", &payload.owner, 128)?;
            validate_bounded_text("CMDB environment", &payload.environment, 64)?;
            reject_sensitive(&payload.owner)?;
            reject_sensitive(&payload.environment)?;
            if payload.service_dependencies.len() > 64 || payload.labels.len() > 64 {
                return Err(invalid_payload("CMDB payload exceeds the bounded collection limit"));
            }
            for value in payload
                .service_dependencies
                .iter()
                .chain(payload.labels.keys())
                .chain(payload.labels.values())
            {
                validate_bounded_text("CMDB metadata", value, 256)?;
                reject_sensitive(value)?;
            }
        }
        EnterpriseIntegrationPayload::GitOps(payload) => {
            validate_bounded_text("GitOps repository reference", &payload.repository_ref, 512)?;
            reject_sensitive(&payload.repository_ref)?;
            if payload.commit_sha.len() < 7
                || payload.commit_sha.len() > 64
                || !payload.commit_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(invalid_payload("GitOps commit SHA is invalid"));
            }
            for digest in [
                payload.desired_image_digest.as_deref(),
                payload.configuration_digest.as_deref(),
                payload.feature_digest.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                if !is_sha256_digest(digest) {
                    return Err(invalid_payload("GitOps digest must be SHA-256"));
                }
            }
            if let Some(link) = payload.rollout_link.as_deref()
                && !valid_endpoint(link)
            {
                return Err(invalid_payload("GitOps rollout link is invalid"));
            }
        }
        EnterpriseIntegrationPayload::Release(payload) => {
            for (name, value, max) in [
                ("release reference", payload.release_ref.as_str(), 256),
                ("change id", payload.change_id.as_str(), 256),
                ("target version", payload.target_version.as_str(), 128),
            ] {
                validate_bounded_text(name, value, max)?;
                reject_sensitive(value)?;
            }
            if !is_sha256_digest(&payload.artifact_digest) {
                return Err(invalid_payload("release artifact digest must be SHA-256"));
            }
        }
    }
    Ok(())
}

fn event_matches_payload(request: &EnterpriseIngressRequest, adapter_kind: IntegrationAdapterKind) -> bool {
    matches!(
        (adapter_kind, request.event_kind, &request.payload),
        (
            IntegrationAdapterKind::MockCmdb,
            EnterpriseIntegrationEventKind::CmdbSnapshot,
            EnterpriseIntegrationPayload::Cmdb(_)
        ) | (
            IntegrationAdapterKind::MockGitOps,
            EnterpriseIntegrationEventKind::GitOpsSnapshot,
            EnterpriseIntegrationPayload::GitOps(_)
        ) | (
            IntegrationAdapterKind::SignedReleaseWebhook,
            EnterpriseIntegrationEventKind::ReleaseStarted
                | EnterpriseIntegrationEventKind::ReleaseCanary
                | EnterpriseIntegrationEventKind::ReleasePromoted
                | EnterpriseIntegrationEventKind::ReleaseRolledBack,
            EnterpriseIntegrationPayload::Release(_)
        )
    )
}

fn validate_nonce(nonce: &str) -> Result<(), ControlPlaneError> {
    if !(16..=128).contains(&nonce.len())
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(ControlPlaneError::validation(
            "integration_nonce_invalid",
            "integration nonce is invalid",
        ));
    }
    Ok(())
}

fn valid_endpoint(value: &str) -> bool {
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    if !url.username().is_empty() || url.password().is_some() || url.host_str().is_none() {
        return false;
    }
    url.scheme() == "https"
        || (url.scheme() == "http"
            && url
                .host_str()
                .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1")))
}

fn invalid_payload(message: &'static str) -> ControlPlaneError {
    ControlPlaneError::validation("integration_payload_invalid", message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CmdbSnapshot;

    use super::*;

    #[test]
    fn adapter_payload_matrix_and_bounds_fail_closed() {
        let cluster_id = ClusterId::new();
        let request = EnterpriseIngressRequest {
            event_kind: EnterpriseIntegrationEventKind::CmdbSnapshot,
            external_event_id: "cmdb-1".to_owned(),
            source_version: "1.0.0".to_owned(),
            occurred_at: Utc::now(),
            payload: EnterpriseIntegrationPayload::Cmdb(CmdbSnapshot {
                cluster_id,
                owner: "messaging-platform".to_owned(),
                environment: "production".to_owned(),
                service_dependencies: BTreeSet::from(["nameserver".to_owned()]),
                labels: BTreeMap::from([("tier".to_owned(), "messaging".to_owned())]),
            }),
        };
        validate_enterprise_payload(&request, cluster_id, IntegrationAdapterKind::MockCmdb)
            .expect("bounded CMDB payload");
        assert!(validate_enterprise_payload(&request, cluster_id, IntegrationAdapterKind::MockGitOps).is_err());
    }
}
