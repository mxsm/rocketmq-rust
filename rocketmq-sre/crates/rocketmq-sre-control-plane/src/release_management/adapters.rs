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

use std::time::Duration;

use hmac::Hmac;
use hmac::Mac;
use rocketmq_sre_contracts::IntegrationAdapterKind;
use serde::Serialize;
use sha2::Sha256;

use super::model::AdapterDeliveryReceipt;
use super::model::IntegrationDeliveryClaim;
use crate::ControlPlaneError;
use crate::PostgresRepository;

const MAX_ADAPTER_BODY_BYTES: usize = 8 * 1024;
const ITSM_TICKET_HEADER: &str = "x-itsm-ticket-key";

/// Bounded ITSM outbox worker. ChatOps, Pager, and Email deliveries remain on
/// the Phase 2 notification worker and are never claimed here.
#[derive(Clone)]
pub(crate) struct IntegrationOutboxWorker {
    repository: PostgresRepository,
    client: reqwest::Client,
}

impl IntegrationOutboxWorker {
    pub(crate) fn new(repository: PostgresRepository) -> Result<Self, ControlPlaneError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(8))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ControlPlaneError::configuration("integration HTTP client cannot be built"))?;
        Ok(Self { repository, client })
    }

    pub(crate) async fn run_due(&self) {
        let claims = match self.repository.claim_integration_deliveries(16).await {
            Ok(claims) => claims,
            Err(_error) => {
                tracing::warn!(error_class = "source_unavailable", "integration outbox claim failed");
                return;
            }
        };
        for claim in claims {
            let result = self.deliver(&claim).await;
            if let Err(_error) = self.repository.finish_integration_delivery(&claim, result).await {
                tracing::warn!(
                    delivery_id = %claim.delivery.id,
                    error_class = "source_unavailable",
                    "integration outbox completion could not be persisted"
                );
            }
        }
    }

    async fn deliver(&self, claim: &IntegrationDeliveryClaim) -> Result<AdapterDeliveryReceipt, &'static str> {
        match claim.adapter_kind {
            IntegrationAdapterKind::MockItsm => Ok(AdapterDeliveryReceipt {
                external_ticket_key: Some(mock_ticket_key(claim)),
            }),
            IntegrationAdapterKind::SignedWebhookItsm => self.deliver_signed_itsm(claim).await,
            IntegrationAdapterKind::ChatOpsWebhook | IntegrationAdapterKind::Pager | IntegrationAdapterKind::Email => {
                Err("adapter_boundary_mismatch")
            }
        }
    }

    async fn deliver_signed_itsm(
        &self,
        claim: &IntegrationDeliveryClaim,
    ) -> Result<AdapterDeliveryReceipt, &'static str> {
        let endpoint = url::Url::parse(&claim.endpoint).map_err(|_| "invalid_endpoint")?;
        if !allowed_endpoint(&endpoint) {
            return Err("endpoint_not_allowed");
        }
        let secret_reference = claim.secret_reference.as_deref().ok_or("secret_reference_missing")?;
        let secret = resolve_secret_reference(secret_reference)?;
        let payload = ItsmPayload {
            schema_version: "rocketmq-sre.itsm-delivery.v1",
            delivery_id: claim.delivery.id.to_string(),
            incident_id: claim.delivery.incident_id.to_string(),
            plan_id: claim.delivery.plan_id.map(|id| id.to_string()),
            release_id: claim.delivery.release_id.map(|id| id.to_string()),
            event_kind: claim.delivery.event_kind,
            summary: &claim.delivery.sanitized_summary,
            deep_link: &claim.delivery.deep_link,
        };
        let body = serde_json::to_vec(&payload).map_err(|_| "payload_encoding_failed")?;
        if body.len() > MAX_ADAPTER_BODY_BYTES {
            return Err("payload_too_large");
        }
        let signature = hmac_sha256(secret.as_bytes(), &body)?;
        let response = self
            .client
            .post(endpoint)
            .header("content-type", "application/json")
            .header("x-rocketmq-sre-delivery", claim.delivery.id.to_string())
            .header("x-rocketmq-sre-signature", format!("sha256={signature}"))
            .body(body)
            .send()
            .await
            .map_err(|_| "transport_unavailable")?
            .error_for_status()
            .map_err(|_| "remote_rejected")?;
        let ticket_key = response
            .headers()
            .get(ITSM_TICKET_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| valid_ticket_key(value))
            .ok_or("ticket_key_missing")?
            .to_owned();
        Ok(AdapterDeliveryReceipt {
            external_ticket_key: Some(ticket_key),
        })
    }
}

#[derive(Serialize)]
struct ItsmPayload<'a> {
    schema_version: &'static str,
    delivery_id: String,
    incident_id: String,
    plan_id: Option<String>,
    release_id: Option<String>,
    event_kind: rocketmq_sre_contracts::IntegrationEventKind,
    summary: &'a str,
    deep_link: &'a str,
}

fn mock_ticket_key(claim: &IntegrationDeliveryClaim) -> String {
    let compact = claim.delivery.id.to_string().replace('-', "");
    format!("CHG-{}", &compact[..12])
}

fn allowed_endpoint(endpoint: &url::Url) -> bool {
    if !endpoint.username().is_empty() || endpoint.password().is_some() || endpoint.host_str().is_none() {
        return false;
    }
    if endpoint.scheme() == "https" {
        return true;
    }
    endpoint.scheme() == "http"
        && endpoint
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"))
}

fn resolve_secret_reference(reference: &str) -> Result<String, &'static str> {
    let name = reference.strip_prefix("env:").ok_or("unsupported_secret_reference")?;
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err("invalid_secret_reference");
    }
    std::env::var(name)
        .ok()
        .filter(|secret| !secret.is_empty() && secret.len() <= 4_096)
        .ok_or("secret_unavailable")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<String, &'static str> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| "invalid_signature_key")?;
    mac.update(message);
    Ok(hex_lower(&mac.finalize().into_bytes()))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn valid_ticket_key(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::IncidentId;
    use rocketmq_sre_contracts::IntegrationDelivery;
    use rocketmq_sre_contracts::IntegrationDeliveryId;
    use rocketmq_sre_contracts::IntegrationDeliveryStatus;
    use rocketmq_sre_contracts::IntegrationEventKind;
    use rocketmq_sre_contracts::IntegrationTargetId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    fn claim() -> IntegrationDeliveryClaim {
        IntegrationDeliveryClaim {
            delivery: IntegrationDelivery {
                schema_version: "rocketmq-sre.integration-delivery.v1".to_owned(),
                id: IntegrationDeliveryId::new(),
                target_id: IntegrationTargetId::new(),
                descriptor_id: "rocketmq-sre.integration.mock-itsm.v1".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                tenant_id: TenantId::new(),
                cluster_id: ClusterId::new(),
                incident_id: IncidentId::new(),
                plan_id: None,
                release_id: None,
                event_kind: IntegrationEventKind::PlanSubmitted,
                idempotency_key: "plan:fixture".to_owned(),
                sanitized_summary: "Plan awaiting approval".to_owned(),
                deep_link: "/changes/plans/fixture".to_owned(),
                status: IntegrationDeliveryStatus::Delivering,
                attempt_count: 0,
                next_attempt_at: None,
                last_error_code: None,
                delivered_at: None,
                created_at: Utc::now(),
            },
            claim_token: uuid::Uuid::new_v4(),
            adapter_kind: IntegrationAdapterKind::MockItsm,
            endpoint: "mock://itsm/change".to_owned(),
            secret_reference: None,
        }
    }

    #[test]
    fn mock_ticket_is_deterministic_and_bounded() {
        let claim = claim();
        let first = mock_ticket_key(&claim);
        let second = mock_ticket_key(&claim);
        assert_eq!(first, second);
        assert!(valid_ticket_key(&first));
    }

    #[test]
    fn endpoint_and_ticket_validation_fail_closed() {
        assert!(allowed_endpoint(
            &url::Url::parse("https://itsm.example.test/change").expect("URL")
        ));
        assert!(allowed_endpoint(
            &url::Url::parse("http://127.0.0.1:9099/change").expect("URL")
        ));
        assert!(!allowed_endpoint(
            &url::Url::parse("http://itsm.example.test/change").expect("URL")
        ));
        assert!(!valid_ticket_key("CHG 1001"));
    }

    #[test]
    fn hmac_uses_sha256_and_lowercase_hex() {
        assert_eq!(
            hmac_sha256(&[0x0b; 20], b"Hi There").expect("HMAC"),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }
}
