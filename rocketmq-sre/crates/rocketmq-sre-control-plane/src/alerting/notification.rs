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

use rocketmq_sre_contracts::NotificationChannel;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use super::model::NotificationClaim;
use crate::ControlPlaneError;
use crate::PostgresRepository;

const MAX_NOTIFICATION_BODY_BYTES: usize = 8 * 1024;

/// Bounded transactional-outbox worker. It owns no detached tasks and is
/// driven by the control plane's scheduled task group.
#[derive(Clone)]
pub(crate) struct NotificationOutboxWorker {
    repository: PostgresRepository,
    client: reqwest::Client,
}

impl NotificationOutboxWorker {
    pub(crate) fn new(repository: PostgresRepository) -> Result<Self, ControlPlaneError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(8))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ControlPlaneError::configuration("notification HTTP client cannot be built"))?;
        Ok(Self { repository, client })
    }

    pub(crate) async fn run_due(&self) {
        let claims = match self.repository.claim_notifications(16).await {
            Ok(claims) => claims,
            Err(_error) => {
                tracing::warn!(error_class = "source_unavailable", "notification outbox claim failed");
                return;
            }
        };
        for claim in claims {
            let result = self.deliver(&claim).await;
            if let Err(_error) = self.repository.finish_notification(&claim, result).await {
                tracing::warn!(
                    delivery_id = %claim.delivery_id,
                    error_class = "source_unavailable",
                    "notification outbox completion could not be persisted"
                );
            }
        }
    }

    async fn deliver(&self, claim: &NotificationClaim) -> Result<(), &'static str> {
        match claim.channel {
            NotificationChannel::SignedWebhook => self.deliver_signed_webhook(claim).await,
            NotificationChannel::Email | NotificationChannel::Pager => Ok(()),
        }
    }

    async fn deliver_signed_webhook(&self, claim: &NotificationClaim) -> Result<(), &'static str> {
        let endpoint = url::Url::parse(&claim.endpoint).map_err(|_| "invalid_endpoint")?;
        if !allowed_webhook_endpoint(&endpoint) {
            return Err("endpoint_not_allowed");
        }
        let secret_reference = claim.secret_reference.as_deref().ok_or("secret_reference_missing")?;
        let secret = resolve_secret_reference(secret_reference)?;
        let payload = NotificationPayload {
            schema_version: "rocketmq-sre.notification.v1",
            delivery_id: claim.delivery_id,
            incident_id: claim.incident_id,
            summary: &claim.sanitized_summary,
            deep_link: &claim.deep_link,
        };
        let body = serde_json::to_vec(&payload).map_err(|_| "payload_encoding_failed")?;
        if body.len() > MAX_NOTIFICATION_BODY_BYTES {
            return Err("payload_too_large");
        }
        let signature = hmac_sha256(secret.as_bytes(), &body);
        self.client
            .post(endpoint)
            .header("content-type", "application/json")
            .header("x-rocketmq-sre-delivery", claim.delivery_id.to_string())
            .header("x-rocketmq-sre-signature", format!("sha256={signature}"))
            .body(body)
            .send()
            .await
            .map_err(|_| "transport_unavailable")?
            .error_for_status()
            .map_err(|_| "remote_rejected")?;
        Ok(())
    }
}

#[derive(Serialize)]
struct NotificationPayload<'a> {
    schema_version: &'static str,
    delivery_id: rocketmq_sre_contracts::NotificationDeliveryId,
    incident_id: rocketmq_sre_contracts::IncidentId,
    summary: &'a str,
    deep_link: &'a str,
}

fn allowed_webhook_endpoint(endpoint: &url::Url) -> bool {
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

fn hmac_sha256(key: &[u8], message: &[u8]) -> String {
    const BLOCK_SIZE: usize = 64;
    let mut block = [0_u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36_u8; BLOCK_SIZE];
    let mut outer_pad = [0x5c_u8; BLOCK_SIZE];
    for index in 0..BLOCK_SIZE {
        inner_pad[index] ^= block[index];
        outer_pad[index] ^= block[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    format!("{:x}", outer.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_rfc_4231_case_one() {
        assert_eq!(
            hmac_sha256(&[0x0b; 20], b"Hi There"),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7".replace(' ', "")
        );
    }

    #[test]
    fn webhook_endpoint_rejects_credentials_redirect_prone_http_and_non_http_schemes() {
        assert!(allowed_webhook_endpoint(
            &url::Url::parse("https://hooks.example.test/sre").expect("url")
        ));
        assert!(allowed_webhook_endpoint(
            &url::Url::parse("http://127.0.0.1:9099/test").expect("url")
        ));
        assert!(!allowed_webhook_endpoint(
            &url::Url::parse("http://hooks.example.test/sre").expect("url")
        ));
        assert!(!allowed_webhook_endpoint(
            &url::Url::parse("https://user:pass@example.test/sre").expect("url")
        ));
    }
}
