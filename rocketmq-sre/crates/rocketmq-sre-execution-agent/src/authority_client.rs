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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerifyFenceGrantRequest;
use rocketmq_sre_contracts::VerifyReconcileGrantRequest;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::ExecutionAgentError;
use crate::config::validate_internal_service_url;

const MAX_AUTHORITY_RESPONSE_BYTES: usize = 64 * 1024;

/// Narrow Lease Authority surface used by the Agent before any side effect.
pub trait LeaseAuthorityClient: Send + Sync {
    /// Verifies that a short-lived dispatch grant still identifies the active epoch.
    fn verify_fence_grant<'a>(&'a self, tenant_id: TenantId, grant: &'a LeaseFenceGrant) -> AuthorityFuture<'a>;

    /// Verifies a read-only pending-epoch reconciliation grant.
    fn verify_reconcile_grant<'a>(&'a self, tenant_id: TenantId, grant: &'a ReconcileGrant) -> AuthorityFuture<'a>;
}

pub type AuthorityFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GrantVerification, ExecutionAgentError>> + Send + 'a>>;

/// Authenticated HTTP client that never receives the Authority signing key.
#[derive(Clone)]
pub struct HttpLeaseAuthorityClient {
    client: reqwest::Client,
    base_url: Url,
    bearer_token: Arc<str>,
    subject: Arc<str>,
}

impl HttpLeaseAuthorityClient {
    /// Constructs a redirect-free bounded internal client.
    ///
    /// # Errors
    ///
    /// Rejects empty workload identity or HTTP client configuration failures.
    pub fn new(
        base_url: Url,
        bearer_token: impl Into<Arc<str>>,
        subject: impl Into<Arc<str>>,
        timeout: Duration,
        dev_insecure_http: bool,
    ) -> Result<Self, ExecutionAgentError> {
        let bearer_token = bearer_token.into();
        let subject = subject.into();
        if bearer_token.trim().is_empty() || subject.trim().is_empty() {
            return Err(ExecutionAgentError::Configuration);
        }
        validate_internal_service_url(&base_url, dev_insecure_http)?;
        let client = reqwest::Client::builder()
            .https_only(!dev_insecure_http)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(ExecutionAgentError::Http)?;
        Ok(Self {
            client,
            base_url,
            bearer_token,
            subject,
        })
    }

    async fn post<T, R>(
        &self,
        path: &str,
        tenant_id: TenantId,
        cluster_id: rocketmq_sre_contracts::ClusterId,
        body: &T,
    ) -> Result<R, ExecutionAgentError>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self
            .base_url
            .join(path)
            .map_err(|_| ExecutionAgentError::Configuration)?;
        let response = self
            .client
            .post(url)
            .bearer_auth(self.bearer_token.as_ref())
            .header("x-rocketmq-tenant", tenant_id.to_string())
            .header("x-rocketmq-clusters", cluster_id.to_string())
            .header("x-rocketmq-subject", self.subject.as_ref())
            .json(body)
            .send()
            .await?;
        if response.status().is_client_error() {
            return Err(ExecutionAgentError::AuthorityRejected);
        }
        if response.status() != StatusCode::OK {
            return Err(ExecutionAgentError::AuthorityUnavailable);
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_AUTHORITY_RESPONSE_BYTES as u64)
        {
            return Err(ExecutionAgentError::AuthorityRejected);
        }
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len().saturating_add(chunk.len()) > MAX_AUTHORITY_RESPONSE_BYTES {
                return Err(ExecutionAgentError::AuthorityRejected);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| ExecutionAgentError::AuthorityRejected)
    }
}

impl LeaseAuthorityClient for HttpLeaseAuthorityClient {
    fn verify_fence_grant<'a>(&'a self, tenant_id: TenantId, grant: &'a LeaseFenceGrant) -> AuthorityFuture<'a> {
        Box::pin(async move {
            let verification: GrantVerification = self
                .post(
                    "/internal/v1/execution-authority/verify/fence-grant",
                    tenant_id,
                    grant.cluster_id,
                    &VerifyFenceGrantRequest {
                        schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                        tenant_id,
                        grant: grant.clone(),
                    },
                )
                .await?;
            validate_verification(&verification, grant.cluster_id, grant.epoch)?;
            Ok(verification)
        })
    }

    fn verify_reconcile_grant<'a>(&'a self, tenant_id: TenantId, grant: &'a ReconcileGrant) -> AuthorityFuture<'a> {
        Box::pin(async move {
            let verification: GrantVerification = self
                .post(
                    "/internal/v1/execution-authority/verify/reconcile-grant",
                    tenant_id,
                    grant.cluster_id,
                    &VerifyReconcileGrantRequest {
                        schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                        tenant_id,
                        grant: grant.clone(),
                    },
                )
                .await?;
            validate_verification(&verification, grant.cluster_id, grant.pending_epoch)?;
            Ok(verification)
        })
    }
}

impl Debug for HttpLeaseAuthorityClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpLeaseAuthorityClient")
            .field("base_url", &self.base_url)
            .field("bearer_token", &"[REDACTED]")
            .field("subject", &self.subject)
            .finish()
    }
}

fn validate_verification(
    verification: &GrantVerification,
    cluster_id: rocketmq_sre_contracts::ClusterId,
    epoch: rocketmq_sre_contracts::LeaseEpoch,
) -> Result<(), ExecutionAgentError> {
    if verification.schema_version == LEASE_AUTHORITY_SCHEMA_VERSION
        && verification.valid
        && verification.cluster_id == cluster_id
        && verification.epoch == epoch
        && verification.expires_at > chrono::Utc::now()
    {
        Ok(())
    } else {
        Err(ExecutionAgentError::AuthorityRejected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_authority_client_rejects_sensitive_url_parts() {
        let url: Url = "https://user:password@control-plane.example.test"
            .parse()
            .expect("syntactically valid URL");
        assert!(
            HttpLeaseAuthorityClient::new(
                url,
                "authority-workload-token",
                "spiffe://rocketmq-sre/execution-agent",
                Duration::from_secs(1),
                false,
            )
            .is_err()
        );
    }

    #[test]
    fn debug_output_redacts_authority_token() {
        let client = HttpLeaseAuthorityClient::new(
            "https://control-plane.example.test".parse().expect("URL"),
            "authority-workload-secret",
            "spiffe://rocketmq-sre/execution-agent",
            Duration::from_secs(1),
            false,
        )
        .expect("client");
        let debug = format!("{client:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("authority-workload-secret"));
    }
}
