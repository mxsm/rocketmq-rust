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
use rocketmq_sre_contracts::ActivateLeaseRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverResponse;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::DynamicSafetyDecision;
use rocketmq_sre_contracts::DynamicSafetyEvaluationRequest;
use rocketmq_sre_contracts::ExecutorLease;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::IssueFenceGrantRequest;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerifyExecutionRequest;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::ExecutorError;
use crate::config::validate_internal_service_url;

const MAX_AUTHORITY_RESPONSE_BYTES: usize = 64 * 1024;

pub type AuthorityFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ExecutorError>> + Send + 'a>>;

/// Minimal online Lease Authority surface available to Executor.
///
/// The interface deliberately has no signing-key access.
pub trait ExecutorAuthorityClient: Send + Sync {
    fn verify_execution<'a>(&'a self, request: &'a VerifyExecutionRequest) -> AuthorityFuture<'a, GrantVerification>;

    fn begin_takeover<'a>(
        &'a self,
        request: &'a BeginLeaseTakeoverRequest,
    ) -> AuthorityFuture<'a, BeginLeaseTakeoverResponse>;

    fn activate<'a>(
        &'a self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        request: &'a ActivateLeaseRequest,
    ) -> AuthorityFuture<'a, ExecutorLease>;

    fn issue_fence_grant<'a>(&'a self, request: &'a IssueFenceGrantRequest) -> AuthorityFuture<'a, LeaseFenceGrant>;

    fn evaluate_dynamic_safety<'a>(
        &'a self,
        request: &'a DynamicSafetyEvaluationRequest,
    ) -> AuthorityFuture<'a, DynamicSafetyDecision>;
}

/// Redirect-free, bounded, workload-authenticated Authority client.
#[derive(Clone)]
pub struct HttpExecutorAuthorityClient {
    client: reqwest::Client,
    base_url: Url,
    bearer_token: Arc<str>,
    subject: Arc<str>,
}

impl HttpExecutorAuthorityClient {
    /// Creates a client that sends no target credentials.
    ///
    /// # Errors
    ///
    /// Rejects empty workload identity and invalid HTTP client configuration.
    pub fn new(
        base_url: Url,
        bearer_token: impl Into<Arc<str>>,
        subject: impl Into<Arc<str>>,
        timeout: Duration,
        dev_insecure_http: bool,
    ) -> Result<Self, ExecutorError> {
        let bearer_token = bearer_token.into();
        let subject = subject.into();
        if bearer_token.trim().is_empty() || subject.trim().is_empty() {
            return Err(ExecutorError::Configuration);
        }
        validate_internal_service_url(&base_url, dev_insecure_http)?;
        let client = reqwest::Client::builder()
            .https_only(!dev_insecure_http)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()?;
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
        cluster_id: ClusterId,
        body: &T,
    ) -> Result<R, ExecutorError>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self.base_url.join(path).map_err(|_| ExecutorError::Configuration)?;
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
        match response.status() {
            StatusCode::OK => {}
            status if status.is_client_error() => return Err(ExecutorError::AuthorityRejected),
            _ => return Err(ExecutorError::AuthorityUnavailable),
        }
        read_bounded(response).await.map_err(|error| match error {
            BoundedResponseError::Http(error) => ExecutorError::Http(error),
            BoundedResponseError::Rejected => ExecutorError::AuthorityRejected,
        })
    }
}

impl ExecutorAuthorityClient for HttpExecutorAuthorityClient {
    fn verify_execution<'a>(&'a self, request: &'a VerifyExecutionRequest) -> AuthorityFuture<'a, GrantVerification> {
        Box::pin(async move {
            let response: GrantVerification = self
                .post(
                    "/internal/v1/execution-authority/verify/execution",
                    request.execution.tenant_id,
                    request.execution.cluster_id,
                    request,
                )
                .await?;
            if response.schema_version != LEASE_AUTHORITY_SCHEMA_VERSION
                || !response.valid
                || response.cluster_id != request.execution.cluster_id
                || response.expires_at != request.execution.expires_at
                || response.epoch.0 != 0
            {
                return Err(ExecutorError::AuthorityRejected);
            }
            Ok(response)
        })
    }

    fn begin_takeover<'a>(
        &'a self,
        request: &'a BeginLeaseTakeoverRequest,
    ) -> AuthorityFuture<'a, BeginLeaseTakeoverResponse> {
        Box::pin(async move {
            let response: BeginLeaseTakeoverResponse = self
                .post(
                    "/internal/v1/execution-authority/leases/takeover",
                    request.tenant_id,
                    request.cluster_id,
                    request,
                )
                .await?;
            if response.schema_version != LEASE_AUTHORITY_SCHEMA_VERSION
                || response.lease.tenant_id != request.tenant_id
                || response.lease.cluster_id != request.cluster_id
                || response.lease.owner != self.subject.as_ref()
                || response.reconcile_grant.lease_id != response.lease.id
                || response.reconcile_grant.pending_epoch != response.lease.epoch
            {
                return Err(ExecutorError::AuthorityRejected);
            }
            Ok(response)
        })
    }

    fn activate<'a>(
        &'a self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        request: &'a ActivateLeaseRequest,
    ) -> AuthorityFuture<'a, ExecutorLease> {
        Box::pin(async move {
            let lease: ExecutorLease = self
                .post(
                    "/internal/v1/execution-authority/leases/activate",
                    tenant_id,
                    cluster_id,
                    request,
                )
                .await?;
            if lease.id != request.lease_id
                || lease.tenant_id != tenant_id
                || lease.cluster_id != cluster_id
                || lease.owner != self.subject.as_ref()
                || lease.state != rocketmq_sre_contracts::LeaseState::Active
                || lease.epoch != request.fence_ack.epoch
            {
                return Err(ExecutorError::AuthorityRejected);
            }
            Ok(lease)
        })
    }

    fn issue_fence_grant<'a>(&'a self, request: &'a IssueFenceGrantRequest) -> AuthorityFuture<'a, LeaseFenceGrant> {
        Box::pin(async move {
            let grant: LeaseFenceGrant = self
                .post(
                    "/internal/v1/execution-authority/leases/fence-grant",
                    request.tenant_id,
                    request.cluster_id,
                    request,
                )
                .await?;
            if grant.lease_id != request.lease_id
                || grant.cluster_id != request.cluster_id
                || grant.epoch != request.epoch
                || grant.execution_id != request.execution_id
                || grant.step_id != request.step_id
                || grant.plan_step_id != request.plan_step_id
                || grant.compensation != request.compensation
                || grant.owner != self.subject.as_ref()
                || grant.expires_at <= chrono::Utc::now()
            {
                return Err(ExecutorError::AuthorityRejected);
            }
            Ok(grant)
        })
    }

    fn evaluate_dynamic_safety<'a>(
        &'a self,
        request: &'a DynamicSafetyEvaluationRequest,
    ) -> AuthorityFuture<'a, DynamicSafetyDecision> {
        Box::pin(async move {
            let decision: DynamicSafetyDecision = self
                .post(
                    "/internal/v1/autonomy/dynamic-safety",
                    request.tenant_id,
                    request.cluster_id,
                    request,
                )
                .await?;
            if decision.validate_allow_at(chrono::Utc::now()).is_err()
                || decision.tenant_id != request.tenant_id
                || decision.cluster_id != request.cluster_id
                || decision.action != request.action
                || decision.action_version != request.action_version
                || decision.plan_id != request.plan_id
                || decision.plan_hash != request.plan_hash
                || decision.execution_id != request.execution_id
                || decision.execution_step_id != request.execution_step_id
                || decision.policy_definition_version != request.policy_definition_version
                || decision.lifecycle_revision != request.lifecycle_revision
            {
                return Err(ExecutorError::AuthorityRejected);
            }
            Ok(decision)
        })
    }
}

impl Debug for HttpExecutorAuthorityClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpExecutorAuthorityClient")
            .field("base_url", &self.base_url)
            .field("bearer_token", &"[REDACTED]")
            .field("subject", &self.subject)
            .finish()
    }
}

enum BoundedResponseError {
    Http(reqwest::Error),
    Rejected,
}

async fn read_bounded<R>(mut response: reqwest::Response) -> Result<R, BoundedResponseError>
where
    R: DeserializeOwned,
{
    if response
        .content_length()
        .is_some_and(|length| length > MAX_AUTHORITY_RESPONSE_BYTES as u64)
    {
        return Err(BoundedResponseError::Rejected);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(BoundedResponseError::Http)? {
        if bytes.len().saturating_add(chunk.len()) > MAX_AUTHORITY_RESPONSE_BYTES {
            return Err(BoundedResponseError::Rejected);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| BoundedResponseError::Rejected)
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
            HttpExecutorAuthorityClient::new(
                url,
                "authority-workload-token",
                "spiffe://rocketmq-sre/executor",
                Duration::from_secs(1),
                false,
            )
            .is_err()
        );
    }

    #[test]
    fn debug_output_redacts_authority_token() {
        let client = HttpExecutorAuthorityClient::new(
            "https://control-plane.example.test".parse().expect("URL"),
            "authority-workload-secret",
            "spiffe://rocketmq-sre/executor",
            Duration::from_secs(1),
            false,
        )
        .expect("client");
        let debug = format!("{client:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("authority-workload-secret"));
    }
}
