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
use rocketmq_sre_contracts::AdvanceFenceRequest;
use rocketmq_sre_contracts::AdvanceFenceResponse;
use rocketmq_sre_contracts::AgentDispatchRequest;
use rocketmq_sre_contracts::AgentDispatchResponse;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionAgentCapabilities;
use rocketmq_sre_contracts::ReconcileEffectRequest;
use rocketmq_sre_contracts::ReconcileEffectResponse;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::ExecutorError;
use crate::config::validate_internal_service_url;

const MAX_AGENT_RESPONSE_BYTES: usize = 128 * 1024;
const EXECUTOR_SPIFFE: &str = "spiffe://rocketmq-sre/executor";

pub type AgentFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ExecutorError>> + Send + 'a>>;

/// Narrow typed Agent RPC surface. No raw target or driver API is exposed.
pub trait ExecutionAgentClient: Send + Sync {
    fn capabilities<'a>(&'a self) -> AgentFuture<'a, ExecutionAgentCapabilities>;
    fn precheck<'a>(&'a self, request: &'a AgentReadRequest) -> AgentFuture<'a, AgentReadResult>;
    fn dispatch<'a>(&'a self, request: &'a AgentDispatchRequest) -> AgentFuture<'a, AgentDispatchResponse>;
    fn reconcile<'a>(&'a self, request: &'a ReconcileEffectRequest) -> AgentFuture<'a, ReconcileEffectResponse>;
    fn advance_fence<'a>(&'a self, request: &'a AdvanceFenceRequest) -> AgentFuture<'a, AdvanceFenceResponse>;
}

/// Workload-authenticated black-box Agent client.
#[derive(Clone)]
pub struct HttpExecutionAgentClient {
    client: reqwest::Client,
    base_url: Url,
    bearer_token: Arc<str>,
}

impl HttpExecutionAgentClient {
    /// Constructs a bounded client with redirects disabled.
    ///
    /// # Errors
    ///
    /// Rejects blank identity credentials and invalid client configuration.
    pub fn new(
        base_url: Url,
        bearer_token: impl Into<Arc<str>>,
        timeout: Duration,
        dev_insecure_http: bool,
    ) -> Result<Self, ExecutorError> {
        let bearer_token = bearer_token.into();
        if bearer_token.trim().is_empty() {
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
        })
    }

    async fn get<R>(&self, path: &str) -> Result<R, ExecutorError>
    where
        R: DeserializeOwned,
    {
        let url = self.base_url.join(path).map_err(|_| ExecutorError::Configuration)?;
        let response = self
            .client
            .get(url)
            .bearer_auth(self.bearer_token.as_ref())
            .header("x-forwarded-client-cert", format!("URI={EXECUTOR_SPIFFE}"))
            .send()
            .await?;
        decode(response).await
    }

    async fn post<T, R>(&self, path: &str, body: &T) -> Result<R, ExecutorError>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self.base_url.join(path).map_err(|_| ExecutorError::Configuration)?;
        let response = self
            .client
            .post(url)
            .bearer_auth(self.bearer_token.as_ref())
            .header("x-forwarded-client-cert", format!("URI={EXECUTOR_SPIFFE}"))
            .json(body)
            .send()
            .await?;
        decode(response).await
    }
}

impl ExecutionAgentClient for HttpExecutionAgentClient {
    fn capabilities<'a>(&'a self) -> AgentFuture<'a, ExecutionAgentCapabilities> {
        Box::pin(async move {
            let capabilities: ExecutionAgentCapabilities =
                self.get("/internal/v1/execution-agent/capabilities").await?;
            if capabilities.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
                || capabilities.raw_admin_request_supported
                || capabilities.arbitrary_json_patch_supported
                || capabilities.shell_supported
                || !capabilities.durable_fencing
            {
                return Err(ExecutorError::AgentRejected);
            }
            Ok(capabilities)
        })
    }

    fn precheck<'a>(&'a self, request: &'a AgentReadRequest) -> AgentFuture<'a, AgentReadResult> {
        Box::pin(async move {
            let result: AgentReadResult = self.post("/internal/v1/execution-agent/precheck", request).await?;
            if result.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
                || result.action != request.action
                || result.target != request.target
            {
                return Err(ExecutorError::AgentRejected);
            }
            Ok(result)
        })
    }

    fn dispatch<'a>(&'a self, request: &'a AgentDispatchRequest) -> AgentFuture<'a, AgentDispatchResponse> {
        Box::pin(async move {
            let result: AgentDispatchResponse = self.post("/internal/v1/execution-agent/dispatch", request).await?;
            if result.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
                || result.result.execution_id != request.request.intent.execution_id
                || result.result.step_id != request.request.intent.step_id
            {
                return Err(ExecutorError::AgentRejected);
            }
            Ok(result)
        })
    }

    fn reconcile<'a>(&'a self, request: &'a ReconcileEffectRequest) -> AgentFuture<'a, ReconcileEffectResponse> {
        Box::pin(async move {
            let result: ReconcileEffectResponse = self.post("/internal/v1/execution-agent/reconcile", request).await?;
            if result.schema_version != EXECUTION_AGENT_SCHEMA_VERSION {
                return Err(ExecutorError::AgentRejected);
            }
            Ok(result)
        })
    }

    fn advance_fence<'a>(&'a self, request: &'a AdvanceFenceRequest) -> AgentFuture<'a, AdvanceFenceResponse> {
        Box::pin(async move {
            let result: AdvanceFenceResponse = self.post("/internal/v1/execution-agent/advance-fence", request).await?;
            if result.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
                || result.fence_ack.cluster_id != request.reconcile_grant.cluster_id
                || result.fence_ack.epoch != request.reconcile_grant.pending_epoch
                || result.fence_ack.pending_nonce != request.reconcile_grant.nonce
            {
                return Err(ExecutorError::AgentRejected);
            }
            Ok(result)
        })
    }
}

impl Debug for HttpExecutionAgentClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpExecutionAgentClient")
            .field("base_url", &self.base_url)
            .field("bearer_token", &"[REDACTED]")
            .finish()
    }
}

async fn decode<R>(mut response: reqwest::Response) -> Result<R, ExecutorError>
where
    R: DeserializeOwned,
{
    match response.status() {
        StatusCode::OK => {}
        status if status.is_client_error() => return Err(ExecutorError::AgentRejected),
        _ => return Err(ExecutorError::AgentUnavailable),
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_AGENT_RESPONSE_BYTES as u64)
    {
        return Err(ExecutorError::AgentRejected);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > MAX_AGENT_RESPONSE_BYTES {
            return Err(ExecutorError::AgentRejected);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| ExecutorError::AgentRejected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_agent_client_rejects_sensitive_url_parts() {
        let url: Url = "https://user:password@execution-agent.example.test"
            .parse()
            .expect("syntactically valid URL");
        assert!(HttpExecutionAgentClient::new(url, "workload-token", Duration::from_secs(1), false).is_err());
    }

    #[test]
    fn debug_output_redacts_agent_token() {
        let client = HttpExecutionAgentClient::new(
            "https://execution-agent.example.test".parse().expect("URL"),
            "agent-workload-secret",
            Duration::from_secs(1),
            false,
        )
        .expect("client");
        let debug = format!("{client:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("agent-workload-secret"));
    }
}
