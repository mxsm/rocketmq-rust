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
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::is_sha256_digest;
use serde::Deserialize;
use url::Url;

use crate::ControlPlaneError;
use crate::config::validate_internal_service_url;

const MAX_EXECUTOR_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(super) struct ExecutorDispatchReceipt {
    pub(super) execution_id: ExecutionId,
    pub(super) state: ExecutionState,
    pub(super) replayed: bool,
    pub(super) accepted_steps: usize,
}

/// Optional client is disabled until a deployment explicitly configures the
/// isolated Executor endpoint and workload credential.
#[derive(Clone)]
pub(crate) enum ExecutorSubmissionClient {
    Disabled,
    Http {
        client: reqwest::Client,
        base_url: Url,
        bearer_token: Arc<str>,
    },
}

impl ExecutorSubmissionClient {
    #[must_use]
    pub(crate) const fn disabled() -> Self {
        Self::Disabled
    }

    /// Constructs a redirect-free bounded internal client.
    ///
    /// # Errors
    ///
    /// Rejects blank credentials and invalid HTTP client configuration.
    pub(crate) fn http(
        base_url: Url,
        bearer_token: impl Into<Arc<str>>,
        timeout: Duration,
        allow_insecure_http: bool,
    ) -> Result<Self, ControlPlaneError> {
        let bearer_token = bearer_token.into();
        if bearer_token.trim().is_empty() {
            return Err(ControlPlaneError::configuration(
                "Executor URL or workload credential is invalid",
            ));
        }
        validate_internal_service_url(&base_url, allow_insecure_http)?;
        let client = reqwest::Client::builder()
            .https_only(!allow_insecure_http)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(ControlPlaneError::Executor)?;
        Ok(Self::Http {
            client,
            base_url,
            bearer_token,
        })
    }

    pub(super) async fn submit(
        &self,
        execution: &ExecutionRequest,
    ) -> Result<ExecutorDispatchReceipt, ControlPlaneError> {
        let Self::Http {
            client,
            base_url,
            bearer_token,
        } = self
        else {
            return Err(ControlPlaneError::conflict_code(
                "executor_not_configured",
                "the isolated Change Executor endpoint is not configured",
            ));
        };
        let url = base_url
            .join("/internal/v1/executor/executions")
            .map_err(|_| ControlPlaneError::configuration("Executor URL is invalid"))?;
        let mut response = client
            .post(url)
            .bearer_auth(bearer_token.as_ref())
            .header("x-forwarded-client-cert", "URI=spiffe://rocketmq-sre/control-plane")
            .json(execution)
            .send()
            .await
            .map_err(ControlPlaneError::Executor)?;
        match response.status() {
            StatusCode::OK => {}
            StatusCode::CONFLICT => {
                return Err(ControlPlaneError::conflict_code(
                    "executor_rejected",
                    "Change Executor rejected the request or requires reconciliation",
                ));
            }
            status if status.is_client_error() => {
                return Err(ControlPlaneError::forbidden(
                    "executor_rejected",
                    "Change Executor rejected the signed request",
                ));
            }
            _ => {
                return Err(ControlPlaneError::conflict_code(
                    "executor_unavailable",
                    "Change Executor is temporarily unavailable",
                ));
            }
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_EXECUTOR_RESPONSE_BYTES as u64)
        {
            return Err(invalid_response());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(ControlPlaneError::Executor)? {
            if bytes.len().saturating_add(chunk.len()) > MAX_EXECUTOR_RESPONSE_BYTES {
                return Err(invalid_response());
            }
            bytes.extend_from_slice(&chunk);
        }
        let receipt: ExecutorDispatchReceipt = serde_json::from_slice(&bytes).map_err(|_| invalid_response())?;
        if receipt.execution_id != execution.id || receipt.accepted_steps != execution.plan.steps.len() {
            return Err(invalid_response());
        }
        Ok(receipt)
    }

    pub(super) async fn read_precondition(
        &self,
        request: &AgentReadRequest,
    ) -> Result<AgentReadResult, ControlPlaneError> {
        let Self::Http {
            client,
            base_url,
            bearer_token,
        } = self
        else {
            return Err(ControlPlaneError::conflict_code(
                "executor_not_configured",
                "the isolated Change Executor endpoint is not configured",
            ));
        };
        let url = base_url
            .join("/internal/v1/executor/preconditions")
            .map_err(|_| ControlPlaneError::configuration("Executor URL is invalid"))?;
        let mut response = client
            .post(url)
            .bearer_auth(bearer_token.as_ref())
            .header("x-forwarded-client-cert", "URI=spiffe://rocketmq-sre/control-plane")
            .json(request)
            .send()
            .await
            .map_err(ControlPlaneError::Executor)?;
        match response.status() {
            StatusCode::OK => {}
            StatusCode::CONFLICT => {
                return Err(ControlPlaneError::conflict_code(
                    "execution_precondition_not_ready",
                    "Execution Agent did not report a ready precondition",
                ));
            }
            status if status.is_client_error() => {
                return Err(ControlPlaneError::forbidden(
                    "executor_rejected",
                    "Change Executor rejected the precondition request",
                ));
            }
            _ => {
                return Err(ControlPlaneError::conflict_code(
                    "executor_unavailable",
                    "Change Executor is temporarily unavailable",
                ));
            }
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_EXECUTOR_RESPONSE_BYTES as u64)
        {
            return Err(invalid_response());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(ControlPlaneError::Executor)? {
            if bytes.len().saturating_add(chunk.len()) > MAX_EXECUTOR_RESPONSE_BYTES {
                return Err(invalid_response());
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: AgentReadResult = serde_json::from_slice(&bytes).map_err(|_| invalid_response())?;
        if result.schema_version != EXECUTION_AGENT_SCHEMA_VERSION
            || result.action != request.action
            || result.target != request.target
            || !result.ready
            || !result.reason_codes.is_empty()
            || !is_sha256_digest(&result.precondition_hash)
        {
            return Err(invalid_response());
        }
        Ok(result)
    }
}

impl Debug for ExecutorSubmissionClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => formatter
                .debug_struct("ExecutorSubmissionClient")
                .field("mode", &"disabled")
                .finish(),
            Self::Http { base_url, .. } => formatter
                .debug_struct("ExecutorSubmissionClient")
                .field("mode", &"http")
                .field("base_url", base_url)
                .field("bearer_token", &"[REDACTED]")
                .finish(),
        }
    }
}

fn invalid_response() -> ControlPlaneError {
    ControlPlaneError::conflict_code(
        "executor_response_invalid",
        "Change Executor returned an invalid response",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_client_construction_rejects_credentialed_url() {
        let url: Url = "https://user:password@executor.example.test"
            .parse()
            .expect("syntactically valid URL");
        assert!(ExecutorSubmissionClient::http(url, "workload-token", Duration::from_secs(1), false).is_err());
    }

    #[test]
    fn debug_output_redacts_workload_token() {
        let client = ExecutorSubmissionClient::http(
            "https://executor.example.test".parse().expect("URL"),
            "executor-workload-secret",
            Duration::from_secs(1),
            false,
        )
        .expect("client");
        let debug = format!("{client:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("executor-workload-secret"));
    }
}
