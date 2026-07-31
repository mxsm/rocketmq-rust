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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use reqwest::StatusCode;
use rocketmq_sre_contracts::EXECUTION_VERIFICATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionSliObservation;
use rocketmq_sre_contracts::ExecutionSliQuery;
use url::Url;

use crate::ExecutorError;
use crate::config::validate_internal_service_url;

const MAX_SLI_RESPONSE_BYTES: usize = 128 * 1024;
const EXECUTOR_SPIFFE: &str = "spiffe://rocketmq-sre/executor";

pub type SliFuture<'a> = Pin<Box<dyn Future<Output = Result<ExecutionSliObservation, ExecutorError>> + Send + 'a>>;

/// Narrow read-only Control Plane SLI surface used by the Executor.
pub trait ExecutionSliClient: Send + Sync {
    fn observe<'a>(&'a self, query: &'a ExecutionSliQuery) -> SliFuture<'a>;
}

/// Workload-authenticated client for independently evaluated technical SLIs.
#[derive(Clone)]
pub struct HttpExecutionSliClient {
    client: reqwest::Client,
    base_url: Url,
    bearer_token: Arc<str>,
    subject: Arc<str>,
}

impl HttpExecutionSliClient {
    /// Constructs a bounded client with redirects disabled.
    ///
    /// # Errors
    ///
    /// Rejects blank workload credentials and invalid internal service URLs.
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
}

impl ExecutionSliClient for HttpExecutionSliClient {
    fn observe<'a>(&'a self, query: &'a ExecutionSliQuery) -> SliFuture<'a> {
        Box::pin(async move {
            let url = self
                .base_url
                .join("/internal/v1/execution-verification/sli")
                .map_err(|_| ExecutorError::Configuration)?;
            let response = self
                .client
                .post(url)
                .bearer_auth(self.bearer_token.as_ref())
                .header("x-forwarded-client-cert", format!("URI={EXECUTOR_SPIFFE}"))
                .header("x-rocketmq-tenant", query.tenant_id.to_string())
                .header("x-rocketmq-clusters", query.cluster_id.to_string())
                .header("x-rocketmq-subject", self.subject.as_ref())
                .json(query)
                .send()
                .await
                .map_err(|_| ExecutorError::VerificationUnavailable)?;
            let observation = decode(response).await?;
            validate_observation(query, &observation)?;
            Ok(observation)
        })
    }
}

impl Debug for HttpExecutionSliClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpExecutionSliClient")
            .field("base_url", &self.base_url)
            .field("bearer_token", &"[REDACTED]")
            .field("subject", &self.subject)
            .finish()
    }
}

async fn decode(mut response: reqwest::Response) -> Result<ExecutionSliObservation, ExecutorError> {
    match response.status() {
        StatusCode::OK => {}
        status if status.is_client_error() => return Err(ExecutorError::VerificationRejected),
        _ => return Err(ExecutorError::VerificationUnavailable),
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_SLI_RESPONSE_BYTES as u64)
    {
        return Err(ExecutorError::VerificationRejected);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ExecutorError::VerificationUnavailable)?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_SLI_RESPONSE_BYTES {
            return Err(ExecutorError::VerificationRejected);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| ExecutorError::VerificationRejected)
}

fn validate_observation(query: &ExecutionSliQuery, observation: &ExecutionSliObservation) -> Result<(), ExecutorError> {
    let expected = query.conditions.iter().collect::<BTreeSet<_>>();
    let actual = observation.conditions.keys().collect::<BTreeSet<_>>();
    if observation.schema_version != EXECUTION_VERIFICATION_SCHEMA_VERSION
        || observation.tenant_id != query.tenant_id
        || observation.cluster_id != query.cluster_id
        || observation.correlation_id != query.correlation_id
        || expected.len() != query.conditions.len()
        || actual != expected
    {
        return Err(ExecutorError::VerificationRejected);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;

    #[test]
    fn observation_scope_and_condition_surface_are_exact() {
        let query = query();
        let mut observation = ExecutionSliObservation {
            schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
            tenant_id: query.tenant_id,
            cluster_id: query.cluster_id,
            correlation_id: query.correlation_id,
            conditions: [("broker_error_ratio".to_owned(), true)].into_iter().collect(),
            complete: true,
            evidence_ids: Vec::new(),
            observed_at: Utc::now(),
        };
        assert!(validate_observation(&query, &observation).is_ok());

        observation.correlation_id = CorrelationId::new();
        assert!(validate_observation(&query, &observation).is_err());
        observation.correlation_id = query.correlation_id;
        observation.conditions.insert("unexpected".to_owned(), true);
        assert!(validate_observation(&query, &observation).is_err());
    }

    #[test]
    fn debug_output_redacts_control_plane_token() {
        let client = HttpExecutionSliClient::new(
            "https://control-plane.example.test".parse().expect("URL"),
            "control-plane-secret",
            "spiffe://rocketmq-sre/executor",
            Duration::from_secs(1),
            false,
        )
        .expect("client");
        let debug = format!("{client:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("control-plane-secret"));
    }

    fn query() -> ExecutionSliQuery {
        ExecutionSliQuery {
            schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            correlation_id: CorrelationId::new(),
            conditions: vec!["broker_error_ratio".to_owned()],
        }
    }
}
