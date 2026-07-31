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

//! Minimal, read-only HTTP client for the RocketMQ Rust AI SRE Control Plane.
//!
//! The client intentionally exposes only status, cluster, incident,
//! inspection, plan, and OpenAPI reads. It has no execution, approval,
//! administrative mutation, arbitrary request, or raw shell escape hatch.

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use reqwest::Method;
use reqwest::header::AUTHORIZATION;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::USER_AGENT;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CriticGateState;
use rocketmq_sre_contracts::CriticReview;
use rocketmq_sre_contracts::Incident;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::InspectionRun;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::PolicyDecision;
use rocketmq_sre_contracts::Recommendation;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;
use url::Url;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const CLIENT_USER_AGENT: &str = concat!("rocketmq-sre-client/", env!("CARGO_PKG_VERSION"));

/// Errors returned by the bounded read-only client.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid Control Plane base URL: {0}")]
    InvalidBaseUrl(String),
    #[error("invalid bearer token")]
    InvalidBearerToken,
    #[error("response exceeded the configured {limit} byte limit")]
    ResponseTooLarge { limit: usize },
    #[error("cluster {cluster_id} is outside the configured client allowlist")]
    ClusterNotAllowed { cluster_id: ClusterId },
    #[error("Control Plane request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Control Plane returned HTTP {status}: {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
        retryable: bool,
        correlation_id: Option<String>,
    },
    #[error("Control Plane response did not match the versioned contract: {0}")]
    Decode(#[from] serde_json::Error),
}

/// Process liveness response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceStatus {
    pub status: String,
}

/// Persisted cluster onboarding lifecycle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingState {
    Pending,
    Handshaking,
    ReadyReadOnly,
    ReadOnlyDegraded,
    Rejected,
    Offboarded,
}

/// Cluster projection returned by list and detail reads.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Cluster {
    pub id: ClusterId,
    pub tenant_id: String,
    pub external_cluster_key: String,
    pub environment: String,
    pub region: String,
    pub rocketmq_version: String,
    pub deployment_mode: String,
    pub owner: String,
    pub state: OnboardingState,
    pub effective_access_profile: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offboarded_at: Option<DateTime<Utc>>,
}

/// Read-only incident projection with bounded supporting material.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct IncidentView {
    pub incident: Incident,
    pub investigation: Option<Value>,
    pub timeline: Vec<Value>,
    pub diagnosis_revisions: Vec<Value>,
}

/// Read-only inspection projection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InspectionView {
    pub run: InspectionRun,
    pub recommendations: Vec<Recommendation>,
    pub pack_diffs: Vec<Value>,
}

/// Read-only supervised-plan projection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ActionPlanView {
    pub plan: ActionPlan,
    pub risk: ActionRisk,
    pub critic_state: CriticGateState,
    pub latest_critic_review: Option<CriticReview>,
    pub latest_policy_decision: Option<PolicyDecision>,
    pub latest_approval: Option<ApprovalRecord>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    code: String,
    message: String,
    retryable: bool,
    #[serde(default)]
    correlation_id: Option<String>,
}

/// Builder for a [`Client`].
///
/// This type deliberately does not implement `Debug`, so a bearer token
/// cannot be emitted by ordinary configuration logging.
pub struct ClientBuilder {
    base_url: Url,
    bearer_token: Option<String>,
    allowed_clusters: Option<BTreeSet<ClusterId>>,
    timeout: Duration,
    max_response_bytes: usize,
}

impl ClientBuilder {
    /// Creates a builder after validating the Control Plane origin.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::InvalidBaseUrl`] for a malformed URL, embedded
    /// credentials, query or fragment data, or a non-HTTP(S) scheme.
    pub fn new(base_url: impl AsRef<str>) -> Result<Self, ClientError> {
        let mut base_url =
            Url::parse(base_url.as_ref()).map_err(|error| ClientError::InvalidBaseUrl(error.to_string()))?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(ClientError::InvalidBaseUrl("scheme must be http or https".to_owned()));
        }
        if !base_url.username().is_empty() || base_url.password().is_some() {
            return Err(ClientError::InvalidBaseUrl(
                "embedded credentials are forbidden".to_owned(),
            ));
        }
        if base_url.query().is_some() || base_url.fragment().is_some() {
            return Err(ClientError::InvalidBaseUrl(
                "query and fragment data are forbidden".to_owned(),
            ));
        }
        if base_url.cannot_be_a_base() || base_url.host_str().is_none() {
            return Err(ClientError::InvalidBaseUrl(
                "URL must identify a network origin".to_owned(),
            ));
        }
        if !base_url.path().ends_with('/') {
            let normalized = format!("{}/", base_url.path());
            base_url.set_path(&normalized);
        }

        Ok(Self {
            base_url,
            bearer_token: None,
            allowed_clusters: None,
            timeout: DEFAULT_TIMEOUT,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        })
    }

    /// Adds an OIDC bearer token. The value is marked sensitive in HTTP
    /// headers and is never included in client errors.
    #[must_use]
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    /// Restricts all cluster-scoped reads to the supplied identifiers.
    ///
    /// An explicitly empty set denies every cluster-scoped read.
    #[must_use]
    pub fn allowed_clusters(mut self, cluster_ids: impl IntoIterator<Item = ClusterId>) -> Self {
        self.allowed_clusters = Some(cluster_ids.into_iter().collect());
        self
    }

    /// Sets the whole-request timeout.
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the maximum decoded HTTP response size.
    #[must_use]
    pub const fn max_response_bytes(mut self, limit: usize) -> Self {
        self.max_response_bytes = limit;
        self
    }

    /// Builds the read-only client.
    ///
    /// # Errors
    ///
    /// Returns an error when the token cannot be represented as a sensitive
    /// HTTP header, the response limit is zero, or the HTTP client fails to
    /// initialize.
    pub fn build(self) -> Result<Client, ClientError> {
        if self.max_response_bytes == 0 {
            return Err(ClientError::InvalidBaseUrl(
                "response byte limit must be greater than zero".to_owned(),
            ));
        }

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(CLIENT_USER_AGENT));
        if let Some(token) = self.bearer_token {
            let mut value =
                HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| ClientError::InvalidBearerToken)?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(self.timeout)
            .build()?;

        Ok(Client {
            http,
            base_url: self.base_url,
            allowed_clusters: self.allowed_clusters,
            max_response_bytes: self.max_response_bytes,
        })
    }
}

/// Minimal cloneable read-only Control Plane client.
///
/// This type deliberately does not implement `Debug`, preventing default
/// request headers from being logged accidentally.
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base_url: Url,
    allowed_clusters: Option<BTreeSet<ClusterId>>,
    max_response_bytes: usize,
}

impl Client {
    /// Creates a validated client builder.
    ///
    /// # Errors
    ///
    /// See [`ClientBuilder::new`].
    pub fn builder(base_url: impl AsRef<str>) -> Result<ClientBuilder, ClientError> {
        ClientBuilder::new(base_url)
    }

    /// Reads process liveness.
    ///
    /// # Errors
    ///
    /// Returns a transport, bounded-response, HTTP API, or decode error.
    pub async fn status(&self) -> Result<ServiceStatus, ClientError> {
        self.get("healthz").await
    }

    /// Reads dependency readiness without interpreting component-specific
    /// extension fields.
    ///
    /// # Errors
    ///
    /// Returns a transport, bounded-response, HTTP API, or decode error.
    pub async fn readiness(&self) -> Result<Value, ClientError> {
        self.get("readyz").await
    }

    /// Reads the server's canonical Phase 5 OpenAPI document.
    ///
    /// # Errors
    ///
    /// Returns a transport, bounded-response, HTTP API, or decode error.
    pub async fn openapi(&self) -> Result<Value, ClientError> {
        self.get("v1/openapi.json").await
    }

    /// Lists clusters visible to the current identity and configured client
    /// allowlist.
    ///
    /// # Errors
    ///
    /// Returns a transport, bounded-response, HTTP API, or decode error.
    pub async fn clusters(&self) -> Result<Vec<Cluster>, ClientError> {
        let mut clusters: Vec<Cluster> = self.get("v1/clusters").await?;
        if let Some(allowed) = &self.allowed_clusters {
            clusters.retain(|cluster| allowed.contains(&cluster.id));
        }
        Ok(clusters)
    }

    /// Reads one cluster.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::ClusterNotAllowed`] before network I/O when the
    /// cluster is outside the configured allowlist, or a normal read error.
    pub async fn cluster(&self, cluster_id: ClusterId) -> Result<Cluster, ClientError> {
        self.ensure_cluster_allowed(cluster_id)?;
        let cluster: Cluster = self.get(&format!("v1/clusters/{cluster_id}")).await?;
        self.ensure_cluster_allowed(cluster.id)?;
        Ok(cluster)
    }

    /// Reads one incident and its bounded context.
    ///
    /// # Errors
    ///
    /// Returns a normal read error or rejects a response whose cluster is
    /// outside the configured allowlist.
    pub async fn incident(&self, incident_id: IncidentId) -> Result<IncidentView, ClientError> {
        let view: IncidentView = self.get(&format!("v1/incidents/{incident_id}")).await?;
        self.ensure_cluster_allowed(view.incident.cluster_id)?;
        Ok(view)
    }

    /// Reads one inspection and its recommendations.
    ///
    /// # Errors
    ///
    /// Returns a normal read error or rejects a response whose cluster is
    /// outside the configured allowlist.
    pub async fn inspection(&self, inspection_id: InspectionRunId) -> Result<InspectionView, ClientError> {
        let view: InspectionView = self.get(&format!("v1/inspections/{inspection_id}")).await?;
        self.ensure_cluster_allowed(view.run.cluster_id)?;
        Ok(view)
    }

    /// Reads one typed action plan and its status. This method cannot approve
    /// or submit the plan.
    ///
    /// # Errors
    ///
    /// Returns a normal read error or rejects a response whose cluster is
    /// outside the configured allowlist.
    pub async fn plan(&self, plan_id: ActionPlanId) -> Result<ActionPlanView, ClientError> {
        let view: ActionPlanView = self.get(&format!("v1/plans/{plan_id}")).await?;
        self.ensure_cluster_allowed(view.plan.cluster_id)?;
        Ok(view)
    }

    fn endpoint(&self, path: &str) -> Result<Url, ClientError> {
        self.base_url
            .join(path)
            .map_err(|error| ClientError::InvalidBaseUrl(error.to_string()))
    }

    fn ensure_cluster_allowed(&self, cluster_id: ClusterId) -> Result<(), ClientError> {
        if self
            .allowed_clusters
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&cluster_id))
        {
            return Err(ClientError::ClusterNotAllowed { cluster_id });
        }
        Ok(())
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let response = self.http.request(Method::GET, self.endpoint(path)?).send().await?;
        let status = response.status();
        let body = self.read_bounded(response).await?;
        if status.is_success() {
            return serde_json::from_slice(&body).map_err(ClientError::Decode);
        }

        let envelope = serde_json::from_slice::<ErrorEnvelope>(&body).ok();
        Err(ClientError::Api {
            status: status.as_u16(),
            code: envelope
                .as_ref()
                .map_or_else(|| "http_error".to_owned(), |value| value.code.clone()),
            message: envelope.as_ref().map_or_else(
                || format!("request failed with HTTP status {}", status.as_u16()),
                |value| value.message.clone(),
            ),
            retryable: envelope.as_ref().is_some_and(|value| value.retryable),
            correlation_id: envelope.and_then(|value| value.correlation_id),
        })
    }

    async fn read_bounded(&self, mut response: reqwest::Response) -> Result<Vec<u8>, ClientError> {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            let next_len = body.len().saturating_add(chunk.len());
            if next_len > self.max_response_bytes {
                return Err(ClientError::ResponseTooLarge {
                    limit: self.max_response_bytes,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_normalizes_a_base_path_and_disables_credential_urls() {
        let client = Client::builder("https://sre.example.test/control-plane")
            .expect("valid base URL")
            .build()
            .expect("client");
        assert_eq!(
            client.endpoint("v1/clusters").expect("endpoint").as_str(),
            "https://sre.example.test/control-plane/v1/clusters"
        );

        assert!(matches!(
            Client::builder("https://operator:secret@sre.example.test"),
            Err(ClientError::InvalidBaseUrl(_))
        ));
        assert!(matches!(
            Client::builder("file:///tmp/control-plane.sock"),
            Err(ClientError::InvalidBaseUrl(_))
        ));
    }

    #[test]
    fn explicit_empty_allowlist_fails_closed() {
        let client = Client::builder("https://sre.example.test")
            .expect("valid base URL")
            .allowed_clusters([])
            .build()
            .expect("client");
        let cluster_id = ClusterId::new();
        assert!(matches!(
            client.ensure_cluster_allowed(cluster_id),
            Err(ClientError::ClusterNotAllowed {
                cluster_id: denied
            }) if denied == cluster_id
        ));
    }

    #[test]
    fn token_with_header_injection_is_rejected_without_echoing_the_value() {
        let error = Client::builder("https://sre.example.test")
            .expect("valid base URL")
            .bearer_token("secret\r\nx-leak: yes")
            .build()
            .err()
            .expect("invalid bearer token");
        assert!(matches!(error, ClientError::InvalidBearerToken));
        assert!(!error.to_string().contains("secret"));
    }
}
