// Copyright 2023 The RocketMQ Rust Authors
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

use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ControlPlaneError;
use crate::MCP_BUSINESS_SCHEMA;
use crate::MCP_PROTOCOL_VERSION;

/// Persisted cluster onboarding state.
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

impl OnboardingState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Offboarded)
    }
}

impl Display for OnboardingState {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Pending => "pending",
            Self::Handshaking => "handshaking",
            Self::ReadyReadOnly => "ready_read_only",
            Self::ReadOnlyDegraded => "read_only_degraded",
            Self::Rejected => "rejected",
            Self::Offboarded => "offboarded",
        };
        formatter.write_str(value)
    }
}

impl FromStr for OnboardingState {
    type Err = ControlPlaneError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "handshaking" => Ok(Self::Handshaking),
            "ready_read_only" => Ok(Self::ReadyReadOnly),
            "read_only_degraded" => Ok(Self::ReadOnlyDegraded),
            "rejected" => Ok(Self::Rejected),
            "offboarded" => Ok(Self::Offboarded),
            _ => Err(ControlPlaneError::configuration(
                "database contains an unknown onboarding state",
            )),
        }
    }
}

/// Request used to register a read-only RocketMQ cluster.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OnboardClusterRequest {
    #[serde(default)]
    pub cluster_id: Option<ClusterId>,
    pub tenant_id: String,
    pub external_cluster_key: String,
    pub environment: String,
    pub region: String,
    pub rocketmq_version: String,
    pub deployment_mode: String,
    pub owner: String,
    #[serde(default = "default_actor")]
    pub actor_subject: String,
    #[serde(default)]
    pub correlation_id: Option<CorrelationId>,
}

impl OnboardClusterRequest {
    pub(crate) fn validate(&self) -> Result<(), ControlPlaneError> {
        for (name, value) in [
            ("tenant_id", &self.tenant_id),
            ("external_cluster_key", &self.external_cluster_key),
            ("environment", &self.environment),
            ("region", &self.region),
            ("rocketmq_version", &self.rocketmq_version),
            ("deployment_mode", &self.deployment_mode),
            ("owner", &self.owner),
            ("actor_subject", &self.actor_subject),
        ] {
            if value.trim().is_empty() {
                return Err(ControlPlaneError::validation(
                    "capability_mismatch",
                    format!("{name} must not be empty"),
                ));
            }
        }
        Ok(())
    }
}

fn default_actor() -> String {
    "rocketmq-sre-control-plane".to_owned()
}

/// Cluster view returned by list and detail endpoints.
#[derive(Clone, Debug, PartialEq, Serialize)]
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
    pub effective_access_profile: &'static str,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offboarded_at: Option<DateTime<Utc>>,
}

pub type ClusterSummary = Cluster;

/// Availability state from the required-signal manifests.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSourceAvailability {
    Existing,
    MissingInstrumentation,
    InProcessOnly,
    Queryable,
}

/// Bounded data-source state attached to a capability handshake.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DataSourceStatus {
    pub id: String,
    pub availability: DataSourceAvailability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Wire representation of a successfully persisted MCP capability snapshot.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CapabilitySnapshot {
    pub cluster_id: ClusterId,
    pub digest: String,
    pub tool_surface_digest: String,
    pub protocol_version: String,
    pub schema_version: String,
    pub mutation_supported: bool,
    pub observed_at: DateTime<Utc>,
    pub data_sources: Vec<DataSourceStatus>,
    #[serde(skip_serializing_if = "Value::is_null")]
    pub manifest: Value,
}

/// Capability data supplied by a connector after its black-box MCP handshake.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HandshakeCapability {
    pub digest: String,
    pub protocol_version: String,
    pub schema_version: String,
    pub mutation_supported: bool,
    #[serde(default)]
    pub manifest: Value,
    #[serde(default)]
    pub data_sources: Vec<DataSourceStatus>,
    #[serde(default = "Utc::now")]
    pub observed_at: DateTime<Utc>,
}

impl HandshakeCapability {
    pub(crate) fn tool_surface_digest(&self) -> Result<&str, ControlPlaneError> {
        let digest = self
            .manifest
            .get("tool_surface_digest")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ControlPlaneError::validation(
                    "schema_digest_mismatch",
                    "capability manifest must include a tool surface digest",
                )
            })?;
        if !is_sha256_digest(digest) {
            return Err(ControlPlaneError::validation(
                "schema_digest_mismatch",
                "capability manifest tool surface digest is malformed",
            ));
        }
        Ok(digest)
    }
}

/// Connector compatibility report accepted by the control plane.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HandshakeRequest {
    pub connector_subject: String,
    pub connector_issuer: String,
    #[serde(default)]
    pub correlation_id: Option<CorrelationId>,
    pub capability: HandshakeCapability,
    #[serde(default = "default_compatible")]
    pub compatible: bool,
    #[serde(default)]
    pub incompatibility_code: Option<String>,
}

fn default_compatible() -> bool {
    true
}

impl HandshakeRequest {
    pub(crate) fn validate(&self) -> Result<HandshakeDecision, ControlPlaneError> {
        if self.connector_subject.trim().is_empty() || self.connector_issuer.trim().is_empty() {
            return Err(ControlPlaneError::validation(
                "unauthorized_scope",
                "connector subject and issuer must not be empty",
            ));
        }
        if self.capability.digest.trim().is_empty() {
            return Err(ControlPlaneError::validation(
                "schema_digest_mismatch",
                "capability digest must not be empty",
            ));
        }
        self.capability.tool_surface_digest()?;
        if self.capability.mutation_supported {
            return Ok(HandshakeDecision {
                state: OnboardingState::Rejected,
                reason: Some("MCP advertised mutation support".to_owned()),
                persist_capability: false,
            });
        }
        if self.capability.protocol_version != MCP_PROTOCOL_VERSION {
            return Ok(HandshakeDecision {
                state: OnboardingState::Rejected,
                reason: Some("MCP protocol version is unsupported".to_owned()),
                persist_capability: true,
            });
        }
        if self.capability.schema_version != MCP_BUSINESS_SCHEMA {
            return Ok(HandshakeDecision {
                state: OnboardingState::Rejected,
                reason: Some("MCP business schema major is unsupported".to_owned()),
                persist_capability: true,
            });
        }
        if !self.compatible {
            return Ok(HandshakeDecision {
                state: OnboardingState::ReadOnlyDegraded,
                reason: self
                    .incompatibility_code
                    .clone()
                    .or_else(|| Some("capability manifest drift".to_owned())),
                persist_capability: true,
            });
        }
        Ok(HandshakeDecision {
            state: OnboardingState::ReadyReadOnly,
            reason: None,
            persist_capability: true,
        })
    }
}

fn is_sha256_digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

#[derive(Clone, Debug)]
pub(crate) struct HandshakeDecision {
    pub state: OnboardingState,
    pub reason: Option<String>,
    pub persist_capability: bool,
}

/// Idempotent offboarding request. Historical evidence is retained.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OffboardRequest {
    #[serde(default = "default_actor")]
    pub actor_subject: String,
    #[serde(default)]
    pub correlation_id: Option<CorrelationId>,
    #[serde(default)]
    pub reason: Option<String>,
}

impl Default for OffboardRequest {
    fn default() -> Self {
        Self {
            actor_subject: default_actor(),
            correlation_id: None,
            reason: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OnboardOutcome {
    pub cluster: Cluster,
    pub created: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct HandshakeOutcome {
    pub cluster: Cluster,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability: Option<CapabilitySnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn handshake() -> HandshakeRequest {
        HandshakeRequest {
            connector_subject: "connector-a".to_owned(),
            connector_issuer: "https://issuer.example".to_owned(),
            correlation_id: None,
            capability: HandshakeCapability {
                digest: format!("sha256:{}", "a".repeat(64)),
                protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
                schema_version: MCP_BUSINESS_SCHEMA.to_owned(),
                mutation_supported: false,
                manifest: json!({
                    "tool_surface_digest": format!("sha256:{}", "b".repeat(64))
                }),
                data_sources: Vec::new(),
                observed_at: Utc::now(),
            },
            compatible: true,
            incompatibility_code: None,
        }
    }

    #[test]
    fn mutation_capability_is_rejected() {
        let mut request = handshake();
        request.capability.mutation_supported = true;

        let decision = request.validate().expect("request shape is valid");

        assert_eq!(decision.state, OnboardingState::Rejected);
        assert!(!decision.persist_capability);
    }

    #[test]
    fn digest_drift_is_read_only_degraded() {
        let mut request = handshake();
        request.compatible = false;
        request.incompatibility_code = Some("schema_digest_mismatch".to_owned());

        let decision = request.validate().expect("request shape is valid");

        assert_eq!(decision.state, OnboardingState::ReadOnlyDegraded);
        assert!(decision.persist_capability);
    }

    #[test]
    fn malformed_tool_surface_digest_fails_closed() {
        let mut request = handshake();
        request.capability.manifest["tool_surface_digest"] = json!("sha256:not-a-digest");

        let error = request.validate().expect_err("malformed digest must fail closed");

        assert!(matches!(
            error,
            ControlPlaneError::Validation {
                code: "schema_digest_mismatch",
                ..
            }
        ));
    }
}
