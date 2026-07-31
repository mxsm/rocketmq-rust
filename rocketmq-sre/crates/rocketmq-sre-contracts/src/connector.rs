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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ClusterId;
use crate::ConnectorSessionId;
use crate::CorrelationId;
use crate::EvidenceQuery;
use crate::EvidenceSnapshot;
use crate::SchemaVersion;
use crate::TenantId;

/// Schema carried by a bounded component Required Signals evidence document.
pub const REQUIRED_SIGNALS_EVIDENCE_SCHEMA_VERSION: &str = "rocketmq.sre.required-signals-evidence.v1";

/// Current availability reported for a connector-backed evidence source.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorSourceStatus {
    Queryable,
    Degraded,
    Missing,
    Unsupported,
}

/// One bounded evidence source advertised by a connector.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConnectorSourceCapability {
    pub source: String,
    pub schema_major: u32,
    pub status: ConnectorSourceStatus,
    pub max_rows: u32,
    pub max_bytes: u64,
    pub max_time_range_seconds: u64,
    pub last_success_at: Option<DateTime<Utc>>,
    pub freshness_seconds: Option<u64>,
}

/// Capability state sent during registration and heartbeat.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConnectorCapabilityState {
    pub mutation_supported: bool,
    pub sources: Vec<ConnectorSourceCapability>,
}

/// Signal kind declared by a Required Signals manifest.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredSignalType {
    Metric,
    Log,
    Span,
    Resource,
}

/// Result of one fixed, bounded Required Signal query.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredSignalStatus {
    Available,
    Missing,
    NotProductionVerified,
}

/// One sanitized signal observation in a component Required Signals document.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RequiredSignalObservation {
    pub requirement_id: String,
    pub registry_reference: String,
    pub signal_type: RequiredSignalType,
    pub query_source: String,
    pub status: RequiredSignalStatus,
    pub observed_at: Option<DateTime<Utc>>,
    pub partial: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub content: Option<Value>,
    pub reason_code: Option<String>,
}

/// Versioned aggregate returned for one RocketMQ component.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RequiredSignalsEvidenceV1 {
    pub schema_version: String,
    pub component: String,
    pub observed_at: DateTime<Utc>,
    pub partial: bool,
    pub observations: Vec<RequiredSignalObservation>,
}

/// Versioned connector registration request.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConnectorRegister {
    pub schema: SchemaVersion,
    pub session_id: ConnectorSessionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub subject: String,
    pub capability: ConnectorCapabilityState,
    pub observed_at: DateTime<Utc>,
}

/// Versioned connector liveness and capability refresh.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConnectorHeartbeat {
    pub schema: SchemaVersion,
    pub session_id: ConnectorSessionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub capability: ConnectorCapabilityState,
    pub observed_at: DateTime<Utc>,
}

/// Query sent by the control plane over the connector channel.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConnectorQueryEnvelope {
    pub schema: SchemaVersion,
    pub session_id: ConnectorSessionId,
    pub correlation_id: CorrelationId,
    pub sequence: u64,
    pub deadline: DateTime<Utc>,
    pub query: EvidenceQuery,
}

/// Response returned by the connector. Missing evidence is represented by a
/// stable error code, never by a fabricated zero-valued snapshot.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ConnectorResponseEnvelope {
    pub schema: SchemaVersion,
    pub session_id: ConnectorSessionId,
    pub correlation_id: CorrelationId,
    pub sequence: u64,
    pub evidence: Option<EvidenceSnapshot>,
    pub error_code: Option<String>,
    pub retryable: bool,
}

impl ConnectorCapabilityState {
    /// Enforces the read-only connector invariant.
    ///
    /// # Errors
    ///
    /// Returns a contract error when a connector advertises mutation support.
    pub fn validate_read_only(&self) -> Result<(), crate::ContractError> {
        if self.mutation_supported {
            return Err(crate::ContractError::InvalidDescriptor {
                reason: "connector capability must keep mutation_supported=false".to_owned(),
            });
        }
        Ok(())
    }
}

impl RequiredSignalsEvidenceV1 {
    /// Validates the fail-closed Required Signals evidence contract.
    ///
    /// An available observation must carry content and no failure reason.
    /// Missing or unverified observations must carry a stable reason and no
    /// content, so absent measurements cannot be represented as a fabricated
    /// zero.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ContractError::InvalidDescriptor`] when the schema,
    /// component, identifiers, or status payloads are inconsistent.
    pub fn validate(&self) -> Result<(), crate::ContractError> {
        if self.schema_version != REQUIRED_SIGNALS_EVIDENCE_SCHEMA_VERSION {
            return Err(invalid_required_signals("unsupported Required Signals evidence schema"));
        }
        if self.component.trim().is_empty() {
            return Err(invalid_required_signals("Required Signals evidence component is empty"));
        }
        if self.observations.is_empty() {
            return Err(invalid_required_signals(
                "Required Signals evidence has no observations",
            ));
        }

        let mut requirement_ids = BTreeSet::new();
        for observation in &self.observations {
            if observation.requirement_id.trim().is_empty()
                || observation.registry_reference.trim().is_empty()
                || observation.query_source.trim().is_empty()
            {
                return Err(invalid_required_signals(
                    "Required Signal identifiers and query source must be non-empty",
                ));
            }
            if !requirement_ids.insert(observation.requirement_id.as_str()) {
                return Err(invalid_required_signals(
                    "Required Signals evidence contains a duplicate requirement",
                ));
            }
            match observation.status {
                RequiredSignalStatus::Available
                    if observation.content.is_some() && observation.reason_code.is_none() => {}
                RequiredSignalStatus::Missing | RequiredSignalStatus::NotProductionVerified
                    if observation.content.is_none()
                        && observation
                            .reason_code
                            .as_deref()
                            .is_some_and(|reason| !reason.trim().is_empty()) => {}
                _ => {
                    return Err(invalid_required_signals(
                        "Required Signal status does not match its content and reason",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn invalid_required_signals(reason: &str) -> crate::ContractError {
    crate::ContractError::InvalidDescriptor {
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    #[test]
    fn connector_capabilities_fail_closed_on_mutation_support() {
        let state = ConnectorCapabilityState {
            mutation_supported: true,
            sources: Vec::new(),
        };
        assert!(state.validate_read_only().is_err());
    }

    #[test]
    fn required_signals_contract_distinguishes_missing_from_zero() {
        let observed_at = Utc
            .with_ymd_and_hms(2026, 7, 29, 0, 0, 0)
            .single()
            .expect("valid timestamp");
        let evidence = RequiredSignalsEvidenceV1 {
            schema_version: REQUIRED_SIGNALS_EVIDENCE_SCHEMA_VERSION.to_owned(),
            component: "broker".to_owned(),
            observed_at,
            partial: true,
            observations: vec![
                RequiredSignalObservation {
                    requirement_id: "broker.availability".to_owned(),
                    registry_reference: "rocketmq_broker_up".to_owned(),
                    signal_type: RequiredSignalType::Metric,
                    query_source: "prometheus".to_owned(),
                    status: RequiredSignalStatus::Available,
                    observed_at: Some(observed_at),
                    partial: false,
                    warnings: Vec::new(),
                    content: Some(json!({"samples": [{"value": 1}]})),
                    reason_code: None,
                },
                RequiredSignalObservation {
                    requirement_id: "broker.ha_replication_lag".to_owned(),
                    registry_reference: "rocketmq_store_ha_replication_lag_bytes".to_owned(),
                    signal_type: RequiredSignalType::Metric,
                    query_source: "prometheus".to_owned(),
                    status: RequiredSignalStatus::Missing,
                    observed_at: None,
                    partial: false,
                    warnings: Vec::new(),
                    content: None,
                    reason_code: Some("prometheus_series_missing".to_owned()),
                },
            ],
        };

        evidence.validate().expect("valid Required Signals evidence");

        let mut fabricated_zero = evidence;
        fabricated_zero.observations[1].content = Some(json!({"value": 0}));
        assert!(fabricated_zero.validate().is_err());
    }

    #[test]
    fn required_signals_contract_rejects_duplicate_requirements() {
        let observed_at = Utc::now();
        let observation = RequiredSignalObservation {
            requirement_id: "mcp.runtime_resource".to_owned(),
            registry_reference: "rocketmq://system/runtime/v1".to_owned(),
            signal_type: RequiredSignalType::Resource,
            query_source: "runtime".to_owned(),
            status: RequiredSignalStatus::NotProductionVerified,
            observed_at: None,
            partial: false,
            warnings: Vec::new(),
            content: None,
            reason_code: Some("runtime_resource_unavailable".to_owned()),
        };
        let evidence = RequiredSignalsEvidenceV1 {
            schema_version: REQUIRED_SIGNALS_EVIDENCE_SCHEMA_VERSION.to_owned(),
            component: "mcp".to_owned(),
            observed_at,
            partial: true,
            observations: vec![observation.clone(), observation],
        };

        assert!(evidence.validate().is_err());
    }
}
