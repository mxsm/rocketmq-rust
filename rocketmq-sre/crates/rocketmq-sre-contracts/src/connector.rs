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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::ConnectorSessionId;
use crate::CorrelationId;
use crate::EvidenceQuery;
use crate::EvidenceSnapshot;
use crate::SchemaVersion;
use crate::TenantId;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_capabilities_fail_closed_on_mutation_support() {
        let state = ConnectorCapabilityState {
            mutation_supported: true,
            sources: Vec::new(),
        };
        assert!(state.validate_read_only().is_err());
    }
}
