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
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ConnectorQueryEnvelope;
use rocketmq_sre_contracts::ConnectorResponseEnvelope;
use rocketmq_sre_contracts::ConnectorSessionId;
use rocketmq_sre_contracts::ContractError;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::SchemaVersion;
use rocketmq_sre_contracts::TenantId;
use serde::Deserialize;
use serde::Serialize;

use crate::ControlPlaneError;

pub(crate) const CHANNEL_SCHEMA_FAMILY: &str = "rocketmq-sre.connector-channel";
pub(crate) const CHANNEL_SCHEMA_MAJOR: u16 = 1;
pub(crate) const MAX_COMMANDS_PER_POLL: usize = 64;
pub(crate) const MAX_POLL_WAIT_MILLIS: u64 = 30_000;
pub(crate) const MAX_RESPONSE_BYTES: usize = 512 * 1024;
pub(crate) const MAX_SOURCES: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectorPrincipal {
    pub subject: String,
    pub issuer: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SessionScope {
    pub session_id: ConnectorSessionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub subject: String,
    pub issuer: String,
    pub last_heartbeat_at: DateTime<Utc>,
    pub queryable_sources: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConnectorLiveness {
    Online,
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ConnectorChannelStatus {
    pub session_id: ConnectorSessionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub liveness: ConnectorLiveness,
    pub last_heartbeat_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct RegisterAcknowledgement {
    pub schema: SchemaVersion,
    pub accepted: bool,
    pub resume_after_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub(crate) struct PollRequest {
    pub schema: SchemaVersion,
    pub session_id: ConnectorSessionId,
    pub after_sequence: u64,
    pub wait_millis: u64,
    pub max_commands: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct PollResponse {
    pub schema: SchemaVersion,
    pub commands: Vec<ConnectorCommand>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ConnectorCommand {
    Query {
        envelope: ConnectorQueryEnvelope,
    },
    Cancel {
        schema: SchemaVersion,
        session_id: ConnectorSessionId,
        correlation_id: CorrelationId,
        sequence: u64,
    },
}

impl ConnectorCommand {
    #[must_use]
    pub(crate) fn sequence(&self) -> u64 {
        match self {
            Self::Query { envelope } => envelope.sequence,
            Self::Cancel { sequence, .. } => *sequence,
        }
    }

    #[must_use]
    pub(crate) fn correlation_id(&self) -> CorrelationId {
        match self {
            Self::Query { envelope } => envelope.correlation_id,
            Self::Cancel { correlation_id, .. } => *correlation_id,
        }
    }

    #[must_use]
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Query { .. } => "query",
            Self::Cancel { .. } => "cancel",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseDisposition {
    Inserted,
    Duplicate,
}

pub(crate) fn channel_schema() -> SchemaVersion {
    SchemaVersion::new(CHANNEL_SCHEMA_FAMILY, CHANNEL_SCHEMA_MAJOR, 0)
}

pub(crate) fn validate_channel_schema(schema: &SchemaVersion) -> Result<(), ControlPlaneError> {
    schema
        .ensure_compatible(
            CHANNEL_SCHEMA_FAMILY,
            CHANNEL_SCHEMA_MAJOR,
            &BTreeSet::from(["cancel".to_owned(), "reverse_poll".to_owned()]),
        )
        .map_err(contract_compatibility_error)
}

pub(crate) fn validate_poll_request(
    path_session_id: ConnectorSessionId,
    request: &PollRequest,
) -> Result<(), ControlPlaneError> {
    validate_channel_schema(&request.schema)?;
    if request.session_id != path_session_id {
        return Err(ControlPlaneError::forbidden(
            "capability_mismatch",
            "connector session does not match the requested channel",
        ));
    }
    if request.max_commands == 0 || request.max_commands > MAX_COMMANDS_PER_POLL {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "max_commands must be between 1 and 64",
        ));
    }
    if request.wait_millis > MAX_POLL_WAIT_MILLIS {
        return Err(ControlPlaneError::validation(
            "capability_mismatch",
            "wait_millis must not exceed 30000",
        ));
    }
    Ok(())
}

pub(crate) fn validate_response(
    path_session_id: ConnectorSessionId,
    scope: &SessionScope,
    response: &ConnectorResponseEnvelope,
) -> Result<(), ControlPlaneError> {
    validate_channel_schema(&response.schema)?;
    if response.session_id != path_session_id || response.session_id != scope.session_id {
        return Err(ControlPlaneError::forbidden(
            "capability_mismatch",
            "connector response session does not match the authenticated channel",
        ));
    }
    if response.evidence.is_some() == response.error_code.is_some() {
        return Err(ControlPlaneError::validation(
            "capability_mismatch",
            "connector response must contain exactly one of evidence or error_code",
        ));
    }
    if response
        .error_code
        .as_ref()
        .is_some_and(|code| code.is_empty() || code.len() > 128 || !is_stable_code(code))
    {
        return Err(ControlPlaneError::validation(
            "capability_mismatch",
            "connector error_code must be a bounded snake_case identifier",
        ));
    }
    if let Some(evidence) = &response.evidence {
        if evidence.tenant_id != scope.tenant_id {
            return Err(ControlPlaneError::forbidden(
                "tenant_mismatch",
                "connector evidence crosses the registered tenant boundary",
            ));
        }
        if evidence.cluster_id != scope.cluster_id {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "connector evidence crosses the registered cluster boundary",
            ));
        }
        evidence.verify_content_hash().map_err(|_| {
            ControlPlaneError::validation("invalid_content_hash", "connector evidence content hash is invalid")
        })?;
    }
    let bytes = serde_json::to_vec(response)
        .map_err(|_| ControlPlaneError::validation("capability_mismatch", "connector response cannot be serialized"))?;
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "connector response exceeds the 512 KiB channel bound",
        ));
    }
    Ok(())
}

fn contract_compatibility_error(error: ContractError) -> ControlPlaneError {
    match error {
        ContractError::UnsupportedSchemaFamily { .. } | ContractError::UnsupportedSchemaMajor { .. } => {
            ControlPlaneError::validation(
                "unsupported_schema_major",
                "connector channel schema family or major is unsupported",
            )
        }
        ContractError::MissingRequiredFeature { .. } => ControlPlaneError::validation(
            "missing_required_feature",
            "connector channel requires an unsupported feature",
        ),
        _ => ControlPlaneError::validation("capability_mismatch", "connector channel schema is invalid"),
    }
}

fn is_stable_code(code: &str) -> bool {
    code.bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_bounds_fail_closed() {
        let session_id = ConnectorSessionId::new();
        let too_many = PollRequest {
            schema: channel_schema(),
            session_id,
            after_sequence: 0,
            wait_millis: 0,
            max_commands: MAX_COMMANDS_PER_POLL + 1,
        };
        assert!(validate_poll_request(session_id, &too_many).is_err());

        let too_long = PollRequest {
            max_commands: 1,
            wait_millis: MAX_POLL_WAIT_MILLIS + 1,
            ..too_many
        };
        assert!(validate_poll_request(session_id, &too_long).is_err());
    }

    #[test]
    fn unknown_schema_major_is_rejected() {
        let schema = SchemaVersion::new(CHANNEL_SCHEMA_FAMILY, CHANNEL_SCHEMA_MAJOR + 1, 0);
        assert!(validate_channel_schema(&schema).is_err());
    }
}
