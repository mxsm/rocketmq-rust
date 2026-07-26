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

use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use rocketmq_sre_contracts::CorrelationId;
use serde::Serialize;
use thiserror::Error;

/// Stable connector failure categories exposed by the internal API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorErrorCode {
    InvalidConfiguration,
    UnsupportedSchemaMajor,
    MissingRequiredFeature,
    SchemaDigestMismatch,
    CapabilityMismatch,
    UnauthorizedScope,
    TenantMismatch,
    ClusterNotAllowed,
    OutputTooLarge,
    SourceUnavailable,
    InvalidEvidenceQuery,
}

impl ConnectorErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "invalid_configuration",
            Self::UnsupportedSchemaMajor => "unsupported_schema_major",
            Self::MissingRequiredFeature => "missing_required_feature",
            Self::SchemaDigestMismatch => "schema_digest_mismatch",
            Self::CapabilityMismatch => "capability_mismatch",
            Self::UnauthorizedScope => "unauthorized_scope",
            Self::TenantMismatch => "tenant_mismatch",
            Self::ClusterNotAllowed => "cluster_not_allowed",
            Self::OutputTooLarge => "output_too_large",
            Self::SourceUnavailable => "source_unavailable",
            Self::InvalidEvidenceQuery => "invalid_evidence_query",
        }
    }

    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::UnauthorizedScope => StatusCode::UNAUTHORIZED,
            Self::TenantMismatch | Self::ClusterNotAllowed => StatusCode::FORBIDDEN,
            Self::OutputTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::SchemaDigestMismatch | Self::CapabilityMismatch => StatusCode::CONFLICT,
            Self::SourceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidConfiguration
            | Self::UnsupportedSchemaMajor
            | Self::MissingRequiredFeature
            | Self::InvalidEvidenceQuery => StatusCode::BAD_REQUEST,
        }
    }
}

/// Typed connector error. `detail` is intentionally retained only for local
/// diagnostics; clients receive the stable sanitized message.
#[derive(Debug, Error)]
#[error("{code}: {detail}", code = .code.as_str())]
pub struct ConnectorError {
    pub code: ConnectorErrorCode,
    pub retryable: bool,
    detail: String,
    correlation_id: Option<CorrelationId>,
}

impl ConnectorError {
    #[must_use]
    pub fn new(code: ConnectorErrorCode, retryable: bool, detail: impl Into<String>) -> Self {
        Self {
            code,
            retryable,
            detail: detail.into(),
            correlation_id: None,
        }
    }

    #[must_use]
    pub fn configuration(detail: impl Into<String>) -> Self {
        Self::new(ConnectorErrorCode::InvalidConfiguration, false, detail)
    }

    #[must_use]
    pub fn source(detail: impl Into<String>) -> Self {
        Self::new(ConnectorErrorCode::SourceUnavailable, true, detail)
    }

    #[must_use]
    pub fn capability(code: ConnectorErrorCode, detail: impl Into<String>) -> Self {
        Self::new(code, false, detail)
    }

    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    #[must_use]
    pub fn correlation_id(&self) -> CorrelationId {
        self.correlation_id.unwrap_or_default()
    }

    #[must_use]
    pub fn sanitized_message(&self) -> &'static str {
        match self.code {
            ConnectorErrorCode::InvalidConfiguration => "connector configuration is invalid",
            ConnectorErrorCode::UnsupportedSchemaMajor => "the MCP business schema is not supported",
            ConnectorErrorCode::MissingRequiredFeature => "a required MCP feature is not supported",
            ConnectorErrorCode::SchemaDigestMismatch => "the MCP schema digest does not match the verified surface",
            ConnectorErrorCode::CapabilityMismatch => "the MCP capability surface is incompatible",
            ConnectorErrorCode::UnauthorizedScope => "the connector identity is not authorized",
            ConnectorErrorCode::TenantMismatch => "the evidence query tenant does not match the connector tenant",
            ConnectorErrorCode::ClusterNotAllowed => "the requested cluster is not in the connector allowlist",
            ConnectorErrorCode::OutputTooLarge => "the MCP response exceeds the configured output bound",
            ConnectorErrorCode::SourceUnavailable => "the MCP evidence source is unavailable",
            ConnectorErrorCode::InvalidEvidenceQuery => "the evidence query is invalid",
        }
    }
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    code: &'static str,
    message: &'static str,
    retryable: bool,
    correlation_id: CorrelationId,
}

impl IntoResponse for ConnectorError {
    fn into_response(self) -> Response {
        let status = self.code.status();
        let envelope = ErrorEnvelope {
            code: self.code.as_str(),
            message: self.sanitized_message(),
            retryable: self.retryable,
            correlation_id: self.correlation_id(),
        };
        (status, Json(envelope)).into_response()
    }
}
