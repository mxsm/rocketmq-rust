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

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rocketmq_sre_contracts::CorrelationId;
use serde::Serialize;

/// Stable, non-error connector failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorFailure {
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
    DeadlineExceeded,
    QueryCancelled,
    RateLimited,
    ChannelUnavailable,
}

/// Closed, non-operational read-admission refusal.
///
/// These outcomes cover policy decisions made before a connector adapter is
/// invoked. Transport, RMCP, I/O, and backend faults remain [`ConnectorError`]
/// values.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ConnectorAdmissionRejection {
    CapabilityMismatch,
    UnauthorizedScope,
    TenantMismatch,
    ClusterNotAllowed,
    InvalidEvidenceQuery,
    DeadlineExceeded,
    QueryCancelled,
    RateLimited,
}

impl ConnectorAdmissionRejection {
    #[must_use]
    pub const fn failure(self) -> ConnectorFailure {
        match self {
            Self::CapabilityMismatch => ConnectorFailure::CapabilityMismatch,
            Self::UnauthorizedScope => ConnectorFailure::UnauthorizedScope,
            Self::TenantMismatch => ConnectorFailure::TenantMismatch,
            Self::ClusterNotAllowed => ConnectorFailure::ClusterNotAllowed,
            Self::InvalidEvidenceQuery => ConnectorFailure::InvalidEvidenceQuery,
            Self::DeadlineExceeded => ConnectorFailure::DeadlineExceeded,
            Self::QueryCancelled => ConnectorFailure::QueryCancelled,
            Self::RateLimited => ConnectorFailure::RateLimited,
        }
    }

    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::DeadlineExceeded | Self::RateLimited)
    }

    #[must_use]
    pub(crate) fn view(self, correlation_id: CorrelationId) -> ConnectorErrorView {
        let failure = self.failure();
        ConnectorErrorView {
            code: failure.as_str(),
            message: failure.public_message(),
            retryable: self.retryable(),
            correlation_id,
        }
    }
}

impl Debug for ConnectorAdmissionRejection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Connector read admission was rejected")
    }
}

impl Display for ConnectorAdmissionRejection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Connector read admission was rejected")
    }
}

/// Result of a connector read after separating expected admission refusal from
/// an operational [`ConnectorError`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorAdmissionOutcome<T> {
    Accepted(T),
    Rejected(ConnectorAdmissionRejection),
}

impl<T> ConnectorAdmissionOutcome<T> {
    #[must_use]
    pub const fn accepted(value: T) -> Self {
        Self::Accepted(value)
    }

    #[must_use]
    pub const fn rejected(rejection: ConnectorAdmissionRejection) -> Self {
        Self::Rejected(rejection)
    }
}

impl ConnectorFailure {
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
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::QueryCancelled => "query_cancelled",
            Self::RateLimited => "rate_limited",
            Self::ChannelUnavailable => "channel_unavailable",
        }
    }

    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::UnauthorizedScope => StatusCode::UNAUTHORIZED,
            Self::TenantMismatch | Self::ClusterNotAllowed => StatusCode::FORBIDDEN,
            Self::OutputTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::SchemaDigestMismatch | Self::CapabilityMismatch => StatusCode::CONFLICT,
            Self::SourceUnavailable | Self::ChannelUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            Self::QueryCancelled => StatusCode::CONFLICT,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::InvalidConfiguration
            | Self::UnsupportedSchemaMajor
            | Self::MissingRequiredFeature
            | Self::InvalidEvidenceQuery => StatusCode::BAD_REQUEST,
        }
    }

    const fn public_message(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "connector configuration is invalid",
            Self::UnsupportedSchemaMajor => "the MCP business schema is not supported",
            Self::MissingRequiredFeature => "a required MCP feature is not supported",
            Self::SchemaDigestMismatch => "the MCP schema digest does not match the verified surface",
            Self::CapabilityMismatch => "the MCP capability surface is incompatible",
            Self::UnauthorizedScope => "the connector identity is not authorized",
            Self::TenantMismatch => "the evidence query tenant does not match the connector tenant",
            Self::ClusterNotAllowed => "the requested cluster is not in the connector allowlist",
            Self::OutputTooLarge => "the MCP response exceeds the configured output bound",
            Self::SourceUnavailable => "the MCP evidence source is unavailable",
            Self::InvalidEvidenceQuery => "the evidence query is invalid",
            Self::DeadlineExceeded => "the evidence query deadline was exceeded",
            Self::QueryCancelled => "the evidence query was cancelled",
            Self::RateLimited => "the connector evidence budget is exhausted",
            Self::ChannelUnavailable => "the control-plane connector channel is unavailable",
        }
    }
}

/// Boundary-safe connector failure view.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ConnectorErrorView {
    pub code: &'static str,
    pub message: &'static str,
    pub retryable: bool,
    pub correlation_id: CorrelationId,
}

#[derive(Clone)]
struct ConnectorErrorInner {
    failure: ConnectorFailure,
    retryable: bool,
    correlation_id: Option<CorrelationId>,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

/// Opaque, redacted connector operational error.
#[derive(Clone)]
pub struct ConnectorError {
    inner: Arc<ConnectorErrorInner>,
}

impl ConnectorError {
    /// Creates an error from a closed connector failure classification.
    #[must_use]
    fn from_failure(failure: ConnectorFailure, retryable: bool) -> Self {
        Self {
            inner: Arc::new(ConnectorErrorInner {
                failure,
                retryable,
                correlation_id: None,
                source: None,
            }),
        }
    }

    pub(crate) fn new(failure: ConnectorFailure, retryable: bool, _detail: impl Into<String>) -> Self {
        Self::from_failure(failure, retryable)
    }

    pub(crate) fn from_source<E>(failure: ConnectorFailure, retryable: bool, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(ConnectorErrorInner {
                failure,
                retryable,
                correlation_id: None,
                source: Some(Arc::new(source)),
            }),
        }
    }

    pub(crate) fn configuration(detail: impl Into<String>) -> Self {
        Self::new(ConnectorFailure::InvalidConfiguration, false, detail)
    }

    pub(crate) fn configuration_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ConnectorFailure::InvalidConfiguration, false, source)
    }

    pub(crate) fn source(detail: impl Into<String>) -> Self {
        Self::new(ConnectorFailure::SourceUnavailable, true, detail)
    }

    pub(crate) fn source_error<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_source(ConnectorFailure::SourceUnavailable, true, source)
    }

    pub(crate) fn capability(failure: ConnectorFailure, detail: impl Into<String>) -> Self {
        Self::new(failure, false, detail)
    }

    #[must_use]
    pub fn with_correlation_id(self, correlation_id: CorrelationId) -> Self {
        Self {
            inner: Arc::new(ConnectorErrorInner {
                correlation_id: Some(correlation_id),
                ..(*self.inner).clone()
            }),
        }
    }

    #[must_use]
    pub fn correlation_id(&self) -> CorrelationId {
        self.inner.correlation_id.unwrap_or_default()
    }

    #[must_use]
    pub fn failure(&self) -> ConnectorFailure {
        self.inner.failure
    }

    #[must_use]
    pub fn retryable(&self) -> bool {
        self.inner.retryable
    }

    #[must_use]
    pub fn sanitized_message(&self) -> &'static str {
        self.inner.failure.public_message()
    }

    #[must_use]
    pub fn view(&self) -> ConnectorErrorView {
        ConnectorErrorView {
            code: self.inner.failure.as_str(),
            message: self.inner.failure.public_message(),
            retryable: self.inner.retryable,
            correlation_id: self.correlation_id(),
        }
    }
}

impl Debug for ConnectorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorError")
            .field("view", &self.view())
            .finish()
    }
}

impl Display for ConnectorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.inner.failure.public_message())
    }
}

impl Error for ConnectorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner
            .source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
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
        let view = self.view();
        let envelope = ErrorEnvelope {
            code: view.code,
            message: view.message,
            retryable: view.retryable,
            correlation_id: view.correlation_id,
        };
        (self.inner.failure.status(), Json(envelope)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn facade_is_small_send_sync_and_preserves_typed_source() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ConnectorError>();
        assert_eq!(std::mem::size_of::<ConnectorError>(), std::mem::size_of::<usize>());
        let error = ConnectorError::from_source(
            ConnectorFailure::SourceUnavailable,
            true,
            serde_json::from_str::<serde_json::Value>("{").expect_err("invalid JSON"),
        );
        assert!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<serde_json::Error>())
                .is_some()
        );
    }

    #[test]
    fn rendering_does_not_expose_source() {
        let error = ConnectorError::from_source(
            ConnectorFailure::SourceUnavailable,
            true,
            std::io::Error::other("secret endpoint and tenant"),
        );
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("endpoint"));
            assert!(!rendered.contains("tenant"));
        }
    }

    #[test]
    fn http_view_preserves_closed_wire_fields() {
        let error = ConnectorError::from_failure(ConnectorFailure::RateLimited, true);
        let view = error.view();
        assert_eq!(error.failure().status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(view.code, "rate_limited");
        assert_eq!(view.message, "the connector evidence budget is exhausted");
        assert!(view.retryable);
    }

    #[test]
    fn admission_rejection_has_a_closed_projection_without_an_error_facade() {
        let correlation_id = CorrelationId::new();
        let view = ConnectorAdmissionRejection::DeadlineExceeded.view(correlation_id);
        assert_eq!(view.code, "deadline_exceeded");
        assert_eq!(view.message, "the evidence query deadline was exceeded");
        assert!(view.retryable);
        assert_eq!(view.correlation_id, correlation_id);
    }
}
