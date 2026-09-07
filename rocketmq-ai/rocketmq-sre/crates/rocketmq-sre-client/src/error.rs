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

use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::CorrelationId;

/// Closed client failure classification suitable for UI and CLI decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ClientFailureCode {
    InvalidBaseUrl,
    InvalidBearerToken,
    InvalidResponseLimit,
    ClusterNotAllowed,
    ResponseTooLarge,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    RateLimited,
    ContractRejected,
    ServiceUnavailable,
    Operational,
}

impl ClientFailureCode {
    /// Returns the stable lowercase identifier used by presentation layers.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidBaseUrl => "invalid_base_url",
            Self::InvalidBearerToken => "invalid_bearer_token",
            Self::InvalidResponseLimit => "invalid_response_limit",
            Self::ClusterNotAllowed => "cluster_not_allowed",
            Self::ResponseTooLarge => "response_too_large",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::RateLimited => "rate_limited",
            Self::ContractRejected => "contract_rejected",
            Self::ServiceUnavailable => "service_unavailable",
            Self::Operational => "operational_failure",
        }
    }
}

/// Opaque operational client error.
///
/// Display and Debug intentionally expose only a fixed classification. Use
/// [`Error::source`] for typed diagnostics inside the trusted process boundary.
pub struct ClientError {
    kind: ClientErrorKind,
}

enum ClientErrorKind {
    Transport(reqwest::Error),
    Decode(serde_json::Error),
}

impl ClientError {
    pub(crate) fn transport(source: reqwest::Error) -> Self {
        Self {
            kind: ClientErrorKind::Transport(source),
        }
    }

    pub(crate) fn decode(source: serde_json::Error) -> Self {
        Self {
            kind: ClientErrorKind::Decode(source),
        }
    }

    const fn label(&self) -> &'static str {
        match self.kind {
            ClientErrorKind::Transport(_) => "transport",
            ClientErrorKind::Decode(_) => "decode",
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ClientErrorKind::Transport(_) => "Control Plane transport failed",
            ClientErrorKind::Decode(_) => "Control Plane response decoding failed",
        })
    }
}

impl fmt::Debug for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientError")
            .field("kind", &self.label())
            .finish()
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ClientErrorKind::Transport(source) => Some(source),
            ClientErrorKind::Decode(source) => Some(source),
        }
    }
}

/// A closed client rejection or an opaque operational failure.
///
/// This type deliberately does not implement [`Error`]. Callers must handle
/// expected policy, input, HTTP, and size outcomes explicitly.
pub struct ClientFailure {
    kind: ClientFailureKind,
}

enum ClientFailureKind {
    Rejected {
        code: ClientFailureCode,
        status: Option<u16>,
        retryable: bool,
        correlation_id: Option<CorrelationId>,
    },
    Operational(ClientError),
}

impl ClientFailure {
    pub(crate) const fn rejected(code: ClientFailureCode) -> Self {
        Self {
            kind: ClientFailureKind::Rejected {
                code,
                status: None,
                retryable: false,
                correlation_id: None,
            },
        }
    }

    pub(crate) const fn remote(
        code: ClientFailureCode,
        status: u16,
        retryable: bool,
        correlation_id: Option<CorrelationId>,
    ) -> Self {
        Self {
            kind: ClientFailureKind::Rejected {
                code,
                status: Some(status),
                retryable,
                correlation_id,
            },
        }
    }

    pub(crate) const fn operational(error: ClientError) -> Self {
        Self {
            kind: ClientFailureKind::Operational(error),
        }
    }

    /// Returns the closed classification for programmatic presentation.
    #[must_use]
    pub const fn code(&self) -> ClientFailureCode {
        match &self.kind {
            ClientFailureKind::Rejected { code, .. } => *code,
            ClientFailureKind::Operational(_) => ClientFailureCode::Operational,
        }
    }

    /// Returns the server's HTTP status for a remote rejection.
    #[must_use]
    pub const fn status(&self) -> Option<u16> {
        match self.kind {
            ClientFailureKind::Rejected { status, .. } => status,
            ClientFailureKind::Operational(_) => None,
        }
    }

    /// Indicates whether the server declared a remote rejection retryable.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self.kind {
            ClientFailureKind::Rejected { retryable, .. } => retryable,
            ClientFailureKind::Operational(_) => false,
        }
    }

    /// Returns a validated correlation identifier when one was supplied.
    #[must_use]
    pub const fn correlation_id(&self) -> Option<CorrelationId> {
        match self.kind {
            ClientFailureKind::Rejected { correlation_id, .. } => correlation_id,
            ClientFailureKind::Operational(_) => None,
        }
    }

    /// Returns the typed source for an operational failure.
    #[must_use]
    pub const fn operational_error(&self) -> Option<&ClientError> {
        match &self.kind {
            ClientFailureKind::Rejected { .. } => None,
            ClientFailureKind::Operational(error) => Some(error),
        }
    }

    /// Separates an operational error from a closed rejection without
    /// requiring callers to inspect private representation details.
    pub fn into_operational_error(self) -> Result<ClientError, Self> {
        match self.kind {
            ClientFailureKind::Operational(error) => Ok(error),
            kind @ ClientFailureKind::Rejected { .. } => Err(Self { kind }),
        }
    }

    /// Returns a bounded, non-sensitive message for user-facing output.
    #[must_use]
    pub const fn public_message(&self) -> &'static str {
        match self.code() {
            ClientFailureCode::InvalidBaseUrl => "Control Plane URL is invalid",
            ClientFailureCode::InvalidBearerToken => "bearer token is invalid",
            ClientFailureCode::InvalidResponseLimit => "response byte limit is invalid",
            ClientFailureCode::ClusterNotAllowed => "cluster is outside the configured allowlist",
            ClientFailureCode::ResponseTooLarge => "Control Plane response exceeded the configured limit",
            ClientFailureCode::Unauthorized => "Control Plane authentication failed",
            ClientFailureCode::Forbidden => "Control Plane authorization failed",
            ClientFailureCode::NotFound => "requested Control Plane resource was not found",
            ClientFailureCode::Conflict => "Control Plane request conflicts with current state",
            ClientFailureCode::RateLimited => "Control Plane rate limit was reached",
            ClientFailureCode::ContractRejected => "Control Plane rejected the request",
            ClientFailureCode::ServiceUnavailable => "Control Plane is unavailable",
            ClientFailureCode::Operational => "Control Plane client operation failed",
        }
    }
}

impl fmt::Display for ClientFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.public_message())
    }
}

impl fmt::Debug for ClientFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientFailure")
            .field("code", &self.code())
            .field("status", &self.status())
            .field("retryable", &self.retryable())
            .field("correlation_id", &self.correlation_id())
            .finish()
    }
}

impl From<ClientError> for ClientFailure {
    fn from(error: ClientError) -> Self {
        Self::operational(error)
    }
}

impl From<reqwest::Error> for ClientFailure {
    fn from(source: reqwest::Error) -> Self {
        ClientError::transport(source).into()
    }
}

impl From<serde_json::Error> for ClientFailure {
    fn from(source: serde_json::Error) -> Self {
        ClientError::decode(source).into()
    }
}
