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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Stable gateway error classification.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderErrorCode {
    InvalidRequest,
    AuthenticationFailed,
    AuthorizationFailed,
    PolicyDenied,
    SafetyRefusal,
    CapabilityUnsupported,
    DataResidencyDenied,
    Timeout,
    Cancelled,
    RateLimited,
    ServiceUnavailable,
    TransportFailed,
    ProtocolError,
    OutputTooLarge,
    StreamBackpressure,
    SchemaValidationFailed,
    SecretUnavailable,
    SecretAccessDenied,
    UnsupportedWireVersion,
    MutualTlsFailed,
    ProfileInvalid,
}

impl ProviderErrorCode {
    /// Whether a caller may retry the same provider after backoff.
    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::Timeout
                | Self::RateLimited
                | Self::ServiceUnavailable
                | Self::TransportFailed
                | Self::StreamBackpressure
        )
    }

    /// Whether the router may move to another eligible provider profile.
    ///
    /// Deliberately narrower than generic retryability: fallback is limited to
    /// timeout, rate limit, and service-unavailable failures.
    #[must_use]
    pub const fn fallback_allowed(self) -> bool {
        matches!(self, Self::Timeout | Self::RateLimited | Self::ServiceUnavailable)
    }
}

/// Redacted model gateway error.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProviderError {
    pub code: ProviderErrorCode,
    pub message: String,
    pub retryable: bool,
    pub provider_status: Option<u16>,
}

impl ProviderError {
    /// Creates a stable, redacted gateway error.
    #[must_use]
    pub fn new(code: ProviderErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.retryable(),
            provider_status: None,
        }
    }

    /// Attaches a non-sensitive HTTP-like status.
    #[must_use]
    pub const fn with_provider_status(mut self, status: u16) -> Self {
        self.provider_status = Some(status);
        self
    }

    /// Whether this failure may trigger the router's limited fallback policy.
    #[must_use]
    pub const fn fallback_allowed(&self) -> bool {
        self.code.fallback_allowed()
    }

    /// Creates a timeout error.
    #[must_use]
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(ProviderErrorCode::Timeout, message)
    }

    /// Creates a policy-denial error.
    #[must_use]
    pub fn policy_denied(message: impl Into<String>) -> Self {
        Self::new(ProviderErrorCode::PolicyDenied, message)
    }

    /// Creates a service-unavailable error.
    #[must_use]
    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::new(ProviderErrorCode::ServiceUnavailable, message)
    }

    /// Creates an unsupported-capability error.
    #[must_use]
    pub fn capability_unsupported(message: impl Into<String>) -> Self {
        Self::new(ProviderErrorCode::CapabilityUnsupported, message)
    }
}

impl Display for ProviderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ProviderError {}

/// Maps a provider HTTP-like status to the stable gateway taxonomy.
#[must_use]
pub fn map_provider_status(status: u16) -> ProviderError {
    let (code, message) = match status {
        400 | 404 | 409 | 422 => (ProviderErrorCode::InvalidRequest, "provider rejected the request"),
        401 => (
            ProviderErrorCode::AuthenticationFailed,
            "provider authentication failed",
        ),
        403 => (ProviderErrorCode::PolicyDenied, "provider policy denied the request"),
        408 | 504 => (ProviderErrorCode::Timeout, "provider request timed out"),
        429 => (ProviderErrorCode::RateLimited, "provider rate limit exceeded"),
        500..=599 => (ProviderErrorCode::ServiceUnavailable, "provider service unavailable"),
        _ => (
            ProviderErrorCode::ProtocolError,
            "provider returned an unexpected status",
        ),
    };
    ProviderError::new(code, message).with_provider_status(status)
}
