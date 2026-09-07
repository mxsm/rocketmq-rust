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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Stable, non-error model-provider failure classification.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailure {
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

/// Failure classifications that represent provider operational errors.
///
/// Expected request refusals are deliberately absent so they cannot be
/// constructed as [`ProviderError`] values.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProviderOperationalFailure {
    Timeout,
    RateLimited,
    ServiceUnavailable,
    TransportFailed,
    ProtocolError,
    OutputTooLarge,
    StreamBackpressure,
    SecretUnavailable,
}

impl ProviderOperationalFailure {
    /// Returns the stable boundary classification.
    #[must_use]
    pub const fn failure(self) -> ProviderFailure {
        match self {
            Self::Timeout => ProviderFailure::Timeout,
            Self::RateLimited => ProviderFailure::RateLimited,
            Self::ServiceUnavailable => ProviderFailure::ServiceUnavailable,
            Self::TransportFailed => ProviderFailure::TransportFailed,
            Self::ProtocolError => ProviderFailure::ProtocolError,
            Self::OutputTooLarge => ProviderFailure::OutputTooLarge,
            Self::StreamBackpressure => ProviderFailure::StreamBackpressure,
            Self::SecretUnavailable => ProviderFailure::SecretUnavailable,
        }
    }
}

impl ProviderFailure {
    const fn public_message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "model provider request was rejected",
            Self::AuthenticationFailed => "model provider authentication failed",
            Self::AuthorizationFailed => "model provider authorization failed",
            Self::PolicyDenied => "model provider policy denied the request",
            Self::SafetyRefusal => "model provider refused the request for safety",
            Self::CapabilityUnsupported => "model provider capability is unavailable",
            Self::DataResidencyDenied => "model provider data-residency policy denied the request",
            Self::Timeout => "model provider request timed out",
            Self::Cancelled => "model provider request was cancelled",
            Self::RateLimited => "model provider rate limit was exceeded",
            Self::ServiceUnavailable => "model provider service is unavailable",
            Self::TransportFailed => "model provider transport failed",
            Self::ProtocolError => "model provider returned an invalid protocol response",
            Self::OutputTooLarge => "model provider response exceeded the configured bound",
            Self::StreamBackpressure => "model provider stream is backpressured",
            Self::SchemaValidationFailed => "model provider response failed schema validation",
            Self::SecretUnavailable => "model provider credential is unavailable",
            Self::SecretAccessDenied => "model provider credential access was denied",
            Self::UnsupportedWireVersion => "model provider wire version is unsupported",
            Self::MutualTlsFailed => "model provider mutual TLS failed",
            Self::ProfileInvalid => "model provider profile is invalid",
        }
    }

    const fn retry_decision(self) -> ProviderRetryDecision {
        if matches!(
            self,
            Self::Timeout
                | Self::RateLimited
                | Self::ServiceUnavailable
                | Self::TransportFailed
                | Self::StreamBackpressure
        ) {
            ProviderRetryDecision::Retry
        } else {
            ProviderRetryDecision::DoNotRetry
        }
    }

    const fn fallback_decision(self) -> ProviderFallbackDecision {
        if matches!(self, Self::Timeout | Self::RateLimited | Self::ServiceUnavailable) {
            ProviderFallbackDecision::TryNextProvider
        } else {
            ProviderFallbackDecision::DoNotFallback
        }
    }
}

/// Whether the same provider operation may be retried by policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderRetryDecision {
    Retry,
    DoNotRetry,
}

/// Whether routing may continue with another eligible provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderFallbackDecision {
    TryNextProvider,
    DoNotFallback,
}

/// Closed provider refusal reported by a request-status boundary.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ProviderRejection {
    InvalidRequest,
    AuthenticationFailed,
    AuthorizationFailed,
    PolicyDenied,
    SafetyRefusal,
    CapabilityUnsupported,
    DataResidencyDenied,
    Cancelled,
    SchemaValidationFailed,
    SecretAccessDenied,
    UnsupportedWireVersion,
    MutualTlsFailed,
    ProfileInvalid,
}

impl ProviderRejection {
    #[must_use]
    pub const fn failure(self) -> ProviderFailure {
        match self {
            Self::InvalidRequest => ProviderFailure::InvalidRequest,
            Self::AuthenticationFailed => ProviderFailure::AuthenticationFailed,
            Self::AuthorizationFailed => ProviderFailure::AuthorizationFailed,
            Self::PolicyDenied => ProviderFailure::PolicyDenied,
            Self::SafetyRefusal => ProviderFailure::SafetyRefusal,
            Self::CapabilityUnsupported => ProviderFailure::CapabilityUnsupported,
            Self::DataResidencyDenied => ProviderFailure::DataResidencyDenied,
            Self::Cancelled => ProviderFailure::Cancelled,
            Self::SchemaValidationFailed => ProviderFailure::SchemaValidationFailed,
            Self::SecretAccessDenied => ProviderFailure::SecretAccessDenied,
            Self::UnsupportedWireVersion => ProviderFailure::UnsupportedWireVersion,
            Self::MutualTlsFailed => ProviderFailure::MutualTlsFailed,
            Self::ProfileInvalid => ProviderFailure::ProfileInvalid,
        }
    }
}

impl TryFrom<ProviderFailure> for ProviderOperationalFailure {
    type Error = ProviderRejection;

    fn try_from(failure: ProviderFailure) -> Result<Self, Self::Error> {
        match failure {
            ProviderFailure::InvalidRequest => Err(ProviderRejection::InvalidRequest),
            ProviderFailure::AuthenticationFailed => Err(ProviderRejection::AuthenticationFailed),
            ProviderFailure::AuthorizationFailed => Err(ProviderRejection::AuthorizationFailed),
            ProviderFailure::PolicyDenied => Err(ProviderRejection::PolicyDenied),
            ProviderFailure::SafetyRefusal => Err(ProviderRejection::SafetyRefusal),
            ProviderFailure::CapabilityUnsupported => Err(ProviderRejection::CapabilityUnsupported),
            ProviderFailure::DataResidencyDenied => Err(ProviderRejection::DataResidencyDenied),
            ProviderFailure::Cancelled => Err(ProviderRejection::Cancelled),
            ProviderFailure::Timeout => Ok(Self::Timeout),
            ProviderFailure::RateLimited => Ok(Self::RateLimited),
            ProviderFailure::ServiceUnavailable => Ok(Self::ServiceUnavailable),
            ProviderFailure::TransportFailed => Ok(Self::TransportFailed),
            ProviderFailure::ProtocolError => Ok(Self::ProtocolError),
            ProviderFailure::OutputTooLarge => Ok(Self::OutputTooLarge),
            ProviderFailure::StreamBackpressure => Ok(Self::StreamBackpressure),
            ProviderFailure::SchemaValidationFailed => Err(ProviderRejection::SchemaValidationFailed),
            ProviderFailure::SecretUnavailable => Ok(Self::SecretUnavailable),
            ProviderFailure::SecretAccessDenied => Err(ProviderRejection::SecretAccessDenied),
            ProviderFailure::UnsupportedWireVersion => Err(ProviderRejection::UnsupportedWireVersion),
            ProviderFailure::MutualTlsFailed => Err(ProviderRejection::MutualTlsFailed),
            ProviderFailure::ProfileInvalid => Err(ProviderRejection::ProfileInvalid),
        }
    }
}

impl Debug for ProviderRejection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Provider request was rejected")
    }
}

impl Display for ProviderRejection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Provider request was rejected")
    }
}

/// Result of mapping a provider HTTP-like status at the public boundary.
#[derive(Clone)]
pub enum ProviderStatusOutcome {
    Rejected {
        rejection: ProviderRejection,
        provider_status: Option<u16>,
    },
    Operational(ProviderError),
}

impl ProviderStatusOutcome {
    /// Creates a source-free closed rejection without an HTTP-like provider
    /// status, such as a gRPC request refusal.
    #[must_use]
    pub const fn rejected(rejection: ProviderRejection) -> Self {
        Self::Rejected {
            rejection,
            provider_status: None,
        }
    }

    /// Returns the stable rejection classification when this is a refusal.
    #[must_use]
    pub const fn rejection(&self) -> Option<ProviderRejection> {
        match self {
            Self::Rejected { rejection, .. } => Some(*rejection),
            Self::Operational(_) => None,
        }
    }

    /// Returns the stable public classification for either a rejection or an
    /// operational failure.
    #[must_use]
    pub fn failure(&self) -> ProviderFailure {
        match self {
            Self::Rejected { rejection, .. } => rejection.failure(),
            Self::Operational(error) => error.failure(),
        }
    }

    /// Returns the fixed, redacted public message.
    #[must_use]
    pub fn message(&self) -> &'static str {
        match self {
            Self::Rejected { rejection, .. } => rejection.failure().public_message(),
            Self::Operational(error) => error.message(),
        }
    }

    /// Returns whether the operational failure may be retried. Closed
    /// rejections are never retryable.
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self {
            Self::Rejected { .. } => false,
            Self::Operational(error) => error.retryable(),
        }
    }

    /// Returns whether routing may try another provider. Closed rejections do
    /// not authorize fallback.
    #[must_use]
    pub fn fallback_decision(&self) -> ProviderFallbackDecision {
        match self {
            Self::Rejected { .. } => ProviderFallbackDecision::DoNotFallback,
            Self::Operational(error) => error.fallback_decision(),
        }
    }

    /// Returns the optional HTTP-like provider status for a refusal.
    #[must_use]
    pub fn provider_status(&self) -> Option<u16> {
        match self {
            Self::Rejected { provider_status, .. } => *provider_status,
            Self::Operational(error) => error.provider_status(),
        }
    }

    /// Borrows the operational error when the boundary itself failed.
    #[must_use]
    pub fn operational_error(&self) -> Option<&ProviderError> {
        match self {
            Self::Rejected { .. } => None,
            Self::Operational(error) => Some(error),
        }
    }
}

impl From<ProviderError> for ProviderStatusOutcome {
    fn from(error: ProviderError) -> Self {
        Self::Operational(error)
    }
}

impl Debug for ProviderStatusOutcome {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected { .. } => formatter.write_str("ProviderStatusOutcome::Rejected"),
            Self::Operational(_) => formatter.write_str("ProviderStatusOutcome::Operational"),
        }
    }
}

impl Display for ProviderStatusOutcome {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

/// Boundary-safe serialization view for a provider failure.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProviderErrorView {
    pub code: ProviderFailure,
    pub message: String,
    pub retryable: bool,
    pub provider_status: Option<u16>,
}

#[derive(Clone)]
struct ProviderErrorInner {
    failure: ProviderOperationalFailure,
    provider_status: Option<u16>,
    retryable: bool,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

/// Opaque, redacted model-provider operational error.
#[derive(Clone)]
pub struct ProviderError {
    inner: Arc<ProviderErrorInner>,
}

impl ProviderError {
    /// Creates a source-free operational provider error.
    #[must_use]
    pub fn from_operational_failure(failure: ProviderOperationalFailure) -> Self {
        Self {
            inner: Arc::new(ProviderErrorInner {
                failure,
                provider_status: None,
                retryable: failure.failure().retry_decision() == ProviderRetryDecision::Retry,
                source: None,
            }),
        }
    }

    pub(crate) fn new(failure: ProviderOperationalFailure, _diagnostic: impl Into<String>) -> Self {
        Self::from_operational_failure(failure)
    }

    /// Creates an opaque provider error while retaining a typed diagnostic source.
    ///
    /// The source is available through [`std::error::Error::source`] but is
    /// excluded from [`Self::view`] and the redacted `Display`/`Debug` output.
    pub fn from_operational_source<E>(failure: ProviderOperationalFailure, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(ProviderErrorInner {
                failure,
                provider_status: None,
                retryable: failure.failure().retry_decision() == ProviderRetryDecision::Retry,
                source: Some(Arc::new(source)),
            }),
        }
    }

    pub(crate) fn from_source<E>(failure: ProviderOperationalFailure, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::from_operational_source(failure, source)
    }

    pub(crate) fn from_remote(failure: ProviderOperationalFailure, retryable: bool) -> Self {
        Self {
            inner: Arc::new(ProviderErrorInner {
                failure,
                provider_status: None,
                retryable: failure.failure().retry_decision() == ProviderRetryDecision::Retry && retryable,
                source: None,
            }),
        }
    }

    /// Attaches a non-sensitive HTTP-like provider status.
    #[must_use]
    pub fn with_provider_status(self, status: u16) -> Self {
        Self {
            inner: Arc::new(ProviderErrorInner {
                provider_status: Some(status),
                ..(*self.inner).clone()
            }),
        }
    }

    /// Returns the stable non-error failure classification.
    #[must_use]
    pub fn failure(&self) -> ProviderFailure {
        self.inner.failure.failure()
    }

    /// Returns the fixed public message.
    #[must_use]
    pub fn message(&self) -> &'static str {
        self.inner.failure.failure().public_message()
    }

    /// Returns the optional provider status.
    #[must_use]
    pub fn provider_status(&self) -> Option<u16> {
        self.inner.provider_status
    }

    /// Returns whether the provider reported a retryable failure.
    #[must_use]
    pub fn retryable(&self) -> bool {
        self.inner.retryable
    }

    /// Returns the boundary-safe error projection.
    #[must_use]
    pub fn view(&self) -> ProviderErrorView {
        ProviderErrorView {
            code: self.inner.failure.failure(),
            message: self.inner.failure.failure().public_message().to_owned(),
            retryable: self.inner.retryable,
            provider_status: self.inner.provider_status,
        }
    }

    /// Returns the retry decision without inspecting rendered error text.
    #[must_use]
    pub fn retry_decision(&self) -> ProviderRetryDecision {
        if self.inner.retryable {
            ProviderRetryDecision::Retry
        } else {
            ProviderRetryDecision::DoNotRetry
        }
    }

    /// Returns the provider-fallback decision.
    #[must_use]
    pub fn fallback_decision(&self) -> ProviderFallbackDecision {
        self.inner.failure.failure().fallback_decision()
    }

    /// Creates a timeout failure.
    #[must_use]
    pub(crate) fn timeout(_diagnostic: impl Into<String>) -> Self {
        Self::from_operational_failure(ProviderOperationalFailure::Timeout)
    }

    /// Creates a service-unavailable failure.
    #[must_use]
    pub(crate) fn service_unavailable(_diagnostic: impl Into<String>) -> Self {
        Self::from_operational_failure(ProviderOperationalFailure::ServiceUnavailable)
    }
}

impl Debug for ProviderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderError")
            .field("view", &self.view())
            .finish()
    }
}

impl Display for ProviderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.inner.failure.failure().public_message())
    }
}

impl Error for ProviderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner
            .source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Maps a provider HTTP-like status to a closed refusal or operational failure.
///
/// Provider-authored 4xx request refusals stay out of [`ProviderError`]; only
/// transport, protocol, and service failures are operational errors.
#[must_use]
pub fn map_provider_status(status: u16) -> ProviderStatusOutcome {
    match status {
        400 | 404 | 409 | 422 => ProviderStatusOutcome::Rejected {
            rejection: ProviderRejection::InvalidRequest,
            provider_status: Some(status),
        },
        401 => ProviderStatusOutcome::Rejected {
            rejection: ProviderRejection::AuthenticationFailed,
            provider_status: Some(status),
        },
        403 => ProviderStatusOutcome::Rejected {
            rejection: ProviderRejection::PolicyDenied,
            provider_status: Some(status),
        },
        408 | 504 => ProviderStatusOutcome::Operational(
            ProviderError::from_operational_failure(ProviderOperationalFailure::Timeout).with_provider_status(status),
        ),
        429 => ProviderStatusOutcome::Operational(
            ProviderError::from_operational_failure(ProviderOperationalFailure::RateLimited)
                .with_provider_status(status),
        ),
        500..=599 => ProviderStatusOutcome::Operational(
            ProviderError::from_operational_failure(ProviderOperationalFailure::ServiceUnavailable)
                .with_provider_status(status),
        ),
        _ => ProviderStatusOutcome::Operational(
            ProviderError::from_operational_failure(ProviderOperationalFailure::ProtocolError)
                .with_provider_status(status),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn facade_is_small_send_sync_and_preserves_typed_source() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<ProviderError>();
        assert_eq!(std::mem::size_of::<ProviderError>(), std::mem::size_of::<usize>());
        let error = ProviderError::from_operational_source(
            ProviderOperationalFailure::ProtocolError,
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
    fn public_rendering_is_fixed_and_source_free() {
        let source = std::io::Error::other("secret endpoint and prompt");
        let error = ProviderError::from_operational_source(ProviderOperationalFailure::TransportFailed, source);
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("endpoint"));
            assert!(!rendered.contains("prompt"));
        }
    }

    #[test]
    fn provider_view_preserves_closed_wire_fields() {
        let ProviderStatusOutcome::Operational(error) = map_provider_status(429) else {
            panic!("rate limiting is operational");
        };
        let view = error.view();
        assert_eq!(view.code, ProviderFailure::RateLimited);
        assert_eq!(view.message, "model provider rate limit was exceeded");
        assert!(view.retryable);
        assert_eq!(view.provider_status, Some(429));
    }

    #[test]
    fn request_status_refusals_do_not_create_provider_errors() {
        assert!(matches!(
            map_provider_status(403),
            ProviderStatusOutcome::Rejected {
                rejection: ProviderRejection::PolicyDenied,
                provider_status: Some(403),
            }
        ));
    }

    #[test]
    fn closed_rejections_never_contain_provider_errors() {
        let outcome = ProviderStatusOutcome::rejected(ProviderRejection::AuthorizationFailed);
        assert_eq!(outcome.rejection(), Some(ProviderRejection::AuthorizationFailed));
        assert_eq!(outcome.provider_status(), None);
        assert!(outcome.operational_error().is_none());
    }

    #[test]
    fn operational_failure_type_excludes_expected_rejections() {
        for (failure, rejection) in [
            (ProviderFailure::InvalidRequest, ProviderRejection::InvalidRequest),
            (
                ProviderFailure::AuthenticationFailed,
                ProviderRejection::AuthenticationFailed,
            ),
            (
                ProviderFailure::AuthorizationFailed,
                ProviderRejection::AuthorizationFailed,
            ),
            (ProviderFailure::PolicyDenied, ProviderRejection::PolicyDenied),
            (ProviderFailure::SafetyRefusal, ProviderRejection::SafetyRefusal),
            (
                ProviderFailure::CapabilityUnsupported,
                ProviderRejection::CapabilityUnsupported,
            ),
            (
                ProviderFailure::DataResidencyDenied,
                ProviderRejection::DataResidencyDenied,
            ),
            (ProviderFailure::Cancelled, ProviderRejection::Cancelled),
            (
                ProviderFailure::SchemaValidationFailed,
                ProviderRejection::SchemaValidationFailed,
            ),
            (
                ProviderFailure::SecretAccessDenied,
                ProviderRejection::SecretAccessDenied,
            ),
            (
                ProviderFailure::UnsupportedWireVersion,
                ProviderRejection::UnsupportedWireVersion,
            ),
            (ProviderFailure::MutualTlsFailed, ProviderRejection::MutualTlsFailed),
            (ProviderFailure::ProfileInvalid, ProviderRejection::ProfileInvalid),
        ] {
            assert_eq!(ProviderOperationalFailure::try_from(failure), Err(rejection));
        }
        assert_eq!(
            ProviderOperationalFailure::try_from(ProviderFailure::TransportFailed),
            Ok(ProviderOperationalFailure::TransportFailed)
        );
    }
}
