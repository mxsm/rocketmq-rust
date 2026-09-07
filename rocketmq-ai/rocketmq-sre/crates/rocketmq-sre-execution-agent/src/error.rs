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

use thiserror::Error;

/// Closed results for rejected Agent dispatches; these are not operational errors.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ExecutionAgentRejection {
    Unauthorized,
    InvalidRequest,
    ActionNotRegistered,
    AuthorityRejected,
    UnresolvedEffect,
    DriverRefused,
    DriverOutcomeUnknown,
    FenceRejected,
}

impl ExecutionAgentRejection {
    #[must_use]
    pub(crate) const fn http_classification(self) -> (axum::http::StatusCode, &'static str, bool) {
        match self {
            Self::Unauthorized => (
                axum::http::StatusCode::UNAUTHORIZED,
                "unauthorized_workload_identity",
                false,
            ),
            Self::InvalidRequest => (axum::http::StatusCode::BAD_REQUEST, "invalid_agent_request", false),
            Self::ActionNotRegistered => (axum::http::StatusCode::CONFLICT, "action_not_registered", false),
            Self::AuthorityRejected | Self::FenceRejected => {
                (axum::http::StatusCode::FORBIDDEN, "stale_lease_epoch", false)
            }
            Self::UnresolvedEffect => (axum::http::StatusCode::CONFLICT, "unresolved_old_effects", false),
            Self::DriverRefused => (axum::http::StatusCode::CONFLICT, "driver_failed", false),
            Self::DriverOutcomeUnknown => (axum::http::StatusCode::CONFLICT, "effect_unknown", false),
        }
    }
}

impl std::fmt::Display for ExecutionAgentRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Execution Agent operation was rejected")
    }
}

impl std::fmt::Debug for ExecutionAgentRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

/// Result of an Agent operation after expected, fail-closed refusal has been
/// separated from an operational [`ExecutionAgentError`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionAgentOperationOutcome<T> {
    /// The requested operation completed successfully.
    Accepted(T),
    /// The requested operation was refused without an operational failure.
    Rejected(ExecutionAgentRejection),
}

/// Dependency boundary separating closed request refusals from operational
/// failures. It intentionally does not implement [`std::error::Error`].
#[derive(Debug)]
pub enum ExecutionAgentRequestFailure {
    Rejected(ExecutionAgentRejection),
    Operational(ExecutionAgentError),
}

impl ExecutionAgentRequestFailure {
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const InvalidRequest: Self = Self::Rejected(ExecutionAgentRejection::InvalidRequest);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const ActionNotRegistered: Self = Self::Rejected(ExecutionAgentRejection::ActionNotRegistered);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const AuthorityRejected: Self = Self::Rejected(ExecutionAgentRejection::AuthorityRejected);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const UnresolvedEffect: Self = Self::Rejected(ExecutionAgentRejection::UnresolvedEffect);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const DriverUnknown: Self = Self::Rejected(ExecutionAgentRejection::DriverOutcomeUnknown);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal operational-failure call sites"
    )]
    pub(crate) const DriverFailed: Self = Self::Operational(ExecutionAgentError::DriverFailed);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal operational-failure call sites"
    )]
    pub(crate) const Configuration: Self = Self::Operational(ExecutionAgentError::Configuration);

    pub(crate) fn driver_source<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Operational(ExecutionAgentError::driver_source(error))
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn stable_code(&self) -> &'static str {
        match self {
            Self::Rejected(rejection) => rejection.http_classification().1,
            Self::Operational(error) => error.stable_code(),
        }
    }
}

impl std::fmt::Display for ExecutionAgentRequestFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(rejection) => std::fmt::Display::fmt(rejection, formatter),
            Self::Operational(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl<T> ExecutionAgentOperationOutcome<T> {
    #[must_use]
    pub const fn accepted(value: T) -> Self {
        Self::Accepted(value)
    }

    #[must_use]
    pub const fn rejected(rejection: ExecutionAgentRejection) -> Self {
        Self::Rejected(rejection)
    }

    /// Converts the closed operation outcome into a standard result without
    /// promoting the rejection to an operational error.
    pub fn into_result(self) -> Result<T, ExecutionAgentRejection> {
        match self {
            Self::Accepted(value) => Ok(value),
            Self::Rejected(rejection) => Err(rejection),
        }
    }
}

/// Stable, closed classification for durable Agent effect-store operations.
///
/// These outcomes are not [`std::error::Error`] values. Operational storage
/// failures retain an opaque [`ExecutionAgentError`] without exposing sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentStoreFailureCode {
    InvalidInput,
    FenceRejected,
    IdempotencyConflict,
    NotFound,
    InvalidTransition,
    Operational,
}

/// Boundary-safe failure from an Agent effect-store operation.
pub struct AgentStoreFailure {
    code: AgentStoreFailureCode,
    operational: Option<Box<ExecutionAgentError>>,
}

impl AgentStoreFailure {
    #[must_use]
    pub const fn code(&self) -> AgentStoreFailureCode {
        self.code
    }

    #[must_use]
    pub fn operational_error(&self) -> Option<&ExecutionAgentError> {
        self.operational.as_deref()
    }

    const fn rejected(code: AgentStoreFailureCode) -> Self {
        Self {
            code,
            operational: None,
        }
    }

    pub(crate) fn invalid_input(_detail: impl Into<String>) -> Self {
        Self::rejected(AgentStoreFailureCode::InvalidInput)
    }

    pub(crate) const fn fence_rejected() -> Self {
        Self::rejected(AgentStoreFailureCode::FenceRejected)
    }

    pub(crate) const fn idempotency_conflict() -> Self {
        Self::rejected(AgentStoreFailureCode::IdempotencyConflict)
    }

    pub(crate) const fn not_found() -> Self {
        Self::rejected(AgentStoreFailureCode::NotFound)
    }

    pub(crate) const fn invalid_transition() -> Self {
        Self::rejected(AgentStoreFailureCode::InvalidTransition)
    }

    fn operational(error: ExecutionAgentError) -> Self {
        Self {
            code: AgentStoreFailureCode::Operational,
            operational: Some(Box::new(error)),
        }
    }
}

impl std::fmt::Display for AgentStoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Agent effect-store operation was rejected or unavailable")
    }
}

impl std::fmt::Debug for AgentStoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentStoreFailure")
            .field("code", &self.code)
            .finish()
    }
}

/// Opaque operational failure at the Execution Agent boundary.
pub struct ExecutionAgentError {
    kind: ExecutionAgentErrorKind,
}

#[derive(Debug)]
enum ExecutionAgentErrorKind {
    Configuration,
    ConfigurationSource(Box<dyn std::error::Error + Send + Sync>),
    AuthorityUnavailable,
    AuthorityDecode(serde_json::Error),
    DriverFailed,
    DispatchBarrierUnavailable,
    DispatchBarrier(sqlx::Error),
    DriverSource(Box<dyn std::error::Error + Send + Sync>),
    Store(AgentStoreError),
    Http(reqwest::Error),
    Io(std::io::Error),
}

impl ExecutionAgentError {
    /// Creates a fixed, redacted operational-unavailability error.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self::new(ExecutionAgentErrorKind::AuthorityUnavailable)
    }

    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const Configuration: Self = Self::new(ExecutionAgentErrorKind::Configuration);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const AuthorityUnavailable: Self = Self::new(ExecutionAgentErrorKind::AuthorityUnavailable);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const DriverFailed: Self = Self::new(ExecutionAgentErrorKind::DriverFailed);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const DispatchBarrierUnavailable: Self = Self::new(ExecutionAgentErrorKind::DispatchBarrierUnavailable);

    const fn new(kind: ExecutionAgentErrorKind) -> Self {
        Self { kind }
    }

    pub(crate) fn configuration_source<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::new(ExecutionAgentErrorKind::ConfigurationSource(Box::new(error)))
    }

    #[allow(
        non_snake_case,
        reason = "private constructors preserve concise internal source conversions"
    )]
    pub(crate) fn Store(error: AgentStoreError) -> Self {
        Self::new(ExecutionAgentErrorKind::Store(error))
    }

    #[allow(
        non_snake_case,
        reason = "private constructors preserve concise internal source conversions"
    )]
    pub(crate) fn Http(error: reqwest::Error) -> Self {
        Self::new(ExecutionAgentErrorKind::Http(error))
    }

    #[allow(
        non_snake_case,
        reason = "private constructors preserve concise internal source conversions"
    )]
    pub(crate) fn Io(error: std::io::Error) -> Self {
        Self::new(ExecutionAgentErrorKind::Io(error))
    }

    pub(crate) fn dispatch_barrier(error: sqlx::Error) -> Self {
        Self::new(ExecutionAgentErrorKind::DispatchBarrier(error))
    }

    pub(crate) fn authority_decode(error: serde_json::Error) -> Self {
        Self::new(ExecutionAgentErrorKind::AuthorityDecode(error))
    }

    pub(crate) fn driver_source<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::new(ExecutionAgentErrorKind::DriverSource(Box::new(error)))
    }

    #[must_use]
    pub(crate) fn stable_code(&self) -> &'static str {
        match &self.kind {
            ExecutionAgentErrorKind::DriverFailed => "source_unavailable",
            ExecutionAgentErrorKind::Configuration | ExecutionAgentErrorKind::ConfigurationSource(_) => {
                "source_unavailable"
            }
            ExecutionAgentErrorKind::AuthorityUnavailable => "authority_unavailable",
            ExecutionAgentErrorKind::AuthorityDecode(_) => "source_unavailable",
            ExecutionAgentErrorKind::DispatchBarrierUnavailable => "dispatch_barrier_unavailable",
            ExecutionAgentErrorKind::DispatchBarrier(_) => "dispatch_barrier_unavailable",
            ExecutionAgentErrorKind::DriverSource(_) => "source_unavailable",
            ExecutionAgentErrorKind::Store(_) => "effect_store_unavailable",
            ExecutionAgentErrorKind::Http(_) => "authority_transport_unavailable",
            ExecutionAgentErrorKind::Io(_) => "service_io_unavailable",
        }
    }

    #[must_use]
    pub(crate) fn http_classification(&self) -> (axum::http::StatusCode, &'static str, bool) {
        match &self.kind {
            ExecutionAgentErrorKind::Configuration | ExecutionAgentErrorKind::ConfigurationSource(_) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "source_unavailable",
                false,
            ),
            ExecutionAgentErrorKind::AuthorityUnavailable
            | ExecutionAgentErrorKind::AuthorityDecode(_)
            | ExecutionAgentErrorKind::DriverFailed
            | ExecutionAgentErrorKind::DriverSource(_)
            | ExecutionAgentErrorKind::DispatchBarrierUnavailable
            | ExecutionAgentErrorKind::DispatchBarrier(_)
            | ExecutionAgentErrorKind::Store(_)
            | ExecutionAgentErrorKind::Http(_)
            | ExecutionAgentErrorKind::Io(_) => {
                (axum::http::StatusCode::SERVICE_UNAVAILABLE, "source_unavailable", true)
            }
        }
    }
}

impl std::fmt::Display for ExecutionAgentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Execution Agent operation failed")
    }
}

impl std::fmt::Debug for ExecutionAgentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for ExecutionAgentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ExecutionAgentErrorKind::ConfigurationSource(error) => Some(error.as_ref()),
            ExecutionAgentErrorKind::AuthorityDecode(error) => Some(error),
            ExecutionAgentErrorKind::Store(error) => Some(error),
            ExecutionAgentErrorKind::DispatchBarrier(error) => Some(error),
            ExecutionAgentErrorKind::DriverSource(error) => Some(error.as_ref()),
            ExecutionAgentErrorKind::Http(error) => Some(error),
            ExecutionAgentErrorKind::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentStoreError> for ExecutionAgentError {
    fn from(error: AgentStoreError) -> Self {
        Self::Store(error)
    }
}

impl From<AgentStoreError> for AgentStoreFailure {
    fn from(error: AgentStoreError) -> Self {
        match error {
            AgentStoreError::Database(error) => {
                Self::operational(ExecutionAgentError::Store(AgentStoreError::Database(error)))
            }
            AgentStoreError::SnapshotEncoding(error) => {
                Self::operational(ExecutionAgentError::Store(AgentStoreError::SnapshotEncoding(error)))
            }
            AgentStoreError::SnapshotDecoding(error) => {
                Self::operational(ExecutionAgentError::Store(AgentStoreError::SnapshotDecoding(error)))
            }
        }
    }
}

impl From<ExecutionAgentError> for ExecutionAgentRequestFailure {
    fn from(error: ExecutionAgentError) -> Self {
        Self::Operational(error)
    }
}

impl From<reqwest::Error> for ExecutionAgentRequestFailure {
    fn from(error: reqwest::Error) -> Self {
        Self::Operational(ExecutionAgentError::Http(error))
    }
}

impl From<AgentStoreFailure> for ExecutionAgentRequestFailure {
    fn from(failure: AgentStoreFailure) -> Self {
        if let Some(error) = failure.operational {
            return Self::Operational(*error);
        }
        match failure.code {
            AgentStoreFailureCode::InvalidInput | AgentStoreFailureCode::NotFound => {
                Self::Rejected(ExecutionAgentRejection::InvalidRequest)
            }
            AgentStoreFailureCode::FenceRejected => Self::Rejected(ExecutionAgentRejection::AuthorityRejected),
            AgentStoreFailureCode::IdempotencyConflict | AgentStoreFailureCode::InvalidTransition => {
                Self::Rejected(ExecutionAgentRejection::UnresolvedEffect)
            }
            AgentStoreFailureCode::Operational => Self::Operational(ExecutionAgentError::Configuration),
        }
    }
}

impl From<sqlx::Error> for AgentStoreFailure {
    fn from(error: sqlx::Error) -> Self {
        Self::from(AgentStoreError::Database(error))
    }
}

impl From<serde_json::Error> for AgentStoreFailure {
    fn from(error: serde_json::Error) -> Self {
        Self::from(AgentStoreError::SnapshotEncoding(error))
    }
}

impl From<reqwest::Error> for ExecutionAgentError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error)
    }
}

impl From<std::io::Error> for ExecutionAgentError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Operational Agent fence/effect persistence error.
#[derive(Debug, Error)]
pub(crate) enum AgentStoreError {
    #[error("execution agent persistence is unavailable")]
    Database(#[source] sqlx::Error),
    #[error("execution agent snapshot encoding failed")]
    SnapshotEncoding(#[source] serde_json::Error),
    #[error("execution agent snapshot decoding failed")]
    SnapshotDecoding(#[source] serde_json::Error),
}

impl From<sqlx::Error> for AgentStoreError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

pub(crate) fn database_message(error: &sqlx::Error) -> Option<&str> {
    error.as_database_error().map(|database| database.message())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn store_conflict_maps_directly_to_a_closed_rejection() {
        let failure = ExecutionAgentRequestFailure::from(AgentStoreFailure::idempotency_conflict());

        assert!(matches!(
            failure,
            ExecutionAgentRequestFailure::Rejected(ExecutionAgentRejection::UnresolvedEffect)
        ));
    }

    #[test]
    fn store_decode_failure_retains_its_typed_operational_source() {
        let decode = serde_json::from_slice::<serde_json::Value>(b"not-json").expect_err("invalid JSON");
        let failure =
            ExecutionAgentRequestFailure::from(AgentStoreFailure::from(AgentStoreError::SnapshotDecoding(decode)));
        let ExecutionAgentRequestFailure::Operational(error) = failure else {
            panic!("snapshot decoding must remain operational");
        };
        let store = error
            .source()
            .and_then(|source| source.downcast_ref::<AgentStoreError>())
            .expect("typed store source");

        assert!(store.source().is_some_and(|source| source.is::<serde_json::Error>()));
    }
}
