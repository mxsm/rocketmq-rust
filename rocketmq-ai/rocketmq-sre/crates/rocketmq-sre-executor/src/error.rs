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

use rocketmq_sre_contracts::SreContractError;
use thiserror::Error;

/// Closed fail-closed results that are not operational failures.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ExecutorRejection {
    Unauthorized,
    InvalidRequest,
    AuthorityRejected,
    AgentRejected,
    VerificationRejected,
    PreconditionChanged,
    ReconcileBlocked,
}

impl ExecutorRejection {
    #[must_use]
    pub(crate) const fn http_classification(self) -> (axum::http::StatusCode, &'static str, bool) {
        match self {
            Self::Unauthorized => (
                axum::http::StatusCode::UNAUTHORIZED,
                "unauthorized_workload_identity",
                false,
            ),
            Self::InvalidRequest => (axum::http::StatusCode::BAD_REQUEST, "invalid_execution_request", false),
            Self::AuthorityRejected => (axum::http::StatusCode::FORBIDDEN, "execution_authority_rejected", false),
            Self::AgentRejected => (axum::http::StatusCode::CONFLICT, "execution_agent_rejected", false),
            Self::VerificationRejected => (
                axum::http::StatusCode::CONFLICT,
                "execution_verification_rejected",
                false,
            ),
            Self::PreconditionChanged => (axum::http::StatusCode::CONFLICT, "precondition_changed", false),
            Self::ReconcileBlocked => (axum::http::StatusCode::CONFLICT, "unresolved_old_effects", false),
        }
    }
}

impl std::fmt::Display for ExecutorRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Change Executor operation was rejected")
    }
}

impl std::fmt::Debug for ExecutorRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

/// Result of an Executor operation after expected, fail-closed refusal has
/// been separated from an operational [`ExecutorError`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorOperationOutcome<T> {
    /// The requested operation completed successfully.
    Accepted(T),
    /// The requested operation was refused without an operational failure.
    Rejected(ExecutorRejection),
}

/// Dependency boundary separating closed request refusals from operational
/// failures. It intentionally does not implement [`std::error::Error`].
#[derive(Debug)]
pub enum ExecutorRequestFailure {
    Rejected(ExecutorRejection),
    Operational(ExecutorError),
}

impl ExecutorRequestFailure {
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const InvalidRequest: Self = Self::Rejected(ExecutorRejection::InvalidRequest);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const AuthorityRejected: Self = Self::Rejected(ExecutorRejection::AuthorityRejected);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const AgentRejected: Self = Self::Rejected(ExecutorRejection::AgentRejected);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const VerificationRejected: Self = Self::Rejected(ExecutorRejection::VerificationRejected);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const PreconditionChanged: Self = Self::Rejected(ExecutorRejection::PreconditionChanged);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal closed-outcome call sites"
    )]
    pub(crate) const ReconcileBlocked: Self = Self::Rejected(ExecutorRejection::ReconcileBlocked);

    #[must_use]
    pub(crate) const fn is_precondition_changed(&self) -> bool {
        matches!(self, Self::Rejected(ExecutorRejection::PreconditionChanged))
    }

    #[must_use]
    pub(crate) const fn is_reconcile_blocked(&self) -> bool {
        matches!(self, Self::Rejected(ExecutorRejection::ReconcileBlocked))
    }
}

impl std::fmt::Display for ExecutorRequestFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(rejection) => std::fmt::Display::fmt(rejection, formatter),
            Self::Operational(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl<T> ExecutorOperationOutcome<T> {
    #[must_use]
    pub const fn accepted(value: T) -> Self {
        Self::Accepted(value)
    }

    #[must_use]
    pub const fn rejected(rejection: ExecutorRejection) -> Self {
        Self::Rejected(rejection)
    }

    /// Converts the closed operation outcome into a standard result without
    /// promoting the rejection to an operational error.
    pub fn into_result(self) -> Result<T, ExecutorRejection> {
        match self {
            Self::Accepted(value) => Ok(value),
            Self::Rejected(rejection) => Err(rejection),
        }
    }
}

/// Stable, closed classification for durable journal operations.
///
/// These outcomes are not [`std::error::Error`] values. Operational storage
/// failures retain an opaque [`ExecutorError`] without exposing their source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalFailureCode {
    InvalidInput,
    IdempotencyConflict,
    ResourceLocked,
    ResourceQuarantined,
    LeaseRejected,
    NotFound,
    Operational,
}

/// Boundary-safe failure from a durable journal operation.
pub struct JournalFailure {
    code: JournalFailureCode,
    operational: Option<Box<ExecutorError>>,
}

impl JournalFailure {
    #[must_use]
    pub const fn code(&self) -> JournalFailureCode {
        self.code
    }

    #[must_use]
    pub fn operational_error(&self) -> Option<&ExecutorError> {
        self.operational.as_deref()
    }

    const fn rejected(code: JournalFailureCode) -> Self {
        Self {
            code,
            operational: None,
        }
    }

    pub(crate) fn invalid_input(_detail: impl Into<String>) -> Self {
        Self::rejected(JournalFailureCode::InvalidInput)
    }

    pub(crate) const fn idempotency_conflict() -> Self {
        Self::rejected(JournalFailureCode::IdempotencyConflict)
    }

    pub(crate) const fn resource_locked() -> Self {
        Self::rejected(JournalFailureCode::ResourceLocked)
    }

    pub(crate) const fn resource_quarantined() -> Self {
        Self::rejected(JournalFailureCode::ResourceQuarantined)
    }

    pub(crate) const fn lease_rejected() -> Self {
        Self::rejected(JournalFailureCode::LeaseRejected)
    }

    pub(crate) const fn not_found() -> Self {
        Self::rejected(JournalFailureCode::NotFound)
    }

    fn operational(error: ExecutorError) -> Self {
        Self {
            code: JournalFailureCode::Operational,
            operational: Some(Box::new(error)),
        }
    }
}

impl std::fmt::Display for JournalFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("durable journal operation was rejected or unavailable")
    }
}

impl std::fmt::Debug for JournalFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JournalFailure")
            .field("code", &self.code)
            .finish()
    }
}

/// Opaque operational failure at the Change Executor boundary.
pub struct ExecutorError {
    kind: ExecutorErrorKind,
}

#[derive(Debug)]
enum ExecutorErrorKind {
    Configuration,
    ConfigurationSource(Box<dyn std::error::Error + Send + Sync>),
    AuthorityUnavailable,
    AgentUnavailable,
    VerificationUnavailable,
    Catalog(SreContractError),
    Journal(JournalError),
    Http(reqwest::Error),
    AgentDecode(serde_json::Error),
    AuthorityDecode(serde_json::Error),
    VerificationDecode(serde_json::Error),
    Io(std::io::Error),
}

impl ExecutorError {
    /// Creates a fixed, redacted operational-unavailability error.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self::new(ExecutorErrorKind::AuthorityUnavailable)
    }

    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const Configuration: Self = Self::new(ExecutorErrorKind::Configuration);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const AuthorityUnavailable: Self = Self::new(ExecutorErrorKind::AuthorityUnavailable);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const AgentUnavailable: Self = Self::new(ExecutorErrorKind::AgentUnavailable);
    #[allow(
        non_upper_case_globals,
        reason = "private constructors preserve concise internal fail-closed call sites"
    )]
    pub(crate) const VerificationUnavailable: Self = Self::new(ExecutorErrorKind::VerificationUnavailable);
    const fn new(kind: ExecutorErrorKind) -> Self {
        Self { kind }
    }

    pub(crate) fn configuration_source<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::new(ExecutorErrorKind::ConfigurationSource(Box::new(error)))
    }

    #[allow(
        non_snake_case,
        reason = "private constructors preserve concise internal source conversions"
    )]
    pub(crate) fn Catalog(error: SreContractError) -> Self {
        Self::new(ExecutorErrorKind::Catalog(error))
    }

    #[allow(
        non_snake_case,
        reason = "private constructors preserve concise internal source conversions"
    )]
    pub(crate) fn Journal(error: JournalError) -> Self {
        Self::new(ExecutorErrorKind::Journal(error))
    }

    #[allow(
        non_snake_case,
        reason = "private constructors preserve concise internal source conversions"
    )]
    pub(crate) fn Http(error: reqwest::Error) -> Self {
        Self::new(ExecutorErrorKind::Http(error))
    }

    pub(crate) fn agent_decode(error: serde_json::Error) -> Self {
        Self::new(ExecutorErrorKind::AgentDecode(error))
    }

    pub(crate) fn authority_decode(error: serde_json::Error) -> Self {
        Self::new(ExecutorErrorKind::AuthorityDecode(error))
    }

    pub(crate) fn verification_decode(error: serde_json::Error) -> Self {
        Self::new(ExecutorErrorKind::VerificationDecode(error))
    }

    #[allow(
        non_snake_case,
        reason = "private constructors preserve concise internal source conversions"
    )]
    pub(crate) fn Io(error: std::io::Error) -> Self {
        Self::new(ExecutorErrorKind::Io(error))
    }

    #[must_use]
    pub(crate) fn http_classification(&self) -> (axum::http::StatusCode, &'static str, bool) {
        match &self.kind {
            ExecutorErrorKind::Configuration | ExecutorErrorKind::ConfigurationSource(_) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "source_unavailable",
                false,
            ),
            ExecutorErrorKind::AuthorityUnavailable
            | ExecutorErrorKind::AgentUnavailable
            | ExecutorErrorKind::VerificationUnavailable
            | ExecutorErrorKind::AgentDecode(_)
            | ExecutorErrorKind::AuthorityDecode(_)
            | ExecutorErrorKind::VerificationDecode(_)
            | ExecutorErrorKind::Catalog(_)
            | ExecutorErrorKind::Journal(_)
            | ExecutorErrorKind::Http(_)
            | ExecutorErrorKind::Io(_) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, "source_unavailable", true),
        }
    }
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Change Executor operation failed")
    }
}

impl std::fmt::Debug for ExecutorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for ExecutorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ExecutorErrorKind::ConfigurationSource(error) => Some(error.as_ref()),
            ExecutorErrorKind::Catalog(error) => Some(error),
            ExecutorErrorKind::Journal(error) => Some(error),
            ExecutorErrorKind::Http(error) => Some(error),
            ExecutorErrorKind::AgentDecode(error)
            | ExecutorErrorKind::AuthorityDecode(error)
            | ExecutorErrorKind::VerificationDecode(error) => Some(error),
            ExecutorErrorKind::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ExecutorError> for ExecutorRequestFailure {
    fn from(error: ExecutorError) -> Self {
        Self::Operational(error)
    }
}

impl From<reqwest::Error> for ExecutorRequestFailure {
    fn from(error: reqwest::Error) -> Self {
        Self::Operational(ExecutorError::Http(error))
    }
}

impl From<SreContractError> for ExecutorRequestFailure {
    fn from(error: SreContractError) -> Self {
        if std::error::Error::source(&error).is_some() {
            Self::Operational(ExecutorError::Catalog(error))
        } else {
            Self::Rejected(ExecutorRejection::InvalidRequest)
        }
    }
}

impl From<JournalError> for ExecutorError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<JournalError> for JournalFailure {
    fn from(error: JournalError) -> Self {
        match error {
            JournalError::Database(error) => Self::operational(ExecutorError::Journal(JournalError::Database(error))),
            JournalError::SnapshotEncoding(error) => {
                Self::operational(ExecutorError::Journal(JournalError::SnapshotEncoding(error)))
            }
            JournalError::SnapshotDecoding(error) => {
                Self::operational(ExecutorError::Journal(JournalError::SnapshotDecoding(error)))
            }
        }
    }
}

impl From<SreContractError> for JournalFailure {
    fn from(error: SreContractError) -> Self {
        if std::error::Error::source(&error).is_some() {
            Self::operational(ExecutorError::Catalog(error))
        } else {
            Self::invalid_input("SRE contract validation was rejected")
        }
    }
}

impl From<JournalFailure> for ExecutorRequestFailure {
    fn from(failure: JournalFailure) -> Self {
        if let Some(error) = failure.operational {
            return Self::Operational(*error);
        }
        match failure.code {
            JournalFailureCode::InvalidInput | JournalFailureCode::NotFound => {
                Self::Rejected(ExecutorRejection::InvalidRequest)
            }
            JournalFailureCode::IdempotencyConflict
            | JournalFailureCode::ResourceLocked
            | JournalFailureCode::ResourceQuarantined
            | JournalFailureCode::LeaseRejected => Self::Rejected(ExecutorRejection::PreconditionChanged),
            JournalFailureCode::Operational => Self::Operational(ExecutorError::Configuration),
        }
    }
}

impl From<sqlx::Error> for JournalFailure {
    fn from(error: sqlx::Error) -> Self {
        Self::from(JournalError::Database(error))
    }
}

impl From<serde_json::Error> for JournalFailure {
    fn from(error: serde_json::Error) -> Self {
        Self::from(JournalError::SnapshotEncoding(error))
    }
}

impl From<reqwest::Error> for ExecutorError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error)
    }
}

impl From<std::io::Error> for ExecutorError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Operational durable execution journal error.
#[derive(Debug, Error)]
pub(crate) enum JournalError {
    #[error("persistent execution state is unavailable")]
    Database(#[source] sqlx::Error),
    #[error("execution snapshot could not be encoded")]
    SnapshotEncoding(#[source] serde_json::Error),
    #[error("execution snapshot could not be decoded")]
    SnapshotDecoding(#[source] serde_json::Error),
}

impl From<sqlx::Error> for JournalError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl From<serde_json::Error> for JournalError {
    fn from(error: serde_json::Error) -> Self {
        Self::SnapshotEncoding(error)
    }
}

pub(crate) fn database_message(error: &sqlx::Error) -> Option<&str> {
    error.as_database_error().map(|database| database.message())
}

pub(crate) fn has_database_code(error: &sqlx::Error, expected: &str) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == expected)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn journal_conflict_maps_directly_to_a_closed_rejection() {
        let failure = ExecutorRequestFailure::from(JournalFailure::resource_locked());

        assert!(matches!(
            failure,
            ExecutorRequestFailure::Rejected(ExecutorRejection::PreconditionChanged)
        ));
    }

    #[test]
    fn journal_decode_failure_retains_its_typed_operational_source() {
        let decode = serde_json::from_slice::<serde_json::Value>(b"not-json").expect_err("invalid JSON");
        let failure = ExecutorRequestFailure::from(JournalFailure::from(JournalError::SnapshotDecoding(decode)));
        let ExecutorRequestFailure::Operational(error) = failure else {
            panic!("snapshot decoding must remain operational");
        };
        let journal = error
            .source()
            .and_then(|source| source.downcast_ref::<JournalError>())
            .expect("typed journal source");

        assert!(journal.source().is_some_and(|source| source.is::<serde_json::Error>()));
    }

    #[test]
    fn contract_failures_split_by_typed_source_at_the_journal_boundary() {
        let rejected = JournalFailure::from(SreContractError::new(
            rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
        ));
        assert_eq!(rejected.code(), JournalFailureCode::InvalidInput);
        assert!(rejected.operational_error().is_none());

        let sourced = JournalFailure::from(SreContractError::from_source(std::io::Error::other(
            "private codec detail",
        )));
        let operational = sourced.operational_error().expect("operational contract failure");
        let contract = operational
            .source()
            .and_then(|source| source.downcast_ref::<SreContractError>())
            .expect("typed contract source");
        assert!(contract.source().is_some_and(|source| source.is::<std::io::Error>()));
    }
}
