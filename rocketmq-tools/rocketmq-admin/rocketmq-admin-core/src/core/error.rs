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

//! Catalog-backed errors for admin contracts.

use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt;
use std::panic::Location;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::CliExitCode;
use rocketmq_error::DiagnosticView;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::RemotingResponseCode;
use rocketmq_error::Sensitive;
use rocketmq_error::SharedError;
use rocketmq_error::ViewContextViolation;

/// Closed classification used by admin-domain consumers.
///
/// Stable identity and boundary policy remain owned by the canonical
/// descriptor returned by [`AdminError::descriptor`]. This classification is
/// intentionally narrower than an error-code taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdminFailure {
    /// Caller-supplied admin input is invalid.
    InvalidArgument,
    /// A requested admin resource was not found.
    NotFound,
    /// An admin dependency or operation failed.
    Backend,
    /// The owning admin session is closed.
    SessionClosed,
}

/// Closed completion state for an admin operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdminOutcome {
    /// Every required target or source completed successfully.
    Success,
    /// Usable data or mutations completed, but at least one target failed.
    PartialSuccess,
    /// No successful result can be returned.
    Failure,
}

/// Opaque, cloneable admin failure backed by one canonical shared error.
///
/// The catalog descriptor is the sole owner of stable code, message, retry,
/// HTTP, CLI, and remoting policy. Admin-only labels are closed or static and
/// private detail is retained without being rendered. Typed dependency errors
/// remain available through the standard [`StdError::source`] chain.
#[derive(Clone)]
pub struct AdminError {
    error: SharedError,
    failure: AdminFailure,
    operation: Option<&'static str>,
    field: Option<&'static str>,
    resource: Option<&'static str>,
    private_detail: Option<Sensitive<String>>,
}

impl AdminError {
    /// Creates an invalid-argument failure.
    #[track_caller]
    pub fn invalid_argument(field: &'static str, reason: impl Into<String>) -> Self {
        Self::source_free(
            &rocketmq_error::CORE_ARGUMENT_INVALID,
            AdminFailure::InvalidArgument,
            None,
            Some(field),
            None,
            Some(reason.into()),
            ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT),
        )
    }

    /// Creates an invalid-argument failure while retaining its typed source.
    #[track_caller]
    pub fn invalid_argument_source(field: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::with_source(
            &rocketmq_error::CORE_ARGUMENT_INVALID,
            AdminFailure::InvalidArgument,
            None,
            Some(field),
            None,
            None,
            ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT),
            source,
        )
    }

    /// Creates a generic administrative operation failure.
    #[track_caller]
    pub fn backend(operation: &'static str, reason: impl Into<String>) -> Self {
        Self::source_free(
            &rocketmq_error::TOOLS_OPERATION_FAILED,
            AdminFailure::Backend,
            Some(operation),
            None,
            None,
            Some(reason.into()),
            ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation),
        )
    }

    /// Creates a generic administrative operation failure with a typed source.
    #[track_caller]
    pub fn backend_source(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::with_source(
            &rocketmq_error::TOOLS_OPERATION_FAILED,
            AdminFailure::Backend,
            Some(operation),
            None,
            None,
            None,
            ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation),
            source,
        )
    }

    /// Promotes a canonical failure without changing its descriptor or protocol projections.
    #[track_caller]
    pub fn from_error(operation: &'static str, source: CanonicalError) -> Self {
        let descriptor = source.descriptor();
        let context = source.context().clone();
        let failure = match descriptor.condition() {
            CanonicalCondition::InvalidArgument => AdminFailure::InvalidArgument,
            CanonicalCondition::NotFound => AdminFailure::NotFound,
            _ => AdminFailure::Backend,
        };
        Self::with_source(descriptor, failure, Some(operation), None, None, None, context, source)
    }

    /// Creates a generic not-found failure.
    #[track_caller]
    pub fn not_found(resource: &'static str, name: impl Into<String>) -> Self {
        Self::not_found_with_descriptor(
            &rocketmq_error::BROKER_QUERY_NOT_FOUND,
            resource,
            name,
            ErrorContext::new().with_text(fields::RESOURCE, resource),
        )
    }

    /// Creates a cluster-not-found failure.
    #[track_caller]
    pub fn cluster_not_found(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::not_found_with_descriptor(
            &rocketmq_error::ROUTE_CLUSTER_NOT_FOUND,
            "cluster",
            name,
            ErrorContext::new(),
        )
    }

    /// Creates a topic-not-found failure.
    #[track_caller]
    pub fn topic_not_found(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::not_found_with_descriptor(
            &rocketmq_error::BROKER_TOPIC_NOT_FOUND,
            "topic",
            name,
            ErrorContext::new(),
        )
    }

    /// Creates a topic-route-not-found failure.
    #[track_caller]
    pub fn topic_route_not_found(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::not_found_with_descriptor(
            &rocketmq_error::ROUTE_TOPIC_NOT_FOUND,
            "topic route",
            name,
            ErrorContext::new(),
        )
    }

    /// Creates a broker-not-found failure.
    #[track_caller]
    pub fn broker_not_found(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::not_found_with_descriptor(
            &rocketmq_error::BROKER_LOOKUP_NOT_FOUND,
            "broker",
            name,
            ErrorContext::new(),
        )
    }

    /// Creates a consumer-group-not-found failure.
    #[track_caller]
    pub fn consumer_group_not_found(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::not_found_with_descriptor(
            &rocketmq_error::BROKER_SUBSCRIPTION_GROUP_NOT_FOUND,
            "consumer group",
            name,
            ErrorContext::new(),
        )
    }

    /// Creates a descriptor-owned target-limit rejection.
    #[track_caller]
    pub fn target_limit(operation: &'static str, reason: impl Into<String>) -> Self {
        Self::source_free(
            &rocketmq_error::CORE_ARGUMENT_INVALID,
            AdminFailure::InvalidArgument,
            Some(operation),
            None,
            None,
            Some(reason.into()),
            ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT),
        )
    }

    /// Creates a descriptor-owned target-state conflict.
    #[track_caller]
    pub fn target_drift(operation: &'static str, reason: impl Into<String>) -> Self {
        Self::source_free(
            &rocketmq_error::CLIENT_LIFECYCLE_INVALID_STATE,
            AdminFailure::Backend,
            Some(operation),
            None,
            None,
            Some(reason.into()),
            ErrorContext::new()
                .with_text(fields::EXPECTED_STATE, "planned")
                .with_text(fields::ACTUAL_STATE, "changed"),
        )
    }

    /// Creates a descriptor-owned admin dependency-unavailable failure.
    #[track_caller]
    pub fn unavailable(operation: &'static str, reason: impl Into<String>) -> Self {
        Self::source_free(
            &rocketmq_error::CLIENT_COMPONENT_UNAVAILABLE,
            AdminFailure::Backend,
            Some(operation),
            None,
            None,
            Some(reason.into()),
            ErrorContext::new().with_text(fields::CLIENT_ROLE, "admin"),
        )
    }

    /// Creates the canonical closed-session failure.
    #[track_caller]
    pub fn session_closed() -> Self {
        Self::source_free(
            &rocketmq_error::CLIENT_LIFECYCLE_NOT_STARTED,
            AdminFailure::SessionClosed,
            None,
            None,
            None,
            None,
            ErrorContext::new(),
        )
    }

    fn not_found_with_descriptor(
        descriptor: &'static ErrorDescriptor,
        resource: &'static str,
        name: impl Into<String>,
        context: ErrorContext,
    ) -> Self {
        Self::source_free(
            descriptor,
            AdminFailure::NotFound,
            None,
            None,
            Some(resource),
            Some(name.into()),
            context,
        )
    }

    #[track_caller]
    fn source_free(
        descriptor: &'static ErrorDescriptor,
        failure: AdminFailure,
        operation: Option<&'static str>,
        field: Option<&'static str>,
        resource: Option<&'static str>,
        private_detail: Option<String>,
        context: ErrorContext,
    ) -> Self {
        Self {
            error: Arc::new(CanonicalError::new(descriptor).with_context(context)),
            failure,
            operation,
            field,
            resource,
            private_detail: private_detail.map(Sensitive::new),
        }
    }

    #[track_caller]
    #[allow(
        clippy::too_many_arguments,
        reason = "keeps canonical error construction in one reviewed boundary"
    )]
    fn with_source(
        descriptor: &'static ErrorDescriptor,
        failure: AdminFailure,
        operation: Option<&'static str>,
        field: Option<&'static str>,
        resource: Option<&'static str>,
        private_detail: Option<String>,
        context: ErrorContext,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            error: Arc::new(CanonicalError::caused_by(descriptor, source).with_context(context)),
            failure,
            operation,
            field,
            resource,
            private_detail: private_detail.map(Sensitive::new),
        }
    }

    /// Returns the closed admin-domain classification.
    pub const fn failure(&self) -> AdminFailure {
        self.failure
    }

    /// Returns the closed failed completion state.
    pub const fn outcome(&self) -> AdminOutcome {
        AdminOutcome::Failure
    }

    /// Returns the stable descriptor-owned code.
    pub fn code(&self) -> ErrorCode {
        self.error.code()
    }

    /// Returns the canonical descriptor.
    pub fn descriptor(&self) -> &'static ErrorDescriptor {
        self.error.descriptor()
    }

    /// Returns the protocol-independent canonical condition.
    pub fn condition(&self) -> CanonicalCondition {
        self.error.condition()
    }

    /// Returns the catalog-owned recovery policy.
    pub fn recovery_hint(&self) -> RecoveryHint {
        self.error.recovery_hint()
    }

    /// Returns the descriptor-owned HTTP status.
    pub fn http_status(&self) -> HttpStatusCode {
        self.error.projection().http().status
    }

    /// Returns the descriptor-owned CLI exit code.
    pub fn cli_exit_code(&self) -> CliExitCode {
        self.error.projection().cli().exit_code
    }

    /// Returns the descriptor-owned RocketMQ remoting response code.
    pub fn remoting_response_code(&self) -> RemotingResponseCode {
        self.error.projection().remoting().code
    }

    /// Returns whether the descriptor recommends automatic recovery.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_hint(),
            RecoveryHint::Backoff
                | RecoveryHint::RefreshRoute
                | RecoveryHint::RefreshLeader
                | RecoveryHint::SwitchBroker
        )
    }

    /// Returns whether this error represents a closed admin session.
    pub const fn is_session_closed(&self) -> bool {
        matches!(self.failure, AdminFailure::SessionClosed)
    }

    /// Returns the static operation label, when the failure has one.
    pub const fn operation(&self) -> Option<&'static str> {
        self.operation
    }

    /// Returns the static invalid field, when the failure has one.
    pub const fn field(&self) -> Option<&'static str> {
        self.field
    }

    /// Returns the static resource label, when the failure has one.
    pub const fn resource(&self) -> Option<&'static str> {
        self.resource
    }

    /// Returns the bounded context retained by the canonical error.
    pub fn context(&self) -> &ErrorContext {
        self.error.context()
    }

    /// Returns the first canonical-promotion location.
    pub fn location(&self) -> &'static Location<'static> {
        self.error.location()
    }

    /// Returns the catalog-controlled backtrace, when captured.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.error.backtrace()
    }

    /// Creates the descriptor-validated public projection.
    pub fn public_view(&self) -> Result<PublicErrorView<'_>, ViewContextViolation> {
        self.error.public_view()
    }

    /// Creates the descriptor-validated diagnostic projection.
    pub fn diagnostic_view(&self) -> Result<DiagnosticView<'_>, ViewContextViolation> {
        self.error.diagnostic_view()
    }

    /// Clones the canonical shared error for another boundary owner.
    pub fn shared_error(&self) -> SharedError {
        Arc::clone(&self.error)
    }

    /// Converts this facade into an owned canonical error.
    pub fn into_error(self) -> CanonicalError {
        let descriptor = self.descriptor();
        let context = self.context().clone();
        CanonicalError::caused_by(descriptor, self).with_context(context)
    }
}

impl PartialEq for AdminError {
    fn eq(&self, other: &Self) -> bool {
        self.descriptor() == other.descriptor()
            && self.context() == other.context()
            && self.failure == other.failure
            && self.operation == other.operation
            && self.field == other.field
            && self.resource == other.resource
            && self.private_detail == other.private_detail
    }
}

impl Eq for AdminError {}

impl fmt::Display for AdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.error.as_ref(), formatter)
    }
}

impl fmt::Debug for AdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminError")
            .field("code", &self.code())
            .field("failure", &self.failure)
            .field("operation", &self.operation)
            .field("field", &self.field)
            .field("resource", &self.resource)
            .field("detail_present", &self.private_detail.is_some())
            .field("source_present", &self.error.source().is_some())
            .finish()
    }
}

impl StdError for AdminError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.source()
    }
}

impl From<AdminError> for CanonicalError {
    fn from(error: AdminError) -> Self {
        error.into_error()
    }
}

pub type AdminResult<T> = Result<T, AdminError>;

pub(crate) fn required(field: &'static str, value: impl Into<String>) -> AdminResult<String> {
    let value = value.into().trim().to_string();
    if value.is_empty() {
        Err(AdminError::invalid_argument(field, "must not be empty"))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_error_is_catalog_backed_and_redaction_safe() {
        let error = AdminError::backend_source(
            "query_topic",
            std::io::Error::other("password=plain-text C:\\private\\admin"),
        );

        assert_eq!(error.descriptor(), &rocketmq_error::TOOLS_OPERATION_FAILED);
        assert_eq!(error.code().as_str(), "tools.operation.failed");
        assert_eq!(error.failure(), AdminFailure::Backend);
        assert_eq!(error.operation(), Some("query_topic"));
        assert!(error.source().is_some_and(|source| source.is::<std::io::Error>()));
        assert_eq!(
            error.to_string(),
            "tools.operation.failed: Administrative operation failed"
        );
        assert!(!error.to_string().contains("plain-text"));
        assert!(!format!("{error:?}").contains("private"));
    }

    #[test]
    fn canonical_promotion_preserves_descriptor_source_and_remoting_code() {
        let source = CanonicalError::new(&rocketmq_error::ROUTE_TOPIC_NOT_FOUND)
            .with_context(ErrorContext::new().with_text(fields::TOPIC, "orders"));
        let expected_remoting = source.descriptor().projection().remoting().code;
        let error = AdminError::from_error("query_topic", source);

        assert_eq!(error.descriptor(), &rocketmq_error::ROUTE_TOPIC_NOT_FOUND);
        assert_eq!(error.remoting_response_code(), expected_remoting);
        assert!(error.source().is_some_and(|source| source.is::<CanonicalError>()));
    }

    #[test]
    fn admin_policy_is_projected_only_from_descriptors() {
        let limit = AdminError::target_limit("query_targets", "too many targets");
        let drift = AdminError::target_drift("apply_plan", "target changed");
        let unavailable = AdminError::unavailable("query_sources", "all sources failed");

        assert_eq!(limit.http_status(), HttpStatusCode::BAD_REQUEST);
        assert_eq!(limit.cli_exit_code(), CliExitCode::USAGE);
        assert_eq!(drift.http_status(), HttpStatusCode::CONFLICT);
        assert_eq!(drift.cli_exit_code(), CliExitCode::DATA);
        assert_eq!(unavailable.http_status(), HttpStatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.cli_exit_code(), CliExitCode::UNAVAILABLE);
    }
}
