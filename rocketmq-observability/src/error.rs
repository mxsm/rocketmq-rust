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

//! Catalog-backed error surface for observability bootstrap and shutdown.

use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt;
use std::panic::Location;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::DiagnosticView;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::SharedError;
use thiserror::Error;

/// Result returned by observability operations.
pub type ObservabilityResult<T> = std::result::Result<T, ObservabilityError>;

/// The closed observability operation retained outside catalog policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObservabilityOperation {
    /// A requested Cargo feature is disabled.
    FeatureDisabled,
    /// Observability configuration validation.
    ValidateConfiguration,
    /// Metrics initialization.
    InitializeMetrics,
    /// Trace initialization.
    InitializeTraces,
    /// Log initialization.
    InitializeLogs,
    /// Local tracing-subscriber initialization.
    InitializeSubscriber,
    /// Log-filter validation.
    ValidateLogFilter,
    /// Global subscriber installation.
    InstallSubscriber {
        /// Whether installation was attempted.
        attempted: bool,
        /// Whether installation succeeded.
        installed: bool,
    },
    /// Metrics shutdown.
    ShutdownMetrics,
    /// Trace shutdown.
    ShutdownTraces,
    /// Log shutdown.
    ShutdownLogs,
}

/// A string-only diagnostic retained as a typed source.
///
/// The canonical error never renders this value. Code that deliberately walks
/// the source chain may inspect it for local diagnostics.
#[derive(Debug, Error)]
#[error("observability failure detail is available")]
pub struct ObservabilityFailureDetail {
    detail: String,
}

impl ObservabilityFailureDetail {
    /// Returns the original diagnostic detail.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Opaque observability error backed by one canonical shared error.
#[derive(Clone)]
pub struct ObservabilityError {
    error: SharedError,
    operation: ObservabilityOperation,
}

impl ObservabilityError {
    #[track_caller]
    fn source_free(
        descriptor: &'static ErrorDescriptor,
        operation: ObservabilityOperation,
        context: ErrorContext,
    ) -> Self {
        Self {
            error: Arc::new(CanonicalError::new(descriptor).with_context(context)),
            operation,
        }
    }

    #[track_caller]
    fn caused_by(
        descriptor: &'static ErrorDescriptor,
        operation: ObservabilityOperation,
        context: ErrorContext,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
            error: Arc::new(CanonicalError::caused_by(descriptor, source).with_context(context)),
            operation,
        }
    }

    #[track_caller]
    fn with_detail(
        descriptor: &'static ErrorDescriptor,
        operation: ObservabilityOperation,
        context: ErrorContext,
        detail: impl ToString,
    ) -> Self {
        Self::caused_by(
            descriptor,
            operation,
            context,
            ObservabilityFailureDetail {
                detail: detail.to_string(),
            },
        )
    }

    /// Creates a failure for a requested observability feature that is disabled.
    #[must_use]
    #[track_caller]
    pub fn feature_disabled(feature: &'static str) -> Self {
        Self::source_free(
            &rocketmq_error::OBSERVABILITY_FEATURE_DISABLED,
            ObservabilityOperation::FeatureDisabled,
            ErrorContext::new().with_text(fields::FEATURE, feature),
        )
    }

    /// Creates an invalid-configuration failure.
    #[must_use]
    #[track_caller]
    pub fn invalid_config(detail: impl Into<String>) -> Self {
        Self::with_detail(
            &rocketmq_error::OBSERVABILITY_CONFIGURATION_INVALID,
            ObservabilityOperation::ValidateConfiguration,
            ErrorContext::new().with_secret_presence(fields::REASON_PRESENT),
            detail.into(),
        )
    }

    /// Creates a metrics-initialization failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn metrics_init(detail: impl ToString) -> Self {
        Self::initialization_detail(ObservabilityOperation::InitializeMetrics, "metrics", detail)
    }

    /// Creates a metrics-initialization failure while retaining its typed source.
    #[must_use]
    #[track_caller]
    pub fn metrics_init_source(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::initialization_source(ObservabilityOperation::InitializeMetrics, "metrics", source)
    }

    /// Creates a trace-initialization failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn traces_init(detail: impl ToString) -> Self {
        Self::initialization_detail(ObservabilityOperation::InitializeTraces, "traces", detail)
    }

    /// Creates a trace-initialization failure while retaining its typed source.
    #[must_use]
    #[track_caller]
    pub fn traces_init_source(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::initialization_source(ObservabilityOperation::InitializeTraces, "traces", source)
    }

    /// Creates a log-initialization failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn logs_init(detail: impl ToString) -> Self {
        Self::initialization_detail(ObservabilityOperation::InitializeLogs, "logs", detail)
    }

    /// Creates a log-initialization failure while retaining its typed source.
    #[must_use]
    #[track_caller]
    pub fn logs_init_source(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::initialization_source(ObservabilityOperation::InitializeLogs, "logs", source)
    }

    /// Creates a subscriber-initialization failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn logging_init(detail: impl ToString) -> Self {
        Self::initialization_detail(ObservabilityOperation::InitializeSubscriber, "subscriber", detail)
    }

    /// Creates a subscriber-initialization failure while retaining its typed source.
    #[must_use]
    #[track_caller]
    pub fn logging_init_source(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::initialization_source(ObservabilityOperation::InitializeSubscriber, "subscriber", source)
    }

    #[track_caller]
    fn initialization_detail(operation: ObservabilityOperation, signal: &'static str, detail: impl ToString) -> Self {
        Self::with_detail(
            &rocketmq_error::OBSERVABILITY_INITIALIZATION_FAILED,
            operation,
            observability_failure_context(signal),
            detail,
        )
    }

    #[track_caller]
    fn initialization_source(
        operation: ObservabilityOperation,
        signal: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::caused_by(
            &rocketmq_error::OBSERVABILITY_INITIALIZATION_FAILED,
            operation,
            observability_failure_context(signal),
            source,
        )
    }

    /// Creates an invalid-log-filter failure while retaining the parser source.
    #[must_use]
    #[track_caller]
    pub fn invalid_log_filter(_filter: impl Into<String>, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::caused_by(
            &rocketmq_error::OBSERVABILITY_LOG_FILTER_INVALID,
            ObservabilityOperation::ValidateLogFilter,
            ErrorContext::new()
                .with_secret_presence(fields::FILTER_PRESENT)
                .with_secret_presence(fields::ERROR_PRESENT),
            source,
        )
    }

    /// Creates an invalid-log-filter failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn invalid_log_filter_detail(_filter: impl Into<String>, detail: impl ToString) -> Self {
        Self::with_detail(
            &rocketmq_error::OBSERVABILITY_LOG_FILTER_INVALID,
            ObservabilityOperation::ValidateLogFilter,
            ErrorContext::new()
                .with_secret_presence(fields::FILTER_PRESENT)
                .with_secret_presence(fields::ERROR_PRESENT),
            detail,
        )
    }

    /// Creates a subscriber-installation failure.
    #[must_use]
    #[track_caller]
    pub fn subscriber_install_failed(attempted: bool, installed: bool) -> Self {
        Self::source_free(
            &rocketmq_error::OBSERVABILITY_SUBSCRIBER_INSTALLATION_FAILED,
            ObservabilityOperation::InstallSubscriber { attempted, installed },
            ErrorContext::new()
                .with_bool(fields::ATTEMPTED, attempted)
                .with_bool(fields::INSTALLED, installed),
        )
    }

    /// Creates a metrics-shutdown failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn metrics_shutdown(detail: impl ToString) -> Self {
        Self::shutdown_detail(ObservabilityOperation::ShutdownMetrics, "metrics", detail)
    }

    /// Creates a trace-shutdown failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn traces_shutdown(detail: impl ToString) -> Self {
        Self::shutdown_detail(ObservabilityOperation::ShutdownTraces, "traces", detail)
    }

    /// Creates a log-shutdown failure from display-only detail.
    #[must_use]
    #[track_caller]
    pub fn logs_shutdown(detail: impl ToString) -> Self {
        Self::shutdown_detail(ObservabilityOperation::ShutdownLogs, "logs", detail)
    }

    #[track_caller]
    fn shutdown_detail(operation: ObservabilityOperation, signal: &'static str, detail: impl ToString) -> Self {
        Self::with_detail(
            &rocketmq_error::OBSERVABILITY_SHUTDOWN_FAILED,
            operation,
            observability_failure_context(signal),
            detail,
        )
    }

    /// Returns the catalog descriptor that owns identity and policy.
    #[must_use]
    pub fn descriptor(&self) -> &'static ErrorDescriptor {
        self.error.descriptor()
    }

    /// Returns the stable catalog code.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        self.error.code()
    }

    /// Returns the descriptor-owned canonical condition.
    #[must_use]
    pub fn condition(&self) -> CanonicalCondition {
        self.error.condition()
    }

    /// Returns the descriptor-owned recovery hint.
    #[must_use]
    pub fn recovery_hint(&self) -> RecoveryHint {
        self.error.recovery_hint()
    }

    /// Returns the closed observability operation.
    #[must_use]
    pub const fn operation(&self) -> ObservabilityOperation {
        self.operation
    }

    /// Returns descriptor-validated bounded context.
    #[must_use]
    pub fn context(&self) -> &ErrorContext {
        self.error.context()
    }

    /// Returns the first canonical promotion location.
    #[must_use]
    pub fn location(&self) -> &'static Location<'static> {
        self.error.location()
    }

    /// Returns the catalog-controlled backtrace, when captured.
    #[must_use]
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.error.backtrace()
    }

    /// Creates a descriptor-validated public projection.
    ///
    /// # Errors
    ///
    /// Returns an error only if the facade built context that violates its
    /// selected descriptor.
    pub fn public_view(&self) -> Result<PublicErrorView<'_>, rocketmq_error::ViewContextViolation> {
        self.error.public_view()
    }

    /// Creates a descriptor-validated diagnostic projection.
    ///
    /// # Errors
    ///
    /// Returns an error only if the facade built context that violates its
    /// selected descriptor.
    pub fn diagnostic_view(&self) -> Result<DiagnosticView<'_>, rocketmq_error::ViewContextViolation> {
        self.error.diagnostic_view()
    }
}

fn observability_failure_context(signal: &'static str) -> ErrorContext {
    ErrorContext::new()
        .with_text(fields::OBSERVABILITY_SIGNAL, signal)
        .with_secret_presence(fields::REASON_PRESENT)
}

impl fmt::Display for ObservabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.error.as_ref(), formatter)
    }
}

impl fmt::Debug for ObservabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservabilityError")
            .field("code", &self.code())
            .field("condition", &self.condition())
            .field("operation", &self.operation)
            .field("has_source", &self.error.source().is_some())
            .finish()
    }
}

impl StdError for ObservabilityError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.source()
    }
}
