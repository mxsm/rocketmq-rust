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

use crate::context::ErrorContext;
use crate::fields;
use crate::kind::ErrorKind;
use crate::ErrorDescriptor;
use crate::OBSERVABILITY_CONFIGURATION_INVALID;
use crate::OBSERVABILITY_FEATURE_DISABLED;
use crate::OBSERVABILITY_INITIALIZATION_FAILED;
use crate::OBSERVABILITY_LOG_FILTER_INVALID;
use crate::OBSERVABILITY_SHUTDOWN_FAILED;
use crate::OBSERVABILITY_SUBSCRIBER_INSTALLATION_FAILED;
use thiserror::Error;

#[derive(Debug, Error)]
/// Identifies the observability error state.
pub enum ObservabilityError {
    #[error("observability feature '{0}' is not enabled")]
    /// Represents the feature disabled case.
    FeatureDisabled(&'static str),

    #[error("invalid observability config: {0}")]
    /// Represents the invalid config case.
    InvalidConfig(String),

    #[error("metrics initialization failed: {0}")]
    /// Represents the metrics init case.
    MetricsInit(String),

    #[error("traces initialization failed: {0}")]
    /// Represents the traces init case.
    TracesInit(String),

    #[error("logs initialization failed: {0}")]
    /// Represents the logs init case.
    LogsInit(String),

    #[error("logging initialization failed: {0}")]
    /// Represents the logging init case.
    LoggingInit(String),

    #[error("invalid log filter '{filter}': {error}")]
    /// Represents the invalid log filter case.
    InvalidLogFilter {
        /// The filter value.
        filter: String,
        /// The error value.
        error: String,
    },

    #[error("tracing subscriber installation failed: attempted={attempted}, installed={installed}")]
    /// Represents the subscriber install failed case.
    SubscriberInstallFailed {
        /// Whether attempted.
        attempted: bool,
        /// Whether installed.
        installed: bool,
    },

    #[error("metrics shutdown failed: {0}")]
    /// Represents the metrics shutdown case.
    MetricsShutdown(String),

    #[error("traces shutdown failed: {0}")]
    /// Represents the traces shutdown case.
    TracesShutdown(String),

    #[error("logs shutdown failed: {0}")]
    /// Represents the logs shutdown case.
    LogsShutdown(String),
}

impl ObservabilityError {
    /// Returns the canonical descriptor for this observability failure.
    pub const fn descriptor(&self) -> &'static ErrorDescriptor {
        match self {
            Self::FeatureDisabled(_) => &OBSERVABILITY_FEATURE_DISABLED,
            Self::InvalidConfig(_) => &OBSERVABILITY_CONFIGURATION_INVALID,
            Self::MetricsInit(_) | Self::TracesInit(_) | Self::LogsInit(_) | Self::LoggingInit(_) => {
                &OBSERVABILITY_INITIALIZATION_FAILED
            }
            Self::InvalidLogFilter { .. } => &OBSERVABILITY_LOG_FILTER_INVALID,
            Self::SubscriberInstallFailed { .. } => &OBSERVABILITY_SUBSCRIBER_INSTALLATION_FAILED,
            Self::MetricsShutdown(_) | Self::TracesShutdown(_) | Self::LogsShutdown(_) => {
                &OBSERVABILITY_SHUTDOWN_FAILED
            }
        }
    }

    /// Returns the kind.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::FeatureDisabled(_) => ErrorKind::ObservabilityFeatureDisabled,
            Self::InvalidConfig(_) => ErrorKind::ObservabilityConfigInvalid,
            Self::MetricsInit(_) => ErrorKind::ObservabilityMetricsInitFailed,
            Self::TracesInit(_) => ErrorKind::ObservabilityTracesInitFailed,
            Self::LogsInit(_) => ErrorKind::ObservabilityLogsInitFailed,
            Self::LoggingInit(_) => ErrorKind::ObservabilityLoggingInitFailed,
            Self::InvalidLogFilter { .. } => ErrorKind::ObservabilityLogFilterInvalid,
            Self::SubscriberInstallFailed { .. } => ErrorKind::ObservabilitySubscriberInstallFailed,
            Self::MetricsShutdown(_) => ErrorKind::ObservabilityMetricsShutdownFailed,
            Self::TracesShutdown(_) => ErrorKind::ObservabilityTracesShutdownFailed,
            Self::LogsShutdown(_) => ErrorKind::ObservabilityLogsShutdownFailed,
        }
    }

    /// Returns the context.
    pub fn context(&self) -> ErrorContext {
        match self {
            Self::FeatureDisabled(feature) => ErrorContext::new().with_text(fields::FEATURE, *feature),
            Self::InvalidConfig(_) => ErrorContext::new().with_secret_presence(fields::REASON_PRESENT),
            Self::MetricsInit(_) => observability_failure_context("metrics"),
            Self::TracesInit(_) => observability_failure_context("traces"),
            Self::LogsInit(_) => observability_failure_context("logs"),
            Self::LoggingInit(_) => observability_failure_context("subscriber"),
            Self::InvalidLogFilter { .. } => ErrorContext::new()
                .with_secret_presence(fields::FILTER_PRESENT)
                .with_secret_presence(fields::ERROR_PRESENT),
            Self::SubscriberInstallFailed { attempted, installed } => ErrorContext::new()
                .with_bool(fields::ATTEMPTED, *attempted)
                .with_bool(fields::INSTALLED, *installed),
            Self::MetricsShutdown(_) => observability_failure_context("metrics"),
            Self::TracesShutdown(_) => observability_failure_context("traces"),
            Self::LogsShutdown(_) => observability_failure_context("logs"),
        }
    }

    /// Creates the invalid config value.
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(message.into())
    }

    /// Creates the metrics init value.
    pub fn metrics_init(error: impl ToString) -> Self {
        Self::MetricsInit(error.to_string())
    }

    /// Creates the traces init value.
    pub fn traces_init(error: impl ToString) -> Self {
        Self::TracesInit(error.to_string())
    }

    /// Creates the logs init value.
    pub fn logs_init(error: impl ToString) -> Self {
        Self::LogsInit(error.to_string())
    }

    /// Creates the logging init value.
    pub fn logging_init(error: impl ToString) -> Self {
        Self::LoggingInit(error.to_string())
    }

    /// Creates the invalid log filter value.
    pub fn invalid_log_filter(filter: impl Into<String>, error: impl ToString) -> Self {
        Self::InvalidLogFilter {
            filter: filter.into(),
            error: error.to_string(),
        }
    }

    /// Creates the subscriber install failed value.
    pub fn subscriber_install_failed(attempted: bool, installed: bool) -> Self {
        Self::SubscriberInstallFailed { attempted, installed }
    }

    /// Creates the metrics shutdown value.
    pub fn metrics_shutdown(error: impl ToString) -> Self {
        Self::MetricsShutdown(error.to_string())
    }

    /// Creates the traces shutdown value.
    pub fn traces_shutdown(error: impl ToString) -> Self {
        Self::TracesShutdown(error.to_string())
    }

    /// Creates the logs shutdown value.
    pub fn logs_shutdown(error: impl ToString) -> Self {
        Self::LogsShutdown(error.to_string())
    }
}

fn observability_failure_context(signal: &'static str) -> ErrorContext {
    ErrorContext::new()
        .with_text(fields::OBSERVABILITY_SIGNAL, signal)
        .with_secret_presence(fields::REASON_PRESENT)
}
