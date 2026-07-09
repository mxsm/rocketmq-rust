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
use crate::context::Sensitive;
use crate::kind::ErrorKind;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("observability feature '{0}' is not enabled")]
    FeatureDisabled(&'static str),

    #[error("invalid observability config: {0}")]
    InvalidConfig(String),

    #[error("metrics initialization failed: {0}")]
    MetricsInit(String),

    #[error("traces initialization failed: {0}")]
    TracesInit(String),

    #[error("logs initialization failed: {0}")]
    LogsInit(String),

    #[error("logging initialization failed: {0}")]
    LoggingInit(String),

    #[error("invalid log filter '{filter}': {error}")]
    InvalidLogFilter { filter: String, error: String },

    #[error("tracing subscriber installation failed: attempted={attempted}, installed={installed}")]
    SubscriberInstallFailed { attempted: bool, installed: bool },

    #[error("metrics shutdown failed: {0}")]
    MetricsShutdown(String),

    #[error("traces shutdown failed: {0}")]
    TracesShutdown(String),

    #[error("logs shutdown failed: {0}")]
    LogsShutdown(String),
}

impl ObservabilityError {
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

    pub fn context(&self) -> ErrorContext {
        match self {
            Self::FeatureDisabled(feature) => ErrorContext::new().with_field("feature", *feature),
            Self::InvalidConfig(reason)
            | Self::MetricsInit(reason)
            | Self::TracesInit(reason)
            | Self::LogsInit(reason)
            | Self::LoggingInit(reason)
            | Self::MetricsShutdown(reason)
            | Self::TracesShutdown(reason)
            | Self::LogsShutdown(reason) => {
                ErrorContext::new().with_sensitive("reason", Sensitive::new(reason.clone()))
            }
            Self::InvalidLogFilter { filter, error } => ErrorContext::new()
                .with_sensitive("filter", Sensitive::new(filter.clone()))
                .with_sensitive("error", Sensitive::new(error.clone())),
            Self::SubscriberInstallFailed { attempted, installed } => ErrorContext::new()
                .with_field("attempted", attempted.to_string())
                .with_field("installed", installed.to_string()),
        }
    }

    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(message.into())
    }

    pub fn metrics_init(error: impl ToString) -> Self {
        Self::MetricsInit(error.to_string())
    }

    pub fn traces_init(error: impl ToString) -> Self {
        Self::TracesInit(error.to_string())
    }

    pub fn logs_init(error: impl ToString) -> Self {
        Self::LogsInit(error.to_string())
    }

    pub fn logging_init(error: impl ToString) -> Self {
        Self::LoggingInit(error.to_string())
    }

    pub fn invalid_log_filter(filter: impl Into<String>, error: impl ToString) -> Self {
        Self::InvalidLogFilter {
            filter: filter.into(),
            error: error.to_string(),
        }
    }

    pub fn subscriber_install_failed(attempted: bool, installed: bool) -> Self {
        Self::SubscriberInstallFailed { attempted, installed }
    }

    pub fn metrics_shutdown(error: impl ToString) -> Self {
        Self::MetricsShutdown(error.to_string())
    }

    pub fn traces_shutdown(error: impl ToString) -> Self {
        Self::TracesShutdown(error.to_string())
    }

    pub fn logs_shutdown(error: impl ToString) -> Self {
        Self::LogsShutdown(error.to_string())
    }
}
