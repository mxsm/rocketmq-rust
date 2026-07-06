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

use thiserror::Error;

use crate::config::SubscriberInstallStatus;

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

    pub fn subscriber_install_failed(status: SubscriberInstallStatus) -> Self {
        Self::SubscriberInstallFailed {
            attempted: status.attempted,
            installed: status.installed,
        }
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
