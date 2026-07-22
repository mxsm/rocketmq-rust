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

use std::ffi::OsString;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use serde::Deserialize;
use serde::Serialize;
use tracing_subscriber::EnvFilter;

use crate::ObservabilityError;
use crate::ReloadConfig;

pub const DEFAULT_LOG_FILTER: &str = "info";

/// Reads the process-level `RUST_LOG` override without silently discarding a
/// non-Unicode value.
pub fn read_rust_log() -> Result<Option<String>, ObservabilityError> {
    std::env::var_os("RUST_LOG").map(decode_environment_filter).transpose()
}

fn decode_environment_filter(value: OsString) -> Result<String, ObservabilityError> {
    value
        .into_string()
        .map_err(|_| ObservabilityError::invalid_config("RUST_LOG contains non-Unicode data"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFilterSource {
    Runtime,
    Cli,
    Env,
    Config,
    Default,
}

impl fmt::Display for LogFilterSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Runtime => "runtime",
            Self::Cli => "cli",
            Self::Env => "env",
            Self::Config => "config",
            Self::Default => "default",
        })
    }
}

impl LogFilterSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Cli => "cli",
            Self::Env => "env",
            Self::Config => "config",
            Self::Default => "default",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LogFilterInputs<'a> {
    pub runtime: Option<&'a str>,
    pub cli: Option<&'a str>,
    pub environment: Option<&'a str>,
    pub config: Option<&'a str>,
    pub legacy_config: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedLogFilter {
    filter: String,
    source: LogFilterSource,
}

impl ResolvedLogFilter {
    pub fn filter(&self) -> &str {
        &self.filter
    }

    pub const fn source(&self) -> LogFilterSource {
        self.source
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFilterReloadRequest {
    filter: String,
}

impl LogFilterReloadRequest {
    pub fn new(filter: impl Into<String>) -> Self {
        Self { filter: filter.into() }
    }

    pub fn filter(&self) -> &str {
        &self.filter
    }
}

type ReloadOperation = dyn Fn(EnvFilter) -> Result<(), ObservabilityError> + Send + Sync + 'static;

struct LogFilterHandleInner {
    service_name: Arc<str>,
    current: RwLock<ResolvedLogFilter>,
    reload: Box<ReloadOperation>,
    operation_lock: Mutex<()>,
}

#[derive(Clone)]
pub struct LogFilterHandle {
    inner: Arc<LogFilterHandleInner>,
}

impl LogFilterHandle {
    pub(crate) fn new<F>(service_name: impl Into<Arc<str>>, initial: ResolvedLogFilter, reload: F) -> Self
    where
        F: Fn(EnvFilter) -> Result<(), ObservabilityError> + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(LogFilterHandleInner {
                service_name: service_name.into(),
                current: RwLock::new(initial),
                reload: Box::new(reload),
                operation_lock: Mutex::new(()),
            }),
        }
    }

    pub fn current(&self) -> ResolvedLogFilter {
        self.inner
            .current
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn reload(&self, request: LogFilterReloadRequest) -> Result<ResolvedLogFilter, ObservabilityError> {
        let resolved = LogFilterResolver::resolve(LogFilterInputs {
            runtime: Some(request.filter()),
            ..LogFilterInputs::default()
        });
        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                crate::metrics::log_filter::record_reload(
                    self.inner.service_name.as_ref(),
                    false,
                    LogFilterSource::Runtime,
                );
                return Err(error);
            }
        };
        self.apply_resolved(resolved)
    }

    /// Restores a previously resolved startup baseline, including its original
    /// source metadata.
    pub fn restore(&self, baseline: &ResolvedLogFilter) -> Result<ResolvedLogFilter, ObservabilityError> {
        self.apply_resolved(baseline.clone())
    }

    fn apply_resolved(&self, resolved: ResolvedLogFilter) -> Result<ResolvedLogFilter, ObservabilityError> {
        let filter = EnvFilter::try_new(resolved.filter())
            .map_err(|error| ObservabilityError::invalid_log_filter(resolved.filter(), error))?;
        let _operation = self
            .inner
            .operation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = self.current();
        if let Err(error) = (self.inner.reload)(filter) {
            crate::metrics::log_filter::record_reload(self.inner.service_name.as_ref(), false, resolved.source());
            return Err(error);
        }
        *self
            .inner
            .current
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = resolved.clone();
        crate::metrics::log_filter::record_reload(self.inner.service_name.as_ref(), true, resolved.source());
        crate::metrics::log_filter::set_active(
            self.inner.service_name.as_ref(),
            Some(previous.source()),
            resolved.source(),
        );
        Ok(resolved)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingOverrides {
    pub logging: LoggingOverrideConfig,
    #[serde(rename = "logFilter", alias = "log_filter")]
    pub log_filter: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingOverrideConfig {
    pub filter: Option<String>,
    pub reload: ReloadConfig,
}

pub struct LogFilterResolver;

impl LogFilterResolver {
    pub fn resolve(inputs: LogFilterInputs<'_>) -> Result<ResolvedLogFilter, ObservabilityError> {
        let config = normalized_config_filter(inputs.config, inputs.legacy_config)?;
        let (candidate, source) = inputs
            .runtime
            .map(|value| (value, LogFilterSource::Runtime))
            .or_else(|| inputs.cli.map(|value| (value, LogFilterSource::Cli)))
            .or_else(|| inputs.environment.map(|value| (value, LogFilterSource::Env)))
            .or_else(|| config.map(|value| (value, LogFilterSource::Config)))
            .unwrap_or((DEFAULT_LOG_FILTER, LogFilterSource::Default));
        let candidate = candidate.trim();
        if candidate.is_empty() {
            return Err(ObservabilityError::invalid_log_filter(
                candidate,
                "log filter must not be blank",
            ));
        }

        EnvFilter::try_new(candidate).map_err(|error| ObservabilityError::invalid_log_filter(candidate, error))?;
        Ok(ResolvedLogFilter {
            filter: candidate.to_string(),
            source,
        })
    }
}

fn normalized_config_filter<'a>(
    config: Option<&'a str>,
    legacy_config: Option<&'a str>,
) -> Result<Option<&'a str>, ObservabilityError> {
    match (config, legacy_config) {
        (Some(config), Some(legacy)) if config.trim() != legacy.trim() => Err(ObservabilityError::invalid_config(
            "logging.filter conflicts with the Java-compatible logFilter value",
        )),
        (Some(config), _) => Ok(Some(config)),
        (None, Some(legacy)) => Ok(Some(legacy)),
        (None, None) => Ok(None),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn non_unicode_environment_filter_is_rejected() {
        use std::os::unix::ffi::OsStringExt;

        let error = decode_environment_filter(OsString::from_vec(vec![0xff]))
            .expect_err("non-Unicode environment values must fail closed");

        assert!(matches!(error, ObservabilityError::InvalidConfig(_)));
    }
}
