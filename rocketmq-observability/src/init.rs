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

use crate::config::ObservabilityConfig;
use crate::error::ObservabilityError;

#[derive(Debug, Default)]
pub struct TelemetryGuard {
    #[cfg(feature = "otel-metrics")]
    meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    #[cfg(feature = "otel-traces")]
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
    #[cfg(feature = "otel-logs")]
    logger_provider: Option<opentelemetry_sdk::logs::SdkLoggerProvider>,
}

impl TelemetryGuard {
    pub fn noop() -> Self {
        Self::default()
    }

    pub fn shutdown(self) -> Result<(), ObservabilityError> {
        self.shutdown_inner()
    }

    #[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
    fn shutdown_inner(mut self) -> Result<(), ObservabilityError> {
        #[cfg(feature = "otel-metrics")]
        if let Some(provider) = self.meter_provider.take() {
            drop(provider);
        }

        #[cfg(feature = "otel-traces")]
        if let Some(provider) = self.tracer_provider.take() {
            drop(provider);
        }

        #[cfg(feature = "otel-logs")]
        if let Some(provider) = self.logger_provider.take() {
            drop(provider);
        }

        Ok(())
    }

    #[cfg(not(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs")))]
    fn shutdown_inner(self) -> Result<(), ObservabilityError> {
        Ok(())
    }

    #[cfg(feature = "otel-metrics")]
    pub fn meter_provider(&self) -> Option<&opentelemetry_sdk::metrics::SdkMeterProvider> {
        self.meter_provider.as_ref()
    }

    #[cfg(feature = "otel-traces")]
    pub fn tracer_provider(&self) -> Option<&opentelemetry_sdk::trace::SdkTracerProvider> {
        self.tracer_provider.as_ref()
    }

    #[cfg(feature = "otel-logs")]
    pub fn logger_provider(&self) -> Option<&opentelemetry_sdk::logs::SdkLoggerProvider> {
        self.logger_provider.as_ref()
    }
}

pub fn init_observability(config: &ObservabilityConfig) -> Result<TelemetryGuard, ObservabilityError> {
    validate_config(config)?;

    if !config.enabled {
        return Ok(TelemetryGuard::noop());
    }

    #[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
    let mut guard = TelemetryGuard::noop();
    #[cfg(not(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs")))]
    let guard = TelemetryGuard::noop();

    if config.metrics.enabled {
        #[cfg(feature = "otel-metrics")]
        {
            guard.meter_provider = Some(init_metrics(config)?);
        }

        #[cfg(not(feature = "otel-metrics"))]
        return Err(ObservabilityError::FeatureDisabled("otel-metrics"));
    }

    if config.traces.enabled {
        #[cfg(feature = "otel-traces")]
        {
            guard.tracer_provider = Some(init_traces(config)?);
        }

        #[cfg(not(feature = "otel-traces"))]
        return Err(ObservabilityError::FeatureDisabled("otel-traces"));
    }

    if config.logs.enabled {
        #[cfg(feature = "otel-logs")]
        {
            guard.logger_provider = Some(init_logs(config)?);
        }

        #[cfg(not(feature = "otel-logs"))]
        return Err(ObservabilityError::FeatureDisabled("otel-logs"));
    }

    Ok(guard)
}

fn validate_config(config: &ObservabilityConfig) -> Result<(), ObservabilityError> {
    if !(0.0..=1.0).contains(&config.traces.sample_ratio) {
        return Err(ObservabilityError::invalid_config(format!(
            "trace sample_ratio must be between 0.0 and 1.0, got {}",
            config.traces.sample_ratio
        )));
    }

    if config.metrics.cardinality_limit == 0 {
        return Err(ObservabilityError::invalid_config(
            "metrics cardinality_limit must be greater than 0",
        ));
    }

    Ok(())
}

#[cfg(feature = "otel-metrics")]
fn init_metrics(
    _config: &ObservabilityConfig,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, ObservabilityError> {
    Ok(opentelemetry_sdk::metrics::SdkMeterProvider::builder().build())
}

#[cfg(feature = "otel-traces")]
fn init_traces(
    _config: &ObservabilityConfig,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, ObservabilityError> {
    Ok(opentelemetry_sdk::trace::SdkTracerProvider::builder().build())
}

#[cfg(feature = "otel-logs")]
fn init_logs(_config: &ObservabilityConfig) -> Result<opentelemetry_sdk::logs::SdkLoggerProvider, ObservabilityError> {
    Ok(opentelemetry_sdk::logs::SdkLoggerProvider::builder().build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_config_returns_noop_guard() {
        let guard = init_observability(&ObservabilityConfig::default()).expect("noop init should succeed");

        guard.shutdown().expect("noop shutdown should succeed");
    }

    #[test]
    fn invalid_sample_ratio_is_rejected() {
        let mut config = ObservabilityConfig::default();
        config.traces.sample_ratio = 1.1;

        let error = init_observability(&config).expect_err("invalid sample ratio should fail");

        assert!(matches!(error, ObservabilityError::InvalidConfig(_)));
    }

    #[test]
    fn zero_cardinality_limit_is_rejected() {
        let mut config = ObservabilityConfig::default();
        config.metrics.cardinality_limit = 0;

        let error = init_observability(&config).expect_err("zero cardinality limit should fail");

        assert!(matches!(error, ObservabilityError::InvalidConfig(_)));
    }
}
