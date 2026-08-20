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

use std::ffi::OsStr;
use std::ffi::OsString;
use std::net::IpAddr;
use std::net::SocketAddr;

use crate::metrics::release_identity::ProcessTelemetryConfig;
use crate::metrics::release_identity::METRICS_BIND_ADDR_ENV;
use crate::metrics::release_identity::METRICS_ENABLED_ENV;
use crate::metrics::release_identity::METRICS_EXPORTER_ENV;
use crate::metrics::release_identity::METRICS_PATH_ENV;
use crate::metrics::release_identity::RELEASE_COMMIT_ENV;
use crate::metrics::release_identity::RELEASE_NONCE_ENV;
use crate::ObservabilityError;
use crate::ObservabilityOverrides;
use crate::StandardOtlpEnvironmentStatus;
use crate::TelemetryBootstrapConfig;
use crate::OTEL_EXPORTER_OTLP_ENDPOINT;
use crate::OTEL_EXPORTER_OTLP_PROTOCOL;

/// Raw process environment values used by the deterministic telemetry resolver.
///
/// Missing variables remain `None`; validation and UTF-8 conversion happen only
/// when a present value participates in resolution.
#[derive(Debug, Clone, Default)]
pub struct TelemetryEnvironmentValues {
    /// `ROCKETMQ_RELEASE_COMMIT`, when present.
    pub release_commit: Option<OsString>,
    /// `ROCKETMQ_RELEASE_NONCE`, when present.
    pub release_nonce: Option<OsString>,
    /// `ROCKETMQ_METRICS_ENABLED`, when present.
    pub metrics_enabled: Option<OsString>,
    /// `ROCKETMQ_METRICS_EXPORTER`, when present.
    pub metrics_exporter: Option<OsString>,
    /// `ROCKETMQ_METRICS_BIND_ADDR`, when present.
    pub metrics_bind_addr: Option<OsString>,
    /// `ROCKETMQ_METRICS_PATH`, when present.
    pub metrics_path: Option<OsString>,
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`, when present.
    pub otlp_endpoint: Option<OsString>,
    /// `OTEL_EXPORTER_OTLP_PROTOCOL`, when present.
    pub otlp_protocol: Option<OsString>,
    /// The service-specific trace sample ratio, when configured and present.
    pub trace_sample_ratio: Option<OsString>,
}

impl TelemetryEnvironmentValues {
    /// Reads the supported telemetry variables from the current process.
    pub fn read(spec: TelemetryEnvironmentSpec) -> Self {
        Self::read_with(spec, std::env::var_os)
    }

    fn read_with(spec: TelemetryEnvironmentSpec, mut read: impl FnMut(&'static str) -> Option<OsString>) -> Self {
        Self {
            release_commit: read(RELEASE_COMMIT_ENV),
            release_nonce: read(RELEASE_NONCE_ENV),
            metrics_enabled: read(METRICS_ENABLED_ENV),
            metrics_exporter: read(METRICS_EXPORTER_ENV),
            metrics_bind_addr: read(METRICS_BIND_ADDR_ENV),
            metrics_path: read(METRICS_PATH_ENV),
            otlp_endpoint: read(OTEL_EXPORTER_OTLP_ENDPOINT),
            otlp_protocol: read(OTEL_EXPORTER_OTLP_PROTOCOL),
            trace_sample_ratio: spec.trace_sample_ratio_env.and_then(read),
        }
    }
}

/// Service-specific environment variable names understood by telemetry resolution.
#[derive(Debug, Clone, Copy, Default)]
pub struct TelemetryEnvironmentSpec {
    /// Optional service-specific trace sample ratio variable name.
    pub trace_sample_ratio_env: Option<&'static str>,
}

/// Fully resolved telemetry configuration and its validated process metadata.
#[derive(Debug)]
pub struct TelemetryResolution {
    /// Final merged and validated bootstrap configuration.
    pub bootstrap: TelemetryBootstrapConfig,
    /// Validated release identity and process metrics selection.
    pub process: ProcessTelemetryConfig,
    /// Final Prometheus listener, present only for an active Prometheus exporter.
    pub prometheus_listener_addr: Option<SocketAddr>,
    /// Whether at least one present environment field participated in resolution.
    pub environment_applied: bool,
}

/// Resolves telemetry using values read from the current process environment.
///
/// # Errors
///
/// Returns [`ObservabilityError`] when a present environment value or the final
/// merged configuration is invalid.
pub fn resolve_telemetry_from_env(
    service: &'static str,
    bootstrap: TelemetryBootstrapConfig,
    file: &ObservabilityOverrides,
    spec: TelemetryEnvironmentSpec,
) -> Result<TelemetryResolution, ObservabilityError> {
    let environment = TelemetryEnvironmentValues::read(spec);
    resolve_telemetry_values(service, bootstrap, file, &environment, spec)
}

/// Resolves defaults, file overrides, and caller-supplied environment values.
///
/// The merge order is deterministic: bootstrap defaults, then file overrides,
/// then only environment fields that are present.
///
/// # Errors
///
/// Returns [`ObservabilityError`] when a present environment value or the final
/// merged configuration is invalid.
pub fn resolve_telemetry_values(
    service: &'static str,
    mut bootstrap: TelemetryBootstrapConfig,
    file: &ObservabilityOverrides,
    environment: &TelemetryEnvironmentValues,
    spec: TelemetryEnvironmentSpec,
) -> Result<TelemetryResolution, ObservabilityError> {
    file.apply_to(&mut bootstrap.observability);
    let process = ProcessTelemetryConfig::try_from_observability_and_values(
        service,
        &bootstrap.observability,
        environment.release_commit.as_deref(),
        environment.release_nonce.as_deref(),
        environment.metrics_enabled.as_deref(),
        environment.metrics_exporter.as_deref(),
        environment.metrics_bind_addr.as_deref(),
        environment.metrics_path.as_deref(),
    )
    .map_err(|error| ObservabilityError::invalid_config(error.to_string()))?;
    process.apply_to(&mut bootstrap.observability);
    let otlp_status = apply_standard_otlp_environment_values(
        &mut bootstrap,
        environment.otlp_endpoint.as_deref(),
        environment.otlp_protocol.as_deref(),
    )?;
    let trace_sample_ratio_applied = apply_optional_trace_sample_ratio(
        &mut bootstrap.observability,
        spec.trace_sample_ratio_env,
        environment.trace_sample_ratio.as_deref(),
    )?;
    normalize_and_validate(&bootstrap.observability)?;
    let prometheus_listener_addr = if bootstrap.observability.metrics.enabled
        && bootstrap.observability.metrics.exporter == crate::MetricsExporter::Prometheus
    {
        process.prometheus_listener_addr()
    } else {
        None
    };

    Ok(TelemetryResolution {
        bootstrap,
        process,
        prometheus_listener_addr,
        environment_applied: environment.release_commit.is_some()
            || environment.release_nonce.is_some()
            || environment.metrics_enabled.is_some()
            || environment.metrics_exporter.is_some()
            || environment.metrics_bind_addr.is_some()
            || environment.metrics_path.is_some()
            || otlp_status == StandardOtlpEnvironmentStatus::Applied
            || trace_sample_ratio_applied,
    })
}

/// Validates the final cross-signal observability configuration.
///
/// Error messages name only the invalid field and constraint; endpoint,
/// header, and resource-attribute values are never included.
///
/// # Errors
///
/// Returns [`ObservabilityError::InvalidConfig`] when a ratio, duration,
/// exporter, endpoint, or Prometheus listener setting violates its invariant.
pub fn normalize_and_validate(config: &crate::ObservabilityConfig) -> Result<(), ObservabilityError> {
    if !is_valid_sample_ratio(config.metrics.sample_ratio) {
        return Err(ObservabilityError::invalid_config(
            "metrics.sample_ratio must be finite and between 0.0 and 1.0",
        ));
    }
    if !is_valid_sample_ratio(config.traces.sample_ratio) {
        return Err(ObservabilityError::invalid_config(
            "traces.sample_ratio must be finite and between 0.0 and 1.0",
        ));
    }
    if config.metrics.export_interval_millis == 0 {
        return Err(ObservabilityError::invalid_config(
            "metrics.export_interval_millis must be greater than 0",
        ));
    }
    if config.metrics.export_timeout_millis == 0 {
        return Err(ObservabilityError::invalid_config(
            "metrics.export_timeout_millis must be greater than 0",
        ));
    }
    if config.metrics.cardinality_limit == 0 {
        return Err(ObservabilityError::invalid_config(
            "metrics.cardinality_limit must be greater than 0",
        ));
    }
    if config.otlp.timeout_millis == 0 {
        return Err(ObservabilityError::invalid_config(
            "otlp.timeout_millis must be greater than 0",
        ));
    }

    let uses_otlp = (config.metrics.enabled && config.metrics.exporter == crate::MetricsExporter::OtlpGrpc)
        || (config.traces.enabled && config.traces.exporter == crate::TraceExporter::OtlpGrpc)
        || (config.logs.enabled && config.logs.exporter == crate::LogsExporter::OtlpGrpc);
    if uses_otlp && config.otlp.endpoint.trim().is_empty() {
        return Err(ObservabilityError::invalid_config(
            "otlp.endpoint must not be blank when an OTLP exporter is enabled",
        ));
    }
    if uses_otlp && config.otlp.protocol != crate::OtlpProtocol::Grpc {
        return Err(ObservabilityError::invalid_config(
            "only OTLP gRPC is implemented; set observability.otlp.protocol to grpc",
        ));
    }

    if parse_prometheus_host(&config.prometheus.host).is_none() {
        return Err(ObservabilityError::invalid_config(
            "prometheus.host must be an IP address",
        ));
    }
    if config.prometheus.port == 0 {
        return Err(ObservabilityError::invalid_config(
            "prometheus.port must be greater than 0",
        ));
    }
    if !is_canonical_prometheus_path(&config.prometheus.path) {
        return Err(ObservabilityError::invalid_config(
            "prometheus.path must be a canonical absolute path of at most 128 ASCII characters",
        ));
    }

    Ok(())
}

fn is_valid_sample_ratio(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

pub(crate) fn parse_prometheus_host(value: &str) -> Option<IpAddr> {
    if value.trim() != value {
        return None;
    }
    value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(value)
        .parse()
        .ok()
}

pub(crate) fn is_canonical_prometheus_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.starts_with('/')
        && (value == "/"
            || value
                .split('/')
                .skip(1)
                .all(|segment| !segment.is_empty() && segment != "." && segment != ".."))
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'/' | b'-' | b'_' | b'.'))
}

fn apply_optional_trace_sample_ratio(
    config: &mut crate::ObservabilityConfig,
    environment_name: Option<&'static str>,
    value: Option<&OsStr>,
) -> Result<bool, ObservabilityError> {
    let (Some(environment_name), Some(value)) = (environment_name, value) else {
        return Ok(false);
    };
    let value = value
        .to_str()
        .ok_or_else(|| ObservabilityError::invalid_config(format!("{environment_name} must contain valid UTF-8")))?;
    let sample_ratio = value.parse::<f64>().map_err(|_| {
        ObservabilityError::invalid_config(format!("{environment_name} must be a floating-point number"))
    })?;
    config.traces.sample_ratio = sample_ratio;
    Ok(true)
}

pub(crate) fn apply_standard_otlp_environment_values(
    config: &mut TelemetryBootstrapConfig,
    endpoint: Option<&OsStr>,
    protocol: Option<&OsStr>,
) -> Result<StandardOtlpEnvironmentStatus, ObservabilityError> {
    let Some(endpoint) = endpoint else {
        return Ok(StandardOtlpEnvironmentStatus::Unchanged);
    };
    let endpoint = endpoint.to_str().ok_or_else(|| {
        ObservabilityError::invalid_config(format!("{OTEL_EXPORTER_OTLP_ENDPOINT} must contain valid UTF-8"))
    })?;
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Ok(StandardOtlpEnvironmentStatus::Unchanged);
    }

    let protocol = protocol.ok_or_else(|| {
        ObservabilityError::invalid_config(format!(
            "{OTEL_EXPORTER_OTLP_PROTOCOL} must be set to grpc when {OTEL_EXPORTER_OTLP_ENDPOINT} is configured"
        ))
    })?;
    let protocol = protocol.to_str().ok_or_else(|| {
        ObservabilityError::invalid_config(format!("{OTEL_EXPORTER_OTLP_PROTOCOL} must contain valid UTF-8"))
    })?;
    if protocol != "grpc" {
        return Err(ObservabilityError::invalid_config(format!(
            "{OTEL_EXPORTER_OTLP_PROTOCOL} must be exactly grpc"
        )));
    }

    config.observability.enabled = true;
    config.observability.metrics.enabled = true;
    config.observability.metrics.exporter = crate::MetricsExporter::OtlpGrpc;
    config.observability.traces.enabled = true;
    config.observability.traces.exporter = crate::TraceExporter::OtlpGrpc;
    config.observability.logs.enabled = true;
    config.observability.logs.exporter = crate::LogsExporter::OtlpGrpc;
    config.observability.otlp.endpoint = endpoint.to_owned();
    config.observability.otlp.protocol = crate::OtlpProtocol::Grpc;

    Ok(StandardOtlpEnvironmentStatus::Applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_reader_uses_supported_names_without_defaulting_absent_values() {
        let mut names = Vec::new();

        let values = TelemetryEnvironmentValues::read_with(
            TelemetryEnvironmentSpec {
                trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
            },
            |name| {
                names.push(name);
                match name {
                    "ROCKETMQ_RELEASE_COMMIT" => Some("commit-value".into()),
                    "ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO" => Some("0.5".into()),
                    _ => None,
                }
            },
        );

        assert_eq!(
            names,
            [
                "ROCKETMQ_RELEASE_COMMIT",
                "ROCKETMQ_RELEASE_NONCE",
                "ROCKETMQ_METRICS_ENABLED",
                "ROCKETMQ_METRICS_EXPORTER",
                "ROCKETMQ_METRICS_BIND_ADDR",
                "ROCKETMQ_METRICS_PATH",
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "OTEL_EXPORTER_OTLP_PROTOCOL",
                "ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO",
            ]
        );
        assert_eq!(
            values.release_commit.as_deref(),
            Some(std::ffi::OsStr::new("commit-value"))
        );
        assert_eq!(values.release_nonce, None);
        assert_eq!(values.metrics_enabled, None);
        assert_eq!(values.metrics_exporter, None);
        assert_eq!(values.metrics_bind_addr, None);
        assert_eq!(values.metrics_path, None);
        assert_eq!(values.otlp_endpoint, None);
        assert_eq!(values.otlp_protocol, None);
        assert_eq!(values.trace_sample_ratio.as_deref(), Some(std::ffi::OsStr::new("0.5")));
    }
}
