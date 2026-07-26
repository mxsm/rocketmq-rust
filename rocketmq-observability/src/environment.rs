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

use crate::LogsExporter;
use crate::MetricsExporter;
use crate::ObservabilityError;
use crate::OtlpProtocol;
use crate::TelemetryBootstrapConfig;
use crate::TraceExporter;

pub const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
pub const OTEL_EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandardOtlpEnvironmentStatus {
    Unchanged,
    Applied,
}

/// Applies the standard process-wide OTLP endpoint and protocol variables.
///
/// A missing or blank endpoint leaves the existing configuration untouched. A non-empty endpoint
/// activates all three telemetry signals only when the protocol is exactly `grpc`.
///
/// # Errors
///
/// Returns [`ObservabilityError::InvalidConfig`] when a configured endpoint or protocol is not
/// valid UTF-8, when the protocol is missing, or when it is not exactly `grpc`. Error text names
/// only the invalid variable and never includes its value.
pub fn apply_standard_otlp_environment(
    config: &mut TelemetryBootstrapConfig,
) -> Result<StandardOtlpEnvironmentStatus, ObservabilityError> {
    let endpoint = std::env::var_os(OTEL_EXPORTER_OTLP_ENDPOINT);
    let protocol = std::env::var_os(OTEL_EXPORTER_OTLP_PROTOCOL);
    apply_standard_otlp_environment_values(config, endpoint.as_deref(), protocol.as_deref())
}

/// Applies caller-supplied standard OTLP environment values without reading process state.
///
/// This is the deterministic parsing boundary used by service bootstrap tests. Configuration is
/// mutated only after both values have been validated.
///
/// # Errors
///
/// Returns [`ObservabilityError::InvalidConfig`] under the same fail-closed conditions as
/// [`apply_standard_otlp_environment`].
pub fn apply_standard_otlp_environment_values(
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
    config.observability.metrics.exporter = MetricsExporter::OtlpGrpc;
    config.observability.traces.enabled = true;
    config.observability.traces.exporter = TraceExporter::OtlpGrpc;
    config.observability.logs.enabled = true;
    config.observability.logs.exporter = LogsExporter::OtlpGrpc;
    config.observability.otlp.endpoint = endpoint.to_owned();
    config.observability.otlp.protocol = OtlpProtocol::Grpc;

    Ok(StandardOtlpEnvironmentStatus::Applied)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    #[cfg(any(unix, windows))]
    use std::ffi::OsString;

    use super::*;

    #[test]
    fn missing_or_blank_endpoint_preserves_existing_configuration() {
        let mut config = configured_bootstrap();
        let before = snapshot(&config);

        assert_eq!(
            apply_standard_otlp_environment_values(&mut config, None, Some(OsStr::new("http/protobuf"))).unwrap(),
            StandardOtlpEnvironmentStatus::Unchanged
        );
        assert_eq!(snapshot(&config), before);

        assert_eq!(
            apply_standard_otlp_environment_values(&mut config, Some(OsStr::new("  ")), Some(OsStr::new("grpc")))
                .unwrap(),
            StandardOtlpEnvironmentStatus::Unchanged
        );
        assert_eq!(snapshot(&config), before);
    }

    #[test]
    fn exact_grpc_protocol_enables_all_otlp_signals() {
        let mut config = configured_bootstrap();

        let status = apply_standard_otlp_environment_values(
            &mut config,
            Some(OsStr::new(" http://collector:4317 ")),
            Some(OsStr::new("grpc")),
        )
        .unwrap();

        assert_eq!(status, StandardOtlpEnvironmentStatus::Applied);
        assert!(config.observability.enabled);
        assert!(config.observability.metrics.enabled);
        assert_eq!(config.observability.metrics.exporter, MetricsExporter::OtlpGrpc);
        assert!(config.observability.traces.enabled);
        assert_eq!(config.observability.traces.exporter, TraceExporter::OtlpGrpc);
        assert!(config.observability.logs.enabled);
        assert_eq!(config.observability.logs.exporter, LogsExporter::OtlpGrpc);
        assert_eq!(config.observability.otlp.endpoint, "http://collector:4317");
        assert_eq!(config.observability.otlp.protocol, OtlpProtocol::Grpc);
    }

    #[test]
    fn missing_or_non_grpc_protocol_fails_without_mutating_configuration() {
        let mut config = configured_bootstrap();
        let before = snapshot(&config);

        let missing =
            apply_standard_otlp_environment_values(&mut config, Some(OsStr::new("http://collector:4317")), None)
                .unwrap_err();
        assert!(!missing.to_string().contains("http://collector:4317"));
        assert_eq!(snapshot(&config), before);

        let invalid = apply_standard_otlp_environment_values(
            &mut config,
            Some(OsStr::new("http://user:secret@collector:4317")),
            Some(OsStr::new("http/protobuf")),
        )
        .unwrap_err();
        assert!(!invalid.to_string().contains("secret"));
        assert_eq!(snapshot(&config), before);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn non_utf8_values_fail_without_mutating_configuration() {
        let mut config = configured_bootstrap();
        let before = snapshot(&config);
        let invalid = invalid_os_string();

        assert!(apply_standard_otlp_environment_values(
            &mut config,
            Some(invalid.as_os_str()),
            Some(OsStr::new("grpc"))
        )
        .is_err());
        assert_eq!(snapshot(&config), before);

        assert!(apply_standard_otlp_environment_values(
            &mut config,
            Some(OsStr::new("http://collector:4317")),
            Some(invalid.as_os_str())
        )
        .is_err());
        assert_eq!(snapshot(&config), before);
    }

    fn configured_bootstrap() -> TelemetryBootstrapConfig {
        let mut config = TelemetryBootstrapConfig::default();
        config.observability.enabled = true;
        config.observability.metrics.enabled = true;
        config.observability.metrics.exporter = MetricsExporter::Log;
        config.observability.traces.enabled = false;
        config.observability.traces.exporter = TraceExporter::Disable;
        config.observability.logs.enabled = true;
        config.observability.logs.exporter = LogsExporter::Log;
        config.observability.otlp.endpoint = "http://existing:4317".to_owned();
        config
    }

    fn snapshot(
        config: &TelemetryBootstrapConfig,
    ) -> (
        bool,
        bool,
        MetricsExporter,
        bool,
        TraceExporter,
        bool,
        LogsExporter,
        String,
        OtlpProtocol,
    ) {
        (
            config.observability.enabled,
            config.observability.metrics.enabled,
            config.observability.metrics.exporter,
            config.observability.traces.enabled,
            config.observability.traces.exporter,
            config.observability.logs.enabled,
            config.observability.logs.exporter,
            config.observability.otlp.endpoint.clone(),
            config.observability.otlp.protocol,
        )
    }

    #[cfg(unix)]
    fn invalid_os_string() -> OsString {
        use std::os::unix::ffi::OsStringExt;

        OsString::from_vec(vec![0xff])
    }

    #[cfg(windows)]
    fn invalid_os_string() -> OsString {
        use std::os::windows::ffi::OsStringExt;

        OsString::from_wide(&[0xd800])
    }
}
