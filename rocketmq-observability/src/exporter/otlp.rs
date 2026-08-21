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

#[cfg(feature = "otlp-metrics")]
pub(crate) fn init_otlp_meter_provider(
    config: &crate::config::ObservabilityConfig,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, crate::error::ObservabilityError> {
    use std::time::Duration;

    use opentelemetry_otlp::MetricExporter;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_otlp::WithTonicConfig;
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::Temporality;

    let timeout_millis = if config.metrics.export_timeout_millis == 0 {
        config.otlp.timeout_millis
    } else {
        config.metrics.export_timeout_millis
    };

    let mut exporter_builder = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp.endpoint.clone())
        .with_timeout(Duration::from_millis(timeout_millis))
        .with_temporality(Temporality::Cumulative);

    if !config.otlp.headers.is_empty() {
        exporter_builder = exporter_builder.with_metadata(build_tonic_metadata(&config.otlp.headers)?);
    }

    let exporter = exporter_builder
        .build()
        .map_err(|_| crate::error::ObservabilityError::metrics_init("OTLP metrics exporter could not be built"))?;
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_millis(config.metrics.export_interval_millis))
        .build();

    Ok(SdkMeterProvider::builder()
        .with_resource(crate::resource::build_resource(config))
        .with_reader(reader)
        .build())
}

#[cfg(feature = "otlp-traces")]
pub fn init_otlp_tracer_provider(
    config: &crate::config::ObservabilityConfig,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, crate::error::ObservabilityError> {
    use std::time::Duration;

    use opentelemetry_otlp::SpanExporter;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_otlp::WithTonicConfig;
    use opentelemetry_sdk::trace::Sampler;
    use opentelemetry_sdk::trace::SdkTracerProvider;

    let mut exporter_builder = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp.endpoint.clone())
        .with_timeout(Duration::from_millis(config.otlp.timeout_millis));

    if !config.otlp.headers.is_empty() {
        exporter_builder = exporter_builder.with_metadata(build_tonic_metadata(&config.otlp.headers)?);
    }

    let exporter = exporter_builder
        .build()
        .map_err(|_| crate::error::ObservabilityError::traces_init("OTLP trace exporter could not be built"))?;
    let processor = crate::exporter::outage::OutageBoundedBatchSpanProcessor::new(exporter);

    Ok(SdkTracerProvider::builder()
        .with_resource(crate::resource::build_resource(config))
        .with_sampler(Sampler::TraceIdRatioBased(config.traces.sample_ratio.clamp(0.0, 1.0)))
        .with_span_processor(processor)
        .build())
}

#[cfg(feature = "otlp-logs")]
pub fn init_otlp_logger_provider(
    config: &crate::config::ObservabilityConfig,
) -> Result<opentelemetry_sdk::logs::SdkLoggerProvider, crate::error::ObservabilityError> {
    use std::time::Duration;

    use opentelemetry_otlp::LogExporter;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_otlp::WithTonicConfig;
    use opentelemetry_sdk::logs::SdkLoggerProvider;

    let mut exporter_builder = LogExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp.endpoint.clone())
        .with_timeout(Duration::from_millis(config.otlp.timeout_millis));

    if !config.otlp.headers.is_empty() {
        exporter_builder = exporter_builder.with_metadata(build_tonic_metadata(&config.otlp.headers)?);
    }

    let exporter = exporter_builder
        .build()
        .map_err(|_| crate::error::ObservabilityError::logs_init("OTLP log exporter could not be built"))?;
    let processor = crate::exporter::outage::OutageBoundedBatchLogProcessor::new(exporter);

    Ok(SdkLoggerProvider::builder()
        .with_resource(crate::resource::build_resource(config))
        .with_log_processor(processor)
        .build())
}

#[cfg(feature = "otlp-grpc")]
fn build_tonic_metadata(
    headers: &std::collections::HashMap<String, String>,
) -> Result<tonic::metadata::MetadataMap, crate::error::ObservabilityError> {
    let mut metadata = tonic::metadata::MetadataMap::with_capacity(headers.len());
    for (key, value) in headers {
        let key = tonic::metadata::MetadataKey::from_bytes(key.as_bytes())
            .map_err(|_| crate::error::ObservabilityError::invalid_config("invalid OTLP gRPC metadata key"))?;
        let value = tonic::metadata::MetadataValue::try_from(value.as_str())
            .map_err(|_| crate::error::ObservabilityError::invalid_config("invalid OTLP gRPC metadata value"))?;
        metadata.insert(key, value);
    }
    Ok(metadata)
}

#[cfg(all(test, feature = "otlp-grpc"))]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn builds_tonic_metadata_from_headers() {
        let headers = HashMap::from([("authorization".to_string(), "Bearer token".to_string())]);

        let metadata = build_tonic_metadata(&headers).expect("valid metadata should build");

        assert_eq!(
            metadata.get("authorization").and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }

    #[test]
    fn rejects_invalid_tonic_metadata_key() {
        const METADATA_KEY_CANARY: &str = "Invalid Header OTLP_METADATA_KEY_CANARY";
        let headers = HashMap::from([(METADATA_KEY_CANARY.to_string(), "value".to_string())]);

        let error = build_tonic_metadata(&headers).expect_err("invalid metadata key should fail");

        assert!(matches!(error, crate::error::ObservabilityError::InvalidConfig(_)));
        for output in [error.to_string(), format!("{error:?}")] {
            assert!(!output.contains(METADATA_KEY_CANARY));
            assert!(output.contains("invalid OTLP gRPC metadata key"));
        }
    }

    #[cfg(feature = "otlp-traces")]
    #[test]
    fn tracer_exporter_build_error_redacts_endpoint_and_upstream_diagnostic() {
        const ENDPOINT_CANARY: &str = "OTLP_ENDPOINT_CANARY";
        let mut config = crate::config::ObservabilityConfig::default();
        config.otlp.endpoint = format!("http://[::{ENDPOINT_CANARY}");

        let error = init_otlp_tracer_provider(&config).expect_err("invalid OTLP endpoint must fail");

        for output in [error.to_string(), format!("{error:?}")] {
            assert!(!output.contains(ENDPOINT_CANARY));
            assert!(output.contains("OTLP trace exporter could not be built"));
        }
    }
}
