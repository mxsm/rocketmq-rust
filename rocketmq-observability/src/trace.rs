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

pub mod broker;
pub mod client;
pub mod controller;
pub mod hooks;
pub mod mcp;
pub mod namesrv;
pub mod remoting;
pub mod span_names;
pub mod store;

use crate::propagation::MessagePropertiesLike;
use crate::TelemetryHandle;
use crate::TracePolicy;
use sha2::Digest;
use sha2::Sha256;

const PROPERTY_MESSAGE_ID: &str = "UNIQ_KEY";
const PROPERTY_MESSAGE_KEYS: &str = "KEYS";

/// Effective message fields admitted by one immutable trace policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageSpanRecordingConfig {
    /// Whether an opaque message identifier may be recorded.
    pub record_message_id: bool,
    /// Whether a one-way hash of message keys may be recorded.
    pub record_message_keys: bool,
    /// Whether message body size may be recorded.
    pub record_body_size: bool,
}

impl MessageSpanRecordingConfig {
    fn from_policy(policy: TracePolicy) -> Self {
        if !policy.enabled {
            return Self {
                record_message_id: false,
                record_message_keys: false,
                record_body_size: false,
            };
        }
        Self {
            record_message_id: policy.record_message_id,
            record_message_keys: policy.record_message_keys,
            record_body_size: policy.record_body_size,
        }
    }
}

/// Returns the effective message recording policy for one explicit telemetry handle.
#[must_use]
pub fn message_span_recording_config(handle: &TelemetryHandle) -> MessageSpanRecordingConfig {
    MessageSpanRecordingConfig::from_policy(handle.trace_policy())
}

/// Records configured message fields on the current span using an explicit telemetry handle.
pub fn record_current_message_properties_with_handle<T>(
    handle: &TelemetryHandle,
    properties: &T,
    body_size: Option<usize>,
) where
    T: MessagePropertiesLike + ?Sized,
{
    record_message_properties_with_handle(handle, &tracing::Span::current(), properties, body_size);
}

/// Records configured message fields on a span using an explicit telemetry handle.
pub fn record_message_properties_with_handle<T>(
    handle: &TelemetryHandle,
    span: &tracing::Span,
    properties: &T,
    body_size: Option<usize>,
) where
    T: MessagePropertiesLike + ?Sized,
{
    record_message_properties_with_policy(handle.trace_policy(), span, properties, body_size);
}

/// Records configured message fields on a span using an immutable trace policy.
pub(crate) fn record_message_properties_with_policy<T>(
    policy: TracePolicy,
    span: &tracing::Span,
    properties: &T,
    body_size: Option<usize>,
) where
    T: MessagePropertiesLike + ?Sized,
{
    record_message_properties_with_config(
        MessageSpanRecordingConfig::from_policy(policy),
        span,
        properties,
        body_size,
    );
}

fn record_message_properties_with_config<T>(
    config: MessageSpanRecordingConfig,
    span: &tracing::Span,
    properties: &T,
    body_size: Option<usize>,
) where
    T: MessagePropertiesLike + ?Sized,
{
    if config.record_message_id {
        if let Some(message_id) = properties.get_property(PROPERTY_MESSAGE_ID) {
            span.record(crate::semantic::trace::MESSAGING_MESSAGE_ID, message_id);
        }
    }

    if config.record_message_keys {
        if let Some(message_keys) = properties.get_property(PROPERTY_MESSAGE_KEYS) {
            let message_keys_hash = correlation_hash(message_keys);
            span.record(
                crate::semantic::trace::MESSAGING_ROCKETMQ_MESSAGE_KEYS,
                message_keys_hash.as_str(),
            );
        }
    }

    if config.record_body_size {
        if let Some(body_size) = body_size {
            span.record(
                crate::semantic::trace::MESSAGING_MESSAGE_BODY_SIZE,
                i64::try_from(body_size).unwrap_or(i64::MAX),
            );
        }
    }
}

#[cfg(feature = "otel-traces")]
pub type OpenTelemetryTracingLayer =
    tracing_opentelemetry::OpenTelemetryLayer<tracing_subscriber::Registry, opentelemetry_sdk::trace::SdkTracer>;

#[cfg(feature = "otel-traces")]
pub fn build_tracing_layer(
    config: &crate::config::ObservabilityConfig,
    tracer_provider: &opentelemetry_sdk::trace::SdkTracerProvider,
) -> OpenTelemetryTracingLayer {
    use opentelemetry::trace::TracerProvider as _;

    let tracer = tracer_provider.tracer(config.service_name.clone());
    tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_error_events_to_status(true)
        .with_error_events_to_exceptions(true)
        .with_error_records_to_exceptions(true)
        .with_context_activation(true)
}

#[cfg(feature = "otel-traces")]
pub fn try_init_tracing_subscriber(
    config: &crate::config::ObservabilityConfig,
    tracer_provider: &opentelemetry_sdk::trace::SdkTracerProvider,
) -> bool {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true);
    let otel_layer = build_tracing_layer(config, tracer_provider);

    match tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer)
        .try_init()
    {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(
                target: "rocketmq_observability",
                %error,
                "tracing subscriber already initialized; OpenTelemetry tracing layer was not installed"
            );
            false
        }
    }
}

fn correlation_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity("sha256:".len() + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_key_correlation_hash_is_stable_and_non_reversible_in_spans() {
        let first = correlation_hash("customer-order-42");
        let second = correlation_hash("customer-order-42");
        let different = correlation_hash("customer-order-43");

        assert_eq!(first, second);
        assert_ne!(first, different);
        assert_eq!(first.len(), "sha256:".len() + 64);
        assert!(!first.contains("customer-order-42"));
    }

    #[test]
    fn message_span_recording_policy_is_isolated_between_handles() {
        let mut first_config = crate::ObservabilityConfig {
            enabled: true,
            ..crate::ObservabilityConfig::default()
        };
        first_config.traces.enabled = true;
        first_config.traces.record_message_id = true;
        first_config.traces.record_message_keys = true;
        first_config.traces.record_body_size = false;
        let mut second_config = first_config.clone();
        second_config.traces.record_message_id = false;
        second_config.traces.record_message_keys = false;
        second_config.traces.record_body_size = true;

        #[cfg(feature = "otel-metrics")]
        let first = TelemetryHandle::active(&first_config, None);
        #[cfg(not(feature = "otel-metrics"))]
        let first = TelemetryHandle::active(&first_config);
        #[cfg(feature = "otel-metrics")]
        let second = TelemetryHandle::active(&second_config, None);
        #[cfg(not(feature = "otel-metrics"))]
        let second = TelemetryHandle::active(&second_config);

        assert_eq!(
            message_span_recording_config(&first),
            MessageSpanRecordingConfig {
                record_message_id: true,
                record_message_keys: true,
                record_body_size: false,
            }
        );
        assert_eq!(
            message_span_recording_config(&second),
            MessageSpanRecordingConfig {
                record_message_id: false,
                record_message_keys: false,
                record_body_size: true,
            }
        );

        first.begin_closing();
        assert_eq!(
            message_span_recording_config(&first),
            MessageSpanRecordingConfig {
                record_message_id: false,
                record_message_keys: false,
                record_body_size: false,
            }
        );
        assert_eq!(
            message_span_recording_config(&second),
            MessageSpanRecordingConfig {
                record_message_id: false,
                record_message_keys: false,
                record_body_size: true,
            }
        );
    }

    #[test]
    fn disabled_trace_policy_records_no_message_fields() {
        let policy = TracePolicy {
            enabled: false,
            propagate_context: true,
            record_message_id: true,
            record_message_keys: true,
            record_body_size: true,
        };

        assert_eq!(
            MessageSpanRecordingConfig::from_policy(policy),
            MessageSpanRecordingConfig {
                record_message_id: false,
                record_message_keys: false,
                record_body_size: false,
            }
        );
    }
}
