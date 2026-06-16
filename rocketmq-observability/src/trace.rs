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
pub mod hooks;
pub mod namesrv;
pub mod remoting;
pub mod span_names;
pub mod store;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;

use crate::config::TracesConfig;
use crate::propagation::MessagePropertiesLike;

static RECORD_MESSAGE_ID: AtomicBool = AtomicBool::new(false);
static RECORD_MESSAGE_KEYS: AtomicBool = AtomicBool::new(false);
static RECORD_BODY_SIZE: AtomicBool = AtomicBool::new(true);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageSpanRecordingConfig {
    pub record_message_id: bool,
    pub record_message_keys: bool,
    pub record_body_size: bool,
}

pub fn configure_message_span_recording(config: &TracesConfig) {
    RECORD_MESSAGE_ID.store(config.record_message_id, Ordering::Relaxed);
    RECORD_MESSAGE_KEYS.store(config.record_message_keys, Ordering::Relaxed);
    RECORD_BODY_SIZE.store(config.record_body_size, Ordering::Relaxed);
}

pub fn message_span_recording_config() -> MessageSpanRecordingConfig {
    MessageSpanRecordingConfig {
        record_message_id: RECORD_MESSAGE_ID.load(Ordering::Relaxed),
        record_message_keys: RECORD_MESSAGE_KEYS.load(Ordering::Relaxed),
        record_body_size: RECORD_BODY_SIZE.load(Ordering::Relaxed),
    }
}

pub fn record_current_message_attributes<T>(message: &T)
where
    T: MessageTrait,
{
    record_message_attributes(&tracing::Span::current(), message);
}

pub fn record_message_attributes<T>(span: &tracing::Span, message: &T)
where
    T: MessageTrait,
{
    let body_size = message.get_body().map(|body| body.len());
    record_message_properties(span, message.get_properties(), body_size);
}

pub fn record_current_message_properties<T>(properties: &T, body_size: Option<usize>)
where
    T: MessagePropertiesLike,
{
    record_message_properties(&tracing::Span::current(), properties, body_size);
}

pub fn record_message_properties<T>(span: &tracing::Span, properties: &T, body_size: Option<usize>)
where
    T: MessagePropertiesLike,
{
    let config = message_span_recording_config();

    if config.record_message_id {
        if let Some(message_id) = properties.get_property(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) {
            span.record(crate::semantic::trace::MESSAGING_MESSAGE_ID, message_id);
        }
    }

    if config.record_message_keys {
        if let Some(message_keys) = properties.get_property(MessageConst::PROPERTY_KEYS) {
            span.record(crate::semantic::trace::MESSAGING_ROCKETMQ_MESSAGE_KEYS, message_keys);
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

#[cfg(test)]
mod tests {
    use super::*;

    static MESSAGE_SPAN_RECORDING_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn message_span_recording_flags_follow_trace_config() {
        let _guard = MESSAGE_SPAN_RECORDING_TEST_LOCK.lock().unwrap();
        let traces = TracesConfig {
            record_message_id: true,
            record_message_keys: true,
            record_body_size: false,
            ..TracesConfig::default()
        };

        configure_message_span_recording(&traces);

        assert_eq!(
            message_span_recording_config(),
            MessageSpanRecordingConfig {
                record_message_id: true,
                record_message_keys: true,
                record_body_size: false,
            }
        );

        configure_message_span_recording(&TracesConfig::default());
    }
}
