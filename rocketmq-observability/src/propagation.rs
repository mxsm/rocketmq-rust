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

pub const TRACEPARENT: &str = crate::semantic::trace::TRACEPARENT;
pub const TRACESTATE: &str = crate::semantic::trace::TRACESTATE;
#[cfg(feature = "otel-traces")]
const MAX_TRACEPARENT_BYTES: usize = 128;
#[cfg(feature = "otel-traces")]
const MAX_TRACESTATE_BYTES: usize = 512;

use std::collections::HashMap;

use cheetah_string::CheetahString;

#[cfg(feature = "otel-traces")]
pub(crate) fn install_trace_context_propagators() {
    // Message properties are durable and cross tenant boundaries. Propagate only the fixed W3C
    // trace identifiers; arbitrary OpenTelemetry baggage can contain credentials, user IDs, or
    // unbounded application data and therefore must never be copied into a RocketMQ message.
    opentelemetry::global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());
}

pub trait MessagePropertiesLike {
    fn get_property(&self, key: &str) -> Option<&str>;

    fn put_property(&mut self, key: &str, value: String);
}

impl MessagePropertiesLike for HashMap<CheetahString, CheetahString> {
    fn get_property(&self, key: &str) -> Option<&str> {
        self.get(key).map(|value| value.as_str())
    }

    fn put_property(&mut self, key: &str, value: String) {
        self.insert(
            CheetahString::from_string(key.to_owned()),
            CheetahString::from_string(value),
        );
    }
}

#[cfg(feature = "otel-traces")]
struct MessagePropertyInjector<'a, T> {
    inner: &'a mut T,
}

#[cfg(feature = "otel-traces")]
impl<'a, T> MessagePropertyInjector<'a, T> {
    pub fn new(inner: &'a mut T) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "otel-traces")]
impl<T> opentelemetry::propagation::Injector for MessagePropertyInjector<'_, T>
where
    T: MessagePropertiesLike,
{
    fn set(&mut self, key: &str, value: String) {
        let admitted = match key {
            TRACEPARENT => value.len() <= MAX_TRACEPARENT_BYTES,
            TRACESTATE => value.len() <= MAX_TRACESTATE_BYTES,
            _ => false,
        };
        if admitted {
            self.inner.put_property(key, value);
        }
    }
}

#[cfg(feature = "otel-traces")]
struct MessagePropertyExtractor<'a, T: ?Sized> {
    inner: &'a T,
}

#[cfg(feature = "otel-traces")]
impl<'a, T: ?Sized> MessagePropertyExtractor<'a, T> {
    pub fn new(inner: &'a T) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "otel-traces")]
impl<T: ?Sized> opentelemetry::propagation::Extractor for MessagePropertyExtractor<'_, T>
where
    T: MessagePropertiesLike,
{
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            TRACEPARENT => self
                .inner
                .get_property(key)
                .filter(|value| value.len() <= MAX_TRACEPARENT_BYTES),
            TRACESTATE => self
                .inner
                .get_property(key)
                .filter(|value| value.len() <= MAX_TRACESTATE_BYTES),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec![TRACEPARENT, TRACESTATE]
    }
}

#[cfg(feature = "otel-traces")]
/// Injects the current trace context when the explicit handle permits propagation.
pub fn inject_current_context_with_handle<T>(handle: &crate::TelemetryHandle, properties: &mut T)
where
    T: MessagePropertiesLike,
{
    let policy = handle.trace_policy();
    if !policy.enabled || !policy.propagate_context {
        return;
    }
    inject_current_context(properties);
}

#[cfg(feature = "otel-traces")]
/// Injects the current trace context using the installed process-level propagation codec.
///
/// Prefer [`inject_current_context_with_handle`] in business components. This lower-level helper
/// is retained for composition code that has already performed an explicit policy check.
fn inject_current_context<T>(properties: &mut T)
where
    T: MessagePropertiesLike,
{
    let context = opentelemetry::Context::current();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut MessagePropertyInjector::new(properties));
    });
}

#[cfg(feature = "otel-traces")]
/// Extracts trace context when the explicit handle permits propagation.
pub fn extract_context_with_handle<T>(handle: &crate::TelemetryHandle, properties: &T) -> opentelemetry::Context
where
    T: MessagePropertiesLike + ?Sized,
{
    let policy = handle.trace_policy();
    if !policy.enabled || !policy.propagate_context {
        return opentelemetry::Context::new();
    }
    extract_context(properties)
}

#[cfg(feature = "otel-traces")]
/// Extracts trace context using the installed process-level propagation codec.
///
/// Prefer [`extract_context_with_handle`] unless policy was checked by the caller.
fn extract_context<T>(properties: &T) -> opentelemetry::Context
where
    T: MessagePropertiesLike + ?Sized,
{
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&MessagePropertyExtractor::new(properties))
    })
}

#[cfg(feature = "otel-traces")]
/// Sets a span's parent when the explicit handle permits message propagation.
pub fn set_span_parent_from_properties_with_handle<T>(
    handle: &crate::TelemetryHandle,
    span: &tracing::Span,
    properties: &T,
) -> Result<(), tracing_opentelemetry::SetParentError>
where
    T: MessagePropertiesLike + ?Sized,
{
    let policy = handle.trace_policy();
    if !policy.enabled || !policy.propagate_context {
        return Ok(());
    }

    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let parent_context = extract_context(properties);
    span.set_parent(parent_context)
}

#[cfg(feature = "otel-traces")]
/// Records a failed span-parent assignment without exposing propagated message properties.
///
/// `operation` must be a fixed, low-cardinality component label.
pub fn record_span_parent_assignment_error(
    handle: &crate::TelemetryHandle,
    operation: &'static str,
    error: tracing_opentelemetry::SetParentError,
) {
    if !handle.trace_policy().enabled {
        return;
    }

    use tracing_opentelemetry::SetParentError;

    match error {
        SetParentError::SpanDisabled => {
            tracing::debug!(
                target: "rocketmq_observability",
                operation,
                failure = "span_disabled",
                "skipped remote span-parent assignment because the span is disabled"
            );
        }
        SetParentError::LayerNotFound => {
            tracing::warn!(
                target: "rocketmq_observability",
                operation,
                failure = "opentelemetry_layer_not_found",
                "failed to assign the remote span parent because the OpenTelemetry layer is unavailable"
            );
        }
        SetParentError::AlreadyStarted => {
            tracing::warn!(
                target: "rocketmq_observability",
                operation,
                failure = "span_already_started",
                "failed to assign the remote span parent because the span has already started"
            );
        }
    }
}

#[cfg(feature = "otel-traces")]
fn add_current_span_event(name: &'static str, attributes: Vec<opentelemetry::KeyValue>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    tracing::Span::current().add_event(name, attributes);
}

#[cfg(feature = "otel-traces")]
pub fn add_current_span_event_with_status(handle: &crate::TelemetryHandle, name: &'static str, status: &'static str) {
    if !handle.trace_policy().enabled {
        return;
    }

    add_current_span_event(
        name,
        vec![opentelemetry::KeyValue::new(
            "rocketmq.messaging.status",
            status.to_owned(),
        )],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_map_properties_like_reads_and_writes_standard_headers() {
        let mut properties = HashMap::new();

        properties.put_property(
            TRACEPARENT,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
        );

        assert_eq!(
            properties.get_property(TRACEPARENT),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }

    #[cfg(feature = "otel-traces")]
    #[test]
    fn context_propagation_policy_is_isolated_between_handles() {
        use opentelemetry::trace::TraceContextExt;

        install_trace_context_propagators();
        let mut properties = HashMap::new();
        properties.put_property(
            TRACEPARENT,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
        );

        let mut enabled_config = crate::ObservabilityConfig {
            enabled: true,
            ..crate::ObservabilityConfig::default()
        };
        enabled_config.traces.enabled = true;
        enabled_config.traces.propagate_context = true;
        let mut disabled_config = enabled_config.clone();
        disabled_config.traces.propagate_context = false;

        #[cfg(feature = "otel-metrics")]
        let enabled_handle = crate::TelemetryHandle::active(&enabled_config, None);
        #[cfg(not(feature = "otel-metrics"))]
        let enabled_handle = crate::TelemetryHandle::active(&enabled_config);
        #[cfg(feature = "otel-metrics")]
        let disabled_handle = crate::TelemetryHandle::active(&disabled_config, None);
        #[cfg(not(feature = "otel-metrics"))]
        let disabled_handle = crate::TelemetryHandle::active(&disabled_config);

        let enabled_context = extract_context_with_handle(&enabled_handle, &properties);
        let disabled_context = extract_context_with_handle(&disabled_handle, &properties);

        assert!(enabled_context.span().span_context().is_valid());
        assert!(!disabled_context.span().span_context().is_valid());
        assert!(enabled_handle.trace_policy().propagate_context);
        assert!(!disabled_handle.trace_policy().propagate_context);
    }

    #[cfg(feature = "otel-traces")]
    #[test]
    fn arbitrary_baggage_is_never_persisted_in_message_properties() {
        use opentelemetry::baggage::BaggageExt;

        install_trace_context_propagators();
        let context = opentelemetry::Context::current_with_baggage([opentelemetry::KeyValue::new(
            "authorization",
            "secret-token",
        )]);
        let _attached = context.attach();
        let mut properties = HashMap::new();

        inject_current_context(&mut properties);

        assert_eq!(properties.get_property("baggage"), None);
        assert!(!properties.values().any(|value| value.contains("secret-token")));
    }
}
