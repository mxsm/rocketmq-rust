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
pub const BAGGAGE: &str = crate::semantic::trace::BAGGAGE;

use std::collections::HashMap;
#[cfg(feature = "otel-traces")]
use std::sync::atomic::AtomicBool;
#[cfg(feature = "otel-traces")]
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;

#[cfg(feature = "otel-traces")]
static CONTEXT_PROPAGATION_ENABLED: AtomicBool = AtomicBool::new(true);

#[cfg(all(test, feature = "otel-traces"))]
static CONTEXT_PROPAGATION_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(all(test, feature = "otel-traces"))]
pub(crate) fn context_propagation_test_lock() -> std::sync::MutexGuard<'static, ()> {
    CONTEXT_PROPAGATION_TEST_LOCK.lock().unwrap()
}

#[cfg(feature = "otel-traces")]
pub fn set_context_propagation_enabled(enabled: bool) {
    CONTEXT_PROPAGATION_ENABLED.store(enabled, Ordering::Relaxed);
}

#[cfg(feature = "otel-traces")]
pub fn is_context_propagation_enabled() -> bool {
    CONTEXT_PROPAGATION_ENABLED.load(Ordering::Relaxed)
}

#[cfg(feature = "otel-traces")]
pub fn install_trace_context_propagators() {
    set_context_propagation_enabled(true);
    opentelemetry::global::set_text_map_propagator(opentelemetry::propagation::TextMapCompositePropagator::new(vec![
        Box::new(opentelemetry_sdk::propagation::TraceContextPropagator::new()),
        Box::new(opentelemetry_sdk::propagation::BaggagePropagator::new()),
    ]));
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
pub struct MessagePropertyInjector<'a, T> {
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
        self.inner.put_property(key, value);
    }
}

#[cfg(feature = "otel-traces")]
pub struct MessagePropertyExtractor<'a, T: ?Sized> {
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
        self.inner.get_property(key)
    }

    fn keys(&self) -> Vec<&str> {
        vec![TRACEPARENT, TRACESTATE, BAGGAGE]
    }
}

#[cfg(feature = "otel-traces")]
pub fn inject_current_context<T>(properties: &mut T)
where
    T: MessagePropertiesLike,
{
    if !is_context_propagation_enabled() {
        return;
    }

    let context = opentelemetry::Context::current();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut MessagePropertyInjector::new(properties));
    });
}

#[cfg(feature = "otel-traces")]
#[cfg(feature = "otel-traces")]
pub fn extract_context<T>(properties: &T) -> opentelemetry::Context
where
    T: MessagePropertiesLike + ?Sized,
{
    if !is_context_propagation_enabled() {
        return opentelemetry::Context::new();
    }

    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&MessagePropertyExtractor::new(properties))
    })
}

#[cfg(feature = "otel-traces")]
pub fn set_span_parent_from_properties<T>(span: &tracing::Span, properties: &T)
where
    T: MessagePropertiesLike + ?Sized,
{
    if !is_context_propagation_enabled() {
        return;
    }

    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let parent_context = extract_context(properties);
    let _ = span.set_parent(parent_context);
}

#[cfg(feature = "otel-traces")]
#[cfg(feature = "otel-traces")]
pub fn set_current_span_parent_from_properties<T>(properties: &T)
where
    T: MessagePropertiesLike,
{
    set_span_parent_from_properties(&tracing::Span::current(), properties);
}

#[cfg(feature = "otel-traces")]
#[cfg(feature = "otel-traces")]
pub fn add_current_span_event(name: &'static str, attributes: Vec<opentelemetry::KeyValue>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    tracing::Span::current().add_event(name, attributes);
}

#[cfg(feature = "otel-traces")]
pub fn add_current_span_event_with_status(name: &'static str, status: &str) {
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
    fn context_propagation_can_be_disabled_for_message_properties() {
        let _guard = context_propagation_test_lock();

        set_context_propagation_enabled(false);
        assert!(!is_context_propagation_enabled());

        let mut properties = HashMap::new();
        inject_current_context(&mut properties);

        assert_eq!(properties.get_property(TRACEPARENT), None);

        set_context_propagation_enabled(true);
        assert!(is_context_propagation_enabled());
    }
}
