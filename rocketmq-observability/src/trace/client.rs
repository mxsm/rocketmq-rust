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

pub use super::span_names::CONSUMER_PROCESS;
pub use super::span_names::PRODUCER_SEND;

#[cfg(feature = "otel-traces")]
use std::collections::HashSet;

use rocketmq_model::message::MessageQueue;

use crate::propagation::MessagePropertiesLike;
use crate::TelemetryHandle;

#[derive(Clone, Copy)]
pub struct MessageSpanContext<'a> {
    properties: &'a dyn MessagePropertiesLike,
    body_size: Option<usize>,
}

impl<'a> MessageSpanContext<'a> {
    pub fn new(properties: &'a dyn MessagePropertiesLike, body_size: Option<usize>) -> Self {
        Self { properties, body_size }
    }
}

pub fn producer_send_span(telemetry: &TelemetryHandle) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        if !telemetry.trace_policy().enabled {
            return tracing::Span::none();
        }
        tracing::info_span!(
            PRODUCER_SEND,
            messaging.system = "rocketmq",
            messaging.message.id = tracing::field::Empty,
            messaging.message.body.size = tracing::field::Empty,
            messaging.rocketmq.message.keys = tracing::field::Empty,
        )
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = telemetry;
        tracing::Span::none()
    }
}

#[cfg(feature = "otel-traces")]
struct ConsumerTraceContextPlan<'a> {
    first_message: Option<MessageSpanContext<'a>>,
    parent_message: Option<MessageSpanContext<'a>>,
    links: Vec<opentelemetry::trace::SpanContext>,
}

#[cfg(feature = "otel-traces")]
fn consumer_trace_context_plan<'a>(
    telemetry: &TelemetryHandle,
    messages: impl IntoIterator<Item = MessageSpanContext<'a>>,
) -> ConsumerTraceContextPlan<'a> {
    use opentelemetry::trace::TraceContextExt;

    let mut first_message = None;
    let mut parent_message = None;
    let mut seen_remote_parents = HashSet::new();
    let mut links = Vec::new();

    for message in messages {
        if first_message.is_none() {
            first_message = Some(message);
        }

        let remote_context = crate::propagation::extract_context_with_handle(telemetry, message.properties);
        let span_context = remote_context.span().span_context().clone();
        if !span_context.is_valid() || !span_context.is_remote() {
            continue;
        }

        if parent_message.is_none() {
            parent_message = Some(message);
        }
        if seen_remote_parents.insert((span_context.trace_id(), span_context.span_id())) {
            links.push(span_context);
        }
    }

    ConsumerTraceContextPlan {
        first_message,
        parent_message,
        links,
    }
}

pub fn consumer_process_span<'a>(
    telemetry: &TelemetryHandle,
    messages: impl IntoIterator<Item = MessageSpanContext<'a>>,
    message_count: usize,
    consumer_group: &str,
    message_queue: &MessageQueue,
    consume_mode: &'static str,
) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        if !telemetry.trace_policy().enabled {
            return tracing::Span::none();
        }
        let context_plan = consumer_trace_context_plan(telemetry, messages);
        let span = tracing::info_span!(
            CONSUMER_PROCESS,
            messaging_system = "rocketmq",
            messaging_operation_name = "process",
            messaging_destination_name = %message_queue.topic(),
            rocketmq_consumer_group = consumer_group,
            rocketmq_broker_name = %message_queue.broker_name(),
            rocketmq_queue_id = message_queue.queue_id(),
            rocketmq_consume_mode = consume_mode,
            messaging_batch_message_count = message_count as i64,
            messaging.message.id = tracing::field::Empty,
            messaging.message.body.size = tracing::field::Empty,
            messaging.rocketmq.message.keys = tracing::field::Empty,
        );
        if let Some(message) = context_plan.parent_message {
            if let Err(error) =
                crate::propagation::set_span_parent_from_properties_with_handle(telemetry, &span, message.properties)
            {
                crate::propagation::record_span_parent_assignment_error(telemetry, "client.consumer.process", error);
            }
        }
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt;

            for link in context_plan.links {
                span.add_link(link);
            }
        }
        if let Some(message) = context_plan.first_message {
            super::record_message_properties_with_handle(telemetry, &span, message.properties, message.body_size);
        }
        span
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = telemetry;
        for message in messages {
            let _ = (message.properties, message.body_size);
        }
        let _ = (message_count, consumer_group, message_queue, consume_mode);
        tracing::Span::none()
    }
}

pub fn record_process_event(span: &tracing::Span, event: &'static str, status: &str, message_count: usize) {
    #[cfg(feature = "otel-traces")]
    {
        let _entered = span.enter();
        tracing::info!(
            target: "rocketmq_observability",
            messaging_system = "rocketmq",
            messaging_operation_name = "process",
            rocketmq_consumer_event = event,
            rocketmq_consumer_status = status,
            messaging_batch_message_count = message_count as i64,
        );
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = (span, event, status, message_count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_process_span_accepts_empty_batch_without_traces() {
        let queue = MessageQueue::from_parts("topic-a", "broker-a", 1);
        let span = consumer_process_span(
            &TelemetryHandle::noop(),
            std::iter::empty(),
            0,
            "group-a",
            &queue,
            "concurrently",
        );

        record_process_event(&span, "complete", "success", 0);
    }

    #[cfg(feature = "otel-traces")]
    #[test]
    fn producer_and_consumer_spans_honor_isolated_handle_policies() {
        let mut enabled_config = crate::ObservabilityConfig {
            enabled: true,
            ..crate::ObservabilityConfig::default()
        };
        enabled_config.traces.enabled = true;
        let mut disabled_config = enabled_config.clone();
        disabled_config.traces.enabled = false;

        #[cfg(feature = "otel-metrics")]
        let enabled = TelemetryHandle::active(&enabled_config, None);
        #[cfg(not(feature = "otel-metrics"))]
        let enabled = TelemetryHandle::active(&enabled_config);
        #[cfg(feature = "otel-metrics")]
        let disabled = TelemetryHandle::active(&disabled_config, None);
        #[cfg(not(feature = "otel-metrics"))]
        let disabled = TelemetryHandle::active(&disabled_config);
        let queue = MessageQueue::from_parts("topic-a", "broker-a", 1);

        tracing::subscriber::with_default(tracing_subscriber::Registry::default(), || {
            assert!(producer_send_span(&enabled).id().is_some());
            assert!(
                consumer_process_span(&enabled, std::iter::empty(), 1, "group-a", &queue, "concurrently")
                    .id()
                    .is_some()
            );
            assert!(producer_send_span(&disabled).id().is_none());
            assert!(
                consumer_process_span(&disabled, std::iter::empty(), 1, "group-b", &queue, "concurrently")
                    .id()
                    .is_none()
            );
        });
    }

    #[cfg(feature = "otel-traces")]
    #[test]
    fn consumer_batch_links_each_unique_valid_remote_context() {
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::sync::Mutex;

        use cheetah_string::CheetahString;
        use opentelemetry::trace::SpanId;
        use opentelemetry_sdk::error::OTelSdkResult;
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use opentelemetry_sdk::trace::SpanData;
        use opentelemetry_sdk::trace::SpanExporter;
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Debug, Default)]
        struct CollectingSpanExporter {
            spans: Arc<Mutex<Vec<SpanData>>>,
        }

        impl SpanExporter for CollectingSpanExporter {
            fn export(&self, batch: Vec<SpanData>) -> impl std::future::Future<Output = OTelSdkResult> + Send {
                let spans = Arc::clone(&self.spans);
                async move {
                    spans.lock().expect("span export lock poisoned").extend(batch);
                    Ok(())
                }
            }
        }

        fn properties(traceparent: &str) -> HashMap<CheetahString, CheetahString> {
            let mut properties = HashMap::new();
            properties.insert(
                CheetahString::from_static_str(crate::TRACEPARENT),
                CheetahString::from_string(traceparent.to_owned()),
            );
            properties
        }

        crate::propagation::install_trace_context_propagators();
        let invalid = properties("not-a-traceparent");
        let first_parent = properties("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let duplicate_parent = properties("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let second_parent = properties("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        let messages = [
            MessageSpanContext::new(&invalid, None),
            MessageSpanContext::new(&first_parent, None),
            MessageSpanContext::new(&duplicate_parent, None),
            MessageSpanContext::new(&second_parent, None),
        ];

        let mut config = crate::ObservabilityConfig {
            enabled: true,
            ..crate::ObservabilityConfig::default()
        };
        config.traces.enabled = true;
        config.traces.propagate_context = true;
        #[cfg(feature = "otel-metrics")]
        let telemetry = TelemetryHandle::active(&config, None);
        #[cfg(not(feature = "otel-metrics"))]
        let telemetry = TelemetryHandle::active(&config);

        let exporter = CollectingSpanExporter::default();
        let tracer_provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber =
            tracing_subscriber::Registry::default().with(crate::trace::build_tracing_layer(&config, &tracer_provider));
        let queue = MessageQueue::from_parts("topic-a", "broker-a", 1);

        tracing::subscriber::with_default(subscriber, || {
            let span = consumer_process_span(&telemetry, messages, 4, "group-a", &queue, "concurrently");
            let _entered = span.enter();
        });
        tracer_provider.force_flush().expect("trace flush failed");

        let spans = exporter.spans.lock().expect("span export lock poisoned");
        let process_span = spans
            .iter()
            .find(|span| span.name == CONSUMER_PROCESS)
            .expect("consumer process span was not exported");
        let first_parent_id = SpanId::from_hex("00f067aa0ba902b7").expect("valid first span id");
        let second_parent_id = SpanId::from_hex("bbbbbbbbbbbbbbbb").expect("valid second span id");
        let linked_parent_ids = process_span
            .links
            .iter()
            .map(|link| link.span_context.span_id())
            .collect::<Vec<_>>();

        assert_eq!(process_span.parent_span_id, first_parent_id);
        assert!(process_span.parent_span_is_remote);
        assert_eq!(linked_parent_ids, vec![first_parent_id, second_parent_id]);
    }
}
