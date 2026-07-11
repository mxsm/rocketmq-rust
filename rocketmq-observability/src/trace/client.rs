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

use rocketmq_model::message::MessageQueue;

use crate::propagation::MessagePropertiesLike;

pub struct MessageSpanContext<'a> {
    properties: &'a dyn MessagePropertiesLike,
    body_size: Option<usize>,
}

impl<'a> MessageSpanContext<'a> {
    pub fn new(properties: &'a dyn MessagePropertiesLike, body_size: Option<usize>) -> Self {
        Self { properties, body_size }
    }
}

pub fn consumer_process_span(
    first_message: Option<MessageSpanContext<'_>>,
    message_count: usize,
    consumer_group: &str,
    message_queue: &MessageQueue,
    consume_mode: &'static str,
) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
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
        if let Some(message) = first_message {
            crate::propagation::set_span_parent_from_properties(&span, message.properties);
            super::record_message_properties(&span, message.properties, message.body_size);
        }
        span
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        if let Some(message) = first_message {
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
        let span = consumer_process_span(None, 0, "group-a", &queue, "concurrently");

        record_process_event(&span, "complete", "success", 0);
    }
}
