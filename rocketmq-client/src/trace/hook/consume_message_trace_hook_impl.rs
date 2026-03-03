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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;

use crate::consumer::listener::consume_return_type::ConsumeReturnType;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::trace::trace_bean::TraceBean;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_dispatcher::ArcTraceDispatcher;
use crate::trace::trace_type::TraceType;

/// Collects trace data before and after message consumption.
///
/// Captures message metadata (topic, msgId, tags, keys), consumption timing, result status,
/// and consumer group information. Trace data is dispatched asynchronously to avoid blocking
/// the consumption flow. Thread-safe and suitable for use across multiple consumer threads
/// via `Arc<ConsumeMessageTraceHookImpl>`.
pub struct ConsumeMessageTraceHookImpl {
    trace_dispatcher: ArcTraceDispatcher,
}

impl ConsumeMessageTraceHookImpl {
    /// Creates a new trace hook using the provided dispatcher for batching and sending trace data.
    #[inline]
    pub fn new(trace_dispatcher: ArcTraceDispatcher) -> Self {
        Self { trace_dispatcher }
    }

    /// Extracts trace metadata from messages, respecting per-message trace switches.
    ///
    /// Messages with `PROPERTY_TRACE_SWITCH = "false"` are skipped. Returns `Some((beans,
    /// region_id))` if at least one message has tracing enabled, or `None` if all messages have
    /// tracing disabled.
    fn build_trace_beans(&self, messages: &[ArcMut<MessageExt>]) -> Option<(Vec<TraceBean>, CheetahString)> {
        let mut beans = Vec::with_capacity(messages.len());
        let mut region_id = CheetahString::new();

        for msg_arc in messages {
            let msg = msg_arc.as_ref();

            // Check if trace is enabled for this message
            if let Some(trace_switch) =
                msg.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH))
            {
                if trace_switch.as_str() == "false" {
                    continue; // Skip messages with trace disabled
                }
            }

            // Extract region ID (all messages assumed to share the same region)
            if region_id.is_empty() {
                if let Some(region) =
                    msg.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION))
                {
                    region_id = region.clone();
                }
            }

            // Build trace bean for this message
            let trace_bean = TraceBean {
                topic: Self::without_namespace(msg.topic()),
                msg_id: msg.msg_id().clone(),
                offset_msg_id: CheetahString::new(),
                tags: msg.get_tags().unwrap_or_default(),
                keys: msg
                    .keys()
                    .map(|keys_vec| CheetahString::from_string(keys_vec.join(" ")))
                    .unwrap_or_default(),
                store_host: CheetahString::new(),
                client_host: CheetahString::new(),
                store_time: msg.store_timestamp(),
                retry_times: msg.reconsume_times(),
                body_length: msg.store_size(),
                msg_type: None,
                transaction_state: None,
                transaction_id: None,
                from_transaction_check: false,
            };

            beans.push(trace_bean);
        }

        if beans.is_empty() {
            None
        } else {
            Some((beans, region_id))
        }
    }

    /// Removes namespace prefix from a name string.
    ///
    /// Extracts the portion after the last `%` character. If no `%` exists, returns the input
    /// unchanged. RocketMQ uses the format `namespace%name` to represent namespaced resources.
    #[inline]
    fn without_namespace(name: &CheetahString) -> CheetahString {
        if let Some(pos) = name.as_str().rfind('%') {
            CheetahString::from_slice(&name[pos + 1..])
        } else {
            name.clone()
        }
    }

    /// Parses the consumption return type from context properties.
    ///
    /// Returns the ordinal value of `ConsumeReturnType` (0=Success, 1=Timeout, 2=Exception,
    /// 3=ReturnNull, 4=Failed). Defaults to 0 (Success) if the property is missing or unrecognized.
    fn parse_context_code(&self, context: &ConsumeMessageContext) -> i32 {
        context
            .props
            .get(&CheetahString::from_static_str(mix_all::CONSUME_CONTEXT_TYPE))
            .and_then(|type_str| match type_str.as_str() {
                "SUCCESS" => Some(ConsumeReturnType::Success),
                "TIME_OUT" | "TIMEOUT" => Some(ConsumeReturnType::TimeOut),
                "EXCEPTION" => Some(ConsumeReturnType::Exception),
                "RETURNNULL" | "RETURN_NULL" => Some(ConsumeReturnType::ReturnNull),
                "FAILED" => Some(ConsumeReturnType::Failed),
                _ => None,
            })
            .map(i32::from)
            .unwrap_or(0) // Default to Success if not found or invalid
    }
}

impl ConsumeMessageHook for ConsumeMessageTraceHookImpl {
    fn hook_name(&self) -> &'static str {
        "ConsumeMessageTraceHook"
    }

    /// Invoked before message consumption begins.
    ///
    /// Builds trace beans from messages (respecting per-message trace switches), creates a
    /// `SubBefore` trace context, and dispatches it asynchronously. Returns early if the
    /// message list is empty or all messages have tracing disabled. Logs warnings on dispatch
    /// failure but does not block.
    fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
        // Early return: empty message list
        if context.msg_list.is_empty() {
            return;
        }

        // Build trace beans (filters messages with trace disabled)
        let Some((beans, region_id)) = self.build_trace_beans(context.msg_list) else {
            // All messages have trace disabled
            return;
        };

        // Create SubBefore trace context
        let trace_context = Arc::new(TraceContext {
            trace_type: Some(TraceType::SubBefore),
            time_stamp: current_millis(),
            region_id,
            region_name: CheetahString::new(),
            group_name: Self::without_namespace(&context.consumer_group),
            cost_time: 0,
            is_success: true,
            request_id: CheetahString::from_string(
                rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter::create_uniq_id(),
            ),
            context_code: 0,
            access_channel: context.access_channel,
            trace_beans: Some(beans),
        });

        // Store trace_context in mq_trace_context for use in consume_message_after
        context.mq_trace_context = Some(trace_context.clone());

        // Dispatch trace context asynchronously (non-blocking)
        if !self.trace_dispatcher.append(trace_context.as_ref()) {
            tracing::warn!(
                "Failed to append SubBefore trace context for consumer group: {}",
                context.consumer_group
            );
        }
    }

    /// Invoked after message consumption completes.
    ///
    /// Retrieves the `SubBefore` trace context from `context.mq_trace_context`, calculates
    /// consumption cost time, parses the consumption return type, and creates a `SubAfter`
    /// trace context. Dispatches the context asynchronously. Returns early if the message list
    /// is empty or the `SubBefore` context is missing. Logs warnings on dispatch failure but
    /// does not block.
    fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
        // Early return: empty message list
        if context.msg_list.is_empty() {
            return;
        }

        // Retrieve SubBefore trace context
        let Some(trace_ctx_any) = &context.mq_trace_context else {
            tracing::debug!("No trace context found in consume_message_after, skipping");
            return;
        };

        let Some(sub_before_context) = trace_ctx_any.downcast_ref::<TraceContext>() else {
            tracing::warn!("Failed to downcast mq_trace_context to TraceContext");
            return;
        };

        // Verify SubBefore has trace beans
        let Some(ref sub_before_beans) = sub_before_context.trace_beans else {
            tracing::debug!("SubBefore context has no trace beans, skipping SubAfter");
            return;
        };

        if sub_before_beans.is_empty() {
            return;
        }

        // Calculate average consumption cost time
        let cost_time = ((current_millis() - sub_before_context.time_stamp) as usize / context.msg_list.len()) as i32;

        // Parse consumption return type from context properties
        let context_code = self.parse_context_code(context);

        // Create SubAfter trace context (reuses data from SubBefore)
        let sub_after_context = TraceContext {
            trace_type: Some(TraceType::SubAfter),
            time_stamp: current_millis(),
            region_id: sub_before_context.region_id.clone(),
            region_name: sub_before_context.region_name.clone(),
            group_name: sub_before_context.group_name.clone(),
            cost_time,
            is_success: context.success,
            request_id: sub_before_context.request_id.clone(),
            context_code,
            access_channel: context.access_channel,
            trace_beans: Some(sub_before_beans.clone()), // Reuse beans from SubBefore
        };

        // Dispatch trace context asynchronously (non-blocking)
        if !self.trace_dispatcher.append(&sub_after_context) {
            tracing::warn!(
                "Failed to append SubAfter trace context for consumer group: {}",
                context.consumer_group
            );
        }
    }
}
