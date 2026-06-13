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

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;

use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_context::SendMessageTraceSnapshot;
use crate::hook::send_message_hook::SendMessageHook;
use crate::producer::send_status::SendStatus;
use crate::trace::trace_bean::TraceBean;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_dispatcher::ArcTraceDispatcher;
use crate::trace::trace_type::TraceType;

pub struct SendMessageTraceHookImpl {
    trace_dispatcher: ArcTraceDispatcher,
}

impl SendMessageTraceHookImpl {
    pub fn new(trace_dispatcher: ArcTraceDispatcher) -> Self {
        Self { trace_dispatcher }
    }
}
impl SendMessageHook for SendMessageTraceHookImpl {
    fn hook_name(&self) -> &'static str {
        "SendMessageTraceHook"
    }

    fn send_message_before(&self, _context: &Option<SendMessageContext<'_>>) {
        // The Rust hook trait currently receives an immutable context, so the
        // trace context is built in `send_message_after` where the send result is
        // available.
    }

    fn send_message_after(&self, context: &Option<SendMessageContext<'_>>) {
        let Some(context) = context.as_ref() else {
            return;
        };
        let Some(send_result) = context.send_result else {
            return;
        };
        if send_result.region_id.is_none() || !send_result.trace_on {
            return;
        }
        let Some(message_snapshot) = message_trace_snapshot(context) else {
            return;
        };
        if is_trace_topic(&self.trace_dispatcher, message_snapshot.topic.as_str()) {
            return;
        }

        let now = current_millis();
        let trace_start_time = context.trace_start_time.unwrap_or(now);
        let cost_time = now.saturating_sub(trace_start_time) as i32;
        let mut trace_context = TraceContext::new();
        trace_context.trace_type = Some(TraceType::Pub);
        trace_context.time_stamp = trace_start_time;
        trace_context.group_name = context
            .producer_group
            .as_ref()
            .map(|group| CheetahString::from_string(NamespaceUtil::without_namespace(group.as_str())))
            .unwrap_or_default();
        trace_context.region_id = send_result
            .region_id
            .as_ref()
            .map(|region| CheetahString::from_string(region.clone()))
            .unwrap_or_default();
        trace_context.is_success = send_result.send_status == SendStatus::SendOk;
        trace_context.cost_time = cost_time;

        let trace_bean = TraceBean {
            topic: CheetahString::from_string(NamespaceUtil::without_namespace(message_snapshot.topic.as_str())),
            tags: message_snapshot.tags.clone(),
            keys: message_snapshot.keys.clone(),
            store_host: context.broker_addr.clone().unwrap_or_default(),
            body_length: message_snapshot.body_length,
            msg_type: context.msg_type,
            msg_id: send_result.msg_id.clone().unwrap_or_default(),
            offset_msg_id: send_result
                .offset_msg_id
                .as_ref()
                .map(|value| CheetahString::from_string(value.clone()))
                .unwrap_or_default(),
            store_time: trace_start_time.saturating_add((cost_time / 2) as u64) as i64,
            ..Default::default()
        };
        trace_context.trace_beans = Some(vec![trace_bean]);

        self.trace_dispatcher.append(&trace_context);
    }
}

fn message_trace_snapshot(context: &SendMessageContext<'_>) -> Option<SendMessageTraceSnapshot> {
    if let Some(message) = context.message {
        return Some(SendMessageTraceSnapshot::from_message(message));
    }
    context.message_trace_snapshot.clone()
}

fn is_trace_topic(trace_dispatcher: &ArcTraceDispatcher, topic: &str) -> bool {
    trace_dispatcher
        .trace_topic_name()
        .is_some_and(|trace_topic| topic.starts_with(trace_topic))
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::Mutex;

    use rocketmq_common::common::message::message_enum::MessageType;
    use rocketmq_common::TimeUtils::current_millis;

    use super::*;
    use crate::base::access_channel::AccessChannel;
    use crate::implementation::communication_mode::CommunicationMode;
    use crate::producer::send_result::SendResult;
    use crate::trace::trace_dispatcher::TraceDispatcher;

    struct CapturingTraceDispatcher {
        trace_topic_name: &'static str,
        contexts: Mutex<Vec<TraceContext>>,
    }

    impl CapturingTraceDispatcher {
        fn new(trace_topic_name: &'static str) -> Self {
            Self {
                trace_topic_name,
                contexts: Mutex::new(Vec::new()),
            }
        }
    }

    impl TraceDispatcher for CapturingTraceDispatcher {
        fn start(&self, _name_srv_addr: &str, _access_channel: AccessChannel) -> rocketmq_error::RocketMQResult<()> {
            Ok(())
        }

        fn append(&self, ctx: &dyn Any) -> bool {
            let Some(trace_context) = ctx.downcast_ref::<TraceContext>() else {
                return false;
            };
            self.contexts.lock().expect("capture lock").push(trace_context.clone());
            true
        }

        fn flush(&self) -> rocketmq_error::RocketMQResult<()> {
            Ok(())
        }

        fn shutdown(&self) {}

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_mut_any(&mut self) -> &mut dyn Any {
            self
        }

        fn trace_topic_name(&self) -> Option<&str> {
            Some(self.trace_topic_name)
        }
    }

    fn send_result(trace_on: bool) -> SendResult {
        SendResult {
            send_status: SendStatus::SendOk,
            msg_id: Some(CheetahString::from_static_str("UNIQ_ID_001")),
            offset_msg_id: Some("OFFSET_ID_001".to_string()),
            region_id: Some("DefaultRegion".to_string()),
            trace_on,
            ..Default::default()
        }
    }

    fn context<'a>(
        snapshot: SendMessageTraceSnapshot,
        send_result: &'a SendResult,
        trace_start_time: u64,
    ) -> Option<SendMessageContext<'a>> {
        Some(SendMessageContext {
            producer_group: Some(CheetahString::from_static_str("ns%ProducerGroup")),
            message_trace_snapshot: Some(snapshot),
            broker_addr: Some(CheetahString::from_static_str("broker-a:10911")),
            communication_mode: Some(CommunicationMode::Sync),
            send_result: Some(send_result),
            trace_start_time: Some(trace_start_time),
            msg_type: Some(MessageType::NormalMsg),
            ..Default::default()
        })
    }

    #[test]
    fn send_trace_hook_appends_java_pub_context_from_snapshot() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::new("TRACE_TOPIC"));
        let hook = SendMessageTraceHookImpl::new(dispatcher.clone());
        let result = send_result(true);
        let trace_start_time = current_millis();
        let snapshot = SendMessageTraceSnapshot {
            topic: CheetahString::from_static_str("ns%TopicA"),
            tags: CheetahString::from_static_str("tag-a"),
            keys: CheetahString::from_static_str("key-a"),
            body_length: 7,
        };

        hook.send_message_after(&context(snapshot, &result, trace_start_time));

        let contexts = dispatcher.contexts.lock().expect("capture lock");
        assert_eq!(contexts.len(), 1);
        let trace_context = &contexts[0];
        assert_eq!(trace_context.trace_type, Some(TraceType::Pub));
        assert_eq!(trace_context.group_name.as_str(), "ProducerGroup");
        assert_eq!(trace_context.region_id.as_str(), "DefaultRegion");
        assert!(trace_context.is_success);
        assert_eq!(trace_context.time_stamp, trace_start_time);
        assert!(trace_context.cost_time >= 0);

        let bean = &trace_context.trace_beans.as_ref().expect("trace beans")[0];
        assert_eq!(bean.topic.as_str(), "TopicA");
        assert_eq!(bean.tags.as_str(), "tag-a");
        assert_eq!(bean.keys.as_str(), "key-a");
        assert_eq!(bean.store_host.as_str(), "broker-a:10911");
        assert_eq!(bean.body_length, 7);
        assert_eq!(bean.msg_type, Some(MessageType::NormalMsg));
        assert_eq!(bean.msg_id.as_str(), "UNIQ_ID_001");
        assert_eq!(bean.offset_msg_id.as_str(), "OFFSET_ID_001");
        assert!(bean.store_time >= trace_start_time as i64);
    }

    #[test]
    fn send_trace_hook_skips_trace_topic_like_java() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::new("TRACE_TOPIC"));
        let hook = SendMessageTraceHookImpl::new(dispatcher.clone());
        let result = send_result(true);
        let snapshot = SendMessageTraceSnapshot {
            topic: CheetahString::from_static_str("TRACE_TOPIC"),
            tags: CheetahString::new(),
            keys: CheetahString::new(),
            body_length: 0,
        };

        hook.send_message_after(&context(snapshot, &result, current_millis()));

        assert!(dispatcher.contexts.lock().expect("capture lock").is_empty());
    }
}
