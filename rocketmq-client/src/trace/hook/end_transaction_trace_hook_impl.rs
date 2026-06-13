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
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;

use crate::hook::end_transaction_context::EndTransactionContext;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::trace::trace_bean::TraceBean;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_dispatcher::ArcTraceDispatcher;
use crate::trace::trace_type::TraceType;

pub struct EndTransactionTraceHookImpl {
    trace_dispatcher: ArcTraceDispatcher,
}

impl EndTransactionTraceHookImpl {
    pub fn new(trace_dispatcher: ArcTraceDispatcher) -> Self {
        Self { trace_dispatcher }
    }
}

impl EndTransactionHook for EndTransactionTraceHookImpl {
    fn hook_name(&self) -> &'static str {
        "EndTransactionTraceHook"
    }

    fn end_transaction(&self, context: &EndTransactionContext) {
        let message = context.message;
        if self
            .trace_dispatcher
            .trace_topic_name()
            .is_some_and(|trace_topic| message.topic().starts_with(trace_topic))
        {
            return;
        }

        let mut trace_context = TraceContext::new();
        trace_context.trace_type = Some(TraceType::EndTransaction);
        trace_context.group_name =
            CheetahString::from_string(NamespaceUtil::without_namespace(context.producer_group.as_str()));
        trace_context.region_id = MessageTrait::property(
            message,
            &CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
        )
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| CheetahString::from_static_str(mix_all::DEFAULT_TRACE_REGION_ID));
        trace_context.time_stamp = current_millis();

        let mut trace_bean = TraceBean {
            topic: CheetahString::from_string(NamespaceUtil::without_namespace(message.topic().as_str())),
            tags: MessageTrait::tags(message).unwrap_or_default(),
            keys: MessageTrait::get_keys(message).unwrap_or_default(),
            store_host: context.broker_addr.clone(),
            msg_type: Some(MessageType::TransMsgCommit),
            msg_id: context.msg_id.clone(),
            transaction_state: Some(context.transaction_state),
            transaction_id: Some(context.transaction_id.clone()),
            from_transaction_check: context.from_transaction_check,
            ..Default::default()
        };
        if let Some(client_host) = self.trace_dispatcher.trace_client_host() {
            trace_bean.client_host = client_host;
        }
        trace_context.trace_beans = Some(vec![trace_bean]);

        self.trace_dispatcher.append(&trace_context);
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::Mutex;

    use rocketmq_common::common::message::message_single::Message;

    use super::*;
    use crate::base::access_channel::AccessChannel;
    use crate::producer::local_transaction_state::LocalTransactionState;
    use crate::trace::trace_dispatcher::TraceDispatcher;

    struct CapturingTraceDispatcher {
        trace_topic_name: &'static str,
        client_host: Option<CheetahString>,
        contexts: Mutex<Vec<TraceContext>>,
    }

    impl CapturingTraceDispatcher {
        fn new(trace_topic_name: &'static str, client_host: Option<CheetahString>) -> Self {
            Self {
                trace_topic_name,
                client_host,
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

        fn trace_client_host(&self) -> Option<CheetahString> {
            self.client_host.clone()
        }
    }

    fn transaction_message(topic: &str) -> Message {
        let mut message = Message::builder()
            .topic(topic)
            .body_slice(b"transaction body")
            .tags("tag-a")
            .key("key-a")
            .build_unchecked();
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
            CheetahString::from_static_str("RegionA"),
        );
        message
    }

    fn context<'a>(message: &'a Message) -> EndTransactionContext<'a> {
        EndTransactionContext {
            producer_group: CheetahString::from_static_str("ns%ProducerGroup"),
            broker_addr: CheetahString::from_static_str("broker-a:10911"),
            message,
            msg_id: CheetahString::from_static_str("MSG_ID_001"),
            transaction_id: CheetahString::from_static_str("TX_ID_001"),
            transaction_state: LocalTransactionState::CommitMessage,
            from_transaction_check: true,
        }
    }

    #[test]
    fn end_transaction_trace_hook_appends_java_compatible_context() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::new(
            "TRACE_TOPIC",
            Some(CheetahString::from_static_str("CLIENT_ID_001")),
        ));
        let hook = EndTransactionTraceHookImpl::new(dispatcher.clone());
        let message = transaction_message("ns%TxTopic");

        hook.end_transaction(&context(&message));

        let contexts = dispatcher.contexts.lock().expect("capture lock");
        assert_eq!(contexts.len(), 1);
        let trace_context = &contexts[0];
        assert_eq!(trace_context.trace_type, Some(TraceType::EndTransaction));
        assert_eq!(trace_context.group_name.as_str(), "ProducerGroup");
        assert_eq!(trace_context.region_id.as_str(), "RegionA");
        assert!(trace_context.time_stamp > 0);

        let bean = &trace_context.trace_beans.as_ref().expect("trace beans")[0];
        assert_eq!(bean.topic.as_str(), "TxTopic");
        assert_eq!(bean.tags.as_str(), "tag-a");
        assert_eq!(bean.keys.as_str(), "key-a");
        assert_eq!(bean.store_host.as_str(), "broker-a:10911");
        assert_eq!(bean.msg_type, Some(MessageType::TransMsgCommit));
        assert_eq!(bean.client_host.as_str(), "CLIENT_ID_001");
        assert_eq!(bean.msg_id.as_str(), "MSG_ID_001");
        assert_eq!(bean.transaction_state, Some(LocalTransactionState::CommitMessage));
        assert_eq!(bean.transaction_id.as_deref(), Some("TX_ID_001"));
        assert!(bean.from_transaction_check);
    }

    #[test]
    fn end_transaction_trace_hook_skips_trace_topic_like_java() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::new(
            "TRACE_TOPIC",
            Some(CheetahString::from_static_str("CLIENT_ID_001")),
        ));
        let hook = EndTransactionTraceHookImpl::new(dispatcher.clone());
        let message = transaction_message("TRACE_TOPIC");

        hook.end_transaction(&context(&message));

        assert!(dispatcher.contexts.lock().expect("capture lock").is_empty());
    }
}
