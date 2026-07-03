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

use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::RecallMessageHandle;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::RPCHook;

use crate::trace::trace_bean::TraceBean;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_dispatcher::ArcTraceDispatcher;
use crate::trace::trace_type::TraceType;

const RECALL_TRACE_ENABLE_KEY: &str = "com.rocketmq.recall.default.trace.enable";

pub struct DefaultRecallMessageTraceHook {
    trace_dispatcher: ArcTraceDispatcher,
    enable_default_trace: bool,
}

impl DefaultRecallMessageTraceHook {
    pub fn new(trace_dispatcher: ArcTraceDispatcher) -> Self {
        let enable_default_trace = std::env::var(RECALL_TRACE_ENABLE_KEY)
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
        Self {
            trace_dispatcher,
            enable_default_trace,
        }
    }

    pub fn new_with_enable_default_trace(trace_dispatcher: ArcTraceDispatcher, enable_default_trace: bool) -> Self {
        Self {
            trace_dispatcher,
            enable_default_trace,
        }
    }
}

impl RPCHook for DefaultRecallMessageTraceHook {
    fn do_before_request(
        &self,
        _remote_addr: SocketAddr,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        if request.code() != RequestCode::RecallMessage as i32 || !self.enable_default_trace {
            return Ok(());
        }

        let Some(ext_fields) = response.ext_fields() else {
            return Ok(());
        };
        let Some(region_id) = ext_fields.get(MessageConst::PROPERTY_MSG_REGION) else {
            return Ok(());
        };

        let Some(trace_context) = build_recall_trace_context(request, response, region_id) else {
            return Ok(());
        };
        self.trace_dispatcher.append(&trace_context);

        Ok(())
    }
}

fn build_recall_trace_context(
    request: &RemotingCommand,
    response: &RemotingCommand,
    region_id: &CheetahString,
) -> Option<TraceContext> {
    let request_header = request
        .decode_command_custom_header::<RecallMessageRequestHeader>()
        .ok()?;
    let recall_handle = RecallMessageHandle::decode_handle(request_header.recall_handle().as_str()).ok()?;
    let topic = NamespaceUtil::without_namespace(request_header.topic().as_str());
    let group = request_header
        .producer_group()
        .map(|group| NamespaceUtil::without_namespace(group.as_str()))
        .unwrap_or_default();

    let trace_bean = TraceBean {
        topic: CheetahString::from_string(topic),
        msg_id: CheetahString::from_slice(recall_handle.message_id()),
        ..Default::default()
    };

    let mut trace_context = TraceContext::new();
    trace_context.region_id = region_id.clone();
    trace_context.trace_type = Some(TraceType::Recall);
    trace_context.group_name = CheetahString::from_string(group);
    trace_context.trace_beans = Some(vec![trace_bean]);
    trace_context.is_success = ResponseCode::from(response.code()) == ResponseCode::Success;

    Some(trace_context)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use rocketmq_common::common::producer::recall_message_handle::HandleV1;
    use rocketmq_remoting::protocol::header::recall_message_response_header::RecallMessageResponseHeader;

    use super::*;
    use crate::base::access_channel::AccessChannel;
    use crate::trace::trace_dispatcher::TraceDispatcher;

    #[derive(Default)]
    struct CapturingTraceDispatcher {
        contexts: Mutex<Vec<TraceContext>>,
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
    }

    #[test]
    fn recall_trace_hook_appends_java_compatible_context_when_enabled() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::default());
        let dispatcher_trait: ArcTraceDispatcher = dispatcher.clone();
        let hook = DefaultRecallMessageTraceHook::new_with_enable_default_trace(dispatcher_trait, true);

        let recall_handle = HandleV1::build_handle("ns%RecallTopic", "broker-a", "1707111111111", "MSG_ID_001");
        let request_header = RecallMessageRequestHeader::new("ns%RecallTopic", recall_handle, Some("ns%ProducerGroup"));
        let mut request = RemotingCommand::create_request_command(RequestCode::RecallMessage, request_header);
        request.make_custom_header_to_net();

        let mut ext_fields = HashMap::new();
        ext_fields.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
            CheetahString::from_static_str("DefaultRegion"),
        );
        let mut response =
            RemotingCommand::create_response_command_with_header(RecallMessageResponseHeader::new("MSG_ID_001"))
                .set_ext_fields(ext_fields);
        response.set_code_ref(ResponseCode::Success);

        hook.do_after_response("127.0.0.1:10911".parse().expect("socket addr"), &request, &mut response)
            .expect("recall hook");

        let contexts = dispatcher.contexts.lock().expect("capture lock");
        assert_eq!(contexts.len(), 1);
        let context = &contexts[0];
        assert_eq!(context.trace_type, Some(TraceType::Recall));
        assert_eq!(context.region_id.as_str(), "DefaultRegion");
        assert_eq!(context.group_name.as_str(), "ProducerGroup");
        assert!(context.is_success);

        let beans = context.trace_beans.as_ref().expect("trace beans");
        assert_eq!(beans.len(), 1);
        assert_eq!(beans[0].topic.as_str(), "RecallTopic");
        assert_eq!(beans[0].msg_id.as_str(), "MSG_ID_001");
    }

    #[test]
    fn recall_trace_hook_is_disabled_by_default_like_java() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::default());
        let dispatcher_trait: ArcTraceDispatcher = dispatcher.clone();
        let hook = DefaultRecallMessageTraceHook::new_with_enable_default_trace(dispatcher_trait, false);

        let mut request = RemotingCommand::create_request_command(
            RequestCode::RecallMessage,
            RecallMessageRequestHeader::new(
                "RecallTopic",
                HandleV1::build_handle("RecallTopic", "broker-a", "1707111111111", "MSG_ID_001"),
                Some("ProducerGroup"),
            ),
        );
        request.make_custom_header_to_net();

        let mut ext_fields = HashMap::new();
        ext_fields.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
            CheetahString::from_static_str("DefaultRegion"),
        );
        let mut response =
            RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_ext_fields(ext_fields);

        hook.do_after_response("127.0.0.1:10911".parse().expect("socket addr"), &request, &mut response)
            .expect("recall hook");

        assert!(dispatcher.contexts.lock().expect("capture lock").is_empty());
    }
}
