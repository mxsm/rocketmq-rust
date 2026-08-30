// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_model::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::ResponsePlan;

use super::core::PopLiteCoreResult;
use super::PopLiteMessageProcessor;
use crate::processor::response_plan::BrokerResponseParts;

#[derive(Clone, Copy)]
pub(crate) enum PopLiteResponseKind {
    Found,
    PollingFull,
    PollingTimeout,
}

impl<MS> PopLiteMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    pub(super) fn compose_pop_lite_command(
        &self,
        opaque: i32,
        request_header: &PopLiteMessageRequestHeader,
        result: PopLiteCoreResult,
        kind: PopLiteResponseKind,
    ) -> RemotingCommand {
        let (mut head, body) = self.compose_pop_lite_parts(request_header, result, kind);
        head.set_opaque_mut(opaque);
        if let Some(body) = body {
            head.set_body_mut_ref(body);
        }
        head
    }

    pub(super) fn compose_pop_lite_plan(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        result: PopLiteCoreResult,
        kind: PopLiteResponseKind,
    ) -> rocketmq_error::RocketMQResult<ResponsePlan> {
        compose_pop_lite_response_plan(&self.context.command_factory, request_header, result, kind)
    }

    fn compose_pop_lite_parts(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        result: PopLiteCoreResult,
        kind: PopLiteResponseKind,
    ) -> (RemotingCommand, Option<bytes::Bytes>) {
        compose_pop_lite_response_parts(&self.context.command_factory, request_header, result, kind)
    }
}

pub(crate) fn compose_pop_lite_response_plan(
    command_factory: &RemotingCommandFactory,
    request_header: &PopLiteMessageRequestHeader,
    result: PopLiteCoreResult,
    kind: PopLiteResponseKind,
) -> rocketmq_error::RocketMQResult<ResponsePlan> {
    let (head, body) = compose_pop_lite_response_parts(command_factory, request_header, result, kind);
    match body {
        Some(body) => BrokerResponseParts::bytes(head, body)?.into_response_plan(),
        None => BrokerResponseParts::command(head)?.into_response_plan(),
    }
}

fn compose_pop_lite_response_parts(
    command_factory: &RemotingCommandFactory,
    request_header: &PopLiteMessageRequestHeader,
    result: PopLiteCoreResult,
    kind: PopLiteResponseKind,
) -> (RemotingCommand, Option<bytes::Bytes>) {
    let response_header = PopLiteMessageResponseHeader {
        pop_time: current_millis() as i64,
        invisible_time: request_header.invisible_time,
        revive_qid: POP_ORDER_REVIVE_QUEUE,
        start_offset_info: None,
        msg_offset_info: None,
        order_count_info: (result.fetched_count > 0).then_some(result.order_count_info).flatten(),
    };
    let mut head = command_factory.create_success_response_command_with_header(response_header);
    match kind {
        PopLiteResponseKind::Found => {
            head.set_code_ref(ResponseCode::Success);
            head.set_remark_mut("FOUND");
        }
        PopLiteResponseKind::PollingFull => {
            head.set_code_ref(ResponseCode::PollingFull);
            head.set_remark_mut("POP_LITE_POLLING_FULL");
        }
        PopLiteResponseKind::PollingTimeout => {
            head.set_code_ref(ResponseCode::PollingTimeout);
            head.set_remark_mut("NO_MESSAGE_IN_QUEUE");
        }
    }
    (head, result.body)
}
