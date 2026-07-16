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
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_model::result::PullOutcome;
use rocketmq_model::result::PullStatus;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;

use crate::out_api::result::BrokerPullResponse;

pub(super) fn build_pull_message_request(
    broker_name: &CheetahString,
    consumer_group: &CheetahString,
    topic: &CheetahString,
    queue_id: i32,
    offset: i64,
    max_nums: i32,
) -> RemotingCommand {
    let request_header = PullMessageRequestHeader {
        consumer_group: consumer_group.clone(),
        topic: topic.clone(),
        queue_id,
        queue_offset: offset,
        max_msg_nums: max_nums,
        sys_flag: PullSysFlag::build_sys_flag(false, false, true, false) as i32,
        commit_offset: 0,
        suspend_timeout_millis: 0,
        subscription: Some(CheetahString::from_static_str(SubscriptionData::SUB_ALL)),
        sub_version: current_millis() as i64,
        expression_type: Some(CheetahString::from_static_str(ExpressionType::TAG)),
        max_msg_bytes: Some(i32::MAX),
        topic_request: Some(TopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
        }),
        ..Default::default()
    };
    RemotingCommand::create_request_command(RequestCode::PullMessage, request_header)
}

pub(super) fn process_pull_response(
    mut response: RemotingCommand,
    addr: &CheetahString,
) -> rocketmq_error::RocketMQResult<BrokerPullResponse> {
    let status = match ResponseCode::from(response.code()) {
        ResponseCode::Success => PullStatus::Found,
        ResponseCode::PullNotFound => PullStatus::NoNewMsg,
        ResponseCode::PullRetryImmediately => PullStatus::NoMatchedMsg,
        ResponseCode::PullOffsetMoved => PullStatus::OffsetIllegal,
        _ => {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "pull_message",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |remark| remark.to_string()),
                broker_addr: Some(addr.to_string()),
            })
        }
    };
    let header = response.decode_command_custom_header::<PullMessageResponseHeader>()?;
    Ok(BrokerPullResponse {
        status,
        next_begin_offset: header.next_begin_offset as u64,
        min_offset: header.min_offset as u64,
        max_offset: header.max_offset as u64,
        message_binary: response.take_body(),
        offset_delta: header.offset_delta,
    })
}

pub(super) fn decode_pull_response(
    mut response: BrokerPullResponse,
    broker_name: &CheetahString,
    queue_id: i32,
) -> PullOutcome<MessageExt> {
    let mut messages = Vec::new();
    if response.status == PullStatus::Found {
        let mut bytes = response.message_binary.take().unwrap_or_default();
        messages = MessageDecoder::decodes_batch(&mut bytes, true, true);
        for message in &mut messages {
            if message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_TRANSACTION_PREPARED,
                ))
                .is_some_and(|value| value == "true")
            {
                if let Some(id) = message.property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                )) {
                    message.set_transaction_id(id);
                }
            }
            MessageAccessor::put_property(
                message,
                CheetahString::from_static_str(MessageConst::PROPERTY_MIN_OFFSET),
                response.min_offset.to_string().into(),
            );
            MessageAccessor::put_property(
                message,
                CheetahString::from_static_str(MessageConst::PROPERTY_MAX_OFFSET),
                response.max_offset.to_string().into(),
            );
            message.set_broker_name(broker_name.clone());
            message.set_queue_id(queue_id);
            if let Some(offset_delta) = response.offset_delta {
                message.set_queue_offset(message.queue_offset + offset_delta);
            }
        }
    }
    PullOutcome::new(
        response.status,
        response.next_begin_offset,
        response.min_offset,
        response.max_offset,
        Some(messages),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pull_response_maps_no_new_message_without_client_types() {
        let header = PullMessageResponseHeader {
            suggest_which_broker_id: 0,
            next_begin_offset: 8,
            min_offset: 1,
            max_offset: 8,
            ..Default::default()
        };
        let mut command =
            RemotingCommand::create_response_command_with_header(header).set_code(ResponseCode::PullNotFound);
        command.make_custom_header_to_net();

        let response = process_pull_response(command, &"127.0.0.1:10911".into()).expect("pull response should map");
        let outcome = decode_pull_response(response, &"broker-a".into(), 2);

        assert_eq!(PullStatus::NoNewMsg, outcome.pull_status());
        assert_eq!(8, outcome.next_begin_offset());
        assert!(outcome.messages().is_some_and(<[MessageExt]>::is_empty));
    }
}
