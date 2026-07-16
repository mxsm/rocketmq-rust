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
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::MessageDecoder;
use rocketmq_error::RocketMQError;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

pub(super) fn build_send_message_request(msg: MessageExt, group: CheetahString) -> RemotingCommand {
    let header = SendMessageRequestHeader {
        producer_group: group,
        topic: msg.topic().clone(),
        default_topic: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
        default_topic_queue_nums: 8,
        queue_id: msg.queue_id,
        sys_flag: msg.sys_flag,
        born_timestamp: msg.born_timestamp,
        flag: msg.get_flag(),
        properties: Some(MessageDecoder::message_properties_to_string(msg.get_properties())),
        reconsume_times: Some(msg.reconsume_times),
        batch: Some(false),
        ..Default::default()
    };
    let header = SendMessageRequestHeaderV2::create_send_message_request_header_v2_with_move(header);
    RemotingCommand::create_request_command(RequestCode::SendMessage, header)
}

pub(crate) fn process_send_response(
    broker_name: &CheetahString,
    uniq_msg_id: CheetahString,
    queue_id: i32,
    topic: CheetahString,
    response: &RemotingCommand,
) -> rocketmq_error::RocketMQResult<SendResult> {
    let status = match ResponseCode::from(response.code()) {
        ResponseCode::FlushDiskTimeout => Some(SendStatus::FlushDiskTimeout),
        ResponseCode::FlushSlaveTimeout => Some(SendStatus::FlushSlaveTimeout),
        ResponseCode::SlaveNotAvailable => Some(SendStatus::SlaveNotAvailable),
        ResponseCode::Success => Some(SendStatus::SendOk),
        _ => None,
    };

    if let Some(status) = status {
        let header = response.decode_command_custom_header::<SendMessageResponseHeader>()?;
        let message_queue = MessageQueue::from_parts(topic, broker_name, queue_id);
        let mut result = SendResult::new(
            status,
            Some(uniq_msg_id),
            Some(header.msg_id().to_string()),
            Some(message_queue),
            header.queue_offset() as u64,
        );
        result.set_transaction_id(header.transaction_id().unwrap_or_default().to_string());
        if let Some(recall_handle) = header.recall_handle() {
            result.set_recall_handle(recall_handle.to_string());
        }
        if let Some(region_id) = response
            .get_ext_fields()
            .and_then(|fields| fields.get(MessageConst::PROPERTY_MSG_REGION))
        {
            result.set_region_id(region_id.to_string());
        } else {
            result.set_region_id(mix_all::DEFAULT_TRACE_REGION_ID.to_string());
        }
        result.set_trace_on(
            response
                .get_ext_fields()
                .and_then(|fields| fields.get(MessageConst::PROPERTY_TRACE_SWITCH))
                .is_some_and(|trace_on| trace_on == "true"),
        );
        return Ok(result);
    }

    Err(RocketMQError::BrokerOperationFailed {
        operation: "send_message",
        code: response.code(),
        message: response.remark().map_or("".to_string(), |remark| remark.to_string()),
        broker_addr: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_response_maps_success_to_model_result() {
        let header = SendMessageResponseHeader::new("offset-id".into(), 2, 17, None, None, None);
        let mut command = RemotingCommand::create_response_command_with_header(header).set_code(ResponseCode::Success);
        command.make_custom_header_to_net();

        let result = process_send_response(&"broker-a".into(), "client-id".into(), 2, "topic-a".into(), &command)
            .expect("send response should map");

        assert_eq!(SendStatus::SendOk, result.send_status);
        assert_eq!(17, result.queue_offset);
    }
}
