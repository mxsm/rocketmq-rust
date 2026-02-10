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

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::producer::recall_message_handle::RecallMessageHandle;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_remoting::protocol::header::recall_message_response_header::RecallMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

const RECALL_MESSAGE_TAG: &str = "_RECALL_TAG_";

pub struct RecallMessageProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> RecallMessageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS> RequestProcessor for RecallMessageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("RecallMessageProcessor received request code: {:?}", request_code);

        match request_code {
            RequestCode::RecallMessage => {
                let response = self.process_recall_message(&channel, &ctx, request).await?;
                Ok(Some(response))
            }
            _ => {
                warn!(
                    "RecallMessageProcessor received unexpected request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!(
                        "RecallMessageProcessor does not support request code {}",
                        request.code()
                    ),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS> RecallMessageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_recall_message(
        &mut self,
        _channel: &Channel,
        ctx: &ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command_with_header(RecallMessageResponseHeader::default());
        response.set_opaque_mut(request.opaque());

        let region_id = self.broker_runtime_inner.broker_config().region_id.clone();
        response.add_ext_field(MessageConst::PROPERTY_MSG_REGION, region_id);

        let request_header = request.decode_command_custom_header::<RecallMessageRequestHeader>()?;

        if !self.broker_runtime_inner.broker_config().recall_message_enable {
            return Ok(response
                .set_code(ResponseCode::NoPermission)
                .set_remark(CheetahString::from_static_str("recall failed, operation is forbidden")));
        }

        if self.broker_runtime_inner.message_store_config().broker_role == BrokerRole::Slave {
            return Ok(response
                .set_code(ResponseCode::SlaveNotAvailable)
                .set_remark(CheetahString::from_static_str(
                    "recall failed, broker service not available",
                )));
        }

        let start_timestamp = self
            .broker_runtime_inner
            .broker_config()
            .start_accept_send_request_time_stamp;
        if get_current_millis() < start_timestamp as u64 {
            return Ok(response.set_code(ResponseCode::ServiceNotAvailable).set_remark(
                CheetahString::from_static_str("recall failed, broker service not available"),
            ));
        }

        let broker_permission = self.broker_runtime_inner.broker_config().broker_permission;
        let allow_recall_when_not_writeable = self
            .broker_runtime_inner
            .broker_config()
            .allow_recall_when_broker_not_writeable;

        if !PermName::is_writeable(broker_permission) && !allow_recall_when_not_writeable {
            return Ok(response.set_code(ResponseCode::ServiceNotAvailable).set_remark(
                CheetahString::from_static_str("recall failed, broker service not available"),
            ));
        }

        let _topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(request_header.topic());

        if _topic_config.is_none() {
            return Ok(response
                .set_code(ResponseCode::TopicNotExist)
                .set_remark(CheetahString::from_string(format!(
                    "recall failed, the topic[{}] not exist",
                    request_header.topic()
                ))));
        }

        let handle = match RecallMessageHandle::decode_handle(request_header.recall_handle()) {
            Ok(handle) => handle,
            Err(e) => {
                return Ok(response
                    .set_code(ResponseCode::IllegalOperation)
                    .set_remark(CheetahString::from_string(format!("recall failed, {}", e))));
            }
        };

        let handle_topic = handle.topic();
        let handle_broker_name = handle.broker_name();
        let handle_timestamp_str = handle.timestamp_str();
        let handle_message_id = handle.message_id();

        if request_header.topic().as_str() != handle_topic {
            return Ok(response
                .set_code(ResponseCode::IllegalOperation)
                .set_remark(CheetahString::from_static_str("recall failed, topic not match")));
        }

        let broker_name = self.broker_runtime_inner.broker_config().broker_name();
        if broker_name.as_str() != handle_broker_name {
            return Ok(response
                .set_code(ResponseCode::IllegalOperation)
                .set_remark(CheetahString::from_static_str(
                    "recall failed, broker service not available",
                )));
        }

        let timestamp = handle_timestamp_str.parse::<i64>().unwrap_or(-1);
        let current_time_millis = get_current_millis() as i64;
        let time_left = timestamp - current_time_millis;
        let timer_max_delay_sec = self.broker_runtime_inner.message_store_config().timer_max_delay_sec as i64;

        if time_left <= 0 || time_left >= timer_max_delay_sec * 1000 {
            return Ok(response
                .set_code(ResponseCode::IllegalOperation)
                .set_remark(CheetahString::from_static_str("recall failed, timestamp invalid")));
        }

        let msg_inner = self.build_message(
            ctx,
            &request_header,
            handle_topic,
            handle_timestamp_str,
            handle_message_id,
        );

        let begin_time_millis = get_current_millis();
        let put_message_result = self
            .broker_runtime_inner
            .message_store_mut()
            .as_mut()
            .expect("message store not initialized")
            .put_message(msg_inner)
            .await;

        self.handle_put_message_result(
            put_message_result,
            &mut response,
            begin_time_millis,
            CheetahString::from_string(handle_topic.to_string()),
        )?;

        Ok(response)
    }

    fn build_message(
        &self,
        ctx: &ConnectionHandlerContext,
        request_header: &RecallMessageRequestHeader,
        handle_topic: &str,
        handle_timestamp_str: &str,
        handle_message_id: &str,
    ) -> MessageExtBrokerInner {
        let timer_del_uniqkey = format!("{}_{}", handle_topic, handle_message_id);

        let body = Bytes::from_static(b"0");

        let tags_code =
            rocketmq_common::common::hasher::string_hasher::JavaStringHasher::hash_str(RECALL_MESSAGE_TAG) as i64;

        let mut properties = std::collections::HashMap::new();
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEL_UNIQKEY),
            CheetahString::from_string(timer_del_uniqkey),
        );
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_string(handle_message_id.to_string()),
        );
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS),
            CheetahString::from_string(handle_timestamp_str.to_string()),
        );
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_BORN_TIMESTAMP),
            CheetahString::from_string(get_current_millis().to_string()),
        );
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_CONTEXT),
            CheetahString::new(),
        );

        if let Some(producer_group) = request_header.producer_group() {
            properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP),
                producer_group.clone(),
            );
        }

        let properties_string = MessageDecoder::message_properties_to_string(&properties);

        let born_host = ctx.remote_address();
        let store_host = self.broker_runtime_inner.store_host();

        let mut message = Message::builder()
            .topic(handle_topic)
            .tags(RECALL_MESSAGE_TAG)
            .body(body)
            .build_unchecked();

        for (key, value) in properties {
            message.put_property(key, value);
        }

        let message_ext = MessageExt {
            message,
            broker_name: CheetahString::default(),
            queue_id: 0,
            store_size: 0,
            queue_offset: 0,
            sys_flag: 0,
            born_timestamp: get_current_millis() as i64,
            born_host,
            store_timestamp: 0,
            store_host,
            msg_id: CheetahString::default(),
            commit_log_offset: 0,
            body_crc: 0,
            reconsume_times: 0,
            prepared_transaction_offset: 0,
        };

        MessageExtBrokerInner {
            message_ext_inner: message_ext,
            properties_string,
            tags_code,
            encoded_buff: None,
            encode_completed: false,
            version: Default::default(),
        }
    }

    fn handle_put_message_result(
        &self,
        put_message_result: PutMessageResult,
        response: &mut RemotingCommand,
        begin_time_millis: u64,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let append_result = match put_message_result.append_message_result() {
            Some(result) => result,
            None => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut(CheetahString::from_static_str("recall failed, execute error"));
                return Ok(());
            }
        };

        match put_message_result.put_message_status() {
            PutMessageStatus::PutOk => {
                let msg_num = append_result.msg_num;
                let wrote_bytes = append_result.wrote_bytes;

                self.broker_runtime_inner
                    .broker_stats_manager()
                    .inc_topic_put_nums(&topic, msg_num, 1);

                self.broker_runtime_inner
                    .broker_stats_manager()
                    .inc_topic_put_size(&topic, wrote_bytes);

                self.broker_runtime_inner
                    .broker_stats_manager()
                    .inc_broker_put_nums(&topic, msg_num);

                let latency = (get_current_millis() - begin_time_millis) as i32;
                self.broker_runtime_inner
                    .broker_stats_manager()
                    .inc_topic_put_latency(&topic, 0, latency);

                {
                    let response_header = response
                        .read_custom_header_mut::<RecallMessageResponseHeader>()
                        .ok_or_else(|| RocketMQError::Internal("Response header missing".to_string()))?;

                    if let Some(msg_id) = &append_result.msg_id {
                        response_header.set_msg_id(msg_id.as_str());
                    }
                }

                response.set_code_mut(ResponseCode::Success);
            }
            PutMessageStatus::FlushDiskTimeout
            | PutMessageStatus::FlushSlaveTimeout
            | PutMessageStatus::SlaveNotAvailable => {
                {
                    let response_header = response
                        .read_custom_header_mut::<RecallMessageResponseHeader>()
                        .ok_or_else(|| RocketMQError::Internal("Response header missing".to_string()))?;

                    if let Some(msg_id) = &append_result.msg_id {
                        response_header.set_msg_id(msg_id.as_str());
                    }
                }

                response.set_code_mut(ResponseCode::Success);
            }
            _ => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut(CheetahString::from_static_str("recall failed, execute error"));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recall_message_tag_constant() {
        assert_eq!(RECALL_MESSAGE_TAG, "_RECALL_TAG_");
    }
}
