/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use rocketmq_common::{
    common::{
        attribute::{cleanup_policy::CleanupPolicy, topic_message_type::TopicMessageType},
        message::{
            message_batch::MessageExtBatch,
            message_client_id_setter,
            message_single::{MessageExt, MessageExtBrokerInner},
            MessageConst,
        },
        mix_all::RETRY_GROUP_TOPIC_PREFIX, topic::TopicValidator,
    },
    CleanupPolicyUtils, MessageDecoder,
};
use rocketmq_remoting::{
    code::{
        request_code::RequestCode,
        response_code::{RemotingSysResponseCode, ResponseCode},
    },
    protocol::{
        header::message_operation_header::{
            send_message_request_header::{parse_request_header, SendMessageRequestHeader},
            send_message_response_header::SendMessageResponseHeader,
            TopicRequestHeaderTrait,
        },
        remoting_command::RemotingCommand,
        static_topic::topic_queue_mapping_context::TopicQueueMappingContext,
    },
    runtime::{processor::RequestProcessor, server::ConnectionHandlerContext},
};
use rocketmq_store::{
    base::message_result::PutMessageResult, config::message_store_config::MessageStoreConfig,
    log_file::MessageStore, status::manager::broker_stats_manager::BrokerStatsManager,
};
use tokio::runtime::Handle;
use tracing::info;

use crate::{
    broker_config::BrokerConfig,
    mqtrace::send_message_context::SendMessageContext,
    processor::SendMessageProcessorInner,
    topic::manager::{
        topic_config_manager::TopicConfigManager,
        topic_queue_mapping_manager::TopicQueueMappingManager,
    },
};

pub struct SendMessageProcessor<MS> {
    inner: SendMessageProcessorInner,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    topic_config_manager: Arc<TopicConfigManager>,
    broker_config: Arc<BrokerConfig>,
    message_store: Arc<MS>,
    store_host: SocketAddr,
}

impl<MS: Default> Default for SendMessageProcessor<MS> {
    fn default() -> Self {
        let store_host = "127.0.0.1:100".parse::<SocketAddr>().unwrap();
        Self {
            inner: SendMessageProcessorInner::default(),
            topic_queue_mapping_manager: Arc::new(TopicQueueMappingManager::default()),
            topic_config_manager: Arc::new(TopicConfigManager::default()),
            broker_config: Arc::new(BrokerConfig::default()),
            message_store: Arc::new(Default::default()),
            store_host,
        }
    }
}

// RequestProcessor implementation
impl<MS: MessageStore + Send> RequestProcessor for SendMessageProcessor<MS> {
    fn process_request(
        &self,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> RemotingCommand {
        let request_code = RequestCode::from(request.code());
        info!("process_request: {:?}", request_code);
        let response = match request_code {
            RequestCode::ConsumerSendMsgBack => self.inner.consumer_send_msg_back(&ctx, &request),
            _ => {
                let mut request_header = parse_request_header(&request).unwrap();
                let mapping_context = self
                    .topic_queue_mapping_manager
                    .build_topic_queue_mapping_context(&request_header, true);
                let rewrite_result = self
                    .topic_queue_mapping_manager
                    .rewrite_request_for_static_topic(&request_header, &mapping_context);
                if let Some(rewrite_result) = rewrite_result {
                    return rewrite_result;
                }

                let send_message_context =
                    self.inner
                        .build_msg_context(&ctx, &mut request_header, &request);
                if request_header.batch.is_none() || !request_header.batch.unwrap() {
                    self.send_message(
                        &ctx,
                        &request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        |_, _| {},
                    )
                } else {
                    self.send_batch_message(
                        &ctx,
                        &request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        |_, _| {},
                    )
                }
            }
        };
        response.unwrap()
    }
}

#[allow(unused_variables)]
impl<MS: MessageStore + Send> SendMessageProcessor<MS> {
    pub fn new(
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        topic_config_manager: Arc<TopicConfigManager>,
        broker_config: Arc<BrokerConfig>,
        message_store: Arc<MS>,
    ) -> Self {
        let store_host = format!("{}:{}", broker_config.broker_ip1, broker_config.listen_port)
            .parse::<SocketAddr>()
            .unwrap();
        Self {
            inner: SendMessageProcessorInner {
                broker_config: broker_config.clone(),
            },
            topic_queue_mapping_manager,
            topic_config_manager,
            broker_config,
            message_store,
            store_host,
        }
    }

    fn send_batch_message<F>(
        &self,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> Option<RemotingCommand>
    where
        F: FnOnce(&SendMessageContext, &RemotingCommand),
    {
        Some(RemotingCommand::create_response_command())
    }

    fn send_message<F>(
        &self,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> Option<RemotingCommand>
    where
        F: FnOnce(&SendMessageContext, &RemotingCommand),
    {
        let mut response = self.pre_send(ctx.as_ref(), request.as_ref(), &request_header);
        if response.code() != -1 {
            return Some(response);
        }

        if request_header.topic.len() > i8::MAX as usize {
            return Some(
                response
                    .set_code(ResponseCode::MessageIllegal)
                    .set_remark(Some(format!(
                        "message topic length too long {}",
                        request_header.topic().len()
                    ))),
            );
        }

        if !request_header.topic.is_empty()
            && request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX)
        {
            return Some(
                response
                    .set_code(ResponseCode::MessageIllegal)
                    .set_remark(Some(format!(
                        "batch request does not support retry group  {}",
                        request_header.topic()
                    ))),
            );
        }

        let response_header = SendMessageResponseHeader::default();
        let mut topic_config = self
            .topic_config_manager
            .select_topic_config(request_header.topic().as_str())
            .unwrap();
        let mut queue_id = request_header.queue_id;
        if queue_id < 0 {
            queue_id = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }

        let mut message_ext_batch = MessageExtBatch::default();
        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .message
            .topic = request_header.topic().to_string();
        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .queue_id = queue_id;
        let mut ori_props =
            MessageDecoder::string_to_message_properties(request_header.properties.as_ref());
        if self.handle_retry_and_dlq(
            &request_header,
            &mut response,
            request,
            &message_ext_batch.message_ext_broker_inner.message_ext_inner,
            &mut topic_config,
            &mut ori_props,
        ) {
            return Some(response);
        }
        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .message
            .body
            .clone_from(request.body());
        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .message
            .flag = request_header.flag;

        let uniq_key = ori_props.get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        let uniq_key = if let Some(value) = uniq_key {
            if value.is_empty() {
                let uniq_key_inner = message_client_id_setter::create_uniq_id();
                ori_props.insert(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.to_string(),
                    uniq_key_inner.clone(),
                );
                uniq_key_inner
            } else {
                value.to_string()
            }
        } else {
            let uniq_key_inner = message_client_id_setter::create_uniq_id();
            ori_props.insert(
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.to_string(),
                uniq_key_inner.clone(),
            );
            uniq_key_inner
        };

        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .message
            .properties = ori_props;
        let cleanup_policy = CleanupPolicyUtils::get_delete_policy(Some(&topic_config));

        if cleanup_policy == CleanupPolicy::COMPACTION {
            if let Some(value) = message_ext_batch
                .message_ext_broker_inner
                .message_ext_inner
                .message
                .properties
                .get(MessageConst::PROPERTY_KEYS)
            {
                if value.trim().is_empty() {
                    return Some(
                        response
                            .set_code(ResponseCode::MessageIllegal)
                            .set_remark(Some("Required message key is missing".to_string())),
                    );
                }
            }
        }
        message_ext_batch.message_ext_broker_inner.tags_code =
            MessageExtBrokerInner::tags_string2tags_code(
                &topic_config.topic_filter_type,
                message_ext_batch
                    .get_tags()
                    .unwrap_or("".to_string())
                    .as_str(),
            ) as i64;

        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .born_timestamp = request_header.born_timestamp;
        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .born_host = ctx.remoting_address();

        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .store_host = self.store_host;

        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .reconsume_times = request_header.reconsume_times.unwrap_or(0);

        message_ext_batch
            .message_ext_broker_inner
            .message_ext_inner
            .message
            .properties
            .insert(
                MessageConst::PROPERTY_CLUSTER.to_string(),
                self.broker_config
                    .broker_identity
                    .broker_cluster_name
                    .clone(),
            );

        message_ext_batch.message_ext_broker_inner.properties_string =
            MessageDecoder::message_properties_to_string(
                &message_ext_batch
                    .message_ext_broker_inner
                    .message_ext_inner
                    .message
                    .properties,
            );

        let result = self
            .message_store
            .put_message(MessageExtBrokerInner::default());
        let put_message_result = Handle::current().block_on(result);
        Some(response)
    }

    fn handle_put_message_result(
        &self,
        put_message_result: PutMessageResult,
        response: RemotingCommand,
        request: &RemotingCommand,
        msg: &MessageExt,
        response_header: &mut SendMessageResponseHeader,
        send_message_context: SendMessageContext,
        ctx: ConnectionHandlerContext,
        queue_id_int: i32,
        begin_time_millis: i64,
        mapping_context: TopicQueueMappingContext,
        message_type: TopicMessageType,
    ) -> Option<RemotingCommand> {
        // Your implementation here
        let mut response = response;
        let mut send_ok = false;
        match put_message_result.put_message_status() {
            rocketmq_store::base::message_status_enum::PutMessageStatus::PutOk => {
                send_ok = true;
                response = response.set_code(RemotingSysResponseCode::Success);
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::FlushDiskTimeout => {
                send_ok = true;
                response = response.set_code(ResponseCode::FlushDiskTimeout);
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::FlushSlaveTimeout => {
                send_ok = true;
                response = response.set_code(ResponseCode::FlushSlaveTimeout);
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::SlaveNotAvailable =>{
                send_ok = true;
                response = response.set_code(ResponseCode::SlaveNotAvailable);
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::ServiceNotAvailable => {
                response = response.setCode(ResponseCode::ServiceNotAvailable).setRemark(Some("service not available now. It may be caused by one of the following reasons: the broker's disk is full %s, messages are put to the slave, message store has been shut down, etc.".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::CreateMappedFileFailed => {
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("create mapped file failed, server is busy or broken.".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::MessageIllegal |
            rocketmq_store::base::message_status_enum::PutMessageStatus::PropertiesSizeExceeded => {
                response = response.setCode(ResponseCode::MessageIllegal).setRemark(Some(format!("the message is illegal, maybe msg body or properties length not matched. msg body length limit {}B, msg properties length limit 32KB.",message_store_config.max_message_size)));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::OsPageCacheBusy =>{
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("[PC_SYNCHRONIZED]broker busy, start flow control for a while".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::UnknownError => {
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("UNKNOWN_ERROR".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::InSyncReplicasNotEnough => {
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("in-sync replicas not enough".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::LmqConsumeQueueNumExceeded => {
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("[LMQ_CONSUME_QUEUE_NUM_EXCEEDED]broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num, default limit 2w.".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::WheelTimerFlowControl => {
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("timer message is under flow control, max num limit is %d or the current value is greater than %d and less than %d, trigger random flow control".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::WheelTimerMsgIllegal => {
                response = response.setCode(ResponseCode::MessageIllegal).setRemark(Some("timer message illegal, the delay time should not be bigger than the max delay %dms; or if set del msg, the delay time should be bigger than the current time".to_string()));
            },
            rocketmq_store::base::message_status_enum::PutMessageStatus::WheelTimerNotEnable => {
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("accurate timer message is not enabled, timerWheelEnable is %s".to_string()));
            },
            _ => {
                response = response.setCode(RemotingSysResponseCode::SystemError).setRemark(Some("UNKNOWN_ERROR DEFAULT".to_string()));
            }
        }

        let ext_fields = request.ext_fields().unwrap_or(&HashMap::new());
        let owner = ext_fields.get(BrokerStatsManager::COMMERCIAL_OWNER);
        let auth_type = ext_fields.get(BrokerStatsManager::ACCOUNT_AUTH_TYPE);
        let owner_parent = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_PARENT);
        let owner_self = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_SELF);
        let commercial_size_per_msg = self.broker_config.commercial_size_per_msg;

        if send_ok {
            if TopicValidator::RMQ_SYS_SCHEDULE_TOPIC == msg.topic() {
                unimplemented!()
            }
        } else {
        }

        unimplemented!()
    }
    pub fn pre_send(
        &self,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
    ) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command();
        response.with_opaque(request.opaque());
        response.add_ext_field(
            MessageConst::PROPERTY_MSG_REGION,
            self.broker_config.region_id(),
        );
        response.add_ext_field(
            MessageConst::PROPERTY_TRACE_SWITCH,
            self.broker_config.trace_on.to_string(),
        );

        //todo java code to implement
        /*        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
            return response;
        }*/
        self.inner
            .msg_check(ctx, request, request_header, &mut response);
        response
    }

    fn handle_retry_and_dlq(
        &self,
        request_header: &SendMessageRequestHeader,
        response: &mut RemotingCommand,
        request: &RemotingCommand,
        msg: &MessageExt,
        topic_config: &mut rocketmq_common::common::config::TopicConfig,
        properties: &mut HashMap<String, String>,
    ) -> bool {
        unimplemented!()
    }
}
