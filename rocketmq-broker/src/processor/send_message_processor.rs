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
        attribute::cleanup_policy::CleanupPolicy,
        broker::broker_config::BrokerConfig,
        message::{
            message_client_id_setter,
            message_single::{MessageExt, MessageExtBrokerInner},
            MessageConst,
        },
        mix_all::RETRY_GROUP_TOPIC_PREFIX,
        topic::TopicValidator,
    },
    utils::{message_utils, util_all},
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
    runtime::server::ConnectionHandlerContext,
};
use rocketmq_store::{
    base::message_result::PutMessageResult, log_file::MessageStore,
    status::manager::broker_stats_manager::BrokerStatsManager,
};
use tracing::debug;

use crate::{
    mqtrace::send_message_context::SendMessageContext,
    processor::SendMessageProcessorInner,
    topic::manager::{
        topic_config_manager::TopicConfigManager,
        topic_queue_mapping_manager::TopicQueueMappingManager,
    },
};

pub struct SendMessageProcessor<MS>
where
    MS: Clone,
{
    inner: SendMessageProcessorInner,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    broker_config: Arc<BrokerConfig>,
    message_store: MS,
    store_host: SocketAddr,
}

impl<MS: Clone> Clone for SendMessageProcessor<MS> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            topic_queue_mapping_manager: self.topic_queue_mapping_manager.clone(),
            broker_config: self.broker_config.clone(),
            message_store: self.message_store.clone(),
            store_host: self.store_host,
        }
    }
}

// RequestProcessor implementation
impl<MS: MessageStore + Send> SendMessageProcessor<MS> {
    pub fn has_send_message_hook(&self) -> bool {
        self.inner.send_message_hook_vec.read().is_empty()
    }

    fn clear_reserved_properties(request_header: &mut SendMessageRequestHeader) {
        let properties = request_header.properties.clone();
        if let Some(value) = properties {
            let delete_properties =
                message_utils::delete_property(value.as_str(), MessageConst::PROPERTY_POP_CK);
            request_header.properties = Some(delete_properties);
        }
    }

    pub async fn process_request(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::ConsumerSendMsgBack => self.inner.consumer_send_msg_back(&ctx, &request),
            _ => {
                let mut request_header = parse_request_header(&request).unwrap();
                let mapping_context = self
                    .topic_queue_mapping_manager
                    .build_topic_queue_mapping_context(&request_header, true);
                let rewrite_result = TopicQueueMappingManager::rewrite_request_for_static_topic(
                    &mut request_header,
                    &mapping_context,
                );
                if let Some(rewrite_result) = rewrite_result {
                    return Some(rewrite_result);
                }

                let send_message_context =
                    self.inner
                        .build_msg_context(&ctx, &mut request_header, &request);
                self.inner
                    .execute_send_message_hook_before(&send_message_context);
                SendMessageProcessor::<MS>::clear_reserved_properties(&mut request_header);
                let inner = self.inner.clone();
                let execute_send_message_hook_after =
                    |ctx: &mut SendMessageContext, cmd: &mut RemotingCommand| {
                        inner.execute_send_message_hook_after(Some(cmd), ctx)
                    };
                if request_header.batch.is_none() || !request_header.batch.unwrap() {
                    self.send_message(
                        &ctx,
                        request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        execute_send_message_hook_after,
                    )
                    .await
                } else {
                    self.send_batch_message(
                        &ctx,
                        request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        execute_send_message_hook_after,
                    )
                    .await
                }
            }
        }
    }
}

#[allow(unused_variables)]
impl<MS: MessageStore> SendMessageProcessor<MS> {
    pub fn new(
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        topic_config_manager: TopicConfigManager,
        broker_config: Arc<BrokerConfig>,
        message_store: &MS,
    ) -> Self {
        let store_host = format!("{}:{}", broker_config.broker_ip1, broker_config.listen_port)
            .parse::<SocketAddr>()
            .unwrap();
        Self {
            inner: SendMessageProcessorInner {
                broker_config: broker_config.clone(),
                topic_config_manager,
                send_message_hook_vec: Arc::new(parking_lot::RwLock::new(Vec::new())),
            },
            topic_queue_mapping_manager,
            broker_config,
            message_store: message_store.clone(),
            store_host,
        }
    }

    async fn send_batch_message<F>(
        &mut self,
        ctx: &ConnectionHandlerContext<'_>,
        request: RemotingCommand,
        send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> Option<RemotingCommand>
    where
        F: Fn(&mut SendMessageContext, &mut RemotingCommand),
    {
        Some(RemotingCommand::create_response_command())
    }

    async fn send_message<F>(
        &mut self,
        ctx: &ConnectionHandlerContext<'_>,
        request: RemotingCommand,
        send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> Option<RemotingCommand>
    where
        F: Fn(&mut SendMessageContext, &mut RemotingCommand),
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

        let mut topic_config = self
            .inner
            .topic_config_manager
            .select_topic_config(request_header.topic().as_str())
            .unwrap();
        let mut queue_id = request_header.queue_id;
        if queue_id.is_none() || queue_id.unwrap() < 0 {
            queue_id = Some(self.inner.random_queue_id(topic_config.write_queue_nums) as i32);
        }

        let mut message_ext = MessageExtBrokerInner::default();
        message_ext.message_ext_inner.message.topic = request_header.topic().to_string();
        message_ext.message_ext_inner.queue_id = *queue_id.as_ref().unwrap();
        let mut ori_props =
            MessageDecoder::string_to_message_properties(request_header.properties.as_ref());
        if !self.handle_retry_and_dlq(
            &request_header,
            &mut response,
            &request,
            &message_ext.message_ext_inner,
            &mut topic_config,
            &mut ori_props,
        ) {
            return Some(response);
        }
        message_ext
            .message_ext_inner
            .message
            .body
            .clone_from(request.body());
        message_ext.message_ext_inner.message.flag = request_header.flag;

        let uniq_key = ori_props.get_mut(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if let Some(uniq_key_inner) = uniq_key {
            if uniq_key_inner.is_empty() {
                let uniq_key_inner = message_client_id_setter::create_uniq_id();
                ori_props.insert(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.to_string(),
                    uniq_key_inner,
                );
            }
        }
        let tra_flag = ori_props
            .get(MessageConst::PROPERTY_TRANSACTION_PREPARED)
            .cloned();
        message_ext.message_ext_inner.message.properties = ori_props;
        let cleanup_policy = CleanupPolicyUtils::get_delete_policy(Some(&topic_config));

        if cleanup_policy == CleanupPolicy::COMPACTION {
            if let Some(value) = message_ext
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
        message_ext.tags_code = MessageExtBrokerInner::tags_string2tags_code(
            &topic_config.topic_filter_type,
            message_ext.get_tags().unwrap_or("".to_string()).as_str(),
        );

        message_ext.message_ext_inner.born_timestamp = request_header.born_timestamp;
        message_ext.message_ext_inner.born_host = ctx.remoting_address();
        message_ext.message_ext_inner.store_host = self.store_host;
        message_ext.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);

        message_ext.message_ext_inner.message.properties.insert(
            MessageConst::PROPERTY_CLUSTER.to_string(),
            self.broker_config
                .broker_identity
                .broker_cluster_name
                .clone(),
        );

        message_ext.properties_string = MessageDecoder::message_properties_to_string(
            &message_ext.message_ext_inner.message.properties,
        );
        let tra_flag = tra_flag.map_or(false, |tra_flag_inner| {
            tra_flag_inner.parse().unwrap_or(false)
        });

        let send_transaction_prepare_message = if tra_flag
            && !(message_ext.reconsume_times() > 0
                && message_ext.message_ext_inner.message.get_delay_time_level() > 0)
        {
            if self.broker_config.reject_transaction_message {
                return Some(
                    response
                        .set_code(ResponseCode::NoPermission)
                        .set_remark(Some(format!(
                            "the broker[{}] sending transaction message is forbidden",
                            self.broker_config.broker_ip1
                        ))),
                );
            }
            true
        } else {
            false
        };

        if self.broker_config.async_send_enable {
            let topic = message_ext.topic().to_string();
            let put_message_handle = if send_transaction_prepare_message {
                unimplemented!()
            } else {
                let mut message_store = self.message_store.clone();
                tokio::spawn(async move { message_store.put_message(message_ext).await })
            };
            let put_message_result = put_message_handle.await.unwrap();
            self.handle_put_message_result(
                put_message_result,
                response,
                &request,
                topic.as_str(),
                queue_id.unwrap(),
            )
        } else {
            let topic = message_ext.topic().to_string();
            let put_message_result = self.message_store.put_message(message_ext).await;
            self.handle_put_message_result(
                put_message_result,
                response,
                &request,
                topic.as_str(),
                *queue_id.as_ref().unwrap(),
            )
        }
    }

    fn handle_put_message_result(
        &self,
        put_message_result: PutMessageResult,
        response: RemotingCommand,
        request: &RemotingCommand,
        topic: &str,
        //  send_message_context: &mut SendMessageContext,
        // ctx: ConnectionHandlerContext,
        queue_id_int: i32,
        //begin_time_millis: i64,
        // mapping_context: TopicQueueMappingContext,
        //message_type: TopicMessageType,
    ) -> Option<RemotingCommand> {
        let mut response_header = SendMessageResponseHeader::default();
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
                   response = response.set_code(ResponseCode::ServiceNotAvailable).set_remark(Some("service not available now. It may be caused by one of the following reasons: the broker's disk is full %s, messages are put to the slave, message store has been shut down, etc.".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::CreateMappedFileFailed => {
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("create mapped file failed, server is busy or broken.".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::MessageIllegal |
               rocketmq_store::base::message_status_enum::PutMessageStatus::PropertiesSizeExceeded => {
                   response = response.set_code(ResponseCode::MessageIllegal).set_remark(Some("the message is illegal, maybe msg body or properties length not matched. msg body length limit B, msg properties length limit 32KB.".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::OsPageCacheBusy =>{
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("[PC_SYNCHRONIZED]broker busy, start flow control for a while".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::UnknownError => {
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("UNKNOWN_ERROR".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::InSyncReplicasNotEnough => {
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("in-sync replicas not enough".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::LmqConsumeQueueNumExceeded => {
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("[LMQ_CONSUME_QUEUE_NUM_EXCEEDED]broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num, default limit 2w.".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::WheelTimerFlowControl => {
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("timer message is under flow control, max num limit is %d or the current value is greater than %d and less than %d, trigger random flow control".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::WheelTimerMsgIllegal => {
                   response = response.set_code(ResponseCode::MessageIllegal).set_remark(Some("timer message illegal, the delay time should not be bigger than the max delay %dms; or if set del msg, the delay time should be bigger than the current time".to_string()));
               },
               rocketmq_store::base::message_status_enum::PutMessageStatus::WheelTimerNotEnable => {
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("accurate timer message is not enabled, timerWheelEnable is %s".to_string()));
               },
               _ => {
                   response = response.set_code(RemotingSysResponseCode::SystemError).set_remark(Some("UNKNOWN_ERROR DEFAULT".to_string()));
               }
           }

        let binding = HashMap::new();
        let ext_fields = request.ext_fields().unwrap_or(&binding);
        let owner = ext_fields.get(BrokerStatsManager::COMMERCIAL_OWNER);
        let auth_type = ext_fields.get(BrokerStatsManager::ACCOUNT_AUTH_TYPE);
        let owner_parent = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_PARENT);
        let owner_self = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_SELF);
        let commercial_size_per_msg = self.broker_config.commercial_size_per_msg;

        if send_ok {
            if TopicValidator::RMQ_SYS_SCHEDULE_TOPIC == topic {
                unimplemented!()
            }
            response_header.set_msg_id(
                put_message_result
                    .append_message_result()
                    .unwrap()
                    .msg_id
                    .clone(),
            );
            response_header.set_queue_id(queue_id_int);
            response_header.set_queue_offset(
                put_message_result
                    .append_message_result()
                    .unwrap()
                    .logics_offset,
            );
            //response_header.set_transaction_id();
            response = response.set_command_custom_header(response_header);
            Some(response)
        } else {
            unimplemented!()
        }
    }
    pub fn pre_send(
        &mut self,
        ctx: &ConnectionHandlerContext<'_>,
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
        debug!("Receive SendMessage request command: {:?}", request_header);
        let start_timestamp = self.broker_config.start_accept_send_request_time_stamp;
        if self.message_store.now() < (start_timestamp as u64) {
            response = response
                .set_code(RemotingSysResponseCode::SystemError)
                .set_remark(Some(format!(
                    "broker unable to service, until {}",
                    util_all::time_millis_to_human_string2(start_timestamp)
                )));
            return response;
        }
        response = response.set_code(-1);
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
        true
    }
}
