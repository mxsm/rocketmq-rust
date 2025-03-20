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

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::TopicFilterType;
use rocketmq_common::utils::string_utils::StringUtils;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::transaction::operation_result::OperationResult;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;
use crate::transaction::transactional_message_service::TransactionalMessageService;

pub struct EndTransactionProcessor<TM, MS> {
    /*  message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,*/
    transactional_message_service: ArcMut<TM>,
    // message_store: ArcMut<MS>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<TM, MS> EndTransactionProcessor<TM, MS> {
    pub fn new(
        /*message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        transactional_message_service: ArcMut<TM>,
        message_store: ArcMut<MS>,*/
        transactional_message_service: ArcMut<TM>,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        Self {
            /*message_store_config,
            broker_config,*/
            transactional_message_service,
            /* message_store, */
            broker_runtime_inner,
        }
    }
}

impl<TM, MS> EndTransactionProcessor<TM, MS>
where
    TM: TransactionalMessageService,
    MS: MessageStore,
{
    pub async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_header = request
            .decode_command_custom_header::<EndTransactionRequestHeader>()
            .expect("EndTransactionRequestHeader decode failed");
        if BrokerRole::Slave == self.broker_runtime_inner.message_store_config().broker_role {
            warn!("Message store is slave mode, so end transaction is forbidden. ");
            return Some(RemotingCommand::create_response_command_with_code(
                ResponseCode::SlaveNotAvailable,
            ));
        }
        if request_header.from_transaction_check {
            match request_header.commit_or_rollback {
                MessageSysFlag::TRANSACTION_NOT_TYPE => return None,
                MessageSysFlag::TRANSACTION_COMMIT_TYPE
                | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => {
                    //nothing to do, can add some log
                }
                _ => return None,
            }
        } else {
            match request_header.commit_or_rollback {
                MessageSysFlag::TRANSACTION_NOT_TYPE => return None,
                MessageSysFlag::TRANSACTION_COMMIT_TYPE
                | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => {
                    //nothing to do, can add some log
                }
                _ => return None,
            }
        }

        let result = if MessageSysFlag::TRANSACTION_COMMIT_TYPE == request_header.commit_or_rollback
        {
            let result = self
                .transactional_message_service
                .commit_message(&request_header);
            if result.response_code == ResponseCode::Success {
                if self.reject_commit_or_rollback(
                    request_header.from_transaction_check,
                    result.prepare_message.as_ref().unwrap(),
                ) {
                    warn!(
                        "Message commit fail [producer end]. currentTimeMillis - bornTime > \
                         checkImmunityTime, msgId={},commitLogOffset={}, wait check",
                        request_header.msg_id, request_header.commit_log_offset
                    );
                    return Some(RemotingCommand::create_response_command_with_code(
                        ResponseCode::IllegalOperation,
                    ));
                }
                let res =
                    self.check_prepare_message(result.prepare_message.as_ref(), &request_header);
                if ResponseCode::from(res.code()) != ResponseCode::Success {
                    let mut msg_inner =
                        end_message_transaction(result.prepare_message.as_ref().unwrap());
                    msg_inner.message_ext_inner.sys_flag = MessageSysFlag::reset_transaction_value(
                        msg_inner.message_ext_inner.sys_flag,
                        request_header.commit_or_rollback,
                    );
                    msg_inner.message_ext_inner.queue_offset =
                        request_header.tran_state_table_offset as i64;
                    msg_inner.message_ext_inner.prepared_transaction_offset =
                        request_header.commit_log_offset as i64;
                    msg_inner.message_ext_inner.store_timestamp =
                        result.prepare_message.as_ref().unwrap().store_timestamp;
                    MessageAccessor::clear_property(
                        &mut msg_inner,
                        MessageConst::PROPERTY_TRANSACTION_PREPARED,
                    );
                    let send_result = self.send_final_message(msg_inner).await;
                    if ResponseCode::from(send_result.code()) == ResponseCode::Success {
                        let _ = self
                            .transactional_message_service
                            .delete_prepare_message(result.prepare_message.as_ref().unwrap())
                            .await;
                    }
                    return Some(send_result);
                }
                return Some(res);
            } else {
                OperationResult::default()
            }
        } else if MessageSysFlag::TRANSACTION_ROLLBACK_TYPE == request_header.commit_or_rollback {
            let result = self
                .transactional_message_service
                .rollback_message(&request_header);
            if result.response_code == ResponseCode::Success {
                if self.reject_commit_or_rollback(
                    request_header.from_transaction_check,
                    result.prepare_message.as_ref().unwrap(),
                ) {
                    warn!(
                        "Message commit fail [producer end]. currentTimeMillis - bornTime > \
                         checkImmunityTime, msgId={},commitLogOffset={}, wait check",
                        request_header.msg_id, request_header.commit_log_offset
                    );
                    return Some(RemotingCommand::create_response_command_with_code(
                        ResponseCode::IllegalOperation,
                    ));
                }
                let res =
                    self.check_prepare_message(result.prepare_message.as_ref(), &request_header);
                if ResponseCode::from(res.code()) == ResponseCode::Success {
                    let _ = self
                        .transactional_message_service
                        .delete_prepare_message(result.prepare_message.as_ref().unwrap())
                        .await;
                }
                return Some(res);
            }
            result
        } else {
            OperationResult::default()
        };

        Some(
            RemotingCommand::create_remoting_command(result.response_code)
                .set_remark_option(result.response_remark),
        )
    }

    pub fn reject_commit_or_rollback(
        &self,
        from_transaction_check: bool,
        message_ext: &MessageExt,
    ) -> bool {
        if from_transaction_check {
            return false;
        }
        let check_immunity_time_str = message_ext.get_user_property(
            &CheetahString::from_static_str(MessageConst::PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS),
        );
        if StringUtils::is_not_empty_ch_string(check_immunity_time_str.as_ref()) {
            let value_of_current_minus_born =
                get_current_millis() - (message_ext.born_timestamp as u64);
            let check_immunity_time = TransactionalMessageUtil::get_immunity_time(
                check_immunity_time_str.as_ref().unwrap(),
                self.broker_runtime_inner
                    .broker_config()
                    .transaction_timeout,
            );
            return value_of_current_minus_born > check_immunity_time;
        }
        false
    }

    fn check_prepare_message(
        &self,
        message_ext: Option<&MessageExt>,
        // params: &(String, i64, i64),
        request_header: &EndTransactionRequestHeader,
    ) -> RemotingCommand {
        let mut command = RemotingCommand::create_response_command();
        if let Some(message_ext) = message_ext {
            let pgroup_read = message_ext.get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_PRODUCER_GROUP,
            ));
            if pgroup_read.is_none() {
                command.set_code_mut(ResponseCode::SystemError);
                command.set_remark_mut("he producer group wrong");
                return command;
            }
            let pgroup = pgroup_read.unwrap();
            if pgroup != request_header.producer_group.as_str() {
                command.set_code_mut(ResponseCode::SystemError);
                command.set_remark_mut("The producer group wrong");
                return command;
            }
            if message_ext.queue_offset != request_header.tran_state_table_offset as i64 {
                command.set_code_mut(ResponseCode::SystemError);
                command.set_remark_mut("The transaction state table offset wrong");
                return command;
            }
            if message_ext.commit_log_offset != request_header.commit_log_offset as i64 {
                command.set_code_mut(ResponseCode::SystemError);
                command.set_remark_mut("The commit log offset wrong");
                return command;
            }
        } else {
            command.set_code_mut(ResponseCode::SystemError);
            command.set_remark_mut("Find prepared transaction message failed");
        }
        command
    }

    async fn send_final_message(&mut self, msg_inner: MessageExtBrokerInner) -> RemotingCommand {
        let put_message_result = self
            .broker_runtime_inner
            .message_store_mut()
            .as_mut()
            .unwrap()
            .put_message(msg_inner)
            .await;
        let mut response = RemotingCommand::create_response_command();
        match put_message_result.put_message_status() {
            PutMessageStatus::PutOk
            | PutMessageStatus::FlushDiskTimeout
            | PutMessageStatus::FlushSlaveTimeout
            | PutMessageStatus::SlaveNotAvailable => {}
            PutMessageStatus::ServiceNotAvailable => {
                response.set_code_mut(ResponseCode::ServiceNotAvailable);
                response.set_remark_mut("Service not available now. ");
            }
            PutMessageStatus::CreateMappedFileFailed => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut("Create mapped file failed.");
            }
            PutMessageStatus::MessageIllegal | PutMessageStatus::PropertiesSizeExceeded => {
                response.set_code_mut(ResponseCode::MessageIllegal);
                response.set_remark_mut(format!(
                    "The message is illegal, maybe msg body or properties length not matched. msg \
                     body length limit {}B, msg properties length limit 32KB.",
                    self.broker_runtime_inner
                        .message_store_config()
                        .max_message_size
                ));
            }
            PutMessageStatus::OsPageCacheBusy => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut("OS page cache busy, please try another machine");
            }
            PutMessageStatus::UnknownError => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut("Unknown error");
            }
            PutMessageStatus::InSyncReplicasNotEnough => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut("In sync replicas not enough");
            }
            PutMessageStatus::PutToRemoteBrokerFail => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut("Put to remote broker failed");
            }
            PutMessageStatus::LmqConsumeQueueNumExceeded => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut("UNKNOWN_ERROR DEFAULT");
            }
            PutMessageStatus::WheelTimerFlowControl => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut(format!(
                    "timer message is under flow control, max num limit is {} or the current \
                     value is greater than {} and less than {}, trigger random flow control",
                    self.broker_runtime_inner
                        .message_store_config()
                        .timer_congest_num_each_slot
                        * 2,
                    self.broker_runtime_inner
                        .message_store_config()
                        .timer_congest_num_each_slot,
                    self.broker_runtime_inner
                        .message_store_config()
                        .timer_congest_num_each_slot
                        * 2,
                ));
            }
            PutMessageStatus::WheelTimerMsgIllegal => {
                response.set_code_mut(ResponseCode::MessageIllegal);
                response.set_remark_mut(format!(
                    "timer message illegal, the delay time should not be bigger than the max \
                     delay {}ms; or if set del msg, the delay time should be bigger than the \
                     current time",
                    self.broker_runtime_inner
                        .message_store_config()
                        .timer_max_delay_sec
                        * 1000
                ));
            }
            PutMessageStatus::WheelTimerNotEnable => {
                response.set_code_mut(ResponseCode::SystemError);
                response.set_remark_mut(format!(
                    "accurate timer message is not enabled, timerWheelEnable is {}",
                    self.broker_runtime_inner
                        .message_store_config()
                        .timer_wheel_enable
                ));
            }
        }
        response
    }
}

fn end_message_transaction(msg_ext: &MessageExt) -> MessageExtBrokerInner {
    let mut msg_inner = MessageExtBrokerInner::default();
    msg_inner.set_topic(
        msg_ext
            .get_user_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_REAL_TOPIC,
            ))
            .unwrap_or_default(),
    );
    msg_inner.message_ext_inner.queue_id = msg_ext
        .get_user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_REAL_QUEUE_ID,
        ))
        .unwrap_or_default()
        .parse()
        .unwrap_or_default();
    if let Some(body) = msg_ext.get_body() {
        msg_inner.set_body(body.clone());
    }
    msg_inner.set_flag(msg_ext.get_flag());
    msg_inner.message_ext_inner.born_timestamp = msg_ext.born_timestamp;
    msg_inner.message_ext_inner.born_host = msg_ext.born_host;
    msg_inner.message_ext_inner.store_host = msg_ext.store_host;
    msg_inner.message_ext_inner.reconsume_times = msg_ext.reconsume_times;
    msg_inner.set_wait_store_msg_ok(false);
    if let Some(transaction_id) = msg_ext.get_user_property(&CheetahString::from_static_str(
        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    )) {
        msg_inner.set_transaction_id(transaction_id);
    }
    msg_inner.message_ext_inner.sys_flag = msg_ext.sys_flag;
    let topic_filter_type = if msg_inner.message_ext_inner.sys_flag
        & MessageSysFlag::MULTI_TAGS_FLAG
        == MessageSysFlag::MULTI_TAGS_FLAG
    {
        TopicFilterType::MultiTag
    } else {
        TopicFilterType::SingleTag
    };
    let tags_code_value = if let Some(tags) = msg_ext.get_tags() {
        MessageExtBrokerInner::tags_string2tags_code(&topic_filter_type, tags.as_str())
    } else {
        0
    };
    msg_inner.tags_code = tags_code_value;
    MessageAccessor::set_properties(&mut msg_inner, msg_ext.get_properties().clone());
    msg_inner.properties_string =
        message_decoder::message_properties_to_string(msg_ext.get_properties());
    MessageAccessor::clear_property(&mut msg_inner, MessageConst::PROPERTY_REAL_TOPIC);
    MessageAccessor::clear_property(&mut msg_inner, MessageConst::PROPERTY_REAL_QUEUE_ID);
    msg_inner
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_message_transaction_with_valid_message() {
        let msg_ext = MessageExt::default();
        let msg_inner = end_message_transaction(&msg_ext);
        assert_eq!(
            msg_inner.get_topic(),
            &msg_ext
                .get_user_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_REAL_TOPIC
                ))
                .unwrap_or_default()
        );
        assert_eq!(
            msg_inner.message_ext_inner.queue_id,
            msg_ext
                .get_user_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_REAL_QUEUE_ID
                ))
                .unwrap_or_default()
                .parse::<i32>()
                .unwrap_or_default()
        );
        assert_eq!(msg_inner.get_body(), msg_ext.get_body());
        assert_eq!(msg_inner.get_flag(), msg_ext.get_flag());
        assert_eq!(
            msg_inner.message_ext_inner.born_timestamp,
            msg_ext.born_timestamp
        );
        assert_eq!(msg_inner.message_ext_inner.born_host, msg_ext.born_host);
        assert_eq!(msg_inner.message_ext_inner.store_host, msg_ext.store_host);
        assert_eq!(
            msg_inner.message_ext_inner.reconsume_times,
            msg_ext.reconsume_times
        );
        assert!(msg_inner.is_wait_store_msg_ok());
        assert_eq!(
            msg_inner.get_transaction_id(),
            msg_ext
                .get_user_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                ))
                .as_ref()
        );
        assert_eq!(msg_inner.message_ext_inner.sys_flag, msg_ext.sys_flag);
        /*        assert_eq!(
            msg_inner.tags_code,
            MessageExtBrokerInner::tags_string2tags_code(
                &TopicFilterType::SingleTag,
                msg_ext.get_tags().as_ref().unwrap()
            )
        );*/
        assert_eq!(msg_inner.get_properties(), msg_ext.get_properties());
        assert_eq!(
            msg_inner.properties_string,
            message_decoder::message_properties_to_string(msg_ext.get_properties())
        );
    }

    #[test]
    fn end_message_transaction_with_empty_body() {
        let msg_ext = MessageExt::default();
        //msg_ext.set_body(None);
        let msg_inner = end_message_transaction(&msg_ext);
        assert!(!msg_inner.get_body().is_some_and(|b| b.is_empty()));
    }

    #[test]
    fn end_message_transaction_with_missing_properties() {
        let mut msg_ext = MessageExt::default();
        msg_ext.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
            CheetahString::empty(),
        );
        msg_ext.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID),
            CheetahString::empty(),
        );
        let msg_inner = end_message_transaction(&msg_ext);
        assert!(msg_inner.get_topic().is_empty());
        assert_eq!(msg_inner.message_ext_inner.queue_id, 0);
    }
}
