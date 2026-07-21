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

use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::TopicFilterType;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::error_response;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::metrics::broker_metrics_manager::BrokerMetricsManager;
use crate::transaction::operation_result::OperationResult;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;
use crate::transaction::transactional_message_service::TransactionalMessageService;

#[derive(Clone, Copy)]
pub(crate) struct EndTransactionPolicy {
    transaction_timeout: u64,
    max_message_size: i32,
    timer_congest_num_each_slot: usize,
    timer_max_delay_sec: u64,
    timer_wheel_enable: bool,
}

impl EndTransactionPolicy {
    pub(crate) fn from_configs(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig) -> Self {
        Self {
            transaction_timeout: broker_config.transaction_timeout,
            max_message_size: message_store_config.max_message_size,
            timer_congest_num_each_slot: message_store_config.timer_congest_num_each_slot,
            timer_max_delay_sec: message_store_config.timer_max_delay_sec,
            timer_wheel_enable: message_store_config.timer_wheel_enable,
        }
    }
}

pub(crate) struct EndTransactionStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> EndTransactionStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn is_slave(&self) -> Result<bool, MessageStoreUnavailable> {
        Ok(self
            .escape_bridge
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .is_message_store_slave())
    }

    async fn put_message(&self, message: MessageExtBrokerInner) -> Result<PutMessageResult, MessageStoreUnavailable> {
        self.escape_bridge
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .put_message_to_local_store(message)
            .await
    }
}

impl<MS: MessageStore> Clone for EndTransactionStoreCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            escape_bridge: Weak::clone(&self.escape_bridge),
        }
    }
}

pub(crate) struct EndTransactionProcessorContext<MS: MessageStore> {
    policy: EndTransactionPolicy,
    message_store: EndTransactionStoreCapability<MS>,
    broker_stats_manager: Arc<BrokerStatsManager>,
}

impl<MS: MessageStore> EndTransactionProcessorContext<MS> {
    pub(crate) fn new(
        policy: EndTransactionPolicy,
        message_store: EndTransactionStoreCapability<MS>,
        broker_stats_manager: Arc<BrokerStatsManager>,
    ) -> Self {
        Self {
            policy,
            message_store,
            broker_stats_manager,
        }
    }
}

impl<MS: MessageStore> Clone for EndTransactionProcessorContext<MS> {
    fn clone(&self) -> Self {
        Self {
            policy: self.policy,
            message_store: self.message_store.clone(),
            broker_stats_manager: Arc::clone(&self.broker_stats_manager),
        }
    }
}

pub struct EndTransactionProcessor<TM, MS: MessageStore> {
    transactional_message_service: Arc<TM>,
    context: EndTransactionProcessorContext<MS>,
}

impl<TM, MS: MessageStore> Clone for EndTransactionProcessor<TM, MS> {
    fn clone(&self) -> Self {
        Self {
            transactional_message_service: self.transactional_message_service.clone(),
            context: self.context.clone(),
        }
    }
}

impl<TM, MS> RequestProcessor for EndTransactionProcessor<TM, MS>
where
    TM: TransactionalMessageService,
    MS: MessageStore,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_mut(channel, ctx, request).await
    }
}

impl<TM, MS> EndTransactionProcessor<TM, MS>
where
    TM: TransactionalMessageService,
    MS: MessageStore,
{
    pub async fn process_request_shared(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut processor = self.clone();
        processor.process_request_mut(channel, ctx, request).await
    }

    async fn process_request_mut(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("EndTransactionProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::EndTransaction => self.process_request_inner(channel, ctx, request_code, request).await,
            _ => {
                warn!(
                    "EndTransactionProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = error_response::request_code_not_supported_with_remark_and_opaque(
                    request.code(),
                    format!("request code {} not supported", request.code()),
                    request.opaque(),
                );
                Ok(Some(response))
            }
        }
    }
}

impl<TM, MS: MessageStore> EndTransactionProcessor<TM, MS> {
    pub(crate) fn new(transactional_message_service: Arc<TM>, context: EndTransactionProcessorContext<MS>) -> Self {
        Self {
            transactional_message_service,
            context,
        }
    }
}

impl<TM, MS> EndTransactionProcessor<TM, MS>
where
    TM: TransactionalMessageService,
    MS: MessageStore,
{
    async fn process_request_inner(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<EndTransactionRequestHeader>()?;
        debug!("Transaction request: {:?}", request_header);

        match self.context.message_store.is_slave() {
            Ok(false) => {}
            Ok(true) => {
                warn!("Message store is slave mode, so end transaction is forbidden. ");
                return Ok(Some(RemotingCommand::create_response_command_with_code(
                    ResponseCode::SlaveNotAvailable,
                )));
            }
            Err(_) => {
                warn!("Message store provider is unavailable, so end transaction is forbidden. ");
                return Ok(Some(message_store_unavailable_response()));
            }
        }
        if request_header.from_transaction_check {
            match request_header.commit_or_rollback {
                MessageSysFlag::TRANSACTION_NOT_TYPE => {
                    warn!(
                        "Check producer transaction state, but it's pending status. RequestHeader: {:?}, Remark: {:?}",
                        request_header,
                        request.remark()
                    );
                    return Ok(None);
                }
                MessageSysFlag::TRANSACTION_COMMIT_TYPE => {
                    warn!(
                        "Check producer transaction state, the producer commit the message. RequestHeader: {:?}, \
                         Remark: {:?}",
                        request_header,
                        request.remark()
                    );
                }
                MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => {
                    warn!(
                        "Check producer transaction state, the producer rollback the message. RequestHeader: {:?}, \
                         Remark: {:?}",
                        request_header,
                        request.remark()
                    );
                }
                _ => return Ok(None),
            }
        } else {
            match request_header.commit_or_rollback {
                MessageSysFlag::TRANSACTION_NOT_TYPE => {
                    warn!(
                        "The producer end transaction in sending message, and it's pending status. RequestHeader: \
                         {:?}, Remark: {:?}",
                        request_header,
                        request.remark()
                    );
                    return Ok(None);
                }
                MessageSysFlag::TRANSACTION_COMMIT_TYPE => {
                    // Normal commit, no log needed
                }
                MessageSysFlag::TRANSACTION_ROLLBACK_TYPE => {
                    warn!(
                        "The producer end transaction in sending message, rollback the message. RequestHeader: {:?}, \
                         Remark: {:?}",
                        request_header,
                        request.remark()
                    );
                }
                _ => return Ok(None),
            }
        }

        let OperationResult {
            response_remark,
            response_code,
            ..
        } = if MessageSysFlag::TRANSACTION_COMMIT_TYPE == request_header.commit_or_rollback {
            let mut result = self.transactional_message_service.commit_message(&request_header).await;
            if result.response_code == ResponseCode::Success {
                if self.reject_commit_or_rollback(
                    request_header.from_transaction_check,
                    result.prepare_message.as_ref().unwrap(),
                ) {
                    warn!(
                        "Message commit fail [producer end]. currentTimeMillis - bornTime > checkImmunityTime, \
                         msgId={},commitLogOffset={}, wait check",
                        request_header.msg_id, request_header.commit_log_offset
                    );
                    return Ok(Some(RemotingCommand::create_response_command_with_code(
                        ResponseCode::IllegalOperation,
                    )));
                }
                let res = self.check_prepare_message(result.prepare_message.as_ref(), &request_header);
                if ResponseCode::from(res.code()) == ResponseCode::Success {
                    // Validation passed, send final message
                    let mut msg_inner = end_message_transaction(result.prepare_message.as_mut().unwrap());
                    msg_inner.message_ext_inner.sys_flag = MessageSysFlag::reset_transaction_value(
                        msg_inner.message_ext_inner.sys_flag,
                        request_header.commit_or_rollback,
                    );
                    msg_inner.message_ext_inner.queue_offset = request_header.tran_state_table_offset;
                    msg_inner.message_ext_inner.prepared_transaction_offset = request_header.commit_log_offset;
                    msg_inner.message_ext_inner.store_timestamp =
                        result.prepare_message.as_ref().unwrap().store_timestamp;
                    MessageAccessor::clear_property(&mut msg_inner, MessageConst::PROPERTY_TRANSACTION_PREPARED);

                    // Save topic and born_timestamp before sending (msg_inner is moved)
                    let topic = msg_inner.get_topic().clone();
                    let born_timestamp = result.prepare_message.as_ref().unwrap().born_timestamp as u64;

                    let send_result = self.send_final_message(msg_inner).await;
                    if ResponseCode::from(send_result.code()) == ResponseCode::Success {
                        let _ = self
                            .transactional_message_service
                            .delete_prepare_message(result.prepare_message.as_ref().unwrap())
                            .await;

                        // Record metrics for successful commit
                        if let Some(metrics) = BrokerMetricsManager::try_global() {
                            // Increment commit messages counter
                            metrics.inc_commit_messages(&topic, 1);

                            // Record transaction finish latency (in seconds)
                            let commit_latency_secs = (current_millis() - born_timestamp) / 1000;
                            metrics.record_transaction_finish_latency(&topic, commit_latency_secs);
                        }

                        // TODO: Update transaction metrics (half messages count -1) when
                        // TransactionMetrics is fully implemented
                        // self.transactional_message_service
                        //     .get_transaction_metrics()
                        //     .add_and_get(&topic, -1);
                    }
                    return Ok(Some(send_result));
                }
                // Validation failed, return error response
                return Ok(Some(res));
            } else {
                OperationResult::default()
            }
        } else if MessageSysFlag::TRANSACTION_ROLLBACK_TYPE == request_header.commit_or_rollback {
            let result = self
                .transactional_message_service
                .rollback_message(&request_header)
                .await;
            if result.response_code == ResponseCode::Success {
                if self.reject_commit_or_rollback(
                    request_header.from_transaction_check,
                    result.prepare_message.as_ref().unwrap(),
                ) {
                    warn!(
                        "Message commit fail [producer end]. currentTimeMillis - bornTime > checkImmunityTime, \
                         msgId={},commitLogOffset={}, wait check",
                        request_header.msg_id, request_header.commit_log_offset
                    );
                    return Ok(Some(RemotingCommand::create_response_command_with_code(
                        ResponseCode::IllegalOperation,
                    )));
                }
                let res = self.check_prepare_message(result.prepare_message.as_ref(), &request_header);
                if ResponseCode::from(res.code()) == ResponseCode::Success {
                    let _ = self
                        .transactional_message_service
                        .delete_prepare_message(result.prepare_message.as_ref().unwrap())
                        .await;

                    // Record metrics for successful rollback
                    if let Some(prepare_msg) = result.prepare_message.as_ref() {
                        let real_topic = prepare_msg
                            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                            .unwrap_or_default();

                        if let Some(metrics) = BrokerMetricsManager::try_global() {
                            // Increment rollback messages counter
                            metrics.inc_rollback_messages(&real_topic, 1);
                        }

                        // TODO: Update transaction metrics (half messages count -1) when
                        // TransactionMetrics is fully implemented
                        // self.transactional_message_service
                        //     .get_transaction_metrics()
                        //     .add_and_get(&real_topic, -1);
                    }
                }
                return Ok(Some(res));
            }
            result
        } else {
            OperationResult::default()
        };

        Ok(Some(
            RemotingCommand::create_remoting_command(response_code).set_remark_option(response_remark),
        ))
    }

    pub fn reject_commit_or_rollback(&self, from_transaction_check: bool, message_ext: &MessageExt) -> bool {
        if from_transaction_check {
            return false;
        }

        // The setting of MessageConst::PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS is configured in the
        // SendMessageActivity of the Proxy. Therefore, messages sent through the SDK will not have
        // this property.
        if let Some(check_immunity_time_str) = message_ext.user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
        )) {
            if !check_immunity_time_str.is_empty() {
                let value_of_current_minus_born = current_millis() - (message_ext.born_timestamp as u64);
                let check_immunity_time = TransactionalMessageUtil::get_immunity_time(
                    &check_immunity_time_str,
                    self.context.policy.transaction_timeout,
                );
                return value_of_current_minus_born > check_immunity_time;
            }
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
            let pgroup_read =
                message_ext.property(&CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP));
            match pgroup_read {
                Some(pgroup) if pgroup == request_header.producer_group.as_str() => {
                    // Producer group matches, continue validation
                }
                Some(_) => {
                    command.set_code_mut(ResponseCode::SystemError);
                    command.set_remark_mut("The producer group wrong");
                    return command;
                }
                None => {
                    command.set_code_mut(ResponseCode::SystemError);
                    command.set_remark_mut("The producer group wrong");
                    return command;
                }
            }
            if message_ext.queue_offset != request_header.tran_state_table_offset {
                command.set_code_mut(ResponseCode::SystemError);
                command.set_remark_mut("The transaction state table offset wrong");
                return command;
            }
            if message_ext.commit_log_offset != request_header.commit_log_offset {
                command.set_code_mut(ResponseCode::SystemError);
                command.set_remark_mut("The commit log offset wrong");
                return command;
            }
            // All validations passed
            command.set_code_mut(ResponseCode::Success);
        } else {
            command.set_code_mut(ResponseCode::SystemError);
            command.set_remark_mut("Find prepared transaction message failed");
        }
        command
    }

    async fn send_final_message(&mut self, msg_inner: MessageExtBrokerInner) -> RemotingCommand {
        // Save topic before moving msg_inner
        let topic = msg_inner.get_topic().clone();

        let put_message_result = match self.context.message_store.put_message(msg_inner).await {
            Ok(result) => result,
            Err(_) => return message_store_unavailable_response(),
        };

        build_put_message_response(
            &self.context.policy,
            self.context.broker_stats_manager.as_ref(),
            &topic,
            put_message_result,
        )
    }
}

fn message_store_unavailable_response() -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(ResponseCode::ServiceNotAvailable)
        .set_remark("Message store is unavailable now.")
}

fn build_put_message_response(
    policy: &EndTransactionPolicy,
    broker_stats_manager: &BrokerStatsManager,
    topic: &CheetahString,
    put_message_result: PutMessageResult,
) -> RemotingCommand {
    let mut response = RemotingCommand::create_response_command();
    match put_message_result.put_message_status() {
        PutMessageStatus::PutOk
        | PutMessageStatus::FlushDiskTimeout
        | PutMessageStatus::FlushSlaveTimeout
        | PutMessageStatus::SlaveNotAvailable => {
            // P2: Update BrokerStats for successful message put
            if let PutMessageStatus::PutOk = put_message_result.put_message_status() {
                if let Some(append_result) = put_message_result.append_message_result() {
                    broker_stats_manager.inc_topic_put_nums(topic, append_result.msg_num, 1);
                    broker_stats_manager.inc_topic_put_size(topic, append_result.wrote_bytes);
                    broker_stats_manager.inc_broker_put_nums(topic, append_result.msg_num);
                }
            }
        }
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
                "The message is illegal, maybe msg body or properties length not matched. msg body length limit {}B, \
                 msg properties length limit 32KB.",
                policy.max_message_size
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
                "timer message is under flow control, max num limit is {} or the current value is greater than {} and \
                 less than {}, trigger random flow control",
                policy.timer_congest_num_each_slot * 2,
                policy.timer_congest_num_each_slot,
                policy.timer_congest_num_each_slot * 2,
            ));
        }
        PutMessageStatus::WheelTimerMsgIllegal => {
            response.set_code_mut(ResponseCode::MessageIllegal);
            response.set_remark_mut(format!(
                "timer message illegal, the delay time should not be bigger than the max delay {}ms; or if set del \
                 msg, the delay time should be bigger than the current time",
                policy.timer_max_delay_sec * 1000
            ));
        }
        PutMessageStatus::WheelTimerNotEnable => {
            response.set_code_mut(ResponseCode::SystemError);
            response.set_remark_mut(format!(
                "accurate timer message is not enabled, timerWheelEnable is {}",
                policy.timer_wheel_enable
            ));
        }
    }
    response
}

fn end_message_transaction(msg_ext: &mut MessageExt) -> MessageExtBrokerInner {
    let mut msg_inner = MessageExtBrokerInner::default();
    msg_inner.set_topic(
        msg_ext
            .user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
            .unwrap_or_default(),
    );
    msg_inner.message_ext_inner.queue_id = msg_ext
        .user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID))
        .unwrap_or_default()
        .parse()
        .unwrap_or_default();
    if let Some(body) = msg_ext.take_body() {
        msg_inner.set_body(body);
    }
    msg_inner.set_flag(msg_ext.get_flag());
    msg_inner.message_ext_inner.born_timestamp = msg_ext.born_timestamp;
    msg_inner.message_ext_inner.born_host = msg_ext.born_host;
    msg_inner.message_ext_inner.store_host = msg_ext.store_host;
    msg_inner.message_ext_inner.reconsume_times = msg_ext.reconsume_times;
    msg_inner.set_wait_store_msg_ok(false);
    if let Some(transaction_id) = msg_ext.user_property(&CheetahString::from_static_str(
        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    )) {
        msg_inner.set_transaction_id(transaction_id);
    }
    msg_inner.message_ext_inner.sys_flag = msg_ext.sys_flag;
    let topic_filter_type =
        if msg_inner.message_ext_inner.sys_flag & MessageSysFlag::MULTI_TAGS_FLAG == MessageSysFlag::MULTI_TAGS_FLAG {
            TopicFilterType::MultiTag
        } else {
            TopicFilterType::SingleTag
        };
    let tags_code_value = if let Some(tags) = msg_ext.tags() {
        MessageExtBrokerInner::tags_string2tags_code(&topic_filter_type, tags.as_str())
    } else {
        0
    };
    msg_inner.tags_code = tags_code_value;
    MessageAccessor::set_properties(&mut msg_inner, msg_ext.get_properties().clone());
    msg_inner.properties_string = message_decoder::message_properties_to_string(msg_ext.get_properties());
    MessageAccessor::clear_property(&mut msg_inner, MessageConst::PROPERTY_REAL_TOPIC);
    MessageAccessor::clear_property(&mut msg_inner, MessageConst::PROPERTY_REAL_QUEUE_ID);
    msg_inner
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_store::message_store::GenericMessageStore;

    #[test]
    fn end_message_transaction_with_valid_message() {
        let mut msg_ext = MessageExt::default();
        let msg_inner = end_message_transaction(&mut msg_ext);
        assert_eq!(
            msg_inner.get_topic(),
            &msg_ext
                .user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                .unwrap_or_default()
        );
        assert_eq!(
            msg_inner.message_ext_inner.queue_id,
            msg_ext
                .user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID))
                .unwrap_or_default()
                .parse::<i32>()
                .unwrap_or_default()
        );
        assert_eq!(msg_inner.get_body(), msg_ext.get_body());
        assert_eq!(msg_inner.get_flag(), msg_ext.get_flag());
        assert_eq!(msg_inner.message_ext_inner.born_timestamp, msg_ext.born_timestamp);
        assert_eq!(msg_inner.message_ext_inner.born_host, msg_ext.born_host);
        assert_eq!(msg_inner.message_ext_inner.store_host, msg_ext.store_host);
        assert_eq!(msg_inner.message_ext_inner.reconsume_times, msg_ext.reconsume_times);
        assert!(msg_inner.is_wait_store_msg_ok());
        assert_eq!(
            msg_inner.get_transaction_id(),
            msg_ext
                .user_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                ))
                .as_ref()
        );
        assert_eq!(msg_inner.message_ext_inner.sys_flag, msg_ext.sys_flag);
        assert_eq!(msg_inner.get_properties(), msg_ext.get_properties());
        assert_eq!(
            msg_inner.properties_string,
            message_decoder::message_properties_to_string(msg_ext.get_properties())
        );
    }

    #[test]
    fn end_message_transaction_with_empty_body() {
        let mut msg_ext = MessageExt::default();
        let msg_inner = end_message_transaction(&mut msg_ext);
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
        let msg_inner = end_message_transaction(&mut msg_ext);
        assert!(msg_inner.get_topic().is_empty());
        assert_eq!(msg_inner.message_ext_inner.queue_id, 0);
    }

    #[test]
    fn end_transaction_policy_snapshots_response_configuration() {
        let broker_config = BrokerConfig {
            transaction_timeout: 12_345,
            ..Default::default()
        };
        let message_store_config = MessageStoreConfig {
            max_message_size: 4_321,
            timer_congest_num_each_slot: 77,
            timer_max_delay_sec: 88,
            timer_wheel_enable: false,
            ..Default::default()
        };

        let policy = EndTransactionPolicy::from_configs(&broker_config, &message_store_config);

        assert_eq!(policy.transaction_timeout, 12_345);
        assert_eq!(policy.max_message_size, 4_321);
        assert_eq!(policy.timer_congest_num_each_slot, 77);
        assert_eq!(policy.timer_max_delay_sec, 88);
        assert!(!policy.timer_wheel_enable);
    }

    #[tokio::test]
    async fn end_transaction_store_capability_fails_closed_after_provider_shutdown() {
        let capability = EndTransactionStoreCapability::<GenericMessageStore> {
            escape_bridge: Weak::new(),
        };

        assert!(capability.is_slave().is_err());
        assert!(capability.put_message(MessageExtBrokerInner::default()).await.is_err());

        let response = message_store_unavailable_response();
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::ServiceNotAvailable);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Message store is unavailable now.")
        );
    }

    #[test]
    fn end_transaction_put_error_responses_use_policy_snapshot() {
        let policy = EndTransactionPolicy {
            transaction_timeout: 1,
            max_message_size: 4_321,
            timer_congest_num_each_slot: 77,
            timer_max_delay_sec: 88,
            timer_wheel_enable: false,
        };
        let broker_stats_manager = BrokerStatsManager::new(Arc::new(BrokerConfig::default()));
        let topic = CheetahString::from_static_str("transaction-topic");

        let illegal = build_put_message_response(
            &policy,
            &broker_stats_manager,
            &topic,
            PutMessageResult::new_default(PutMessageStatus::MessageIllegal),
        );
        assert_eq!(ResponseCode::from(illegal.code()), ResponseCode::MessageIllegal);
        assert!(illegal.remark().is_some_and(|remark| remark.contains("4321B")));

        let timer_flow_control = build_put_message_response(
            &policy,
            &broker_stats_manager,
            &topic,
            PutMessageResult::new_default(PutMessageStatus::WheelTimerFlowControl),
        );
        assert_eq!(ResponseCode::from(timer_flow_control.code()), ResponseCode::SystemError);
        assert!(timer_flow_control
            .remark()
            .is_some_and(|remark| remark.contains("154") && remark.contains("77")));

        let timer_disabled = build_put_message_response(
            &policy,
            &broker_stats_manager,
            &topic,
            PutMessageResult::new_default(PutMessageStatus::WheelTimerNotEnable),
        );
        assert_eq!(ResponseCode::from(timer_disabled.code()), ResponseCode::SystemError);
        assert!(timer_disabled.remark().is_some_and(|remark| remark.contains("false")));
    }

    #[test]
    fn end_transaction_processor_source_uses_explicit_capabilities() {
        let source = include_str!("end_transaction_processor.rs");

        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(source.contains("Weak<EscapeBridge<MS>>"));
        assert!(source.contains("EndTransactionProcessorContext<MS>"));
    }
}
