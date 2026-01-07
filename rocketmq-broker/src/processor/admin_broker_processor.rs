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

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::processor::admin_broker_processor::batch_mq_handler::BatchMqHandler;
use crate::processor::admin_broker_processor::broker_config_request_handler::BrokerConfigRequestHandler;
use crate::processor::admin_broker_processor::broker_epoch_cache_handler::BrokerEpochCacheHandler;
use crate::processor::admin_broker_processor::consumer_request_handler::ConsumerRequestHandler;
use crate::processor::admin_broker_processor::message_related_handler::MessageRelatedHandler;
use crate::processor::admin_broker_processor::notify_broker_role_change_handler::NotifyBrokerRoleChangeHandler;
use crate::processor::admin_broker_processor::notify_min_broker_id_handler::NotifyMinBrokerChangeIdHandler;
use crate::processor::admin_broker_processor::offset_request_handler::OffsetRequestHandler;
use crate::processor::admin_broker_processor::producer_request_handler::ProducerRequestHandler;
use crate::processor::admin_broker_processor::reset_master_flusg_offset_handler::ResetMasterFlushOffsetHandler;
use crate::processor::admin_broker_processor::subscription_group_handler::SubscriptionGroupHandler;
use crate::processor::admin_broker_processor::topic_request_handler::TopicRequestHandler;
use crate::processor::admin_broker_processor::update_broker_ha_handler::UpdateBrokerHaHandler;

mod batch_mq_handler;
mod broker_config_request_handler;
mod broker_epoch_cache_handler;
mod consumer_request_handler;
mod message_related_handler;
mod notify_broker_role_change_handler;
mod notify_min_broker_id_handler;
mod offset_request_handler;
mod producer_request_handler;
mod reset_master_flusg_offset_handler;
mod subscription_group_handler;
mod topic_request_handler;
mod update_broker_ha_handler;

pub struct AdminBrokerProcessor<MS: MessageStore> {
    topic_request_handler: TopicRequestHandler<MS>,
    broker_config_request_handler: BrokerConfigRequestHandler<MS>,
    consumer_request_handler: ConsumerRequestHandler<MS>,
    offset_request_handler: OffsetRequestHandler<MS>,
    batch_mq_handler: BatchMqHandler<MS>,
    subscription_group_handler: SubscriptionGroupHandler<MS>,

    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,

    notify_min_broker_handler: NotifyMinBrokerChangeIdHandler<MS>,
    update_broker_ha_handler: UpdateBrokerHaHandler<MS>,
    reset_master_flusg_offset_handler: ResetMasterFlushOffsetHandler<MS>,
    broker_epoch_cache_handler: BrokerEpochCacheHandler<MS>,
    notify_broker_role_change_handler: NotifyBrokerRoleChangeHandler<MS>,
    message_related_handler: MessageRelatedHandler<MS>,
    producer_request_handler: ProducerRequestHandler<MS>,
}

impl<MS> RequestProcessor for AdminBrokerProcessor<MS>
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
        self.process_request_inner(channel, ctx, request_code, request).await
    }
}

impl<MS: MessageStore> AdminBrokerProcessor<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let topic_request_handler = TopicRequestHandler::new(broker_runtime_inner.clone());
        let broker_config_request_handler = BrokerConfigRequestHandler::new(broker_runtime_inner.clone());
        let consumer_request_handler = ConsumerRequestHandler::new(broker_runtime_inner.clone());
        let offset_request_handler = OffsetRequestHandler::new(broker_runtime_inner.clone());
        let batch_mq_handler = BatchMqHandler::new(broker_runtime_inner.clone());
        let subscription_group_handler = SubscriptionGroupHandler::new(broker_runtime_inner.clone());

        let notify_min_broker_handler = NotifyMinBrokerChangeIdHandler::new(broker_runtime_inner.clone());

        let update_broker_ha_handler = UpdateBrokerHaHandler::new(broker_runtime_inner.clone());

        let reset_master_flusg_offset_handler = ResetMasterFlushOffsetHandler::new(broker_runtime_inner.clone());

        let broker_epoch_cache_handler = BrokerEpochCacheHandler::new(broker_runtime_inner.clone());

        let notify_broker_role_change_handler = NotifyBrokerRoleChangeHandler::new(broker_runtime_inner.clone());

        let message_related_handler = MessageRelatedHandler::new(broker_runtime_inner.clone());
        let producer_request_handler = ProducerRequestHandler::new(broker_runtime_inner.clone());
        AdminBrokerProcessor {
            topic_request_handler,
            broker_config_request_handler,
            consumer_request_handler,
            offset_request_handler,
            batch_mq_handler,
            subscription_group_handler,
            broker_runtime_inner,
            notify_min_broker_handler,
            update_broker_ha_handler,
            reset_master_flusg_offset_handler,
            broker_epoch_cache_handler,
            notify_broker_role_change_handler,
            message_related_handler,
            producer_request_handler,
        }
    }
}

impl<MS: MessageStore> AdminBrokerProcessor<MS> {
    async fn process_request_inner(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::UpdateAndCreateTopic => {
                self.topic_request_handler
                    .update_and_create_topic(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateAndCreateTopicList => {
                self.topic_request_handler
                    .update_and_create_topic_list(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::DeleteTopicInBroker => {
                self.topic_request_handler
                    .delete_topic(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetAllTopicConfig => {
                self.topic_request_handler
                    .get_all_topic_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetTimerCheckPoint => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetTimerMetrics => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::UpdateBrokerConfig => {
                self.broker_config_request_handler
                    .update_broker_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetBrokerConfig => {
                self.broker_config_request_handler
                    .get_broker_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateColdDataFlowCtrConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::RemoveColdDataFlowCtrConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetColdDataFlowCtrInfo => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::SetCommitlogReadMode => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::SearchOffsetByTimestamp => {
                self.message_related_handler
                    .search_offset_by_timestamp(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetMaxOffset => {
                self.offset_request_handler
                    .get_max_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetMinOffset => {
                self.offset_request_handler
                    .get_min_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetEarliestMsgStoreTime => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetBrokerRuntimeInfo => {
                self.broker_config_request_handler
                    .get_broker_runtime_info(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::LockBatchMq => {
                self.batch_mq_handler
                    .lock_natch_mq(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UnlockBatchMq => {
                self.batch_mq_handler
                    .unlock_batch_mq(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateAndCreateSubscriptionGroup => {
                self.subscription_group_handler
                    .update_and_create_subscription_group(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateAndCreateSubscriptionGroupList => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetAllSubscriptionGroupConfig => {
                self.offset_request_handler
                    .get_all_subscription_group_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::DeleteSubscriptionGroup => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetTopicStatsInfo => {
                self.topic_request_handler
                    .get_topic_stats_info(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetConsumerConnectionList => {
                self.consumer_request_handler
                    .get_consumer_connection_list(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetProducerConnectionList => {
                self.producer_request_handler
                    .get_producer_connection_list(ctx, request)
                    .await
            }
            RequestCode::GetAllProducerInfo => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetConsumeStats => {
                self.consumer_request_handler
                    .get_consume_stats(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetAllConsumerOffset => {
                self.consumer_request_handler
                    .get_all_consumer_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetAllDelayOffset => {
                self.offset_request_handler
                    .get_all_delay_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetAllMessageRequestMode => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::InvokeBrokerToResetOffset => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::InvokeBrokerToGetConsumerStatus => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::QueryTopicConsumeByWho => {
                self.topic_request_handler
                    .query_topic_consume_by_who(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::QueryTopicsByConsumer => {
                self.topic_request_handler
                    .query_topics_by_consumer(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::QuerySubscriptionByConsumer => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::QueryConsumeTimeSpan => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetSystemTopicListFromBroker => {
                self.topic_request_handler
                    .get_system_topic_list_from_broker(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::CleanExpiredConsumequeue => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::DeleteExpiredCommitlog => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::CleanUnusedTopic => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetConsumerRunningInfo => {
                self.consumer_request_handler
                    .get_consumer_running_info(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::QueryCorrectionOffset => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::ConsumeMessageDirectly => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::CloneGroupOffset => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::ViewBrokerStatsData => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetBrokerConsumeStats => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::QueryConsumeQueue => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::CheckRocksdbCqWriteProgress => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::UpdateAndGetGroupForbidden => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetSubscriptionGroupConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::UpdateAndCreateAclConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::DeleteAclConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetBrokerClusterAclInfo => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::UpdateGlobalWhiteAddrsConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::ResumeCheckHalfMessage => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetTopicConfig => {
                self.topic_request_handler
                    .get_topic_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateAndCreateStaticTopic => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::NotifyMinBrokerIdChange => {
                self.notify_min_broker_handler
                    .notify_min_broker_id_change(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::ExchangeBrokerHaInfo => {
                self.update_broker_ha_handler
                    .update_broker_ha_info(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetBrokerHaStatus => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::ResetMasterFlushOffset => {
                self.reset_master_flusg_offset_handler
                    .reset_master_flush_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetBrokerEpochCache => {
                self.broker_epoch_cache_handler
                    .get_broker_epoch_cache(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::NotifyBrokerRoleChanged => {
                self.notify_broker_role_change_handler
                    .notify_broker_role_changed(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthCreateUser => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthUpdateUser => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthDeleteUser => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthGetUser => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthListUser => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthCreateAcl => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthUpdateAcl => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthDeleteAcl => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthGetAcl => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::AuthListAcl => Ok(get_unknown_cmd_response(request_code)),
            _ => Ok(get_unknown_cmd_response(request_code)),
        }
    }
}

fn get_unknown_cmd_response(request_code: RequestCode) -> Option<RemotingCommand> {
    warn!(
        "request type {:?}-{} not supported",
        request_code,
        request_code.to_i32()
    );
    Some(RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::RequestCodeNotSupported,
        format!(" request type {} not supported", request_code.to_i32()),
    ))
}
