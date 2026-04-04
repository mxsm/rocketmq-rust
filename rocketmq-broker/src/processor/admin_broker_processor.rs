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

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker_runtime::BrokerRuntimeInner;
use crate::processor::admin_broker_processor::batch_mq_handler::BatchMqHandler;
use crate::processor::admin_broker_processor::broker_config_request_handler::BrokerConfigRequestHandler;
use crate::processor::admin_broker_processor::broker_epoch_cache_handler::BrokerEpochCacheHandler;
use crate::processor::admin_broker_processor::broker_stats_handler::BrokerStatsHandler;
use crate::processor::admin_broker_processor::consumer_request_handler::ConsumerRequestHandler;
use crate::processor::admin_broker_processor::create_acl_request_handler::CreateAclRequestHandler;
use crate::processor::admin_broker_processor::create_user_request_handler::CreateUserRequestHandler;
use crate::processor::admin_broker_processor::delete_acl_request_handler::DeleteAclRequestHandler;
use crate::processor::admin_broker_processor::delete_user_request_handler::DeleteUserRequestHandler;
use crate::processor::admin_broker_processor::get_acl_request_handler::GetAclRequestHandler;
use crate::processor::admin_broker_processor::get_broker_ha_status_handler::GetBrokerHaStatusHandler;
use crate::processor::admin_broker_processor::get_user_request_handler::GetUserRequestHandler;
use crate::processor::admin_broker_processor::list_acl_request_handler::ListAclRequestHandler;
use crate::processor::admin_broker_processor::list_users_request_handler::ListUsersRequestHandler;
use crate::processor::admin_broker_processor::message_related_handler::MessageRelatedHandler;
use crate::processor::admin_broker_processor::notify_broker_role_change_handler::NotifyBrokerRoleChangeHandler;
use crate::processor::admin_broker_processor::notify_min_broker_id_handler::NotifyMinBrokerChangeIdHandler;
use crate::processor::admin_broker_processor::offset_request_handler::OffsetRequestHandler;
use crate::processor::admin_broker_processor::producer_request_handler::ProducerRequestHandler;
use crate::processor::admin_broker_processor::reset_master_flusg_offset_handler::ResetMasterFlushOffsetHandler;
use crate::processor::admin_broker_processor::subscription_group_handler::SubscriptionGroupHandler;
use crate::processor::admin_broker_processor::topic_request_handler::TopicRequestHandler;
use crate::processor::admin_broker_processor::update_acl_request_handler::UpdateAclRequestHandler;
use crate::processor::admin_broker_processor::update_broker_ha_handler::UpdateBrokerHaHandler;
use crate::processor::admin_broker_processor::update_cold_data_flow_ctr_group_config::UpdateColdDataFlowCtrGroupConfigRequestHandler;
use crate::processor::admin_broker_processor::update_user_request_handler::UpdateUserRequestHandler;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use std::sync::Arc;
use tracing::warn;

mod batch_mq_handler;
mod broker_config_request_handler;
mod broker_epoch_cache_handler;
mod broker_stats_handler;
mod consumer_request_handler;
mod create_acl_request_handler;
mod create_user_request_handler;
mod delete_acl_request_handler;
mod delete_user_request_handler;
mod get_acl_request_handler;
mod get_broker_ha_status_handler;
mod get_user_request_handler;
mod list_acl_request_handler;
mod list_users_request_handler;
mod message_related_handler;
mod notify_broker_role_change_handler;
mod notify_min_broker_id_handler;
mod offset_request_handler;
mod producer_request_handler;
mod reset_master_flusg_offset_handler;
mod subscription_group_handler;
mod topic_request_handler;
mod update_acl_request_handler;
mod update_broker_ha_handler;
mod update_cold_data_flow_ctr_group_config;
mod update_user_request_handler;

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
    create_acl_request_handler: CreateAclRequestHandler<MS>,
    create_user_request_handler: CreateUserRequestHandler<MS>,
    get_acl_request_handler: GetAclRequestHandler<MS>,
    update_user_request_handler: UpdateUserRequestHandler<MS>,
    update_acl_request_handler: UpdateAclRequestHandler<MS>,
    delete_user_request_handler: DeleteUserRequestHandler<MS>,
    list_users_request_handler: ListUsersRequestHandler<MS>,
    get_user_request_handler: GetUserRequestHandler<MS>,
    delete_acl_request_handler: DeleteAclRequestHandler<MS>,
    list_acl_request_handler: ListAclRequestHandler<MS>,
    update_cold_data_flow_ctr_group_config_request_handler: UpdateColdDataFlowCtrGroupConfigRequestHandler<MS>,
    get_broker_ha_status_handler: GetBrokerHaStatusHandler<MS>,
    broker_stats_handler: BrokerStatsHandler<MS>,
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
    pub fn new(
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
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
        let create_acl_request_handler =
            CreateAclRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let create_user_request_handler =
            CreateUserRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let get_acl_request_handler =
            GetAclRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let update_user_request_handler =
            UpdateUserRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let update_acl_request_handler =
            UpdateAclRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let delete_user_request_handler =
            DeleteUserRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let list_users_request_handler =
            ListUsersRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let get_user_request_handler =
            GetUserRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let delete_acl_request_handler =
            DeleteAclRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service.clone());
        let list_acl_request_handler = ListAclRequestHandler::new(broker_runtime_inner.clone(), auth_admin_service);
        let update_cold_data_flow_ctr_group_config_request_handler =
            UpdateColdDataFlowCtrGroupConfigRequestHandler::new(broker_runtime_inner.clone());
        let get_broker_ha_status_handler = GetBrokerHaStatusHandler::new(broker_runtime_inner.clone());
        let broker_stats_handler = BrokerStatsHandler::new(broker_runtime_inner.clone());

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
            create_acl_request_handler,
            create_user_request_handler,
            get_acl_request_handler,
            update_user_request_handler,
            update_acl_request_handler,
            delete_user_request_handler,
            list_users_request_handler,
            get_user_request_handler,
            delete_acl_request_handler,
            list_acl_request_handler,
            update_cold_data_flow_ctr_group_config_request_handler,
            get_broker_ha_status_handler,
            broker_stats_handler,
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
            RequestCode::GetTimerCheckPoint => {
                self.broker_config_request_handler
                    .get_timer_check_point(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetTimerMetrics => {
                self.broker_config_request_handler
                    .get_timer_metrics(channel, ctx, request_code, request)
                    .await
            }
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
            RequestCode::UpdateColdDataFlowCtrConfig => {
                self.update_cold_data_flow_ctr_group_config_request_handler
                    .update_cold_data_flow_ctr_group_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::RemoveColdDataFlowCtrConfig => {
                self.update_cold_data_flow_ctr_group_config_request_handler
                    .remove_cold_data_flow_ctr_group_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetColdDataFlowCtrInfo => {
                self.update_cold_data_flow_ctr_group_config_request_handler
                    .get_cold_data_flow_ctr_info(channel, ctx, request_code, request)
                    .await
            }
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
            RequestCode::GetEarliestMsgStoreTime => {
                self.offset_request_handler
                    .get_earliest_msg_store_time(channel, ctx, request_code, request)
                    .await
            }
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
            RequestCode::UpdateAndCreateSubscriptionGroupList => {
                self.subscription_group_handler
                    .update_and_create_subscription_group_list(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetAllSubscriptionGroupConfig => {
                self.offset_request_handler
                    .get_all_subscription_group_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::DeleteSubscriptionGroup => {
                self.subscription_group_handler
                    .delete_subscription_group(channel, ctx, request_code, request)
                    .await
            }
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
            RequestCode::GetAllProducerInfo => self.producer_request_handler.get_all_producer_info(ctx, request).await,
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
            RequestCode::GetAllMessageRequestMode => {
                self.consumer_request_handler
                    .get_all_message_request_mode(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::InvokeBrokerToResetOffset => {
                self.consumer_request_handler
                    .invoke_broker_to_reset_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::InvokeBrokerToGetConsumerStatus => {
                self.consumer_request_handler
                    .invoke_broker_to_get_consumer_status(channel, ctx, request_code, request)
                    .await
            }
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
            RequestCode::QuerySubscriptionByConsumer => {
                self.consumer_request_handler
                    .query_subscription_by_consumer(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::QueryConsumeTimeSpan => {
                self.consumer_request_handler
                    .query_consume_time_span(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetSystemTopicListFromBroker => {
                self.topic_request_handler
                    .get_system_topic_list_from_broker(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::CleanExpiredConsumequeue => {
                self.offset_request_handler
                    .clean_expired_consumequeue(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::DeleteExpiredCommitlog => {
                self.offset_request_handler
                    .delete_expired_commitlog(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::CleanUnusedTopic => {
                self.topic_request_handler
                    .clean_unused_topic(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetConsumerRunningInfo => {
                self.consumer_request_handler
                    .get_consumer_running_info(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::QueryCorrectionOffset => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::ConsumeMessageDirectly => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::CloneGroupOffset => {
                self.consumer_request_handler
                    .clone_group_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::ViewBrokerStatsData => {
                self.broker_stats_handler
                    .view_broker_stats_data(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetBrokerConsumeStats => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::QueryConsumeQueue => {
                self.message_related_handler
                    .query_consume_queue(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::CheckRocksdbCqWriteProgress => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::UpdateAndGetGroupForbidden => {
                self.subscription_group_handler
                    .update_and_get_group_forbidden(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetSubscriptionGroupConfig => {
                self.subscription_group_handler
                    .get_subscription_group_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateAndCreateAclConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::DeleteAclConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::GetBrokerClusterAclInfo => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::UpdateGlobalWhiteAddrsConfig => Ok(get_unknown_cmd_response(request_code)),
            RequestCode::ResumeCheckHalfMessage => {
                self.message_related_handler
                    .resume_check_half_message(channel, ctx, request_code, request)
                    .await
            }
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
            RequestCode::GetBrokerHaStatus => {
                self.get_broker_ha_status_handler
                    .get_broker_ha_status(channel, ctx, request_code, request)
                    .await
            }
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
            RequestCode::AuthCreateUser => {
                self.create_user_request_handler
                    .create_user(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthUpdateUser => {
                self.update_user_request_handler
                    .update_user(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthDeleteUser => {
                self.delete_user_request_handler
                    .delete_user(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthGetUser => {
                self.get_user_request_handler
                    .get_user(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthListUsers => {
                self.list_users_request_handler
                    .list_users(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthCreateAcl => {
                self.create_acl_request_handler
                    .create_acl(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthUpdateAcl => {
                self.update_acl_request_handler
                    .update_acl(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthDeleteAcl => {
                self.delete_acl_request_handler
                    .delete_acl(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthGetAcl => {
                self.get_acl_request_handler
                    .get_acl(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::AuthListAcl => {
                self.list_acl_request_handler
                    .list_acl(channel, ctx, request_code, request)
                    .await
            }
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
