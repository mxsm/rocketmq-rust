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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::IS_SUB_CHANGE;
use rocketmq_common::common::mix_all::IS_SUPPORT_HEART_BEAT_V2;
use rocketmq_common::common::sys_flag::topic_sys_flag;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::client::client_channel_info::ClientChannelInfo;

pub struct ClientManageProcessor<MS: MessageStore> {
    consumer_group_heartbeat_table:
        Arc<parking_lot::RwLock<HashMap<CheetahString /* ConsumerGroup */, i32 /* HeartbeatFingerprint */>>>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> RequestProcessor for ClientManageProcessor<MS>
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
        info!("ClientManageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::HeartBeat | RequestCode::UnregisterClient | RequestCode::CheckClientConfig => {
                self.process_request_inner(channel, ctx, request_code, request).await
            }
            _ => {
                warn!(
                    "ClientManageProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("ClientManageProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS> ClientManageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            consumer_group_heartbeat_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            broker_runtime_inner,
        }
    }
}

impl<MS> ClientManageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request_inner(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::HeartBeat => self.heart_beat(channel, ctx, request).await,
            RequestCode::UnregisterClient => self.unregister_client(channel, ctx, request),
            RequestCode::CheckClientConfig => {
                unimplemented!("CheckClientConfig")
            }
            _ => {
                unimplemented!("CheckClientConfig")
            }
        }
    }

    fn unregister_client(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<UnregisterClientRequestHeader>()?;

        let client_channel_info = ClientChannelInfo::new(
            channel,
            request_header.client_id.clone(),
            request.language(),
            request.version(),
        );

        if let Some(ref group) = request_header.producer_group {
            self.broker_runtime_inner
                .producer_manager()
                .unregister_producer(group, &client_channel_info, &ctx);
        }

        if let Some(ref group) = request_header.consumer_group {
            let subscription_group_config = self
                .broker_runtime_inner
                .subscription_group_manager()
                .find_subscription_group_config(group);
            let is_notify_consumer_ids_changed_enable =
                if let Some(ref subscription_group_config) = subscription_group_config {
                    subscription_group_config.notify_consumer_ids_changed_enable()
                } else {
                    true
                };
            self.broker_runtime_inner.consumer_manager().unregister_consumer(
                group,
                &client_channel_info,
                is_notify_consumer_ids_changed_enable,
            );
        }

        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn heart_beat(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let heartbeat_data =
            SerdeJsonUtils::from_json_bytes::<HeartbeatData>(request.body().as_ref().map(|v| v.as_ref()).unwrap())
                .unwrap();
        let client_channel_info = ClientChannelInfo::new(
            channel.clone(),
            heartbeat_data.client_id.clone(),
            request.language(),
            request.version(),
        );
        if heartbeat_data.heartbeat_fingerprint != 0 {
            return Ok(self.heart_beat_v2(&channel, &ctx, heartbeat_data, client_channel_info));
        }

        //do consumer data handle
        for consumer_data in heartbeat_data.consumer_data_set.iter() {
            if self.broker_runtime_inner.broker_config().reject_pull_consumer_enable
                && ConsumeType::ConsumeActively == consumer_data.consume_type
            {
                continue;
            }
            self.consumer_group_heartbeat_table
                .write()
                .insert(consumer_data.group_name.clone(), heartbeat_data.heartbeat_fingerprint);
            let mut has_order_topic_sub = false;
            for subscription_data in consumer_data.subscription_data_set.iter() {
                if self
                    .broker_runtime_inner
                    .topic_config_manager()
                    .is_order_topic(subscription_data.topic.as_str())
                {
                    has_order_topic_sub = true;
                    break;
                }
            }
            let subscription_group_config = self
                .broker_runtime_inner
                .subscription_group_manager()
                .find_subscription_group_config(consumer_data.group_name.as_ref());
            if subscription_group_config.is_none() {
                continue;
            }
            let subscription_group_config = subscription_group_config.unwrap();
            let is_notify_consumer_ids_changed_enable = subscription_group_config.notify_consumer_ids_changed_enable();
            let topic_sys_flag = if consumer_data.unit_mode {
                topic_sys_flag::build_sys_flag(false, true)
            } else {
                0
            };
            let new_topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_data.group_name.as_str()));
            self.broker_runtime_inner
                .topic_config_manager_mut()
                .create_topic_in_send_message_back_method(
                    &new_topic,
                    subscription_group_config.retry_queue_nums(),
                    PermName::PERM_WRITE | PermName::PERM_READ,
                    has_order_topic_sub,
                    topic_sys_flag,
                )
                .await;
            let changed = self.broker_runtime_inner.consumer_manager().register_consumer(
                consumer_data.group_name.as_ref(),
                client_channel_info.clone(),
                consumer_data.consume_type,
                consumer_data.message_model,
                consumer_data.consume_from_where,
                consumer_data.subscription_data_set.clone(),
                is_notify_consumer_ids_changed_enable,
            );
            if changed {
                info!(
                    "ClientManageProcessor: registerConsumer info changed, SDK address={}, consumerData={:?}",
                    channel.remote_address(),
                    consumer_data
                )
            }
        }
        //do producer data handle
        for producer_data in heartbeat_data.producer_data_set.iter() {
            self.broker_runtime_inner
                .producer_manager()
                .register_producer(&producer_data.group_name, &client_channel_info);
        }
        let mut response_command = RemotingCommand::create_response_command();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), true.to_string());
        Ok(Some(response_command))
    }

    fn heart_beat_v2(
        &self,
        _channel: &Channel,
        _ctx: &ConnectionHandlerContext,
        heartbeat_data: HeartbeatData,
        client_channel_info: ClientChannelInfo,
    ) -> Option<RemotingCommand> {
        let is_sub_change = false;
        //handle consumer data

        //handle producer data
        for producer_data in heartbeat_data.producer_data_set.iter() {
            self.broker_runtime_inner
                .producer_manager()
                .register_producer(&producer_data.group_name, &client_channel_info);
        }
        let mut response_command = RemotingCommand::create_response_command();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), is_sub_change.to_string());
        Some(response_command)
    }
}
