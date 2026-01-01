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
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct SubscriptionGroupHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> SubscriptionGroupHandler<MS> {
    pub(super) fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn update_and_create_subscription_group(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let start_time = get_current_millis() as i64;

        let mut response = RemotingCommand::create_response_command();

        info!(
            "AdminBrokerProcessor#updateAndCreateSubscriptionGroup called by {}",
            _channel.remote_address()
        );
        let mut config = SubscriptionGroupConfig::decode(request.get_body().unwrap());
        if let Ok(config) = config.as_mut() {
            self.broker_runtime_inner
                .subscription_group_manager_mut()
                .update_subscription_group_config(config)
        }
        response.set_code_ref(ResponseCode::Success);
        let execution_time = get_current_millis() as i64 - start_time;

        if let Ok(config) = config.as_ref() {
            info!(
                "executionTime of create subscriptionGroup:{} is {} ms",
                config.group_name(),
                execution_time
            );
        }

        // todo
        // InvocationStatus status =
        // response.getCode() == ResponseCode.SUCCESS ? InvocationStatus.SUCCESS :
        // InvocationStatus.FAILURE; Attributes attributes =
        // BrokerMetricsManager.newAttributesBuilder()     .put(LABEL_INVOCATION_STATUS,
        // status.getName())     .build();
        // BrokerMetricsManager.consumerGroupCreateExecuteTime.record(executionTime, attributes);
        Ok(Some(response))
    }

    pub async fn unlock_batch_mq(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_body = UnlockBatchRequestBody::decode(request.get_body().unwrap()).unwrap();
        if request_body.only_this_broker || !self.broker_runtime_inner.broker_config().lock_in_strict_mode {
            self.broker_runtime_inner.rebalance_lock_manager().unlock_batch(
                request_body.consumer_group.as_ref().unwrap(),
                &request_body.mq_set,
                request_body.client_id.as_ref().unwrap(),
            );
        } else {
            request_body.only_this_broker = true;
            let request_body = Bytes::from(request_body.encode().expect("unlockBatchMQ encode error"));
            for broker_addr in self.broker_runtime_inner.broker_member_group().broker_addrs.values() {
                match self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .unlock_batch_mq_async(broker_addr, request_body.clone(), 1000)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("unlockBatchMQ exception on {}, {}", broker_addr, e);
                    }
                }
            }
        }
        Ok(Some(RemotingCommand::create_response_command()))
    }
}
