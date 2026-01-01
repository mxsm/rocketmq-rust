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

use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::FAQUrl;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::polling_info_request_header::PollingInfoRequestHeader;
use rocketmq_remoting::protocol::header::polling_info_response_header::PollingInfoResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

/// PollingInfoProcessor handles requests for polling information from clients.
/// It checks the number of polling requests for a specific topic, consumer group, and queue.
pub struct PollingInfoProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> PollingInfoProcessor<MS> {
    /// Create a new PollingInfoProcessor instance
    ///
    /// # Arguments
    /// * `broker_runtime_inner` - The broker runtime containing all necessary managers and
    ///   configurations
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS: MessageStore> RequestProcessor for PollingInfoProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_internal(channel, request).await
    }
}

impl<MS: MessageStore> PollingInfoProcessor<MS> {
    async fn process_request_internal(
        &mut self,
        channel: Channel,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();

        // Decode request header
        let request_header = request
            .decode_command_custom_header::<PollingInfoRequestHeader>()
            .map_err(|e| {
                error!(
                    "Failed to decode PollingInfoRequestHeader: {:?}, channel: {}",
                    e,
                    channel.remote_address()
                );
                e
            })?;

        // Set response opaque to match request
        response.set_opaque_mut(request.opaque());

        let broker_permission = self.broker_runtime_inner.broker_config().broker_permission;
        if !PermName::is_readable(broker_permission) {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "the broker[{}] peeking message is forbidden",
                self.broker_runtime_inner.broker_config().broker_ip1
            ));
            return Ok(Some(response));
        }

        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&request_header.topic);

        if topic_config.is_none() {
            error!(
                "The topic {} not exist, consumer: {}",
                request_header.topic,
                channel.remote_address()
            );
            let response = response.set_code(ResponseCode::TopicNotExist).set_remark(format!(
                "topic[{}] not exist, apply first please! {}",
                request_header.topic, "https://rocketmq.apache.org/docs/bestPractice/06FAQ"
            ));
            return Ok(Some(response));
        }

        let topic_config = topic_config.unwrap();

        if !PermName::is_readable(topic_config.perm) {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "the topic[{}] peeking message is forbidden",
                request_header.topic
            ));
            return Ok(Some(response));
        }

        if request_header.queue_id >= topic_config.read_queue_nums as i32 {
            let error_info = format!(
                "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.read_queue_nums,
                channel.remote_address()
            );
            warn!("{}", error_info);
            let response = response.set_code(ResponseCode::SystemError).set_remark(error_info);
            return Ok(Some(response));
        }

        let subscription_group_config = self
            .broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(&request_header.consumer_group);

        if subscription_group_config.is_none() {
            let response = response
                .set_code(ResponseCode::SubscriptionGroupNotExist)
                .set_remark(format!(
                    "subscription group [{}] does not exist, {}",
                    request_header.consumer_group,
                    FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                ));
            return Ok(Some(response));
        }

        // Unwrap subscription group config safely, checked for None above
        let subscription_group_config = subscription_group_config.unwrap();

        if !subscription_group_config.consume_enable() {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "subscription group no permission, {}",
                request_header.consumer_group
            ));
            return Ok(Some(response));
        }

        let key = KeyBuilder::build_polling_key(
            &request_header.topic,
            &request_header.consumer_group,
            request_header.queue_id,
        );

        let polling_num = self.get_polling_num(&key);

        let response_header = PollingInfoResponseHeader { polling_num };
        let final_response = response
            .set_command_custom_header(response_header)
            .set_code(ResponseCode::Success)
            .set_opaque(request.opaque());

        Ok(Some(final_response))
    }

    /// Get the number of polling requests for a given key
    ///
    /// # Arguments
    /// * `key` - The polling key (topic@consumerGroup@queueId)
    ///
    /// # Returns
    /// The number of polling requests, or 0 if no polling requests exist
    fn get_polling_num(&self, key: &str) -> i32 {
        // Access the polling map through PopMessageProcessor via pop_long_polling_service
        // Note: In the current Rust implementation, we need to access the polling_map
        // through the pop_long_polling_service which is part of the broker runtime

        // Get the pop_long_polling_service if available
        if let Some(pop_message_processor) = self.broker_runtime_inner.pop_message_processor() {
            if let Some(pop_long_polling_service) = pop_message_processor.pop_long_polling_service() {
                return pop_long_polling_service.get_polling_num(key);
            }
        }

        // If no polling service or no entry found, return 0
        0
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::key_builder::KeyBuilder;

    #[test]
    fn test_build_polling_key() {
        let topic = CheetahString::from_static_str("TestTopic");
        let consumer_group = CheetahString::from_static_str("TestConsumerGroup");
        let queue_id = 0;

        let key = KeyBuilder::build_polling_key(&topic, &consumer_group, queue_id);

        assert!(key.contains("TestTopic"));
        assert!(key.contains("TestConsumerGroup"));
        assert!(key.contains("0"));
    }
}
