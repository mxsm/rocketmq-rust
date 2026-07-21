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
use tracing::error;
use tracing::warn;

use rocketmq_common::common::broker::broker_config::BrokerConfig;

use crate::long_polling::long_polling_service::pop_long_polling_service::PollingCountProvider;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

/// PollingInfoProcessor handles requests for polling information from clients.
/// It checks the number of polling requests for a specific topic, consumer group, and queue.
pub struct PollingInfoProcessor {
    broker_config: Arc<BrokerConfig>,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    polling_count_provider: Weak<dyn PollingCountProvider>,
}

impl PollingInfoProcessor {
    /// Create a new PollingInfoProcessor instance
    ///
    /// # Arguments
    /// * `broker_config` - Immutable broker request policy.
    /// * `topic_config_manager` - Live topic configuration capability.
    /// * `subscription_group_lookup` - Live subscription-group lookup capability.
    /// * `polling_count_provider` - Weak POP polling-count capability.
    pub(crate) fn new(
        broker_config: Arc<BrokerConfig>,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        polling_count_provider: Weak<dyn PollingCountProvider>,
    ) -> Self {
        Self {
            broker_config,
            topic_config_manager,
            subscription_group_lookup,
            polling_count_provider,
        }
    }
}

impl Clone for PollingInfoProcessor {
    fn clone(&self) -> Self {
        Self {
            broker_config: Arc::clone(&self.broker_config),
            topic_config_manager: Arc::clone(&self.topic_config_manager),
            subscription_group_lookup: self.subscription_group_lookup.clone(),
            polling_count_provider: self.polling_count_provider.clone(),
        }
    }
}

impl RequestProcessor for PollingInfoProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_internal(channel, request).await
    }
}

impl PollingInfoProcessor {
    pub async fn process_request_shared(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut processor = self.clone();
        processor.process_request(channel, ctx, request).await
    }

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

        let broker_permission = self.broker_config.broker_permission;
        if !PermName::is_readable(broker_permission) {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "the broker[{}] peeking message is forbidden",
                self.broker_config.broker_ip1
            ));
            return Ok(Some(response));
        }

        let topic_config = self.topic_config_manager.select_topic_config(&request_header.topic);

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
            .subscription_group_lookup
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
        self.polling_count_provider
            .upgrade()
            .map(|provider| provider.polling_count(key))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::key_builder::KeyBuilder;
    use rocketmq_store::base::message_store::StateMachineVersionView;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::PollingCountProvider;
    use super::PollingInfoProcessor;
    use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
    use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManagerConfig;
    use crate::topic::manager::topic_config_manager::TopicConfigManager;

    struct FixedPollingCount(i32);

    impl PollingCountProvider for FixedPollingCount {
        fn polling_count(&self, _key: &str) -> i32 {
            self.0
        }
    }

    fn test_processor(provider: std::sync::Weak<dyn PollingCountProvider>) -> PollingInfoProcessor {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = MessageStoreConfig::default();
        let topic_config_manager = Arc::new(TopicConfigManager::new(
            broker_config.as_ref(),
            &message_store_config,
            true,
        ));
        let subscription_group_manager = SubscriptionGroupManager::new(
            SubscriptionGroupManagerConfig::from_configs(broker_config.as_ref(), &message_store_config),
            StateMachineVersionView::default(),
        );
        PollingInfoProcessor::new(
            broker_config,
            topic_config_manager,
            subscription_group_manager.config_lookup(),
            provider,
        )
    }

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

    #[test]
    fn polling_count_provider_does_not_extend_pop_service_lifetime() {
        let provider: Arc<dyn PollingCountProvider> = Arc::new(FixedPollingCount(7));
        let processor = test_processor(Arc::downgrade(&provider));

        assert_eq!(processor.get_polling_num("topic@group@0"), 7);
        drop(provider);
        assert_eq!(processor.get_polling_num("topic@group@0"), 0);
    }

    #[test]
    fn source_does_not_retain_complete_broker_runtime() {
        let source = include_str!("polling_info_processor.rs");

        assert!(!source.contains(concat!("Arc", "Mut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
    }
}
