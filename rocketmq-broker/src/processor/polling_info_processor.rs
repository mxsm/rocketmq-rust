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
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::polling_info_request_header::PollingInfoRequestHeader;
use rocketmq_protocol::protocol::header::polling_info_response_header::PollingInfoResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::RequestProcessorV2;
use tracing::error;
use tracing::warn;

use crate::config::broker_config::BrokerConfig;

use crate::long_polling::pop_deferred::service::PollingCountProvider;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

/// PollingInfoProcessor handles requests for polling information from clients.
/// It checks the number of polling requests for a specific topic, consumer group, and queue.
pub struct PollingInfoProcessor {
    command_factory: RemotingCommandFactory,
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
        Self::new_with_factory(
            broker_config,
            topic_config_manager,
            subscription_group_lookup,
            polling_count_provider,
            application_remoting_command_factory(),
        )
    }

    pub(crate) fn new_with_factory(
        broker_config: Arc<BrokerConfig>,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        polling_count_provider: Weak<dyn PollingCountProvider>,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self {
            command_factory,
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
            command_factory: self.command_factory,
            broker_config: Arc::clone(&self.broker_config),
            topic_config_manager: Arc::clone(&self.topic_config_manager),
            subscription_group_lookup: self.subscription_group_lookup.clone(),
            polling_count_provider: self.polling_count_provider.clone(),
        }
    }
}

impl RequestProcessorV2 for PollingInfoProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_v2_shared(request).await
    }
}

impl PollingInfoProcessor {
    pub(crate) async fn process_v2_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original_opaque = request.original_identity().original_opaque();
        let command_factory = self.command_factory;
        let peer_label = request_peer_label(request.origin());
        let result = self.process_command(&peer_label, request.command_mut()).await;
        crate::processor::response_plan::immediate_outcome_from_command_result(
            &command_factory,
            result,
            original_opaque,
            "PollingInfoProcessor V2 command dispatch completed without a response",
        )
    }
}

impl PollingInfoProcessor {
    pub(crate) async fn process_legacy(
        &self,
        peer_label: String,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let processor = self.clone();
        processor.process_command(&peer_label, request).await
    }

    /// V2 leaf business contract; the trusted peer label is diagnostic metadata only.
    async fn process_command(
        &self,
        peer_label: &str,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = self.command_factory.create_java_default_error_response_command();

        // Decode request header
        let request_header = request
            .decode_command_custom_header::<PollingInfoRequestHeader>()
            .map_err(|e| {
                error!(
                    "Failed to decode PollingInfoRequestHeader: {:?}, channel: {}",
                    e, peer_label
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
            error!("The topic {} not exist, consumer: {}", request_header.topic, peer_label);
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
                request_header.queue_id, request_header.topic, topic_config.read_queue_nums, peer_label
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

        let polling_num = self.get_polling_num(
            &request_header.topic,
            &request_header.consumer_group,
            request_header.queue_id,
        );

        let response_header = PollingInfoResponseHeader { polling_num };
        let final_response = self
            .command_factory
            .create_success_response_command_with_header(response_header)
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
    fn get_polling_num(&self, topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> i32 {
        self.polling_count_provider
            .upgrade()
            .map(|provider| provider.polling_count(topic, consumer_group, queue_id))
            .unwrap_or_default()
    }
}

fn request_peer_label(origin: &RequestOrigin) -> String {
    match origin {
        RequestOrigin::Network { peer } => peer.address().to_string(),
        RequestOrigin::Embedded { .. } => "embedded".to_owned(),
        _ => "unrecognized-origin".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::broker_config::BrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::key_builder::KeyBuilder;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_security_api::AuthenticatedRequestContext;
    use rocketmq_security_api::Decision;
    use rocketmq_security_api::Principal;
    use rocketmq_security_api::RequestPolicy;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_store::StateMachineVersionView;
    use rocketmq_transport::api::v1::AdmissionController;
    use rocketmq_transport::api::v1::AdmissionLimits;
    use rocketmq_transport::api::v1::ServerConfig;
    use rocketmq_transport::api::v1::TransportSecurity;
    use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
    use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::v2::ResponseBodyKind;
    use rocketmq_transport::api::v2::TransportServerV2;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::EmbeddedRequestHarnessV2;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::PollingCountProvider;
    use super::PollingInfoProcessor;
    use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
    use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManagerConfig;
    use crate::topic::manager::topic_config_manager::TopicConfigManager;

    struct FixedPollingCount(i32);

    impl PollingCountProvider for FixedPollingCount {
        fn polling_count(&self, _topic: &CheetahString, _consumer_group: &CheetahString, _queue_id: i32) -> i32 {
            self.0
        }
    }

    struct AllowEmbeddedPolicy;

    impl RequestPolicy for AllowEmbeddedPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    struct EmbeddedV2Fixture {
        owner: RuntimeOwner,
        context: rocketmq_runtime::ChildServiceContext,
        harness: EmbeddedRequestHarnessV2<PollingInfoProcessor>,
    }

    impl EmbeddedV2Fixture {
        fn new(processor: PollingInfoProcessor) -> Self {
            let owner = RuntimeOwner::new(RuntimeConfig::server_default("polling-info-v2-test"))
                .expect("PollingInfo V2 test runtime");
            let context = owner.root_context().component("polling-info-v2-test.request");
            let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
                processor,
                Vec::new(),
                Arc::new(TransportSecurity::secure_enforced(
                    Some(Arc::new(AllowEmbeddedPolicy)),
                    None,
                )),
                Arc::new(AdmissionController::new(AdmissionLimits::default())),
            ));
            let harness = EmbeddedRequestHarnessV2::new(
                dispatcher,
                context.task_group().clone(),
                Principal::new("polling-info-v2-test"),
            );
            Self {
                owner,
                context,
                harness,
            }
        }

        async fn finish(self) {
            drop(self.harness);
            drop(self.context);
            assert!(self.owner.shutdown_tasks().await.is_healthy());
            assert!(self.owner.shutdown_background().is_healthy());
        }
    }

    fn test_processor(provider: std::sync::Weak<dyn PollingCountProvider>) -> PollingInfoProcessor {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = MessageStoreConfig::default();
        let topic_config_manager = Arc::new(TopicConfigManager::new(
            broker_config.as_ref(),
            &message_store_config,
            true,
            None,
        ));
        let subscription_group_manager = SubscriptionGroupManager::new(
            SubscriptionGroupManagerConfig::from_configs(broker_config.as_ref(), &message_store_config),
            StateMachineVersionView::default(),
            None,
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

        let topic = CheetahString::from_static_str("topic");
        let group = CheetahString::from_static_str("group");
        assert_eq!(processor.get_polling_num(&topic, &group, 0), 7);
        drop(provider);
        assert_eq!(processor.get_polling_num(&topic, &group, 0), 0);
    }

    #[tokio::test]
    async fn v2_embedded_header_error_is_an_empty_reply_plan() {
        let provider: Arc<dyn PollingCountProvider> = Arc::new(FixedPollingCount(0));
        let fixture = EmbeddedV2Fixture::new(test_processor(Arc::downgrade(&provider)));
        let request = RemotingCommand::create_remoting_command(RequestCode::PollingInfo).set_opaque(711);

        let outcome = fixture
            .harness
            .dispatch(None, request)
            .await
            .expect("embedded PollingInfo V2 header-error response");
        let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
            panic!("PollingInfo V2 header error must return a reply plan");
        };

        assert_eq!(plan.response_code(), ResponseCode::SystemError as i32);
        assert_eq!(plan.body_kind(), ResponseBodyKind::Empty);
        assert_eq!(plan.body_len(), 0);
        fixture.finish().await;
    }

    #[tokio::test]
    async fn v2_network_header_error_preserves_request_identity_and_wire_metadata() {
        const ORIGINAL_OPAQUE: i32 = 9_013;
        let owner = RuntimeOwner::new(RuntimeConfig::server_default("polling-info-v2-network-test"))
            .expect("PollingInfo V2 network test runtime");
        let server_context = owner.root_context().component("polling-info-v2-network-test.server");
        let runner_context = owner.root_context().component("polling-info-v2-network-test.runner");
        let factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(4_201, SerializeType::ROCKETMQ));
        let provider: Arc<dyn PollingCountProvider> = Arc::new(FixedPollingCount(0));
        let mut processor = test_processor(Arc::downgrade(&provider));
        processor.command_factory = factory;
        let server = TransportServerV2::new(
            Arc::new(ServerConfig {
                bind_address: "127.0.0.1".to_owned(),
                listen_port: 0,
                ..ServerConfig::default()
            }),
            server_context,
            processor,
        );
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (startup_sender, startup_receiver) = oneshot::channel();
        let (result_sender, result_receiver) = oneshot::channel();
        runner_context
            .spawn_service("polling-info-v2-server", async move {
                let result = server
                    .try_run_with_shutdown_report_and_startup(
                        async move {
                            let _ = shutdown_receiver.await;
                        },
                        startup_sender,
                    )
                    .await;
                let _ = result_sender.send(result);
            })
            .expect("spawn PollingInfo V2 server");

        let address = startup_receiver
            .await
            .expect("PollingInfo V2 startup channel")
            .expect("PollingInfo V2 server startup");
        let mut client = Connection::new(
            TcpStream::connect(address)
                .await
                .expect("connect PollingInfo V2 client"),
        );
        let request = RemotingCommand::create_remoting_command(RequestCode::PollingInfo).set_opaque(ORIGINAL_OPAQUE);
        let request_version = request.version();
        let request_serialize_type = request.serialize_type();
        assert_ne!(request_version, factory.defaults().version());
        client.send_command(request).await.expect("send PollingInfo V2 request");

        let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
            .await
            .expect("PollingInfo V2 response deadline")
            .expect("PollingInfo V2 connection remains open")
            .expect("PollingInfo V2 response frame");
        assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
        assert_eq!(response.code(), ResponseCode::SystemError as i32);
        assert_eq!(response.version(), request_version);
        assert_eq!(response.serialize_type(), request_serialize_type);

        client.shutdown().await.expect("shutdown PollingInfo V2 client");
        let _ = shutdown_sender.send(());
        let report = tokio::time::timeout(Duration::from_secs(2), result_receiver)
            .await
            .expect("PollingInfo V2 shutdown deadline")
            .expect("PollingInfo V2 shutdown result channel")
            .expect("PollingInfo V2 shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
    }

    #[test]
    fn source_does_not_retain_complete_broker_runtime() {
        let source = include_str!("polling_info_processor.rs");

        assert!(!source.contains(concat!("Arc", "Mut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
    }
}
