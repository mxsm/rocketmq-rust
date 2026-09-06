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

use crate::client::manager::consumer_manager::ConsumerAssignmentView;

use crate::load_balance::message_request_mode_manager::MessageRequestModeCasError;
use crate::load_balance::message_request_mode_manager::MessageRequestModeManager;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;

use crate::config::broker_config::BrokerConfig;
use crate::config::config_manager::ConfigManager;
use cheetah_string::CheetahString;
use rocketmq_error::PublicErrorView;
use rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED;
use rocketmq_model::allocation::AllocateMessageQueueAveragely;
use rocketmq_model::allocation::AllocateMessageQueueAveragelyByCircle;
use rocketmq_model::allocation::AllocateMessageQueueStrategy;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_protocol::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_protocol::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_protocol::protocol::body::supervised_mutation::ExpectedMessageRequestMode;
use rocketmq_protocol::protocol::body::supervised_mutation::GetMessageRequestModeRequestBody;
use rocketmq_protocol::protocol::body::supervised_mutation::MessageRequestModeMutationResultBody;
use rocketmq_protocol::protocol::body::supervised_mutation::MessageRequestModeStateBody;
use rocketmq_protocol::protocol::body::supervised_mutation::MutationPersistenceState;
use rocketmq_protocol::protocol::body::supervised_mutation::SetMessageRequestModeCasRequestBody;
use rocketmq_protocol::protocol::body::supervised_mutation::SupervisedMessageRequestMode;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::subscription::subscription_group_config::validate_subscription_group_name;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::MetadataDeadline;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_store::MessageStoreConfig;
use rocketmq_transport::api::error_response;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing::warn;

/// A processor for handling query assignments in the RocketMQ broker.
///
/// This struct manages the message request modes and load balancing strategies
/// for message queues. It interacts with the broker runtime to process assignment
/// requests and allocate message queues to consumers.
pub struct QueryAssignmentProcessor {
    command_factory: RemotingCommandFactory,
    // Manages the message request modes for different topics and consumer groups.
    message_request_mode_manager: MessageRequestModeManager,

    // A map of load balancing strategies for message queue allocation.
    load_strategy: HashMap<CheetahString, Arc<dyn AllocateMessageQueueStrategy>>,

    // These assignment defaults are startup configuration and are not part of BrokerConfig's
    // dynamic-update allowlist. If that changes, this must become a narrow live-config handle.
    broker_config: Arc<BrokerConfig>,
    topic_route_info_manager: TopicRouteInfoManager,
    consumer_assignment_view: ConsumerAssignmentView,
    metadata_io: Option<MetadataIoActor>,
    supervised_topic_configs: Option<Arc<TopicConfigManager>>,
    supervised_subscription_groups: Option<SubscriptionGroupConfigLookup>,
}

impl RequestProcessor for QueryAssignmentProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl QueryAssignmentProcessor {
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original_opaque = request.original_identity().original_opaque();
        let command_factory = self.command_factory;
        let peer_label = request_peer_label(request.origin());
        let result = self.process_command(request.command_mut(), &peer_label).await;
        crate::processor::response_assembly::immediate_outcome_from_command_result(
            &command_factory,
            result,
            original_opaque,
            "QueryAssignmentProcessor command dispatch completed without a response",
        )
    }
}

impl QueryAssignmentProcessor {
    /// processor business contract; the peer label is retained only for diagnostics.
    async fn process_command(
        &self,
        request: &mut RemotingCommand,
        peer_label: &str,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("QueryAssignmentProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::QueryAssignment
            | RequestCode::SetMessageRequestMode
            | RequestCode::GetMessageRequestMode
            | RequestCode::SetMessageRequestModeCas => {
                self.process_command_inner(request_code, request, peer_label).await
            }
            _ => {
                warn!(
                    "QueryAssignmentProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = error_response(
                    PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
                    RemotingErrorTarget::Reply {
                        factory: &self.command_factory,
                        opaque: request.opaque(),
                    },
                );
                Ok(Some(response))
            }
        }
    }

    pub(crate) fn new(
        broker_config: Arc<BrokerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
        topic_route_info_manager: TopicRouteInfoManager,
        consumer_assignment_view: ConsumerAssignmentView,
    ) -> Self {
        Self::new_with_metadata_io(
            broker_config,
            message_store_config,
            topic_route_info_manager,
            consumer_assignment_view,
            None,
        )
    }

    pub(crate) fn new_with_metadata_io(
        broker_config: Arc<BrokerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
        topic_route_info_manager: TopicRouteInfoManager,
        consumer_assignment_view: ConsumerAssignmentView,
        metadata_io: Option<MetadataIoActor>,
    ) -> Self {
        Self::new_with_metadata_io_and_factory(
            broker_config,
            message_store_config,
            topic_route_info_manager,
            consumer_assignment_view,
            metadata_io,
            application_remoting_command_factory(),
        )
    }

    pub(crate) fn new_with_metadata_io_and_factory(
        broker_config: Arc<BrokerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
        topic_route_info_manager: TopicRouteInfoManager,
        consumer_assignment_view: ConsumerAssignmentView,
        metadata_io: Option<MetadataIoActor>,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        let allocate_message_queue_averagely: Arc<dyn AllocateMessageQueueStrategy> =
            Arc::new(AllocateMessageQueueAveragely);
        let allocate_message_queue_averagely_by_circle: Arc<dyn AllocateMessageQueueStrategy> =
            Arc::new(AllocateMessageQueueAveragelyByCircle);
        let mut load_strategy = HashMap::new();
        load_strategy.insert(
            CheetahString::from_static_str(allocate_message_queue_averagely.get_name()),
            allocate_message_queue_averagely,
        );
        load_strategy.insert(
            CheetahString::from_static_str(allocate_message_queue_averagely_by_circle.get_name()),
            allocate_message_queue_averagely_by_circle,
        );
        let manager = MessageRequestModeManager::new(message_store_config);
        let _ = manager.load();
        Self {
            command_factory,
            message_request_mode_manager: manager,
            load_strategy,
            broker_config,
            topic_route_info_manager,
            consumer_assignment_view,
            metadata_io,
            supervised_topic_configs: None,
            supervised_subscription_groups: None,
        }
    }

    pub(crate) fn with_supervised_target_lookups(
        mut self,
        topic_configs: Arc<TopicConfigManager>,
        subscription_groups: SubscriptionGroupConfigLookup,
    ) -> Self {
        self.supervised_topic_configs = Some(topic_configs);
        self.supervised_subscription_groups = Some(subscription_groups);
        self
    }

    pub fn message_request_mode_manager(&self) -> &MessageRequestModeManager {
        &self.message_request_mode_manager
    }
}

impl Clone for QueryAssignmentProcessor {
    fn clone(&self) -> Self {
        Self {
            command_factory: self.command_factory,
            message_request_mode_manager: self.message_request_mode_manager.clone(),
            load_strategy: self.load_strategy.clone(),
            broker_config: Arc::clone(&self.broker_config),
            topic_route_info_manager: self.topic_route_info_manager.clone(),
            consumer_assignment_view: self.consumer_assignment_view.clone(),
            metadata_io: self.metadata_io.clone(),
            supervised_topic_configs: self.supervised_topic_configs.clone(),
            supervised_subscription_groups: self.supervised_subscription_groups.clone(),
        }
    }
}

impl QueryAssignmentProcessor {
    async fn process_command_inner(
        &self,
        request_code: RequestCode,
        request: &mut RemotingCommand,
        peer_label: &str,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::QueryAssignment => self.query_assignment(request, peer_label).await,
            RequestCode::SetMessageRequestMode => self.set_message_request_mode(request).await,
            RequestCode::GetMessageRequestMode => self.get_message_request_mode(request).await,
            RequestCode::SetMessageRequestModeCas => self.set_message_request_mode_cas(request).await,
            _ => Ok(None),
        }
    }

    /// Processes query assignment requests from consumers.
    ///
    /// This method corresponds to Java's `QueryAssignmentProcessor.queryAssignment()`.
    /// It validates the request, performs load balancing, and returns assigned message queues.
    ///
    /// # Arguments
    ///
    /// * `request` - The remoting command containing QueryAssignmentRequestBody
    ///
    /// # Returns
    ///
    /// A RemotingCommand response containing QueryAssignmentResponseBody with assigned queues
    async fn query_assignment(
        &self,
        request: &mut RemotingCommand,
        peer_label: &str,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if request.get_body().is_none() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "empty body",
            )));
        }
        // Safe to unwrap: already checked is_none() above
        let request_body = QueryAssignmentRequestBody::decode(request.get_body().unwrap())?;

        // Validate required fields
        if request_body.topic.is_empty() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "topic is empty",
            )));
        }
        if request_body.consumer_group.is_empty() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "consumerGroup is empty",
            )));
        }
        if request_body.client_id.is_empty() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "clientId is empty",
            )));
        }

        let set_message_request_mode_request_body = self
            .message_request_mode_manager
            .get_message_request_mode(&request_body.topic, &request_body.consumer_group);

        let set_message_request_mode_request_body =
            if let Some(set_message_request_mode_request_body) = set_message_request_mode_request_body {
                set_message_request_mode_request_body
            } else {
                let mut body = SetMessageRequestModeRequestBody {
                    topic: request_body.topic.clone(),
                    consumer_group: request_body.consumer_group.clone(),
                    ..Default::default()
                };
                if request_body.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
                    // retry topic must be pull mode
                    body.mode = MessageRequestMode::Pull;
                } else {
                    body.mode = self.broker_config.default_message_request_mode;
                }
                if body.mode == MessageRequestMode::Pop {
                    body.pop_share_queue_num = self.broker_config.default_pop_share_queue_num;
                }
                body
            };
        let mode = set_message_request_mode_request_body.mode;

        // Perform load balancing to get assigned message queues
        info!(
            "QueryAssignment: topic={}, group={}, clientId={}, model={:?}, strategy={}",
            request_body.topic,
            request_body.consumer_group,
            request_body.client_id,
            request_body.message_model,
            request_body.strategy_name
        );

        let message_queues = self
            .do_load_balance(
                &request_body.topic,
                &request_body.consumer_group,
                &request_body.client_id,
                request_body.message_model,
                &request_body.strategy_name,
                set_message_request_mode_request_body,
                peer_label,
            )
            .await;
        let assignments = if let Some(message_queues) = message_queues {
            let assignment_count = message_queues.len();
            let assignments: HashSet<MessageQueueAssignment> = message_queues
                .into_iter()
                .map(|mq| MessageQueueAssignment {
                    message_queue: Some(mq),
                    mode,
                    attachments: None,
                })
                .collect();

            info!(
                "QueryAssignment: allocated {} queues for group={}, clientId={}",
                assignment_count, request_body.consumer_group, request_body.client_id
            );
            assignments
        } else {
            info!(
                "QueryAssignment: no queues allocated for group={}, clientId={}",
                request_body.consumer_group, request_body.client_id
            );
            HashSet::with_capacity(0)
        };
        let body = QueryAssignmentResponseBody {
            message_queue_assignments: assignments,
        };
        Ok(Some(
            self.command_factory
                .create_success_response_command()
                .set_body(body.encode()?),
        ))
    }

    /// Performs load balancing to allocate message queues to consumers.
    ///
    /// This function handles both broadcasting and clustering message models.
    /// For broadcasting, it returns all message queues. For clustering, it uses
    /// the specified load balancing strategy to allocate message queues to consumers.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic name.
    /// * `consumer_group` - A reference to a `CheetahString` representing the consumer group name.
    /// * `client_id` - A reference to a `CheetahString` representing the client ID.
    /// * `message_model` - A `MessageModel` enum indicating the message model (Broadcasting or
    ///   Clustering).
    /// * `strategy_name` - A reference to a `CheetahString` representing the name of the load
    ///   balancing strategy.
    /// * `set_message_request_mode_request_body` - A `SetMessageRequestModeRequestBody` containing
    ///   the message request mode settings.
    /// * `peer_label` - Trusted ingress metadata used only in diagnostics.
    ///
    /// # Returns
    ///
    /// An `Option<HashSet<MessageQueue>>` containing the allocated message queues, or `None` if no
    /// queues are allocated.
    async fn do_load_balance(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        client_id: &CheetahString,
        message_model: MessageModel,
        strategy_name: &CheetahString,
        set_message_request_mode_request_body: SetMessageRequestModeRequestBody,
        peer_label: &str,
    ) -> Option<HashSet<MessageQueue>> {
        match message_model {
            // handle broadcasting consumer, this mode returns all message queues
            MessageModel::Broadcasting => {
                let assigned_queue_set = self.topic_route_info_manager.get_topic_subscribe_info(topic).await;
                if assigned_queue_set.is_none() {
                    warn!(
                        "QueryLoad: no assignment for group[{}], the topic[{}] does not exist.",
                        consumer_group, topic
                    );
                }
                assigned_queue_set
            }
            // handle clustering consumer
            MessageModel::Clustering => {
                // get all message queues for the topic
                let mq_set = if mix_all::is_lmq(Some(topic.as_str())) {
                    let mut set = HashSet::new();
                    let queue = MessageQueue::from_parts(
                        topic.clone(),
                        self.broker_config.broker_name().clone(),
                        mix_all::LMQ_QUEUE_ID as i32,
                    );
                    set.insert(queue);
                    Some(set)
                } else {
                    self.topic_route_info_manager.get_topic_subscribe_info(topic).await
                };

                if mq_set.is_none() {
                    if !topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
                        warn!(
                            "QueryLoad: no assignment for group[{}], the topic[{}] does not exist.",
                            consumer_group, topic
                        );
                    }
                    return None;
                }

                if !self.broker_config.server_load_balancer_enable {
                    return mq_set;
                }
                // get all consumer ids for the consumer group
                let mut cid_all = self.consumer_assignment_view.client_ids(consumer_group);
                if cid_all.is_empty() {
                    warn!(
                        "QueryLoad: no assignment for group[{}] topic[{}], get consumer id list failed",
                        consumer_group, topic
                    );
                    return None;
                }
                // Safe to unwrap here: already checked mq_set.is_none() above
                let mut mq_all = mq_set.unwrap().into_iter().collect::<Vec<MessageQueue>>();
                // sort message queues and consumer ids
                mq_all.sort();
                cid_all.sort();

                let strategy = self.load_strategy.get(strategy_name);
                if strategy.is_none() {
                    warn!("QueryLoad: unsupported strategy [{}],  {}", strategy_name, peer_label);
                    return None;
                }
                // Safe to unwrap here: already checked strategy.is_none() above
                let strategy = strategy.unwrap();
                let result = if set_message_request_mode_request_body.mode == MessageRequestMode::Pop {
                    // allocate message queues for pop mode
                    self.allocate_for_pop(
                        strategy,
                        consumer_group,
                        client_id,
                        mq_all.as_slice(),
                        cid_all.as_slice(),
                        set_message_request_mode_request_body.pop_share_queue_num,
                    )
                } else {
                    // allocate message queues for pull mode
                    match strategy.allocate(consumer_group, client_id, mq_all.as_slice(), cid_all.as_slice()) {
                        Ok(value) => Ok(value.into_iter().collect::<HashSet<MessageQueue>>()),
                        Err(e) => Err(e),
                    }
                };
                result.ok()
            }
        }
    }

    pub fn allocate_for_pop(
        &self,
        strategy: &Arc<dyn AllocateMessageQueueStrategy>,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
        pop_share_queue_num: i32,
    ) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
        if pop_share_queue_num <= 0 || pop_share_queue_num >= cid_all.len() as i32 - 1 {
            //Each consumer can consume all queues, return all queues. Queue ID -1 means consume
            // all queues when consuming in Pop mode
            //each client pop all message queue
            Ok(mq_all
                .iter()
                .map(|mq| MessageQueue::from_parts(mq.topic().clone(), mq.broker_name().clone(), -1))
                .collect::<HashSet<MessageQueue>>())
        } else if cid_all.len() <= mq_all.len() {
            //consumer working in pop mode could share the MessageQueues assigned to
            // the N (N = popWorkGroupSize) consumer following it in the cid list
            let mut allocate_result = strategy.allocate(consumer_group, current_cid, mq_all, cid_all)?;
            let index = cid_all.iter().position(|cid| cid == current_cid);
            if let Some(mut index) = index {
                for _i in 1..=pop_share_queue_num {
                    index += 1;
                    index %= cid_all.len();
                    let result = strategy.allocate(consumer_group, &cid_all[index], mq_all, cid_all)?;
                    allocate_result.extend(result);
                }
            }
            Ok(allocate_result.into_iter().collect::<HashSet<MessageQueue>>())
        } else {
            //make sure each cid is assigned
            allocate(consumer_group, current_cid, mq_all, cid_all)
        }
    }

    async fn set_message_request_mode(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if request.get_body().is_none() {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "empty body",
            )));
        }
        // Safe to unwrap: already checked is_none() above
        let request_body = SetMessageRequestModeRequestBody::decode(request.get_body().unwrap())?;
        if request_body.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return Ok(Some(
                self.command_factory
                    .create_response_command_with_code(ResponseCode::NoPermission)
                    .set_remark(CheetahString::from_static_str("retry topic is not allowed to set mode")),
            ));
        }
        self.message_request_mode_manager.set_message_request_mode(
            request_body.topic.clone(),
            request_body.consumer_group.clone(),
            request_body,
        );
        if let Some(metadata_io) = &self.metadata_io {
            let content = self.message_request_mode_manager.encode_pretty(true);
            metadata_io
                .submit_next_durable(
                    "broker.message-request-mode",
                    self.message_request_mode_manager.config_file_path(),
                    content.into_bytes(),
                    MetadataDeadline::after(Duration::from_secs(5)),
                )
                .await
                .map_err(crate::runtime_to_rocketmq_error)
                .and_then(crate::require_metadata_durability)?;
        } else {
            self.message_request_mode_manager.persist()?;
        }
        Ok(Some(
            self.command_factory
                .create_response_command_with_code(ResponseCode::Success),
        ))
    }

    async fn get_message_request_mode(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let Some(body) = request.body() else {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::InvalidParameter,
                "request body is required",
            )));
        };
        let query = match serde_json::from_slice::<GetMessageRequestModeRequestBody>(body.as_ref()) {
            Ok(query) if supervised_request_mode_target_is_valid(&query.topic, &query.consumer_group) => query,
            _ => {
                return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                    ResponseCode::InvalidParameter,
                    "request-mode target is invalid",
                )));
            }
        };
        let topic = CheetahString::from(&query.topic);
        let consumer_group = CheetahString::from(&query.consumer_group);
        if !self
            .supervised_topic_configs
            .as_ref()
            .is_some_and(|topics| topics.contains_topic(&topic))
            || !self
                .supervised_subscription_groups
                .as_ref()
                .is_some_and(|groups| groups.contains_subscription_group(&consumer_group))
        {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::InvalidParameter,
                "request-mode target is unavailable",
            )));
        }
        let current = self
            .message_request_mode_manager
            .get_message_request_mode(&topic, &consumer_group)
            .map(|value| SupervisedMessageRequestMode {
                mode: match value.mode {
                    MessageRequestMode::Pull => "PULL".to_owned(),
                    MessageRequestMode::Pop => "POP".to_owned(),
                },
                pop_share_queue_num: value.pop_share_queue_num,
            });
        Ok(Some(
            self.command_factory
                .create_success_response_command()
                .set_body(MessageRequestModeStateBody { current }.encode()?),
        ))
    }

    async fn set_message_request_mode_cas(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let Some(body) = request.body() else {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::InvalidParameter,
                "request body is required",
            )));
        };
        let body = match serde_json::from_slice::<SetMessageRequestModeCasRequestBody>(body.as_ref()) {
            Ok(body) => body,
            Err(_) => {
                return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                    ResponseCode::InvalidParameter,
                    "request-mode replacement is invalid",
                )));
            }
        };
        if !supervised_request_mode_target_is_valid(&body.topic, &body.consumer_group)
            || body.replacement.pop_share_queue_num < 0
        {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::InvalidParameter,
                "request-mode replacement is not eligible",
            )));
        }
        let topic = CheetahString::from(&body.topic);
        let consumer_group = CheetahString::from(&body.consumer_group);
        if !self
            .supervised_topic_configs
            .as_ref()
            .is_some_and(|topics| topics.contains_topic(&topic))
            || !self
                .supervised_subscription_groups
                .as_ref()
                .is_some_and(|groups| groups.contains_subscription_group(&consumer_group))
        {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::InvalidParameter,
                "request-mode target is unavailable",
            )));
        }
        let parse_mode = |mode: &str| match mode.trim().to_ascii_uppercase().as_str() {
            "PULL" => Some(MessageRequestMode::Pull),
            "POP" => Some(MessageRequestMode::Pop),
            _ => None,
        };
        let Some(replacement_mode) = parse_mode(&body.replacement.mode) else {
            return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::InvalidParameter,
                "request mode must be PULL or POP",
            )));
        };
        let expected = match &body.expected_state {
            ExpectedMessageRequestMode::Absent => None,
            ExpectedMessageRequestMode::Present {
                mode,
                pop_share_queue_num,
            } => {
                let Some(mode) = parse_mode(mode) else {
                    return Ok(Some(self.command_factory.create_response_command_with_code_remark(
                        ResponseCode::InvalidParameter,
                        "expected request mode is invalid",
                    )));
                };
                Some(SetMessageRequestModeRequestBody {
                    topic: CheetahString::from(&body.topic),
                    consumer_group: CheetahString::from(&body.consumer_group),
                    mode,
                    pop_share_queue_num: *pop_share_queue_num,
                })
            }
        };
        let replacement = SetMessageRequestModeRequestBody {
            topic: CheetahString::from(&body.topic),
            consumer_group: CheetahString::from(&body.consumer_group),
            mode: replacement_mode,
            pop_share_queue_num: body.replacement.pop_share_queue_num,
        };
        let result = self.message_request_mode_manager.set_message_request_mode_if_current(
            CheetahString::from(&body.topic),
            CheetahString::from(&body.consumer_group),
            expected.as_ref(),
            replacement.clone(),
        );
        let (mut code, current, applied, changed, mut persistence, requires_persistence) = match result {
            Ok(update) => (
                ResponseCode::Success,
                Some(update.value),
                true,
                update.changed,
                if update.changed {
                    MutationPersistenceState::Persisted
                } else {
                    MutationPersistenceState::NotRequired
                },
                update.changed,
            ),
            Err(MessageRequestModeCasError::Conflict(current)) => (
                ResponseCode::InvalidParameter,
                current,
                false,
                false,
                MutationPersistenceState::NotRequired,
                false,
            ),
            Err(MessageRequestModeCasError::PersistenceDirty(current)) => (
                ResponseCode::SystemError,
                current,
                false,
                false,
                MutationPersistenceState::Failed,
                false,
            ),
        };
        if requires_persistence {
            let persisted = if let Some(metadata_io) = &self.metadata_io {
                metadata_io
                    .submit_next_durable(
                        "broker.message-request-mode",
                        self.message_request_mode_manager.config_file_path(),
                        self.message_request_mode_manager.encode_pretty(true).into_bytes(),
                        MetadataDeadline::after(Duration::from_secs(5)),
                    )
                    .await
                    .map_err(crate::runtime_to_rocketmq_error)
                    .and_then(crate::require_metadata_durability)
            } else {
                self.message_request_mode_manager.persist()
            };
            if persisted.is_err() {
                code = ResponseCode::SystemError;
                persistence = MutationPersistenceState::Failed;
            }
            self.message_request_mode_manager.complete_supervised_persistence(
                &CheetahString::from(&body.topic),
                &CheetahString::from(&body.consumer_group),
                persisted.is_ok(),
            );
        }
        let current = current.map(|value| SupervisedMessageRequestMode {
            mode: match value.mode {
                MessageRequestMode::Pull => "PULL".to_owned(),
                MessageRequestMode::Pop => "POP".to_owned(),
            },
            pop_share_queue_num: value.pop_share_queue_num,
        });
        Ok(Some(
            self.command_factory.create_response_command_with_code(code).set_body(
                MessageRequestModeMutationResultBody {
                    applied,
                    changed,
                    current,
                    persistence,
                }
                .encode()?,
            ),
        ))
    }
}

fn supervised_request_mode_target_is_valid(topic: &str, consumer_group: &str) -> bool {
    TopicValidator::validate_topic(topic).valid()
        && !TopicValidator::is_system_topic(topic)
        && !topic.starts_with(RETRY_GROUP_TOPIC_PREFIX)
        && validate_subscription_group_name(consumer_group).is_ok()
        && !mix_all::is_sys_consumer_group(consumer_group)
}

fn request_peer_label(origin: &RequestOrigin) -> String {
    match origin {
        RequestOrigin::Network { peer } => peer.address().to_string(),
        RequestOrigin::Embedded { .. } => "embedded".to_owned(),
        _ => "unrecognized-origin".to_owned(),
    }
}

fn allocate(
    consumer_group: &CheetahString,
    current_cid: &CheetahString,
    mq_all: &[MessageQueue],
    cid_all: &[CheetahString],
) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
    if current_cid.is_empty() {
        return Err(rocketmq_error::RocketMQError::IllegalArgument(
            "currentCID is empty".to_string(),
        ));
    }
    if mq_all.is_empty() {
        return Err(rocketmq_error::RocketMQError::IllegalArgument(
            "mqAll is null or mqAll empty".to_string(),
        ));
    }
    if cid_all.is_empty() {
        return Err(rocketmq_error::RocketMQError::IllegalArgument(
            "cidAll is null or cidAll empty".to_string(),
        ));
    }

    let mut result = HashSet::new();
    if !cid_all.contains(current_cid) {
        info!(
            "[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {:?}",
            consumer_group, current_cid, cid_all
        );
        return Ok(result);
    }

    let index = cid_all.iter().position(|cid| cid == current_cid).unwrap();
    result.insert(mq_all[index % mq_all.len()].clone());
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_model::allocation::AllocateMessageQueueAveragely;
    use rocketmq_model::allocation::AllocateMessageQueueAveragelyByCircle;
    use rocketmq_model::allocation::AllocateMessageQueueStrategy;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::message::message_queue::MessageQueue;
    use rocketmq_model::common::topic::TopicValidator;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
    use rocketmq_protocol::protocol::body::supervised_mutation::{
        ExpectedMessageRequestMode, MessageRequestModeMutationResultBody, MutationPersistenceState,
        SetMessageRequestModeCasRequestBody, SupervisedMessageRequestMode,
    };
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
    use rocketmq_protocol::protocol::RemotingDeserializable;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_security_api::AuthenticatedRequestContext;
    use rocketmq_security_api::Decision;
    use rocketmq_security_api::Principal;
    use rocketmq_security_api::RequestPolicy;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::AuthorizedCommandDispatcher;
    use rocketmq_transport::api::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::ResponseBodyKind;
    use rocketmq_transport::api::ServerConfig;
    use rocketmq_transport::api::TransportClientConfig;
    use rocketmq_transport::api::TransportSecurity;
    use rocketmq_transport::api::TransportServer;
    use rocketmq_transport::api::TransportTelemetry;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::EmbeddedRequestHarness;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::allocate;
    use super::supervised_request_mode_target_is_valid;
    use super::QueryAssignmentProcessor;
    use crate::broker_runtime::BrokerRuntime;
    use crate::client::consumer_group_event::ConsumerGroupEvent;
    use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
    use crate::client::manager::consumer_manager::ConsumerManager;
    use crate::config::broker_config::BrokerConfig;
    use crate::config::config_manager::ConfigManager;
    use crate::out_api::broker_outer_api::BrokerOuterAPI;
    use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;

    struct NoopConsumerListener;

    #[test]
    fn supervised_request_mode_target_uses_closed_non_system_names() {
        assert!(supervised_request_mode_target_is_valid(
            "orders_v1",
            "%RETRY%orders_group"
        ));
        for (topic, group) in [
            ("orders.v1", "orders_group"),
            (TopicValidator::RMQ_SYS_SCHEDULE_TOPIC, "orders_group"),
            ("orders", "orders.group"),
            ("orders", "CID_RMQ_SYS_internal"),
        ] {
            assert!(!supervised_request_mode_target_is_valid(topic, group));
        }
    }

    impl ConsumerIdsChangeListener for NoopConsumerListener {
        fn handle(&self, _event: ConsumerGroupEvent, _group: &str, _args: &[&dyn Any]) {}

        fn shutdown(&self) {}
    }

    struct AllowEmbeddedPolicy;

    impl RequestPolicy for AllowEmbeddedPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    fn test_processor(owner: &RuntimeOwner) -> QueryAssignmentProcessor {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let broker_outer_api = BrokerOuterAPI::new(
            Arc::new(TransportClientConfig::default()),
            owner.root_context().component("query-assignment-test.outer-api"),
            TransportTelemetry::noop(),
        );
        let topic_route_info_manager = TopicRouteInfoManager::new(broker_outer_api, 60_000, None);
        topic_route_info_manager.topic_subscribe_info_table.insert(
            CheetahString::from_static_str("assignment-topic"),
            [MessageQueue::from_parts("assignment-topic", "broker-a", 0)]
                .into_iter()
                .collect(),
        );
        let consumer_assignment_view = ConsumerManager::new(Arc::new(NoopConsumerListener), 60_000).assignment_view();
        QueryAssignmentProcessor::new(
            broker_config,
            message_store_config,
            topic_route_info_manager,
            consumer_assignment_view,
        )
    }

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "rocketmq-broker-request-mode-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ))
    }

    async fn supervised_test_runtime(label: &str) -> BrokerRuntime {
        let root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await.is_ok());
        runtime
    }

    #[tokio::test]
    async fn supervised_request_mode_requires_targets_and_reports_noop() {
        let mut runtime = supervised_test_runtime("target-noop").await;
        runtime.init_processor_checked().expect("processors");
        let processor = runtime
            .runtime_state_mut()
            .query_assignment_processor()
            .cloned()
            .expect("query assignment processor");
        let replacement = SupervisedMessageRequestMode {
            mode: "POP".to_owned(),
            pop_share_queue_num: 4,
        };
        let request_body = |expected_state| SetMessageRequestModeCasRequestBody {
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            expected_state,
            replacement: replacement.clone(),
        };

        let mut missing = RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestModeCas).set_body(
            request_body(ExpectedMessageRequestMode::Absent)
                .encode()
                .expect("request"),
        );
        let response = processor
            .set_message_request_mode_cas(&mut missing)
            .await
            .expect("handler")
            .expect("response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert!(processor
            .message_request_mode_manager()
            .get_message_request_mode(&"orders".into(), &"orders-consumer".into())
            .is_none());

        {
            let state = runtime.runtime_state_mut();
            state
                .topic_config_manager()
                .update_topic_config(TopicConfig::with_queues("orders", 4, 4), 0);
            let mut group = SubscriptionGroupConfig::new("orders-consumer".into());
            state
                .subscription_group_manager()
                .update_subscription_group_config(&mut group);
        }
        let mut create = RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestModeCas).set_body(
            request_body(ExpectedMessageRequestMode::Absent)
                .encode()
                .expect("request"),
        );
        let response = processor
            .set_message_request_mode_cas(&mut create)
            .await
            .expect("handler")
            .expect("response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let created = MessageRequestModeMutationResultBody::decode(response.body().expect("body")).expect("result");
        assert!(created.applied);
        assert!(created.changed);

        let mut noop = RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestModeCas).set_body(
            request_body(ExpectedMessageRequestMode::Present {
                mode: "POP".to_owned(),
                pop_share_queue_num: 4,
            })
            .encode()
            .expect("request"),
        );
        let response = processor
            .set_message_request_mode_cas(&mut noop)
            .await
            .expect("handler")
            .expect("response");
        let noop = MessageRequestModeMutationResultBody::decode(response.body().expect("body")).expect("result");
        assert!(noop.applied);
        assert!(!noop.changed);
        assert_eq!(noop.persistence, MutationPersistenceState::NotRequired);

        let persistence_target = processor.message_request_mode_manager().config_file_path();
        let _ = std::fs::remove_file(persistence_target.as_str());
        std::fs::create_dir_all(persistence_target.as_str()).expect("occupy persistence target with a directory");
        let mut persist_failure = RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestModeCas)
            .set_body(
                SetMessageRequestModeCasRequestBody {
                    topic: "orders".to_owned(),
                    consumer_group: "orders-consumer".to_owned(),
                    expected_state: ExpectedMessageRequestMode::Present {
                        mode: "POP".to_owned(),
                        pop_share_queue_num: 4,
                    },
                    replacement: SupervisedMessageRequestMode {
                        mode: "PULL".to_owned(),
                        pop_share_queue_num: 0,
                    },
                }
                .encode()
                .expect("request"),
            );
        let response = processor
            .set_message_request_mode_cas(&mut persist_failure)
            .await
            .expect("handler")
            .expect("response");
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        let outcome = MessageRequestModeMutationResultBody::decode(response.body().expect("body")).expect("result");
        assert!(outcome.applied);
        assert!(outcome.changed);
        assert_eq!(outcome.persistence, MutationPersistenceState::Failed);
        assert_eq!(outcome.current.expect("current").mode, "PULL");

        for replacement in [
            SupervisedMessageRequestMode {
                mode: "PULL".to_owned(),
                pop_share_queue_num: 0,
            },
            SupervisedMessageRequestMode {
                mode: "POP".to_owned(),
                pop_share_queue_num: 4,
            },
        ] {
            let mut follow_up = RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestModeCas)
                .set_body(
                    SetMessageRequestModeCasRequestBody {
                        topic: "orders".to_owned(),
                        consumer_group: "orders-consumer".to_owned(),
                        expected_state: ExpectedMessageRequestMode::Present {
                            mode: "PULL".to_owned(),
                            pop_share_queue_num: 0,
                        },
                        replacement,
                    }
                    .encode()
                    .expect("follow-up request"),
                );
            let response = processor
                .set_message_request_mode_cas(&mut follow_up)
                .await
                .expect("handler")
                .expect("response");
            assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
            let follow_up =
                MessageRequestModeMutationResultBody::decode(response.body().expect("body")).expect("result");
            assert!(!follow_up.applied);
            assert!(!follow_up.changed);
            assert_eq!(follow_up.persistence, MutationPersistenceState::Failed);
            assert_eq!(follow_up.current.expect("current").mode, "PULL");
        }

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn allocate_returns_error_when_current_cid_is_empty() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("");
        let mq_all = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let cid_all = vec![CheetahString::from("consumer1")];

        let result = allocate(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn allocate_returns_error_when_mq_all_is_empty() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![];
        let cid_all = vec![CheetahString::from("consumer1")];

        let result = allocate(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn allocate_returns_error_when_cid_all_is_empty() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let cid_all = vec![];

        let result = allocate(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn allocate_returns_empty_when_current_cid_not_in_cid_all() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer2");
        let mq_all = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let cid_all = vec![CheetahString::from("consumer1")];

        let result = allocate(&consumer_group, &current_cid, &mq_all, &cid_all).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn allocate_returns_correct_queue_for_single_consumer() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let cid_all = vec![CheetahString::from("consumer1")];

        let result = allocate(&consumer_group, &current_cid, &mq_all, &cid_all).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.iter().next().unwrap().queue_id(), 0);
    }

    #[test]
    fn allocate_returns_correct_queue_for_multiple_consumers() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer2");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "broker", 0),
            MessageQueue::from_parts("topic", "broker", 1),
        ];
        let cid_all = vec![CheetahString::from("consumer1"), CheetahString::from("consumer2")];

        let result = allocate(&consumer_group, &current_cid, &mq_all, &cid_all).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.iter().next().unwrap().queue_id(), 1);
    }

    #[test]
    fn allocate_for_pop_validates_empty_mq_list() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all: Vec<MessageQueue> = vec![];
        let cid_all = vec![CheetahString::from("consumer1")];
        let strategy = Arc::new(AllocateMessageQueueAveragely);

        let result = strategy.allocate(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn allocate_for_pop_validates_empty_consumer_list() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let cid_all: Vec<CheetahString> = vec![];
        let strategy = Arc::new(AllocateMessageQueueAveragely);

        let result = strategy.allocate(&consumer_group, &current_cid, &mq_all, &cid_all);
        assert!(result.is_err());
    }

    #[test]
    fn allocate_averagely_distributes_queues_evenly() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer2");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "broker", 0),
            MessageQueue::from_parts("topic", "broker", 1),
            MessageQueue::from_parts("topic", "broker", 2),
            MessageQueue::from_parts("topic", "broker", 3),
        ];
        let cid_all = vec![CheetahString::from("consumer1"), CheetahString::from("consumer2")];
        let strategy = Arc::new(AllocateMessageQueueAveragely);

        let result = strategy
            .allocate(&consumer_group, &current_cid, &mq_all, &cid_all)
            .unwrap();
        // With 4 queues and 2 consumers, consumer2 should get 2 queues (2,3)
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn allocate_by_circle_distributes_round_robin() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "broker", 0),
            MessageQueue::from_parts("topic", "broker", 1),
            MessageQueue::from_parts("topic", "broker", 2),
        ];
        let cid_all = vec![CheetahString::from("consumer1"), CheetahString::from("consumer2")];
        let strategy = Arc::new(AllocateMessageQueueAveragelyByCircle);

        let result = strategy
            .allocate(&consumer_group, &current_cid, &mq_all, &cid_all)
            .unwrap();
        // With round-robin, consumer1 should get queue 0 and 2
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn embedded_unknown_request_returns_a_reply_response() {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("query-assignment-test"))
            .expect("runtime configuration is valid")
            .build()
            .expect("QueryAssignment test runtime");
        let context = owner.root_context().component("query-assignment-test.request");
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            test_processor(&owner),
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarness::new(
            dispatcher,
            context.task_group().clone(),
            Principal::new("query-assignment-test"),
        );

        let outcome = harness
            .dispatch(None, RemotingCommand::create_remoting_command(-98_453).set_opaque(320))
            .await
            .expect("embedded QueryAssignment response");
        let EmbeddedDispatchOutcome::Reply(response) = outcome else {
            panic!("QueryAssignment unknown request must return a reply response");
        };

        assert_eq!(response.response_code(), ResponseCode::RequestCodeNotSupported as i32);
        assert_eq!(response.body_kind(), ResponseBodyKind::Empty);

        drop(harness);
        drop(context);
        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
    }

    #[tokio::test]
    async fn embedded_query_success_returns_an_owned_body_plan() {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("query-assignment-body-test"))
            .expect("runtime configuration is valid")
            .build()
            .expect("QueryAssignment body test runtime");
        let context = owner.root_context().component("query-assignment-body-test.request");
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            test_processor(&owner),
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarness::new(
            dispatcher,
            context.task_group().clone(),
            Principal::new("query-assignment-body-test"),
        );
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from_static_str("assignment-topic"),
            consumer_group: CheetahString::from_static_str("assignment-group"),
            client_id: CheetahString::from_static_str("assignment-client"),
            strategy_name: CheetahString::from_static_str("AVG"),
            message_model: MessageModel::Broadcasting,
        }
        .encode()
        .expect("encode QueryAssignment request body");
        let request = RemotingCommand::create_remoting_command(RequestCode::QueryAssignment)
            .set_body(body)
            .set_opaque(322);

        let outcome = harness
            .dispatch(None, request)
            .await
            .expect("embedded QueryAssignment success response");
        let EmbeddedDispatchOutcome::Reply(response) = outcome else {
            panic!("QueryAssignment success must return a reply response");
        };

        assert_eq!(response.response_code(), ResponseCode::Success as i32);
        assert_eq!(response.body_kind(), ResponseBodyKind::Bytes);
        assert!(response.body_len() > 0);
        assert_eq!(response.body_part_count(), 1);

        drop(harness);
        drop(context);
        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
    }

    #[tokio::test]
    async fn network_query_success_preserves_opaque_and_body() {
        const ORIGINAL_OPAQUE: i32 = 9_014;
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("query-assignment-network-test"))
            .expect("runtime configuration is valid")
            .build()
            .expect("QueryAssignment network test runtime");
        let server_context = owner.root_context().component("query-assignment-network-test.server");
        let runner_context = owner.root_context().component("query-assignment-network-test.runner");
        let server = TransportServer::new(
            Arc::new(ServerConfig {
                bind_address: "127.0.0.1".to_owned(),
                listen_port: 0,
                ..ServerConfig::default()
            }),
            server_context,
            test_processor(&owner),
        );
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (startup_sender, startup_receiver) = oneshot::channel();
        let (result_sender, result_receiver) = oneshot::channel();
        runner_context
            .spawn_service("query-assignment-server", async move {
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
            .expect("spawn QueryAssignment server");

        let address = startup_receiver
            .await
            .expect("QueryAssignment startup channel")
            .expect("QueryAssignment server startup");
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from_static_str("assignment-topic"),
            consumer_group: CheetahString::from_static_str("assignment-group"),
            client_id: CheetahString::from_static_str("assignment-client"),
            strategy_name: CheetahString::from_static_str("AVG"),
            message_model: MessageModel::Broadcasting,
        }
        .encode()
        .expect("encode QueryAssignment network request body");
        let mut client = Connection::new(
            TcpStream::connect(address)
                .await
                .expect("connect QueryAssignment client"),
        );
        client
            .send_command(
                RemotingCommand::create_remoting_command(RequestCode::QueryAssignment)
                    .set_body(body)
                    .set_opaque(ORIGINAL_OPAQUE),
            )
            .await
            .expect("send QueryAssignment request");

        let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
            .await
            .expect("QueryAssignment response deadline")
            .expect("QueryAssignment connection remains open")
            .expect("QueryAssignment response frame");
        assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
        assert_eq!(response.code(), ResponseCode::Success as i32);
        assert!(response.body().is_some_and(|body| !body.is_empty()));

        client.shutdown().await.expect("shutdown QueryAssignment client");
        let _ = shutdown_sender.send(());
        let report = tokio::time::timeout(Duration::from_secs(2), result_receiver)
            .await
            .expect("QueryAssignment shutdown deadline")
            .expect("QueryAssignment shutdown result channel")
            .expect("QueryAssignment shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
    }
}
