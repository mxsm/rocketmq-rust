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

use crate::broker_runtime::BrokerRuntimeInner;

use crate::load_balance::message_request_mode_manager::MessageRequestModeManager;

use cheetah_string::CheetahString;
use rocketmq_client_rust::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely;
use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_averagely_by_circle::AllocateMessageQueueAveragelyByCircle;

use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;

use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::base::message_store::MessageStore;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;
use tracing::warn;

/// A processor for handling query assignments in the RocketMQ broker.
///
/// This struct manages the message request modes and load balancing strategies
/// for message queues. It interacts with the broker runtime to process assignment
/// requests and allocate message queues to consumers.
///
/// # Type Parameters
///
/// * `MS` - A type that implements the `MessageStore` trait, representing the message store used by
///   the broker.
pub struct QueryAssignmentProcessor<MS: MessageStore> {
    // Manages the message request modes for different topics and consumer groups.
    message_request_mode_manager: MessageRequestModeManager,

    // A map of load balancing strategies for message queue allocation.
    load_strategy: HashMap<CheetahString, Arc<dyn AllocateMessageQueueStrategy>>,

    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> RequestProcessor for QueryAssignmentProcessor<MS>
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
        info!("QueryAssignmentProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::QueryAssignment | RequestCode::SetMessageRequestMode => {
                self.process_request_inner(channel, ctx, request_code, request).await
            }
            _ => {
                warn!(
                    "QueryAssignmentProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("QueryAssignmentProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS: MessageStore> QueryAssignmentProcessor<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
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
        let manager = MessageRequestModeManager::new(Arc::new(broker_runtime_inner.message_store_config().clone()));
        let _ = manager.load();
        Self {
            message_request_mode_manager: manager,
            load_strategy,
            broker_runtime_inner,
        }
    }

    pub fn message_request_mode_manager(&self) -> &MessageRequestModeManager {
        &self.message_request_mode_manager
    }
}

impl<MS: MessageStore> QueryAssignmentProcessor<MS> {
    pub async fn process_request_inner(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::QueryAssignment => self.query_assignment(channel, ctx, request).await,
            RequestCode::SetMessageRequestMode => self.set_message_request_mode(channel, ctx, request).await,
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
    /// * `channel` - The network channel
    /// * `_ctx` - Connection handler context (unused)
    /// * `request` - The remoting command containing QueryAssignmentRequestBody
    ///
    /// # Returns
    ///
    /// A RemotingCommand response containing QueryAssignmentResponseBody with assigned queues
    async fn query_assignment(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if request.get_body().is_none() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "empty body",
            )));
        }
        // Safe to unwrap: already checked is_none() above
        let request_body = QueryAssignmentRequestBody::decode(request.get_body().unwrap())?;

        // Validate required fields
        if request_body.topic.is_empty() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "topic is empty",
            )));
        }
        if request_body.consumer_group.is_empty() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "consumerGroup is empty",
            )));
        }
        if request_body.client_id.is_empty() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
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
                    body.mode = self.broker_runtime_inner.broker_config().default_message_request_mode;
                }
                if body.mode == MessageRequestMode::Pop {
                    body.pop_share_queue_num = self.broker_runtime_inner.broker_config().default_pop_share_queue_num;
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
                channel,
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
            RemotingCommand::create_response_command().set_body(body.encode()?),
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
    /// * `channel` - A `Channel` representing the network channel.
    ///
    /// # Returns
    ///
    /// An `Option<HashSet<MessageQueue>>` containing the allocated message queues, or `None` if no
    /// queues are allocated.
    async fn do_load_balance(
        &mut self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        client_id: &CheetahString,
        message_model: MessageModel,
        strategy_name: &CheetahString,
        set_message_request_mode_request_body: SetMessageRequestModeRequestBody,
        channel: Channel,
    ) -> Option<HashSet<MessageQueue>> {
        match message_model {
            // handle broadcasting consumer, this mode returns all message queues
            MessageModel::Broadcasting => {
                let assigned_queue_set = self
                    .broker_runtime_inner
                    .topic_route_info_manager()
                    .get_topic_subscribe_info(topic)
                    .await;
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
                        self.broker_runtime_inner.broker_config().broker_name().clone(),
                        mix_all::LMQ_QUEUE_ID as i32,
                    );
                    set.insert(queue);
                    Some(set)
                } else {
                    self.broker_runtime_inner
                        .topic_route_info_manager()
                        .get_topic_subscribe_info(topic)
                        .await
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

                if !self.broker_runtime_inner.broker_config().server_load_balancer_enable {
                    return mq_set;
                }
                // get all consumer ids for the consumer group
                let consumer_group_info = self
                    .broker_runtime_inner
                    .consumer_manager()
                    .get_consumer_group_info(consumer_group);
                let mut cid_all = consumer_group_info.map_or_else(Vec::new, |info| info.get_all_client_ids());
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
                    warn!(
                        "QueryLoad: unsupported strategy [{}],  {}",
                        strategy_name,
                        channel.remote_address()
                    );
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
                .map(|mq| MessageQueue::from_parts(mq.get_topic_cs().clone(), mq.get_broker_name().clone(), -1))
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
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if request.get_body().is_none() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "empty body",
            )));
        }
        // Safe to unwrap: already checked is_none() above
        let request_body = SetMessageRequestModeRequestBody::decode(request.get_body().unwrap())?;
        if request_body.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NoPermission)
                    .set_remark(CheetahString::from_static_str("retry topic is not allowed to set mode")),
            ));
        }
        self.message_request_mode_manager.set_message_request_mode(
            request_body.topic.clone(),
            request_body.consumer_group.clone(),
            request_body,
        );
        self.message_request_mode_manager.persist();
        Ok(Some(RemotingCommand::create_response_command_with_code(
            ResponseCode::Success,
        )))
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
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;

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
        assert_eq!(result.iter().next().unwrap().get_queue_id(), 0);
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
        assert_eq!(result.iter().next().unwrap().get_queue_id(), 1);
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
}
