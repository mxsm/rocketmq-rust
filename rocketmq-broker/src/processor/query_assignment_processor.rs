/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::load_balance::message_request_mode_manager::MessageRequestModeManager;
 use cheetah_string::CheetahString;
 use rocketmq_client_rust::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
 use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely;
 use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_averagely_by_circle::AllocateMessageQueueAveragelyByCircle;
 use rocketmq_common::common::config_manager::ConfigManager;
 use rocketmq_remoting::code::request_code::RequestCode;
 use rocketmq_remoting::net::channel::Channel;
 use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
 use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
 use rocketmq_store::config::message_store_config::MessageStoreConfig;
 use std::collections::{HashMap, HashSet};
 use std::sync::Arc;
use tracing::{info, warn};
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::{RemotingDeserializable, RemotingSerializable};
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::broker_error::BrokerError;
use crate::broker_error::BrokerError::IllegalArgumentError;
use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;
use crate::Result;

pub struct QueryAssignmentProcessor {
    message_request_mode_manager: MessageRequestModeManager,
    load_strategy: HashMap<CheetahString, Arc<dyn AllocateMessageQueueStrategy>>,
    message_store_config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
    topic_route_info_manager: Arc<TopicRouteInfoManager>,
    consumer_manager: Arc<ConsumerManager>,
}

impl QueryAssignmentProcessor {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_route_info_manager: Arc<TopicRouteInfoManager>,
        consumer_manager: Arc<ConsumerManager>,
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
        let manager = MessageRequestModeManager::new(message_store_config.clone());
        let _ = manager.load();
        Self {
            message_request_mode_manager: manager,
            load_strategy,
            message_store_config,
            broker_config,
            topic_route_info_manager,
            consumer_manager,
        }
    }
}

impl QueryAssignmentProcessor {
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::QueryAssignment => self.query_assignment(channel, ctx, request).await,
            RequestCode::SetMessageRequestMode => {
                self.set_message_request_mode(channel, ctx, request).await
            }
            _ => None,
        }
    }

    async fn query_assignment(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_body =
            QueryAssignmentRequestBody::decode(request.get_body().expect("empty body"))
                .expect("decode QueryAssignmentRequestBody failed");
        let set_message_request_mode_request_body = self
            .message_request_mode_manager
            .get_message_request_mode(&request_body.topic, &request_body.consumer_group);
        let set_message_request_mode_request_body =
            if let Some(set_message_request_mode_request_body) =
                set_message_request_mode_request_body
            {
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

        //do load balance, get message queues
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
            message_queues
                .into_iter()
                .map(|mq| MessageQueueAssignment {
                    message_queue: Some(mq),
                    mode,
                    attachments: None,
                })
                .collect()
        } else {
            HashSet::with_capacity(0)
        };
        let body = QueryAssignmentResponseBody {
            message_queue_assignments: assignments,
        };
        Some(RemotingCommand::create_response_command().set_body(body.encode()))
    }

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
            // handle broadcasting consumer
            MessageModel::Broadcasting => {
                let assigned_queue_set = self
                    .topic_route_info_manager
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
                let mq_set = if mix_all::is_lmq(Some(topic.as_str())) {
                    let mut set = HashSet::new();
                    let queue = MessageQueue::from_parts(
                        topic.clone(),
                        self.broker_config.broker_name.clone(),
                        mix_all::LMQ_QUEUE_ID as i32,
                    );
                    set.insert(queue);
                    Some(set)
                } else {
                    self.topic_route_info_manager
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

                if !self.broker_config.server_load_balancer_enable {
                    return mq_set;
                }
                let consumer_group_info = self
                    .consumer_manager
                    .get_consumer_group_info(consumer_group);
                let mut cid_all = if let Some(consumer_group_info) = consumer_group_info {
                    consumer_group_info.get_all_client_ids()
                } else {
                    vec![]
                };
                if cid_all.is_empty() {
                    warn!(
                        "QueryLoad: no assignment for group[{}] topic[{}], get consumer id list \
                         failed",
                        consumer_group, topic
                    );
                    return None;
                }
                let mut mq_all = mq_set.unwrap().into_iter().collect::<Vec<MessageQueue>>();
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
                let strategy = strategy.unwrap();
                let result =
                    if set_message_request_mode_request_body.mode == MessageRequestMode::Pop {
                        self.allocate_for_pop(
                            strategy,
                            consumer_group,
                            client_id,
                            mq_all.as_slice(),
                            cid_all.as_slice(),
                            set_message_request_mode_request_body.pop_share_queue_num,
                        )
                    } else {
                        match strategy.allocate(
                            consumer_group,
                            client_id,
                            mq_all.as_slice(),
                            cid_all.as_slice(),
                        ) {
                            Ok(value) => Ok(value.into_iter().collect::<HashSet<MessageQueue>>()),
                            Err(e) => Err(BrokerError::ClientError(e)),
                        }
                    };
                match result {
                    Ok(value) => Some(value),
                    Err(_) => None,
                }
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
    ) -> Result<HashSet<MessageQueue>> {
        if pop_share_queue_num <= 0 || pop_share_queue_num >= cid_all.len() as i32 - 1 {
            Ok(mq_all
                .iter()
                .map(|mq| {
                    MessageQueue::from_parts(
                        mq.get_topic_cs().clone(),
                        mq.get_broker_name().clone(),
                        -1,
                    )
                })
                .collect::<HashSet<MessageQueue>>())
        } else if cid_all.len() <= mq_all.len() {
            let mut allocate_result = strategy
                .allocate(consumer_group, current_cid, mq_all, cid_all)
                .unwrap();
            let index = cid_all.iter().position(|cid| cid == current_cid);
            if let Some(mut index) = index {
                for _i in 1..pop_share_queue_num {
                    index += 1;
                    index %= cid_all.len();
                    let result = strategy
                        .allocate(consumer_group, &cid_all[index], mq_all, cid_all)
                        .unwrap();
                    allocate_result.extend(result);
                }
            }
            Ok(allocate_result
                .into_iter()
                .collect::<HashSet<MessageQueue>>())
        } else {
            //make sure each cid is assigned
            allocate(consumer_group, current_cid, mq_all, cid_all)
        }
    }

    async fn set_message_request_mode(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_body =
            SetMessageRequestModeRequestBody::decode(request.get_body().expect("empty body"))
                .expect("decode SetMessageRequestModeRequestBody failed");
        if request_body.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NoPermission)
                    .set_remark(CheetahString::from_static_str(
                        "retry topic is not allowed to set mode",
                    )),
            );
        }
        self.message_request_mode_manager.set_message_request_mode(
            request_body.topic.clone(),
            request_body.consumer_group.clone(),
            request_body,
        );
        self.message_request_mode_manager.persist();
        Some(RemotingCommand::create_response_command_with_code(
            ResponseCode::Success,
        ))
    }
}

fn allocate(
    consumer_group: &CheetahString,
    current_cid: &CheetahString,
    mq_all: &[MessageQueue],
    cid_all: &[CheetahString],
) -> Result<HashSet<MessageQueue>> {
    if current_cid.is_empty() {
        return Err(IllegalArgumentError("currentCID is empty".to_string()));
    }
    if mq_all.is_empty() {
        return Err(IllegalArgumentError(
            "mqAll is null or mqAll empty".to_string(),
        ));
    }
    if cid_all.is_empty() {
        return Err(IllegalArgumentError(
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
        let cid_all = vec![
            CheetahString::from("consumer1"),
            CheetahString::from("consumer2"),
        ];

        let result = allocate(&consumer_group, &current_cid, &mq_all, &cid_all).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.iter().next().unwrap().get_queue_id(), 1);
    }
}
