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

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::attribute_parser::AttributeParser;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::TopicFilterType;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::admin::topic_offset::TopicOffset;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::create_topic_list_request_body::CreateTopicListRequestBody;
use rocketmq_remoting::protocol::body::group_list::GroupList;
use rocketmq_remoting::protocol::body::topic::topic_list::TopicList;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_remoting::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_remoting::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
use rocketmq_remoting::protocol::header::get_topic_stats_request_header::GetTopicStatsRequestHeader;
use rocketmq_remoting::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;
use rocketmq_remoting::protocol::header::query_topics_by_consumer_request_header::QueryTopicsByConsumerRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct TopicRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> TopicRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        TopicRequestHandler { broker_runtime_inner }
    }
}

impl<MS: MessageStore> TopicRequestHandler<MS> {
    pub async fn update_and_create_topic(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<CreateTopicRequestHeader>()
            .unwrap();
        info!(
            "Broker receive request to update or create topic={}, caller address={}",
            request_header.topic,
            channel.remote_address()
        );
        let topic = request_header.topic.clone();
        let result = TopicValidator::validate_topic(topic.as_str());
        if !result.valid() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(result.remark().clone()),
            ));
        }
        if self
            .broker_runtime_inner
            .broker_config()
            .validate_system_topic_when_update_topic
            && TopicValidator::is_system_topic(topic.as_str())
        {
            return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                "The topic[{}] is conflict with system topic.",
                topic.as_str()
            ))));
        }

        let attributes = match AttributeParser::parse_to_map(
            request_header
                .attributes
                .clone()
                .unwrap_or(CheetahString::empty())
                .as_str(),
        ) {
            Ok(value) => value.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
            Err(err) => {
                return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(err)));
            }
        };

        let topic_config = ArcMut::new(TopicConfig {
            topic_name: Some(topic.clone()),
            read_queue_nums: request_header.read_queue_nums as u32,
            write_queue_nums: request_header.write_queue_nums as u32,
            perm: request_header.perm as u32,
            topic_filter_type: TopicFilterType::from(request_header.topic_filter_type.as_str()),
            topic_sys_flag: if let Some(value) = request_header.topic_sys_flag {
                value as u32
            } else {
                0
            },
            order: request_header.order,
            attributes,
        });
        if topic_config.get_topic_message_type() == TopicMessageType::Mixed
            && !self.broker_runtime_inner.broker_config().enable_mixed_message_type
        {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("MIXED message type is not supported."),
            ));
        }

        let topic_config_origin = self
            .broker_runtime_inner
            .topic_config_manager()
            .get_topic_config(topic.as_str())
            .clone();
        if topic_config_origin.is_some() && topic_config == topic_config_origin.unwrap() {
            info!(
                "Broker receive request to update or create topic={}, but topicConfig has  no changes , so \
                 idempotent, caller address={}",
                topic.as_str(),
                channel.remote_address(),
            );
            return Ok(Some(response.set_code(ResponseCode::Success)));
        }
        self.broker_runtime_inner
            .topic_config_manager_mut()
            .update_topic_config(topic_config.clone());

        if self.broker_runtime_inner.broker_config().enable_single_topic_register {
            self.broker_runtime_inner
                .topic_config_manager()
                .broker_runtime_inner()
                .register_single_topic_all(topic_config.clone())
                .await;
        } else {
            BrokerRuntimeInner::<MS>::register_increment_broker_data(
                self.broker_runtime_inner.clone(),
                vec![topic_config],
                self.broker_runtime_inner
                    .topic_config_manager()
                    .data_version()
                    .as_ref()
                    .clone(),
            )
            .await;
        }

        Ok(Some(response.set_code(ResponseCode::Success)))
    }

    pub async fn update_and_create_topic_list(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_body = CreateTopicListRequestBody::decode(request.body().as_ref().unwrap().as_ref()).unwrap();
        let mut topic_names = String::new();
        for topic_config in request_body.topic_config_list.iter() {
            topic_names.push_str(topic_config.topic_name.as_ref().unwrap().as_str());
            topic_names.push(';');
        }
        info!(
            "AdminBrokerProcessor#updateAndCreateTopicList: topicNames: {}, called by {}",
            topic_names,
            channel.remote_address()
        );
        let response = RemotingCommand::create_response_command();
        for topic_config in request_body.topic_config_list.iter() {
            let topic = topic_config.topic_name.as_ref().unwrap().as_str();
            let result = TopicValidator::validate_topic(topic);
            if !result.valid() {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(result.remark().clone()),
                ));
            }
            if self
                .broker_runtime_inner
                .broker_config()
                .validate_system_topic_when_update_topic
                && TopicValidator::is_system_topic(topic)
            {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(format!("The topic[{topic}] is conflict with system topic.",)),
                ));
            }
            if topic_config.get_topic_message_type() == TopicMessageType::Mixed
                && !self.broker_runtime_inner.broker_config().enable_mixed_message_type
            {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("MIXED message type is not supported.".to_string()),
                ));
            }
            let topic_config_origin = self.broker_runtime_inner.topic_config_manager().get_topic_config(topic);
            if topic_config_origin.is_some() && topic_config.clone() == topic_config_origin.unwrap() {
                info!(
                    "Broker receive request to update or create topic={}, but topicConfig has  no changes , so \
                     idempotent, caller address={}",
                    topic,
                    channel.remote_address(),
                );
                return Ok(Some(response.set_code(ResponseCode::Success)));
            }
        }

        self.broker_runtime_inner
            .topic_config_manager_mut()
            .update_topic_config_list(request_body.topic_config_list.as_mut_slice());
        if self.broker_runtime_inner.broker_config().enable_single_topic_register {
            for topic_config in request_body.topic_config_list.iter() {
                self.broker_runtime_inner
                    .topic_config_manager()
                    .broker_runtime_inner()
                    .register_single_topic_all(topic_config.clone())
                    .await;
            }
        } else {
            BrokerRuntimeInner::<MS>::register_increment_broker_data(
                self.broker_runtime_inner.clone(),
                request_body.topic_config_list,
                self.broker_runtime_inner
                    .topic_config_manager()
                    .data_version()
                    .as_ref()
                    .clone(),
            )
            .await;
        }
        Ok(Some(response.set_code(ResponseCode::Success)))
    }

    pub async fn delete_topic(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<DeleteTopicRequestHeader>()
            .unwrap();
        let topic = &request_header.topic;
        info!(
            "AdminBrokerProcessor#deleteTopic: broker receive request to delete topic={}, caller={}",
            topic,
            channel.remote_address()
        );
        if topic.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("he specified topic is blank."),
            ));
        }
        if self
            .broker_runtime_inner
            .broker_config()
            .validate_system_topic_when_update_topic
            && TopicValidator::is_system_topic(topic)
        {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(format!("The topic[{topic}] is conflict with system topic.",)),
            ));
        }
        let groups = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .which_group_by_topic(topic);
        for group in groups.iter() {
            let pop_retry_topic_v2 =
                CheetahString::from_string(KeyBuilder::build_pop_retry_topic(topic, group.as_str(), true));
            if self
                .broker_runtime_inner
                .topic_config_manager()
                .select_topic_config(pop_retry_topic_v2.as_ref())
                .is_some()
            {
                self.delete_topic_in_broker(pop_retry_topic_v2.as_ref());
            }
            let pop_retry_topic_v1 =
                CheetahString::from_string(KeyBuilder::build_pop_retry_topic_v1(topic, group.as_str()));
            if self
                .broker_runtime_inner
                .topic_config_manager()
                .select_topic_config(pop_retry_topic_v1.as_ref())
                .is_some()
            {
                self.delete_topic_in_broker(pop_retry_topic_v1.as_ref());
            }
        }
        self.delete_topic_in_broker(topic);
        Ok(Some(response.set_code(ResponseCode::Success)))
    }

    pub async fn get_all_topic_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let topic_config_and_mapping_serialize_wrapper = TopicConfigAndMappingSerializeWrapper {
            topic_queue_mapping_detail_map: self
                .broker_runtime_inner
                .topic_queue_mapping_manager()
                .topic_queue_mapping_table
                .clone(),
            mapping_data_version: self
                .broker_runtime_inner
                .topic_queue_mapping_manager()
                .data_version
                .lock()
                .clone(),
            topic_config_serialize_wrapper: TopicConfigSerializeWrapper {
                data_version: self
                    .broker_runtime_inner
                    .topic_config_manager()
                    .data_version()
                    .as_ref()
                    .clone(),
                topic_config_table: self
                    .broker_runtime_inner
                    .topic_config_manager()
                    .topic_config_table_hash_map(),
            },
            ..Default::default()
        };
        let content = topic_config_and_mapping_serialize_wrapper
            .serialize_json()
            .expect("encode failed");
        if !content.is_empty() {
            response.set_body_mut_ref(content);
        }
        Ok(Some(response))
    }

    pub async fn get_system_topic_list_from_broker(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let topics = TopicValidator::get_system_topic_set();
        let topic_list = TopicList {
            topic_list: topics,
            broker_addr: None,
        };
        response.set_body_mut_ref(topic_list.encode().expect("encode TopicList failed"));
        Ok(Some(response))
    }

    pub async fn get_topic_stats_info(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<GetTopicStatsRequestHeader>()
            .unwrap();
        let topic = request_header.topic.as_ref();
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(topic);
        if topic_config.is_none() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(format!("The topic[{topic}] not exist.")),
            ));
        }
        let topic_config = topic_config.unwrap();
        let max_queue_nums = topic_config.write_queue_nums.max(topic_config.read_queue_nums);
        let mut topic_stats_table = TopicStatsTable::new();
        let mut map = HashMap::new();
        for i in 0..max_queue_nums {
            let mut message_queue = MessageQueue::new();
            message_queue.set_topic(topic.clone());
            message_queue.set_broker_name(self.broker_runtime_inner.broker_config().broker_name().clone());
            message_queue.set_queue_id(i as i32);
            let mut topic_offset = TopicOffset::new();
            let min = std::cmp::max(
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_min_offset_in_queue(topic, i as i32),
                0,
            );
            let max = std::cmp::max(
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_max_offset_in_queue(topic, i as i32),
                0,
            );
            let mut timestamp = 0;
            if max > 0 {
                timestamp = self
                    .broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_message_store_timestamp(topic, i as i32, max - 1);
            }
            topic_offset.set_min_offset(min);
            topic_offset.set_max_offset(max);
            topic_offset.set_last_update_timestamp(timestamp);
            map.insert(message_queue, topic_offset);
        }
        topic_stats_table.set_offset_table(map);
        response.set_body_mut_ref(topic_stats_table.encode().expect("encode TopicStatsTable failed"));
        Ok(Some(response))
    }

    pub async fn get_topic_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<GetTopicConfigRequestHeader>()?;
        let topic = &request_header.topic;
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(topic);
        if topic_config.is_none() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(format!("No topic in this broker. topic: {topic}")),
            ));
        }
        let mut topic_queue_mapping_detail: Option<ArcMut<TopicQueueMappingDetail>> = None;
        if let Some(value) = request_header.topic_request_header.as_ref() {
            if let Some(lo) = value.get_lo() {
                if *lo {
                    topic_queue_mapping_detail = self
                        .broker_runtime_inner
                        .topic_queue_mapping_manager()
                        .topic_queue_mapping_table
                        .get(topic)
                        .as_deref()
                        .cloned();
                }
            }
        }
        let topic_config = (*topic_config.unwrap()).clone();
        let topic_config_and_queue_mapping = TopicConfigAndQueueMapping::new(topic_config, topic_queue_mapping_detail);
        response.set_body_mut_ref(topic_config_and_queue_mapping.encode()?);
        Ok(Some(response))
    }

    pub async fn query_topic_consume_by_who(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<QueryTopicConsumeByWhoRequestHeader>()?;
        let topic = request_header.topic.as_ref();
        let mut groups = self
            .broker_runtime_inner
            .consumer_manager()
            .query_topic_consume_by_who(topic);
        let group_in_offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .which_group_by_topic(topic);
        groups.extend(group_in_offset);
        let group_list = GroupList { group_list: groups };
        response.set_body_mut_ref(group_list.encode().expect("encode GroupList failed"));
        Ok(Some(response))
    }

    pub async fn query_topics_by_consumer(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<QueryTopicsByConsumerRequestHeader>()
            .unwrap();
        let topics = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .which_topic_by_consumer(request_header.get_group());
        let broker_addr = format!(
            "{}:{}",
            self.broker_runtime_inner.broker_config().broker_ip1,
            self.broker_runtime_inner.server_config().listen_port
        );
        let topic_list = TopicList {
            topic_list: topics.into_iter().collect(),
            broker_addr: Some(broker_addr.into()),
        };
        response.set_body_mut_ref(topic_list.encode().expect("encode TopicList failed"));
        Ok(Some(response))
    }

    fn delete_topic_in_broker(&mut self, topic: &CheetahString) {
        self.broker_runtime_inner
            .topic_config_manager()
            .delete_topic_config(topic);
        self.broker_runtime_inner.topic_queue_mapping_manager().delete(topic);
        self.broker_runtime_inner
            .consumer_offset_manager()
            .clean_offset_by_topic(topic);
        self.broker_runtime_inner
            .pop_inflight_message_counter()
            .clear_in_flight_message_num_by_topic_name(topic);
        self.broker_runtime_inner
            .message_store_mut()
            .as_mut()
            .unwrap()
            .delete_topics(vec![topic]);
    }
}
