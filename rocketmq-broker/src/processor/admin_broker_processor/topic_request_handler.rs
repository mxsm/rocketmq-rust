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

use std::collections::HashMap;

use bytes::Bytes;
use rocketmq_common::common::attribute::attribute_parser::AttributeParser;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::TopicFilterType;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::create_topic_list_request_body::CreateTopicListRequestBody;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_remoting::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_store::log_file::MessageStore;
use tracing::info;

use crate::processor::admin_broker_processor::Inner;

#[derive(Clone)]
pub(super) struct TopicRequestHandler {
    inner: Inner,
}

impl TopicRequestHandler {
    pub fn new(inner: Inner) -> Self {
        TopicRequestHandler { inner }
    }
}

impl TopicRequestHandler {
    pub async fn update_and_create_topic(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<CreateTopicRequestHeader>()
            .unwrap();
        info!(
            "Broker receive request to update or create topic={}, caller address={}",
            request_header.topic,
            channel.remote_address()
        );
        let topic = request_header.topic.as_str();
        let result = TopicValidator::validate_topic(topic);
        if !result.valid() {
            return Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(Some(result.remark().to_string())),
            );
        }
        if self
            .inner
            .broker_config
            .validate_system_topic_when_update_topic
            && TopicValidator::is_system_topic(topic)
        {
            return Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(Some(format!(
                        "The topic[{}] is conflict with system topic.",
                        topic
                    ))),
            );
        }

        let attributes = match AttributeParser::parse_to_map(
            request_header
                .attributes
                .clone()
                .unwrap_or("".to_string())
                .as_str(),
        ) {
            Ok(value) => value,
            Err(err) => {
                return Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(Some(err)),
                );
            }
        };
        let mut topic_config = TopicConfig {
            topic_name: Some(topic.to_string()),
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
        };
        if topic_config.get_topic_message_type() == TopicMessageType::Mixed
            && !self.inner.broker_config.enable_mixed_message_type
        {
            return Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(Some("MIXED message type is not supported.".to_string())),
            );
        }

        let topic_config_origin = self
            .inner
            .topic_config_manager
            .topic_config_table()
            .lock()
            .get(topic)
            .cloned();
        if topic_config_origin.is_some() && topic_config == topic_config_origin.unwrap() {
            info!(
                "Broker receive request to update or create topic={}, but topicConfig has  no \
                 changes , so idempotent, caller address={}",
                topic,
                channel.remote_address(),
            );
            return Some(response.set_code(ResponseCode::Success));
        }
        self.inner
            .topic_config_manager
            .update_topic_config(&mut topic_config);

        if self.inner.broker_config.enable_single_topic_register {
            self.inner
                .topic_config_manager
                .broker_runtime_inner()
                .register_single_topic_all(topic_config)
                .await;
        } else {
            self.inner
                .topic_config_manager
                .broker_runtime_inner()
                .register_increment_broker_data(
                    vec![topic_config],
                    self.inner
                        .topic_config_manager
                        .data_version()
                        .as_ref()
                        .clone(),
                )
                .await;
        }

        Some(response.set_code(ResponseCode::Success))
    }

    pub async fn update_and_create_topic_list(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut request_body =
            CreateTopicListRequestBody::decode(request.body().as_ref().unwrap().as_ref()).unwrap();
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
                return Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(Some(result.remark().to_string())),
                );
            }
            if self
                .inner
                .broker_config
                .validate_system_topic_when_update_topic
                && TopicValidator::is_system_topic(topic)
            {
                return Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(Some(format!(
                            "The topic[{}] is conflict with system topic.",
                            topic
                        ))),
                );
            }
            if topic_config.get_topic_message_type() == TopicMessageType::Mixed
                && !self.inner.broker_config.enable_mixed_message_type
            {
                return Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(Some("MIXED message type is not supported.".to_string())),
                );
            }
            let topic_config_origin = self
                .inner
                .topic_config_manager
                .topic_config_table()
                .lock()
                .get(topic)
                .cloned();
            if topic_config_origin.is_some() && topic_config.clone() == topic_config_origin.unwrap()
            {
                info!(
                    "Broker receive request to update or create topic={}, but topicConfig has  no \
                     changes , so idempotent, caller address={}",
                    topic,
                    channel.remote_address(),
                );
                return Some(response.set_code(ResponseCode::Success));
            }
        }

        self.inner
            .topic_config_manager
            .update_topic_config_list(request_body.topic_config_list.as_mut_slice());
        if self.inner.broker_config.enable_single_topic_register {
            for topic_config in request_body.topic_config_list.iter() {
                self.inner
                    .topic_config_manager
                    .broker_runtime_inner()
                    .register_single_topic_all(topic_config.clone())
                    .await;
            }
        } else {
            self.inner
                .topic_config_manager
                .broker_runtime_inner()
                .register_increment_broker_data(
                    request_body.topic_config_list,
                    self.inner
                        .topic_config_manager
                        .data_version()
                        .as_ref()
                        .clone(),
                )
                .await;
        }
        Some(response.set_code(ResponseCode::Success))
    }

    pub async fn delete_topic(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<DeleteTopicRequestHeader>()
            .unwrap();
        let topic = request_header.topic.as_str();
        info!(
            "AdminBrokerProcessor#deleteTopic: broker receive request to delete topic={}, \
             caller={}",
            topic,
            channel.remote_address()
        );
        if topic.is_empty() {
            return Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(Some("he specified topic is blank.".to_string())),
            );
        }
        if self
            .inner
            .broker_config
            .validate_system_topic_when_update_topic
            && TopicValidator::is_system_topic(topic)
        {
            return Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(Some(format!(
                        "The topic[{}] is conflict with system topic.",
                        topic
                    ))),
            );
        }
        let groups = self
            .inner
            .consumer_offset_manager
            .which_group_by_topic(topic);
        for group in groups.iter() {
            let pop_retry_topic_v2 = KeyBuilder::build_pop_retry_topic(topic, group.as_str(), true);
            if self
                .inner
                .topic_config_manager
                .select_topic_config(pop_retry_topic_v2.as_str())
                .is_some()
            {
                self.delete_topic_in_broker(pop_retry_topic_v2.as_str());
            }
            let pop_retry_topic_v1 = KeyBuilder::build_pop_retry_topic_v1(topic, group.as_str());
            if self
                .inner
                .topic_config_manager
                .select_topic_config(pop_retry_topic_v1.as_str())
                .is_some()
            {
                self.delete_topic_in_broker(pop_retry_topic_v1.as_str());
            }
            self.delete_topic_in_broker(topic);
        }
        Some(response.set_code(ResponseCode::Success))
    }

    pub async fn get_all_topic_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        let topic_config_and_mapping_serialize_wrapper = TopicConfigAndMappingSerializeWrapper {
            topic_queue_mapping_detail_map: self
                .inner
                .topic_queue_mapping_manager
                .topic_queue_mapping_table
                .lock()
                .clone(),
            mapping_data_version: self
                .inner
                .topic_queue_mapping_manager
                .data_version
                .lock()
                .clone(),
            topic_config_serialize_wrapper: TopicConfigSerializeWrapper {
                data_version: self
                    .inner
                    .topic_config_manager
                    .data_version()
                    .as_ref()
                    .clone(),
                topic_config_table: self
                    .inner
                    .topic_config_manager
                    .topic_config_table()
                    .lock()
                    .clone(),
            },
            ..Default::default()
        };
        let content = topic_config_and_mapping_serialize_wrapper.to_json();
        if !content.is_empty() {
            response.set_body_mut_ref(Some(Bytes::from(content)));
        }
        Some(response)
    }

    pub async fn update_broker_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        todo!()
    }

    pub async fn get_broker_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        // broker config => broker config
        // default message store config => message store config
        let broker_config = self.inner.broker_config.clone();
        let message_store_config = self
            .inner
            .default_message_store
            .message_store_config()
            .clone();
        let broker_config_properties = broker_config.get_properties();
        let message_store_config_properties = message_store_config.get_properties();
        let combine_map = broker_config_properties
            .iter()
            .chain(message_store_config_properties.iter())
            .collect::<HashMap<_, _>>();
        let mut body = String::new();
        for (key, value) in combine_map.iter() {
            body.push_str(key.as_str());
            body.push(':');
            body.push_str(value.as_str());
            body.push('\n');
        }
        if !body.is_empty() {
            response.set_body_mut_ref(Some(Bytes::from(body)));
        }
        Some(response)
    }

    fn delete_topic_in_broker(&mut self, topic: &str) {
        self.inner.topic_config_manager.delete_topic_config(topic);
        self.inner.topic_queue_mapping_manager.delete(topic);
        self.inner
            .consumer_offset_manager
            .clean_offset_by_topic(topic);
        self.inner
            .pop_inflight_message_counter
            .clear_in_flight_message_num_by_topic_name(topic);
        self.inner.default_message_store.delete_topics(vec![topic]);
    }
}
