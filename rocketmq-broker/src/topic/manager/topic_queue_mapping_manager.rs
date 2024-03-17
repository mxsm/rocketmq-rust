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

use std::{collections::HashMap, sync::Arc};

use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_remoting::protocol::{
    body::topic_info_wrapper::topic_queue_wrapper::TopicQueueMappingSerializeWrapper,
    header::message_operation_header::TopicRequestHeaderTrait,
    remoting_command::RemotingCommand,
    static_topic::{
        topic_queue_mapping_context::TopicQueueMappingContext,
        topic_queue_mapping_detail::TopicQueueMappingDetail,
    },
    DataVersion,
};

use crate::{broker_config::BrokerConfig, broker_path_config_helper::get_topic_queue_mapping_path};

#[derive(Default)]
pub(crate) struct TopicQueueMappingManager {
    pub(crate) data_version: DataVersion,
    pub(crate) topic_queue_mapping_table: HashMap<String /* topic */, TopicQueueMappingDetail>,
    pub(crate) broker_config: Arc<BrokerConfig>,
}

impl TopicQueueMappingManager {
    pub(crate) fn build_topic_queue_mapping_context(
        &self,
        request_header: &impl TopicRequestHeaderTrait,
        select_one_when_miss: bool,
    ) -> TopicQueueMappingContext {
        if let Some(value) = request_header.lo() {
            if !value {
                return TopicQueueMappingContext::new(
                    request_header.topic(),
                    None,
                    None,
                    vec![],
                    None,
                );
            }
            //do
        }
        let topic = request_header.topic();

        let mut global_id: Option<i32> = None;
        /*if let Some(header) = request_header
            .as_any()
            .downcast_ref::<TopicQueueRequestHeaderT>()
        {
            global_id = header.queue_id;
        }*/
        if let Some(mapping_detail) = self.topic_queue_mapping_table.get(&topic) {
            // it is not static topic
            if mapping_detail
                .topic_queue_mapping_info
                .bname
                .clone()
                .unwrap()
                != self.broker_config.broker_name
            {
                return TopicQueueMappingContext {
                    topic: topic.clone(),
                    global_id: None,
                    mapping_detail: None,
                    mapping_item_list: vec![],
                    leader_item: None,
                    current_item: None,
                };
            }
            if global_id.is_none() {
                return TopicQueueMappingContext {
                    topic: topic.clone(),
                    global_id: None,
                    mapping_detail: Some(mapping_detail.clone()),
                    mapping_item_list: vec![],
                    leader_item: None,
                    current_item: None,
                };
            }
            // If not find mappingItem, it encounters some errors
            if let Some(global_id_value) = global_id {
                if global_id_value < 0 && !select_one_when_miss {
                    return TopicQueueMappingContext {
                        topic: topic.clone(),
                        global_id,
                        mapping_detail: Some(mapping_detail.clone()),
                        mapping_item_list: vec![],
                        leader_item: None,
                        current_item: None,
                    };
                }
            }
            if let Some(ref mut global_id_value) = global_id {
                if *global_id_value < 0 {
                    if let Some(hosted_queues) = &mapping_detail.hosted_queues {
                        if !hosted_queues.is_empty() {
                            // do not check
                            *global_id_value = *hosted_queues.keys().next().unwrap_or(&-1);
                        }
                    }
                }
            }
            if let Some(global_id_value) = global_id {
                if global_id_value < 0 {
                    return TopicQueueMappingContext {
                        topic: topic.clone(),
                        global_id,
                        mapping_detail: Some(mapping_detail.clone()),
                        mapping_item_list: vec![],
                        leader_item: None,
                        current_item: None,
                    };
                }
            }
            if let Some(mapping_item_list) =
                TopicQueueMappingDetail::get_mapping_info(mapping_detail, global_id.unwrap())
            {
                let leader_item = if !mapping_item_list.is_empty() {
                    Some(mapping_item_list[mapping_item_list.len() - 1].clone())
                } else {
                    None
                };
                return TopicQueueMappingContext {
                    topic: topic.clone(),
                    global_id,
                    mapping_detail: Some(mapping_detail.clone()),
                    mapping_item_list,
                    leader_item,
                    current_item: None,
                };
            }
        }
        TopicQueueMappingContext {
            topic,
            global_id,
            mapping_detail: None,
            mapping_item_list: vec![],
            leader_item: None,
            current_item: None,
        }
    }

    pub(crate) fn rewrite_request_for_static_topic(
        &self,
        _request_header: &impl TopicRequestHeaderTrait,
        _mapping_context: &TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        //TODO
        None
    }
}

//Fully implemented will be removed
#[allow(unused_variables)]
impl ConfigManager for TopicQueueMappingManager {
    fn decode0(&mut self, key: &[u8], body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&mut self) -> String {
        get_topic_queue_mapping_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&mut self, pretty_format: bool) -> String {
        todo!()
    }

    fn decode(&mut self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper = serde_json::from_str::<TopicQueueMappingSerializeWrapper>(json_string)
            .unwrap_or_default();
        if let Some(value) = wrapper.data_version() {
            self.data_version.assign_new_one(value);
        }
        if let Some(map) = wrapper.topic_queue_mapping_info_map() {
            for (key, value) in map {
                self.topic_queue_mapping_table
                    .insert(key.clone(), value.clone());
            }
        }
    }
}
