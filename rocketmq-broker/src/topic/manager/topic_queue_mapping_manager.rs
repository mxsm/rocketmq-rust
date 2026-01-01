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

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_queue_wrapper::TopicQueueMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use tracing::info;
use tracing::warn;

use crate::broker_path_config_helper::get_topic_queue_mapping_path;

#[derive(Default)]
pub(crate) struct TopicQueueMappingManager {
    pub(crate) data_version: Arc<parking_lot::Mutex<DataVersion>>,
    pub(crate) topic_queue_mapping_table: DashMap<CheetahString /* topic */, ArcMut<TopicQueueMappingDetail>>,
    pub(crate) broker_config: Arc<BrokerConfig>,
}

impl TopicQueueMappingManager {
    pub(crate) fn new(broker_config: Arc<BrokerConfig>) -> Self {
        Self {
            broker_config,
            ..Default::default()
        }
    }

    pub fn data_version(&self) -> DataVersion {
        self.data_version.lock().clone()
    }

    pub fn data_version_clone(&self) -> Arc<parking_lot::Mutex<DataVersion>> {
        self.data_version.clone()
    }

    pub(crate) fn rewrite_request_for_static_topic(
        request_header: &mut impl TopicRequestHeaderTrait,
        mapping_context: &TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        let mapping_detail = mapping_context.mapping_detail.as_ref()?;

        if !mapping_context.is_leader() {
            return Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NotLeaderForQueue,
                format!(
                    "{}-{:?} does not exit in request process of current broker {}",
                    request_header.topic(),
                    request_header.queue_id(),
                    mapping_detail
                        .topic_queue_mapping_info
                        .bname
                        .clone()
                        .unwrap_or_default()
                ),
            ));
        }
        if let Some(mapping_item) = mapping_context.leader_item.as_ref() {
            request_header.set_queue_id(mapping_item.queue_id);
        }
        None
    }
}

impl TopicQueueMappingManager {
    pub(crate) fn build_topic_queue_mapping_context(
        &self,
        request_header: &impl TopicRequestHeaderTrait,
        select_one_when_miss: bool,
    ) -> TopicQueueMappingContext {
        if let Some(value) = request_header.lo() {
            if !value {
                return TopicQueueMappingContext::new(request_header.topic().clone(), None, None, vec![], None);
            }
            //do
        }
        let topic = request_header.topic();
        let mut global_id = request_header.queue_id();
        let tqmd = self.topic_queue_mapping_table.get(topic);
        if tqmd.is_none() {
            return TopicQueueMappingContext {
                topic: topic.clone(),
                global_id: None,
                mapping_detail: None,
                mapping_item_list: vec![],
                leader_item: None,
                current_item: None,
            };
        }
        let mapping_detail = tqmd.unwrap();

        assert_eq!(
            mapping_detail.topic_queue_mapping_info.bname.clone().unwrap(),
            self.broker_config.broker_name().clone()
        );

        // If not find mappingItem, it encounters some errors
        if global_id < 0 && !select_one_when_miss {
            return TopicQueueMappingContext {
                topic: topic.clone(),
                global_id: Some(global_id),
                mapping_detail: Some(mapping_detail.clone()),
                mapping_item_list: vec![],
                leader_item: None,
                current_item: None,
            };
        }

        if global_id < 0 {
            if let Some(hosted_queues) = &mapping_detail.hosted_queues {
                if !hosted_queues.is_empty() {
                    // do not check
                    global_id = *hosted_queues.keys().next().unwrap_or(&-1);
                }
            }
        }
        //}
        // if let Some(global_id_value) = global_id {
        if global_id < 0 {
            return TopicQueueMappingContext {
                topic: topic.clone(),
                global_id: Some(global_id),
                mapping_detail: Some(mapping_detail.clone()),
                mapping_item_list: vec![],
                leader_item: None,
                current_item: None,
            };
        }
        // }
        let (leader_item, mapping_item_list) = if let Some(mapping_item_list) =
            TopicQueueMappingDetail::get_mapping_info(mapping_detail.as_ref(), global_id)
        {
            if !mapping_item_list.is_empty() {
                (
                    Some(mapping_item_list[mapping_item_list.len() - 1].clone()),
                    mapping_item_list.clone(),
                )
            } else {
                (None, vec![])
            }
        } else {
            (None, vec![])
        };
        TopicQueueMappingContext {
            topic: topic.clone(),
            global_id: Some(global_id),
            mapping_detail: Some(mapping_detail.clone()),
            mapping_item_list,
            leader_item,
            current_item: None,
        }
    }

    pub fn get_topic_queue_mapping(&self, topic: &str) -> Option<ArcMut<TopicQueueMappingDetail>> {
        self.topic_queue_mapping_table.get(topic).as_deref().cloned()
    }

    pub fn delete(&self, topic: &CheetahString) {
        let old = self.topic_queue_mapping_table.remove(topic);
        match old {
            None => {
                warn!("delete topic queue mapping failed, static topic: {} not exists", topic)
            }
            Some(value) => {
                info!("delete topic queue mapping OK, static topic queue mapping: {:?}", value);
                self.data_version.lock().next_version();
                self.persist();
            }
        }
    }
}

//Fully implemented will be removed
impl ConfigManager for TopicQueueMappingManager {
    fn config_file_path(&self) -> String {
        get_topic_queue_mapping_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let wrapper = TopicQueueMappingSerializeWrapper::new(
            Some(self.topic_queue_mapping_table.clone()),
            Some(self.data_version.lock().clone()),
        );
        match pretty_format {
            true => wrapper
                .serialize_json_pretty()
                .expect("encode topic queue mapping pretty failed"),
            false => wrapper.serialize_json().expect("encode topic queue mapping failed"),
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let mut wrapper = serde_json::from_str::<TopicQueueMappingSerializeWrapper>(json_string).unwrap_or_default();
        if let Some(value) = wrapper.data_version() {
            self.data_version.lock().assign_new_one(value);
        }
        if let Some(map) = wrapper.take_topic_queue_mapping_info_map() {
            for (key, value) in map {
                self.topic_queue_mapping_table.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;

    use super::*;

    #[test]
    fn new_creates_default_manager() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = TopicQueueMappingManager::new(broker_config.clone());

        assert!(Arc::ptr_eq(&manager.broker_config, &broker_config));
        assert_eq!(manager.data_version.lock().get_state_version(), 0);
        assert_eq!(manager.topic_queue_mapping_table.len(), 0);
    }

    #[test]
    fn get_topic_queue_mapping_returns_none_for_non_existent_topic() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = TopicQueueMappingManager::new(broker_config);

        assert!(manager.get_topic_queue_mapping("non_existent_topic").is_none());
    }

    #[test]
    fn get_topic_queue_mapping_returns_mapping_for_existing_topic() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = TopicQueueMappingManager::new(broker_config);
        let detail = ArcMut::new(TopicQueueMappingDetail::default());
        manager
            .topic_queue_mapping_table
            .insert(CheetahString::from_static_str("existing_topic"), detail);

        assert!(manager.get_topic_queue_mapping("existing_topic").is_some());
    }

    #[test]
    fn delete_removes_existing_topic() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = TopicQueueMappingManager::new(broker_config);
        let detail = ArcMut::new(TopicQueueMappingDetail::default());
        manager
            .topic_queue_mapping_table
            .insert("existing_topic".into(), detail);

        manager.delete(&CheetahString::from_static_str("existing_topic"));

        assert!(manager.get_topic_queue_mapping("existing_topic").is_none());
    }
}
