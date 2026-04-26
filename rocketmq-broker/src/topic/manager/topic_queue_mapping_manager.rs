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
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_queue_wrapper::TopicQueueMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
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

    pub fn update_topic_queue_mapping(&self, mut topic_queue_mapping_detail: TopicQueueMappingDetail) {
        let topic = topic_queue_mapping_detail
            .topic_queue_mapping_info
            .topic
            .clone()
            .unwrap_or_default();
        if topic.is_empty() {
            return;
        }

        if topic_queue_mapping_detail.topic_queue_mapping_info.bname.is_none() {
            topic_queue_mapping_detail.topic_queue_mapping_info.bname = Some(self.broker_config.broker_name().clone());
        }
        if topic_queue_mapping_detail.hosted_queues.is_none() {
            topic_queue_mapping_detail.hosted_queues = Some(std::collections::HashMap::new());
        }

        self.topic_queue_mapping_table
            .insert(topic, ArcMut::new(topic_queue_mapping_detail));
        self.data_version.lock().next_version();
        self.persist();
    }

    pub(crate) fn snapshot_topic_queue_mapping_table(&self) -> Vec<(CheetahString, TopicQueueMappingDetail)> {
        self.topic_queue_mapping_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect()
    }

    pub(crate) fn replace_cleaned_queue_items(
        &self,
        topic: &CheetahString,
        expected_epoch: i64,
        expected_bname: &CheetahString,
        replacements: &HashMap<i32, (Vec<LogicQueueMappingItem>, Vec<LogicQueueMappingItem>)>,
    ) -> bool {
        if replacements.is_empty() {
            return false;
        }

        let Some(current_ref) = self.topic_queue_mapping_table.get(topic) else {
            return false;
        };
        let current = current_ref.as_ref();
        if current.topic_queue_mapping_info.epoch != expected_epoch
            || current.topic_queue_mapping_info.bname.as_ref() != Some(expected_bname)
        {
            return false;
        }

        let Some(current_hosted_queues) = current.hosted_queues.as_ref() else {
            return false;
        };

        let mut new_detail = current.clone();
        let mut changed = false;
        {
            let new_hosted_queues = new_detail.hosted_queues.get_or_insert_with(HashMap::new);
            for (qid, (expected_items, new_items)) in replacements {
                if new_items.is_empty() {
                    continue;
                }
                let Some(current_items) = current_hosted_queues.get(qid) else {
                    continue;
                };
                if current_items != expected_items || current_items == new_items {
                    continue;
                }
                new_hosted_queues.insert(*qid, new_items.clone());
                changed = true;
            }
        }
        drop(current_ref);

        if changed {
            self.topic_queue_mapping_table
                .insert(topic.clone(), ArcMut::new(new_detail));
        }
        changed
    }

    pub(crate) fn remove_cleaned_hosted_queues(
        &self,
        topic: &CheetahString,
        expected_epoch: i64,
        expected_bname: &CheetahString,
        expected_items_by_qid: &HashMap<i32, Vec<LogicQueueMappingItem>>,
    ) -> bool {
        if expected_items_by_qid.is_empty() {
            return false;
        }

        let Some(current_ref) = self.topic_queue_mapping_table.get(topic) else {
            return false;
        };
        let current = current_ref.as_ref();
        if current.topic_queue_mapping_info.epoch != expected_epoch
            || current.topic_queue_mapping_info.bname.as_ref() != Some(expected_bname)
        {
            return false;
        }

        let Some(current_hosted_queues) = current.hosted_queues.as_ref() else {
            return false;
        };

        let mut new_detail = current.clone();
        let mut changed = false;
        if let Some(new_hosted_queues) = new_detail.hosted_queues.as_mut() {
            for (qid, expected_items) in expected_items_by_qid {
                let Some(current_items) = current_hosted_queues.get(qid) else {
                    continue;
                };
                if current_items != expected_items {
                    continue;
                }
                if new_hosted_queues.remove(qid).is_some() {
                    changed = true;
                }
            }
        }
        drop(current_ref);

        if changed {
            self.topic_queue_mapping_table
                .insert(topic.clone(), ArcMut::new(new_detail));
        }
        changed
    }

    pub(crate) async fn persist_clean_result(&self) -> RocketMQResult<()> {
        self.data_version.lock().next_version();
        let json = self.encode_pretty(true);
        if json.is_empty() {
            return Ok(());
        }

        let file_name = self.config_file_path();
        tokio::task::spawn_blocking(move || {
            rocketmq_common::FileUtils::string_to_file(json.as_str(), file_name.as_str())
        })
        .await
        .map_err(|err| RocketMQError::Internal(format!("persist topic queue mapping clean result failed: {err}")))?
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

    #[test]
    fn update_topic_queue_mapping_inserts_detail_and_defaults_broker_name() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = TopicQueueMappingManager::new(broker_config.clone());
        let mut detail = TopicQueueMappingDetail::default();
        detail.topic_queue_mapping_info.topic = Some(CheetahString::from_static_str("static-topic"));

        manager.update_topic_queue_mapping(detail);

        let mapping = manager
            .get_topic_queue_mapping("static-topic")
            .expect("mapping should exist after update");
        assert_eq!(
            mapping.topic_queue_mapping_info.bname.as_deref(),
            Some(broker_config.broker_name().as_str())
        );
        assert!(mapping.hosted_queues.is_some());
    }

    #[test]
    fn replace_cleaned_queue_items_rechecks_expected_items() {
        let broker_config = Arc::new(BrokerConfig::default());
        let broker_name = broker_config.broker_name().clone();
        let manager = TopicQueueMappingManager::new(broker_config);
        let topic = CheetahString::from_static_str("static-topic");
        let item0 = LogicQueueMappingItem {
            gen: 0,
            queue_id: 0,
            bname: Some(broker_name.clone()),
            end_offset: 10,
            ..Default::default()
        };
        let item1 = LogicQueueMappingItem {
            gen: 1,
            queue_id: 1,
            bname: Some(broker_name.clone()),
            logic_offset: 10,
            ..Default::default()
        };
        let mut hosted_queues = HashMap::new();
        hosted_queues.insert(0, vec![item0.clone(), item1.clone()]);
        manager.topic_queue_mapping_table.insert(
            topic.clone(),
            ArcMut::new(TopicQueueMappingDetail {
                topic_queue_mapping_info:
                    rocketmq_remoting::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo {
                        topic: Some(topic.clone()),
                        bname: Some(broker_name.clone()),
                        epoch: 1,
                        ..Default::default()
                    },
                hosted_queues: Some(hosted_queues),
            }),
        );
        let mut replacements = HashMap::new();
        replacements.insert(0, (vec![item0, item1.clone()], vec![item1.clone()]));

        assert!(manager.replace_cleaned_queue_items(&topic, 1, &broker_name, &replacements));

        let mapping = manager.get_topic_queue_mapping(topic.as_str()).unwrap();
        assert_eq!(mapping.hosted_queues.as_ref().unwrap().get(&0).unwrap(), &vec![item1]);
        assert_eq!(manager.data_version.lock().get_state_version(), 0);
    }

    #[test]
    fn remove_cleaned_hosted_queues_skips_when_current_items_changed() {
        let broker_config = Arc::new(BrokerConfig::default());
        let broker_name = broker_config.broker_name().clone();
        let manager = TopicQueueMappingManager::new(broker_config);
        let topic = CheetahString::from_static_str("static-topic");
        let old_item = LogicQueueMappingItem {
            gen: 0,
            queue_id: 0,
            bname: Some(CheetahString::from_static_str("old-broker")),
            ..Default::default()
        };
        let current_item = LogicQueueMappingItem {
            gen: 1,
            queue_id: 1,
            bname: Some(broker_name.clone()),
            ..Default::default()
        };
        let mut hosted_queues = HashMap::new();
        hosted_queues.insert(0, vec![current_item]);
        manager.topic_queue_mapping_table.insert(
            topic.clone(),
            ArcMut::new(TopicQueueMappingDetail {
                topic_queue_mapping_info:
                    rocketmq_remoting::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo {
                        topic: Some(topic.clone()),
                        bname: Some(broker_name.clone()),
                        epoch: 1,
                        ..Default::default()
                    },
                hosted_queues: Some(hosted_queues),
            }),
        );
        let mut expected_items = HashMap::new();
        expected_items.insert(0, vec![old_item]);

        assert!(!manager.remove_cleaned_hosted_queues(&topic, 1, &broker_name, &expected_items));
        assert!(manager
            .get_topic_queue_mapping(topic.as_str())
            .unwrap()
            .hosted_queues
            .as_ref()
            .unwrap()
            .contains_key(&0));
    }
}
