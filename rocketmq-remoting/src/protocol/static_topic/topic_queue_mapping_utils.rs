use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rand::seq::SliceRandom;
use rocketmq_common::common::config::TopicConfig;
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
use rocketmq_common::common::mix_all;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::ArcMut;

use crate::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use crate::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
use crate::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use crate::protocol::static_topic::topic_queue_mapping_one::TopicQueueMappingOne;
use crate::protocol::static_topic::topic_remapping_detail_wrapper;
use crate::protocol::static_topic::topic_remapping_detail_wrapper::TopicRemappingDetailWrapper;
use crate::protocol::RemotingSerializable;

pub struct TopicQueueMappingUtils;

impl TopicQueueMappingUtils {
    pub fn find_logic_queue_mapping_item(
        mapping_items: &[LogicQueueMappingItem],
        logic_offset: i64,
        ignore_negative: bool,
    ) -> Option<&LogicQueueMappingItem> {
        if mapping_items.is_empty() {
            return None;
        }
        // Could use binary search to improve performance
        for i in (0..mapping_items.len()).rev() {
            let item = &mapping_items[i];
            if ignore_negative && item.logic_offset < 0 {
                continue;
            }
            if logic_offset >= item.logic_offset {
                return Some(item);
            }
        }
        // If not found, maybe out of range, return the first one
        for item in mapping_items.iter() {
            if ignore_negative && item.logic_offset < 0 {
                continue;
            } else {
                return Some(item);
            }
        }
        None
    }

    pub fn find_next<'a>(
        items: &'a [LogicQueueMappingItem],
        current_item: Option<&'a LogicQueueMappingItem>,
        ignore_negative: bool,
    ) -> Option<&'a LogicQueueMappingItem> {
        if items.is_empty() || current_item.is_none() {
            return None;
        }
        let current_item = current_item.unwrap();
        for i in 0..items.len() {
            let item = &items[i];
            if ignore_negative && item.logic_offset < 0 {
                continue;
            }
            if item.gen == current_item.gen {
                if i < items.len() - 1 {
                    let next_item = &items[i + 1];
                    if ignore_negative && next_item.logic_offset < 0 {
                        return None;
                    } else {
                        return Some(next_item);
                    }
                } else {
                    return None;
                }
            }
        }
        None
    }

    pub fn get_mock_broker_name(scope: &str) -> CheetahString {
        assert!(!scope.is_empty(), "Scope cannot be null");

        if scope == mix_all::METADATA_SCOPE_GLOBAL {
            format!("{}{}", mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX, &scope[2..]).into()
        } else {
            format!("{}{}", mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX, scope).into()
        }
    }
    pub fn get_mapping_detail_from_config(
        configs: Vec<TopicConfigAndQueueMapping>,
    ) -> RocketMQResult<Vec<TopicQueueMappingDetail>> {
        let mut detail_list = vec![];
        for config_mapping in &configs {
            if let Some(detail) = &config_mapping.topic_queue_mapping_detail {
                detail_list.push((**detail).clone());
            }
        }
        Ok(detail_list)
    }
    pub fn check_name_epoch_num_consistence(
        topic: &CheetahString,
        broker_config_map: &HashMap<CheetahString, TopicConfigAndQueueMapping>,
    ) -> RocketMQResult<(i64, i32)> {
        if broker_config_map.is_empty() {
            return Err(RocketMQError::Internal(
                "check_name_epoch_num_consistence get empty config map".to_string(),
            ));
        }
        //make sure it is not null
        let mut max_epoch = -1;
        let mut max_num = -1;
        let scope = CheetahString::new();
        for entry in broker_config_map {
            let broker = entry.0;
            let config_mapping = entry.1;
            if config_mapping.topic_queue_mapping_detail.is_none() {
                return Err(RocketMQError::Internal(format!(
                    "Mapping info should not be null in broker {}",
                    broker
                )));
            }
            let mapping_detail = &config_mapping.topic_queue_mapping_detail;
            if let Some(mapping_detail) = &mapping_detail {
                if let Some(broker_name) = &mapping_detail.topic_queue_mapping_info.bname {
                    if !broker.eq(broker_name) {
                        return Err(RocketMQError::Internal(format!(
                            "The broker name is not equal {} != {} ",
                            broker, broker_name
                        )));
                    }
                    if mapping_detail.topic_queue_mapping_info.dirty {
                        return Err(RocketMQError::Internal(format!(
                            "The mapping info is dirty in broker  {}",
                            broker
                        )));
                    }
                    if let Some(top) = &config_mapping.topic_config.topic_name {
                        if let Some(mapping_top) = &mapping_detail.topic_queue_mapping_info.topic {
                            if !top.eq(mapping_top) {
                                return Err(RocketMQError::Internal(format!(
                                    "The topic name is inconsistent in broker  {}",
                                    broker
                                )));
                            }
                            if !topic.eq(mapping_top) {
                                return Err(RocketMQError::Internal(format!(
                                    "The topic name is not match for broker  {}",
                                    broker
                                )));
                            }
                            if let Some(m_scope) = &mapping_detail.topic_queue_mapping_info.scope {
                                if !scope.eq(m_scope) {
                                    return Err(RocketMQError::Internal(format!(
                                        "scope does not match {} != {} in {}",
                                        m_scope, scope, broker
                                    )));
                                }
                                if max_epoch != -1 && max_epoch != mapping_detail.topic_queue_mapping_info.epoch {
                                    return Err(RocketMQError::Internal(format!(
                                        "epoch does not match {} != {} in {}",
                                        max_epoch, mapping_detail.topic_queue_mapping_info.epoch, broker_name
                                    )));
                                } else {
                                    max_epoch = mapping_detail.topic_queue_mapping_info.epoch;
                                }
                                if max_num != -1 && max_num != mapping_detail.topic_queue_mapping_info.total_queues {
                                    return Err(RocketMQError::Internal(format!(
                                        "total queue number does not match {} != {} in {}",
                                        max_num, mapping_detail.topic_queue_mapping_info.total_queues, broker_name
                                    )));
                                } else {
                                    max_num = mapping_detail.topic_queue_mapping_info.total_queues;
                                }
                                return Ok((max_epoch, max_num));
                            }
                        }
                    }
                }
            }
        }
        Err(RocketMQError::Internal(
            "check_name_epoch_num_consistence err ! maybe some var is none".to_string(),
        ))
    }
    pub fn check_if_reuse_physical_queue(mapping_ones: &Vec<TopicQueueMappingOne>) -> RocketMQResult<()> {
        let mut physical_queue_id_map = HashMap::new();
        for mapping_one in mapping_ones {
            for item in mapping_one.items() {
                if let Some(bname) = &item.bname {
                    let physical_queue_id = format!("{} - {}", bname, item.queue_id);
                    physical_queue_id_map
                        .entry(physical_queue_id.clone())
                        .or_insert(mapping_one.clone());
                    if let Some(id) = physical_queue_id_map.get(&physical_queue_id) {
                        return Err(RocketMQError::Internal(format!(
                            "Topic {} global queue id {} and {} shared the same physical queue {}",
                            mapping_one.topic(),
                            mapping_one.global_id(),
                            id.global_id(),
                            physical_queue_id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn check_logic_queue_mapping_item_offset(items: &[LogicQueueMappingItem]) -> RocketMQResult<()> {
        if items.is_empty() {
            return Err(RocketMQError::Internal(
                "check_logic_queue_mapping_item_offset input items is empty".to_string(),
            ));
        }
        let mut last_gen = -1;
        let mut last_offset = -1;
        for i in items.len() - 1..=0 {
            let item = &items[i];
            if item.start_offset < 0 || item.gen < 0 || item.queue_id < 0 {
                return Err(RocketMQError::Internal(
                    "The field is illegal, should not be negative".to_string(),
                ));
            }
            if items.len() >= 2 && i <= items.len() - 2 && items[i].logic_offset < 0 {
                return Err(RocketMQError::Internal(
                    "The non-latest item has negative logic offset".to_string(),
                ));
            }
            if last_gen != -1 && item.gen >= last_gen {
                return Err(RocketMQError::Internal(
                    "The gen does not increase monotonically".to_string(),
                ));
            }

            if item.end_offset != -1 && item.end_offset < item.start_offset {
                return Err(RocketMQError::Internal(
                    "The endOffset is smaller than the start offset".to_string(),
                ));
            }

            if last_offset != -1 && item.logic_offset != -1 {
                if item.logic_offset >= last_offset {
                    return Err(RocketMQError::Internal(
                        "The base logic offset does not increase monotonically".to_string(),
                    ));
                }
                if item.compute_max_static_queue_offset() >= last_offset {
                    return Err(RocketMQError::Internal(
                        "The max logic offset does not increase monotonically".to_string(),
                    ));
                }
            }
            last_gen = item.gen;
            last_offset = item.logic_offset;
        }
        Ok(())
    }
    pub fn get_leader_item(items: &[LogicQueueMappingItem]) -> RocketMQResult<LogicQueueMappingItem> {
        if items.is_empty() {
            return Err(RocketMQError::Internal(
                "get_leader_item failed with empty items".to_string(),
            ));
        }
        if let Some(i) = items.last() {
            return Ok(i.clone());
        }
        Err(RocketMQError::Internal(
            "get_leader_item failed with empty items".to_string(),
        ))
    }
    pub fn get_leader_broker(items: &[LogicQueueMappingItem]) -> RocketMQResult<CheetahString> {
        let item = TopicQueueMappingUtils::get_leader_item(items)?;
        if let Some(bname) = &item.bname {
            return Ok(bname.to_string().into());
        }
        Err(RocketMQError::Internal(
            "get_leader_broker fn get Item with None bname".to_string(),
        ))
    }
    pub fn check_and_build_mapping_items(
        mut mapping_detail_list: Vec<TopicQueueMappingDetail>,
        replace: bool,
        check_consistence: bool,
    ) -> RocketMQResult<HashMap<i32, TopicQueueMappingOne>> {
        mapping_detail_list.sort_by(|o1, o2| {
            let sub = o1.topic_queue_mapping_info.epoch - o2.topic_queue_mapping_info.epoch;
            if sub > 0 {
                return std::cmp::Ordering::Greater;
            } else if sub < 0 {
                return std::cmp::Ordering::Less;
            }
            std::cmp::Ordering::Equal
        });

        let mut max_num = 0;
        let mut global_id_map = HashMap::new();
        for mapping_detail in &mapping_detail_list {
            if mapping_detail.topic_queue_mapping_info.total_queues > max_num {
                max_num = mapping_detail.topic_queue_mapping_info.total_queues;
            }
            if let Some(queue) = &mapping_detail.hosted_queues {
                for entry in queue {
                    let global_id = entry.0;
                    TopicQueueMappingUtils::check_logic_queue_mapping_item_offset(entry.1)?;
                    let leader_broker_name = TopicQueueMappingUtils::get_leader_broker(entry.1)?;
                    if let Some(broker_name) = &mapping_detail.topic_queue_mapping_info.bname {
                        if !leader_broker_name.eq(broker_name) {
                            //not the leader
                            continue;
                        }

                        if global_id_map.contains_key(global_id) {
                            if !replace {
                                return Err(RocketMQError::Internal(format!(
                                    "The queue id is duplicated in broker {} {}",
                                    leader_broker_name, broker_name
                                )));
                            }
                        } else if let Some(top) = &mapping_detail.topic_queue_mapping_info.topic {
                            global_id_map.insert(
                                *global_id,
                                TopicQueueMappingOne::new(
                                    mapping_detail.clone(),
                                    top.clone().into(),
                                    broker_name.clone().into(),
                                    *global_id,
                                    entry.1.clone(),
                                ),
                            );
                        }
                    }
                }
            }
        }

        if check_consistence {
            if max_num as usize != global_id_map.len() {
                return Err(RocketMQError::Internal(format!(
                    "The total queue number in config does not match the real hosted queues {} != {}",
                    max_num,
                    global_id_map.len()
                )));
            }
            for i in 0..max_num {
                if !global_id_map.contains_key(&i) {
                    return Err(RocketMQError::Internal(format!(
                        "The queue number {} is not in globalIdMap",
                        i
                    )));
                }
            }
        }
        let values = global_id_map.values().cloned().collect();
        TopicQueueMappingUtils::check_if_reuse_physical_queue(&values)?;
        Ok(global_id_map)
    }
    pub fn write_to_temp(wrapper: &TopicRemappingDetailWrapper, after: bool) -> RocketMQResult<CheetahString> {
        let topic = wrapper.topic();
        let data = wrapper.serialize_json()?;
        let mut suffix = topic_remapping_detail_wrapper::SUFFIX_BEFORE;
        if after {
            suffix = topic_remapping_detail_wrapper::SUFFIX_AFTER;
        }
        if let Some(tmpdir) = &EnvUtils::get_property("java.io.tmpdir") {
            let file_name = format!("{}/{}-{}{}", tmpdir, topic, wrapper.get_epoch(), suffix);
            string_to_file(&data, &file_name)?;
            return Ok(file_name.into());
        }

        Err(RocketMQError::Internal("write file failed".to_string()))
    }
    pub fn check_target_brokers_complete(
        target_brokers: &HashSet<CheetahString>,
        broker_config_map: &HashMap<CheetahString, TopicConfigAndQueueMapping>,
    ) -> RocketMQResult<()> {
        for broker in broker_config_map.keys() {
            if let Some(mapping) = broker_config_map.get(broker) {
                if let Some(detail) = mapping.get_topic_queue_mapping_detail() {
                    if let Some(queues) = &detail.hosted_queues {
                        if queues.is_empty() {
                            continue;
                        }
                    }
                }
            }
            if !target_brokers.contains(broker) {
                return Err(RocketMQError::Internal(format!(
                    "The existed broker {} does not in target brokers ",
                    broker
                )));
            }
        }

        Ok(())
    }
    pub fn check_physical_queue_consistence(
        broker_config_map: &HashMap<CheetahString, TopicConfigAndQueueMapping>,
    ) -> RocketMQResult<()> {
        for entry in broker_config_map {
            let config_mapping = entry.1;
            if let Some(detail) = config_mapping.get_topic_queue_mapping_detail() {
                if config_mapping.topic_config.read_queue_nums < config_mapping.topic_config.write_queue_nums {
                    return Err(RocketMQError::Internal(
                        "Read queues is smaller than write queues".to_string(),
                    ));
                }
                if let Some(queues) = &detail.hosted_queues {
                    for items in queues.values() {
                        for item in items {
                            if item.start_offset != 0 {
                                return Err(RocketMQError::Internal(
                                    "The start offset does not begin from 0".to_string(),
                                ));
                            }
                            if let Some(bname) = &item.bname {
                                let topic_config = broker_config_map.get(&CheetahString::from(bname));
                                if topic_config.is_none() {
                                    return Err(RocketMQError::Internal(
                                        "The broker of item does not exist".to_string(),
                                    ));
                                } else if let Some(topic_config) = topic_config {
                                    if item.queue_id >= topic_config.topic_config.write_queue_nums as i32 {
                                        return Err(RocketMQError::Internal(
                                            "The physical queue id is overflow the write queues".to_string(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn create_topic_config_mapping(
        topic: &str,
        queue_num: i32,
        target_brokers: &HashSet<CheetahString>,
        broker_config_map: &mut HashMap<CheetahString, TopicConfigAndQueueMapping>,
    ) -> RocketMQResult<TopicRemappingDetailWrapper> {
        TopicQueueMappingUtils::check_target_brokers_complete(target_brokers, broker_config_map)?;
        let mut global_id_map = HashMap::new();
        let mut max_epoch_and_num = (get_current_millis(), queue_num);
        if !broker_config_map.is_empty() {
            let new_max_epoch_and_num = TopicQueueMappingUtils::check_name_epoch_num_consistence(
                &CheetahString::from(topic),
                broker_config_map,
            )?;
            max_epoch_and_num.0 = new_max_epoch_and_num.0 as u64;
            max_epoch_and_num.1 = new_max_epoch_and_num.1;
            let mut detail_list = vec![];
            detail_list.extend(TopicQueueMappingUtils::get_mapping_detail_from_config(
                broker_config_map.values().cloned().collect(),
            )?);
            global_id_map = TopicQueueMappingUtils::check_and_build_mapping_items(detail_list, false, true)?;
            TopicQueueMappingUtils::check_if_reuse_physical_queue(&global_id_map.values().cloned().collect())?;
            TopicQueueMappingUtils::check_physical_queue_consistence(broker_config_map)?;
        }
        if (queue_num as usize) < global_id_map.len() {
            return Err(RocketMQError::Internal(format!(
                "Cannot decrease the queue num for static topic {} < {}",
                queue_num,
                global_id_map.len()
            )));
        }
        //check the queue number
        if (queue_num as usize) == global_id_map.len() {
            return Err(RocketMQError::Internal(
                "The topic queue num is equal the existed queue num, do nothing".to_string(),
            ));
        }

        //the check is ok, now do the mapping allocation
        let mut broker_num_map = HashMap::new();
        for broker in target_brokers {
            broker_num_map.insert(broker.clone(), 0);
        }
        let mut old_id_to_broker = HashMap::new();
        for entry in &global_id_map {
            let leader_broker = entry.1.bname();
            old_id_to_broker.insert(*entry.0, CheetahString::from(leader_broker));
            if !broker_num_map.contains_key(leader_broker) {
                broker_num_map.insert(leader_broker.into(), 1);
            } else {
                broker_num_map.insert(leader_broker.into(), broker_num_map[leader_broker] + 1);
            }
        }
        let mut allocator = MappingAllocator::new(old_id_to_broker, broker_num_map, HashMap::new());
        allocator.up_to_num(queue_num);
        let new_id_to_broker = allocator.id_to_broker();

        //construct the topic configAndMapping
        let new_epoch = (max_epoch_and_num.0 + 1000).max(get_current_millis());
        for e in new_id_to_broker {
            let queue_id = e.0;
            let broker = e.1;
            if global_id_map.contains_key(queue_id) {
                //ignore the exited
                continue;
            }
            let mut config_mapping;
            if !broker_config_map.contains_key(broker) {
                config_mapping = TopicConfigAndQueueMapping::new(
                    TopicConfig::new(topic),
                    Some(ArcMut::new(TopicQueueMappingDetail {
                        topic_queue_mapping_info: TopicQueueMappingInfo::new(
                            topic.into(),
                            0,
                            broker.into(),
                            get_current_millis() as i64,
                        ),
                        hosted_queues: None,
                    })),
                );
                config_mapping.topic_config.write_queue_nums = 1;
                config_mapping.topic_config.read_queue_nums = 1;
                broker_config_map.insert(broker.clone(), config_mapping.clone());
            } else {
                config_mapping = broker_config_map[broker].clone();
                config_mapping.topic_config.write_queue_nums += 1;
                config_mapping.topic_config.read_queue_nums += 1;
            }
            let mapping_item = LogicQueueMappingItem {
                gen: 0,
                queue_id: config_mapping.topic_config.write_queue_nums as i32,
                bname: Some(broker.clone()),
                logic_offset: 0,
                start_offset: 0,
                end_offset: -1,
                time_of_start: -1,
                time_of_end: -1,
            };
            if let Some(detail) = config_mapping.topic_queue_mapping_detail {
                TopicQueueMappingDetail::put_mapping_info(detail.clone(), *queue_id, vec![mapping_item]);
            }
        }

        // set the topic config
        for entry in &mut *broker_config_map {
            let config_mapping = entry.1;
            if let Some(detail) = &mut config_mapping.topic_queue_mapping_detail {
                detail.topic_queue_mapping_info.epoch = new_epoch as i64;
                detail.topic_queue_mapping_info.total_queues = queue_num;
            }
        }
        //double check the config

        TopicQueueMappingUtils::check_name_epoch_num_consistence(&CheetahString::from(topic), broker_config_map)?;
        global_id_map = TopicQueueMappingUtils::check_and_build_mapping_items(
            TopicQueueMappingUtils::get_mapping_detail_from_config(broker_config_map.values().cloned().collect())?,
            false,
            true,
        )?;
        TopicQueueMappingUtils::check_if_reuse_physical_queue(&global_id_map.values().cloned().collect())?;
        TopicQueueMappingUtils::check_physical_queue_consistence(broker_config_map)?;
        let map = broker_config_map
            .iter()
            .map(|(k, v)| (CheetahString::from_string(k.to_string()), v.clone()))
            .collect();
        Ok(TopicRemappingDetailWrapper::new(
            topic.to_string().into(),
            topic_remapping_detail_wrapper::TYPE_CREATE_OR_UPDATE.to_string().into(),
            new_epoch,
            map,
            HashSet::new(),
            HashSet::new(),
        ))
    }
}
pub struct MappingAllocator {
    broker_num_map: HashMap<CheetahString, i32>,
    id_to_broker: HashMap<i32, CheetahString>,
    //used for remapping
    broker_num_map_before_remapping: HashMap<CheetahString, i32>,
    current_index: i32,
    least_brokers: Vec<CheetahString>,
}
impl MappingAllocator {
    pub fn new(
        id_to_broker: HashMap<i32, CheetahString>,
        broker_num_map: HashMap<CheetahString, i32>,
        broker_num_map_before_remapping: HashMap<CheetahString, i32>,
    ) -> Self {
        Self {
            id_to_broker,
            broker_num_map,
            broker_num_map_before_remapping,
            current_index: 0,
            least_brokers: vec![],
        }
    }

    fn fresh_state(&mut self) {
        let mut min_num = i32::MAX;
        for entry in &self.broker_num_map {
            if *entry.1 < min_num {
                self.least_brokers.clear();
                self.least_brokers.push(entry.0.clone());
                min_num = *entry.1;
            } else if *entry.1 == min_num {
                self.least_brokers.push(entry.0.clone());
            }
        }
        //reduce the remapping
        if !self.broker_num_map_before_remapping.is_empty() {
            self.least_brokers.sort_by(|o1, o2| {
                let mut i1 = 0;
                let mut i2 = 0;
                if self.broker_num_map_before_remapping.contains_key(o1) {
                    if let Some(s) = self.broker_num_map_before_remapping.get(o1) {
                        i1 = *s;
                    }
                }
                if self.broker_num_map_before_remapping.contains_key(o2) {
                    if let Some(s) = self.broker_num_map_before_remapping.get(o2) {
                        i2 = *s;
                    }
                }
                if i1 - i2 > 0 {
                    return std::cmp::Ordering::Greater;
                } else if i1 - i2 < 0 {
                    return Ordering::Less;
                }
                Ordering::Equal
            });
        } else {
            //reduce the imbalance
            let mut rng = rand::rng();
            self.least_brokers.shuffle(&mut rng);
        }
        self.current_index = (self.least_brokers.len() - 1) as i32;
    }
    fn next_broker(&mut self) -> CheetahString {
        if self.least_brokers.is_empty() {
            self.fresh_state();
        }
        let tmp_index = self.current_index as usize % self.least_brokers.len();
        self.least_brokers.remove(tmp_index)
    }

    pub fn broker_num_map(&self) -> &HashMap<CheetahString, i32> {
        &self.broker_num_map
    }

    pub fn up_to_num(&mut self, max_queue_num: i32) {
        let curr_size = self.id_to_broker.len();
        if (max_queue_num as usize) <= curr_size {
            return;
        }
        for i in curr_size..(max_queue_num as usize) {
            let next_broker = self.next_broker();
            if self.broker_num_map.contains_key(&next_broker) {
                self.broker_num_map
                    .insert(next_broker.clone(), self.broker_num_map[&next_broker] + 1);
            } else {
                self.broker_num_map.insert(next_broker.clone(), 1);
            }
            self.id_to_broker.insert(i as i32, next_broker);
        }
    }

    pub fn id_to_broker(&self) -> &HashMap<i32, CheetahString> {
        &self.id_to_broker
    }
}

impl TopicQueueMappingUtils {
    pub fn check_leader_in_target_brokers(
        mapping_ones: &[TopicQueueMappingOne],
        target_brokers: &HashSet<CheetahString>,
    ) -> RocketMQResult<()> {
        for mapping_one in mapping_ones {
            if !target_brokers.contains(&CheetahString::from(mapping_one.bname())) {
                return Err(RocketMQError::Internal(
                    "The leader broker is not in target brokers".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn block_seq_round_up(offset: i64, block_seq_size: i64) -> i64 {
        let num = offset / block_seq_size;
        let left = offset % block_seq_size;
        if left < block_seq_size / 2 {
            (num + 1) * block_seq_size
        } else {
            (num + 2) * block_seq_size
        }
    }

    pub fn remapping_static_topic(
        topic: &str,
        broker_config_map: &mut HashMap<CheetahString, TopicConfigAndQueueMapping>,
        target_brokers: &HashSet<CheetahString>,
    ) -> RocketMQResult<TopicRemappingDetailWrapper> {
        let max_epoch_and_num =
            TopicQueueMappingUtils::check_name_epoch_num_consistence(&CheetahString::from(topic), broker_config_map)?;

        let mut global_id_map = TopicQueueMappingUtils::check_and_build_mapping_items(
            TopicQueueMappingUtils::get_mapping_detail_from_config(broker_config_map.values().cloned().collect())?,
            false,
            true,
        )?;

        TopicQueueMappingUtils::check_physical_queue_consistence(broker_config_map)?;
        TopicQueueMappingUtils::check_if_reuse_physical_queue(&global_id_map.values().cloned().collect())?;

        let max_num = max_epoch_and_num.1;
        let mut broker_num_map: HashMap<CheetahString, i32> = HashMap::new();
        for broker in target_brokers {
            broker_num_map.insert(broker.clone(), 0);
        }

        let mut broker_num_map_before_remapping: HashMap<CheetahString, i32> = HashMap::new();
        for mapping_one in global_id_map.values() {
            let bname = CheetahString::from(mapping_one.bname());
            if broker_num_map_before_remapping.contains_key(&bname) {
                broker_num_map_before_remapping.insert(bname.clone(), broker_num_map_before_remapping[&bname] + 1);
            } else {
                broker_num_map_before_remapping.insert(bname, 1);
            }
        }

        let mut allocator =
            MappingAllocator::new(HashMap::new(), broker_num_map.clone(), broker_num_map_before_remapping);
        allocator.up_to_num(max_num);
        let expected_broker_num_map = allocator.broker_num_map().clone();

        let mut wait_assign_queues = std::collections::VecDeque::new();
        let mut expected_id_to_broker: HashMap<i32, CheetahString> = HashMap::new();
        let mut expected_broker_num_map_mut = expected_broker_num_map;
        for (queue_id, mapping_one) in &global_id_map {
            let leader_broker = CheetahString::from(mapping_one.bname());
            if expected_broker_num_map_mut.contains_key(&leader_broker) {
                if expected_broker_num_map_mut[&leader_broker] > 0 {
                    expected_id_to_broker.insert(*queue_id, leader_broker.clone());
                    expected_broker_num_map_mut
                        .insert(leader_broker.clone(), expected_broker_num_map_mut[&leader_broker] - 1);
                } else {
                    wait_assign_queues.push_back(*queue_id);
                    expected_broker_num_map_mut.remove(&leader_broker);
                }
            } else {
                wait_assign_queues.push_back(*queue_id);
            }
        }

        for (broker, queue_num) in &expected_broker_num_map_mut {
            for _ in 0..*queue_num {
                if let Some(queue_id) = wait_assign_queues.pop_front() {
                    expected_id_to_broker.insert(queue_id, broker.clone());
                }
            }
        }

        let new_epoch = ((max_epoch_and_num.0 as u64 + 1000).max(get_current_millis())) as i64;

        // construct the remapping info
        let mut brokers_to_map_out: HashSet<CheetahString> = HashSet::new();
        let mut brokers_to_map_in: HashSet<CheetahString> = HashSet::new();

        for (queue_id, broker) in &expected_id_to_broker {
            let topic_queue_mapping_one = global_id_map.get(queue_id);
            if topic_queue_mapping_one.is_none() {
                continue;
            }
            let topic_queue_mapping_one = topic_queue_mapping_one.unwrap();
            if topic_queue_mapping_one.bname() == broker.as_str() {
                continue;
            }

            // remapping
            let map_in_broker = broker.clone();
            let map_out_broker = CheetahString::from(topic_queue_mapping_one.bname());
            brokers_to_map_in.insert(map_in_broker.clone());
            brokers_to_map_out.insert(map_out_broker.clone());

            let map_in_config = broker_config_map.get_mut(&map_in_broker);
            if map_in_config.is_none() {
                let new_config = TopicConfigAndQueueMapping::new(
                    TopicConfig::with_queues(topic, 0, 0),
                    Some(ArcMut::new(TopicQueueMappingDetail {
                        topic_queue_mapping_info: TopicQueueMappingInfo::new(
                            topic.into(),
                            max_num,
                            map_in_broker.clone(),
                            new_epoch,
                        ),
                        hosted_queues: Some(HashMap::new()),
                    })),
                );
                broker_config_map.insert(map_in_broker.clone(), new_config);
            }

            let map_in_config = broker_config_map.get_mut(&map_in_broker).unwrap();
            map_in_config.topic_config.write_queue_nums += 1;
            map_in_config.topic_config.read_queue_nums += 1;

            let mut items: Vec<LogicQueueMappingItem> = topic_queue_mapping_one.items().clone();
            let last = items.last().cloned();
            if let Some(last) = last {
                items.push(LogicQueueMappingItem {
                    gen: last.gen + 1,
                    queue_id: map_in_config.topic_config.write_queue_nums as i32 - 1,
                    bname: Some(map_in_broker.clone()),
                    logic_offset: -1,
                    start_offset: 0,
                    end_offset: -1,
                    time_of_start: -1,
                    time_of_end: -1,
                });
            }

            // use the same object
            if let Some(detail) = &mut map_in_config.topic_queue_mapping_detail {
                if detail.hosted_queues.is_none() {
                    detail.hosted_queues = Some(HashMap::new());
                }
                if let Some(queues) = &mut detail.hosted_queues {
                    queues.insert(*queue_id, items.clone());
                }
            }

            let map_out_config = broker_config_map.get_mut(&map_out_broker);
            if let Some(map_out_config) = map_out_config {
                if let Some(detail) = &mut map_out_config.topic_queue_mapping_detail {
                    if detail.hosted_queues.is_none() {
                        detail.hosted_queues = Some(HashMap::new());
                    }
                    if let Some(queues) = &mut detail.hosted_queues {
                        queues.insert(*queue_id, items);
                    }
                }
            }
        }

        for config_mapping in broker_config_map.values_mut() {
            if let Some(detail) = &mut config_mapping.topic_queue_mapping_detail {
                detail.topic_queue_mapping_info.epoch = new_epoch;
                detail.topic_queue_mapping_info.total_queues = max_num;
            }
        }

        // double check
        TopicQueueMappingUtils::check_name_epoch_num_consistence(&CheetahString::from(topic), broker_config_map)?;
        global_id_map = TopicQueueMappingUtils::check_and_build_mapping_items(
            TopicQueueMappingUtils::get_mapping_detail_from_config(broker_config_map.values().cloned().collect())?,
            false,
            true,
        )?;
        TopicQueueMappingUtils::check_physical_queue_consistence(broker_config_map)?;
        TopicQueueMappingUtils::check_if_reuse_physical_queue(&global_id_map.values().cloned().collect())?;
        TopicQueueMappingUtils::check_leader_in_target_brokers(
            &global_id_map.values().cloned().collect::<Vec<_>>(),
            target_brokers,
        )?;

        let map = broker_config_map
            .iter()
            .map(|(k, v)| (CheetahString::from_string(k.to_string()), v.clone()))
            .collect();

        Ok(TopicRemappingDetailWrapper::new(
            topic.to_string().into(),
            topic_remapping_detail_wrapper::TYPE_REMAPPING.to_string().into(),
            new_epoch as u64,
            map,
            brokers_to_map_in,
            brokers_to_map_out,
        ))
    }
}
