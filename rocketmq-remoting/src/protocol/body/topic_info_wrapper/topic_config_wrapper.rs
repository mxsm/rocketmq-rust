// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper as Canonical;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

pub use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;

#[derive(Debug, Clone, Default)]
pub struct TopicConfigAndMappingSerializeWrapper {
    pub topic_queue_mapping_info_map: DashMap<CheetahString, ArcMut<TopicQueueMappingInfo>>,
    pub topic_queue_mapping_detail_map: DashMap<CheetahString, ArcMut<TopicQueueMappingDetail>>,
    pub mapping_data_version: DataVersion,
    pub topic_config_serialize_wrapper: TopicConfigSerializeWrapper,
}

impl Serialize for TopicConfigAndMappingSerializeWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Canonical::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TopicConfigAndMappingSerializeWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Canonical::deserialize(deserializer).map(Self::from)
    }
}

impl From<&TopicConfigAndMappingSerializeWrapper> for Canonical {
    fn from(value: &TopicConfigAndMappingSerializeWrapper) -> Self {
        Self {
            topic_queue_mapping_info_map: value
                .topic_queue_mapping_info_map
                .iter()
                .map(|entry| (entry.key().clone(), (**entry.value()).clone()))
                .collect(),
            topic_queue_mapping_detail_map: value
                .topic_queue_mapping_detail_map
                .iter()
                .map(|entry| (entry.key().clone(), (**entry.value()).clone()))
                .collect(),
            mapping_data_version: value.mapping_data_version.clone(),
            topic_config_serialize_wrapper: value.topic_config_serialize_wrapper.clone(),
        }
    }
}

impl From<Canonical> for TopicConfigAndMappingSerializeWrapper {
    fn from(value: Canonical) -> Self {
        Self {
            topic_queue_mapping_info_map: value
                .topic_queue_mapping_info_map
                .into_iter()
                .map(|(key, info)| (key, ArcMut::new(info)))
                .collect(),
            topic_queue_mapping_detail_map: value
                .topic_queue_mapping_detail_map
                .into_iter()
                .map(|(key, detail)| (key, ArcMut::new(detail)))
                .collect(),
            mapping_data_version: value.mapping_data_version,
            topic_config_serialize_wrapper: value.topic_config_serialize_wrapper,
        }
    }
}

impl TopicConfigAndMappingSerializeWrapper {
    pub fn topic_queue_mapping_info_map(&self) -> &DashMap<CheetahString, ArcMut<TopicQueueMappingInfo>> {
        &self.topic_queue_mapping_info_map
    }

    pub fn topic_queue_mapping_detail_map(&self) -> &DashMap<CheetahString, ArcMut<TopicQueueMappingDetail>> {
        &self.topic_queue_mapping_detail_map
    }

    pub fn mapping_data_version(&self) -> &DataVersion {
        &self.mapping_data_version
    }

    pub fn topic_config_serialize_wrapper(&self) -> &TopicConfigSerializeWrapper {
        &self.topic_config_serialize_wrapper
    }
}
