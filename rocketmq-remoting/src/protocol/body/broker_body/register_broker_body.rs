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
use std::io::prelude::*;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
use crate::protocol::DataVersion;
use crate::protocol::RemotingDeserializable;
use crate::protocol::RemotingSerializable;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RegisterBrokerBody {
    #[serde(rename = "topicConfigSerializeWrapper")]
    pub topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
    #[serde(rename = "filterServerList")]
    pub filter_server_list: Vec<CheetahString>,
}

impl RegisterBrokerBody {
    pub fn new(
        topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<CheetahString>,
    ) -> Self {
        RegisterBrokerBody {
            topic_config_serialize_wrapper,
            filter_server_list,
        }
    }

    pub fn topic_config_serialize_wrapper(&self) -> &TopicConfigAndMappingSerializeWrapper {
        &self.topic_config_serialize_wrapper
    }

    pub fn filter_server_list(&self) -> &Vec<CheetahString> {
        &self.filter_server_list
    }

    pub fn encode(&self, compress: bool) -> Vec<u8> {
        if !compress {
            return <Self as RemotingSerializable>::encode(self)
                .expect("Encode RegisterBrokerBody failed");
        }
        let mut bytes_mut = BytesMut::new();
        {
            let buffer = self
                .topic_config_serialize_wrapper
                .mapping_data_version
                .encode()
                .expect("Encode DataVersion failed");
            let topic_config_table = self
                .topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .clone();

            bytes_mut.put_i32(buffer.len() as i32);
            bytes_mut.put(buffer.as_ref());
            let topic_number = topic_config_table.len();
            bytes_mut.put_i32(topic_number as i32);
            for (_topic, topic_config) in topic_config_table {
                let topic_config_bytes = topic_config.encode();
                bytes_mut.put_i32(topic_config_bytes.len() as i32);
                bytes_mut.put(topic_config_bytes.as_bytes());
            }
            let buffer = SerdeJsonUtils::serialize_json(&self.filter_server_list).unwrap();
            bytes_mut.put_i32(buffer.len() as i32);
            bytes_mut.put(buffer.as_bytes());
            let topic_queue_mapping_info_map = self
                .topic_config_serialize_wrapper
                .topic_queue_mapping_info_map
                .clone();
            bytes_mut.put_i32(topic_queue_mapping_info_map.len() as i32);
            for (_, queue_mapping) in topic_queue_mapping_info_map {
                let queue_mapping_bytes =
                    queue_mapping.encode().expect("Encode queue mapping failed");
                bytes_mut.put_i32(queue_mapping_bytes.len() as i32);
                bytes_mut.put(queue_mapping_bytes.as_slice());
            }
        }
        let bytes = bytes_mut.freeze();

        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(bytes.as_ref()).unwrap();
        encoder.finish().unwrap()
    }
}

impl RegisterBrokerBody {
    pub fn decode(
        bytes: &Bytes,
        compressed: bool,
        broker_version: RocketMqVersion,
    ) -> RegisterBrokerBody {
        if !compressed {
            return SerdeJsonUtils::from_json_bytes::<RegisterBrokerBody>(bytes.iter().as_slice())
                .unwrap();
        }
        let mut decoder = DeflateDecoder::new(bytes.as_ref());
        let mut vec = Vec::new();
        let result = decoder.read_to_end(&mut vec);
        let mut register_broker_body = RegisterBrokerBody::default();
        if result.is_err() {
            return register_broker_body;
        }
        let mut bytes = Bytes::from(vec);
        let data_version_length = bytes.get_i32();
        let data_version_bytes = bytes.copy_to_bytes(data_version_length as usize);
        let data_version = DataVersion::decode(data_version_bytes.as_ref()).unwrap();
        register_broker_body
            .topic_config_serialize_wrapper
            .mapping_data_version = data_version;

        let topic_config_number = bytes.get_i32();
        for _ in 0..topic_config_number {
            let topic_config_length = bytes.get_i32();
            let topic_config_bytes = bytes.copy_to_bytes(topic_config_length as usize);
            let cow = String::from_utf8_lossy(topic_config_bytes.as_ref()).to_string();
            let mut topic_config = TopicConfig::default();
            topic_config.decode(cow.as_str());
            let topic = topic_config.topic_name.clone().unwrap_or_default();
            register_broker_body
                .topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .insert(topic, topic_config);
        }

        let filter_server_list_json_length = bytes.get_i32();
        let filter_server_list_json = bytes.copy_to_bytes(filter_server_list_json_length as usize);
        register_broker_body.filter_server_list =
            SerdeJsonUtils::from_json_slice(filter_server_list_json.as_ref()).unwrap();

        if broker_version >= RocketMqVersion::V5_0_0 {
            let topic_queue_mapping_num = bytes.get_i32();
            let topic_queue_mapping_info_map = DashMap::new();
            for _ in 0..topic_queue_mapping_num {
                let queue_mapping_length = bytes.get_i32();
                let buffer = bytes.copy_to_bytes(queue_mapping_length as usize);
                let info = ArcMut::new(TopicQueueMappingInfo::decode(buffer.as_ref()).unwrap());
                topic_queue_mapping_info_map.insert(info.topic.clone().unwrap_or_default(), info);
            }
            register_broker_body
                .topic_config_serialize_wrapper
                .topic_queue_mapping_info_map = topic_queue_mapping_info_map;
        }
        register_broker_body
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::mq_version::RocketMqVersion;

    use super::*;

    #[test]
    fn encode_without_compression() {
        let wrapper = TopicConfigAndMappingSerializeWrapper::default();
        let filter_list = vec!["filter1".into(), "filter2".into()];
        let body = RegisterBrokerBody::new(wrapper, filter_list);
        let encoded = body.encode(false);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn encode_with_compression() {
        let wrapper = TopicConfigAndMappingSerializeWrapper::default();
        let filter_list = vec!["filter1".into(), "filter2".into()];
        let body = RegisterBrokerBody::new(wrapper, filter_list);
        let encoded = body.encode(true);
        assert!(!encoded.is_empty());
    }

    #[test]
    fn decode_without_compression() {
        let wrapper = TopicConfigAndMappingSerializeWrapper::default();
        let filter_list = vec!["filter1".into(), "filter2".into()];
        let body = RegisterBrokerBody::new(wrapper, filter_list);
        let encoded = body.encode(false);
        let decoded =
            RegisterBrokerBody::decode(&Bytes::from(encoded), false, RocketMqVersion::V5_0_0);
        assert_eq!(decoded.filter_server_list, body.filter_server_list);
    }

    #[test]
    fn test_encode() {
        let mut register_broker_body = RegisterBrokerBody::default();
        let mut topic_config_table = HashMap::new();
        for i in 0..1 {
            topic_config_table.insert(
                CheetahString::from_string(i.to_string()),
                TopicConfig::new(CheetahString::from_string(i.to_string())),
            );
        }
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table = topic_config_table;
        let compare_encode = register_broker_body.encode(true);
        let compare_decode =
            RegisterBrokerBody::decode(&Bytes::from(compare_encode), true, RocketMqVersion::V5_0_0);
        assert_eq!(
            register_broker_body
                .topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .get("1"),
            compare_decode
                .topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .get("1")
        );
    }
}
