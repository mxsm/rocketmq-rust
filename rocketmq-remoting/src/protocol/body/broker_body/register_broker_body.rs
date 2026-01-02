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
        use std::time::Instant;

        use tracing::error;
        use tracing::info;

        // Fast path: non-compressed data
        if !compress {
            return <Self as RemotingSerializable>::encode(self).unwrap_or_else(|e| {
                error!("Failed to encode RegisterBrokerBody: {:?}", e);
                Vec::new()
            });
        }

        let start = Instant::now();

        // Get DataVersion from topic_config_serialize_wrapper (align with Java)
        let data_version = &self
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .data_version;

        // Encode DataVersion
        let data_version_buffer = match data_version.encode() {
            Ok(buf) => buf,
            Err(e) => {
                error!("Failed to encode DataVersion: {:?}", e);
                return Vec::new();
            }
        };

        // Get topic config table reference (avoid clone for better performance)
        let topic_config_table = &self
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table;

        // Pre-calculate buffer size estimation for better performance
        let estimated_size = data_version_buffer.len()
            + topic_config_table.len() * 128  // estimate 128 bytes per topic config
            + 1024; // buffer for other data
        let mut bytes_mut = BytesMut::with_capacity(estimated_size);

        // Write DataVersion length and data
        bytes_mut.put_i32(data_version_buffer.len() as i32);
        bytes_mut.put(data_version_buffer.as_slice());

        // Write topic config number
        let topic_number = topic_config_table.len();
        bytes_mut.put_i32(topic_number as i32);

        // Write topic configs one by one (align with Java: iterate entries)
        for (_topic_name, topic_config) in topic_config_table.iter() {
            let topic_config_str = topic_config.encode();
            let topic_config_bytes = topic_config_str.as_bytes();
            bytes_mut.put_i32(topic_config_bytes.len() as i32);
            bytes_mut.put(topic_config_bytes);
        }

        // Serialize filter server list to JSON (align with Java)
        let filter_server_list_json = match SerdeJsonUtils::serialize_json(&self.filter_server_list) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize filter server list: {:?}", e);
                String::from("[]")
            }
        };
        let filter_server_list_bytes = filter_server_list_json.as_bytes();

        // Write filter server list JSON length
        bytes_mut.put_i32(filter_server_list_bytes.len() as i32);
        // Write filter server list JSON
        bytes_mut.put(filter_server_list_bytes);

        // Write topic queue mapping info (align with Java: handle null case)
        let topic_queue_mapping_info_map = &self.topic_config_serialize_wrapper.topic_queue_mapping_info_map;

        bytes_mut.put_i32(topic_queue_mapping_info_map.len() as i32);

        for entry in topic_queue_mapping_info_map.iter() {
            let queue_mapping = entry.value();
            match queue_mapping.encode() {
                Ok(mapping_bytes) => {
                    let bytes_slice: &[u8] = mapping_bytes.as_slice();
                    bytes_mut.put_i32(bytes_slice.len() as i32);
                    bytes_mut.put(bytes_slice);
                }
                Err(e) => {
                    error!("Failed to encode TopicQueueMappingInfo: {:?}", e);
                    // Continue encoding other mappings
                }
            }
        }

        let uncompressed_data = bytes_mut.freeze();

        // Compress data using Deflate with best compression
        let mut encoder = DeflateEncoder::new(Vec::with_capacity(uncompressed_data.len() / 2), Compression::best());

        match encoder.write_all(uncompressed_data.as_ref()) {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to write data to compressor: {:?}", e);
                return Vec::new();
            }
        }

        let compressed = match encoder.finish() {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to finish compression: {:?}", e);
                return Vec::new();
            }
        };

        let elapsed = start.elapsed().as_millis();
        if elapsed > Self::MINIMUM_TAKE_TIME_MILLISECOND {
            info!("Compressing RegisterBrokerBody takes {}ms", elapsed);
        }

        compressed
    }
}

impl RegisterBrokerBody {
    /// Minimum time threshold (in milliseconds) for logging decompression performance
    const MINIMUM_TAKE_TIME_MILLISECOND: u128 = 100;

    pub fn decode(
        bytes: &Bytes,
        compressed: bool,
        broker_version: RocketMqVersion,
    ) -> rocketmq_error::RocketMQResult<RegisterBrokerBody> {
        use std::time::Instant;

        use tracing::debug;
        use tracing::error;
        use tracing::info;

        // Fast path: non-compressed data
        if !compressed {
            return SerdeJsonUtils::from_json_bytes::<RegisterBrokerBody>(bytes.as_ref()).map_err(|e| {
                error!("Failed to decode RegisterBrokerBody: {:?}", e);
                rocketmq_error::RocketMQError::request_body_invalid(
                    "decode",
                    format!("Failed to decode RegisterBrokerBody: {:?}", e),
                )
            });
        }

        let start = Instant::now();
        let mut register_broker_body = RegisterBrokerBody::default();

        // Decompress data
        let mut decoder = DeflateDecoder::new(bytes.as_ref());
        // Pre-allocate with estimated size to reduce reallocations
        let mut decompressed = Vec::with_capacity(bytes.len() * 2);

        if let Err(e) = decoder.read_to_end(&mut decompressed) {
            error!("Failed to decompress RegisterBrokerBody: {:?}", e);
            return Err(rocketmq_error::RocketMQError::request_body_invalid(
                "decompress",
                format!("Failed to decompress RegisterBrokerBody: {:?}", e),
            ));
        }

        let mut buf = Bytes::from(decompressed);

        // 1. Decode DataVersion
        if buf.remaining() < 4 {
            error!("Insufficient data for DataVersion length");
            return Err(rocketmq_error::RocketMQError::request_body_invalid(
                "decode",
                "Insufficient data for DataVersion length",
            ));
        }
        let data_version_length = buf.get_i32() as usize;
        if buf.remaining() < data_version_length {
            let msg = format!(
                "Insufficient data for DataVersion (expected: {}, remaining: {})",
                data_version_length,
                buf.remaining()
            );
            error!("{}", msg);
            return Err(rocketmq_error::RocketMQError::request_body_invalid("decode", msg));
        }

        let data_version_bytes = buf.split_to(data_version_length);
        let data_version = DataVersion::decode(data_version_bytes.as_ref()).map_err(|e| {
            error!("Failed to decode DataVersion: {:?}", e);
            rocketmq_error::RocketMQError::request_body_invalid(
                "decode",
                format!("Failed to decode DataVersion: {:?}", e),
            )
        })?;

        register_broker_body.topic_config_serialize_wrapper.mapping_data_version = data_version.clone();
        // set in topic_config_serialize_wrapper
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .data_version = data_version;

        // 2. Decode TopicConfig table
        if buf.remaining() < 4 {
            error!("Insufficient data for topic config count");
            return Err(rocketmq_error::RocketMQError::request_body_invalid(
                "decode",
                "Insufficient data for topic config count",
            ));
        }
        let topic_config_number = buf.get_i32();
        debug!("{} topic configs to extract", topic_config_number);

        for i in 0..topic_config_number {
            if buf.remaining() < 4 {
                error!("Insufficient data for topic config {} length", i);
                break;
            }
            let topic_config_length = buf.get_i32() as usize;

            if buf.remaining() < topic_config_length {
                error!(
                    "Insufficient data for topic config {} (expected: {}, remaining: {})",
                    i,
                    topic_config_length,
                    buf.remaining()
                );
                break;
            }

            let topic_config_bytes = buf.split_to(topic_config_length);

            // Avoid unnecessary allocation by using from_utf8 directly
            match std::str::from_utf8(&topic_config_bytes) {
                Ok(topic_config_json) => {
                    let mut topic_config = TopicConfig::default();
                    topic_config.decode(topic_config_json);
                    if let Some(topic_name) = &topic_config.topic_name {
                        register_broker_body
                            .topic_config_serialize_wrapper
                            .topic_config_serialize_wrapper
                            .topic_config_table
                            .insert(topic_name.clone(), topic_config);
                    }
                }
                Err(_) => {
                    // Fallback to lossy conversion
                    let topic_config_json = String::from_utf8_lossy(&topic_config_bytes);
                    let mut topic_config = TopicConfig::default();
                    topic_config.decode(&topic_config_json);
                    if let Some(topic_name) = &topic_config.topic_name {
                        register_broker_body
                            .topic_config_serialize_wrapper
                            .topic_config_serialize_wrapper
                            .topic_config_table
                            .insert(topic_name.clone(), topic_config);
                    }
                }
            }
        }

        // 3. Decode filter server list
        if buf.remaining() < 4 {
            error!("Insufficient data for filter server list length");
            return Err(rocketmq_error::RocketMQError::request_body_invalid(
                "decode",
                "Insufficient data for filter server list length",
            ));
        }
        let filter_server_list_json_length = buf.get_i32() as usize;

        if buf.remaining() < filter_server_list_json_length {
            let msg = format!(
                "Insufficient data for filter server list (expected: {}, remaining: {})",
                filter_server_list_json_length,
                buf.remaining()
            );
            error!("{}", msg);
            return Err(rocketmq_error::RocketMQError::request_body_invalid("decode", msg));
        }

        let filter_server_list_json = buf.split_to(filter_server_list_json_length);
        match SerdeJsonUtils::from_json_slice::<Vec<CheetahString>>(filter_server_list_json.as_ref()) {
            Ok(list) => register_broker_body.filter_server_list = list,
            Err(e) => {
                error!(
                    "Failed to parse filter server list: {:?}, json: {}",
                    e,
                    String::from_utf8_lossy(filter_server_list_json.as_ref())
                );
            }
        }

        // 4. Decode TopicQueueMappingInfo (V5.0.0+)
        if broker_version >= RocketMqVersion::V5_0_0 && buf.remaining() >= 4 {
            let topic_queue_mapping_num = buf.get_i32();
            // Pre-allocate with shard count for better performance
            let topic_queue_mapping_info_map =
                DashMap::with_capacity_and_shard_amount(topic_queue_mapping_num as usize, 16);

            for i in 0..topic_queue_mapping_num {
                if buf.remaining() < 4 {
                    error!("Insufficient data for queue mapping {} length", i);
                    break;
                }
                let mapping_json_len = buf.get_i32() as usize;

                if buf.remaining() < mapping_json_len {
                    error!(
                        "Insufficient data for queue mapping {} (expected: {}, remaining: {})",
                        i,
                        mapping_json_len,
                        buf.remaining()
                    );
                    break;
                }

                let buffer = buf.split_to(mapping_json_len);
                match TopicQueueMappingInfo::decode(buffer.as_ref()) {
                    Ok(info) => {
                        if let Some(topic) = &info.topic {
                            topic_queue_mapping_info_map.insert(topic.clone(), ArcMut::new(info));
                        }
                    }
                    Err(e) => {
                        error!("Failed to decode TopicQueueMappingInfo {}: {:?}", i, e);
                    }
                }
            }
            register_broker_body
                .topic_config_serialize_wrapper
                .topic_queue_mapping_info_map = topic_queue_mapping_info_map;
        }

        let elapsed = start.elapsed().as_millis();
        if elapsed > Self::MINIMUM_TAKE_TIME_MILLISECOND {
            info!("Decompressing RegisterBrokerBody takes {}ms", elapsed);
        }

        Ok(register_broker_body)
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
        let decoded = RegisterBrokerBody::decode(&Bytes::from(encoded), false, RocketMqVersion::V5_0_0)
            .expect("decode should succeed");
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
        let compare_decode = RegisterBrokerBody::decode(&Bytes::from(compare_encode), true, RocketMqVersion::V5_0_0)
            .expect("decode should succeed");
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

    #[test]
    fn test_decode_with_large_dataset() {
        // Test with larger dataset to verify performance optimizations
        let mut register_broker_body = RegisterBrokerBody::default();
        let mut topic_config_table = HashMap::new();

        // Create 100 topic configs
        for i in 0..100 {
            topic_config_table.insert(
                CheetahString::from_string(format!("topic_{}", i)),
                TopicConfig::new(CheetahString::from_string(format!("topic_{}", i))),
            );
        }
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table = topic_config_table;

        // Add filter servers
        register_broker_body.filter_server_list = vec!["filter1".into(), "filter2".into(), "filter3".into()];

        let encoded = register_broker_body.encode(true);
        let decoded = RegisterBrokerBody::decode(&Bytes::from(encoded), true, RocketMqVersion::V5_0_0)
            .expect("decode should succeed");

        assert_eq!(
            decoded
                .topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .len(),
            100
        );
        assert_eq!(decoded.filter_server_list.len(), 3);
    }

    #[test]
    fn test_decode_empty_compressed_data() {
        // Test edge case: empty compressed data
        let body = RegisterBrokerBody::default();
        let encoded = body.encode(true);
        let decoded = RegisterBrokerBody::decode(&Bytes::from(encoded), true, RocketMqVersion::V5_0_0)
            .expect("decode should succeed");
        assert!(decoded
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table
            .is_empty());
    }

    #[test]
    fn test_decode_version_compatibility() {
        // Test V3 version (no TopicQueueMappingInfo)
        let body = RegisterBrokerBody::default();
        let encoded = body.encode(true);
        let decoded = RegisterBrokerBody::decode(&Bytes::from(encoded.clone()), true, RocketMqVersion::V3_0_11)
            .expect("decode should succeed");
        assert!(decoded
            .topic_config_serialize_wrapper
            .topic_queue_mapping_info_map
            .is_empty());

        // Test V5 version (with TopicQueueMappingInfo)
        let decoded_v5 = RegisterBrokerBody::decode(&Bytes::from(encoded), true, RocketMqVersion::V5_0_0)
            .expect("decode should succeed");
        // Should not panic
        assert!(decoded_v5
            .topic_config_serialize_wrapper
            .topic_queue_mapping_info_map
            .is_empty());
    }

    #[test]
    fn test_decode_invalid_compressed_data() {
        // Test with invalid compressed data
        let invalid_data = vec![1, 2, 3, 4, 5];
        let result = RegisterBrokerBody::decode(&Bytes::from(invalid_data), true, RocketMqVersion::V5_0_0);
        assert!(result.is_err(), "Should fail with invalid compressed data");
    }

    #[test]
    fn test_decode_insufficient_data() {
        // Test with insufficient data (less than 4 bytes for length field)
        let body = RegisterBrokerBody::default();
        let mut encoded = body.encode(true);
        // Truncate to create invalid data
        encoded.truncate(5);

        let result = RegisterBrokerBody::decode(&Bytes::from(encoded), true, RocketMqVersion::V5_0_0);
        assert!(result.is_err(), "Should fail with insufficient data");
    }

    #[test]
    fn test_decode_invalid_json() {
        // Test with invalid JSON in non-compressed mode
        let invalid_json = b"{ invalid json }";
        let result = RegisterBrokerBody::decode(&Bytes::from(invalid_json.to_vec()), false, RocketMqVersion::V5_0_0);
        assert!(result.is_err(), "Should fail with invalid JSON");
    }

    #[test]
    fn test_encode_performance_with_large_dataset() {
        // Test encoding performance with large dataset
        let mut register_broker_body = RegisterBrokerBody::default();
        let mut topic_config_table = HashMap::new();

        // Create 1000 topic configs
        for i in 0..1000 {
            topic_config_table.insert(
                CheetahString::from_string(format!("topic_{}", i)),
                TopicConfig::new(CheetahString::from_string(format!("topic_{}", i))),
            );
        }
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table = topic_config_table;

        // Add filter servers
        register_broker_body.filter_server_list = vec![
            "filter1".into(),
            "filter2".into(),
            "filter3".into(),
            "filter4".into(),
            "filter5".into(),
        ];

        // Test compression
        let compressed = register_broker_body.encode(true);
        assert!(!compressed.is_empty());

        // Test non-compression
        let uncompressed = register_broker_body.encode(false);
        assert!(!uncompressed.is_empty());

        // Compressed should be smaller than uncompressed
        assert!(
            compressed.len() < uncompressed.len(),
            "Compressed size ({}) should be less than uncompressed size ({})",
            compressed.len(),
            uncompressed.len()
        );
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        // Test complete roundtrip with various data
        let mut register_broker_body = RegisterBrokerBody::default();
        let mut topic_config_table = HashMap::new();

        // Add some topic configs
        for i in 0..50 {
            topic_config_table.insert(
                CheetahString::from_string(format!("test_topic_{}", i)),
                TopicConfig::new(CheetahString::from_string(format!("test_topic_{}", i))),
            );
        }
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table = topic_config_table;

        register_broker_body.filter_server_list = vec!["192.168.1.1:8080".into(), "192.168.1.2:8080".into()];

        // Encode with compression
        let encoded = register_broker_body.encode(true);

        // Decode
        let decoded = RegisterBrokerBody::decode(&Bytes::from(encoded), true, RocketMqVersion::V5_0_0)
            .expect("decode should succeed");

        // Verify data integrity
        assert_eq!(
            decoded
                .topic_config_serialize_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .len(),
            50
        );
        assert_eq!(decoded.filter_server_list.len(), 2);
        assert_eq!(
            decoded.filter_server_list[0],
            CheetahString::from_static_str("192.168.1.1:8080")
        );
    }
}
