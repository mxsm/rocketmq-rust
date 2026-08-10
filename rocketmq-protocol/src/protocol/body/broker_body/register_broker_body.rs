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
use std::io::prelude::*;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use rocketmq_model::topic::TopicConfig;
use rocketmq_model::version::RocketMqVersion;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use crate::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
use crate::protocol::DataVersion;
use crate::protocol::RemotingDeserializable;
use crate::protocol::RemotingSerializable;

macro_rules! error { ($($tokens:tt)*) => { let _ = format_args!($($tokens)*); }; }
macro_rules! debug { ($($tokens:tt)*) => { let _ = format_args!($($tokens)*); }; }

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RegisterBrokerBody {
    #[serde(rename = "topicConfigSerializeWrapper")]
    pub topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
    #[serde(rename = "filterServerList")]
    pub filter_server_list: Vec<CheetahString>,
}

/// Resource limits applied while decoding a broker registration body.
///
/// The defaults retain compatibility with large RocketMQ deployments while
/// bounding allocations before data supplied by the peer is trusted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegisterBrokerDecodeLimits {
    pub max_wire_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_topic_count: usize,
    pub max_mapping_count: usize,
    pub max_filter_server_count: usize,
    pub max_single_entry_bytes: usize,
}

impl Default for RegisterBrokerDecodeLimits {
    fn default() -> Self {
        Self {
            max_wire_bytes: 16 * 1024 * 1024,
            max_decompressed_bytes: 64 * 1024 * 1024,
            max_topic_count: 100_000,
            max_mapping_count: 100_000,
            max_filter_server_count: 10_000,
            max_single_entry_bytes: 1024 * 1024,
        }
    }
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
        // Fast path: non-compressed data
        if !compress {
            return <Self as RemotingSerializable>::encode(self).unwrap_or_else(|e| {
                error!("Failed to encode RegisterBrokerBody: {:?}", e);
                Vec::new()
            });
        }

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

        // Write topic configs one by one.
        for topic_config in topic_config_table.values() {
            let topic_config_str = topic_config.encode();
            let topic_config_bytes = topic_config_str.as_bytes();
            bytes_mut.put_i32(topic_config_bytes.len() as i32);
            bytes_mut.put(topic_config_bytes);
        }

        // Serialize filter server list to JSON (align with Java)
        let filter_server_list_json = match serde_json::to_string(&self.filter_server_list) {
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

        for queue_mapping in topic_queue_mapping_info_map.values() {
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

        compressed
    }
}

impl RegisterBrokerBody {
    pub fn decode(
        bytes: &Bytes,
        compressed: bool,
        broker_version: RocketMqVersion,
    ) -> rocketmq_error::RocketMQResult<RegisterBrokerBody> {
        Self::decode_with_limits(bytes, compressed, broker_version, RegisterBrokerDecodeLimits::default())
    }

    pub fn decode_with_limits(
        bytes: &Bytes,
        compressed: bool,
        broker_version: RocketMqVersion,
        limits: RegisterBrokerDecodeLimits,
    ) -> rocketmq_error::RocketMQResult<RegisterBrokerBody> {
        if bytes.len() > limits.max_wire_bytes {
            return Err(invalid_registration(format!(
                "registration body exceeds wire limit: {} > {} bytes",
                bytes.len(),
                limits.max_wire_bytes
            )));
        }

        // Fast path: non-compressed data
        if !compressed {
            let body = serde_json::from_slice::<RegisterBrokerBody>(bytes.as_ref()).map_err(|e| {
                error!("Failed to decode RegisterBrokerBody: {:?}", e);
                invalid_registration(format!("Failed to decode RegisterBrokerBody: {e}"))
            })?;
            validate_decoded_body(&body, limits)?;
            return Ok(body);
        }

        // Decompress data
        let decoder = DeflateDecoder::new(bytes.as_ref());
        let read_limit = u64::try_from(limits.max_decompressed_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut limited_decoder = decoder.take(read_limit);
        let initial_capacity = bytes.len().saturating_mul(2).min(limits.max_decompressed_bytes);
        let mut decompressed = Vec::new();
        decompressed
            .try_reserve(initial_capacity)
            .map_err(|e| invalid_registration(format!("unable to reserve decompression buffer: {e}")))?;

        if let Err(e) = limited_decoder.read_to_end(&mut decompressed) {
            error!("Failed to decompress RegisterBrokerBody: {:?}", e);
            return Err(invalid_registration(format!(
                "Failed to decompress RegisterBrokerBody: {e}"
            )));
        }
        if decompressed.len() > limits.max_decompressed_bytes {
            return Err(invalid_registration(format!(
                "registration body exceeds decompressed limit: {} > {} bytes",
                decompressed.len(),
                limits.max_decompressed_bytes
            )));
        }

        let mut buf = Bytes::from(decompressed);

        // 1. Decode DataVersion
        let data_version_bytes = read_entry(&mut buf, "DataVersion", limits.max_single_entry_bytes)?;
        let data_version = DataVersion::decode(data_version_bytes.as_ref()).map_err(|e| {
            error!("Failed to decode DataVersion: {:?}", e);
            invalid_registration(format!("Failed to decode DataVersion: {e}"))
        })?;

        // 2. Decode TopicConfig table
        let topic_config_number = read_count(&mut buf, "topic config", limits.max_topic_count)?;
        debug!("{} topic configs to extract", topic_config_number);
        let mut topic_config_table = HashMap::new();
        topic_config_table
            .try_reserve(topic_config_number)
            .map_err(|e| invalid_registration(format!("unable to reserve topic config table: {e}")))?;

        for i in 0..topic_config_number {
            let topic_config_bytes = read_entry(&mut buf, "topic config", limits.max_single_entry_bytes)?;
            let topic_config_text = std::str::from_utf8(topic_config_bytes.as_ref())
                .map_err(|e| invalid_registration(format!("topic config {i} is not valid UTF-8: {e}")))?;
            let mut topic_config = TopicConfig::default();
            if !topic_config.decode(topic_config_text) {
                return Err(invalid_registration(format!(
                    "topic config {i} has an invalid wire representation"
                )));
            }
            let Some(topic_name) = topic_config.topic_name.clone().filter(|name| !name.is_empty()) else {
                return Err(invalid_registration(format!("topic config {i} has no topic name")));
            };
            topic_config_table.insert(topic_name, topic_config);
        }

        // 3. Decode filter server list
        let filter_server_list_json = read_entry(&mut buf, "filter server list", limits.max_single_entry_bytes)?;
        let filter_server_list = serde_json::from_slice::<Vec<CheetahString>>(filter_server_list_json.as_ref())
            .map_err(|e| invalid_registration(format!("Failed to parse filter server list: {e}")))?;
        if filter_server_list.len() > limits.max_filter_server_count {
            return Err(invalid_registration(format!(
                "filter server count exceeds limit: {} > {}",
                filter_server_list.len(),
                limits.max_filter_server_count
            )));
        }

        // 4. Decode TopicQueueMappingInfo (V5.0.0+)
        let mut topic_queue_mapping_info_map = HashMap::new();
        if broker_version >= RocketMqVersion::V5_0_0 {
            let topic_queue_mapping_num = read_count(&mut buf, "queue mapping", limits.max_mapping_count)?;
            topic_queue_mapping_info_map
                .try_reserve(topic_queue_mapping_num)
                .map_err(|e| invalid_registration(format!("unable to reserve queue mapping table: {e}")))?;

            for i in 0..topic_queue_mapping_num {
                let buffer = read_entry(&mut buf, "queue mapping", limits.max_single_entry_bytes)?;
                let info = TopicQueueMappingInfo::decode(buffer.as_ref())
                    .map_err(|e| invalid_registration(format!("Failed to decode queue mapping {i}: {e}")))?;
                let Some(topic) = info.topic.clone().filter(|topic| !topic.is_empty()) else {
                    return Err(invalid_registration(format!("queue mapping {i} has no topic name")));
                };
                topic_queue_mapping_info_map.insert(topic, info);
            }
        }

        if broker_version >= RocketMqVersion::V5_0_0 && buf.has_remaining() {
            return Err(invalid_registration(format!(
                "registration body contains {} trailing bytes",
                buf.remaining()
            )));
        }

        let mut register_broker_body = RegisterBrokerBody::default();
        register_broker_body.topic_config_serialize_wrapper.mapping_data_version = data_version.clone();
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .data_version = data_version;
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table = topic_config_table;
        register_broker_body
            .topic_config_serialize_wrapper
            .topic_queue_mapping_info_map = topic_queue_mapping_info_map;
        register_broker_body.filter_server_list = filter_server_list;

        Ok(register_broker_body)
    }
}

fn invalid_registration(reason: impl Into<String>) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::request_body_invalid("decode_register_broker_body", reason)
}

fn read_count(buf: &mut Bytes, field: &str, maximum: usize) -> rocketmq_error::RocketMQResult<usize> {
    let count = read_i32(buf, &format!("{field} count"))?;
    let count =
        usize::try_from(count).map_err(|_| invalid_registration(format!("{field} count must not be negative")))?;
    if count > maximum {
        return Err(invalid_registration(format!(
            "{field} count exceeds limit: {count} > {maximum}"
        )));
    }
    Ok(count)
}

fn read_entry(buf: &mut Bytes, field: &str, maximum: usize) -> rocketmq_error::RocketMQResult<Bytes> {
    let length = read_i32(buf, &format!("{field} length"))?;
    let length =
        usize::try_from(length).map_err(|_| invalid_registration(format!("{field} length must not be negative")))?;
    if length > maximum {
        return Err(invalid_registration(format!(
            "{field} length exceeds limit: {length} > {maximum}"
        )));
    }
    if buf.remaining() < length {
        return Err(invalid_registration(format!(
            "insufficient data for {field}: expected {length}, remaining {}",
            buf.remaining()
        )));
    }
    Ok(buf.split_to(length))
}

fn read_i32(buf: &mut Bytes, field: &str) -> rocketmq_error::RocketMQResult<i32> {
    if buf.remaining() < std::mem::size_of::<i32>() {
        return Err(invalid_registration(format!("insufficient data for {field}")));
    }
    Ok(buf.get_i32())
}

fn validate_decoded_body(
    body: &RegisterBrokerBody,
    limits: RegisterBrokerDecodeLimits,
) -> rocketmq_error::RocketMQResult<()> {
    let topic_count = body
        .topic_config_serialize_wrapper
        .topic_config_serialize_wrapper
        .topic_config_table
        .len();
    if topic_count > limits.max_topic_count {
        return Err(invalid_registration(format!(
            "topic config count exceeds limit: {topic_count} > {}",
            limits.max_topic_count
        )));
    }
    let mapping_count = body.topic_config_serialize_wrapper.topic_queue_mapping_info_map.len();
    if mapping_count > limits.max_mapping_count {
        return Err(invalid_registration(format!(
            "queue mapping count exceeds limit: {mapping_count} > {}",
            limits.max_mapping_count
        )));
    }
    if body.filter_server_list.len() > limits.max_filter_server_count {
        return Err(invalid_registration(format!(
            "filter server count exceeds limit: {} > {}",
            body.filter_server_list.len(),
            limits.max_filter_server_count
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_model::topic::TopicConfig;
    use rocketmq_model::version::RocketMqVersion;

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
