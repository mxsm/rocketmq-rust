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

use rocketmq_common::common::message::MessageConst;
use rocketmq_error::RocketMQError;

use crate::rocksdb::error::codec_error;

pub const MAX_PHYSICAL_OFFSET_CHECKPOINT_TOPIC: &str = "CHECKPOINT_TOPIC";
pub const INDEX_KEY_SPLIT: &str = "@";
const MILLIS_FOR_HOUR: i64 = 3_600_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeQueueKey {
    pub topic: String,
    pub queue_id: i32,
    pub cq_offset: i64,
}

impl ConsumeQueueKey {
    const CTRL_0: u8 = 0;
    const CTRL_1: u8 = 1;
    const CTRL_2: u8 = 2;

    pub fn encoded_len(&self) -> usize {
        4 + 1 + self.topic.len() + 1 + 4 + 1 + 8
    }

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        let topic_len = i32::try_from(self.topic.len()).map_err(|_| RocketMQError::ConfigInvalidValue {
            key: "rocksdb.consume_queue.topic",
            value: self.topic.len().to_string(),
            reason: "topic length exceeds Java i32 key layout".to_string(),
        })?;

        dst.reserve(self.encoded_len());
        dst.extend_from_slice(&topic_len.to_be_bytes());
        dst.push(Self::CTRL_1);
        dst.extend_from_slice(self.topic.as_bytes());
        dst.push(Self::CTRL_1);
        dst.extend_from_slice(&self.queue_id.to_be_bytes());
        dst.push(Self::CTRL_1);
        dst.extend_from_slice(&self.cq_offset.to_be_bytes());
        Ok(())
    }

    pub fn delete_range(topic: impl AsRef<str>, queue_id: i32) -> Result<(Vec<u8>, Vec<u8>), RocketMQError> {
        let topic = topic.as_ref();
        let start_key = Self::delete_range_bound(topic, queue_id, Self::CTRL_0)?;
        let end_key = Self::delete_range_bound(topic, queue_id, Self::CTRL_2)?;
        Ok((start_key, end_key))
    }

    fn delete_range_bound(topic: &str, queue_id: i32, boundary: u8) -> Result<Vec<u8>, RocketMQError> {
        let topic_len = i32::try_from(topic.len()).map_err(|_| RocketMQError::ConfigInvalidValue {
            key: "rocksdb.consume_queue.topic",
            value: topic.len().to_string(),
            reason: "topic length exceeds Java i32 key layout".to_string(),
        })?;

        let mut key = Vec::with_capacity(4 + 1 + topic.len() + 1 + 4 + 1);
        key.extend_from_slice(&topic_len.to_be_bytes());
        key.push(Self::CTRL_1);
        key.extend_from_slice(topic.as_bytes());
        key.push(Self::CTRL_1);
        key.extend_from_slice(&queue_id.to_be_bytes());
        key.push(boundary);
        Ok(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeQueueOffsetBoundary {
    Max,
    Min,
}

impl ConsumeQueueOffsetBoundary {
    fn marker(self) -> &'static [u8; 3] {
        match self {
            Self::Max => b"max",
            Self::Min => b"min",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeQueueOffsetKey {
    pub topic: String,
    pub queue_id: i32,
    pub boundary: ConsumeQueueOffsetBoundary,
}

impl ConsumeQueueOffsetKey {
    const CTRL_1: u8 = 1;

    pub fn max_physical_offset_checkpoint() -> Self {
        Self {
            topic: MAX_PHYSICAL_OFFSET_CHECKPOINT_TOPIC.to_string(),
            queue_id: 0,
            boundary: ConsumeQueueOffsetBoundary::Max,
        }
    }

    pub fn encoded_len(&self) -> usize {
        4 + 1 + self.topic.len() + 1 + 3 + 1 + 4
    }

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        let topic_len = i32::try_from(self.topic.len()).map_err(|_| RocketMQError::ConfigInvalidValue {
            key: "rocksdb.consume_queue_offset.topic",
            value: self.topic.len().to_string(),
            reason: "topic length exceeds Java i32 key layout".to_string(),
        })?;

        dst.reserve(self.encoded_len());
        dst.extend_from_slice(&topic_len.to_be_bytes());
        dst.push(Self::CTRL_1);
        dst.extend_from_slice(self.topic.as_bytes());
        dst.push(Self::CTRL_1);
        dst.extend_from_slice(self.boundary.marker());
        dst.push(Self::CTRL_1);
        dst.extend_from_slice(&self.queue_id.to_be_bytes());
        Ok(())
    }

    pub fn topic_boundary_prefix(
        topic: impl AsRef<str>,
        boundary: ConsumeQueueOffsetBoundary,
    ) -> Result<Vec<u8>, RocketMQError> {
        let topic = topic.as_ref();
        let topic_len = i32::try_from(topic.len()).map_err(|_| RocketMQError::ConfigInvalidValue {
            key: "rocksdb.consume_queue_offset.topic",
            value: topic.len().to_string(),
            reason: "topic length exceeds Java i32 key layout".to_string(),
        })?;

        let mut prefix = Vec::with_capacity(4 + 1 + topic.len() + 1 + 3 + 1);
        prefix.extend_from_slice(&topic_len.to_be_bytes());
        prefix.push(Self::CTRL_1);
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(Self::CTRL_1);
        prefix.extend_from_slice(boundary.marker());
        prefix.push(Self::CTRL_1);
        Ok(prefix)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRocksDbKey {
    pub store_time_hour: i64,
    pub topic: String,
    pub index_type: String,
    pub key: String,
    pub uniq_key: Option<String>,
    pub offset_py: i64,
}

impl IndexRocksDbKey {
    pub fn normal_key(
        topic: impl Into<String>,
        key: impl Into<String>,
        uniq_key: impl Into<String>,
        store_time: i64,
        offset_py: i64,
    ) -> Result<Self, RocketMQError> {
        Self::new(
            topic,
            MessageConst::INDEX_KEY_TYPE,
            key,
            Some(uniq_key.into()),
            store_time,
            offset_py,
        )
    }

    pub fn tag_key(
        topic: impl Into<String>,
        tag: impl Into<String>,
        uniq_key: impl Into<String>,
        store_time: i64,
        offset_py: i64,
    ) -> Result<Self, RocketMQError> {
        Self::new(
            topic,
            MessageConst::INDEX_TAG_TYPE,
            tag,
            Some(uniq_key.into()),
            store_time,
            offset_py,
        )
    }

    pub fn unique_key(
        topic: impl Into<String>,
        uniq_key: impl Into<String>,
        store_time: i64,
        offset_py: i64,
    ) -> Result<Self, RocketMQError> {
        Self::new(
            topic,
            MessageConst::INDEX_UNIQUE_TYPE,
            uniq_key,
            None,
            store_time,
            offset_py,
        )
    }

    pub fn query_prefix(
        topic: impl AsRef<str>,
        index_type: impl AsRef<str>,
        key: impl AsRef<str>,
        store_time_hour: i64,
    ) -> Result<Vec<u8>, RocketMQError> {
        let topic = topic.as_ref();
        let index_type = index_type.as_ref();
        let key = key.as_ref();
        validate_non_empty("rocksdb.index.topic", topic)?;
        validate_non_empty("rocksdb.index.type", index_type)?;
        validate_non_empty("rocksdb.index.key", key)?;
        if store_time_hour <= 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.index.store_time_hour",
                value: store_time_hour.to_string(),
                reason: "store time hour must be greater than zero".to_string(),
            });
        }

        let mut prefix = Vec::with_capacity(8 + 1 + topic.len() + 1 + index_type.len() + 1 + key.len() + 1);
        prefix.extend_from_slice(&store_time_hour.to_be_bytes());
        prefix.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        prefix.extend_from_slice(topic.as_bytes());
        prefix.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        prefix.extend_from_slice(index_type.as_bytes());
        prefix.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        prefix.extend_from_slice(key.as_bytes());
        prefix.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        Ok(prefix)
    }

    fn new(
        topic: impl Into<String>,
        index_type: impl Into<String>,
        key: impl Into<String>,
        uniq_key: Option<String>,
        store_time: i64,
        offset_py: i64,
    ) -> Result<Self, RocketMQError> {
        let topic = topic.into();
        let key = key.into();
        let index_type = index_type.into();
        validate_non_empty("rocksdb.index.topic", &topic)?;
        validate_non_empty("rocksdb.index.type", &index_type)?;
        validate_non_empty("rocksdb.index.key", &key)?;
        if let Some(uniq_key) = &uniq_key {
            validate_non_empty("rocksdb.index.uniq_key", uniq_key)?;
        }
        if store_time <= 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.index.store_time",
                value: store_time.to_string(),
                reason: "store time must be greater than zero".to_string(),
            });
        }
        if offset_py < 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.index.offset_py",
                value: offset_py.to_string(),
                reason: "physical offset must be non-negative".to_string(),
            });
        }
        let store_time_hour = deal_time_to_hour_stamps(store_time);
        if store_time_hour <= 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.index.store_time_hour",
                value: store_time_hour.to_string(),
                reason: "store time hour must be greater than zero".to_string(),
            });
        }
        Ok(Self {
            store_time_hour,
            topic,
            index_type,
            key,
            uniq_key,
            offset_py,
        })
    }

    pub fn encoded_len(&self) -> usize {
        8 + self.middle_key_len() + 8
    }

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        dst.reserve(self.encoded_len());
        dst.extend_from_slice(&self.store_time_hour.to_be_bytes());
        self.encode_middle_key(dst)?;
        dst.extend_from_slice(&self.offset_py.to_be_bytes());
        Ok(())
    }

    fn middle_key_len(&self) -> usize {
        let uniq_len = self.uniq_key.as_ref().map_or(0, |uniq_key| uniq_key.len() + 1);
        1 + self.topic.len() + 1 + self.index_type.len() + 1 + self.key.len() + 1 + uniq_len
    }

    fn encode_middle_key(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        validate_non_empty("rocksdb.index.topic", &self.topic)?;
        validate_non_empty("rocksdb.index.type", &self.index_type)?;
        validate_non_empty("rocksdb.index.key", &self.key)?;

        dst.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        dst.extend_from_slice(self.topic.as_bytes());
        dst.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        dst.extend_from_slice(self.index_type.as_bytes());
        dst.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        dst.extend_from_slice(self.key.as_bytes());
        dst.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        if let Some(uniq_key) = &self.uniq_key {
            validate_non_empty("rocksdb.index.uniq_key", uniq_key)?;
            dst.extend_from_slice(uniq_key.as_bytes());
            dst.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerRocksDbKey {
    pub delay_time: i64,
    pub uniq_key: String,
}

impl TimerRocksDbKey {
    pub fn encoded_len(&self) -> usize {
        8 + self.uniq_key.len()
    }

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        validate_non_empty("rocksdb.timer.uniq_key", &self.uniq_key)?;
        if self.delay_time <= 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.timer.delay_time",
                value: self.delay_time.to_string(),
                reason: "delay time must be greater than zero".to_string(),
            });
        }
        dst.reserve(self.encoded_len());
        dst.extend_from_slice(&self.delay_time.to_be_bytes());
        dst.extend_from_slice(self.uniq_key.as_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() <= 8 {
            return Err(codec_error(format!(
                "timer key must be longer than 8 bytes, got {}",
                src.len()
            )));
        }
        let mut delay_time = [0_u8; 8];
        delay_time.copy_from_slice(&src[..8]);
        let uniq_key = std::str::from_utf8(&src[8..])
            .map_err(|error| codec_error(format!("timer uniq key is not valid UTF-8: {error}")))?
            .to_string();
        Ok(Self {
            delay_time: i64::from_be_bytes(delay_time),
            uniq_key,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransRocksDbKey {
    pub offset_py: i64,
    pub topic: String,
    pub uniq_key: String,
}

impl TransRocksDbKey {
    pub fn encoded_len(&self) -> usize {
        8 + 1 + self.topic.len() + 1 + self.uniq_key.len()
    }

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        if self.offset_py < 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.trans.offset_py",
                value: self.offset_py.to_string(),
                reason: "physical offset must be non-negative".to_string(),
            });
        }
        validate_non_empty("rocksdb.trans.topic", &self.topic)?;
        validate_non_empty("rocksdb.trans.uniq_key", &self.uniq_key)?;

        dst.reserve(self.encoded_len());
        dst.extend_from_slice(&self.offset_py.to_be_bytes());
        dst.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        dst.extend_from_slice(self.topic.as_bytes());
        dst.extend_from_slice(INDEX_KEY_SPLIT.as_bytes());
        dst.extend_from_slice(self.uniq_key.as_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() <= 8 {
            return Err(codec_error(format!(
                "trans key must be longer than 8 bytes, got {}",
                src.len()
            )));
        }
        let mut offset_py = [0_u8; 8];
        offset_py.copy_from_slice(&src[..8]);
        let suffix = std::str::from_utf8(&src[8..])
            .map_err(|error| codec_error(format!("trans key suffix is not valid UTF-8: {error}")))?;
        let parts = suffix.split(INDEX_KEY_SPLIT).collect::<Vec<_>>();
        if parts.len() != 3 || !parts[0].is_empty() || parts[1].is_empty() || parts[2].is_empty() {
            return Err(codec_error("trans key suffix must match Java layout @topic@uniqKey"));
        }
        Ok(Self {
            offset_py: i64::from_be_bytes(offset_py),
            topic: parts[1].to_string(),
            uniq_key: parts[2].to_string(),
        })
    }
}

pub fn deal_time_to_hour_stamps(time_stamp: i64) -> i64 {
    if time_stamp <= 0 {
        return time_stamp;
    }
    (time_stamp / MILLIS_FOR_HOUR) * MILLIS_FOR_HOUR
}

fn validate_non_empty(key: &'static str, value: &str) -> Result<(), RocketMQError> {
    if value.is_empty() {
        return Err(RocketMQError::ConfigInvalidValue {
            key,
            value: value.to_string(),
            reason: "value must not be empty".to_string(),
        });
    }
    Ok(())
}
