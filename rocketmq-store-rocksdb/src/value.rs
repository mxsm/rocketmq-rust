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

use rocketmq_error::RocketMQError;

use crate::error::codec_error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeQueueValue {
    pub commit_log_physical_offset: i64,
    pub body_size: i32,
    pub tag_hash_code: i64,
    pub msg_store_time: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeQueueOffsetValue {
    pub commit_log_offset: i64,
    pub consume_queue_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxPhysicalOffsetCheckpointValue {
    pub max_physical_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexRocksDbValue {
    pub store_time: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerRocksDbValue {
    pub size_py: i32,
    pub offset_py: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransRocksDbValue {
    pub check_times: i32,
    pub size_py: i32,
}

impl ConsumeQueueOffsetValue {
    pub const ENCODED_LEN: usize = 16;

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        dst.reserve(Self::ENCODED_LEN);
        dst.extend_from_slice(&self.commit_log_offset.to_be_bytes());
        dst.extend_from_slice(&self.consume_queue_offset.to_be_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() != Self::ENCODED_LEN {
            return Err(codec_error(format!(
                "consume queue offset value must be {} bytes, got {}",
                Self::ENCODED_LEN,
                src.len()
            )));
        }

        Ok(Self {
            commit_log_offset: read_i64(src, 0)?,
            consume_queue_offset: read_i64(src, 8)?,
        })
    }
}

impl MaxPhysicalOffsetCheckpointValue {
    pub const ENCODED_LEN: usize = 8;

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        dst.reserve(Self::ENCODED_LEN);
        dst.extend_from_slice(&self.max_physical_offset.to_be_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() != Self::ENCODED_LEN {
            return Err(codec_error(format!(
                "max physical offset checkpoint value must be {} bytes, got {}",
                Self::ENCODED_LEN,
                src.len()
            )));
        }

        Ok(Self {
            max_physical_offset: read_i64(src, 0)?,
        })
    }
}

impl ConsumeQueueValue {
    pub const ENCODED_LEN: usize = 28;

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        dst.reserve(Self::ENCODED_LEN);
        dst.extend_from_slice(&self.commit_log_physical_offset.to_be_bytes());
        dst.extend_from_slice(&self.body_size.to_be_bytes());
        dst.extend_from_slice(&self.tag_hash_code.to_be_bytes());
        dst.extend_from_slice(&self.msg_store_time.to_be_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() != Self::ENCODED_LEN {
            return Err(codec_error(format!(
                "consume queue value must be {} bytes, got {}",
                Self::ENCODED_LEN,
                src.len()
            )));
        }

        Ok(Self {
            commit_log_physical_offset: read_i64(src, 0)?,
            body_size: read_i32(src, 8)?,
            tag_hash_code: read_i64(src, 12)?,
            msg_store_time: read_i64(src, 20)?,
        })
    }
}

impl IndexRocksDbValue {
    pub const ENCODED_LEN: usize = 8;

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        if self.store_time <= 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.index.store_time",
                value: self.store_time.to_string(),
                reason: "store time must be greater than zero".to_string(),
            });
        }
        dst.reserve(Self::ENCODED_LEN);
        dst.extend_from_slice(&self.store_time.to_be_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() != Self::ENCODED_LEN {
            return Err(codec_error(format!(
                "index value must be {} bytes, got {}",
                Self::ENCODED_LEN,
                src.len()
            )));
        }
        Ok(Self {
            store_time: read_i64(src, 0)?,
        })
    }
}

impl TimerRocksDbValue {
    pub const ENCODED_LEN: usize = 12;

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        if self.size_py <= 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.timer.size_py",
                value: self.size_py.to_string(),
                reason: "message size must be greater than zero".to_string(),
            });
        }
        if self.offset_py < 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.timer.offset_py",
                value: self.offset_py.to_string(),
                reason: "physical offset must be non-negative".to_string(),
            });
        }
        dst.reserve(Self::ENCODED_LEN);
        dst.extend_from_slice(&self.size_py.to_be_bytes());
        dst.extend_from_slice(&self.offset_py.to_be_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() != Self::ENCODED_LEN {
            return Err(codec_error(format!(
                "timer value must be {} bytes, got {}",
                Self::ENCODED_LEN,
                src.len()
            )));
        }
        Ok(Self {
            size_py: read_i32(src, 0)?,
            offset_py: read_i64(src, 4)?,
        })
    }
}

impl TransRocksDbValue {
    pub const ENCODED_LEN: usize = 8;

    pub fn encode(&self, dst: &mut Vec<u8>) -> Result<(), RocketMQError> {
        if self.check_times < 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.trans.check_times",
                value: self.check_times.to_string(),
                reason: "check times must be non-negative".to_string(),
            });
        }
        if self.size_py <= 0 {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "rocksdb.trans.size_py",
                value: self.size_py.to_string(),
                reason: "message size must be greater than zero".to_string(),
            });
        }
        dst.reserve(Self::ENCODED_LEN);
        dst.extend_from_slice(&self.check_times.to_be_bytes());
        dst.extend_from_slice(&self.size_py.to_be_bytes());
        Ok(())
    }

    pub fn decode(src: &[u8]) -> Result<Self, RocketMQError> {
        if src.len() != Self::ENCODED_LEN {
            return Err(codec_error(format!(
                "trans value must be {} bytes, got {}",
                Self::ENCODED_LEN,
                src.len()
            )));
        }
        Ok(Self {
            check_times: read_i32(src, 0)?,
            size_py: read_i32(src, 4)?,
        })
    }
}

fn read_i64(src: &[u8], start: usize) -> Result<i64, RocketMQError> {
    let end = start + 8;
    if end > src.len() {
        return Err(codec_error("not enough bytes to decode i64"));
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&src[start..end]);
    Ok(i64::from_be_bytes(bytes))
}

fn read_i32(src: &[u8], start: usize) -> Result<i32, RocketMQError> {
    let end = start + 4;
    if end > src.len() {
        return Err(codec_error("not enough bytes to decode i32"));
    }
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&src[start..end]);
    Ok(i32::from_be_bytes(bytes))
}
