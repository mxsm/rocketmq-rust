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

use crate::rocksdb::error::codec_error;

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
