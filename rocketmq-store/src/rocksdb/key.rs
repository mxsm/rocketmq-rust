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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeQueueKey {
    pub topic: String,
    pub queue_id: i32,
    pub cq_offset: i64,
}

impl ConsumeQueueKey {
    const CTRL_1: u8 = 1;

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
}
