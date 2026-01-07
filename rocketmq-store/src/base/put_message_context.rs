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

#[derive(Debug, Clone, Default)]
pub struct PutMessageContext {
    topic_queue_table_key: String,
    phy_pos: Vec<i64>,
    batch_size: i32,
}

impl PutMessageContext {
    #[inline]
    pub fn new(topic_queue_table_key: String) -> Self {
        PutMessageContext {
            topic_queue_table_key,
            phy_pos: Vec::new(),
            batch_size: 0,
        }
    }

    #[inline]
    pub fn get_topic_queue_table_key(&self) -> &str {
        &self.topic_queue_table_key
    }

    #[inline]
    pub fn get_phy_pos(&self) -> &[i64] {
        &self.phy_pos
    }

    #[inline]
    pub fn set_phy_pos(&mut self, phy_pos: Vec<i64>) {
        self.phy_pos = phy_pos;
    }

    #[inline]
    pub fn get_phy_pos_mut(&mut self) -> &mut [i64] {
        &mut self.phy_pos
    }

    #[inline]
    pub fn get_batch_size(&self) -> i32 {
        self.batch_size
    }

    #[inline]
    pub fn set_batch_size(&mut self, batch_size: i32) {
        self.batch_size = batch_size;
    }

    #[inline]
    pub fn set_topic_queue_table_key(&mut self, topic_queue_table_key: String) {
        self.topic_queue_table_key = topic_queue_table_key;
    }
}
