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

use std::fmt;

use rocketmq_common::common::message::message_ext::MessageExt;

#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    index_last_update_timestamp: u64,
    message_list: Vec<MessageExt>,
}

impl QueryResult {
    // Constructor equivalent
    #[inline]
    pub fn new(index_last_update_timestamp: u64, message_list: Vec<MessageExt>) -> Self {
        QueryResult {
            index_last_update_timestamp,
            message_list,
        }
    }

    // Getter methods equivalent
    #[inline]
    pub fn index_last_update_timestamp(&self) -> u64 {
        self.index_last_update_timestamp
    }

    #[inline]
    pub fn message_list(&self) -> &Vec<MessageExt> {
        &self.message_list
    }
}

// Implementing the Display trait for pretty printing, similar to Java's toString method
impl fmt::Display for QueryResult {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueryResult [index_last_update_timestamp={}, message_list={:?}]",
            self.index_last_update_timestamp, self.message_list
        )
    }
}
