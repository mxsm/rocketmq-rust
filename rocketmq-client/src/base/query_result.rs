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

fn format_message_list(message_list: &[MessageExt]) -> String {
    let mut result = String::from("[");
    for (index, message) in message_list.iter().enumerate() {
        if index > 0 {
            result.push_str(", ");
        }
        result.push_str(&message.to_string());
    }
    result.push(']');
    result
}

// Implementing the Display trait for pretty printing, similar to Java's toString method
impl fmt::Display for QueryResult {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueryResult [indexLastUpdateTimestamp={}, messageList={}]",
            self.index_last_update_timestamp,
            format_message_list(&self.message_list)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_result_display_matches_java_to_string_shape_for_empty_list() {
        let query_result = QueryResult::new(123, Vec::new());

        assert_eq!(
            query_result.to_string(),
            "QueryResult [indexLastUpdateTimestamp=123, messageList=[]]"
        );
    }
}
