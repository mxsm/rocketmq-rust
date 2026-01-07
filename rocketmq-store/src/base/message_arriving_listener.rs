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

use cheetah_string::CheetahString;

pub trait MessageArrivingListener {
    /// This method is called when a new message arrives.
    ///
    /// # Arguments
    ///
    /// * `topic` - A string that holds the topic of the message.
    /// * `queue_id` - An i32 that holds the id of the queue where the message is placed.
    /// * `logic_offset` - An i64 that represents the logical offset of the message in the queue.
    /// * `tags_code` - An i64 that represents the tags associated with the message.
    /// * `msg_store_time` - An i64 that represents the time when the message was stored.
    /// * `filter_bit_map` - A Vec<u8> that represents the filter bit map for the message.
    /// * `properties` - An Option containing a reference to a HashMap<String, String> that holds
    ///   the properties of the message.
    fn arriving(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        logic_offset: i64,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    );
}
