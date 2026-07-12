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

pub use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::*;

use rocketmq_rust::ArcMut;

use super::logic_queue_mapping_item::LogicQueueMappingItem;

#[deprecated(note = "use TopicQueueMappingDetail::put_mapping_info with an exclusive mutable reference")]
pub fn put_mapping_info(
    mut mapping_detail: ArcMut<TopicQueueMappingDetail>,
    global_id: i32,
    mapping_info: Vec<LogicQueueMappingItem>,
) {
    TopicQueueMappingDetail::put_mapping_info(&mut mapping_detail, global_id, mapping_info);
}
