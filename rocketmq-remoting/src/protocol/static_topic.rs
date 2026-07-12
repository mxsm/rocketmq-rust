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

pub use rocketmq_protocol::protocol::static_topic::logic_queue_mapping_item;
pub use rocketmq_protocol::protocol::static_topic::topic_config_and_queue_mapping;
pub use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context;
pub mod topic_queue_mapping_detail;
pub use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_info;
pub use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_one;
pub use rocketmq_protocol::protocol::static_topic::topic_remapping_detail_wrapper;
pub mod topic_queue_mapping_utils;

#[allow(deprecated)]
pub use topic_queue_mapping_detail::put_mapping_info;
