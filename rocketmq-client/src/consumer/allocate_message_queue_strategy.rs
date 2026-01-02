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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;

/// Trait for allocating message queues to consumers in a consumer group.
/// This trait is implemented by different strategies for message queue allocation.
pub trait AllocateMessageQueueStrategy: Send + Sync {
    /// Allocates message queues to a consumer in a consumer group.
    ///
    /// # Arguments
    ///
    /// * `consumer_group` - The name of the consumer group.
    /// * `current_cid` - The ID of the current consumer.
    /// * `mq_all` - A slice of all available message queues.
    /// * `cid_all` - A slice of all consumer IDs in the consumer group.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of allocated message queues or an error.
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>>;

    /// Returns the name of the allocation strategy.
    ///
    /// # Returns
    ///
    /// A static string slice representing the name of the strategy.
    fn get_name(&self) -> &'static str;
}
