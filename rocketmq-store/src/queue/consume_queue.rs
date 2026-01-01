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
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;

use crate::base::dispatch_request::DispatchRequest;
use crate::filter::MessageFilter;
use crate::queue::file_queue_life_cycle::FileQueueLifeCycle;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::CqUnit;

pub trait ConsumeQueueTrait: FileQueueLifeCycle {
    /// Get the topic name
    ///
    /// # Returns
    /// The topic this consume queue belongs to
    fn get_topic(&self) -> &CheetahString;

    /// Get queue ID
    ///
    /// # Returns
    /// The queue ID this consume queue belongs to
    fn get_queue_id(&self) -> i32;

    /// Get the units from the start offset
    ///
    /// # Parameters
    /// * `start_index` - The starting index
    ///
    /// # Returns
    /// An iterator over CqUnits
    fn iterate_from(&self, start_index: i64) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>>;

    /// Get the units from the start offset with count limit
    ///
    /// # Parameters
    /// * `start_index` - The starting index
    /// * `count` - Maximum number of units to iterate
    ///
    /// # Returns
    /// An iterator over CqUnits
    fn iterate_from_with_count(
        &self,
        start_index: i64,
        count: i32,
    ) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>>;

    /// Get a CqUnit at a specified index
    ///
    /// # Parameters
    /// * `index` - The index to retrieve
    ///
    /// # Returns
    /// The CqUnit at the specified index
    fn get(&self, index: i64) -> Option<CqUnit>;

    /// Get a CqUnit and its store time at a specified index
    ///
    /// # Parameters
    /// * `index` - The index to retrieve
    ///
    /// # Returns
    /// A tuple of the CqUnit and its store time
    fn get_cq_unit_and_store_time(&self, index: i64) -> Option<(CqUnit, i64)>;

    /// Get the earliest CqUnit and its store time
    ///
    /// # Returns
    /// A tuple of the earliest CqUnit and its store time
    fn get_earliest_unit_and_store_time(&self) -> Option<(CqUnit, i64)>;

    /// Get the earliest CqUnit
    ///
    /// # Returns
    /// The earliest CqUnit
    fn get_earliest_unit(&self) -> Option<CqUnit>;

    /// Get the latest CqUnit
    ///
    /// # Returns
    /// The latest CqUnit
    fn get_latest_unit(&self) -> Option<CqUnit>;

    /// Get the last commit log offset
    ///
    /// # Returns
    /// The last commit log offset
    fn get_last_offset(&self) -> i64;

    /// Get the minimum offset (index) in the queue
    ///
    /// # Returns
    /// The minimum offset (index) in the queue
    fn get_min_offset_in_queue(&self) -> i64;

    /// Get the maximum offset (index) in the queue
    ///
    /// # Returns
    /// The maximum offset (index) in the queue
    fn get_max_offset_in_queue(&self) -> i64;

    /// Get the total message count in the queue
    ///
    /// # Returns
    /// The total message count
    fn get_message_total_in_queue(&self) -> i64;

    /// Get the offset (index) of a message at or after the specified timestamp
    ///
    /// # Parameters
    /// * `timestamp` - The timestamp to search for
    ///
    /// # Returns
    /// The offset (index) of the message
    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64;

    /// Get the offset (index) of a message at or after the specified timestamp
    ///
    /// When there are multiple messages at the same timestamp, the boundary type
    /// determines which one to return.
    ///
    /// # Parameters
    /// * `timestamp` - The timestamp to search for
    /// * `boundary_type` - Whether to return the lower or upper boundary message
    ///
    /// # Returns
    /// The offset (index) of the message
    fn get_offset_in_queue_by_time_with_boundary(&self, timestamp: i64, boundary_type: BoundaryType) -> i64;

    /// Get the maximum physical offset in the commit log that has been dispatched to this queue
    ///
    /// This should be exclusive.
    ///
    /// # Returns
    /// The maximum physical offset pointing to the commit log
    fn get_max_physic_offset(&self) -> i64;

    /// Get the minimum logical offset in the queue
    ///
    /// The CQ files may not be exactly consistent with the commit log;
    /// there may be redundant data in the first CQ file.
    ///
    /// # Returns
    /// The minimum effective position of the CQ file
    fn get_min_logic_offset(&self) -> i64;

    /// Get the consume queue type
    ///
    /// # Returns
    /// The consume queue type
    fn get_cq_type(&self) -> CQType;

    /// Get the total size of CQ files on disk
    ///
    /// # Returns
    /// The total size in bytes
    fn get_total_size(&self) -> i64;

    /// Get the size of each unit in this consume queue
    ///
    /// # Returns
    /// The unit size in bytes
    fn get_unit_size(&self) -> i32;

    /// Correct the minimum offset by the minimum commit log offset
    ///
    /// # Parameters
    /// * `min_commit_log_offset` - The minimum commit log offset
    fn correct_min_offset(&self, min_commit_log_offset: i64);

    /// Process a dispatch request and update the consume queue
    ///
    /// # Parameters
    /// * `request` - The dispatch request
    fn put_message_position_info_wrapper(&mut self, request: &DispatchRequest);

    /// Assign a queue offset to a message
    ///
    /// # Parameters
    /// * `queue_offset_assigner` - The offset assigner
    /// * `msg` - The message to assign an offset to
    ///
    /// # Returns
    /// Result indicating success or failure
    fn assign_queue_offset(&self, queue_offset_assigner: &QueueOffsetOperator, msg: &mut MessageExtBrokerInner);

    /// Increase the queue offset for a message
    ///
    /// # Parameters
    /// * `queue_offset_assigner` - The offset assigner
    /// * `msg` - The message
    /// * `message_num` - The number of messages
    fn increase_queue_offset(
        &self,
        queue_offset_assigner: &QueueOffsetOperator,
        msg: &MessageExtBrokerInner,
        message_num: i16,
    );

    /// Estimate the number of messages matching a filter within a range
    ///
    /// # Parameters
    /// * `from` - Lower boundary (inclusive)
    /// * `to` - Upper boundary (inclusive)
    /// * `filter` - The message filter
    ///
    /// # Returns
    /// The estimated number of matching messages
    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64;
}
