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

pub mod ack_msg;
pub mod batch_ack_msg;
pub mod pop_check_point;

/// A trait representing an acknowledgment message that can be converted to and from `Any`.
pub trait AckMessage: std::fmt::Display {
    fn ack_offset(&self) -> i64;
    fn set_ack_offset(&mut self, ack_offset: i64);

    fn start_offset(&self) -> i64;
    fn set_start_offset(&mut self, start_offset: i64);

    fn consumer_group(&self) -> &CheetahString;
    fn set_consumer_group(&mut self, consumer_group: CheetahString);

    fn topic(&self) -> &CheetahString;
    fn set_topic(&mut self, topic: CheetahString);

    fn queue_id(&self) -> i32;
    fn set_queue_id(&mut self, queue_id: i32);

    fn pop_time(&self) -> i64;
    fn set_pop_time(&mut self, pop_time: i64);

    fn broker_name(&self) -> &CheetahString;
    fn set_broker_name(&mut self, broker_name: CheetahString);

    /// Converts the acknowledgment message to a reference of type `Any`.
    ///
    /// # Returns
    ///
    /// A reference to the acknowledgment message as `Any`.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Converts the acknowledgment message to a mutable reference of type `Any`.
    ///
    /// # Returns
    ///
    /// A mutable reference to the acknowledgment message as `Any`.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}
