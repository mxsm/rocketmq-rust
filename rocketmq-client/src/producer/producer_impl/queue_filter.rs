/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use rocketmq_common::common::message::message_queue::MessageQueue;

/// A trait for filtering message queues.
///
/// This trait defines a method for filtering message queues based on custom criteria.
pub trait QueueFilter: Send + Sync + 'static {
    /// Filters a message queue.
    ///
    /// This method determines whether the specified message queue meets the filter criteria.
    ///
    /// # Arguments
    /// * `mq` - A reference to the `MessageQueue` to be filtered.
    ///
    /// # Returns
    /// A boolean value indicating whether the message queue meets the filter criteria.
    fn filter(&self, mq: &MessageQueue) -> bool;
}
