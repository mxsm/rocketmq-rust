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

use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;

use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::PutMessageStatus;

/// The `RocketMQFlushManager` trait defines the operations for managing the flushing of messages to
/// disk in RocketMQ.
#[trait_variant::make(FlushManager: Send)]
pub trait RocketMQFlushManager {
    /// Starts the flush manager. This should be called when the broker starts up.
    fn start(&mut self);

    /// Shuts down the flush manager. This should be called when the broker is shutting down.
    fn shutdown(&mut self);

    /// Wakes up the flush manager to flush messages to disk. This should be called when new
    /// messages are available to be flushed.
    fn wake_up_flush(&self);

    /// Wakes up the flush manager to commit messages to disk. This should be called when messages
    /// have been successfully flushed and are ready to be committed.
    fn wake_up_commit(&self);

    /// Handles the asynchronous disk flushing of messages.
    /// This method is responsible for writing the messages to disk and updating the message store's
    /// write position.
    ///
    /// # Arguments
    ///
    /// * `result` - The result of appending the message to the message store.
    /// * `message_ext` - The message to be flushed to disk.
    ///
    /// # Returns
    ///
    /// * `PutMessageStatus` - The status of the put message operation.
    async fn handle_disk_flush(
        &mut self,
        result: &AppendMessageResult,
        message_ext: &MessageExtBrokerInner,
    ) -> PutMessageStatus;
}
