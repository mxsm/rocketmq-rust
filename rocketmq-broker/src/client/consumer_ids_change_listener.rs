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

use std::any::Any;

use crate::client::consumer_group_event::ConsumerGroupEvent;

/// Trait for listening to consumer ID changes.
///
/// This trait defines the functionality required to listen to changes in consumer IDs within a
/// consumer group. Implementors of this trait can react to consumer group events, handle them, and
/// perform necessary actions such as resource cleanup during shutdown.
pub trait ConsumerIdsChangeListener {
    /// Handles consumer group events.
    ///
    /// This method is called when a consumer group event occurs, allowing the implementor to react
    /// to changes in consumer IDs. It can be used to update internal state or perform actions
    /// based on the event.
    ///
    /// # Arguments
    /// * `event` - The consumer group event that occurred.
    /// * `group` - The name of the consumer group affected by the event.
    /// * `args` - Additional arguments or context provided with the event.
    fn handle(&self, event: ConsumerGroupEvent, group: &str, args: &[&dyn Any]);

    /// Performs cleanup actions before shutdown.
    ///
    /// This method should be implemented to perform any necessary cleanup actions before the
    /// listener is shutdown. It's a good place to release resources or perform other shutdown
    /// procedures.
    fn shutdown(&self);
}
