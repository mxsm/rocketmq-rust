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

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::producer_group_event::ProducerGroupEvent;

pub type ArcProducerChangeListener = std::sync::Arc<dyn ProducerChangeListener>;

/// A trait for handling producer group change events.
///
/// This trait is implemented by types that need to respond to changes
/// in producer groups, such as group registration, unregistration, or
/// client updates.
///
/// # Required Methods
/// - `handle`: Handles a specific producer group event.
pub trait ProducerChangeListener: Send + Sync {
    /// Handles a producer group event.
    ///
    /// # Parameters
    /// - `event`: The event that occurred, represented by a `ProducerGroupEvent`.
    /// - `group`: The name of the producer group as a string slice.
    /// - `client_channel_info`: Optional reference to `ClientChannelInfo` containing details about
    ///   the client channel associated with the event.
    fn handle(&self, event: ProducerGroupEvent, group: &str, client_channel_info: Option<&ClientChannelInfo>);
}
