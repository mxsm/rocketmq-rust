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

use crate::net::channel::Channel;

/// Receives connection lifecycle notifications from the remoting event dispatcher.
///
/// The transport runtime runs callbacks on its injected, bounded blocking executor and applies a
/// private callback deadline. A callback that exceeds that deadline is not cancelled because a
/// synchronous closure cannot be forcefully stopped; the executor continues to track it until it
/// returns and it continues to occupy its permit.
///
/// The event dispatcher proceeds after a callback deadline expires. Consequently, callbacks
/// scheduled after a timed-out callback can overlap it and can complete in a different order than
/// their lifecycle events. Implementations must therefore be thread-safe and should return
/// promptly.
pub trait ChannelEventListener: Sync + Send {
    fn on_channel_connect(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_close(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_exception(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_idle(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_active(&self, remote_addr: &str, channel: &Channel);
}
