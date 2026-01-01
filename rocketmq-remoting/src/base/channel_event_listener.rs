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

pub trait ChannelEventListener: Sync + Send {
    fn on_channel_connect(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_close(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_exception(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_idle(&self, remote_addr: &str, channel: &Channel);

    fn on_channel_active(&self, remote_addr: &str, channel: &Channel);
}
