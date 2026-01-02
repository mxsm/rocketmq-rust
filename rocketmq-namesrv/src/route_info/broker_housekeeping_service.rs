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

use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_rust::ArcMut;

use crate::bootstrap::NameServerRuntimeInner;

pub struct BrokerHousekeepingService {
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
}

impl BrokerHousekeepingService {
    pub fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        Self {
            name_server_runtime_inner,
        }
    }
}

impl ChannelEventListener for BrokerHousekeepingService {
    #[inline]
    fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
        //nothing needs to be done
    }

    #[inline]
    fn on_channel_close(&self, _remote_addr: &str, channel: &Channel) {
        self.name_server_runtime_inner
            .route_info_manager()
            .on_channel_destroy(channel)
    }

    #[inline]
    fn on_channel_exception(&self, _remote_addr: &str, channel: &Channel) {
        self.name_server_runtime_inner
            .route_info_manager()
            .on_channel_destroy(channel)
    }

    #[inline]
    fn on_channel_idle(&self, _remote_addr: &str, channel: &Channel) {
        self.name_server_runtime_inner
            .route_info_manager()
            .on_channel_destroy(channel)
    }

    #[inline]
    fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {
        //nothing needs to be done
    }
}
