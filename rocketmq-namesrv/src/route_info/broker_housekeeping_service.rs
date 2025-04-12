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
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_rust::ArcMut;
use tracing::warn;

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
    fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
        warn!("warning: on_channel_connect is not implemented");
    }

    fn on_channel_close(&self, _remote_addr: &str, _channel: &Channel) {
        warn!("warning: on_channel_close is not implemented");
    }

    fn on_channel_exception(&self, _remote_addr: &str, _channel: &Channel) {
        warn!("warning: on_channel_exception is not implemented");
    }

    fn on_channel_idle(&self, _remote_addr: &str, _channel: &Channel) {
        warn!("warning: on_channel_idle is not implemented");
    }

    fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {
        warn!("warning: on_channel_active is not implemented");
    }
}
