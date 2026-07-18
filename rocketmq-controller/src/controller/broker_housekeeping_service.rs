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

use std::sync::Arc;
use std::sync::Weak;

use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::ControllerManager;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::net::channel::Channel;

pub struct BrokerHousekeepingService {
    controller_manager: Option<Weak<ControllerManager>>,
}

impl Default for BrokerHousekeepingService {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerHousekeepingService {
    pub fn new() -> Self {
        BrokerHousekeepingService {
            controller_manager: None,
        }
    }

    /// Creates a service that observes the manager without keeping it alive.
    pub fn new_with_controller_manager(controller_manager: Arc<ControllerManager>) -> Self {
        BrokerHousekeepingService {
            controller_manager: Some(Arc::downgrade(&controller_manager)),
        }
    }

    /// Replaces the observed manager without taking ownership of its lifecycle.
    pub fn set_controller_manager(&mut self, controller_manager: Arc<ControllerManager>) {
        self.controller_manager = Some(Arc::downgrade(&controller_manager));
    }
}

impl ChannelEventListener for BrokerHousekeepingService {
    fn on_channel_connect(&self, __remote_addr: &str, _channel: &Channel) {
        // nothing to do
    }

    fn on_channel_close(&self, _remote_addr: &str, channel: &Channel) {
        if let Some(controller_manager) = self.controller_manager.as_ref().and_then(Weak::upgrade) {
            controller_manager.heartbeat_manager().on_broker_channel_close(channel);
        }
    }

    fn on_channel_exception(&self, _remote_addr: &str, channel: &Channel) {
        if let Some(controller_manager) = self.controller_manager.as_ref().and_then(Weak::upgrade) {
            controller_manager.heartbeat_manager().on_broker_channel_close(channel);
        }
    }

    fn on_channel_idle(&self, _remote_addr: &str, channel: &Channel) {
        if let Some(controller_manager) = self.controller_manager.as_ref().and_then(Weak::upgrade) {
            controller_manager.heartbeat_manager().on_broker_channel_close(channel);
        }
    }

    fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {
        //nothing to do
    }
}
