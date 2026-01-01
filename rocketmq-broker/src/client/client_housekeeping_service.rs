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

use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tokio::select;
use tokio::sync::Notify;

use crate::broker_runtime::BrokerRuntimeInner;

pub struct ClientHousekeepingService<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    shutdown: Arc<Notify>,
}

impl<MS: MessageStore> Clone for ClientHousekeepingService<MS> {
    fn clone(&self) -> Self {
        Self {
            broker_runtime_inner: self.broker_runtime_inner.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<MS> ClientHousekeepingService<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn start(&self) {
        let broker_runtime_inner = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(10_000));
            loop {
                select! {
                    _ = interval.tick() => {
                        broker_runtime_inner.scan_exception_channel();
                    }
                    _ = shutdown.notified() => {
                        break;
                    }
                }
            }
        });
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    fn scan_exception_channel(&self) {
        self.broker_runtime_inner.producer_manager().scan_not_active_channel();
        self.broker_runtime_inner.consumer_manager().scan_not_active_channel();
    }
}

impl<MS> ChannelEventListener for ClientHousekeepingService<MS>
where
    MS: MessageStore,
{
    fn on_channel_connect(&self, _remote_addr: &str, _channel: &Channel) {
        self.broker_runtime_inner
            .broker_stats_manager()
            .inc_channel_connect_num()
    }

    fn on_channel_close(&self, remote_addr: &str, channel: &Channel) {
        self.broker_runtime_inner
            .producer_manager()
            .do_channel_close_event(remote_addr, channel);
        self.broker_runtime_inner
            .consumer_manager()
            .do_channel_close_event(remote_addr, channel);
        self.broker_runtime_inner.broker_stats_manager().inc_channel_close_num()
    }

    fn on_channel_exception(&self, remote_addr: &str, channel: &Channel) {
        self.broker_runtime_inner
            .producer_manager()
            .do_channel_close_event(remote_addr, channel);
        self.broker_runtime_inner
            .consumer_manager()
            .do_channel_close_event(remote_addr, channel);
        self.broker_runtime_inner
            .broker_stats_manager()
            .inc_channel_exception_num()
    }

    fn on_channel_idle(&self, remote_addr: &str, channel: &Channel) {
        self.broker_runtime_inner
            .producer_manager()
            .do_channel_close_event(remote_addr, channel);
        self.broker_runtime_inner
            .consumer_manager()
            .do_channel_close_event(remote_addr, channel);
        self.broker_runtime_inner.broker_stats_manager().inc_channel_idle_num()
    }

    fn on_channel_active(&self, _remote_addr: &str, _channel: &Channel) {
        //nothing to do
    }
}
