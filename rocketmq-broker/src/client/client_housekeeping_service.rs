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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tokio::select;
use tokio::sync::Notify;
use tracing::debug;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

pub struct ClientHousekeepingService<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    shutdown: Arc<Notify>,
    shutdown_requested: Arc<AtomicBool>,
    task_group: Arc<Mutex<Option<TaskGroup>>>,
}

impl<MS: MessageStore> Clone for ClientHousekeepingService<MS> {
    fn clone(&self) -> Self {
        Self {
            broker_runtime_inner: self.broker_runtime_inner.clone(),
            shutdown: self.shutdown.clone(),
            shutdown_requested: self.shutdown_requested.clone(),
            task_group: self.task_group.clone(),
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
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            task_group: Arc::new(Mutex::new(None)),
        }
    }

    pub fn start(&self) {
        if self.shutdown_requested.load(Ordering::Acquire) {
            debug!("Broker client housekeeping service is already shutting down");
            return;
        }

        let Some(task_group) = self.task_group() else {
            return;
        };

        if task_group.task_count() > 0 {
            debug!("Broker client housekeeping service is already running");
            return;
        }

        let broker_runtime_inner = self.clone();
        let shutdown = self.shutdown.clone();
        let cancellation_token = task_group.cancellation_token();
        if let Err(error) = task_group.spawn_service("broker.client-housekeeping.scan", async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(10_000));
            loop {
                select! {
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                    _ = interval.tick() => {
                        broker_runtime_inner.scan_exception_channel();
                    }
                    _ = shutdown.notified() => {
                        break;
                    }
                }
            }
        }) {
            warn!(?error, "failed to spawn broker client housekeeping task");
        }
    }

    pub fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        if let Some(task_group) = self.task_group.lock().take() {
            let report = task_group.shutdown_now();
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "Broker client housekeeping task shutdown report is unhealthy"
                );
            }
        }
    }

    fn scan_exception_channel(&self) {
        self.broker_runtime_inner.producer_manager().scan_not_active_channel();
        self.broker_runtime_inner.consumer_manager().scan_not_active_channel();
    }

    fn task_group(&self) -> Option<TaskGroup> {
        let mut task_group = self.task_group.lock();
        if let Some(group) = task_group.as_ref() {
            if group.lifecycle_state() == TaskGroupLifecycleState::Open {
                return Some(group.clone());
            }
        }

        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(error) => {
                warn!(
                    ?error,
                    "failed to start broker client housekeeping outside Tokio runtime"
                );
                return None;
            }
        };
        let group = TaskGroup::root("rocketmq-broker.client-housekeeping", RuntimeHandle::new(handle));
        *task_group = Some(group.clone());
        Some(group)
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
