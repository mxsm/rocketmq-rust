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
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_remoting::base::channel_event_listener::ChannelEventListener;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tokio::sync::Notify;
use tracing::debug;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ClientHousekeepingService<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    shutdown: Arc<Notify>,
    shutdown_requested: Arc<AtomicBool>,
    task_group: Arc<Mutex<Option<TaskGroup>>>,
    scheduled_tasks: Arc<Mutex<Option<ScheduledTaskGroup>>>,
}

impl<MS: MessageStore> Clone for ClientHousekeepingService<MS> {
    fn clone(&self) -> Self {
        Self {
            broker_runtime_inner: self.broker_runtime_inner.clone(),
            shutdown: self.shutdown.clone(),
            shutdown_requested: self.shutdown_requested.clone(),
            task_group: self.task_group.clone(),
            scheduled_tasks: self.scheduled_tasks.clone(),
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
            scheduled_tasks: Arc::new(Mutex::new(None)),
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

        if self.task_count() > 0 {
            debug!("Broker client housekeeping service is already running");
            return;
        }

        let broker_runtime_inner = self.clone();
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
        if let Err(error) = scheduled_tasks.schedule_fixed_rate_no_overlap(
            ScheduledTaskConfig::fixed_rate_no_overlap(
                "broker.client-housekeeping.scan",
                tokio::time::Duration::from_millis(10_000),
            ),
            move || {
                let broker_runtime_inner = broker_runtime_inner.clone();
                async move {
                    broker_runtime_inner.scan_exception_channel();
                }
            },
        ) {
            warn!(?error, "failed to spawn broker client housekeeping task");
            return;
        }
        *self.scheduled_tasks.lock() = Some(scheduled_tasks);
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_with_report().await;
    }

    pub async fn shutdown_with_report(&self) -> Option<ShutdownReport> {
        self.shutdown_requested.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        self.scheduled_tasks.lock().take();
        let task_group = { self.task_group.lock().take() };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(SHUTDOWN_TIMEOUT).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "Broker client housekeeping task shutdown report is unhealthy"
                );
            }
            return Some(report);
        }
        None
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

        let group = self.broker_runtime_inner.broker_task_group_or_current(
            "rocketmq-broker.client-housekeeping",
            "failed to start broker client housekeeping outside Tokio runtime",
        )?;
        *task_group = Some(group.clone());
        Some(group)
    }

    pub(crate) fn task_count(&self) -> usize {
        let root_count = self
            .task_group
            .lock()
            .as_ref()
            .map(TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = self
            .scheduled_tasks
            .lock()
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
    }

    pub(crate) fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks
            .lock()
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use crate::broker_runtime::BrokerRuntime;

    use super::*;

    #[tokio::test]
    async fn start_is_idempotent_and_shutdown_stops_background_task() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut broker_runtime = BrokerRuntime::new(broker_config, message_store_config);
        let service = ClientHousekeepingService::new(broker_runtime.inner_for_test().clone());

        service.start();
        service.start();
        assert_eq!(service.task_count(), 1);

        let report = service
            .shutdown_with_report()
            .await
            .expect("shutdown should return a report");

        assert!(service.shutdown_requested.load(Ordering::Acquire));
        assert!(service.task_group.lock().is_none());
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn service_context_parents_task_group() {
        let context = RuntimeContext::from_current("broker-client-housekeeping-context-test");
        let broker_service = context.service_context("broker-service");
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut broker_runtime =
            BrokerRuntime::new_with_service_context(broker_config, message_store_config, broker_service.clone());
        let service = ClientHousekeepingService::new(broker_runtime.inner_for_test().clone());

        service.start();

        let task_group = service
            .task_group
            .lock()
            .as_ref()
            .expect("client housekeeping task group should be installed")
            .clone();
        assert_eq!(task_group.parent_id(), Some(broker_service.task_group().id()));

        let report = service
            .shutdown_with_report()
            .await
            .expect("shutdown should return a report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let broker_report = broker_service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(broker_report.is_healthy(), "{}", broker_report.to_json());
    }
}
