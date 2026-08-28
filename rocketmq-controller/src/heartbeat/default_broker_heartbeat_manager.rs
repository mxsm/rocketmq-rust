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

//! Default implementation of BrokerHeartbeatManager

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_transport::api::v1::Channel;
use tracing::info;
use tracing::warn;

#[cfg(test)]
use crate::config::ControllerConfig;
use crate::config::ControllerConfigReader;
use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::controller::broker_heartbeat_manager::BrokerSession;
use crate::controller::broker_heartbeat_manager::BrokerSessionHeartbeatManager;
use crate::controller::broker_heartbeat_manager::BrokerSessionId;
use crate::controller::broker_heartbeat_manager::DEFAULT_BROKER_CHANNEL_EXPIRED_TIME;
use crate::heartbeat::broker_identity_info::BrokerIdentityInfo;
use crate::heartbeat::broker_live_info::BrokerLiveInfo;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;
use crate::helper::broker_valid_predicate::BrokerValidPredicate;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct HeartbeatLifecycle {
    scan_task_group: Option<TaskGroup>,
    scan_scheduled_tasks: Option<ScheduledTaskGroup>,
}

/// Default implementation of BrokerHeartbeatManager
///
/// This implementation uses:
/// - `DashMap` for concurrent access to broker live information
/// - Tokio for async background scanning
/// - `RwLock<Vec<_>>` for the listener list
/// - `Mutex` for start/shutdown lifecycle resources
///
/// # Example
///
/// ```no_run,ignore
/// use rocketmq_controller::{ControllerConfig, ControllerConfigReader};
/// use rocketmq_controller::DefaultBrokerHeartbeatManager;
/// use rocketmq_controller::BrokerHeartbeatManager;
///
/// # async fn example() {
/// let config = ControllerConfigReader::new(ControllerConfig::test_config());
/// let mut manager = DefaultBrokerHeartbeatManager::new(config);
/// manager.initialize();
/// manager.start();
/// // ... use manager ...
/// manager.shutdown();
/// # }
/// ```
pub struct DefaultBrokerHeartbeatManager {
    /// Broker live information table
    /// Key: BrokerIdentityInfo, Value: BrokerLiveInfo
    broker_live_table: Arc<DashMap<BrokerIdentityInfo, BrokerLiveInfo>>,

    /// Registered lifecycle listeners. Scans observe the latest synchronized snapshot.
    lifecycle_listeners: Arc<RwLock<Vec<Arc<dyn BrokerLifecycleListener>>>>,

    /// Lifecycle-owned background scan resources.
    lifecycle: Mutex<HeartbeatLifecycle>,

    /// Scan interval in milliseconds
    scan_interval_ms: u64,

    parent_task_group: TaskGroup,
}

impl DefaultBrokerHeartbeatManager {
    /// Create a new DefaultBrokerHeartbeatManager
    ///
    /// # Arguments
    ///
    /// * `config` - Controller configuration
    pub fn new(config: ControllerConfigReader, parent_task_group: TaskGroup) -> Self {
        let scan_interval_ms = config.snapshot().scan_not_active_broker_interval.max(1);
        Self {
            broker_live_table: Arc::new(DashMap::with_capacity(256)),
            lifecycle_listeners: Arc::new(RwLock::new(Vec::new())),
            lifecycle: Mutex::new(HeartbeatLifecycle::default()),
            scan_interval_ms,
            parent_task_group,
        }
    }

    pub(crate) fn initialize_shared(&self) {
        info!("DefaultBrokerHeartbeatManager initialized");
    }

    pub(crate) fn register_broker_lifecycle_listener_shared(&self, listener: Arc<dyn BrokerLifecycleListener>) {
        self.lifecycle_listeners.write().push(listener);
    }

    pub(crate) async fn shutdown_gracefully_with_report(&self) -> ShutdownReport {
        self.shutdown_gracefully_until(ShutdownDeadline::after(SHUTDOWN_TIMEOUT))
            .await
    }

    pub(crate) async fn shutdown_gracefully_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        let task_group = {
            let mut lifecycle = self.lifecycle.lock();
            lifecycle.scan_scheduled_tasks.take();
            lifecycle.scan_task_group.take()
        };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown_until(deadline).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "DefaultBrokerHeartbeatManager shutdown report is unhealthy"
                );
            }
            info!("DefaultBrokerHeartbeatManager background scan stopped");
            report
        } else {
            ShutdownReport::new("rocketmq-controller.heartbeat", Duration::ZERO)
        }
    }

    pub(crate) fn scan_task_count(&self) -> usize {
        let lifecycle = self.lifecycle.lock();
        lifecycle
            .scan_task_group
            .as_ref()
            .map(TaskGroup::task_count)
            .unwrap_or_default()
    }

    pub(crate) fn scan_schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.lifecycle
            .lock()
            .scan_scheduled_tasks
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    /// Set the scan interval
    ///
    /// # Arguments
    ///
    /// * `interval_ms` - Scan interval in milliseconds
    pub fn with_scan_interval_ms(mut self, interval_ms: u64) -> Self {
        self.scan_interval_ms = interval_ms;
        self
    }

    /// Scan for inactive brokers and remove them
    ///
    /// This is called periodically by the background task.
    async fn scan_not_active_broker(
        broker_live_table: Arc<DashMap<BrokerIdentityInfo, BrokerLiveInfo>>,
        listeners: Arc<RwLock<Vec<Arc<dyn BrokerLifecycleListener>>>>,
    ) {
        let now_millis = current_millis();
        let mut candidates = Vec::new();

        // Collect brokers to remove
        for entry in broker_live_table.iter() {
            let broker_identity = entry.key();
            let live_info = entry.value();

            let last_update_timestamp = live_info.last_update_timestamp();
            let timeout_millis = live_info.heartbeat_timeout_millis();

            // A closed canonical session is terminal even when its last heartbeat
            // remains inside the configured timeout window.
            if live_info.session().is_closed() || now_millis > last_update_timestamp.saturating_add(timeout_millis) {
                candidates.push(broker_identity.clone());
            }
        }
        // Recheck while holding the map entry so a replacement heartbeat cannot
        // be removed after the scan observed an older session.
        let listeners = listeners.read().clone();
        for identity in candidates {
            let removed = broker_live_table.remove_if(&identity, |_, live_info| {
                live_info.session().is_closed()
                    || now_millis
                        > live_info
                            .last_update_timestamp()
                            .saturating_add(live_info.heartbeat_timeout_millis())
            });
            let Some((_, live_info)) = removed else {
                continue;
            };

            // Notify all listeners
            for listener in listeners.iter() {
                listener.on_broker_inactive(
                    Some(identity.cluster_name.as_str()),
                    identity.broker_name.as_str(),
                    Some(live_info.broker_id()),
                );
            }
        }
    }

    /// Notify listeners that a broker is inactive
    fn notify_broker_inactive(
        listeners: Vec<Arc<dyn BrokerLifecycleListener>>,
        cluster_name: &str,
        broker_name: &str,
        broker_id: i64,
    ) {
        for listener in listeners.iter() {
            listener.on_broker_inactive(Some(cluster_name), broker_name, Some(broker_id));
        }
    }

    pub(crate) fn start_shared(&self) {
        let mut lifecycle = self.lifecycle.lock();
        if lifecycle.scan_task_group.is_some() {
            warn!("DefaultBrokerHeartbeatManager background scan already started");
            return;
        }

        let broker_live_table = self.broker_live_table.clone();
        let listeners = self.lifecycle_listeners.clone();
        let scan_interval_ms = self.scan_interval_ms;
        let task_group = self.parent_task_group.clone();
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
        let mut config = ScheduledTaskConfig::fixed_delay(
            "controller.heartbeat.scan-not-active-broker",
            Duration::from_millis(scan_interval_ms),
        );
        config.initial_delay = Duration::from_millis(scan_interval_ms);

        if let Err(error) = scheduled_tasks.schedule_fixed_delay(config, move || {
            let broker_live_table = broker_live_table.clone();
            let listeners = listeners.clone();
            async move {
                Self::scan_not_active_broker(broker_live_table, listeners).await;
            }
        }) {
            warn!(?error, "failed to spawn DefaultBrokerHeartbeatManager scan task");
            return;
        }

        lifecycle.scan_scheduled_tasks = Some(scheduled_tasks);
        lifecycle.scan_task_group = Some(task_group);
        info!("DefaultBrokerHeartbeatManager background scan started");
    }

    pub(crate) fn shutdown_shared(&self) {
        let task_group = {
            let mut lifecycle = self.lifecycle.lock();
            lifecycle.scan_scheduled_tasks.take();
            lifecycle.scan_task_group.take()
        };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown_now();
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "DefaultBrokerHeartbeatManager immediate shutdown report is unhealthy"
                );
            }
            info!("DefaultBrokerHeartbeatManager background scan stopped");
        }
    }
}

impl DefaultBrokerHeartbeatManager {
    #[allow(clippy::too_many_arguments)]
    fn record_broker_heartbeat(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_addr: &str,
        broker_id: i64,
        timeout_millis: Option<u64>,
        session: BrokerSession,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        election_priority: Option<i32>,
    ) {
        let broker_identity = BrokerIdentityInfo::new(
            cluster_name.to_string(),
            broker_name.to_string(),
            Some(broker_id as u64),
        );
        let real_epoch = epoch.unwrap_or(-1);
        let real_max_offset = max_offset.unwrap_or(-1);
        let real_confirm_offset = confirm_offset.unwrap_or(-1);
        let real_timeout_millis = timeout_millis.unwrap_or(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        let real_election_priority = election_priority.or(Some(i32::MAX));
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if let Some(mut previous) = self.broker_live_table.get_mut(&broker_identity) {
            previous.set_last_update_timestamp(now_millis);
            previous.set_heartbeat_timeout_millis(real_timeout_millis);
            previous.set_election_priority(real_election_priority);
            previous.set_session(session);
            if real_epoch > previous.epoch()
                || (real_epoch == previous.epoch() && real_max_offset > previous.max_offset())
            {
                previous.set_epoch(real_epoch);
                previous.set_max_offset(real_max_offset);
                previous.set_confirm_offset(real_confirm_offset);
            }
            return;
        }

        let live_info = BrokerLiveInfo::new(
            broker_name.to_string(),
            broker_addr.to_string(),
            broker_id,
            now_millis,
            real_timeout_millis,
            session,
            real_epoch,
            real_max_offset,
            real_election_priority,
            Some(real_confirm_offset),
        );
        self.broker_live_table.insert(broker_identity.clone(), live_info);
        info!("new broker registered, {}, brokerId:{}", broker_identity, broker_id);
    }

    fn remove_broker_session(&self, session_id: BrokerSessionId) {
        let mut broker_identity_to_remove = None;
        for entry in self.broker_live_table.iter() {
            if entry.value().session().id() == session_id {
                let identity = entry.key().clone();
                let live_info = entry.value();
                info!(
                    "Session inactive, broker {}, addr:{}, id:{}",
                    live_info.broker_name(),
                    live_info.broker_addr(),
                    live_info.broker_id()
                );
                broker_identity_to_remove = Some(identity);
                break;
            }
        }

        if let Some(identity) = broker_identity_to_remove {
            if let Some((_, live_info)) = self
                .broker_live_table
                .remove_if(&identity, |_, live_info| live_info.session().id() == session_id)
            {
                let listeners = self.lifecycle_listeners.read().clone();
                Self::notify_broker_inactive(
                    listeners,
                    identity.cluster_name.as_str(),
                    live_info.broker_name(),
                    live_info.broker_id(),
                );
            }
        }
    }
}

impl BrokerHeartbeatManager for DefaultBrokerHeartbeatManager {
    fn initialize(&mut self) {
        self.initialize_shared();
    }

    fn on_broker_heartbeat(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_addr: &str,
        broker_id: i64,
        timeout_millis: Option<u64>,
        channel: Channel,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        election_priority: Option<i32>,
    ) {
        self.record_broker_heartbeat(
            cluster_name,
            broker_name,
            broker_addr,
            broker_id,
            timeout_millis,
            BrokerSession::from_legacy_channel(&channel),
            epoch,
            max_offset,
            confirm_offset,
            election_priority,
        );
    }

    fn start(&mut self) {
        self.start_shared();
    }

    fn shutdown(&mut self) {
        self.shutdown_shared();
    }

    fn register_broker_lifecycle_listener(&mut self, listener: Arc<dyn BrokerLifecycleListener>) {
        self.register_broker_lifecycle_listener_shared(listener);
    }

    fn on_broker_channel_close(&self, channel: &Channel) {
        self.remove_broker_session(BrokerSessionId::legacy(channel.channel_id_owned()));
    }

    fn get_broker_live_info(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> Option<BrokerLiveInfo> {
        let identity = BrokerIdentityInfo::new(
            cluster_name.to_string(),
            broker_name.to_string(),
            Some(broker_id as u64),
        );

        self.broker_live_table.get(&identity).map(|r| r.clone())
    }

    fn is_broker_active(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> bool {
        let identity = BrokerIdentityInfo::new(
            cluster_name.to_string(),
            broker_name.to_string(),
            Some(broker_id as u64),
        );

        if let Some(info) = self.broker_live_table.get(&identity) {
            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let last = info.last_update_timestamp();
            let timeout_millis = info.heartbeat_timeout_millis();

            return !info.session().is_closed() && last.saturating_add(timeout_millis) >= now_millis;
        }

        false
    }

    fn get_active_brokers_num(&self) -> HashMap<String, HashMap<String, u32>> {
        let mut result: HashMap<String, HashMap<String, u32>> = HashMap::new();

        for entry in self.broker_live_table.iter() {
            let identity = entry.key();

            // Check if broker is active
            if self.is_broker_active(
                &identity.cluster_name,
                &identity.broker_name,
                identity.broker_id.unwrap_or(0) as i64,
            ) {
                result
                    .entry(identity.cluster_name.to_string())
                    .or_default()
                    .entry(identity.broker_name.to_string())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }

        result
    }
}

impl BrokerSessionHeartbeatManager for DefaultBrokerHeartbeatManager {
    fn on_broker_session_heartbeat(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_addr: &str,
        broker_id: i64,
        timeout_millis: Option<u64>,
        session: BrokerSession,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        election_priority: Option<i32>,
    ) {
        self.record_broker_heartbeat(
            cluster_name,
            broker_name,
            broker_addr,
            broker_id,
            timeout_millis,
            session,
            epoch,
            max_offset,
            confirm_offset,
            election_priority,
        );
    }

    fn on_broker_session_close(&self, session_id: BrokerSessionId) {
        self.remove_broker_session(session_id);
    }
}

impl BrokerValidPredicate for DefaultBrokerHeartbeatManager {
    fn check(&self, cluster_name: &str, broker_name: &str, broker_id: Option<i64>) -> bool {
        self.is_broker_active(cluster_name, broker_name, broker_id.unwrap_or_default())
    }
}

impl Drop for DefaultBrokerHeartbeatManager {
    fn drop(&mut self) {
        self.shutdown_shared();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::OnceLock;

    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    use super::*;

    fn test_task_group(name: &'static str) -> TaskGroup {
        static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
        OWNER
            .get_or_init(|| {
                RuntimeOwner::new(RuntimeConfig::server_default("controller-heartbeat-tests"))
                    .expect("controller heartbeat test runtime should start")
            })
            .root_context()
            .component(name)
            .task_group()
            .clone()
    }

    fn test_session(id: u64) -> (BrokerSession, Arc<AtomicBool>) {
        let closed = Arc::new(AtomicBool::new(false));
        (BrokerSession::for_test(id, Arc::clone(&closed)), closed)
    }

    #[test]
    fn test_broker_heartbeat_registration() {
        let config = ControllerConfigReader::new(ControllerConfig::test_config());
        let manager = DefaultBrokerHeartbeatManager::new(config, test_task_group("registration"));
        let (session, _) = test_session(11);

        manager.on_broker_session_heartbeat(
            "cluster",
            "broker",
            "127.0.0.1:10911",
            1,
            Some(60_000),
            session,
            Some(1),
            Some(10),
            Some(9),
            Some(3),
        );

        let live = manager
            .get_broker_live_info("cluster", "broker", 1)
            .expect("heartbeat should register broker");
        assert_eq!(live.session().id(), BrokerSessionId::for_test(11));
        assert!(manager.is_broker_active("cluster", "broker", 1));
    }

    #[tokio::test]
    async fn replacement_session_survives_old_session_cleanup() {
        let config = ControllerConfigReader::new(ControllerConfig::test_config());
        let manager = DefaultBrokerHeartbeatManager::new(config, test_task_group("update"));
        let (first, first_closed) = test_session(21);
        manager.on_broker_session_heartbeat(
            "cluster",
            "broker",
            "127.0.0.1:10911",
            1,
            Some(60_000),
            first,
            Some(1),
            Some(10),
            Some(9),
            None,
        );
        let (replacement, _) = test_session(22);
        manager.on_broker_session_heartbeat(
            "cluster",
            "broker",
            "127.0.0.1:10911",
            1,
            Some(60_000),
            replacement,
            Some(2),
            Some(20),
            Some(19),
            None,
        );
        first_closed.store(true, Ordering::Release);
        manager.on_broker_session_close(BrokerSessionId::for_test(21));
        DefaultBrokerHeartbeatManager::scan_not_active_broker(
            Arc::clone(&manager.broker_live_table),
            Arc::clone(&manager.lifecycle_listeners),
        )
        .await;

        let live = manager
            .get_broker_live_info("cluster", "broker", 1)
            .expect("replacement heartbeat should remain registered");
        assert_eq!(live.session().id(), BrokerSessionId::for_test(22));
        assert_eq!(live.epoch(), 2);
        assert!(manager.is_broker_active("cluster", "broker", 1));
    }

    #[tokio::test]
    async fn closed_session_is_removed_without_waiting_for_heartbeat_timeout() {
        let config = ControllerConfigReader::new(ControllerConfig::test_config());
        let manager = DefaultBrokerHeartbeatManager::new(config, test_task_group("closed-session"));
        let (session, closed) = test_session(31);
        manager.on_broker_session_heartbeat(
            "cluster",
            "broker",
            "127.0.0.1:10911",
            1,
            Some(60_000),
            session,
            None,
            None,
            None,
            None,
        );
        assert_eq!(manager.get_active_brokers_num()["cluster"]["broker"], 1);

        closed.store(true, Ordering::Release);
        DefaultBrokerHeartbeatManager::scan_not_active_broker(
            Arc::clone(&manager.broker_live_table),
            Arc::clone(&manager.lifecycle_listeners),
        )
        .await;

        assert!(manager.get_broker_live_info("cluster", "broker", 1).is_none());
        assert!(manager.get_active_brokers_num().is_empty());
    }

    #[test]
    fn test_broker_identity_info_creation() {
        let identity = BrokerIdentityInfo::new("cluster1".to_string(), "broker1".to_string(), Some(1));
        assert_eq!(identity.cluster_name, "cluster1");
        assert_eq!(identity.broker_name, "broker1");
        assert_eq!(identity.broker_id, Some(1));
    }

    #[test]
    fn test_default_broker_heartbeat_manager_creation() {
        let config = ControllerConfigReader::new(ControllerConfig::test_config());
        let manager = DefaultBrokerHeartbeatManager::new(config.clone(), test_task_group("creation"));
        assert_eq!(
            manager.scan_interval_ms,
            config.snapshot().scan_not_active_broker_interval
        );
    }

    #[test]
    fn injected_runtime_allows_start_without_ambient_tokio_runtime() {
        let config = ControllerConfigReader::new(ControllerConfig::test_config());
        let manager = DefaultBrokerHeartbeatManager::new(config, test_task_group("non-ambient-start"));

        manager.start_shared();

        assert_eq!(manager.scan_task_count(), 1);
        manager.shutdown_shared();
        assert_eq!(manager.scan_task_count(), 0);
    }

    #[tokio::test]
    async fn sync_shutdown_clears_scan_task_group() {
        let config = ControllerConfigReader::new(ControllerConfig::test_config());
        let manager =
            DefaultBrokerHeartbeatManager::new(config, test_task_group("sync-shutdown")).with_scan_interval_ms(1);

        manager.start_shared();
        assert_eq!(manager.scan_task_count(), 1);

        manager.shutdown_shared();

        assert_eq!(manager.scan_task_count(), 0);
    }

    #[tokio::test]
    async fn concurrent_start_and_shutdown_own_one_scan_lifecycle() {
        let config = ControllerConfigReader::new(ControllerConfig::test_config());
        let manager = Arc::new(
            DefaultBrokerHeartbeatManager::new(config, test_task_group("concurrent-lifecycle"))
                .with_scan_interval_ms(1),
        );

        let first_manager = manager.clone();
        let first = tokio::spawn(async move { first_manager.start_shared() });
        let second_manager = manager.clone();
        let second = tokio::spawn(async move { second_manager.start_shared() });
        first.await.expect("first start task");
        second.await.expect("second start task");

        assert_eq!(manager.scan_task_count(), 1);

        let (first_report, second_report) = tokio::join!(
            manager.shutdown_gracefully_with_report(),
            manager.shutdown_gracefully_with_report()
        );
        assert!(first_report.is_healthy());
        assert!(second_report.is_healthy());
        assert_eq!(manager.scan_task_count(), 0);
    }
}
