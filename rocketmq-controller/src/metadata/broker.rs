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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config::ControllerConfig;
use crate::error::ControllerError;
use crate::error::Result;

/// Broker information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    /// Broker name
    pub name: String,

    /// Broker ID
    pub broker_id: u64,

    /// Cluster name
    pub cluster_name: String,

    /// Broker address
    pub addr: SocketAddr,

    /// Last heartbeat time
    pub last_heartbeat: SystemTime,

    /// Broker version
    pub version: String,

    /// Broker role (MASTER, SLAVE)
    pub role: BrokerRole,

    /// Additional metadata
    pub metadata: serde_json::Value,
}

/// Broker role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerRole {
    Master,
    Slave,
}

/// Broker manager
pub struct BrokerManager {
    /// Registered brokers: broker_name -> BrokerInfo
    brokers: Arc<DashMap<String, BrokerInfo>>,

    /// Configuration
    config: ArcMut<ControllerConfig>,

    /// Heartbeat timeout duration
    heartbeat_timeout: Duration,

    /// Background heartbeat checker task group
    task_group: Mutex<Option<TaskGroup>>,

    /// Scheduled heartbeat checker task group
    scheduled_tasks: Mutex<Option<ScheduledTaskGroup>>,
}

impl BrokerManager {
    /// Create a new broker manager
    pub fn new(config: ArcMut<ControllerConfig>) -> Self {
        Self {
            brokers: Arc::new(DashMap::new()),
            config,
            heartbeat_timeout: Duration::from_secs(30),
            task_group: Mutex::new(None),
            scheduled_tasks: Mutex::new(None),
        }
    }

    /// Start the broker manager
    pub async fn start(&self) -> Result<()> {
        info!("Starting broker manager");

        // Start heartbeat checker
        if self.task_group.lock().is_some() {
            warn!("Broker manager heartbeat checker already started");
            return Ok(());
        }

        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            ControllerError::Internal(format!(
                "No Tokio runtime for broker manager heartbeat checker: {error}"
            ))
        })?;
        let brokers = self.brokers.clone();
        let timeout = self.heartbeat_timeout;
        let task_group = TaskGroup::root("rocketmq-controller.metadata.broker", RuntimeHandle::new(runtime));
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
        scheduled_tasks
            .schedule_fixed_delay(
                ScheduledTaskConfig::fixed_delay(
                    "controller.metadata.broker.heartbeat-checker",
                    Duration::from_secs(5),
                ),
                move || {
                    let brokers = brokers.clone();
                    async move {
                        Self::check_heartbeats(&brokers, timeout);
                    }
                },
            )
            .map_err(|error| ControllerError::Internal(format!("Failed to spawn broker heartbeat checker: {error}")))?;
        *self.scheduled_tasks.lock() = Some(scheduled_tasks);
        *self.task_group.lock() = Some(task_group);

        Ok(())
    }

    /// Shutdown the broker manager
    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown_with_report().await?;
        Ok(())
    }

    pub(crate) async fn shutdown_with_report(&self) -> Result<Option<ShutdownReport>> {
        info!("Shutting down broker manager");
        self.scheduled_tasks.lock().take();
        let task_group = self.task_group.lock().take();
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(10)).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "Broker manager heartbeat checker shutdown report is unhealthy"
                );
            }
            self.brokers.clear();
            return Ok(Some(report));
        }
        self.brokers.clear();
        Ok(None)
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

    /// Register a broker
    pub async fn register(&self, info: BrokerInfo) -> Result<()> {
        info!("Registering broker: {} ({})", info.name, info.addr);

        // Validate broker info
        if info.name.is_empty() {
            return Err(ControllerError::InvalidRequest(
                "Broker name cannot be empty".to_string(),
            ));
        }

        // Update broker info
        self.brokers.insert(info.name.clone(), info);

        Ok(())
    }

    /// Unregister a broker
    pub async fn unregister(&self, broker_name: &str) -> Result<()> {
        info!("Unregistering broker: {}", broker_name);

        self.brokers
            .remove(broker_name)
            .ok_or_else(|| ControllerError::MetadataNotFound {
                key: broker_name.to_string(),
            })?;

        Ok(())
    }

    /// Update broker heartbeat
    pub async fn heartbeat(&self, broker_name: &str) -> Result<()> {
        debug!("Heartbeat from broker: {}", broker_name);

        let mut broker = self
            .brokers
            .get_mut(broker_name)
            .ok_or_else(|| ControllerError::MetadataNotFound {
                key: broker_name.to_string(),
            })?;

        broker.last_heartbeat = SystemTime::now();

        Ok(())
    }

    /// Get broker information
    pub async fn get_broker(&self, broker_name: &str) -> Result<BrokerInfo> {
        self.brokers
            .get(broker_name)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| ControllerError::MetadataNotFound {
                key: broker_name.to_string(),
            })
    }

    /// List all brokers
    pub async fn list_brokers(&self) -> Vec<BrokerInfo> {
        self.brokers.iter().map(|entry| entry.value().clone()).collect()
    }

    /// List brokers by cluster
    pub async fn list_brokers_by_cluster(&self, cluster_name: &str) -> Vec<BrokerInfo> {
        self.brokers
            .iter()
            .filter(|entry| entry.value().cluster_name == cluster_name)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Check heartbeats and remove stale brokers
    fn check_heartbeats(brokers: &DashMap<String, BrokerInfo>, timeout: Duration) {
        let now = SystemTime::now();
        let mut to_remove = Vec::new();

        for entry in brokers.iter() {
            let broker = entry.value();
            if let Ok(elapsed) = now.duration_since(broker.last_heartbeat) {
                if elapsed > timeout {
                    warn!(
                        "Broker {} heartbeat timeout, removing (last: {:?})",
                        broker.name, elapsed
                    );
                    to_remove.push(broker.name.clone());
                }
            }
        }

        for name in to_remove {
            brokers.remove(&name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_broker_registration() {
        let config = ArcMut::new(ControllerConfig::test_config());

        let manager = BrokerManager::new(config);

        let info = BrokerInfo {
            name: "broker-a".to_string(),
            broker_id: 0,
            cluster_name: "DefaultCluster".to_string(),
            addr: "127.0.0.1:10911".parse().unwrap(),
            last_heartbeat: SystemTime::now(),
            version: "5.0.0".to_string(),
            role: BrokerRole::Master,
            metadata: serde_json::json!({}),
        };

        assert!(manager.register(info.clone()).await.is_ok());
        assert!(manager.get_broker("broker-a").await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_checker_uses_scheduled_task_group_and_shutdowns_cleanly() {
        let config = ArcMut::new(ControllerConfig::test_config());
        let manager = BrokerManager::new(config);
        let info = BrokerInfo {
            name: "broker-expired".to_string(),
            broker_id: 0,
            cluster_name: "DefaultCluster".to_string(),
            addr: "127.0.0.1:10911".parse().unwrap(),
            last_heartbeat: SystemTime::now() - Duration::from_secs(60),
            version: "5.0.0".to_string(),
            role: BrokerRole::Master,
            metadata: serde_json::json!({}),
        };

        manager.register(info).await.expect("register expired broker");
        manager.start().await.expect("start broker manager");
        assert_eq!(manager.task_count(), 1);

        let mut snapshots = manager.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = manager.schedule_snapshot();
        }

        assert!(manager.list_brokers().await.is_empty());
        assert!(snapshots.iter().any(|snapshot| snapshot.runs > 0), "{snapshots:?}");

        let report = manager
            .shutdown_with_report()
            .await
            .expect("shutdown should succeed")
            .expect("shutdown should return report");
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(manager.task_count(), 0);
    }
}
