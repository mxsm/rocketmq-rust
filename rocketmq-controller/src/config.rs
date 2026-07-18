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

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tokio::sync::Mutex;

// Re-export the shared ControllerConfig from rocketmq-common so downstream code in
// this crate and tests can continue to refer to `crate::config::ControllerConfig`.
// Re-export common RaftPeer and StorageBackendType (from the controller_config module)
// so examples/tests using `rocketmq_controller::config::RaftPeer` or
// `StorageBackendType` keep working.
pub use rocketmq_common::common::controller::controller_config::RaftPeer;
pub use rocketmq_common::common::controller::controller_config::StorageBackendType;
pub use rocketmq_common::common::controller::ControllerConfig;

/// Shared owner for immutable controller configuration snapshots.
///
/// Readers clone the current [`Arc`] without holding a lock while they inspect
/// the configuration. Updates are serialized, applied to a private clone, and
/// published only after every property has been validated.
#[derive(Clone)]
pub struct ControllerConfigReader {
    current: Arc<ArcSwap<ControllerConfig>>,
}

impl ControllerConfigReader {
    /// Creates a reader whose initial snapshot is `config`.
    pub fn new(config: ControllerConfig) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(config)),
        }
    }

    /// Returns one coherent immutable configuration snapshot.
    pub fn snapshot(&self) -> Arc<ControllerConfig> {
        self.current.load_full()
    }
}

impl From<ControllerConfig> for ControllerConfigReader {
    fn from(config: ControllerConfig) -> Self {
        Self::new(config)
    }
}

pub(crate) struct ControllerConfigHandle {
    reader: ControllerConfigReader,
    update_lock: Arc<Mutex<()>>,
}

impl ControllerConfigHandle {
    pub(crate) fn new(config: ControllerConfig) -> Self {
        Self {
            reader: ControllerConfigReader::new(config),
            update_lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn reader(&self) -> ControllerConfigReader {
        self.reader.clone()
    }

    pub(crate) fn snapshot(&self) -> Arc<ControllerConfig> {
        self.reader.snapshot()
    }

    /// Validates and atomically publishes a new configuration snapshot.
    ///
    /// # Errors
    ///
    /// Returns the configuration validation error without changing the active
    /// snapshot when any supplied property is invalid or unknown.
    pub(crate) async fn update(&self, properties: HashMap<String, String>) -> RocketMQResult<()> {
        let _update_guard = self.update_lock.lock().await;
        let mut next = (*self.snapshot()).clone();
        next.update(properties).await?;
        next.validate().map_err(|reason| RocketMQError::ConfigInvalidValue {
            key: "controllerConfig",
            value: "candidate".to_string(),
            reason,
        })?;
        self.reader.current.store(Arc::new(next));
        Ok(())
    }
}

// Controller node-specific configuration is now carried by the shared
// `rocketmq_common::common::controller::ControllerConfig` type. Use that type
// in the controller crate where node-local fields are required.

// NOTE: crate-local node tests should construct node-aware configs via
// `rocketmq_common::common::controller::ControllerConfig::new_node(...)` or
// `::test_config()` helpers.

#[cfg(test)]
mod tests {
    use tokio::sync::Barrier;

    use super::*;

    #[tokio::test]
    async fn update_publishes_a_new_snapshot_without_mutating_existing_readers() {
        let handle = ControllerConfigHandle::new(ControllerConfig::test_config());
        let previous = handle.snapshot();
        let mut properties = HashMap::new();
        properties.insert("nodeId".to_string(), "7".to_string());

        handle.update(properties).await.expect("valid update must publish");

        assert_ne!(previous.node_id, 7);
        assert_eq!(handle.snapshot().node_id, 7);
    }

    #[tokio::test]
    async fn rejected_update_keeps_the_previous_snapshot() {
        let handle = ControllerConfigHandle::new(ControllerConfig::test_config());
        let previous = handle.snapshot();
        let mut properties = HashMap::new();
        properties.insert("scanNotActiveBrokerInterval".to_string(), "invalid".to_string());

        assert!(handle.update(properties).await.is_err());

        let current = handle.snapshot();
        assert!(Arc::ptr_eq(&previous, &current));
        assert_eq!(
            current.scan_not_active_broker_interval,
            previous.scan_not_active_broker_interval
        );
    }

    #[tokio::test]
    async fn whole_config_validation_failure_is_not_published() {
        let handle = ControllerConfigHandle::new(ControllerConfig::test_config());
        let previous = handle.snapshot();
        let mut properties = HashMap::new();
        properties.insert("controllerThreadPoolNums".to_string(), "0".to_string());

        assert!(handle.update(properties).await.is_err());

        assert!(Arc::ptr_eq(&previous, &handle.snapshot()));
    }

    #[tokio::test]
    async fn concurrent_writers_build_on_the_latest_published_snapshot() {
        let handle = ControllerConfigHandle::new(ControllerConfig::test_config());
        let mut retry_properties = HashMap::new();
        retry_properties.insert("electMasterMaxRetryCount".to_string(), "9".to_string());
        let mut notification_properties = HashMap::new();
        notification_properties.insert("notifyBrokerRoleChanged".to_string(), "false".to_string());

        let (retry_result, notification_result) =
            tokio::join!(handle.update(retry_properties), handle.update(notification_properties));

        retry_result.expect("retry update must publish");
        notification_result.expect("notification update must publish");
        let current = handle.snapshot();
        assert_eq!(current.elect_master_max_retry_count, 9);
        assert!(!current.notify_broker_role_changed);
    }

    #[tokio::test]
    async fn concurrent_readers_observe_only_complete_snapshots() {
        let handle = Arc::new(ControllerConfigHandle::new(ControllerConfig::test_config()));
        let reader = handle.reader();
        let barrier = Arc::new(Barrier::new(9));
        let mut readers = Vec::new();
        for _ in 0..8 {
            let reader = reader.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..256 {
                    let snapshot = reader.snapshot();
                    let observed = (
                        snapshot.elect_master_max_retry_count,
                        snapshot.notify_broker_role_changed,
                    );
                    assert!(observed == (3, true) || observed == (9, false));
                    tokio::task::yield_now().await;
                }
            }));
        }

        let writer_barrier = barrier.clone();
        let writer_handle = handle.clone();
        let writer = tokio::spawn(async move {
            writer_barrier.wait().await;
            let mut properties = HashMap::new();
            properties.insert("electMasterMaxRetryCount".to_string(), "9".to_string());
            properties.insert("notifyBrokerRoleChanged".to_string(), "false".to_string());
            writer_handle
                .update(properties)
                .await
                .expect("valid update must publish");
        });

        writer.await.expect("writer task must complete");
        for reader in readers {
            reader.await.expect("reader task must complete");
        }
    }
}
