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

mod broker;
mod config;
mod replica;
mod topic;

use std::sync::Arc;

pub use broker::BrokerInfo;
pub use broker::BrokerManager;
pub use broker::BrokerRole;
pub use config::ConfigInfo;
pub use config::ConfigManager;
pub use replica::BrokerReplicaInfo;
pub use replica::ReplicaRole;
pub use replica::ReplicasManager;
pub use replica::SyncStateSet;
pub use topic::TopicConfig;
pub use topic::TopicInfo;
pub use topic::TopicManager;
use tracing::info;

use crate::config::ControllerConfig;
use crate::error::Result;

/// Metadata store
///
/// This component manages all metadata for the controller:
/// - Broker registration and heartbeat
/// - Topic configuration
/// - Controller configuration
/// - Replica and ISR management
///
/// All metadata is replicated through Raft for consistency.
pub struct MetadataStore {
    /// Broker manager
    broker_manager: Arc<BrokerManager>,

    /// Topic manager
    topic_manager: Arc<TopicManager>,

    /// Config manager
    config_manager: Arc<ConfigManager>,

    /// Replicas manager
    replicas_manager: Arc<ReplicasManager>,
}

impl MetadataStore {
    /// Create a new metadata store
    pub async fn new(config: Arc<ControllerConfig>) -> Result<Self> {
        info!("Initializing metadata store");

        let broker_manager = Arc::new(BrokerManager::new(config.clone()));
        let topic_manager = Arc::new(TopicManager::new(config.clone()));
        let config_manager = Arc::new(ConfigManager::new(config.clone()));
        let replicas_manager = Arc::new(ReplicasManager::new(config));

        Ok(Self {
            broker_manager,
            topic_manager,
            config_manager,
            replicas_manager,
        })
    }

    /// Start the metadata store
    pub async fn start(&self) -> Result<()> {
        info!("Starting metadata store");
        self.broker_manager.start().await?;
        self.topic_manager.start().await?;
        self.config_manager.start().await?;
        self.replicas_manager.start().await?;
        Ok(())
    }

    /// Shutdown the metadata store
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down metadata store");
        self.broker_manager.shutdown().await?;
        self.topic_manager.shutdown().await?;
        self.config_manager.shutdown().await?;
        self.replicas_manager.shutdown().await?;
        Ok(())
    }

    /// Get the broker manager
    pub fn broker_manager(&self) -> &Arc<BrokerManager> {
        &self.broker_manager
    }

    /// Get the topic manager
    pub fn topic_manager(&self) -> &Arc<TopicManager> {
        &self.topic_manager
    }

    /// Get the config manager
    pub fn config_manager(&self) -> &Arc<ConfigManager> {
        &self.config_manager
    }

    /// Get the replicas manager
    pub fn replicas_manager(&self) -> &Arc<ReplicasManager> {
        &self.replicas_manager
    }
}

/*#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metadata_store() {
        // Placeholder test
        assert!(true);
    }
}*/
