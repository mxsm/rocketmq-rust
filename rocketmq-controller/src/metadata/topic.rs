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

use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use tracing::info;

use crate::config::ControllerConfig;
use crate::error::ControllerError;
use crate::error::Result;

/// Topic configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Topic name
    pub topic_name: String,

    /// Number of read queues
    pub read_queue_nums: u32,

    /// Number of write queues
    pub write_queue_nums: u32,

    /// Permission
    pub perm: u32,

    /// Topic filter type
    pub topic_filter_type: u32,

    /// Topic system flag
    pub topic_sys_flag: u32,

    /// Order
    pub order: bool,

    /// Attributes
    pub attributes: serde_json::Value,
}

/// Topic information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    /// Topic name
    pub name: String,

    /// Number of read queues
    pub read_queue_nums: u32,

    /// Number of write queues
    pub write_queue_nums: u32,

    /// Permission
    pub perm: u32,

    /// Topic system flag
    pub topic_sys_flag: u32,

    /// Broker addresses that have this topic
    pub brokers: Vec<String>,

    /// Additional metadata
    pub metadata: serde_json::Value,
}

/// Topic manager
pub struct TopicManager {
    /// Topics: topic_name -> TopicInfo
    topics: Arc<DashMap<String, TopicInfo>>,

    /// Configuration
    #[allow(dead_code)]
    config: Arc<ControllerConfig>,
}

impl TopicManager {
    /// Create a new topic manager
    pub fn new(config: Arc<ControllerConfig>) -> Self {
        Self {
            topics: Arc::new(DashMap::new()),
            config,
        }
    }

    /// Start the topic manager
    pub async fn start(&self) -> Result<()> {
        info!("Starting topic manager");
        Ok(())
    }

    /// Shutdown the topic manager
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down topic manager");
        self.topics.clear();
        Ok(())
    }

    /// Create a topic from config
    pub async fn create_topic(&self, config: TopicConfig) -> Result<()> {
        info!("Creating topic: {}", config.topic_name);

        if config.topic_name.is_empty() {
            return Err(ControllerError::InvalidRequest(
                "Topic name cannot be empty".to_string(),
            ));
        }

        // Convert config to info
        let info = TopicInfo {
            name: config.topic_name.clone(),
            read_queue_nums: config.read_queue_nums,
            write_queue_nums: config.write_queue_nums,
            perm: config.perm,
            topic_sys_flag: config.topic_sys_flag,
            brokers: Vec::new(),
            metadata: config.attributes,
        };

        self.topics.insert(info.name.clone(), info);
        Ok(())
    }

    /// Update a topic from config
    pub async fn update_topic(&self, config: TopicConfig) -> Result<()> {
        info!("Updating topic: {}", config.topic_name);

        if config.topic_name.is_empty() {
            return Err(ControllerError::InvalidRequest(
                "Topic name cannot be empty".to_string(),
            ));
        }

        // Check if topic exists
        if !self.topics.contains_key(&config.topic_name) {
            return Err(ControllerError::MetadataNotFound { key: config.topic_name });
        }

        // Convert config to info (preserving brokers list)
        let old_brokers = self
            .topics
            .get(&config.topic_name)
            .map(|v| v.brokers.clone())
            .unwrap_or_default();

        let info = TopicInfo {
            name: config.topic_name.clone(),
            read_queue_nums: config.read_queue_nums,
            write_queue_nums: config.write_queue_nums,
            perm: config.perm,
            topic_sys_flag: config.topic_sys_flag,
            brokers: old_brokers,
            metadata: config.attributes,
        };

        self.topics.insert(info.name.clone(), info);
        Ok(())
    }

    /// Create or update a topic
    pub async fn create_or_update_topic(&self, info: TopicInfo) -> Result<()> {
        info!("Creating/updating topic: {}", info.name);

        if info.name.is_empty() {
            return Err(ControllerError::InvalidRequest(
                "Topic name cannot be empty".to_string(),
            ));
        }

        self.topics.insert(info.name.clone(), info);
        Ok(())
    }

    /// Delete a topic
    pub async fn delete_topic(&self, topic_name: &str) -> Result<()> {
        info!("Deleting topic: {}", topic_name);

        self.topics
            .remove(topic_name)
            .ok_or_else(|| ControllerError::MetadataNotFound {
                key: topic_name.to_string(),
            })?;

        Ok(())
    }

    /// Get topic information
    pub async fn get_topic(&self, topic_name: &str) -> Result<TopicInfo> {
        self.topics
            .get(topic_name)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| ControllerError::MetadataNotFound {
                key: topic_name.to_string(),
            })
    }

    /// List all topics
    pub async fn list_topics(&self) -> Vec<TopicInfo> {
        self.topics.iter().map(|entry| entry.value().clone()).collect()
    }

    /// List topics by broker
    pub async fn list_topics_by_broker(&self, broker_name: &str) -> Vec<TopicInfo> {
        self.topics
            .iter()
            .filter(|entry| entry.value().brokers.contains(&broker_name.to_string()))
            .map(|entry| entry.value().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_topic_creation() {
        let config = Arc::new(ControllerConfig::test_config());

        let manager = TopicManager::new(config);

        let info = TopicInfo {
            name: "TestTopic".to_string(),
            read_queue_nums: 4,
            write_queue_nums: 4,
            perm: 6,
            topic_sys_flag: 0,
            brokers: vec!["broker-a".to_string()],
            metadata: serde_json::json!({}),
        };

        assert!(manager.create_or_update_topic(info.clone()).await.is_ok());
        assert!(manager.get_topic("TestTopic").await.is_ok());
    }
}
