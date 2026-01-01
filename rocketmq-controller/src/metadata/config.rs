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

/// Configuration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigInfo {
    /// Config key
    pub key: String,

    /// Config value
    pub value: String,

    /// Config description
    pub description: Option<String>,
}

/// Configuration manager
pub struct ConfigManager {
    /// Configurations: key -> ConfigInfo
    configs: Arc<DashMap<String, ConfigInfo>>,

    /// Controller configuration
    #[allow(dead_code)]
    config: Arc<ControllerConfig>,
}

impl ConfigManager {
    /// Create a new config manager
    pub fn new(config: Arc<ControllerConfig>) -> Self {
        Self {
            configs: Arc::new(DashMap::new()),
            config,
        }
    }

    /// Start the config manager
    pub async fn start(&self) -> Result<()> {
        info!("Starting config manager");
        Ok(())
    }

    /// Shutdown the config manager
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down config manager");
        self.configs.clear();
        Ok(())
    }

    /// Set a configuration
    pub async fn set_config(&self, info: ConfigInfo) -> Result<()> {
        info!("Setting config: {} = {}", info.key, info.value);

        if info.key.is_empty() {
            return Err(ControllerError::InvalidRequest(
                "Config key cannot be empty".to_string(),
            ));
        }

        self.configs.insert(info.key.clone(), info);
        Ok(())
    }

    /// Get a configuration
    pub async fn get_config(&self, key: &str) -> Result<ConfigInfo> {
        self.configs
            .get(key)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| ControllerError::MetadataNotFound { key: key.to_string() })
    }

    /// Delete a configuration
    pub async fn delete_config(&self, key: &str) -> Result<()> {
        info!("Deleting config: {}", key);

        self.configs
            .remove(key)
            .ok_or_else(|| ControllerError::MetadataNotFound { key: key.to_string() })?;

        Ok(())
    }

    /// List all configurations
    pub async fn list_configs(&self) -> Vec<ConfigInfo> {
        self.configs.iter().map(|entry| entry.value().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_management() {
        let config = Arc::new(ControllerConfig::test_config());

        let manager = ConfigManager::new(config);

        let info = ConfigInfo {
            key: "test.key".to_string(),
            value: "test.value".to_string(),
            description: Some("Test configuration".to_string()),
        };

        assert!(manager.set_config(info.clone()).await.is_ok());
        assert!(manager.get_config("test.key").await.is_ok());
    }
}
