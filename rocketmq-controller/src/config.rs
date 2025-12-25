//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::error::ControllerError;
use crate::error::Result;
use crate::storage::StorageConfig;

/// Raft peer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPeer {
    /// Node ID
    pub id: u64,

    /// Peer address
    pub addr: SocketAddr,
}

/// Storage backend type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageBackendType {
    /// RocksDB storage
    RocksDB,

    /// File-based storage
    File,

    /// In-memory storage (for testing)
    Memory,
}

/// Controller configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerConfig {
    /// Node ID
    pub node_id: u64,

    /// Listen address
    pub listen_addr: SocketAddr,

    /// Raft peer list
    pub raft_peers: Vec<RaftPeer>,

    /// Storage path
    pub storage_path: PathBuf,

    /// Storage backend type
    pub storage_backend: StorageBackendType,

    /// Election timeout in milliseconds
    pub election_timeout_ms: u64,

    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,

    /// Enable elect unclean master (master not in sync state set)
    pub enable_elect_unclean_master: bool,
}

impl ControllerConfig {
    /// Create a new controller configuration
    pub fn new(node_id: u64, listen_addr: SocketAddr) -> Self {
        Self {
            node_id,
            listen_addr,
            raft_peers: Vec::new(),
            storage_path: PathBuf::from("/tmp/rocketmq-controller"),
            storage_backend: StorageBackendType::RocksDB,
            election_timeout_ms: 1000,
            heartbeat_interval_ms: 300,
            enable_elect_unclean_master: false,
        }
    }

    /// Set Raft peers
    pub fn with_raft_peers(mut self, peers: Vec<RaftPeer>) -> Self {
        self.raft_peers = peers;
        self
    }

    /// Set storage path
    pub fn with_storage_path(mut self, path: PathBuf) -> Self {
        self.storage_path = path;
        self
    }

    /// Set storage backend
    pub fn with_storage_backend(mut self, backend: StorageBackendType) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Set election timeout
    pub fn with_election_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.election_timeout_ms = timeout_ms;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval_ms(mut self, interval_ms: u64) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self
    }

    /// Set enable elect unclean master
    pub fn with_enable_elect_unclean_master(mut self, enable: bool) -> Self {
        self.enable_elect_unclean_master = enable;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.node_id == 0 {
            return Err(ControllerError::ConfigError(
                "Node ID cannot be 0".to_string(),
            ));
        }

        if self.election_timeout_ms == 0 {
            return Err(ControllerError::ConfigError(
                "Election timeout cannot be 0".to_string(),
            ));
        }

        if self.heartbeat_interval_ms == 0 {
            return Err(ControllerError::ConfigError(
                "Heartbeat interval cannot be 0".to_string(),
            ));
        }

        if self.heartbeat_interval_ms >= self.election_timeout_ms {
            return Err(ControllerError::ConfigError(
                "Heartbeat interval must be less than election timeout".to_string(),
            ));
        }

        Ok(())
    }

    /// Convert to storage configuration
    pub fn to_storage_config(&self) -> StorageConfig {
        match self.storage_backend {
            #[cfg(feature = "storage-rocksdb")]
            StorageBackendType::RocksDB => StorageConfig::RocksDB {
                path: self.storage_path.join("rocksdb"),
            },

            #[cfg(feature = "storage-file")]
            StorageBackendType::File => StorageConfig::File {
                path: self.storage_path.join("filedb"),
            },

            StorageBackendType::Memory => StorageConfig::Memory,

            #[allow(unreachable_patterns)]
            _ => StorageConfig::Memory,
        }
    }

    /// Create a test configuration (for testing only)
    #[cfg(test)]
    pub fn test_config() -> Self {
        Self {
            node_id: 1,
            listen_addr: "127.0.0.1:29876".parse().unwrap(),
            raft_peers: vec![RaftPeer {
                id: 1,
                addr: "127.0.0.1:29876".parse().unwrap(),
            }],
            storage_path: std::path::PathBuf::from("/tmp/controller_test"),
            storage_backend: StorageBackendType::Memory,
            election_timeout_ms: 1000,
            heartbeat_interval_ms: 300,
            enable_elect_unclean_master: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = ControllerConfig::new(1, "127.0.0.1:9876".parse().unwrap())
            .with_election_timeout_ms(2000)
            .with_heartbeat_interval_ms(600);

        assert_eq!(config.node_id, 1);
        assert_eq!(config.election_timeout_ms, 2000);
        assert_eq!(config.heartbeat_interval_ms, 600);
    }

    #[test]
    fn test_config_validation() {
        let config = ControllerConfig::new(1, "127.0.0.1:9876".parse().unwrap());
        assert!(config.validate().is_ok());

        let invalid_config = ControllerConfig::new(0, "127.0.0.1:9876".parse().unwrap());
        assert!(invalid_config.validate().is_err());
    }
}
