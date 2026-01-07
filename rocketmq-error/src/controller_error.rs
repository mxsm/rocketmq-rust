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

//! Controller module error types
//!
//! This module provides error types specific to the RocketMQ controller subsystem,
//! which manages broker lifecycles, master elections, and cluster coordination.

use std::io;

use thiserror::Error;

/// Controller module error types
///
/// Errors that can occur during controller operations including:
/// - Raft consensus failures
/// - Leadership transitions
/// - Broker registration and metadata management
/// - Network and serialization issues
#[derive(Debug, Error)]
pub enum ControllerError {
    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Raft consensus errors
    #[error("Raft error: {0}")]
    Raft(String),

    /// Not the leader error
    #[error("Not leader, current leader is: {}", leader_id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".to_string()))]
    NotLeader { leader_id: Option<u64> },

    /// Metadata not found
    #[error("Metadata not found: {key}")]
    MetadataNotFound { key: String },

    /// Invalid request
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Broker registration error
    #[error("Broker registration failed: {0}")]
    BrokerRegistrationFailed(String),

    /// Not initialized error
    #[error("Not initialized: {0}")]
    NotInitialized(String),

    /// Initialization failed error
    #[error("Initialization failed")]
    InitializationFailed,

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Network error
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Timeout error
    #[error("Operation timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Shutdown error
    #[error("Controller is shutting down")]
    Shutdown,
}

/// Result type alias for Controller operations
pub type ControllerResult<T> = std::result::Result<T, ControllerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_controller_error() {
        let err = ControllerError::Io(io::Error::other("test"));
        assert_eq!(err.to_string(), "IO error: test");

        let err = ControllerError::Raft("raft error".to_string());
        assert_eq!(err.to_string(), "Raft error: raft error");

        let err = ControllerError::NotLeader { leader_id: Some(1) };
        assert_eq!(err.to_string(), "Not leader, current leader is: 1");

        let err = ControllerError::NotLeader { leader_id: None };
        assert_eq!(err.to_string(), "Not leader, current leader is: unknown");

        let err = ControllerError::MetadataNotFound {
            key: "broker-a".to_string(),
        };
        assert_eq!(err.to_string(), "Metadata not found: broker-a");

        let err = ControllerError::InvalidRequest("bad request".to_string());
        assert_eq!(err.to_string(), "Invalid request: bad request");

        let err = ControllerError::BrokerRegistrationFailed("failed".to_string());
        assert_eq!(err.to_string(), "Broker registration failed: failed");

        let err = ControllerError::NotInitialized("init first".to_string());
        assert_eq!(err.to_string(), "Not initialized: init first");

        let err = ControllerError::InitializationFailed;
        assert_eq!(err.to_string(), "Initialization failed");

        let err = ControllerError::ConfigError("invalid config".to_string());
        assert_eq!(err.to_string(), "Configuration error: invalid config");

        let err = ControllerError::SerializationError("serde error".to_string());
        assert_eq!(err.to_string(), "Serialization error: serde error");

        let err = ControllerError::StorageError("disk full".to_string());
        assert_eq!(err.to_string(), "Storage error: disk full");

        let err = ControllerError::NetworkError("disconnected".to_string());
        assert_eq!(err.to_string(), "Network error: disconnected");

        let err = ControllerError::Timeout { timeout_ms: 5000 };
        assert_eq!(err.to_string(), "Operation timeout after 5000ms");

        let err = ControllerError::Internal("panic".to_string());
        assert_eq!(err.to_string(), "Internal error: panic");

        let err = ControllerError::Shutdown;
        assert_eq!(err.to_string(), "Controller is shutting down");
    }
}
