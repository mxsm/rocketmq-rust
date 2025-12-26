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

use std::io;

use thiserror::Error;

/// Controller module error types
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

impl From<serde_json::Error> for ControllerError {
    fn from(e: serde_json::Error) -> Self {
        ControllerError::SerializationError(e.to_string())
    }
}

impl From<bincode::Error> for ControllerError {
    fn from(e: bincode::Error) -> Self {
        ControllerError::SerializationError(e.to_string())
    }
}

impl From<raft::Error> for ControllerError {
    fn from(e: raft::Error) -> Self {
        ControllerError::Raft(format!("{:?}", e))
    }
}

/// Result type alias for Controller operations
pub type Result<T> = std::result::Result<T, ControllerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ControllerError::NotLeader { leader_id: Some(1) };
        assert!(err.to_string().contains("Not leader"));
    }

    #[test]
    fn test_error_conversion() {
        let io_err = io::Error::other("test");
        let controller_err: ControllerError = io_err.into();
        assert!(matches!(controller_err, ControllerError::Io(_)));
    }
}
