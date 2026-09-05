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

use std::error::Error as StdError;
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

    /// Raft consensus errors with preserved source.
    #[error("Raft error: {message}")]
    RaftSource {
        /// The struct field value.
        message: String,
        #[source]
        /// The struct field value.
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Invalid request
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Invalid request with preserved decode or validation source.
    #[error("Invalid request: {message}")]
    InvalidRequestSource {
        /// The struct field value.
        message: String,
        #[source]
        /// The struct field value.
        source: Box<dyn StdError + Send + Sync>,
    },

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

    /// Serialization error with preserved source.
    #[error("Serialization error: {message}")]
    SerializationSource {
        /// The struct field value.
        message: String,
        #[source]
        /// The struct field value.
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Storage error with preserved source.
    #[error("Storage error: {message}")]
    StorageSource {
        /// The struct field value.
        message: String,
        #[source]
        /// The struct field value.
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Timeout error
    #[error("Operation timeout after {timeout_ms}ms")]
    /// The timeout value.
    Timeout {
        /// The timeout duration in milliseconds.
        timeout_ms: u64,
    },

    /// Controller runtime or task lifecycle error.
    #[error("Runtime error: {0}")]
    RuntimeError(String),

    /// Runtime operation failed with a preserved typed source.
    #[error("Runtime operation {operation} failed")]
    RuntimeSource {
        /// The struct field value.
        operation: &'static str,
        #[source]
        /// The struct field value.
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Shutdown error
    #[error("Controller is shutting down")]
    Shutdown,
}

impl ControllerError {
    #[inline]
    /// Creates the raft source value.
    pub fn raft_source(message: impl Into<String>, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::RaftSource {
            message: message.into(),
            source: Box::new(source),
        }
    }

    #[inline]
    /// Creates the invalid request source value.
    pub fn invalid_request_source(message: impl Into<String>, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::InvalidRequestSource {
            message: message.into(),
            source: Box::new(source),
        }
    }

    #[inline]
    /// Creates the serialization source value.
    pub fn serialization_source(message: impl Into<String>, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::SerializationSource {
            message: message.into(),
            source: Box::new(source),
        }
    }

    #[inline]
    /// Creates the storage source value.
    pub fn storage_source(message: impl Into<String>, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::StorageSource {
            message: message.into(),
            source: Box::new(source),
        }
    }

    #[inline]
    /// Creates the runtime error value.
    pub fn runtime_error(message: impl Into<String>) -> Self {
        Self::RuntimeError(message.into())
    }

    #[inline]
    /// Creates the runtime source value.
    pub fn runtime_source(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::RuntimeSource {
            operation,
            source: Box::new(source),
        }
    }
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

        let err = ControllerError::raft_source("raft append failed", io::Error::other("transport closed"));
        assert_eq!(err.to_string(), "Raft error: raft append failed");
        assert!(err.source().is_some());

        let err = ControllerError::InvalidRequest("bad request".to_string());
        assert_eq!(err.to_string(), "Invalid request: bad request");

        let err = ControllerError::invalid_request_source("decode request", io::Error::other("bad bytes"));
        assert_eq!(err.to_string(), "Invalid request: decode request");
        assert!(err.source().is_some());

        let err = ControllerError::NotInitialized("init first".to_string());
        assert_eq!(err.to_string(), "Not initialized: init first");

        let err = ControllerError::InitializationFailed;
        assert_eq!(err.to_string(), "Initialization failed");

        let err = ControllerError::ConfigError("invalid config".to_string());
        assert_eq!(err.to_string(), "Configuration error: invalid config");

        let err = ControllerError::SerializationError("serde error".to_string());
        assert_eq!(err.to_string(), "Serialization error: serde error");

        let err = ControllerError::serialization_source("encode response", io::Error::other("serde failure"));
        assert_eq!(err.to_string(), "Serialization error: encode response");
        assert!(err.source().is_some());

        let err = ControllerError::StorageError("disk full".to_string());
        assert_eq!(err.to_string(), "Storage error: disk full");

        let err = ControllerError::storage_source("write metadata", io::Error::other("disk full"));
        assert_eq!(err.to_string(), "Storage error: write metadata");
        assert!(err.source().is_some());

        let err = ControllerError::Timeout { timeout_ms: 5000 };
        assert_eq!(err.to_string(), "Operation timeout after 5000ms");

        let err = ControllerError::runtime_error("task group closed");
        assert_eq!(err.to_string(), "Runtime error: task group closed");

        let err = ControllerError::runtime_source("join controller task", io::Error::other("task failed"));
        assert_eq!(err.to_string(), "Runtime operation join controller task failed");
        assert!(err.source().is_some());

        let err = ControllerError::Shutdown;
        assert_eq!(err.to_string(), "Controller is shutting down");
    }
}
