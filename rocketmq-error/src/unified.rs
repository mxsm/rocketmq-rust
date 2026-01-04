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

//! Unified error system for RocketMQ Rust implementation
//!
//! This module provides a centralized, semantic, and performant error handling system
//! for all RocketMQ operations. All errors are categorized into logical groups for
//! better debuggability and maintainability.

mod network;
mod protocol;
mod rpc;
mod serialization;
mod tools;

use std::io;

// Re-export filter error
pub use crate::filter_error::FilterError;

pub use network::NetworkError;
pub use protocol::ProtocolError;
pub use rpc::RpcClientError;
pub use serialization::SerializationError;
use thiserror::Error;
pub use tools::ToolsError;

// Re-export auth error from the auth_error module
pub use crate::auth_error::AuthError;
// Re-export legacy error types for backward compatibility (will be deprecated)
#[allow(deprecated)]
pub use crate::client_error::*;
#[allow(deprecated)]
pub use crate::common_error::*;
// Re-export controller error from the controller_error module
pub use crate::controller_error::ControllerError;

/// Main error type for all RocketMQ operations
///
/// This enum provides a unified error system across all RocketMQ crates.
/// Each variant represents a logical category of errors with rich context information.
///
/// # Design Principles
/// - **Semantic**: Each error clearly expresses what went wrong
/// - **Performance**: Minimal heap allocations, use of &'static str where possible
/// - **Debuggability**: Rich context for production debugging
/// - **Ergonomics**: Automatic conversions via From trait
///
/// # Examples
///
/// ```rust
/// use rocketmq_error::RocketMQError;
/// use rocketmq_error::RocketMQResult;
///
/// fn send_message(addr: &str) -> RocketMQResult<()> {
///     // Create a network error
///     if addr.is_empty() {
///         return Err(RocketMQError::network_connection_failed(
///             "localhost:9876",
///             "empty address",
///         ));
///     }
///     Ok(())
/// }
///
/// fn authenticate_user(username: &str) -> RocketMQResult<()> {
///     // Create an authentication error
///     if username.is_empty() {
///         return Err(RocketMQError::user_not_found(""));
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug, Error)]
pub enum RocketMQError {
    // ============================================================================
    // Network Errors
    // ============================================================================
    /// Network operation errors (connection, timeout, send/receive failures)
    #[error(transparent)]
    Network(#[from] NetworkError),

    // ============================================================================
    // Serialization Errors
    // ============================================================================
    /// Serialization/deserialization errors (encoding, decoding, format validation)
    #[error(transparent)]
    Serialization(#[from] SerializationError),

    // ============================================================================
    // Protocol Errors
    // ============================================================================
    /// RocketMQ protocol errors (invalid commands, version mismatch, etc.)
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    // ============================================================================
    // RPC Client Errors
    // ============================================================================
    /// RPC client specific errors (broker lookup, request failures, etc.)
    #[error(transparent)]
    Rpc(#[from] RpcClientError),

    // ============================================================================
    // Authentication Errors
    // ============================================================================
    /// Authentication/authorization errors (credential validation, access control, etc.)
    #[error(transparent)]
    Authentication(#[from] AuthError),

    // ============================================================================
    // Controller Errors
    // ============================================================================
    /// Controller operation errors (Raft consensus, leader election, broker management, etc.)
    #[error(transparent)]
    Controller(#[from] ControllerError),

    // ============================================================================
    // Broker Errors
    // ============================================================================
    /// Broker not found
    #[error("Broker not found: {name}")]
    BrokerNotFound { name: String },

    /// Broker registration failed
    #[error("Broker registration failed for '{name}': {reason}")]
    BrokerRegistrationFailed { name: String, reason: String },

    /// Broker operation failed with error code
    #[error("Broker operation '{operation}' failed: code={code}, message={message}")]
    BrokerOperationFailed {
        operation: &'static str,
        code: i32,
        message: String,
        broker_addr: Option<String>,
    },

    /// Topic does not exist
    #[error("Topic '{topic}' does not exist")]
    TopicNotExist { topic: String },

    /// Queue does not exist
    #[error("Queue does not exist: topic='{topic}', queue_id={queue_id}")]
    QueueNotExist { topic: String, queue_id: i32 },

    /// Subscription group not found
    #[error("Subscription group '{group}' not found")]
    SubscriptionGroupNotExist { group: String },

    /// Queue ID out of range
    #[error("Queue {queue_id} out of range (0-{max}) for topic '{topic}'")]
    QueueIdOutOfRange { topic: String, queue_id: i32, max: i32 },

    /// Message body too large
    #[error("Message body length {actual} bytes exceeds limit {limit} bytes")]
    MessageTooLarge { actual: usize, limit: usize },

    /// Message validation failed
    #[error("Message validation failed: {reason}")]
    MessageValidationFailed { reason: String },

    /// Retry limit exceeded
    #[error("Retry limit {current}/{max} exceeded for group '{group}'")]
    RetryLimitExceeded { group: String, current: i32, max: i32 },

    /// Transaction message rejected
    #[error("Transaction message rejected by broker policy")]
    TransactionRejected,

    /// Broker permission denied
    #[error("Broker permission denied: {operation}")]
    BrokerPermissionDenied { operation: String },

    /// Not master broker
    #[error("Not master broker, master address: {master_address}")]
    NotMasterBroker { master_address: String },

    /// Message lookup failed
    #[error("Message lookup failed at offset {offset}")]
    MessageLookupFailed { offset: i64 },

    /// Topic sending forbidden
    #[error("Sending to topic '{topic}' is forbidden")]
    TopicSendingForbidden { topic: String },

    /// Async task failed
    #[error("Async task '{task}' failed: {context}")]
    BrokerAsyncTaskFailed {
        task: &'static str,
        context: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // ============================================================================
    // Request/Response Errors
    // ============================================================================
    /// Request body missing or invalid
    #[error("Request body {operation} failed: {reason}")]
    RequestBodyInvalid { operation: &'static str, reason: String },

    /// Request header missing or invalid
    #[error("Request header error: {0}")]
    RequestHeaderError(String),

    /// Response encoding/decoding failed
    #[error("Response {operation} failed: {reason}")]
    ResponseProcessFailed { operation: &'static str, reason: String },

    // ============================================================================
    // NameServer/Route Errors
    // ============================================================================
    /// Route information not found
    #[error("Route information not found for topic '{topic}'")]
    RouteNotFound { topic: String },

    /// Route data inconsistency detected
    #[error("Route data inconsistency detected for topic '{topic}': {reason}")]
    RouteInconsistent { topic: String, reason: String },

    /// Broker registration conflict
    #[error("Broker registration conflict for '{broker_name}': {reason}")]
    RouteRegistrationConflict { broker_name: String, reason: String },

    /// Route state version conflict
    #[error("Route state version conflict: expected={expected}, actual={actual}")]
    RouteVersionConflict { expected: u64, actual: u64 },

    /// Cluster not found
    #[error("Cluster '{cluster}' not found")]
    ClusterNotFound { cluster: String },

    // ============================================================================
    // Client Errors
    // ============================================================================
    /// Client not started
    #[error("Client is not started")]
    ClientNotStarted,

    /// Client already started
    #[error("Client is already started")]
    ClientAlreadyStarted,

    /// Client is shutting down
    #[error("Client is shutting down")]
    ClientShuttingDown,

    /// Invalid client state
    #[error("Invalid client state: expected {expected}, got {actual}")]
    ClientInvalidState { expected: &'static str, actual: String },

    /// Producer not available
    #[error("Producer is not available")]
    ProducerNotAvailable,

    /// Consumer not available
    #[error("Consumer is not available")]
    ConsumerNotAvailable,

    // ============================================================================
    // Tools/Admin Errors
    // ============================================================================
    /// Tools and admin operation errors
    #[error(transparent)]
    Tools(#[from] ToolsError),

    // ============================================================================
    // Filter Errors
    // ============================================================================
    /// Bloom filter and bit array operation errors
    #[error(transparent)]
    Filter(#[from] FilterError),

    // ============================================================================
    // Storage Errors
    // ============================================================================
    /// Storage read failed
    #[error("Storage read failed for '{path}': {reason}")]
    StorageReadFailed { path: String, reason: String },

    /// Storage write failed
    #[error("Storage write failed for '{path}': {reason}")]
    StorageWriteFailed { path: String, reason: String },

    /// Data corruption detected
    #[error("Corrupted data detected in '{path}'")]
    StorageCorrupted { path: String },

    /// Out of storage space
    #[error("Out of storage space: {path}")]
    StorageOutOfSpace { path: String },

    /// Storage lock failed
    #[error("Failed to acquire lock for '{path}'")]
    StorageLockFailed { path: String },

    // ============================================================================
    // Configuration Errors
    // ============================================================================
    /// Configuration parsing failed
    #[error("Configuration parse error for '{key}': {reason}")]
    ConfigParseFailed { key: &'static str, reason: String },

    /// Required configuration missing
    #[error("Required configuration '{key}' is missing")]
    ConfigMissing { key: &'static str },

    /// Invalid configuration value
    #[error("Invalid configuration for '{key}': value='{value}', reason={reason}")]
    ConfigInvalidValue {
        key: &'static str,
        value: String,
        reason: String,
    },

    // ============================================================================
    // Controller/Raft Errors
    // ============================================================================
    /// Not the Raft leader
    #[error("Not leader, current leader is: {}", leader_id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".to_string()))]
    ControllerNotLeader { leader_id: Option<u64> },

    /// Raft consensus error
    #[error("Raft consensus error: {reason}")]
    ControllerRaftError { reason: String },

    /// Consensus operation timeout
    #[error("Consensus operation '{operation}' timed out after {timeout_ms}ms")]
    ControllerConsensusTimeout { operation: &'static str, timeout_ms: u64 },

    /// Snapshot operation failed
    #[error("Snapshot operation failed: {reason}")]
    ControllerSnapshotFailed { reason: String },

    // ============================================================================
    // System Errors
    // ============================================================================
    /// IO error from std::io
    #[error("IO error: {0}")]
    IO(#[from] io::Error),

    /// Illegal argument
    #[error("Illegal argument: {0}")]
    IllegalArgument(String),

    /// Operation timeout
    #[error("Operation '{operation}' timed out after {timeout_ms}ms")]
    Timeout { operation: &'static str, timeout_ms: u64 },

    /// Internal error (should be rare)
    #[error("Internal error: {0}")]
    Internal(String),

    /// Service lifecycle error
    #[error("Service error: {0}")]
    Service(#[from] ServiceError),

    // ============================================================================
    // Version Errors
    // ============================================================================
    /// Invalid RocketMQ version ordinal value
    #[error("Invalid RocketMQ version ordinal: {0}")]
    InvalidVersionOrdinal(u32),

    // ============================================================================
    // Legacy Errors (Deprecated - for backward compatibility)
    // ============================================================================
    /// Legacy error - use specific error types instead
    #[deprecated(since = "0.7.0", note = "Use specific error types instead")]
    #[error("{0}")]
    Legacy(String),

    #[error("Not initialized: {0}")]
    NotInitialized(String),

    #[error("Message is missing required property: {property}")]
    MissingRequiredMessageProperty { property: &'static str },
}

// ============================================================================
// Convenience Constructors
// ============================================================================

impl RocketMQError {
    /// Create a network connection failed error
    #[inline]
    pub fn network_connection_failed(addr: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Network(NetworkError::connection_failed(addr, reason))
    }

    /// Create a broker operation failed error
    #[inline]
    pub fn broker_operation_failed(operation: &'static str, code: i32, message: impl Into<String>) -> Self {
        Self::BrokerOperationFailed {
            operation,
            code,
            message: message.into(),
            broker_addr: None,
        }
    }

    /// Create a storage read failed error
    #[inline]
    pub fn storage_read_failed(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::StorageReadFailed {
            path: path.into(),
            reason: reason.into(),
        }
    }

    /// Create a storage write failed error
    #[inline]
    pub fn storage_write_failed(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::StorageWriteFailed {
            path: path.into(),
            reason: reason.into(),
        }
    }

    /// Create an illegal argument error
    #[inline]
    pub fn illegal_argument(message: impl Into<String>) -> Self {
        Self::IllegalArgument(message.into())
    }

    /// Create a route not found error
    #[inline]
    pub fn route_not_found(topic: impl Into<String>) -> Self {
        Self::RouteNotFound { topic: topic.into() }
    }

    /// Create a route registration conflict error
    #[inline]
    pub fn route_registration_conflict(broker_name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::RouteRegistrationConflict {
            broker_name: broker_name.into(),
            reason: reason.into(),
        }
    }

    /// Create a cluster not found error
    #[inline]
    pub fn cluster_not_found(cluster: impl Into<String>) -> Self {
        Self::ClusterNotFound {
            cluster: cluster.into(),
        }
    }

    /// Create a request body invalid error
    #[inline]
    pub fn request_body_invalid(operation: &'static str, reason: impl Into<String>) -> Self {
        Self::RequestBodyInvalid {
            operation,
            reason: reason.into(),
        }
    }

    /// Create a request header error
    #[inline]
    pub fn request_header_error(message: impl Into<String>) -> Self {
        Self::RequestHeaderError(message.into())
    }

    /// Create a response process failed error
    #[inline]
    pub fn response_process_failed(operation: &'static str, reason: impl Into<String>) -> Self {
        Self::ResponseProcessFailed {
            operation,
            reason: reason.into(),
        }
    }

    /// Add broker address context to broker operation error
    pub fn with_broker_addr(self, addr: impl Into<String>) -> Self {
        match self {
            Self::BrokerOperationFailed {
                operation,
                code,
                message,
                broker_addr: _,
            } => Self::BrokerOperationFailed {
                operation,
                code,
                message,
                broker_addr: Some(addr.into()),
            },
            other => other,
        }
    }

    /// Create a validation error
    #[inline]
    pub fn validation_error(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Tools(ToolsError::validation_error(field, reason))
    }

    /// Create a topic not found error (alias for TopicNotExist)
    #[inline]
    pub fn topic_not_found(topic: impl Into<String>) -> Self {
        Self::Tools(ToolsError::topic_not_found(topic))
    }

    /// Create a topic already exists error
    #[inline]
    pub fn topic_already_exists(topic: impl Into<String>) -> Self {
        Self::Tools(ToolsError::topic_already_exists(topic))
    }

    /// Create a nameserver unreachable error
    #[inline]
    pub fn nameserver_unreachable(addr: impl Into<String>) -> Self {
        Self::Tools(ToolsError::nameserver_unreachable(addr))
    }

    /// Create a nameserver config invalid error
    #[inline]
    pub fn nameserver_config_invalid(reason: impl Into<String>) -> Self {
        Self::Tools(ToolsError::nameserver_config_invalid(reason))
    }

    /// Create a not initialized error
    #[inline]
    pub fn not_initialized(reason: impl Into<String>) -> Self {
        Self::NotInitialized(reason.into())
    }

    // ============================================================================
    // Authentication Error Constructors
    // ============================================================================

    /// Create an authentication failed error
    #[inline]
    pub fn authentication_failed(reason: impl Into<String>) -> Self {
        Self::Authentication(AuthError::AuthenticationFailed(reason.into()))
    }

    /// Create an invalid credential error
    #[inline]
    pub fn invalid_credential(reason: impl Into<String>) -> Self {
        Self::Authentication(AuthError::InvalidCredential(reason.into()))
    }

    /// Create a user not found error
    #[inline]
    pub fn user_not_found(username: impl Into<String>) -> Self {
        Self::Authentication(AuthError::UserNotFound(username.into()))
    }

    /// Create an invalid signature error
    #[inline]
    pub fn invalid_signature(reason: impl Into<String>) -> Self {
        Self::Authentication(AuthError::InvalidSignature(reason.into()))
    }

    // ============================================================================
    // Controller Error Constructors
    // ============================================================================

    /// Create a controller not leader error
    #[inline]
    pub fn controller_not_leader(leader_id: Option<u64>) -> Self {
        Self::Controller(ControllerError::NotLeader { leader_id })
    }

    /// Create a controller Raft error
    #[inline]
    pub fn controller_raft_error(reason: impl Into<String>) -> Self {
        Self::Controller(ControllerError::Raft(reason.into()))
    }

    /// Create a controller metadata not found error
    #[inline]
    pub fn controller_metadata_not_found(key: impl Into<String>) -> Self {
        Self::Controller(ControllerError::MetadataNotFound { key: key.into() })
    }

    /// Create a controller invalid request error
    #[inline]
    pub fn controller_invalid_request(reason: impl Into<String>) -> Self {
        Self::Controller(ControllerError::InvalidRequest(reason.into()))
    }

    /// Create a controller timeout error
    #[inline]
    pub fn controller_timeout(timeout_ms: u64) -> Self {
        Self::Controller(ControllerError::Timeout { timeout_ms })
    }

    /// Create a controller shutdown error
    #[inline]
    pub fn controller_shutdown() -> Self {
        Self::Controller(ControllerError::Shutdown)
    }

    // ============================================================================
    // Filter Error Constructors
    // ============================================================================

    /// Create an empty bytes error
    #[inline]
    pub fn filter_empty_bytes() -> Self {
        Self::Filter(FilterError::empty_bytes())
    }

    /// Create an invalid bit length error
    #[inline]
    pub fn filter_invalid_bit_length() -> Self {
        Self::Filter(FilterError::invalid_bit_length())
    }

    /// Create a bit length too small error
    #[inline]
    pub fn filter_bit_length_too_small() -> Self {
        Self::Filter(FilterError::bit_length_too_small())
    }

    /// Create a bit position out of bounds error
    #[inline]
    pub fn filter_bit_position_out_of_bounds(pos: usize, max: usize) -> Self {
        Self::Filter(FilterError::bit_position_out_of_bounds(pos, max))
    }

    /// Create a byte position out of bounds error
    #[inline]
    pub fn filter_byte_position_out_of_bounds(pos: usize, max: usize) -> Self {
        Self::Filter(FilterError::byte_position_out_of_bounds(pos, max))
    }

    /// Create an uninitialized error
    #[inline]
    pub fn filter_uninitialized() -> Self {
        Self::Filter(FilterError::uninitialized())
    }
}

// ============================================================================
// Error Conversion Implementations
// ============================================================================

impl From<std::str::Utf8Error> for RocketMQError {
    #[inline]
    fn from(e: std::str::Utf8Error) -> Self {
        Self::Serialization(SerializationError::from(e))
    }
}

#[cfg(feature = "with_serde")]
impl From<serde_json::Error> for RocketMQError {
    #[inline]
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(SerializationError::from(e))
    }
}

#[cfg(feature = "with_config")]
impl From<config::ConfigError> for RocketMQError {
    fn from(e: config::ConfigError) -> Self {
        Self::ConfigParseFailed {
            key: "unknown",
            reason: e.to_string(),
        }
    }
}

// ============================================================================
// Service Error (moved from ServiceError)
// ============================================================================

/// Service lifecycle errors
#[derive(Debug, Error)]
pub enum ServiceError {
    /// Service is already running
    #[error("Service is already running")]
    AlreadyRunning,

    /// Service is not running
    #[error("Service is not running")]
    NotRunning,

    /// Service startup failed
    #[error("Service startup failed: {0}")]
    StartupFailed(String),

    /// Service shutdown failed
    #[error("Service shutdown failed: {0}")]
    ShutdownFailed(String),

    /// Service operation timeout
    #[error("Service operation timeout")]
    Timeout,

    /// Service interrupted
    #[error("Service interrupted")]
    Interrupted,
}

// ============================================================================
// Type Aliases
// ============================================================================

/// Result type alias for RocketMQ operations
///
/// This is the standard result type used across all RocketMQ crates.
///
/// # Examples
///
/// ```rust
/// use rocketmq_error::RocketMQResult;
///
/// fn send_message() -> RocketMQResult<()> {
///     // ... operation
///     Ok(())
/// }
/// ```
pub type RocketMQResult<T> = std::result::Result<T, RocketMQError>;

/// Alias for anyhow::Result for internal use
///
/// Use this for internal error handling where you don't need
/// to expose specific error types to the public API.
pub type Result<T> = anyhow::Result<T>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = RocketMQError::network_connection_failed("127.0.0.1:9876", "timeout");
        assert!(err.to_string().contains("Connection failed"));
    }

    #[test]
    fn test_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let rmq_err: RocketMQError = io_err.into();
        assert!(matches!(rmq_err, RocketMQError::IO(_)));
    }

    #[test]
    fn test_broker_operation_with_addr() {
        let err =
            RocketMQError::broker_operation_failed("SEND_MESSAGE", 1, "failed").with_broker_addr("127.0.0.1:10911");

        if let RocketMQError::BrokerOperationFailed { broker_addr, .. } = err {
            assert_eq!(broker_addr, Some("127.0.0.1:10911".to_string()));
        } else {
            panic!("Expected BrokerOperationFailed");
        }
    }

    #[test]
    fn test_topic_not_exist() {
        let err = RocketMQError::TopicNotExist {
            topic: "TestTopic".to_string(),
        };
        assert_eq!(err.to_string(), "Topic 'TestTopic' does not exist");
    }
}
