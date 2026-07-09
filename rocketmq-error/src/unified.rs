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
pub use crate::observability_error::ObservabilityError;

use crate::boundary::BoundaryErrorView;
use crate::context::ErrorContext;
use crate::context::Sensitive;
use crate::kind::ErrorKind;
use crate::spec::ErrorSpec;
pub use network::NetworkError;
pub use protocol::ProtocolError;
pub use rpc::RpcClientError;
pub use serialization::SerializationError;
use thiserror::Error;
pub use tools::ToolsError;

// Re-export auth error from the auth_error module
pub use crate::auth_error::AuthError;
pub use crate::client_error::*;
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
    // Message Property Errors
    // ============================================================================
    /// Invalid message property
    #[error("Invalid message property: {0}")]
    InvalidProperty(String),

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

    /// Query result was not found
    #[error("Query result was not found: {resource}")]
    QueryNotFound { resource: String },

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
    // Observability Errors
    // ============================================================================
    /// Telemetry, logging, exporter, and provider lifecycle errors.
    #[error(transparent)]
    Observability(#[from] ObservabilityError),

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

    /// Invalid authentication or authorization configuration.
    #[error("Invalid auth configuration for '{key}': {reason}")]
    AuthConfigInvalid { key: &'static str, reason: String },

    /// Authentication or authorization hot reload failed.
    #[error("Auth hot reload failed for '{path}': {reason}")]
    AuthHotReloadFailed { path: String, reason: String },

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

    #[error("Not initialized: {0}")]
    NotInitialized(String),

    #[error("Message is missing required property: {property}")]
    MissingRequiredMessageProperty { property: &'static str },
}

// ============================================================================
// Convenience Constructors
// ============================================================================

impl RocketMQError {
    /// Return the stable logical error kind.
    #[inline]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Network(_) => ErrorKind::Network,
            Self::Serialization(_) => ErrorKind::Serialization,
            Self::Protocol(_) => ErrorKind::Protocol,
            Self::Rpc(_) => ErrorKind::Rpc,
            Self::Authentication(_) => ErrorKind::Authentication,
            Self::Controller(_) => ErrorKind::Controller,
            Self::InvalidProperty(_) => ErrorKind::InvalidProperty,
            Self::BrokerNotFound { .. } => ErrorKind::BrokerNotFound,
            Self::BrokerRegistrationFailed { .. } => ErrorKind::BrokerRegistrationFailed,
            Self::BrokerOperationFailed { .. } => ErrorKind::BrokerOperationFailed,
            Self::TopicNotExist { .. } => ErrorKind::TopicNotExist,
            Self::QueueNotExist { .. } => ErrorKind::QueueNotExist,
            Self::SubscriptionGroupNotExist { .. } => ErrorKind::SubscriptionGroupNotExist,
            Self::QueueIdOutOfRange { .. } => ErrorKind::QueueIdOutOfRange,
            Self::MessageTooLarge { .. } => ErrorKind::MessageTooLarge,
            Self::MessageValidationFailed { .. } => ErrorKind::MessageValidationFailed,
            Self::RetryLimitExceeded { .. } => ErrorKind::RetryLimitExceeded,
            Self::TransactionRejected => ErrorKind::TransactionRejected,
            Self::BrokerPermissionDenied { .. } => ErrorKind::BrokerPermissionDenied,
            Self::NotMasterBroker { .. } => ErrorKind::NotMasterBroker,
            Self::MessageLookupFailed { .. } => ErrorKind::MessageLookupFailed,
            Self::QueryNotFound { .. } => ErrorKind::QueryNotFound,
            Self::TopicSendingForbidden { .. } => ErrorKind::TopicSendingForbidden,
            Self::BrokerAsyncTaskFailed { .. } => ErrorKind::BrokerAsyncTaskFailed,
            Self::RequestBodyInvalid { .. } => ErrorKind::RequestBodyInvalid,
            Self::RequestHeaderError(_) => ErrorKind::RequestHeaderError,
            Self::ResponseProcessFailed { .. } => ErrorKind::ResponseProcessFailed,
            Self::RouteNotFound { .. } => ErrorKind::RouteNotFound,
            Self::RouteInconsistent { .. } => ErrorKind::RouteInconsistent,
            Self::RouteRegistrationConflict { .. } => ErrorKind::RouteRegistrationConflict,
            Self::RouteVersionConflict { .. } => ErrorKind::RouteVersionConflict,
            Self::ClusterNotFound { .. } => ErrorKind::ClusterNotFound,
            Self::ClientNotStarted => ErrorKind::ClientNotStarted,
            Self::ClientAlreadyStarted => ErrorKind::ClientAlreadyStarted,
            Self::ClientShuttingDown => ErrorKind::ClientShuttingDown,
            Self::ClientInvalidState { .. } => ErrorKind::ClientInvalidState,
            Self::ProducerNotAvailable => ErrorKind::ProducerNotAvailable,
            Self::ConsumerNotAvailable => ErrorKind::ConsumerNotAvailable,
            Self::Tools(error) => error.kind(),
            Self::Filter(_) => ErrorKind::Filter,
            Self::Observability(_) => ErrorKind::Internal,
            Self::StorageReadFailed { .. } => ErrorKind::StorageReadFailed,
            Self::StorageWriteFailed { .. } => ErrorKind::StorageWriteFailed,
            Self::StorageCorrupted { .. } => ErrorKind::StorageCorrupted,
            Self::StorageOutOfSpace { .. } => ErrorKind::StorageOutOfSpace,
            Self::StorageLockFailed { .. } => ErrorKind::StorageLockFailed,
            Self::ConfigParseFailed { .. } => ErrorKind::ConfigParseFailed,
            Self::ConfigMissing { .. } => ErrorKind::ConfigMissing,
            Self::ConfigInvalidValue { .. } => ErrorKind::ConfigInvalidValue,
            Self::AuthConfigInvalid { .. } => ErrorKind::AuthConfigInvalid,
            Self::AuthHotReloadFailed { .. } => ErrorKind::AuthHotReloadFailed,
            Self::ControllerNotLeader { .. } => ErrorKind::ControllerNotLeader,
            Self::ControllerRaftError { .. } => ErrorKind::ControllerRaftError,
            Self::ControllerConsensusTimeout { .. } => ErrorKind::ControllerConsensusTimeout,
            Self::ControllerSnapshotFailed { .. } => ErrorKind::ControllerSnapshotFailed,
            Self::IO(_) => ErrorKind::Io,
            Self::IllegalArgument(_) => ErrorKind::IllegalArgument,
            Self::Timeout { .. } => ErrorKind::Timeout,
            Self::Internal(_) => ErrorKind::Internal,
            Self::Service(_) => ErrorKind::Service,
            Self::InvalidVersionOrdinal(_) => ErrorKind::InvalidVersionOrdinal,
            Self::NotInitialized(_) => ErrorKind::NotInitialized,
            Self::MissingRequiredMessageProperty { .. } => ErrorKind::MissingRequiredMessageProperty,
        }
    }

    /// Return the static metadata for this error.
    #[inline]
    pub fn spec(&self) -> &'static ErrorSpec {
        self.kind().spec()
    }

    /// Return the stable external message for this error.
    ///
    /// `Display` and `Debug` remain diagnostic surfaces and may include local
    /// details. Boundary adapters should use this public message together with
    /// [`Self::context`] when building API, CLI, log, or protocol responses.
    #[inline]
    pub fn public_message(&self) -> &'static str {
        self.spec().public_message
    }

    /// Return redaction-aware structured context for this error.
    ///
    /// The returned context is a snapshot derived from the current enum variant.
    /// Sensitive details are represented through [`Sensitive`] so external
    /// adapters can safely render the context without leaking raw values.
    pub fn context(&self) -> ErrorContext {
        match self {
            Self::Network(error) => {
                ErrorContext::new().with_sensitive("addr", Sensitive::new(error.addr().to_string()))
            }
            Self::Serialization(error) => redacted_context("serialization_error", error.to_string()),
            Self::Protocol(error) => ErrorContext::new().with_field("protocol_error", error.to_string()),
            Self::Rpc(error) => redacted_context("rpc_error", error.to_string()),
            Self::Authentication(error) => redacted_context("auth_error", error.to_string()),
            Self::Controller(error) => redacted_context("controller_error", error.to_string()),
            Self::InvalidProperty(property) => ErrorContext::new().with_field("property", property.as_str()),
            Self::BrokerNotFound { name } => ErrorContext::new().with_field("broker", name.as_str()),
            Self::BrokerRegistrationFailed { name, reason } => ErrorContext::new()
                .with_field("broker", name.as_str())
                .with_field("reason", reason.as_str()),
            Self::BrokerOperationFailed {
                operation,
                code,
                message,
                broker_addr,
            } => {
                let mut context = ErrorContext::new()
                    .with_field("operation", *operation)
                    .with_field("broker_code", code.to_string())
                    .with_field("message", message.as_str());
                if let Some(addr) = broker_addr {
                    context = context.with_sensitive("broker_addr", Sensitive::new(addr.clone()));
                }
                context
            }
            Self::TopicNotExist { topic } => ErrorContext::new().with_field("topic", topic.as_str()),
            Self::QueueNotExist { topic, queue_id } => ErrorContext::new()
                .with_field("topic", topic.as_str())
                .with_field("queue_id", queue_id.to_string()),
            Self::SubscriptionGroupNotExist { group } => ErrorContext::new().with_field("group", group.as_str()),
            Self::QueueIdOutOfRange { topic, queue_id, max } => ErrorContext::new()
                .with_field("topic", topic.as_str())
                .with_field("queue_id", queue_id.to_string())
                .with_field("max_queue_id", max.to_string()),
            Self::MessageTooLarge { actual, limit } => ErrorContext::new()
                .with_field("actual_bytes", actual.to_string())
                .with_field("limit_bytes", limit.to_string()),
            Self::MessageValidationFailed { reason } => ErrorContext::new().with_field("reason", reason.as_str()),
            Self::RetryLimitExceeded { group, current, max } => ErrorContext::new()
                .with_field("group", group.as_str())
                .with_field("current", current.to_string())
                .with_field("max", max.to_string()),
            Self::TransactionRejected => ErrorContext::new(),
            Self::BrokerPermissionDenied { operation } => {
                ErrorContext::new().with_field("operation", operation.as_str())
            }
            Self::NotMasterBroker { master_address } => {
                ErrorContext::new().with_sensitive("master_address", Sensitive::new(master_address.clone()))
            }
            Self::MessageLookupFailed { offset } => ErrorContext::new().with_field("offset", offset.to_string()),
            Self::QueryNotFound { resource } => ErrorContext::new().with_field("resource", resource.as_str()),
            Self::TopicSendingForbidden { topic } => ErrorContext::new().with_field("topic", topic.as_str()),
            Self::BrokerAsyncTaskFailed { task, context, .. } => ErrorContext::new()
                .with_field("task", *task)
                .with_sensitive("context", Sensitive::new(context.clone())),
            Self::RequestBodyInvalid { operation, reason } => ErrorContext::new()
                .with_field("operation", *operation)
                .with_field("reason", reason.as_str()),
            Self::RequestHeaderError(reason) => ErrorContext::new().with_field("reason", reason.as_str()),
            Self::ResponseProcessFailed { operation, reason } => ErrorContext::new()
                .with_field("operation", *operation)
                .with_field("reason", reason.as_str()),
            Self::RouteNotFound { topic } => ErrorContext::new().with_field("topic", topic.as_str()),
            Self::RouteInconsistent { topic, reason } => ErrorContext::new()
                .with_field("topic", topic.as_str())
                .with_field("reason", reason.as_str()),
            Self::RouteRegistrationConflict { broker_name, reason } => ErrorContext::new()
                .with_field("broker", broker_name.as_str())
                .with_field("reason", reason.as_str()),
            Self::RouteVersionConflict { expected, actual } => ErrorContext::new()
                .with_field("expected", expected.to_string())
                .with_field("actual", actual.to_string()),
            Self::ClusterNotFound { cluster } => ErrorContext::new().with_field("cluster", cluster.as_str()),
            Self::ClientNotStarted
            | Self::ClientAlreadyStarted
            | Self::ClientShuttingDown
            | Self::ProducerNotAvailable
            | Self::ConsumerNotAvailable => ErrorContext::new(),
            Self::ClientInvalidState { expected, actual } => ErrorContext::new()
                .with_field("expected", *expected)
                .with_field("actual", actual.as_str()),
            Self::Tools(error) => error.context(),
            Self::Filter(error) => ErrorContext::new().with_field("filter_error", error.to_string()),
            Self::Observability(error) => redacted_context("observability_error", error.to_string()),
            Self::StorageReadFailed { path, reason } | Self::StorageWriteFailed { path, reason } => ErrorContext::new()
                .with_sensitive("path", Sensitive::new(path.clone()))
                .with_sensitive("reason", Sensitive::new(reason.clone())),
            Self::StorageCorrupted { path } | Self::StorageOutOfSpace { path } | Self::StorageLockFailed { path } => {
                ErrorContext::new().with_sensitive("path", Sensitive::new(path.clone()))
            }
            Self::ConfigParseFailed { key, reason } => ErrorContext::new()
                .with_field("key", *key)
                .with_sensitive("reason", Sensitive::new(reason.clone())),
            Self::ConfigMissing { key } => ErrorContext::new().with_field("key", *key),
            Self::ConfigInvalidValue { key, value, reason } => ErrorContext::new()
                .with_field("key", *key)
                .with_sensitive("value", Sensitive::new(value.clone()))
                .with_sensitive("reason", Sensitive::new(reason.clone())),
            Self::AuthConfigInvalid { key, reason } => ErrorContext::new()
                .with_field("key", *key)
                .with_sensitive("reason", Sensitive::new(reason.clone())),
            Self::AuthHotReloadFailed { path, reason } => ErrorContext::new()
                .with_sensitive("path", Sensitive::new(path.clone()))
                .with_sensitive("reason", Sensitive::new(reason.clone())),
            Self::ControllerNotLeader { leader_id } => ErrorContext::new().with_field(
                "leader_id",
                leader_id.map_or_else(|| "unknown".to_string(), |id| id.to_string()),
            ),
            Self::ControllerRaftError { reason } | Self::ControllerSnapshotFailed { reason } => {
                ErrorContext::new().with_sensitive("reason", Sensitive::new(reason.clone()))
            }
            Self::ControllerConsensusTimeout { operation, timeout_ms } => ErrorContext::new()
                .with_field("operation", *operation)
                .with_field("timeout_ms", timeout_ms.to_string()),
            Self::IO(error) => redacted_context("io_error", error.to_string()),
            Self::IllegalArgument(message) => ErrorContext::new().with_field("message", message.as_str()),
            Self::Timeout { operation, timeout_ms } => ErrorContext::new()
                .with_field("operation", *operation)
                .with_field("timeout_ms", timeout_ms.to_string()),
            Self::Internal(message) => redacted_context("internal_error", message.clone()),
            Self::Service(error) => redacted_context("service_error", error.to_string()),
            Self::InvalidVersionOrdinal(ordinal) => ErrorContext::new().with_field("ordinal", ordinal.to_string()),
            Self::NotInitialized(reason) => ErrorContext::new().with_field("reason", reason.as_str()),
            Self::MissingRequiredMessageProperty { property } => ErrorContext::new().with_field("property", *property),
        }
    }

    /// Return a public, redaction-aware snapshot for protocol and UI
    /// boundaries.
    #[inline]
    pub fn boundary_view(&self) -> BoundaryErrorView {
        let spec = self.spec();
        BoundaryErrorView::new(
            spec.kind,
            spec.code,
            spec.category,
            spec.public_message,
            self.context(),
            spec.remoting,
            spec.grpc,
            spec.http,
            spec.cli,
            spec.recovery,
            spec.observe,
        )
    }

    /// Create a network connection failed error
    #[inline]
    pub fn network_connection_failed(addr: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Network(NetworkError::connection_failed(addr, reason))
    }

    /// Create a network timeout error
    #[inline]
    pub fn network_timeout(addr: impl Into<String>, timeout: std::time::Duration) -> Self {
        Self::Network(NetworkError::request_timeout(addr, timeout.as_millis() as u64))
    }

    /// Create a network request failed error
    #[inline]
    pub fn network_request_failed(addr: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Network(NetworkError::send_failed(addr, reason))
    }

    /// Create a deserialization failed error
    #[inline]
    pub fn deserialization_failed(format: &'static str, reason: impl Into<String>) -> Self {
        Self::Serialization(SerializationError::decode_failed(format, reason))
    }

    /// Create a validation failed error
    #[inline]
    pub fn validation_failed(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Tools(ToolsError::validation_error(field, reason))
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

    /// Create a generic query-not-found error.
    #[inline]
    pub fn query_not_found(resource: impl Into<String>) -> Self {
        Self::QueryNotFound {
            resource: resource.into(),
        }
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

    /// Create an auth configuration error.
    #[inline]
    pub fn auth_config_invalid(key: &'static str, reason: impl Into<String>) -> Self {
        Self::AuthConfigInvalid {
            key,
            reason: reason.into(),
        }
    }

    /// Create an auth hot-reload error.
    #[inline]
    pub fn auth_hot_reload_failed(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::AuthHotReloadFailed {
            path: path.into(),
            reason: reason.into(),
        }
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

fn redacted_context(key: &'static str, value: impl Into<String>) -> ErrorContext {
    ErrorContext::new().with_sensitive(key, Sensitive::new(value.into()))
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

    #[test]
    fn auth_config_and_hot_reload_errors_are_distinct() {
        let config = RocketMQError::auth_config_invalid("auth.authorization", "provider not ready");
        assert!(matches!(
            config,
            RocketMQError::AuthConfigInvalid {
                key: "auth.authorization",
                ..
            }
        ));
        assert!(config.to_string().contains("Invalid auth configuration"));

        let reload = RocketMQError::auth_hot_reload_failed("conf/plain_acl.yml", "parse failed");
        assert!(matches!(
            reload,
            RocketMQError::AuthHotReloadFailed {
                ref path,
                ref reason
            } if path == "conf/plain_acl.yml" && reason == "parse failed"
        ));
        assert!(reload.to_string().contains("Auth hot reload failed"));
    }
}
