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

//! Tools and Admin operation specific errors
//!
//! This module contains error types specific to RocketMQ admin tools and CLI operations.

use thiserror::Error;

/// Tools-specific errors for admin operations
#[derive(Debug, Error)]
pub enum ToolsError {
    // ============================================================================
    // Topic Management Errors
    // ============================================================================
    /// Topic not found
    #[error("Topic '{topic}' not found")]
    TopicNotFound { topic: String },

    /// Topic already exists
    #[error("Topic '{topic}' already exists")]
    TopicAlreadyExists { topic: String },

    /// Invalid topic configuration
    #[error("Invalid topic configuration: {reason}")]
    TopicInvalid { reason: String },

    // ============================================================================
    // Cluster Management Errors
    // ============================================================================
    /// Cluster not found
    #[error("Cluster '{cluster}' not found")]
    ClusterNotFound { cluster: String },

    /// Invalid cluster configuration
    #[error("Invalid cluster configuration: {reason}")]
    ClusterInvalid { reason: String },

    // ============================================================================
    // Broker Management Errors
    // ============================================================================
    /// Broker not found
    #[error("Broker '{broker}' not found")]
    BrokerNotFound { broker: String },

    /// Broker offline
    #[error("Broker '{broker}' is offline")]
    BrokerOffline { broker: String },

    // ============================================================================
    // Consumer Management Errors
    // ============================================================================
    /// Consumer group not found
    #[error("Consumer group '{group}' not found")]
    ConsumerGroupNotFound { group: String },

    /// Consumer offline
    #[error("Consumer '{consumer}' is offline")]
    ConsumerOffline { consumer: String },

    // ============================================================================
    // NameServer Management Errors
    // ============================================================================
    /// NameServer unreachable
    #[error("NameServer '{addr}' is unreachable")]
    NameServerUnreachable { addr: String },

    /// NameServer configuration invalid
    #[error("Invalid NameServer configuration: {reason}")]
    NameServerConfigInvalid { reason: String },

    // ============================================================================
    // Configuration Errors
    // ============================================================================
    /// Invalid configuration field
    #[error("Invalid configuration for '{field}': {reason}")]
    InvalidConfiguration { field: String, reason: String },

    /// Missing required field
    #[error("Missing required field: '{field}'")]
    MissingRequiredField { field: String },

    // ============================================================================
    // Validation Errors
    // ============================================================================
    /// Input validation failed
    #[error("Validation failed for '{field}': {reason}")]
    ValidationError { field: String, reason: String },

    /// Generic validation error
    #[error("Validation error: {message}")]
    ValidationFailed { message: String },

    // ============================================================================
    // Permission Errors
    // ============================================================================
    /// Permission denied for operation
    #[error("Permission denied for operation: {operation}")]
    PermissionDenied { operation: String },

    /// Invalid permission value
    #[error("Invalid permission value: {value}, allowed values: {}", .allowed.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))]
    InvalidPermission { value: i32, allowed: Vec<i32> },

    // ============================================================================
    // Operation Errors
    // ============================================================================
    /// Operation timeout
    #[error("Operation '{operation}' timed out after {duration_ms}ms")]
    OperationTimeout { operation: String, duration_ms: u64 },

    /// Generic internal error
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl ToolsError {
    // ============================================================================
    // Convenience Constructors
    // ============================================================================

    /// Create a topic not found error
    #[inline]
    pub fn topic_not_found(topic: impl Into<String>) -> Self {
        Self::TopicNotFound { topic: topic.into() }
    }

    /// Create a topic already exists error
    #[inline]
    pub fn topic_already_exists(topic: impl Into<String>) -> Self {
        Self::TopicAlreadyExists { topic: topic.into() }
    }

    /// Create a cluster not found error
    #[inline]
    pub fn cluster_not_found(cluster: impl Into<String>) -> Self {
        Self::ClusterNotFound {
            cluster: cluster.into(),
        }
    }

    /// Create a broker not found error
    #[inline]
    pub fn broker_not_found(broker: impl Into<String>) -> Self {
        Self::BrokerNotFound { broker: broker.into() }
    }

    /// Create a validation error
    #[inline]
    pub fn validation_error(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ValidationError {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Create a nameserver unreachable error
    #[inline]
    pub fn nameserver_unreachable(addr: impl Into<String>) -> Self {
        Self::NameServerUnreachable { addr: addr.into() }
    }

    /// Create a nameserver config invalid error
    #[inline]
    pub fn nameserver_config_invalid(reason: impl Into<String>) -> Self {
        Self::NameServerConfigInvalid { reason: reason.into() }
    }

    /// Create an internal error
    #[inline]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_management_errors() {
        let err = ToolsError::topic_not_found("TestTopic");
        assert_eq!(err.to_string(), "Topic 'TestTopic' not found");

        let err = ToolsError::topic_already_exists("TestTopic");
        assert_eq!(err.to_string(), "Topic 'TestTopic' already exists");

        let err = ToolsError::TopicInvalid {
            reason: "invalid partitions".to_string(),
        };
        assert_eq!(err.to_string(), "Invalid topic configuration: invalid partitions");
    }

    #[test]
    fn test_cluster_management_errors() {
        let err = ToolsError::cluster_not_found("TestCluster");
        assert_eq!(err.to_string(), "Cluster 'TestCluster' not found");

        let err = ToolsError::ClusterInvalid {
            reason: "missing brokers".to_string(),
        };
        assert_eq!(err.to_string(), "Invalid cluster configuration: missing brokers");
    }

    #[test]
    fn test_broker_management_errors() {
        let err = ToolsError::broker_not_found("broker-a");
        assert_eq!(err.to_string(), "Broker 'broker-a' not found");

        let err = ToolsError::BrokerOffline {
            broker: "broker-a".to_string(),
        };
        assert_eq!(err.to_string(), "Broker 'broker-a' is offline");
    }

    #[test]
    fn test_consumer_management_errors() {
        let err = ToolsError::ConsumerGroupNotFound {
            group: "test-group".to_string(),
        };
        assert_eq!(err.to_string(), "Consumer group 'test-group' not found");

        let err = ToolsError::ConsumerOffline {
            consumer: "consumer-1".to_string(),
        };
        assert_eq!(err.to_string(), "Consumer 'consumer-1' is offline");
    }

    #[test]
    fn test_nameserver_management_errors() {
        let err = ToolsError::nameserver_unreachable("127.0.0.1:9876");
        assert_eq!(err.to_string(), "NameServer '127.0.0.1:9876' is unreachable");

        let err = ToolsError::nameserver_config_invalid("missing nameserver");
        assert_eq!(err.to_string(), "Invalid NameServer configuration: missing nameserver");
    }

    #[test]
    fn test_configuration_errors() {
        let err = ToolsError::InvalidConfiguration {
            field: "name_server".to_string(),
            reason: "missing nameserver".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Invalid configuration for 'name_server': missing nameserver"
        );

        let err = ToolsError::MissingRequiredField {
            field: "topic".to_string(),
        };
        assert_eq!(err.to_string(), "Missing required field: 'topic'");
    }

    #[test]
    fn test_validation_errors() {
        let err = ToolsError::validation_error("topic_name", "name too long");
        assert_eq!(err.to_string(), "Validation failed for 'topic_name': name too long");

        let err = ToolsError::ValidationFailed {
            message: "generic validation error".to_string(),
        };
        assert_eq!(err.to_string(), "Validation error: generic validation error");
    }

    #[test]
    fn test_permission_errors() {
        let err = ToolsError::PermissionDenied {
            operation: "createTopic".to_string(),
        };
        assert_eq!(err.to_string(), "Permission denied for operation: createTopic");

        let err = ToolsError::InvalidPermission {
            value: 1,
            allowed: vec![2, 4, 6],
        };
        assert!(err.to_string().contains("Invalid permission value: 1"));
        assert!(err.to_string().contains("2, 4, 6"));
    }

    #[test]
    fn test_operation_errors() {
        let err = ToolsError::OperationTimeout {
            operation: "createTopic".to_string(),
            duration_ms: 5000,
        };
        assert!(err
            .to_string()
            .contains("Operation 'createTopic' timed out after 5000ms"));

        let err = ToolsError::internal("unexpected error");
        assert!(err.to_string().contains("Internal error: unexpected error"));
    }
}
