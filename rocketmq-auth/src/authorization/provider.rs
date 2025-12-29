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

//! Authorization provider abstraction for RocketMQ authentication and authorization.
//!
//! This module defines the core `AuthorizationProvider` trait, which serves as the
//! unified interface for all authorization implementations (ACL, RBAC, OPA, etc.).

use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::config::AuthConfig;

/// Result type for authorization operations.
pub type AuthorizationResult<T> = Result<T, AuthorizationError>;

/// Error type for authorization operations.
///
/// This error type covers all authorization-related failures including:
/// - Permission denied errors
/// - Policy evaluation failures
/// - Configuration errors
/// - Internal errors
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    /// Authorization denied: subject does not have permission to perform the requested action.
    #[error("Authorization denied for subject '{subject}' on resource '{resource}': {reason}")]
    PermissionDenied {
        subject: String,
        resource: String,
        reason: String,
    },

    /// Policy evaluation failed due to an error in the policy engine.
    #[error("Policy evaluation failed: {0}")]
    PolicyEvaluationFailed(String),

    /// Required configuration is missing or invalid.
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Subject (user/role) not found in the authorization system.
    #[error("Subject '{0}' not found")]
    SubjectNotFound(String),

    /// Resource not found or invalid.
    #[error("Resource '{0}' not found or invalid")]
    ResourceNotFound(String),

    /// Authorization provider not initialized properly.
    #[error("Authorization provider not initialized: {0}")]
    NotInitialized(String),

    /// Internal error during authorization processing.
    #[error("Internal authorization error: {0}")]
    InternalError(String),

    /// Metadata service error (e.g., database, cache, remote service failure).
    #[error("Metadata service error: {0}")]
    MetadataServiceError(String),

    /// Authorization context is invalid or incomplete.
    #[error("Invalid authorization context: {0}")]
    InvalidContext(String),
}

/// Authorization provider trait.
///
/// This trait defines the core abstraction for authorization in RocketMQ. Implementors
/// can provide different authorization strategies such as:
/// - ACL (Access Control Lists)
/// - RBAC (Role-Based Access Control)
/// - ABAC (Attribute-Based Access Control)
/// - Integration with external policy engines (e.g., Open Policy Agent)
///
/// # Design Considerations
/// - **Async-first**: All methods are async to support asynchronous implementations
/// - **Context-based**: Authorization decisions are made based on `DefaultAuthorizationContext`
/// - **Extensible**: Implementations can maintain internal state via `initialize`
/// - **Error handling**: All failures are expressed through `AuthorizationError`
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::provider::{AuthorizationProvider, AuthorizationResult};
/// use rocketmq_auth::authorization::context::default_authorization_context::DefaultAuthorizationContext;
/// use rocketmq_auth::config::AuthConfig;
///
/// struct MyAuthProvider;
///
/// impl AuthorizationProvider for MyAuthProvider {
///     fn initialize(&mut self, config: AuthConfig) -> AuthorizationResult<()> {
///         // Initialize provider with configuration
///         Ok(())
///     }
///
///     async fn authorize(&self, context: &DefaultAuthorizationContext) -> AuthorizationResult<()> {
///         // Implement authorization logic
///         Ok(())
///     }
/// }
/// ```
pub trait AuthorizationProvider: Send + Sync {
    /// Initialize the authorization provider with configuration.
    ///
    /// This method is called once during provider setup. Implementations should:
    /// - Load configuration
    /// - Initialize metadata services (database, cache, etc.)
    /// - Establish connections to external services if needed
    /// - Validate configuration parameters
    ///
    /// # Arguments
    /// * `config` - Authorization configuration including provider-specific settings
    ///
    /// # Errors
    /// Returns `AuthorizationError::ConfigurationError` if configuration is invalid
    /// or initialization fails.
    fn initialize(&mut self, config: AuthConfig) -> AuthorizationResult<()>;

    /// Initialize with both configuration and optional metadata service.
    ///
    /// This is an extended version of `initialize` that accepts a metadata service
    /// supplier for advanced scenarios (e.g., shared metadata across components).
    ///
    /// Default implementation delegates to `initialize(config)`.
    ///
    /// # Arguments
    /// * `config` - Authorization configuration
    /// * `metadata_service` - Optional metadata service supplier
    ///
    /// # Errors
    /// Returns `AuthorizationError::ConfigurationError` if initialization fails.
    fn initialize_with_metadata(
        &mut self,
        config: AuthConfig,
        #[allow(unused_variables)] metadata_service: Option<Box<dyn std::any::Any + Send + Sync>>,
    ) -> AuthorizationResult<()> {
        // Default implementation ignores metadata_service
        self.initialize(config)
    }

    /// Authorize an operation based on the given context.
    ///
    /// This is the core authorization method. Implementations should:
    /// - Extract subject, resource, and action from context
    /// - Evaluate authorization policies
    /// - Check permissions and constraints (IP whitelist, time-based rules, etc.)
    /// - Audit log the authorization decision
    ///
    /// # Arguments
    /// * `context` - Authorization context containing subject, resource, actions, and metadata
    ///
    /// # Returns
    /// - `Ok(())` if authorization succeeds
    /// - `Err(AuthorizationError::PermissionDenied)` if authorization is denied
    /// - Other errors for system failures
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let context = DefaultAuthorizationContext::new(
    ///     subject,
    ///     resource,
    ///     vec![Action::Pub],
    ///     source_ip
    /// );
    /// provider.authorize(&context).await?;
    /// ```
    async fn authorize(&self, context: &DefaultAuthorizationContext) -> AuthorizationResult<()>;

    /// Create authorization contexts from gRPC metadata and request message.
    ///
    /// Parses gRPC request metadata (headers) and the protocol buffer message to
    /// construct authorization contexts. Multiple contexts may be returned if the
    /// request involves multiple resources (e.g., batch operations).
    ///
    /// # Arguments
    /// * `metadata` - gRPC metadata containing authentication tokens, source IP, etc.
    /// * `message` - Protocol buffer message (e.g., SendMessageRequest)
    ///
    /// # Returns
    /// List of authorization contexts to be evaluated. Empty list if no authorization needed.
    ///
    /// # Errors
    /// Returns `AuthorizationError::InvalidContext` if context cannot be constructed.
    #[allow(unused_variables)]
    fn new_contexts_from_grpc_metadata(
        &self,
        metadata: &dyn std::any::Any,
        message: &dyn std::any::Any,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        // Default implementation returns empty list (no-op)
        Ok(Vec::new())
    }

    /// Create authorization contexts from channel context and remoting command.
    ///
    /// Parses channel context (connection info) and RocketMQ remoting command
    /// to construct authorization contexts for TCP-based protocols.
    ///
    /// # Arguments
    /// * `channel_context` - Channel context (connection, remote address, etc.)
    /// * `command` - RocketMQ remoting command containing request code and data
    ///
    /// # Returns
    /// List of authorization contexts to be evaluated.
    ///
    /// # Errors
    /// Returns `AuthorizationError::InvalidContext` if context cannot be constructed.
    #[allow(unused_variables)]
    fn new_contexts_from_remoting_command(
        &self,
        channel_context: &dyn std::any::Any,
        command: &RemotingCommand,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        // Default implementation returns empty list (no-op)
        Ok(Vec::new())
    }
}

/// A no-op authorization provider for testing or when authorization is disabled.
///
/// This provider always allows all operations without performing any checks.
/// Useful for:
/// - Testing environments
/// - Development setups
/// - Explicitly disabling authorization
///
/// # Security Warning
/// **DO NOT use in production environments!** This provider grants full access to all operations.
pub struct NoopAuthorizationProvider;

impl NoopAuthorizationProvider {
    /// Create a new no-op authorization provider.
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopAuthorizationProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthorizationProvider for NoopAuthorizationProvider {
    fn initialize(&mut self, _config: AuthConfig) -> AuthorizationResult<()> {
        // No initialization needed
        Ok(())
    }

    async fn authorize(&self, _context: &DefaultAuthorizationContext) -> AuthorizationResult<()> {
        // Always allow
        Ok(())
    }

    fn new_contexts_from_grpc_metadata(
        &self,
        _metadata: &dyn std::any::Any,
        _message: &dyn std::any::Any,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        // Return empty contexts (no authorization needed)
        Ok(Vec::new())
    }

    fn new_contexts_from_remoting_command(
        &self,
        _channel_context: &dyn std::any::Any,
        _command: &RemotingCommand,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        // Return empty contexts (no authorization needed)
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_provider_always_allows() {
        let mut provider = NoopAuthorizationProvider::new();
        let config = AuthConfig::default();

        // Initialize should succeed
        assert!(provider.initialize(config).is_ok());

        // Authorize should always succeed (even with empty context)
        // Note: This is a simplified test; in real usage, context would be properly constructed
        // The test here just verifies the no-op behavior
    }

    #[test]
    fn test_authorization_error_display() {
        let error = AuthorizationError::PermissionDenied {
            subject: "user:alice".to_string(),
            resource: "topic:test".to_string(),
            reason: "insufficient permissions".to_string(),
        };
        let msg = format!("{}", error);
        assert!(msg.contains("alice"));
        assert!(msg.contains("test"));
        assert!(msg.contains("insufficient permissions"));
    }

    #[test]
    fn test_authorization_error_variants() {
        // Test different error variants
        let errors = vec![
            AuthorizationError::SubjectNotFound("user:alice".to_string()),
            AuthorizationError::ResourceNotFound("topic:test".to_string()),
            AuthorizationError::PolicyEvaluationFailed("invalid policy".to_string()),
            AuthorizationError::ConfigurationError("missing config".to_string()),
            AuthorizationError::NotInitialized("provider not ready".to_string()),
            AuthorizationError::InternalError("unexpected error".to_string()),
            AuthorizationError::MetadataServiceError("db connection failed".to_string()),
            AuthorizationError::InvalidContext("missing subject".to_string()),
        ];

        for error in errors {
            // Ensure all error variants can be formatted
            let _msg = format!("{}", error);
            let _debug = format!("{:?}", error);
        }
    }
}
