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

//! Authorization provider abstraction for RocketMQ authentication and authorization.
//!
//! This module defines the core `AuthorizationProvider` trait, which serves as the
//! unified interface for all authorization implementations (ACL, RBAC, OPA, etc.).

use std::sync::Arc;

use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::authentication::provider::LocalAuthenticationMetadataProvider;
use crate::authorization::builder::default_authorization_context_builder::DefaultAuthorizationContextBuilder;
use crate::authorization::builder::AuthorizationContextBuilder;
use crate::authorization::chain::AclAuthorizationHandler;
use crate::authorization::chain::AuthorizationHandler;
use crate::authorization::chain::UserAuthorizationDecision;
use crate::authorization::chain::UserAuthorizationHandler;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use crate::config::AuthConfig;
use crate::runtime::ProviderRegistry;

/// Result type for authorization operations.
pub type AuthorizationResult<T> = Result<T, AuthorizationError>;

/// Error type for authorization operations.
///
/// This error type covers all authorization-related failures including:
/// - Permission denied errors
/// - Policy evaluation failures
/// - Configuration errors
/// - Internal errors
#[derive(Debug, Clone, thiserror::Error)]
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
#[allow(async_fn_in_trait)]
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
        channel_context: &(dyn std::any::Any + Send + Sync),
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
        _channel_context: &(dyn std::any::Any + Send + Sync),
        _command: &RemotingCommand,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        // Return empty contexts (no authorization needed)
        Ok(Vec::new())
    }
}

/// Default authorization provider implementation.
///
/// This provider implements a chain-of-responsibility pattern for authorization,
/// delegating to specialized handlers:
/// 1. **UserAuthorizationHandler**: Handles super-user bypass logic
/// 2. **AclAuthorizationHandler**: Performs ACL-based permission checks
///
/// # Architecture
///
/// The provider follows RocketMQ's authorization model:
/// - Subject-based access control (users, roles, service accounts)
/// - Resource-level permissions (topics, groups, clusters)
/// - Action-based authorization (PUB, SUB, CREATE, UPDATE, DELETE, GET, LIST)
/// - Policy evaluation with ALLOW/DENY decisions
/// - IP whitelist support
/// - Default-deny security policy
///
/// # Authorization Flow
///
/// 1. Extract subject, resource, and actions from context
/// 2. Check if user is a super-user (bypass authorization)
/// 3. Query ACL metadata for the subject
/// 4. Evaluate policies against the requested resource and actions
/// 5. Check environment constraints (IP whitelist, time-based rules)
/// 6. Apply decision (ALLOW/DENY) with default-deny policy
/// 7. Audit log the authorization decision
///
/// # Thread Safety
///
/// This implementation is thread-safe and can be shared across multiple async tasks.
/// Internal state is protected using `Arc` and atomic operations.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::provider::DefaultAuthorizationProvider;
/// use std::sync::Arc;
///
/// let mut provider = DefaultAuthorizationProvider::new();
/// provider.initialize(config)?;
///
/// // Authorize a request
/// provider.authorize(&context).await?;
/// ```
pub struct DefaultAuthorizationProvider {
    /// Authorization configuration
    config: Option<AuthConfig>,

    /// Metadata service supplier (reserved for future external providers)
    metadata_service: Option<Box<dyn std::any::Any + Send + Sync>>,

    authentication_metadata_provider: Option<Arc<LocalAuthenticationMetadataProvider>>,
    authorization_metadata_provider: Option<Arc<LocalAuthorizationMetadataProvider>>,
    context_builder: Option<DefaultAuthorizationContextBuilder>,
}

impl DefaultAuthorizationProvider {
    /// Create a new default authorization provider.
    pub fn new() -> Self {
        Self {
            config: None,
            metadata_service: None,
            authentication_metadata_provider: None,
            authorization_metadata_provider: None,
            context_builder: None,
        }
    }

    pub fn authentication_metadata_provider(&self) -> Option<Arc<LocalAuthenticationMetadataProvider>> {
        self.authentication_metadata_provider.clone()
    }

    pub fn authorization_metadata_provider(&self) -> Option<Arc<LocalAuthorizationMetadataProvider>> {
        self.authorization_metadata_provider.clone()
    }

    pub fn initialize_with_registry(
        &mut self,
        config: AuthConfig,
        provider_registry: ProviderRegistry,
    ) -> AuthorizationResult<()> {
        self.config = Some(config.clone());
        self.metadata_service = None;
        self.context_builder = Some(DefaultAuthorizationContextBuilder::new(config));
        self.authentication_metadata_provider = Some(provider_registry.authentication_metadata_provider());
        self.authorization_metadata_provider = Some(provider_registry.authorization_metadata_provider());
        Ok(())
    }

    /// Audit log an authorization decision.
    ///
    /// Logs successful authorizations at DEBUG level and denials at INFO level.
    /// Follows the format: [AUTHORIZATION] Subject = {subject} is {decision} Action = {actions}
    /// from sourceIp = {ip} on resource = {resource} for request = {rpc_code}
    fn audit_log(&self, context: &DefaultAuthorizationContext, error: Option<&AuthorizationError>) {
        use tracing::debug;
        use tracing::info;

        let subject_key = match context.subject_key() {
            Some(key) => key,
            None => return, // No subject, skip logging
        };

        let decision = if error.is_some() { "DENY" } else { "ALLOW" };

        let actions = context
            .actions()
            .iter()
            .map(|a| format!("{:?}", a))
            .collect::<Vec<_>>()
            .join(",");

        let source_ip = context.source_ip().unwrap_or("unknown");

        let resource = context
            .resource()
            .map(|r| format!("{:?}", r))
            .unwrap_or_else(|| "unknown".to_string());

        let rpc_code = context.rpc_code().unwrap_or("unknown");

        if error.is_none() {
            debug!(
                "[AUTHORIZATION] Subject = {} is {} Action = {} from sourceIp = {} on resource = {} for request = {}.",
                subject_key, decision, actions, source_ip, resource, rpc_code
            );
        } else {
            info!(
                "[AUTHORIZATION] Subject = {} is {} Action = {} from sourceIp = {} on resource = {} for request = {}.",
                subject_key, decision, actions, source_ip, resource, rpc_code
            );
        }
    }
}

impl Default for DefaultAuthorizationProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(async_fn_in_trait)]
impl AuthorizationProvider for DefaultAuthorizationProvider {
    fn initialize(&mut self, config: AuthConfig) -> AuthorizationResult<()> {
        self.initialize_with_metadata(config, None)
    }

    fn initialize_with_metadata(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Box<dyn std::any::Any + Send + Sync>>,
    ) -> AuthorizationResult<()> {
        use tracing::debug;

        debug!("Initializing DefaultAuthorizationProvider");
        self.config = Some(config.clone());
        self.metadata_service = metadata_service;
        self.context_builder = Some(DefaultAuthorizationContextBuilder::new(config.clone()));

        self.authentication_metadata_provider = Some(Arc::new(LocalAuthenticationMetadataProvider::new()));

        let mut authorization_metadata_provider = LocalAuthorizationMetadataProvider::new();
        authorization_metadata_provider.initialize(config, None)?;
        self.authorization_metadata_provider = Some(Arc::new(authorization_metadata_provider));

        Ok(())
    }

    async fn authorize(&self, context: &DefaultAuthorizationContext) -> AuthorizationResult<()> {
        use tracing::debug;
        use tracing::warn;

        // Validate context
        if context.subject_key().is_none() {
            warn!("Authorization context missing subject");
            return Err(AuthorizationError::InvalidContext(
                "Missing subject in authorization context".to_string(),
            ));
        }

        if context.resource().is_none() {
            warn!("Authorization context missing resource");
            return Err(AuthorizationError::InvalidContext(
                "Missing resource in authorization context".to_string(),
            ));
        }

        if context.actions().is_empty() {
            warn!("Authorization context has no actions");
            return Err(AuthorizationError::InvalidContext(
                "No actions specified in authorization context".to_string(),
            ));
        }

        debug!(
            "Authorizing subject={:?} resource={:?} actions={:?}",
            context.subject_key(),
            context.resource(),
            context.actions()
        );

        let authentication_metadata_provider = self.authentication_metadata_provider.as_ref().ok_or_else(|| {
            AuthorizationError::NotInitialized("Authentication metadata provider is not configured".to_string())
        })?;
        let authorization_metadata_provider = self.authorization_metadata_provider.as_ref().ok_or_else(|| {
            AuthorizationError::NotInitialized("Authorization metadata provider is not configured".to_string())
        })?;

        let result = async {
            let user_handler = UserAuthorizationHandler::new(authentication_metadata_provider.clone());
            match user_handler
                .authorize_subject(context)
                .await
                .map_err(|error| map_handler_error(context, error))?
            {
                UserAuthorizationDecision::SuperUser => Ok(()),
                UserAuthorizationDecision::Continue => {
                    let acl_handler = AclAuthorizationHandler::new(authorization_metadata_provider.clone());
                    acl_handler
                        .handle(context)
                        .await
                        .map_err(|error| map_handler_error(context, error))
                }
            }
        }
        .await;

        self.audit_log(context, result.as_ref().err());
        result
    }

    fn new_contexts_from_remoting_command(
        &self,
        channel_context: &(dyn std::any::Any + Send + Sync),
        command: &RemotingCommand,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        let builder = self.context_builder.as_ref().ok_or_else(|| {
            AuthorizationError::NotInitialized("Authorization context builder is not configured".to_string())
        })?;
        builder.build_from_remoting(channel_context, command)
    }
}

fn map_handler_error(
    context: &DefaultAuthorizationContext,
    error: rocketmq_error::RocketMQError,
) -> AuthorizationError {
    AuthorizationError::PermissionDenied {
        subject: context.subject_key().unwrap_or("unknown").to_string(),
        resource: context.resource_key().unwrap_or_else(|| "unknown".to_string()),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::enums::subject_type::SubjectType;
    use crate::authentication::provider::authentication_metadata_provider::AuthenticationMetadataProvider;
    use crate::authorization::metadata_provider::AuthorizationMetadataProvider;

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

    #[tokio::test]
    async fn test_default_provider_initialization() {
        let mut provider = DefaultAuthorizationProvider::new();
        let config = AuthConfig::default();

        let result = provider.initialize(config);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_default_provider_authorize_missing_subject() {
        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(AuthConfig::default()).unwrap();

        let context = DefaultAuthorizationContext::default();
        let result = provider.authorize(&context).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AuthorizationError::InvalidContext(msg) => {
                assert!(msg.contains("subject"));
            }
            _ => panic!("Expected InvalidContext error"),
        }
    }

    #[tokio::test]
    async fn test_default_provider_authorize_missing_resource() {
        use crate::authentication::enums::subject_type::SubjectType;

        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(AuthConfig::default()).unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("user:test", SubjectType::User);

        let result = provider.authorize(&context).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AuthorizationError::InvalidContext(msg) => {
                assert!(msg.contains("resource"));
            }
            _ => panic!("Expected InvalidContext error"),
        }
    }

    #[tokio::test]
    async fn test_default_provider_authorize_missing_actions() {
        use crate::authentication::enums::subject_type::SubjectType;
        use crate::authorization::model::resource::Resource;

        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(AuthConfig::default()).unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("user:test", SubjectType::User);
        context.set_resource(Resource::of_topic("test-topic"));

        let result = provider.authorize(&context).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AuthorizationError::InvalidContext(msg) => {
                assert!(msg.contains("actions"));
            }
            _ => panic!("Expected InvalidContext error"),
        }
    }

    #[test]
    fn test_default_provider_default_construction() {
        let provider1 = DefaultAuthorizationProvider::new();
        let provider2 = DefaultAuthorizationProvider::default();

        // Both should be properly initialized
        assert!(provider1.config.is_none());
        assert!(provider2.config.is_none());
    }

    #[tokio::test]
    async fn test_default_provider_super_user_bypass() {
        use crate::authentication::enums::user_status::UserStatus;
        use crate::authentication::enums::user_type::UserType;
        use crate::authorization::model::resource::Resource;

        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(AuthConfig::default()).unwrap();

        let auth_provider = provider.authentication_metadata_provider().unwrap();
        let mut user = crate::authentication::model::user::User::of_with_type("alice", "secret", UserType::Super);
        user.set_user_status(UserStatus::Enable);
        auth_provider.create_user(user).await.unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("alice", SubjectType::User);
        context.set_resource(Resource::of_topic("test-topic"));
        context.set_actions(vec![rocketmq_common::common::action::Action::Pub]);
        context.set_source_ip("127.0.0.1");

        assert!(provider.authorize(&context).await.is_ok());
    }

    #[tokio::test]
    async fn test_default_provider_acl_authorization() {
        use crate::authentication::enums::user_status::UserStatus;
        use crate::authentication::enums::user_type::UserType;
        use crate::authorization::enums::decision::Decision;
        use crate::authorization::model::acl::Acl;
        use crate::authorization::model::policy::Policy;
        use crate::authorization::model::resource::Resource;

        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(AuthConfig::default()).unwrap();

        let auth_provider = provider.authentication_metadata_provider().unwrap();
        let mut user = crate::authentication::model::user::User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        auth_provider.create_user(user).await.unwrap();

        let acl_provider = provider.authorization_metadata_provider().unwrap();
        let resource = Resource::of_topic("test-topic");
        let acl = Acl::of(
            "alice",
            SubjectType::User,
            Policy::of(
                vec![resource.clone()],
                vec![rocketmq_common::common::action::Action::Pub],
                None,
                Decision::Allow,
            ),
        );
        acl_provider.create_acl(acl).await.unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("alice", SubjectType::User);
        context.set_resource(resource);
        context.set_actions(vec![rocketmq_common::common::action::Action::Pub]);
        context.set_source_ip("127.0.0.1");

        assert!(provider.authorize(&context).await.is_ok());
    }
}
