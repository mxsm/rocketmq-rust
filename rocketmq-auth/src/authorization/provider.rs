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

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::AuthorizationDecision;

use crate::authentication::provider::LocalAuthenticationMetadataProvider;
use crate::authorization::builder::default_authorization_context_builder::DefaultAuthorizationContextBuilder;
use crate::authorization::builder::AuthorizationContextBuilder;
use crate::authorization::chain::AclAuthorizationHandler;
use crate::authorization::chain::AuthorizationHandler;
use crate::authorization::chain::UserAuthorizationHandler;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use crate::config::AuthConfig;
use crate::runtime::ProviderRegistry;
use crate::AuthFailureKind;
use crate::AuthMetrics;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;
use crate::RemotingAuthContext;

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
/// - **Error handling**: All failures are expressed through `AuthServiceError`
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::{AuthorizationProvider, AuthServiceResult};
/// use rocketmq_auth::DefaultAuthorizationContext;
/// use rocketmq_auth::AuthConfig;
///
/// struct MyAuthProvider;
///
/// impl AuthorizationProvider for MyAuthProvider {
///     fn initialize(&mut self, config: AuthConfig) -> AuthServiceResult<()> {
///         // Initialize provider with configuration
///         Ok(())
///     }
///
///     async fn authorize(
///         &self,
///         context: &DefaultAuthorizationContext,
///     ) -> AuthServiceResult<AuthorizationDecision> {
///         // Implement authorization logic
///         Ok(AuthorizationDecision::Allow)
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
    /// Returns `AuthFailureKind::InvalidConfiguration` if configuration is invalid
    /// or initialization fails.
    fn initialize(&mut self, config: AuthConfig) -> AuthServiceResult<()>;

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
    /// Returns `AuthFailureKind::InvalidConfiguration` if initialization fails.
    fn initialize_with_metadata(
        &mut self,
        config: AuthConfig,
        #[allow(unused_variables)] metadata_service: Option<Box<dyn std::any::Any + Send + Sync>>,
    ) -> AuthServiceResult<()> {
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
    /// - `Ok(AuthorizationDecision::Allow)` if authorization succeeds
    /// - `Ok(AuthorizationDecision::Deny(_))` if policy denies the request
    /// - `Err(AuthServiceError)` if no decision can be made
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
    /// match provider.authorize(&context).await? {
    ///     AuthorizationDecision::Allow => proceed(),
    ///     AuthorizationDecision::Deny(reason) => reject(reason),
    /// }
    /// ```
    async fn authorize(&self, context: &DefaultAuthorizationContext) -> AuthServiceResult<AuthorizationDecision>;

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
    /// Returns `AuthFailureKind::InvalidInput` if context cannot be constructed.
    #[allow(unused_variables)]
    fn new_contexts_from_grpc_metadata(
        &self,
        metadata: &dyn std::any::Any,
        message: &dyn std::any::Any,
    ) -> AuthServiceResult<Vec<DefaultAuthorizationContext>> {
        // Default implementation returns empty list (no-op)
        Ok(Vec::new())
    }

    /// Create authorization contexts from trusted remoting ingress facts and command.
    ///
    /// Parses typed ingress facts and RocketMQ remoting command
    /// to construct authorization contexts for TCP-based protocols.
    ///
    /// # Arguments
    /// * `auth_context` - Trusted, read-only source and session facts
    /// * `command` - RocketMQ remoting command containing request code and data
    ///
    /// # Returns
    /// List of authorization contexts to be evaluated.
    ///
    /// # Errors
    /// Returns `AuthFailureKind::InvalidInput` if context cannot be constructed.
    #[allow(unused_variables)]
    fn new_contexts_from_remoting_command(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
    ) -> AuthServiceResult<Vec<DefaultAuthorizationContext>> {
        // Default implementation returns empty list (no-op)
        Ok(Vec::new())
    }
}

#[cfg(test)]
struct NoopAuthorizationProvider;

#[cfg(test)]
impl NoopAuthorizationProvider {
    fn new() -> Self {
        Self
    }
}

#[cfg(test)]
impl Default for NoopAuthorizationProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl AuthorizationProvider for NoopAuthorizationProvider {
    fn initialize(&mut self, _config: AuthConfig) -> AuthServiceResult<()> {
        // No initialization needed
        Ok(())
    }

    async fn authorize(&self, _context: &DefaultAuthorizationContext) -> AuthServiceResult<AuthorizationDecision> {
        // Always allow
        Ok(AuthorizationDecision::Allow)
    }

    fn new_contexts_from_grpc_metadata(
        &self,
        _metadata: &dyn std::any::Any,
        _message: &dyn std::any::Any,
    ) -> AuthServiceResult<Vec<DefaultAuthorizationContext>> {
        // Return empty contexts (no authorization needed)
        Ok(Vec::new())
    }

    fn new_contexts_from_remoting_command(
        &self,
        _auth_context: &RemotingAuthContext,
        _command: &RemotingCommand,
    ) -> AuthServiceResult<Vec<DefaultAuthorizationContext>> {
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
/// use rocketmq_auth::DefaultAuthorizationProvider;
/// use std::sync::Arc;
///
/// let mut provider = DefaultAuthorizationProvider::new();
/// provider.initialize(config)?;
///
/// // Authorize a request
/// match provider.authorize(&context).await? {
///     AuthorizationDecision::Allow => proceed(),
///     AuthorizationDecision::Deny(reason) => reject(reason),
/// }
/// ```
pub struct DefaultAuthorizationProvider {
    /// Authorization configuration
    config: Option<AuthConfig>,

    /// Metadata service supplier (reserved for future external providers)
    metadata_service: Option<Box<dyn std::any::Any + Send + Sync>>,

    authentication_metadata_provider: Option<Arc<LocalAuthenticationMetadataProvider>>,
    authorization_metadata_provider: Option<Arc<LocalAuthorizationMetadataProvider>>,
    context_builder: Option<DefaultAuthorizationContextBuilder>,
    metrics: AuthMetrics,
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
            metrics: AuthMetrics::default(),
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
    ) -> AuthServiceResult<()> {
        self.config = Some(config.clone());
        self.metadata_service = None;
        self.context_builder = Some(DefaultAuthorizationContextBuilder::new(config));
        self.authentication_metadata_provider = Some(provider_registry.authentication_metadata_provider());
        self.authorization_metadata_provider = Some(provider_registry.authorization_metadata_provider());
        self.metrics = provider_registry.metrics();
        Ok(())
    }

    /// Audit an authorization outcome without recording request identities or ACL data.
    ///
    /// Only closed decision categories and the action count are emitted. Subject,
    /// resource, source address, credentials, and operational error details remain
    /// available to typed diagnostics rather than tracing fields.
    fn audit_log(&self, context: &DefaultAuthorizationContext, result: &AuthServiceResult<AuthorizationDecision>) {
        use tracing::debug;
        use tracing::info;
        use tracing::warn;

        let action_count = context.actions().len();

        match result {
            Ok(AuthorizationDecision::Allow) => {
                debug!(outcome = "allow", action_count, "authorization decision")
            }
            Ok(AuthorizationDecision::Deny(denial)) => info!(
                outcome = "deny",
                denial = ?denial,
                action_count,
                "authorization decision"
            ),
            Err(_) => warn!(outcome = "error", action_count, "authorization evaluation failed"),
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
    fn initialize(&mut self, config: AuthConfig) -> AuthServiceResult<()> {
        self.initialize_with_metadata(config, None)
    }

    fn initialize_with_metadata(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Box<dyn std::any::Any + Send + Sync>>,
    ) -> AuthServiceResult<()> {
        use tracing::debug;

        debug!("Initializing DefaultAuthorizationProvider");
        self.config = Some(config.clone());
        self.metadata_service = metadata_service;
        self.context_builder = Some(DefaultAuthorizationContextBuilder::new(config.clone()));

        let authentication_metadata_provider =
            LocalAuthenticationMetadataProvider::with_config(&config).map_err(|source| {
                AuthServiceError::with_source(AuthOperation::InitializeProvider, AuthFailureKind::Unavailable, source)
            })?;
        self.authentication_metadata_provider = Some(Arc::new(authentication_metadata_provider));

        let mut authorization_metadata_provider = LocalAuthorizationMetadataProvider::new();
        authorization_metadata_provider.initialize(config, None)?;
        self.authorization_metadata_provider = Some(Arc::new(authorization_metadata_provider));
        self.metrics = AuthMetrics::default();

        Ok(())
    }

    async fn authorize(&self, context: &DefaultAuthorizationContext) -> AuthServiceResult<AuthorizationDecision> {
        use tracing::debug;
        use tracing::warn;

        // Validate context
        if context.subject_key().is_none() {
            warn!("Authorization context missing subject");
            self.metrics.record_authorization_result(false);
            return Err(AuthServiceError::invalid_context(
                "Missing subject in authorization context".to_string(),
            ));
        }

        if context.resource().is_none() {
            warn!("Authorization context missing resource");
            self.metrics.record_authorization_result(false);
            return Err(AuthServiceError::invalid_context(
                "Missing resource in authorization context".to_string(),
            ));
        }

        if context.actions().is_empty() {
            warn!("Authorization context has no actions");
            self.metrics.record_authorization_result(false);
            return Err(AuthServiceError::invalid_context(
                "No actions specified in authorization context".to_string(),
            ));
        }

        debug!(
            action_count = context.actions().len(),
            "authorization evaluation started"
        );

        let authentication_metadata_provider = self
            .authentication_metadata_provider
            .as_ref()
            .ok_or_else(|| AuthServiceError::not_initialized("Authentication metadata provider is not configured"))?;
        let authorization_metadata_provider = self
            .authorization_metadata_provider
            .as_ref()
            .ok_or_else(|| AuthServiceError::not_initialized("Authorization metadata provider is not configured"))?;

        let result: AuthServiceResult<AuthorizationDecision> = async {
            let user_handler = UserAuthorizationHandler::new(authentication_metadata_provider.clone());
            match user_handler.authorize_subject(context).await? {
                Some(decision) => Ok(decision),
                None => {
                    let acl_handler = AclAuthorizationHandler::new(authorization_metadata_provider.clone());
                    acl_handler.handle(context).await
                }
            }
        }
        .await;

        self.audit_log(context, &result);
        self.metrics
            .record_authorization_result(matches!(&result, Ok(AuthorizationDecision::Allow)));
        result
    }

    fn new_contexts_from_remoting_command(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
    ) -> AuthServiceResult<Vec<DefaultAuthorizationContext>> {
        let builder = self
            .context_builder
            .as_ref()
            .ok_or_else(|| AuthServiceError::not_initialized("Authorization context builder is not configured"))?;
        builder.build_from_remoting(auth_context, command)
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use tempfile::TempDir;

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
        assert_eq!(
            provider
                .authorize(&DefaultAuthorizationContext::default())
                .await
                .unwrap(),
            AuthorizationDecision::Allow
        );
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

        let error = result.unwrap_err();
        assert_eq!(error.operation(), AuthOperation::BuildContext);
        assert_eq!(error.kind(), AuthFailureKind::InvalidInput);
    }

    #[tokio::test]
    async fn test_default_provider_authorize_missing_resource() {
        use crate::authentication::enums::subject_type::SubjectType;

        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(AuthConfig::default()).unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("user:test", SubjectType::User);

        let result = provider.authorize(&context).await;

        let error = result.unwrap_err();
        assert_eq!(error.operation(), AuthOperation::BuildContext);
        assert_eq!(error.kind(), AuthFailureKind::InvalidInput);
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

        let error = result.unwrap_err();
        assert_eq!(error.operation(), AuthOperation::BuildContext);
        assert_eq!(error.kind(), AuthFailureKind::InvalidInput);
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
        context.set_actions(vec![rocketmq_security_api::Action::Pub]);
        context.set_source_ip("127.0.0.1");

        assert_eq!(
            provider.authorize(&context).await.unwrap(),
            AuthorizationDecision::Allow
        );
    }

    #[tokio::test]
    async fn default_provider_loads_persisted_authentication_users() {
        use crate::authentication::enums::user_status::UserStatus;
        use crate::authentication::enums::user_type::UserType;
        use crate::authorization::model::resource::Resource;

        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from_string(temp.path().join("auth.yml").to_string_lossy().into_owned()),
            ..AuthConfig::default()
        };

        let mut seed_provider = LocalAuthenticationMetadataProvider::new();
        seed_provider.initialize(config.clone(), None).await.unwrap();
        let mut user = crate::authentication::model::user::User::of_with_type("persisted", "secret", UserType::Super);
        user.set_user_status(UserStatus::Enable);
        seed_provider.create_user(user).await.unwrap();

        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(config).unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("persisted", SubjectType::User);
        context.set_resource(Resource::of_topic("test-topic"));
        context.set_actions(vec![rocketmq_security_api::Action::Pub]);
        context.set_source_ip("127.0.0.1");

        assert_eq!(
            provider.authorize(&context).await.unwrap(),
            AuthorizationDecision::Allow
        );
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
                vec![rocketmq_security_api::Action::Pub],
                None,
                Decision::Allow,
            ),
        );
        acl_provider.create_acl(acl).await.unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("alice", SubjectType::User);
        context.set_resource(resource);
        context.set_actions(vec![rocketmq_security_api::Action::Pub]);
        context.set_source_ip("127.0.0.1");

        assert_eq!(
            provider.authorize(&context).await.unwrap(),
            AuthorizationDecision::Allow
        );
    }

    #[tokio::test]
    async fn test_default_provider_unknown_user_is_subject_denial() {
        use crate::authorization::model::resource::Resource;
        use rocketmq_security_api::AuthorizationDenial;

        let mut provider = DefaultAuthorizationProvider::new();
        provider.initialize(AuthConfig::default()).unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_subject("missing-user", SubjectType::User);
        context.set_resource(Resource::of_topic("test-topic"));
        context.set_actions(vec![rocketmq_security_api::Action::Pub]);
        context.set_source_ip("127.0.0.1");

        assert_eq!(
            provider.authorize(&context).await.unwrap(),
            AuthorizationDecision::Deny(AuthorizationDenial::SubjectUnknown)
        );
    }
}
