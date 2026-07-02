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

//! Abstract authorization strategy base implementation.
//!
//! This module provides a base implementation for authorization strategies, encapsulating
//! common logic such as whitelist checking, authorization provider initialization, and
//! error handling.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use tracing::debug;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::factory::AuthorizationFactory;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationProvider;
use crate::authorization::provider::DefaultAuthorizationProvider;
use crate::config::AuthConfig;

/// Result type for authorization strategy operations.
pub type StrategyResult<T> = Result<T, AuthorizationError>;

/// Trait defining the core authorization strategy behavior.
///
/// This trait must be implemented by all authorization strategies. It provides
/// the entry point for authorization evaluation.
pub trait AuthorizationStrategy: Send + Sync {
    /// Evaluates the authorization context to determine if access should be granted.
    ///
    /// # Arguments
    ///
    /// * `context` - The authorization context containing request information
    ///
    /// # Returns
    ///
    /// * `Ok(())` if authorization is granted
    /// * `Err(AuthorizationError)` if authorization is denied or evaluation fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let strategy = StatelessAuthorizationStrategy::new(config, None)?;
    /// let context = DefaultAuthorizationContext::of(subject, resource, action, source_ip);
    /// strategy.evaluate(&context)?;
    /// ```
    fn evaluate(&self, context: &DefaultAuthorizationContext) -> StrategyResult<()>;
}

/// Abstract base implementation for authorization strategies.
///
/// This struct provides common functionality for all authorization strategies:
/// - Whitelist checking
/// - Configuration management
/// - Default authorization logic
///
/// Concrete strategies should use this as a base and implement their specific
/// caching or stateful behavior on top.
///
/// # Architecture
///
/// ```text
/// AbstractAuthorizationStrategy
/// ├── auth_config: Configuration settings
/// └── authorization_whitelist: RPC codes that bypass authorization
/// ```
///
/// # Examples
///
/// ```rust,ignore
/// // Create a stateless strategy
/// let strategy = AbstractAuthorizationStrategy::new(auth_config, None)?;
///
/// // Evaluate authorization
/// let context = DefaultAuthorizationContext::of(subject, resource, action, source_ip);
/// strategy.do_evaluate(&context).await?;
/// ```
pub struct AbstractAuthorizationStrategy {
    /// Authorization configuration
    auth_config: AuthConfig,

    /// Set of RPC codes that bypass authorization (whitelist)
    authorization_whitelist: HashSet<String>,

    /// Provider used for concrete authorization checks.
    authorization_provider: Option<Arc<DefaultAuthorizationProvider>>,
}

impl AbstractAuthorizationStrategy {
    /// Creates a new `AbstractAuthorizationStrategy` instance.
    ///
    /// # Arguments
    ///
    /// * `auth_config` - Authorization configuration
    /// * `metadata_service` - Optional metadata service supplier for provider initialization
    ///
    /// # Returns
    ///
    /// Returns a new `AbstractAuthorizationStrategy` instance with initialized
    /// whitelist.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let config = AuthConfig::default();
    /// let strategy = AbstractAuthorizationStrategy::new(config, None)?;
    /// ```
    pub fn new(auth_config: AuthConfig, _metadata_service: Option<Box<dyn Any + Send + Sync>>) -> StrategyResult<Self> {
        // Parse and build whitelist from configuration
        let mut authorization_whitelist = HashSet::new();
        let whitelist_str = auth_config.authorization_whitelist.as_str();

        if !whitelist_str.is_empty() {
            for rpc_code in whitelist_str.split(',') {
                let trimmed = rpc_code.trim();
                if !trimmed.is_empty() {
                    authorization_whitelist.insert(trimmed.to_string());
                    debug!("Added RPC code '{}' to authorization whitelist", trimmed);
                }
            }
        }

        debug!(
            "AbstractAuthorizationStrategy initialized with {} whitelisted RPC codes",
            authorization_whitelist.len()
        );

        let authorization_provider = Some(
            AuthorizationFactory::get_provider(&auth_config)
                .map_err(|error| AuthorizationError::ConfigurationError(error.to_string()))?,
        );

        Ok(Self {
            auth_config,
            authorization_whitelist,
            authorization_provider,
        })
    }

    /// Core authorization evaluation logic shared by all strategies.
    ///
    /// This method encapsulates the common authorization flow:
    /// 1. Check if context is null
    /// 2. Check if authorization is enabled
    /// 3. Check whitelist
    /// 4. Fails closed when no concrete authorization provider is attached
    ///
    /// # Arguments
    ///
    /// * `context` - The authorization context to evaluate
    ///
    /// # Returns
    ///
    /// * `Ok(())` if authorization passes or is skipped (disabled/whitelisted)
    /// * `Err(AuthorizationError)` if authorization fails
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The authorization provider rejects the request
    /// - An internal error occurs during evaluation
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let strategy = AbstractAuthorizationStrategy::new(config, None)?;
    /// let context = DefaultAuthorizationContext::of(subject, resource, action, ip);
    ///
    /// // This will check all preconditions
    /// strategy.do_evaluate(&context).await?;
    /// ```
    pub async fn do_evaluate(&self, context: &DefaultAuthorizationContext) -> StrategyResult<()> {
        // Early return for null/empty context
        if context.subject().is_none() {
            debug!("Authorization skipped: context has no subject");
            return Ok(());
        }

        // Check if authorization is enabled
        if !self.auth_config.authorization_enabled {
            debug!("Authorization disabled in configuration, allowing access");
            return Ok(());
        }

        // Check whitelist
        if let Some(rpc_code) = context.rpc_code() {
            if self.authorization_whitelist.contains(rpc_code) {
                debug!("RPC code '{}' is whitelisted, allowing access", rpc_code);
                return Ok(());
            }
        }

        debug!(
            "Authorization evaluation for subject: {:?}, resource: {:?}, actions: {:?}",
            context.subject().map(|s| s.subject_key()),
            context.resource(),
            context.actions()
        );

        let provider = self.authorization_provider.as_ref().ok_or_else(|| {
            AuthorizationError::NotInitialized("authorization strategy does not have a concrete provider".to_owned())
        })?;
        provider.authorize(context).await
    }

    /// Gets a reference to the authorization configuration.
    pub fn auth_config(&self) -> &AuthConfig {
        &self.auth_config
    }

    /// Gets a reference to the whitelist set.
    pub fn authorization_whitelist(&self) -> &HashSet<String> {
        &self.authorization_whitelist
    }

    /// Returns true when a concrete provider is attached.
    pub fn has_authorization_provider(&self) -> bool {
        self.authorization_provider.is_some()
    }

    /// Checks if a given RPC code is whitelisted.
    ///
    /// # Arguments
    ///
    /// * `rpc_code` - The RPC code to check
    ///
    /// # Returns
    ///
    /// `true` if the RPC code is in the whitelist, `false` otherwise
    pub fn is_whitelisted(&self, rpc_code: &str) -> bool {
        self.authorization_whitelist.contains(rpc_code)
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::action::Action;

    use super::*;
    use crate::authentication::enums::subject_type::SubjectType;
    use crate::authorization::model::resource::Resource;

    fn create_test_config(enabled: bool, whitelist: &str) -> AuthConfig {
        AuthConfig {
            config_name: CheetahString::from("test"),
            cluster_name: CheetahString::from("test-cluster"),
            auth_config_path: CheetahString::from("/tmp/auth"),
            acl_file: CheetahString::new(),
            acl_file_watch_enabled: false,
            acl_file_watch_interval_millis: 5_000,
            authentication_enabled: false,
            authentication_provider: CheetahString::new(),
            authentication_metadata_provider: CheetahString::new(),
            authentication_strategy: CheetahString::new(),
            authentication_whitelist: CheetahString::new(),
            init_authentication_user: CheetahString::new(),
            inner_client_authentication_credentials: CheetahString::new(),
            signature_algorithm: Default::default(),
            request_timestamp_expired_millis: 0,
            authorization_enabled: enabled,
            authorization_provider: CheetahString::new(),
            authorization_metadata_provider: CheetahString::new(),
            authorization_strategy: CheetahString::new(),
            authorization_whitelist: CheetahString::from(whitelist),
            migrate_auth_from_v1_enabled: false,
            user_cache_max_num: 1000,
            user_cache_expired_second: 300,
            user_cache_refresh_second: 60,
            acl_cache_max_num: 1000,
            acl_cache_expired_second: 300,
            acl_cache_refresh_second: 60,
            stateful_authentication_cache_max_num: 1000,
            stateful_authentication_cache_expired_second: 300,
            stateful_authorization_cache_max_num: 1000,
            stateful_authorization_cache_expired_second: 300,
            stateful_authorization_cache_negative_enable: false,
        }
    }

    #[test]
    fn test_whitelist_parsing() {
        let config = create_test_config(true, "10,11,12");
        let strategy = AbstractAuthorizationStrategy::new(config, None).unwrap();

        assert_eq!(strategy.authorization_whitelist().len(), 3);
        assert!(strategy.is_whitelisted("10"));
        assert!(strategy.is_whitelisted("11"));
        assert!(strategy.is_whitelisted("12"));
        assert!(!strategy.is_whitelisted("13"));
    }

    #[test]
    fn test_whitelist_with_spaces() {
        let config = create_test_config(true, " 10 , 11 , 12 ");
        let strategy = AbstractAuthorizationStrategy::new(config, None).unwrap();

        assert_eq!(strategy.authorization_whitelist().len(), 3);
        assert!(strategy.is_whitelisted("10"));
        assert!(strategy.is_whitelisted("11"));
        assert!(strategy.is_whitelisted("12"));
    }

    #[test]
    fn test_empty_whitelist() {
        let config = create_test_config(true, "");
        let strategy = AbstractAuthorizationStrategy::new(config, None).unwrap();

        assert_eq!(strategy.authorization_whitelist().len(), 0);
        assert!(!strategy.is_whitelisted("10"));
    }

    #[test]
    fn test_disabled_authorization() {
        let config = create_test_config(false, "");
        let strategy = AbstractAuthorizationStrategy::new(config, None).unwrap();

        assert!(!strategy.auth_config().authorization_enabled);
    }

    #[test]
    fn test_provider_is_initialized() {
        let config = create_test_config(true, "");
        let strategy = AbstractAuthorizationStrategy::new(config, None).unwrap();

        assert!(strategy.has_authorization_provider());
    }

    #[tokio::test]
    async fn test_do_evaluate_disabled() {
        let config = create_test_config(false, "");
        let strategy = AbstractAuthorizationStrategy::new(config, None).unwrap();

        let context = DefaultAuthorizationContext::default();
        let result = strategy.do_evaluate(&context).await;

        // Should pass because authorization is disabled
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_do_evaluate_delegates_to_provider() {
        let config = create_test_config(true, "");
        let strategy = AbstractAuthorizationStrategy::new(config, None).unwrap();

        let context = DefaultAuthorizationContext::of(
            "alice",
            SubjectType::User,
            Resource::of_topic("topic-a"),
            Action::Get,
            "127.0.0.1",
        );
        let result = strategy.do_evaluate(&context).await;

        assert!(matches!(result, Err(AuthorizationError::PermissionDenied { .. })));
    }
}
