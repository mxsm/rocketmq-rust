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

//! Stateless authorization strategy implementation.
//!
//! This strategy performs authorization evaluation on every request without caching,
//! making it suitable for scenarios where authorization policies change frequently
//! or where memory constraints are important.

use std::any::Any;

use tracing::debug;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::strategy::abstract_authorization_strategy::AbstractAuthorizationStrategy;
use crate::authorization::strategy::abstract_authorization_strategy::AuthorizationStrategy;
use crate::authorization::strategy::abstract_authorization_strategy::StrategyResult;
use crate::config::AuthConfig;

/// Stateless authorization strategy.
///
/// This strategy delegates all authorization decisions directly to the underlying
/// provider without any caching. Each request is fully evaluated, ensuring that
/// the most up-to-date policies are always applied.
///
/// # Use Cases
///
/// - High-security environments where real-time policy enforcement is critical
/// - Systems with frequently changing authorization policies
/// - Deployments with limited memory resources
/// - Scenarios where authorization overhead is acceptable
///
/// # Trade-offs
///
/// **Advantages:**
/// - Always uses the latest policies
/// - No cache consistency issues
/// - Lower memory footprint
/// - Simpler implementation
///
/// **Disadvantages:**
/// - Higher latency per request
/// - Increased load on authorization provider
/// - No performance optimization for repeated requests
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::strategy::StatelessAuthorizationStrategy;
/// use rocketmq_auth::config::AuthConfig;
///
/// let config = AuthConfig::default();
/// let strategy = StatelessAuthorizationStrategy::new(config, None)?;
///
/// // Each evaluation queries the provider directly
/// let context = DefaultAuthorizationContext::of(subject, resource, action, ip);
/// strategy.evaluate(&context)?;
/// ```
pub struct StatelessAuthorizationStrategy {
    /// Base authorization strategy implementation
    base: AbstractAuthorizationStrategy,
}

impl StatelessAuthorizationStrategy {
    /// Creates a new stateless authorization strategy.
    ///
    /// # Arguments
    ///
    /// * `auth_config` - Authorization configuration
    /// * `metadata_service` - Optional metadata service for provider initialization
    ///
    /// # Returns
    ///
    /// A new `StatelessAuthorizationStrategy` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the base strategy initialization fails.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let config = AuthConfig::default();
    /// let strategy = StatelessAuthorizationStrategy::new(config, None)?;
    /// ```
    pub fn new(auth_config: AuthConfig, metadata_service: Option<Box<dyn Any + Send + Sync>>) -> StrategyResult<Self> {
        let base = AbstractAuthorizationStrategy::new(auth_config, metadata_service)?;
        debug!("StatelessAuthorizationStrategy initialized");
        Ok(Self { base })
    }

    /// Gets a reference to the base authorization strategy.
    pub fn base(&self) -> &AbstractAuthorizationStrategy {
        &self.base
    }
}

impl AuthorizationStrategy for StatelessAuthorizationStrategy {
    /// Evaluates authorization for the given context without caching.
    ///
    /// This method delegates directly to the base `do_evaluate` method,
    /// ensuring every request is fully evaluated.
    ///
    /// # Arguments
    ///
    /// * `context` - The authorization context to evaluate
    ///
    /// # Returns
    ///
    /// * `Ok(())` if authorization is granted
    /// * `Err(AuthorizationError)` if authorization is denied
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let strategy = StatelessAuthorizationStrategy::new(config, None)?;
    /// let context = DefaultAuthorizationContext::of(subject, resource, action, ip);
    /// strategy.evaluate(&context)?;
    /// ```
    fn evaluate(&self, context: &DefaultAuthorizationContext) -> StrategyResult<()> {
        debug!(
            "Stateless authorization evaluation for subject: {:?}",
            context.subject().map(|s| s.subject_key())
        );

        // Use async runtime to execute the async do_evaluate method
        tokio::runtime::Handle::current().block_on(self.base.do_evaluate(context))
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    fn create_test_config() -> AuthConfig {
        AuthConfig {
            config_name: CheetahString::from("test"),
            cluster_name: CheetahString::from("test-cluster"),
            auth_config_path: CheetahString::from("/tmp/auth"),
            authentication_enabled: false,
            authentication_provider: CheetahString::new(),
            authentication_metadata_provider: CheetahString::new(),
            authentication_strategy: CheetahString::new(),
            authentication_whitelist: CheetahString::new(),
            init_authentication_user: CheetahString::new(),
            inner_client_authentication_credentials: CheetahString::new(),
            authorization_enabled: true,
            authorization_provider: CheetahString::new(),
            authorization_metadata_provider: CheetahString::new(),
            authorization_strategy: CheetahString::new(),
            authorization_whitelist: CheetahString::from("10,11"),
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
        }
    }

    #[test]
    fn test_stateless_strategy_creation() {
        let config = create_test_config();
        let strategy = StatelessAuthorizationStrategy::new(config, None);
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_stateless_strategy_whitelist() {
        let config = create_test_config();
        let strategy = StatelessAuthorizationStrategy::new(config, None).unwrap();

        assert!(strategy.base().is_whitelisted("10"));
        assert!(strategy.base().is_whitelisted("11"));
        assert!(!strategy.base().is_whitelisted("12"));
    }

    #[test]
    fn test_stateless_evaluate_disabled() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

        let mut config = create_test_config();
        config.authorization_enabled = false;

        let strategy = StatelessAuthorizationStrategy::new(config, None).unwrap();
        let context = DefaultAuthorizationContext::default();

        let result = strategy.evaluate(&context);
        assert!(result.is_ok());
    }
}
