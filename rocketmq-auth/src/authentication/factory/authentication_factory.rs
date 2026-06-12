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

//! Authentication Factory Implementation
//!
//! Central factory for creating and caching authentication components.
//! Provides singleton-like behavior for providers and metadata managers.
//!
//! # Example
//!
//! ```rust,ignore
//! use rocketmq_auth::authentication::factory::AuthenticationFactory;
//! use rocketmq_auth::config::AuthConfig;
//!
//! let config = AuthConfig::default();
//!
//! // Get cached provider instance
//! let provider = AuthenticationFactory::get_provider(&config)?;
//!
//! // Create authentication context from command
//! let context = AuthenticationFactory::new_context_from_command(&config, &command)?;
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use crate::authentication::evaluator::AuthenticationEvaluator;
use crate::authentication::manager::AuthenticationMetadataManagerImpl;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authentication::provider::AuthenticationProvider;
use crate::authentication::provider::DefaultAuthenticationProvider;
use crate::authentication::provider::LocalAuthenticationMetadataProvider;
use crate::authentication::strategy::AuthenticationStrategy;
use crate::authentication::strategy::StatefulAuthenticationStrategy;
use crate::authentication::strategy::StatelessAuthenticationStrategy;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::config::AuthConfig;

/// Global instance cache for authentication components
///
/// Thread-safe singleton cache using double-checked locking pattern.
/// Keys are formatted as "{PREFIX}_{config_name}" to avoid collisions.
static INSTANCE_CACHE: OnceLock<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>> = OnceLock::new();

/// Prefix for provider cache keys
const PROVIDER_PREFIX: &str = "PROVIDER_";

/// Prefix for metadata provider cache keys
const METADATA_PROVIDER_PREFIX: &str = "METADATA_PROVIDER_";

/// Prefix for evaluator cache keys
const EVALUATOR_PREFIX: &str = "EVALUATOR_";

/// Authentication factory for creating and managing authentication components
///
/// This factory provides centralized creation logic with caching for efficiency.
/// Instances are cached based on config name to avoid redundant creation.
///
/// # Thread Safety
///
/// All methods are thread-safe and can be called concurrently from multiple threads.
/// The factory uses `OnceLock` and `Mutex` to ensure safe concurrent access.
pub struct AuthenticationFactory;

impl AuthenticationFactory {
    /// Get or create an authentication provider
    ///
    /// Returns a cached `DefaultAuthenticationProvider` instance based on the config name.
    /// If no instance exists for this config, creates a new one and caches it.
    ///
    /// # Arguments
    ///
    /// * `config` - Authentication configuration
    ///
    /// # Returns
    ///
    /// * `Ok(Arc<DefaultAuthenticationProvider>)` - Cached or newly created provider
    /// * `Err(RocketMQError)` - If provider creation or caching fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = AuthConfig::default();
    /// let provider = AuthenticationFactory::get_provider(&config)?;
    /// ```
    pub fn get_provider(config: &AuthConfig) -> RocketMQResult<Arc<DefaultAuthenticationProvider>> {
        if !is_blank_or_supported(
            config.authentication_provider.as_str(),
            &["DefaultAuthenticationProvider", "default"],
        ) {
            return Err(RocketMQError::auth_config_invalid(
                "authenticationProvider",
                format!("Unsupported authenticationProvider: {}", config.authentication_provider),
            ));
        }

        let key = format!("{}{}", PROVIDER_PREFIX, config.config_name);

        Self::compute_if_absent(&key, || {
            let provider = new_initialized_default_provider(config.clone(), None)?;
            Ok(Arc::new(provider) as Arc<dyn Any + Send + Sync>)
        })
        .and_then(|any_arc| {
            any_arc
                .downcast::<DefaultAuthenticationProvider>()
                .map_err(|_| RocketMQError::illegal_argument("Failed to downcast provider"))
        })
    }

    /// Get authentication metadata provider
    ///
    /// Mirrors Java's factory behavior: a blank provider configuration returns
    /// `None`; a configured provider is initialized, cached by config name, and
    /// returned on later calls.
    ///
    /// # Arguments
    ///
    /// * `config` - Authentication configuration
    ///
    /// # Returns
    pub fn get_metadata_provider(
        config: &AuthConfig,
    ) -> RocketMQResult<Option<Arc<dyn AuthenticationMetadataProvider>>> {
        Self::get_metadata_provider_with_service(config, None)
    }

    /// Get authentication metadata provider with service
    ///
    /// # Arguments
    ///
    /// * `config` - Authentication configuration
    /// * `metadata_service` - Optional provider-specific metadata service
    ///
    /// # Returns
    pub fn get_metadata_provider_with_service(
        config: &AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<Option<Arc<dyn AuthenticationMetadataProvider>>> {
        let configured = config.authentication_metadata_provider.as_str();
        if configured.trim().is_empty() {
            return Ok(None);
        }
        if !is_supported(configured, &["LocalAuthenticationMetadataProvider", "local"]) {
            return Err(RocketMQError::auth_config_invalid(
                "authenticationMetadataProvider",
                format!("Unsupported authenticationMetadataProvider: {configured}"),
            ));
        }

        let key = format!("{}{}", METADATA_PROVIDER_PREFIX, config.config_name);
        Self::compute_if_absent(&key, || {
            let provider = new_initialized_local_metadata_provider(config.clone(), metadata_service)?;
            Ok(Arc::new(provider) as Arc<dyn Any + Send + Sync>)
        })
        .and_then(|any_arc| {
            any_arc
                .downcast::<LocalAuthenticationMetadataProvider>()
                .map_err(|_| RocketMQError::illegal_argument("Failed to downcast metadata provider"))
        })
        .map(|provider| Some(provider as Arc<dyn AuthenticationMetadataProvider>))
    }

    /// Create an authentication strategy matching the configured Java class name.
    ///
    /// Blank configuration uses `StatelessAuthenticationStrategy`, matching Java.
    /// The Rust implementation supports the built-in stateless and stateful
    /// strategies and returns a configuration error for unsupported class names.
    pub fn get_strategy(
        config: &AuthConfig,
        _metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<Box<dyn AuthenticationStrategy>> {
        let provider = Self::get_provider(config)?;
        let strategy = config.authentication_strategy.as_str();
        if strategy.trim().is_empty()
            || strategy.ends_with("StatelessAuthenticationStrategy")
            || strategy.eq_ignore_ascii_case("stateless")
        {
            return Ok(Box::new(StatelessAuthenticationStrategy::new(
                config.clone(),
                Some(provider),
            )));
        }
        if strategy.ends_with("StatefulAuthenticationStrategy") || strategy.eq_ignore_ascii_case("stateful") {
            return Ok(Box::new(StatefulAuthenticationStrategy::new(
                config.clone(),
                Some(provider),
            )));
        }
        Err(RocketMQError::auth_config_invalid(
            "authenticationStrategy",
            format!("Unsupported authenticationStrategy: {strategy}"),
        ))
    }

    /// Get or create a cached authentication evaluator.
    pub fn get_evaluator(
        config: &AuthConfig,
    ) -> RocketMQResult<Arc<AuthenticationEvaluator<Box<dyn AuthenticationStrategy>>>> {
        Self::get_evaluator_with_service(config, None)
    }

    /// Get or create a cached authentication evaluator with provider-specific service.
    pub fn get_evaluator_with_service(
        config: &AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<Arc<AuthenticationEvaluator<Box<dyn AuthenticationStrategy>>>> {
        let key = format!("{}{}", EVALUATOR_PREFIX, config.config_name);
        Self::compute_if_absent(&key, || {
            let strategy = Self::get_strategy(config, metadata_service)?;
            Ok(Arc::new(AuthenticationEvaluator::new(strategy)) as Arc<dyn Any + Send + Sync>)
        })
        .and_then(|any_arc| {
            any_arc
                .downcast::<AuthenticationEvaluator<Box<dyn AuthenticationStrategy>>>()
                .map_err(|_| RocketMQError::illegal_argument("Failed to downcast evaluator"))
        })
    }

    /// Create a new authentication metadata manager
    ///
    /// Unlike other factory methods, this always creates a new instance (no caching).
    /// The manager coordinates authentication and authorization metadata providers.
    ///
    /// # Type Parameters
    ///
    /// * `A` - Authentication metadata provider type
    /// * `B` - Authorization metadata provider type
    ///
    /// # Arguments
    ///
    /// * `_config` - Authentication configuration (currently unused but kept for API compatibility)
    /// * `auth_provider` - Optional authentication metadata provider
    /// * `authz_provider` - Optional authorization metadata provider
    ///
    /// # Returns
    ///
    /// A new `AuthenticationMetadataManagerImpl` instance
    pub fn get_metadata_manager<A, B>(
        _config: &AuthConfig,
        auth_provider: Option<A>,
        authz_provider: Option<B>,
    ) -> AuthenticationMetadataManagerImpl<A, B>
    where
        A: AuthenticationMetadataProvider + 'static,
        B: AuthorizationMetadataProvider + 'static,
    {
        AuthenticationMetadataManagerImpl::new(auth_provider, authz_provider)
    }

    /// Create a new authentication context from gRPC metadata
    ///
    /// # Arguments
    ///
    /// * `config` - Authentication configuration
    /// * `metadata` - gRPC metadata headers
    /// * `request` - Protocol buffer request message
    ///
    /// # Returns
    ///
    /// * `Ok(Some(context))` - Successfully created context
    /// * `Err(RocketMQError)` - If provider retrieval fails
    pub fn new_context_from_metadata(
        config: &AuthConfig,
        metadata: &HashMap<String, String>,
        request: Box<dyn Any + Send>,
    ) -> RocketMQResult<Option<DefaultAuthenticationContext>> {
        let provider = Self::get_provider(config)?;
        Ok(Some(
            <DefaultAuthenticationProvider as AuthenticationProvider>::new_context_from_metadata(
                &provider, metadata, request,
            ),
        ))
    }

    /// Create a new authentication context from a remoting command
    ///
    /// # Arguments
    ///
    /// * `config` - Authentication configuration
    /// * `command` - RocketMQ remoting command
    ///
    /// # Returns
    ///
    /// * `Ok(Some(context))` - Successfully created context
    /// * `Err(RocketMQError)` - If provider retrieval fails
    pub fn new_context_from_command(
        config: &AuthConfig,
        command: &RemotingCommand,
    ) -> RocketMQResult<Option<DefaultAuthenticationContext>> {
        let provider = Self::get_provider(config)?;
        Ok(Some(
            <DefaultAuthenticationProvider as AuthenticationProvider>::new_context_from_command(&provider, command),
        ))
    }

    /// Internal helper for compute-if-absent pattern with double-checked locking
    ///
    /// This implements the singleton pattern with lazy initialization and caching.
    /// Uses double-checked locking to avoid lock contention on cached values.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key (usually "{PREFIX}_{config_name}")
    /// * `factory_fn` - Factory function to create the instance if not cached
    ///
    /// # Returns
    ///
    /// * `Ok(Arc<dyn Any>)` - Cached or newly created instance
    /// * `Err(RocketMQError)` - If factory function fails or lock acquisition fails
    ///
    /// # Algorithm
    ///
    /// 1. Check cache without lock (fast path)
    /// 2. If not found, acquire lock
    /// 3. Double-check inside lock
    /// 4. If still not found, call factory_fn
    /// 5. Store in cache and return
    fn compute_if_absent<F>(key: &str, factory_fn: F) -> RocketMQResult<Arc<dyn Any + Send + Sync>>
    where
        F: FnOnce() -> RocketMQResult<Arc<dyn Any + Send + Sync>>,
    {
        let cache = INSTANCE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));

        {
            let cache_guard = cache
                .lock()
                .map_err(|e| RocketMQError::illegal_argument(format!("Cache lock error: {}", e)))?;
            if let Some(cached) = cache_guard.get(key) {
                return Ok(Arc::clone(cached));
            }
        }

        let instance = factory_fn()?;

        let mut cache_guard = cache
            .lock()
            .map_err(|e| RocketMQError::illegal_argument(format!("Cache lock error: {}", e)))?;
        if let Some(cached) = cache_guard.get(key) {
            return Ok(Arc::clone(cached));
        }

        cache_guard.insert(key.to_string(), Arc::clone(&instance));
        Ok(instance)
    }
}

impl AuthenticationStrategy for Box<dyn AuthenticationStrategy> {
    fn authenticate(
        &self,
        context: &dyn crate::authorization::context::authentication_context::AuthenticationContext,
    ) -> Result<(), rocketmq_error::AuthError> {
        self.as_ref().authenticate(context)
    }
}

fn new_initialized_default_provider(
    config: AuthConfig,
    metadata_service: Option<Arc<dyn Any + Send + Sync>>,
) -> RocketMQResult<DefaultAuthenticationProvider> {
    std::thread::spawn(move || {
        let mut provider = DefaultAuthenticationProvider::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                RocketMQError::auth_config_invalid(
                    "authenticationProvider",
                    format!("Failed to create initialization runtime: {error}"),
                )
            })?;
        runtime
            .block_on(provider.initialize(config, metadata_service))
            .map_err(|error| RocketMQError::auth_config_invalid("authenticationProvider", error.to_string()))?;
        Ok(provider)
    })
    .join()
    .map_err(|_| {
        RocketMQError::auth_config_invalid(
            "authenticationProvider",
            "DefaultAuthenticationProvider initialization panicked",
        )
    })?
}

fn new_initialized_local_metadata_provider(
    config: AuthConfig,
    metadata_service: Option<Arc<dyn Any + Send + Sync>>,
) -> RocketMQResult<LocalAuthenticationMetadataProvider> {
    std::thread::spawn(move || {
        let mut provider = LocalAuthenticationMetadataProvider::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                RocketMQError::auth_config_invalid(
                    "authenticationMetadataProvider",
                    format!("Failed to create initialization runtime: {error}"),
                )
            })?;
        runtime
            .block_on(provider.initialize(config, metadata_service))
            .map_err(|error| RocketMQError::auth_config_invalid("authenticationMetadataProvider", error.to_string()))?;
        Ok(provider)
    })
    .join()
    .map_err(|_| {
        RocketMQError::auth_config_invalid(
            "authenticationMetadataProvider",
            "LocalAuthenticationMetadataProvider initialization panicked",
        )
    })?
}

fn is_blank_or_supported(configured: &str, supported: &[&str]) -> bool {
    configured.trim().is_empty() || is_supported(configured, supported)
}

fn is_supported(configured: &str, supported: &[&str]) -> bool {
    supported
        .iter()
        .any(|value| configured.eq_ignore_ascii_case(value) || configured.ends_with(value))
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    fn create_test_config(config_name: &str) -> AuthConfig {
        AuthConfig {
            config_name: CheetahString::from_string(config_name.to_string()),
            ..AuthConfig::default()
        }
    }

    #[test]
    fn test_get_provider_default() {
        let config = create_test_config("test_provider_default");
        let provider = AuthenticationFactory::get_provider(&config);
        assert!(provider.is_ok());
    }

    #[test]
    fn test_get_provider_unsupported() {
        let mut config = create_test_config("test_provider_unsupported");
        config.authentication_provider = CheetahString::from_static_str("UnsupportedAuthenticationProvider");

        let error = match AuthenticationFactory::get_provider(&config) {
            Ok(_) => panic!("unsupported authentication provider should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("authenticationProvider"));
    }

    #[test]
    fn test_get_metadata_provider_not_configured() {
        let config = create_test_config("test_metadata_provider_none");
        let provider = AuthenticationFactory::get_metadata_provider(&config);
        assert!(provider.is_ok());
        assert!(provider.unwrap().is_none());
    }

    #[test]
    fn test_get_metadata_provider_local() {
        let mut config = create_test_config("test_metadata_provider_local");
        config.authentication_metadata_provider = CheetahString::from_static_str("LocalAuthenticationMetadataProvider");
        let provider1 = AuthenticationFactory::get_metadata_provider(&config).unwrap().unwrap();
        let provider2 = AuthenticationFactory::get_metadata_provider(&config).unwrap().unwrap();

        assert!(Arc::ptr_eq(&provider1, &provider2));
    }

    #[test]
    fn test_get_metadata_provider_unsupported() {
        let mut config = create_test_config("test_metadata_provider_unsupported");
        config.authentication_metadata_provider = CheetahString::from_static_str("UnsupportedMetadataProvider");

        let error = match AuthenticationFactory::get_metadata_provider(&config) {
            Ok(_) => panic!("unsupported metadata provider should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("authenticationMetadataProvider"));
    }

    #[test]
    fn test_provider_caching() {
        let config = create_test_config("test_cache");
        let provider1 = AuthenticationFactory::get_provider(&config).unwrap();
        let provider2 = AuthenticationFactory::get_provider(&config).unwrap();
        assert!(Arc::ptr_eq(&provider1, &provider2));
    }

    #[test]
    fn test_get_strategy_defaults_to_stateless() {
        let config = create_test_config("test_strategy_default");
        let strategy = AuthenticationFactory::get_strategy(&config, None).unwrap();
        let context = DefaultAuthenticationContext::new();

        assert!(strategy.authenticate(&context).is_ok());
    }

    #[test]
    fn test_get_strategy_supports_stateful_alias() {
        let mut config = create_test_config("test_strategy_stateful");
        config.authentication_strategy = CheetahString::from_static_str("stateful");
        let strategy = AuthenticationFactory::get_strategy(&config, None).unwrap();
        let context = DefaultAuthenticationContext::new();

        assert!(strategy.authenticate(&context).is_ok());
    }

    #[test]
    fn test_get_strategy_unsupported() {
        let mut config = create_test_config("test_strategy_unsupported");
        config.authentication_strategy = CheetahString::from_static_str("UnsupportedAuthenticationStrategy");

        let error = match AuthenticationFactory::get_strategy(&config, None) {
            Ok(_) => panic!("unsupported authentication strategy should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("authenticationStrategy"));
    }

    #[test]
    fn test_get_evaluator_caching() {
        let config = create_test_config("test_evaluator_cache");
        let evaluator1 = AuthenticationFactory::get_evaluator(&config).unwrap();
        let evaluator2 = AuthenticationFactory::get_evaluator(&config).unwrap();

        assert!(Arc::ptr_eq(&evaluator1, &evaluator2));
    }
}
