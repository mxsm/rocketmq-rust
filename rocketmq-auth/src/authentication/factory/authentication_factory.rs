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
use crate::authentication::manager::AuthenticationMetadataManagerImpl;
use crate::authentication::provider::AuthenticationMetadataProvider;
use crate::authentication::provider::AuthenticationProvider;
use crate::authentication::provider::DefaultAuthenticationProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::config::AuthConfig;

/// Global instance cache for authentication components
///
/// Thread-safe singleton cache using double-checked locking pattern.
/// Keys are formatted as "{PREFIX}_{config_name}" to avoid collisions.
static INSTANCE_CACHE: OnceLock<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>> = OnceLock::new();

/// Prefix for provider cache keys
const PROVIDER_PREFIX: &str = "PROVIDER_";

/// Prefix for metadata provider cache keys (currently unused in simplified version)
#[allow(dead_code)]
const METADATA_PROVIDER_PREFIX: &str = "METADATA_PROVIDER_";

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
        let key = format!("{}{}", PROVIDER_PREFIX, config.config_name);

        Self::compute_if_absent(&key, || {
            let provider = DefaultAuthenticationProvider::new();
            Ok(Arc::new(provider) as Arc<dyn Any + Send + Sync>)
        })
        .and_then(|any_arc| {
            any_arc
                .downcast::<DefaultAuthenticationProvider>()
                .map_err(|_| RocketMQError::illegal_argument("Failed to downcast provider"))
        })
    }

    /// Get authentication metadata provider (simplified version)
    ///
    /// **Note**: In this simplified Rust implementation, this always returns `None`.
    /// Metadata providers require async initialization, so they should be
    /// created directly by callers instead of through this factory.
    ///
    /// # Arguments
    ///
    /// * `_config` - Authentication configuration (unused)
    ///
    /// # Returns
    ///
    /// Always returns `Ok(None)` in this implementation.
    pub fn get_metadata_provider(
        _config: &AuthConfig,
    ) -> RocketMQResult<Option<Arc<dyn AuthenticationMetadataProvider>>> {
        // Metadata providers require async initialization, so they should be
        // created directly by callers instead of through this factory.
        Ok(None)
    }

    /// Get authentication metadata provider with service (simplified version)
    ///
    /// **Note**: In this simplified Rust implementation, this always returns `None`.
    /// Metadata providers require async initialization, so they should be
    /// created directly by callers instead of through this factory.
    ///
    /// # Arguments
    ///
    /// * `_config` - Authentication configuration (unused)
    /// * `_metadata_service` - Metadata service supplier (unused)
    ///
    /// # Returns
    ///
    /// Always returns `Ok(None)` in this implementation.
    pub fn get_metadata_provider_with_service(
        _config: &AuthConfig,
        _metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<Option<Arc<dyn AuthenticationMetadataProvider>>> {
        // Metadata providers require async initialization, so they should be
        // created directly by callers instead of through this factory.
        Ok(None)
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

        let mut cache_guard = cache
            .lock()
            .map_err(|e| RocketMQError::illegal_argument(format!("Cache lock error: {}", e)))?;

        if let Some(cached) = cache_guard.get(key) {
            return Ok(Arc::clone(cached));
        }

        let instance = factory_fn()?;
        cache_guard.insert(key.to_string(), Arc::clone(&instance));
        Ok(instance)
    }
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
        let provider = AuthenticationFactory::get_metadata_provider(&config);
        // In the simplified version, this always returns None because
        // metadata providers require async initialization
        assert!(provider.is_ok());
        assert!(provider.unwrap().is_none());
    }

    #[test]
    fn test_provider_caching() {
        let config = create_test_config("test_cache");
        let provider1 = AuthenticationFactory::get_provider(&config).unwrap();
        let provider2 = AuthenticationFactory::get_provider(&config).unwrap();
        assert!(Arc::ptr_eq(&provider1, &provider2));
    }
}
