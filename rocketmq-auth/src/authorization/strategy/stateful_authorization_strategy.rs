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

//! Stateful authorization strategy with caching.
//!
//! This strategy caches authorization decisions for connection-based requests,
//! providing better performance for repeated authorization checks on the same
//! channel/connection.

use std::any::Any;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use tracing::debug;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::strategy::abstract_authorization_strategy::AbstractAuthorizationStrategy;
use crate::authorization::strategy::abstract_authorization_strategy::AuthorizationStrategy;
use crate::authorization::strategy::abstract_authorization_strategy::StrategyResult;
use crate::config::AuthConfig;

/// Cached authorization result.
#[derive(Clone, Debug)]
struct CachedAuthResult {
    /// Whether the authorization was granted
    granted: bool,
    /// The error if authorization was denied
    error: Option<String>,
    /// Timestamp when this entry was cached
    cached_at: Instant,
}

impl CachedAuthResult {
    fn new_granted() -> Self {
        Self {
            granted: true,
            error: None,
            cached_at: Instant::now(),
        }
    }

    fn new_denied(error: AuthorizationError) -> Self {
        Self {
            granted: false,
            error: Some(error.to_string()),
            cached_at: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// Stateful authorization strategy with caching.
///
/// This strategy caches authorization decisions based on channel ID, reducing
/// the overhead of repeated authorization checks for the same connection.
///
/// # Cache Key
///
/// The cache key is built from:
/// - Channel ID
/// - Subject key
/// - Resource key
/// - Actions (comma-separated)
/// - Source IP
///
/// Format: `{channel_id}#{subject_key}#{resource_key}#{actions}#{source_ip}`
///
/// # Cache Behavior
///
/// - **Cache Hit**: Returns cached result immediately
/// - **Cache Miss**: Evaluates authorization and caches the result
/// - **TTL**: Cached entries expire after configured duration
/// - **No Channel ID**: Bypasses cache and evaluates directly
///
/// # Use Cases
///
/// - Connection-based protocols (e.g., long-lived TCP connections)
/// - High-throughput scenarios with repeated authorization checks
/// - Systems where authorization policies are relatively stable
///
/// # Trade-offs
///
/// **Advantages:**
/// - Significantly reduced latency for cached requests
/// - Lower load on authorization provider
/// - Better performance for connection-based protocols
///
/// **Disadvantages:**
/// - Potential delay in policy updates (up to TTL)
/// - Higher memory footprint
/// - Cache consistency considerations
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_auth::authorization::strategy::StatefulAuthorizationStrategy;
/// use rocketmq_auth::config::AuthConfig;
///
/// let config = AuthConfig::default();
/// let strategy = StatefulAuthorizationStrategy::new(config, None)?;
///
/// // First evaluation: queries provider
/// let context = DefaultAuthorizationContext::of(subject, resource, action, ip);
/// context.set_channel_id("channel-123");
/// strategy.evaluate(&context)?; // Cache miss
///
/// // Second evaluation: uses cache
/// strategy.evaluate(&context)?; // Cache hit!
/// ```
pub struct StatefulAuthorizationStrategy {
    /// Base authorization strategy
    base: AbstractAuthorizationStrategy,

    /// Authorization cache (key -> result)
    auth_cache: Arc<RwLock<std::collections::HashMap<String, CachedAuthResult>>>,

    /// Cache TTL
    cache_ttl: Duration,

    /// Maximum cache size
    cache_max_size: usize,

    /// Request counter for periodic cleanup
    request_counter: AtomicUsize,
}

impl StatefulAuthorizationStrategy {
    /// Creates a new stateful authorization strategy with caching.
    ///
    /// # Arguments
    ///
    /// * `auth_config` - Authorization configuration
    /// * `metadata_service` - Optional metadata service for provider initialization
    ///
    /// # Returns
    ///
    /// A new `StatefulAuthorizationStrategy` instance.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let config = AuthConfig::default();
    /// let strategy = StatefulAuthorizationStrategy::new(config, None)?;
    /// ```
    pub fn new(auth_config: AuthConfig, metadata_service: Option<Box<dyn Any + Send + Sync>>) -> StrategyResult<Self> {
        let cache_ttl = Duration::from_secs(auth_config.stateful_authorization_cache_expired_second as u64);
        let cache_max_size = auth_config.stateful_authorization_cache_max_num as usize;

        let base = AbstractAuthorizationStrategy::new(auth_config, metadata_service)?;

        debug!(
            "StatefulAuthorizationStrategy initialized with TTL: {:?}, max size: {}",
            cache_ttl, cache_max_size
        );

        Ok(Self {
            base,
            auth_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
            cache_ttl,
            cache_max_size,
            request_counter: AtomicUsize::new(0),
        })
    }

    /// Builds a cache key from the authorization context.
    ///
    /// Format: `{channel_id}#{subject_key}#{resource_key}#{actions}#{source_ip}`
    fn build_key(&self, context: &DefaultAuthorizationContext) -> String {
        let channel_id = context.channel_id().unwrap_or("");
        let subject_key = context.subject().map(|s| s.subject_key()).unwrap_or_default();
        let resource_key = context.resource().map(|r| format!("{:?}", r)).unwrap_or_default();
        let actions = context
            .actions()
            .iter()
            .map(|a| format!("{:?}", a))
            .collect::<Vec<_>>()
            .join(",");
        let source_ip = context.source_ip().unwrap_or("");

        format!(
            "{}#{}#{}#{}#{}",
            channel_id, subject_key, resource_key, actions, source_ip
        )
    }

    /// Evicts expired entries from the cache.
    fn evict_expired(&self) {
        let mut cache = self.auth_cache.write().unwrap();
        let before_size = cache.len();

        cache.retain(|_, result| !result.is_expired(self.cache_ttl));

        let evicted = before_size - cache.len();
        if evicted > 0 {
            debug!("Evicted {} expired cache entries", evicted);
        }
    }

    /// Evicts oldest entries if cache is over capacity.
    fn evict_if_full(&self) {
        let mut cache = self.auth_cache.write().unwrap();

        if cache.len() >= self.cache_max_size {
            // Simple eviction: remove first 10% of entries
            let to_remove = (self.cache_max_size / 10).max(1);
            let keys_to_remove: Vec<String> = cache.keys().take(to_remove).cloned().collect();

            for key in keys_to_remove {
                cache.remove(&key);
            }

            debug!("Evicted {} entries to maintain cache size", to_remove);
        }
    }

    /// Gets a reference to the base strategy.
    pub fn base(&self) -> &AbstractAuthorizationStrategy {
        &self.base
    }

    /// Gets the current cache size.
    pub fn cache_size(&self) -> usize {
        self.auth_cache.read().unwrap().len()
    }

    /// Clears the entire cache.
    pub fn clear_cache(&self) {
        let mut cache = self.auth_cache.write().unwrap();
        cache.clear();
        debug!("Authorization cache cleared");
    }
}

impl AuthorizationStrategy for StatefulAuthorizationStrategy {
    /// Evaluates authorization with caching support.
    ///
    /// If the context has a channel ID, the result is cached. Subsequent
    /// requests with the same cache key will use the cached result until
    /// it expires.
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
    /// let strategy = StatefulAuthorizationStrategy::new(config, None)?;
    /// let mut context = DefaultAuthorizationContext::of(subject, resource, action, ip);
    /// context.set_channel_id("channel-123");
    /// strategy.evaluate(&context)?;
    /// ```
    fn evaluate(&self, context: &DefaultAuthorizationContext) -> StrategyResult<()> {
        // If no channel ID, bypass cache
        if context.channel_id().is_none() || context.channel_id().unwrap().is_empty() {
            debug!("No channel ID, bypassing cache");
            return tokio::runtime::Handle::current().block_on(self.base.do_evaluate(context));
        }

        let cache_key = self.build_key(context);

        // Check cache
        {
            let cache = self.auth_cache.read().unwrap();
            if let Some(cached_result) = cache.get(&cache_key) {
                if !cached_result.is_expired(self.cache_ttl) {
                    debug!("Cache hit for key: {}", cache_key);
                    return if cached_result.granted {
                        Ok(())
                    } else {
                        Err(AuthorizationError::InternalError(
                            cached_result
                                .error
                                .clone()
                                .unwrap_or_else(|| "Authorization denied (cached)".to_string()),
                        ))
                    };
                } else {
                    debug!("Cache entry expired for key: {}", cache_key);
                }
            }
        }

        // Cache miss - evaluate and cache result
        debug!("Cache miss for key: {}", cache_key);

        let result = tokio::runtime::Handle::current().block_on(self.base.do_evaluate(context));

        // Cache the result
        {
            self.evict_if_full();

            let mut cache = self.auth_cache.write().unwrap();
            let cached_result = match &result {
                Ok(()) => CachedAuthResult::new_granted(),
                Err(e) => CachedAuthResult::new_denied(e.clone()),
            };
            cache.insert(cache_key.clone(), cached_result);
            debug!("Cached result for key: {}", cache_key);
        }

        // Periodic cleanup every 100 requests
        let counter = self.request_counter.fetch_add(1, Ordering::Relaxed);
        if counter % 100 == 0 {
            self.evict_expired();
        }

        result
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
            authorization_whitelist: CheetahString::new(),
            migrate_auth_from_v1_enabled: false,
            user_cache_max_num: 1000,
            user_cache_expired_second: 300,
            user_cache_refresh_second: 60,
            acl_cache_max_num: 1000,
            acl_cache_expired_second: 300,
            acl_cache_refresh_second: 60,
            stateful_authentication_cache_max_num: 1000,
            stateful_authentication_cache_expired_second: 300,
            stateful_authorization_cache_max_num: 100,
            stateful_authorization_cache_expired_second: 60,
        }
    }

    #[test]
    fn test_stateful_strategy_creation() {
        let config = create_test_config();
        let strategy = StatefulAuthorizationStrategy::new(config, None);
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_cache_key_building() {
        let config = create_test_config();
        let strategy = StatefulAuthorizationStrategy::new(config, None).unwrap();

        let mut context = DefaultAuthorizationContext::default();
        context.set_channel_id("ch-123".to_string());
        context.set_source_ip("192.168.0.1".to_string());

        let key = strategy.build_key(&context);
        assert!(key.contains("ch-123"));
        assert!(key.contains("192.168.0.1"));
    }

    #[test]
    fn test_no_channel_id_bypasses_cache() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

        let mut config = create_test_config();
        config.authorization_enabled = false;

        let strategy = StatefulAuthorizationStrategy::new(config, None).unwrap();
        let context = DefaultAuthorizationContext::default();

        let result = strategy.evaluate(&context);
        assert!(result.is_ok());
        assert_eq!(strategy.cache_size(), 0); // Nothing cached
    }

    #[test]
    fn test_clear_cache() {
        let config = create_test_config();
        let strategy = StatefulAuthorizationStrategy::new(config, None).unwrap();

        strategy.clear_cache();
        assert_eq!(strategy.cache_size(), 0);
    }
}
