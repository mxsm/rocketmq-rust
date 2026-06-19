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

//! Stateful authentication strategy with caching support.
//!
//! This strategy caches authentication results per channel/user combination
//! to avoid redundant authentication checks for the same connection.

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;
use rocketmq_error::AuthError;
use rocketmq_error::RocketMQResult;

use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use crate::authentication::provider::AuthenticationProvider;
use crate::authentication::strategy::abstract_authentication_strategy::AbstractAuthenticationStrategy;
use crate::authentication::strategy::authentication_strategy::AuthenticationStrategy;
use crate::authentication::strategy::block_on_authentication_provider;
use crate::authorization::context::authentication_context::AuthenticationContext;
use crate::config::AuthConfig;
use crate::AuthMetrics;

const POUND: &str = "#";

/// Authentication cache entry storing the result of an authentication attempt.
#[derive(Clone, Debug)]
struct AuthCacheEntry {
    /// Whether authentication succeeded
    success: bool,
    /// Error message if authentication failed
    error_message: Option<String>,
}

impl AuthCacheEntry {
    fn success() -> Self {
        Self {
            success: true,
            error_message: None,
        }
    }

    fn failure(error: String) -> Self {
        Self {
            success: false,
            error_message: Some(error),
        }
    }
}

/// Stateful authentication strategy with caching.
///
/// Caches authentication results per channel ID and username combination
/// to avoid redundant authentication checks for the same connection/user.
///
/// - Configurable cache size and expiration
/// - Falls back to stateless evaluation if channel ID is not available
pub struct StatefulAuthenticationStrategy<P>
where
    P: AuthenticationProvider<Context = DefaultAuthenticationContext>,
{
    /// Authentication configuration
    auth_config: AuthConfig,
    /// Whitelist of RPC codes that bypass authentication
    authentication_white_set: HashSet<String>,
    /// Authentication provider
    authentication_provider: Option<Arc<P>>,
    /// Cache for authentication results
    auth_cache: Cache<String, AuthCacheEntry>,
    /// Shared ACL snapshot generation. A successful ACL reload advances this value.
    acl_generation: Arc<AtomicU64>,
    /// Last generation observed by this strategy, used to drop old entries promptly.
    last_seen_acl_generation: AtomicU64,
    metrics: AuthMetrics,
}

impl<P> StatefulAuthenticationStrategy<P>
where
    P: AuthenticationProvider<Context = DefaultAuthenticationContext> + Send + Sync + 'static,
{
    /// Create a new stateful authentication strategy.
    pub fn new(auth_config: AuthConfig, provider: Option<Arc<P>>) -> Self {
        Self::new_with_acl_generation(auth_config, provider, Arc::new(AtomicU64::new(0)))
    }

    /// Create a new stateful authentication strategy bound to a shared ACL generation counter.
    pub fn new_with_acl_generation(
        auth_config: AuthConfig,
        provider: Option<Arc<P>>,
        acl_generation: Arc<AtomicU64>,
    ) -> Self {
        Self::new_with_acl_generation_and_metrics(auth_config, provider, acl_generation, AuthMetrics::default())
    }

    pub fn new_with_acl_generation_and_metrics(
        auth_config: AuthConfig,
        provider: Option<Arc<P>>,
        acl_generation: Arc<AtomicU64>,
        metrics: AuthMetrics,
    ) -> Self {
        let mut authentication_white_set = HashSet::new();

        let whitelist = auth_config.authentication_whitelist.to_string();
        if !whitelist.is_empty() {
            for rpc_code in whitelist.split(',') {
                let trimmed = rpc_code.trim();
                if !trimmed.is_empty() {
                    authentication_white_set.insert(trimmed.to_string());
                }
            }
        }

        let auth_cache = Cache::builder()
            .time_to_live(Duration::from_secs(
                auth_config.stateful_authentication_cache_expired_second as u64,
            ))
            .max_capacity(auth_config.stateful_authentication_cache_max_num as u64)
            .build();
        let initial_generation = acl_generation.load(Ordering::Acquire);

        Self {
            auth_config,
            authentication_white_set,
            authentication_provider: provider,
            auth_cache,
            acl_generation,
            last_seen_acl_generation: AtomicU64::new(initial_generation),
            metrics,
        }
    }

    /// Build cache key from authentication context.
    ///
    /// Key format:
    /// - `{generation}#{channel_id}` if username is not available
    /// - `{generation}#{channel_id}#{username}` if username is available
    fn build_cache_key(&self, context: &DefaultAuthenticationContext) -> Result<String, AuthError> {
        let channel_id = context
            .base
            .channel_id()
            .ok_or_else(|| AuthError::AuthenticationFailed("Channel ID is required for stateful auth".to_string()))?;
        let generation = self.refresh_acl_generation();

        if let Some(username) = context.username() {
            Ok(format!("{}{}{}{}{}", generation, POUND, channel_id, POUND, username))
        } else {
            Ok(format!("{}{}{}", generation, POUND, channel_id))
        }
    }

    fn refresh_acl_generation(&self) -> u64 {
        let current = self.acl_generation.load(Ordering::Acquire);
        let previous = self.last_seen_acl_generation.load(Ordering::Acquire);
        if previous != current
            && self
                .last_seen_acl_generation
                .compare_exchange(previous, current, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.auth_cache.invalidate_all();
            self.metrics.record_cache_invalidation();
        }
        current
    }

    /// Perform actual authentication without caching.
    fn do_authenticate_internal(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError> {
        if !self.auth_config.authentication_enabled {
            return Ok(());
        }

        let default_context = context
            .as_any()
            .downcast_ref::<DefaultAuthenticationContext>()
            .ok_or_else(|| {
                AuthError::AuthenticationFailed(
                    "Stateful authentication requires DefaultAuthenticationContext".to_string(),
                )
            })?;

        if let Some(rpc_code) = default_context.base.rpc_code() {
            if self.is_whitelisted(rpc_code.as_str()) {
                return Ok(());
            }
        }

        let provider = match &self.authentication_provider {
            Some(p) => p,
            None => return Ok(()),
        };

        block_on_authentication_provider(provider.as_ref(), default_context)
    }

    pub fn provider(&self) -> Option<&P> {
        self.authentication_provider.as_deref()
    }
}

#[allow(async_fn_in_trait)]
impl<P> AbstractAuthenticationStrategy for StatefulAuthenticationStrategy<P>
where
    P: AuthenticationProvider<Context = DefaultAuthenticationContext> + Send + Sync + 'static,
{
    fn auth_config(&self) -> &AuthConfig {
        &self.auth_config
    }

    fn authentication_white_set(&self) -> &HashSet<String> {
        &self.authentication_white_set
    }

    fn authentication_provider(&self) -> Option<&dyn Any> {
        self.authentication_provider.as_ref().map(|p| p.as_ref() as &dyn Any)
    }

    async fn authenticate_with_provider<C: AuthenticationContext>(
        &self,
        _provider: &dyn Any,
        _context: &C,
    ) -> RocketMQResult<()> {
        Err(rocketmq_error::RocketMQError::authentication_failed(
            "Use authenticate() method for stateful authentication",
        ))
    }
}

impl<P> AuthenticationStrategy for StatefulAuthenticationStrategy<P>
where
    P: AuthenticationProvider<Context = DefaultAuthenticationContext> + Send + Sync + 'static,
{
    fn authenticate(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError> {
        let default_context = context
            .as_any()
            .downcast_ref::<DefaultAuthenticationContext>()
            .ok_or_else(|| {
                AuthError::AuthenticationFailed(
                    "Stateful authentication requires DefaultAuthenticationContext".to_string(),
                )
            })?;

        if default_context.base.channel_id().is_none() {
            return self.do_authenticate_internal(context);
        }

        let cache_key = self.build_cache_key(default_context)?;

        let result = if let Some(result) = self.auth_cache.get(&cache_key) {
            self.metrics.record_cache_hit();
            result
        } else {
            self.metrics.record_cache_miss();
            self.auth_cache
                .try_get_with(cache_key, || -> Result<AuthCacheEntry, AuthError> {
                    match self.do_authenticate_internal(context) {
                        Ok(()) => Ok(AuthCacheEntry::success()),
                        Err(e) => Ok(AuthCacheEntry::failure(e.to_string())),
                    }
                })
                .map_err(|e| AuthError::AuthenticationFailed(format!("Cache operation failed: {}", e)))?
        };

        if !result.success {
            return Err(AuthError::AuthenticationFailed(
                result
                    .error_message
                    .unwrap_or_else(|| "Authentication failed".to_string()),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;

    use super::*;
    use crate::authentication::provider::AuthenticationProvider;
    use crate::authentication::provider::DefaultAuthenticationProvider;

    struct CountingAuthenticationProvider {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        should_succeed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl AuthenticationProvider for CountingAuthenticationProvider {
        type Context = DefaultAuthenticationContext;

        async fn initialize(
            &mut self,
            _config: AuthConfig,
            _metadata_service: Option<Arc<dyn Any + Send + Sync>>,
        ) -> RocketMQResult<()> {
            Ok(())
        }

        async fn authenticate(&self, _context: &Self::Context) -> RocketMQResult<()> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.should_succeed.load(std::sync::atomic::Ordering::SeqCst) {
                Ok(())
            } else {
                Err(RocketMQError::authentication_failed("denied by test provider"))
            }
        }

        fn new_context_from_metadata(
            &self,
            _metadata: &std::collections::HashMap<String, String>,
            _request: Box<dyn Any + Send>,
        ) -> Self::Context {
            DefaultAuthenticationContext::new()
        }

        fn new_context_from_command(
            &self,
            _command: &rocketmq_remoting::protocol::remoting_command::RemotingCommand,
        ) -> Self::Context {
            DefaultAuthenticationContext::new()
        }
    }

    #[test]
    fn test_stateful_strategy_creation() {
        let config = AuthConfig::default();
        let provider = Arc::new(DefaultAuthenticationProvider::new());

        let strategy = StatefulAuthenticationStrategy::new(config, Some(provider));

        assert!(strategy.provider().is_some());
        assert!(strategy.authentication_white_set().is_empty());
    }

    #[test]
    fn test_build_cache_key_with_username() {
        let config = AuthConfig::default();
        let strategy: StatefulAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatefulAuthenticationStrategy::new(config, None);

        let mut context = DefaultAuthenticationContext::new();
        context.base.set_channel_id(Some(CheetahString::from("channel-123")));
        context.set_username(CheetahString::from("user-456"));

        let key = strategy.build_cache_key(&context).unwrap();
        assert_eq!(key, "0#channel-123#user-456");
    }

    #[test]
    fn test_build_cache_key_without_username() {
        let config = AuthConfig::default();
        let strategy: StatefulAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatefulAuthenticationStrategy::new(config, None);

        let mut context = DefaultAuthenticationContext::new();
        context.base.set_channel_id(Some(CheetahString::from("channel-789")));

        let key = strategy.build_cache_key(&context).unwrap();
        assert_eq!(key, "0#channel-789");
    }

    #[test]
    fn test_build_cache_key_includes_acl_generation() {
        let config = AuthConfig::default();
        let acl_generation = Arc::new(AtomicU64::new(7));
        let strategy: StatefulAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatefulAuthenticationStrategy::new_with_acl_generation(config, None, acl_generation.clone());

        let mut context = DefaultAuthenticationContext::new();
        context.base.set_channel_id(Some(CheetahString::from("channel-789")));

        let key = strategy.build_cache_key(&context).unwrap();
        assert_eq!(key, "7#channel-789");

        acl_generation.store(8, Ordering::Release);
        let key = strategy.build_cache_key(&context).unwrap();
        assert_eq!(key, "8#channel-789");
    }

    #[test]
    fn test_authentication_disabled() {
        let config = AuthConfig {
            authentication_enabled: false,
            ..Default::default()
        };

        let strategy: StatefulAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatefulAuthenticationStrategy::new(config, None);

        let context = DefaultAuthenticationContext::new();
        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cache_expiration_config() {
        let config = AuthConfig {
            stateful_authentication_cache_max_num: 500,
            stateful_authentication_cache_expired_second: 30,
            ..Default::default()
        };

        let strategy: StatefulAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatefulAuthenticationStrategy::new(config, None);

        assert_eq!(strategy.auth_config.stateful_authentication_cache_max_num, 500);
        assert_eq!(strategy.auth_config.stateful_authentication_cache_expired_second, 30);
    }

    #[test]
    fn test_whitelist_parsing() {
        let config = AuthConfig {
            authentication_whitelist: "SEND_MESSAGE,PULL_MESSAGE".into(),
            ..Default::default()
        };

        let strategy: StatefulAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatefulAuthenticationStrategy::new(config, None);

        assert!(strategy.authentication_white_set().contains("SEND_MESSAGE"));
        assert!(strategy.authentication_white_set().contains("PULL_MESSAGE"));
    }

    #[tokio::test]
    async fn test_authentication_without_channel_id() {
        let config = AuthConfig {
            authentication_enabled: false,
            ..Default::default()
        };

        let strategy: StatefulAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatefulAuthenticationStrategy::new(config, None);

        let context = DefaultAuthenticationContext::new();

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_acl_generation_change_invalidates_cached_authentication_result() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let should_succeed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let provider = Arc::new(CountingAuthenticationProvider {
            calls: calls.clone(),
            should_succeed: should_succeed.clone(),
        });
        let acl_generation = Arc::new(AtomicU64::new(0));
        let strategy = StatefulAuthenticationStrategy::new_with_acl_generation(
            AuthConfig {
                authentication_enabled: true,
                ..AuthConfig::default()
            },
            Some(provider),
            acl_generation.clone(),
        );
        let mut context = DefaultAuthenticationContext::new();
        context.base.set_channel_id(Some(CheetahString::from("channel-1")));
        context.set_username(CheetahString::from("alice"));

        assert!(strategy.authenticate(&context).is_ok());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        should_succeed.store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(strategy.authenticate(&context).is_ok());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        acl_generation.fetch_add(1, Ordering::AcqRel);
        let result = strategy.authenticate(&context);

        assert!(result.is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn authenticates_inside_current_thread_runtime_without_block_in_place_panic() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let should_succeed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let provider = Arc::new(CountingAuthenticationProvider {
            calls: calls.clone(),
            should_succeed,
        });
        let strategy = StatefulAuthenticationStrategy::new(
            AuthConfig {
                authentication_enabled: true,
                ..AuthConfig::default()
            },
            Some(provider),
        );
        let mut context = DefaultAuthenticationContext::new();
        context
            .base
            .set_channel_id(Some(CheetahString::from("channel-current-thread")));
        context.set_username(CheetahString::from("alice"));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            assert!(strategy.authenticate(&context).is_ok());
        });

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
