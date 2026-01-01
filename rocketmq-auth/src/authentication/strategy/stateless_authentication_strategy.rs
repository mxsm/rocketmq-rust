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

//! Stateless authentication strategy without caching.
//!
//! This strategy performs authentication for every request without caching results.
//! Each authentication is independent and does not rely on historical state.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_error::AuthError;
use rocketmq_error::RocketMQResult;

use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use crate::authentication::provider::AuthenticationProvider;
use crate::authentication::strategy::abstract_authentication_strategy::AbstractAuthenticationStrategy;
use crate::authentication::strategy::authentication_strategy::AuthenticationStrategy;
use crate::authorization::context::authentication_context::AuthenticationContext;
use crate::config::AuthConfig;

/// Stateless authentication strategy.
///
/// Performs authentication for every request without any caching or state retention.
/// This ensures that each authentication check is independent and current.
///
/// - No state shared between requests
/// - No caching of authentication results
/// - Suitable for scenarios requiring fresh validation on every request
pub struct StatelessAuthenticationStrategy<P>
where
    P: AuthenticationProvider<Context = DefaultAuthenticationContext>,
{
    auth_config: AuthConfig,
    authentication_white_set: HashSet<String>,
    authentication_provider: Option<Arc<P>>,
}

impl<P> StatelessAuthenticationStrategy<P>
where
    P: AuthenticationProvider<Context = DefaultAuthenticationContext> + Send + Sync + 'static,
{
    /// Create a new stateless authentication strategy.
    pub fn new(auth_config: AuthConfig, provider: Option<Arc<P>>) -> Self {
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

        Self {
            auth_config,
            authentication_white_set,
            authentication_provider: provider,
        }
    }

    /// Perform authentication without caching.
    fn do_authenticate_internal(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError> {
        if !self.auth_config.authentication_enabled {
            return Ok(());
        }

        let default_context = context
            .as_any()
            .downcast_ref::<DefaultAuthenticationContext>()
            .ok_or_else(|| {
                AuthError::AuthenticationFailed(
                    "Stateless authentication requires DefaultAuthenticationContext".to_string(),
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

        let auth_result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(provider.authenticate(default_context))
        });

        auth_result.map_err(|e| AuthError::AuthenticationFailed(e.to_string()))
    }

    pub fn provider(&self) -> Option<&P> {
        self.authentication_provider.as_deref()
    }
}

#[allow(async_fn_in_trait)]
impl<P> AbstractAuthenticationStrategy for StatelessAuthenticationStrategy<P>
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
            "Use authenticate() method for stateless authentication",
        ))
    }
}

impl<P> AuthenticationStrategy for StatelessAuthenticationStrategy<P>
where
    P: AuthenticationProvider<Context = DefaultAuthenticationContext> + Send + Sync + 'static,
{
    fn authenticate(&self, context: &dyn AuthenticationContext) -> Result<(), AuthError> {
        self.do_authenticate_internal(context)
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;
    use crate::authentication::provider::DefaultAuthenticationProvider;

    #[test]
    fn test_stateless_strategy_creation() {
        let config = AuthConfig::default();
        let provider = Arc::new(DefaultAuthenticationProvider::new());

        let strategy = StatelessAuthenticationStrategy::new(config, Some(provider));

        assert!(strategy.provider().is_some());
        assert!(strategy.authentication_white_set().is_empty());
    }

    #[test]
    fn test_authentication_disabled() {
        let config = AuthConfig {
            authentication_enabled: false,
            ..Default::default()
        };

        let strategy: StatelessAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatelessAuthenticationStrategy::new(config, None);

        let context = DefaultAuthenticationContext::new();
        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_whitelist_parsing() {
        let config = AuthConfig {
            authentication_whitelist: "SEND_MESSAGE,PULL_MESSAGE".into(),
            ..Default::default()
        };

        let strategy: StatelessAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatelessAuthenticationStrategy::new(config, None);

        assert!(strategy.authentication_white_set().contains("SEND_MESSAGE"));
        assert!(strategy.authentication_white_set().contains("PULL_MESSAGE"));
    }

    #[test]
    fn test_whitelist_with_spaces() {
        let config = AuthConfig {
            authentication_whitelist: " SEND_MESSAGE , PULL_MESSAGE , QUERY_MESSAGE ".into(),
            ..Default::default()
        };

        let strategy: StatelessAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatelessAuthenticationStrategy::new(config, None);

        assert_eq!(strategy.authentication_white_set().len(), 3);
        assert!(strategy.authentication_white_set().contains("SEND_MESSAGE"));
        assert!(strategy.authentication_white_set().contains("PULL_MESSAGE"));
        assert!(strategy.authentication_white_set().contains("QUERY_MESSAGE"));
    }

    #[test]
    fn test_authentication_enabled_without_provider() {
        let config = AuthConfig {
            authentication_enabled: true,
            ..Default::default()
        };

        let strategy: StatelessAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatelessAuthenticationStrategy::new(config, None);

        let context = DefaultAuthenticationContext::new();
        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_whitelisted_rpc_code() {
        let config = AuthConfig {
            authentication_enabled: true,
            authentication_whitelist: "100,200".into(),
            ..Default::default()
        };

        let strategy: StatelessAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatelessAuthenticationStrategy::new(config, None);

        let mut context = DefaultAuthenticationContext::new();
        context.base.set_rpc_code(Some(CheetahString::from("100")));

        let result = strategy.authenticate(&context);
        assert!(result.is_ok());
    }

    #[test]
    fn test_no_caching_behavior() {
        let config = AuthConfig {
            authentication_enabled: false,
            ..Default::default()
        };

        let strategy: StatelessAuthenticationStrategy<DefaultAuthenticationProvider> =
            StatelessAuthenticationStrategy::new(config, None);

        let context = DefaultAuthenticationContext::new();

        let result1 = strategy.authenticate(&context);
        assert!(result1.is_ok());

        let result2 = strategy.authenticate(&context);
        assert!(result2.is_ok());
    }
}
