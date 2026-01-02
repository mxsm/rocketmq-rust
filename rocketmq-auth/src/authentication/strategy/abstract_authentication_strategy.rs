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

//! Abstract authentication strategy with template method pattern.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::authentication::provider::AuthenticationProvider;
use crate::authorization::context::authentication_context::AuthenticationContext;
use crate::config::AuthConfig;

/// Authentication strategy trait with template method pattern.
#[allow(async_fn_in_trait)]
pub trait AbstractAuthenticationStrategy: Send + Sync {
    /// Get the authentication configuration.
    fn auth_config(&self) -> &AuthConfig;

    /// Get the authentication whitelist set.
    fn authentication_white_set(&self) -> &HashSet<String>;

    /// Get the authentication provider (if available).
    fn authentication_provider(&self) -> Option<&dyn Any>;

    /// Template method for authentication evaluation.
    async fn do_evaluate<C: AuthenticationContext>(&self, context: &C) -> RocketMQResult<()> {
        if !self.auth_config().authentication_enabled {
            return Ok(());
        }

        let provider = match self.authentication_provider() {
            Some(p) => p,
            None => return Ok(()),
        };

        self.authenticate_with_provider(provider, context).await
    }

    /// Authenticate using the provider.
    async fn authenticate_with_provider<C: AuthenticationContext>(
        &self,
        _provider: &dyn Any,
        _context: &C,
    ) -> RocketMQResult<()>;

    /// Check if RPC code is whitelisted.
    fn is_whitelisted(&self, rpc_code: &str) -> bool {
        self.authentication_white_set().contains(rpc_code)
    }
}

/// Base implementation of authentication strategy.
pub struct BaseAuthenticationStrategy<P>
where
    P: AuthenticationProvider,
{
    auth_config: AuthConfig,
    authentication_white_set: HashSet<String>,
    authentication_provider: Option<Arc<P>>,
}

impl<P> BaseAuthenticationStrategy<P>
where
    P: AuthenticationProvider + Send + Sync + 'static,
{
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

    pub async fn initialize_provider(
        &mut self,
        _metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<()> {
        Ok(())
    }

    pub fn provider(&self) -> Option<&P> {
        self.authentication_provider.as_deref()
    }
}

#[allow(async_fn_in_trait)]
impl<P> AbstractAuthenticationStrategy for BaseAuthenticationStrategy<P>
where
    P: AuthenticationProvider + Send + Sync + 'static,
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
        if self.authentication_provider.is_some() {
            return Err(RocketMQError::authentication_failed(
                "Provider authentication not yet implemented for generic context",
            ));
        }

        Err(RocketMQError::authentication_failed(
            "No authentication provider available",
        ))
    }
}

/// Factory trait for creating authentication strategies.
pub trait AuthenticationStrategyFactory {
    /// Create a new authentication strategy from configuration.
    fn create_strategy(
        auth_config: AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<Self>
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
    use crate::authentication::provider::DefaultAuthenticationProvider;

    #[tokio::test]
    async fn test_base_strategy_creation() {
        let config = AuthConfig::default();
        let provider = Arc::new(DefaultAuthenticationProvider::new());

        let strategy = BaseAuthenticationStrategy::new(config, Some(provider));

        assert!(strategy.provider().is_some());
        assert!(strategy.authentication_white_set().is_empty());
    }

    #[tokio::test]
    async fn test_whitelist_parsing() {
        let config = AuthConfig {
            authentication_whitelist: "100,200, 300 ".into(),
            ..Default::default()
        };

        let strategy: BaseAuthenticationStrategy<DefaultAuthenticationProvider> =
            BaseAuthenticationStrategy::new(config, None);

        assert_eq!(strategy.authentication_white_set().len(), 3);
        assert!(strategy.authentication_white_set().contains("100"));
        assert!(strategy.authentication_white_set().contains("200"));
        assert!(strategy.authentication_white_set().contains("300"));
    }

    #[tokio::test]
    async fn test_authentication_disabled() {
        let config = AuthConfig {
            authentication_enabled: false,
            ..Default::default()
        };

        let strategy: BaseAuthenticationStrategy<DefaultAuthenticationProvider> =
            BaseAuthenticationStrategy::new(config, None);

        let context = DefaultAuthenticationContext::new();

        let result = strategy.do_evaluate(&context).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_whitelisted_rpc_code() {
        let config = AuthConfig {
            authentication_enabled: true,
            authentication_whitelist: "SEND_MESSAGE,PULL_MESSAGE".into(),
            ..Default::default()
        };

        let strategy: BaseAuthenticationStrategy<DefaultAuthenticationProvider> =
            BaseAuthenticationStrategy::new(config, None);

        assert!(strategy.authentication_white_set().contains("SEND_MESSAGE"));
    }
}
