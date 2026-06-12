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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::authorization::builder::default_authorization_context_builder::DefaultAuthorizationContextBuilder;
use crate::authorization::builder::AuthorizationContextBuilder;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::evaluator::AuthorizationEvaluator;
use crate::authorization::manager::metadata_manager_impl::AuthorizationMetadataManagerImpl;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationProvider;
use crate::authorization::provider::DefaultAuthorizationProvider;
use crate::authorization::strategy::AuthorizationStrategy;
use crate::authorization::strategy::StatefulAuthorizationStrategy;
use crate::authorization::strategy::StatelessAuthorizationStrategy;
use crate::config::AuthConfig;

static INSTANCE_CACHE: OnceLock<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>> = OnceLock::new();

const PROVIDER_PREFIX: &str = "PROVIDER_";
const METADATA_PROVIDER_PREFIX: &str = "METADATA_PROVIDER_";
const EVALUATOR_PREFIX: &str = "EVALUATOR_";

pub struct AuthorizationFactory;

impl AuthorizationFactory {
    pub fn get_provider(config: &AuthConfig) -> RocketMQResult<Arc<DefaultAuthorizationProvider>> {
        if !is_blank_or_supported(
            config.authorization_provider.as_str(),
            &["DefaultAuthorizationProvider", "default"],
        ) {
            return Err(unsupported(
                "authorizationProvider",
                config.authorization_provider.as_str(),
            ));
        }

        let key = format!("{}{}", PROVIDER_PREFIX, config.config_name);
        Self::compute_if_absent(&key, || {
            let provider = new_initialized_default_authorization_provider(config.clone(), None)?;
            Ok(Arc::new(provider) as Arc<dyn Any + Send + Sync>)
        })
        .and_then(|value| {
            value
                .downcast::<DefaultAuthorizationProvider>()
                .map_err(|_| RocketMQError::illegal_argument("Failed to downcast authorization provider"))
        })
    }

    pub fn get_metadata_provider(
        config: &AuthConfig,
    ) -> RocketMQResult<Option<Arc<LocalAuthorizationMetadataProvider>>> {
        if config.authorization_metadata_provider.is_empty() {
            return Ok(None);
        }
        if !is_supported(
            config.authorization_metadata_provider.as_str(),
            &["LocalAuthorizationMetadataProvider", "local"],
        ) {
            return Err(unsupported(
                "authorizationMetadataProvider",
                config.authorization_metadata_provider.as_str(),
            ));
        }

        let key = format!("{}{}", METADATA_PROVIDER_PREFIX, config.config_name);
        Self::compute_if_absent(&key, || {
            let mut provider = LocalAuthorizationMetadataProvider::new();
            provider.initialize(config.clone(), None).map_err(|error| {
                RocketMQError::auth_config_invalid("authorizationMetadataProvider", error.to_string())
            })?;
            Ok(Arc::new(provider) as Arc<dyn Any + Send + Sync>)
        })
        .and_then(|value| {
            value
                .downcast::<LocalAuthorizationMetadataProvider>()
                .map_err(|_| RocketMQError::illegal_argument("Failed to downcast authorization metadata provider"))
        })
        .map(Some)
    }

    pub fn get_metadata_manager(config: &AuthConfig) -> Result<AuthorizationMetadataManagerImpl, AuthorizationError> {
        AuthorizationMetadataManagerImpl::from_config(config)
    }

    pub fn get_strategy(
        config: &AuthConfig,
        metadata_service: Option<Box<dyn Any + Send + Sync>>,
    ) -> Result<Box<dyn AuthorizationStrategy>, AuthorizationError> {
        let strategy = config.authorization_strategy.as_str();
        if strategy.trim().is_empty()
            || strategy.ends_with("StatelessAuthorizationStrategy")
            || strategy.eq_ignore_ascii_case("stateless")
        {
            return StatelessAuthorizationStrategy::new(config.clone(), metadata_service)
                .map(|strategy| Box::new(strategy) as Box<dyn AuthorizationStrategy>);
        }
        if strategy.ends_with("StatefulAuthorizationStrategy") || strategy.eq_ignore_ascii_case("stateful") {
            return StatefulAuthorizationStrategy::new(config.clone(), metadata_service)
                .map(|strategy| Box::new(strategy) as Box<dyn AuthorizationStrategy>);
        }
        Err(AuthorizationError::ConfigurationError(format!(
            "Unsupported authorization strategy: {strategy}"
        )))
    }

    pub fn get_evaluator(
        config: &AuthConfig,
    ) -> Result<Arc<AuthorizationEvaluator<Box<dyn AuthorizationStrategy>>>, AuthorizationError> {
        let key = format!("{}{}", EVALUATOR_PREFIX, config.config_name);
        Self::compute_if_absent(&key, || {
            let strategy = Self::get_strategy(config, None)
                .map_err(|error| RocketMQError::auth_config_invalid("authorizationStrategy", error.to_string()))?;
            Ok(Arc::new(AuthorizationEvaluator::new(strategy)) as Arc<dyn Any + Send + Sync>)
        })
        .map_err(|error| AuthorizationError::ConfigurationError(error.to_string()))
        .and_then(|value| {
            value
                .downcast::<AuthorizationEvaluator<Box<dyn AuthorizationStrategy>>>()
                .map_err(|_| {
                    AuthorizationError::ConfigurationError("Failed to downcast authorization evaluator".to_string())
                })
        })
    }

    pub fn new_contexts_from_command(
        config: &AuthConfig,
        channel_context: &(dyn Any + Send + Sync),
        command: &RemotingCommand,
    ) -> RocketMQResult<Option<Vec<DefaultAuthorizationContext>>> {
        let _provider = Self::get_provider(config)?;
        let builder = DefaultAuthorizationContextBuilder::new(config.clone());
        builder
            .build_from_remoting(channel_context, command)
            .map(Some)
            .map_err(RocketMQError::from)
    }

    fn compute_if_absent<F>(key: &str, factory: F) -> RocketMQResult<Arc<dyn Any + Send + Sync>>
    where
        F: FnOnce() -> RocketMQResult<Arc<dyn Any + Send + Sync>>,
    {
        let cache = INSTANCE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        {
            let guard = cache
                .lock()
                .map_err(|error| RocketMQError::illegal_argument(format!("Cache lock error: {error}")))?;
            if let Some(value) = guard.get(key) {
                return Ok(Arc::clone(value));
            }
        }

        let value = factory()?;

        let mut guard = cache
            .lock()
            .map_err(|error| RocketMQError::illegal_argument(format!("Cache lock error: {error}")))?;
        if let Some(value) = guard.get(key) {
            return Ok(Arc::clone(value));
        }
        guard.insert(key.to_string(), Arc::clone(&value));
        Ok(value)
    }
}

impl AuthorizationStrategy for Box<dyn AuthorizationStrategy> {
    fn evaluate(
        &self,
        context: &crate::authorization::context::default_authorization_context::DefaultAuthorizationContext,
    ) -> crate::authorization::strategy::StrategyResult<()> {
        self.as_ref().evaluate(context)
    }
}

fn is_blank_or_supported(configured: &str, supported: &[&str]) -> bool {
    configured.trim().is_empty() || is_supported(configured, supported)
}

fn is_supported(configured: &str, supported: &[&str]) -> bool {
    supported
        .iter()
        .any(|value| configured.eq_ignore_ascii_case(value) || configured.ends_with(value))
}

fn unsupported(key: &'static str, configured: &str) -> RocketMQError {
    RocketMQError::auth_config_invalid(key, format!("Unsupported {key}: {configured}"))
}

fn new_initialized_default_authorization_provider(
    config: AuthConfig,
    metadata_service: Option<Box<dyn Any + Send + Sync>>,
) -> RocketMQResult<DefaultAuthorizationProvider> {
    let mut provider = DefaultAuthorizationProvider::new();
    provider
        .initialize_with_metadata(config, metadata_service)
        .map_err(|error| RocketMQError::auth_config_invalid("authorizationProvider", error.to_string()))?;
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::action::Action;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

    use super::*;

    fn config(name: &str) -> AuthConfig {
        AuthConfig {
            config_name: CheetahString::from(name),
            cluster_name: CheetahString::from_static_str("DefaultCluster"),
            ..AuthConfig::default()
        }
    }

    #[test]
    fn get_provider_caches_by_config_name() {
        let config = config("authorization-factory-cache");

        let first = AuthorizationFactory::get_provider(&config).unwrap();
        let second = AuthorizationFactory::get_provider(&config).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn new_contexts_from_command_uses_default_builder() {
        let config = config("authorization-factory-contexts");
        let mut fields = HashMap::new();
        fields.insert(
            CheetahString::from_static_str("AccessKey"),
            CheetahString::from_static_str("alice"),
        );
        fields.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("TopicA"),
        );
        let command =
            RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32()).set_ext_fields(fields);

        let contexts = AuthorizationFactory::new_contexts_from_command(&config, &(), &command)
            .unwrap()
            .unwrap();

        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].subject_key(), Some("User:alice"));
        assert_eq!(contexts[0].resource_key(), Some("Topic:TopicA".to_string()));
        assert_eq!(contexts[0].actions(), &[Action::Pub]);
    }
}
