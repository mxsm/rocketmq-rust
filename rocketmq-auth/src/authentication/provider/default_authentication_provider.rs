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

//! Default Authentication Provider Implementation (Rust 2021 Standard)

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_error::AuthError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tracing::debug;
use tracing::info;

use crate::authentication::builder::default_authentication_context_builder::DefaultAuthenticationContextBuilder;
use crate::authentication::builder::AuthenticationContextBuilder;
use crate::authentication::chain::default_authentication_handler::DefaultAuthenticationHandler;
use crate::authentication::chain::handler::AuthenticationHandler;
use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use crate::authentication::provider::authentication_metadata_provider::AuthenticationMetadataProvider;
use crate::authentication::provider::local_authentication_metadata_provider::LocalAuthenticationMetadataProvider;
use crate::config::AuthConfig;
use crate::runtime::ProviderRegistry;

use super::authentication_provider::AuthenticationProvider;

/// Default authentication provider.
pub struct DefaultAuthenticationProvider {
    /// Authentication configuration.
    auth_config: Option<AuthConfig>,

    /// Metadata service supplier.
    metadata_service: Option<Arc<dyn Any + Send + Sync>>,

    /// Local metadata provider used by the default handler chain.
    metadata_provider: Option<Arc<LocalAuthenticationMetadataProvider>>,

    /// Authentication context builder.
    authentication_context_builder: DefaultAuthenticationContextBuilder,
}

impl DefaultAuthenticationProvider {
    /// Create a new default authentication provider.
    pub fn new() -> Self {
        Self {
            auth_config: None,
            metadata_service: None,
            metadata_provider: None,
            authentication_context_builder: DefaultAuthenticationContextBuilder::new(),
        }
    }

    pub fn metadata_provider(&self) -> Option<Arc<LocalAuthenticationMetadataProvider>> {
        self.metadata_provider.clone()
    }

    pub fn initialize_with_registry(
        &mut self,
        config: AuthConfig,
        provider_registry: ProviderRegistry,
    ) -> RocketMQResult<()> {
        self.auth_config = Some(config);
        self.metadata_service = None;
        self.authentication_context_builder = DefaultAuthenticationContextBuilder::new();
        self.metadata_provider = Some(provider_registry.authentication_metadata_provider());
        Ok(())
    }

    /// Perform audit logging.
    fn do_audit_log(&self, context: &DefaultAuthenticationContext, error: Option<&str>) {
        if let Some(username) = context.username() {
            if username.is_empty() {
                return;
            }

            if let Some(err) = error {
                info!(
                    "[AUTHENTICATION] User:{} is authenticated failed with Signature = {}. Error: {}",
                    username,
                    context.signature().map(|s| s.as_str()).unwrap_or(""),
                    err
                );
            } else {
                debug!(
                    "[AUTHENTICATION] User:{} is authenticated success with Signature = {}.",
                    username,
                    context.signature().map(|s| s.as_str()).unwrap_or("")
                );
            }
        }
    }

    /// Internal authentication logic.
    async fn authenticate_internal(&self, context: &DefaultAuthenticationContext) -> RocketMQResult<()> {
        let metadata_provider = self.metadata_provider.as_ref().ok_or_else(|| {
            rocketmq_error::RocketMQError::authentication_failed("authentication metadata provider is not configured")
        })?;
        let handler = DefaultAuthenticationHandler::new(metadata_provider.clone());
        handler.handle(context).await.map_err(map_auth_error)
    }
}

impl Default for DefaultAuthenticationProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthenticationProvider for DefaultAuthenticationProvider {
    type Context = DefaultAuthenticationContext;

    /// Initialize the provider.
    async fn initialize(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<()> {
        self.auth_config = Some(config);
        self.metadata_service = metadata_service;
        self.authentication_context_builder = DefaultAuthenticationContextBuilder::new();
        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider
            .initialize(
                self.auth_config
                    .clone()
                    .expect("auth_config was just set in initialize"),
                None,
            )
            .await?;
        self.metadata_provider = Some(Arc::new(provider));
        Ok(())
    }

    /// Authenticate a request.
    async fn authenticate(&self, context: &Self::Context) -> RocketMQResult<()> {
        // Note: This uses handler chain pattern. For full chain support,
        // you'd need to implement a handler chain.

        let result = self.authenticate_internal(context).await;

        // Audit log
        match &result {
            Ok(_) => self.do_audit_log(context, None),
            Err(e) => self.do_audit_log(context, Some(&e.to_string())),
        }

        result
    }

    /// Create context from gRPC metadata.
    fn new_context_from_metadata(
        &self,
        _metadata: &HashMap<String, String>,
        _request: Box<dyn Any + Send>,
    ) -> Self::Context {
        // TODO: Implement gRPC metadata parsing when grpc feature is enabled
        DefaultAuthenticationContext::new()
    }

    /// Create context from remoting command.
    fn new_context_from_command(&self, command: &RemotingCommand) -> Self::Context {
        self.authentication_context_builder
            .build_from_remoting(command, None)
            .unwrap_or_else(|_| DefaultAuthenticationContext::new())
    }
}

fn map_auth_error(error: AuthError) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::authentication_failed(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::provider::authentication_metadata_provider::AuthenticationMetadataProvider;

    #[tokio::test]
    async fn test_default_provider_initialization() {
        let mut provider = DefaultAuthenticationProvider::new();
        let config = AuthConfig::default();

        assert!(provider.initialize(config, None).await.is_ok());
    }

    #[test]
    fn test_default_provider_creation() {
        let provider = DefaultAuthenticationProvider::new();
        assert!(provider.auth_config.is_none());
    }

    #[tokio::test]
    async fn test_authenticate() {
        let mut provider = DefaultAuthenticationProvider::new();
        provider.initialize(AuthConfig::default(), None).await.unwrap();

        let context = DefaultAuthenticationContext::new();

        let result = provider.authenticate(&context).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_authenticate_success() {
        use crate::authentication::chain::acl_signer;
        use crate::authentication::enums::user_status::UserStatus;
        use crate::authentication::enums::user_type::UserType;

        let mut provider = DefaultAuthenticationProvider::new();
        provider.initialize(AuthConfig::default(), None).await.unwrap();

        let metadata_provider = provider.metadata_provider().unwrap();
        let mut user = crate::authentication::model::user::User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        metadata_provider.create_user(user).await.unwrap();

        let content = b"test-content".to_vec();
        let signature = acl_signer::cal_signature(&content, "secret").unwrap();

        let mut context = DefaultAuthenticationContext::new();
        context.set_username("alice".into());
        context.set_content(content);
        context.set_signature(signature.into());

        assert!(provider.authenticate(&context).await.is_ok());
    }
}
