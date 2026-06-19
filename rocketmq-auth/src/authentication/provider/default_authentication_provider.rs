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
use crate::AuthMetrics;

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

    /// Authentication counters shared with the owning runtime when available.
    metrics: AuthMetrics,
}

impl DefaultAuthenticationProvider {
    /// Create a new default authentication provider.
    pub fn new() -> Self {
        Self {
            auth_config: None,
            metadata_service: None,
            metadata_provider: None,
            authentication_context_builder: DefaultAuthenticationContextBuilder::new(),
            metrics: AuthMetrics::default(),
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
        self.metrics = provider_registry.metrics();
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
                    "[AUTHENTICATION] User:{} is authenticated failed. Error: {}",
                    username, err
                );
            } else {
                debug!("[AUTHENTICATION] User:{} is authenticated success.", username);
            }
        }
    }

    /// Internal authentication logic.
    async fn authenticate_internal(&self, context: &DefaultAuthenticationContext) -> RocketMQResult<()> {
        let metadata_provider = self.metadata_provider.as_ref().ok_or_else(|| {
            rocketmq_error::RocketMQError::authentication_failed("authentication metadata provider is not configured")
        })?;
        let signature_algorithm = self
            .auth_config
            .as_ref()
            .map(|config| config.signature_algorithm)
            .unwrap_or_default();
        let request_timestamp_expired_millis = self
            .auth_config
            .as_ref()
            .map(|config| config.request_timestamp_expired_millis)
            .unwrap_or_default();
        let handler = DefaultAuthenticationHandler::with_options(
            metadata_provider.clone(),
            signature_algorithm,
            request_timestamp_expired_millis,
            self.metrics.clone(),
        );
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
        self.auth_config = Some(config.clone());
        self.metadata_service = metadata_service;
        self.authentication_context_builder = DefaultAuthenticationContextBuilder::new();
        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(config, None).await?;
        self.metadata_provider = Some(Arc::new(provider));
        self.metrics = AuthMetrics::default();
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

        self.metrics.record_authentication_result(result.is_ok());
        result
    }

    /// Create context from gRPC metadata.
    fn new_context_from_metadata(
        &self,
        metadata: &HashMap<String, String>,
        request: Box<dyn Any + Send>,
    ) -> Self::Context {
        self.authentication_context_builder
            .build_from_grpc_metadata_map(metadata, request.as_ref())
            .unwrap_or_else(|_| DefaultAuthenticationContext::new())
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
    use cheetah_string::CheetahString;

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

    #[test]
    fn test_new_context_from_metadata_uses_grpc_header_parser() {
        let provider = DefaultAuthenticationProvider::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            "authorization".to_owned(),
            "MQv2-HMAC-SHA1 Credential=alice, SignedHeaders=x-mq-date-time, Signature=48656C6C6F".to_owned(),
        );
        metadata.insert("x-mq-date-time".to_owned(), "20231227T194619Z".to_owned());
        metadata.insert("channel-id".to_owned(), "channel-a".to_owned());

        let context = provider.new_context_from_metadata(&metadata, Box::new(()));

        assert_eq!(context.username(), Some(&CheetahString::from("alice")));
        assert_eq!(context.signature(), Some(&CheetahString::from("SGVsbG8=")));
        assert_eq!(context.content(), Some(b"20231227T194619Z".as_slice()));
        assert_eq!(context.base.channel_id(), Some(&CheetahString::from("channel-a")));
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

    #[tokio::test]
    async fn test_authenticate_uses_configured_signature_algorithm() {
        use crate::authentication::chain::acl_signer;
        use crate::authentication::chain::acl_signer::SignatureAlgorithm;
        use crate::authentication::enums::user_status::UserStatus;
        use crate::authentication::enums::user_type::UserType;

        let mut provider = DefaultAuthenticationProvider::new();
        provider
            .initialize(
                AuthConfig {
                    signature_algorithm: SignatureAlgorithm::HmacSha256,
                    ..AuthConfig::default()
                },
                None,
            )
            .await
            .unwrap();

        let metadata_provider = provider.metadata_provider().unwrap();
        let mut user = crate::authentication::model::user::User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        metadata_provider.create_user(user).await.unwrap();

        let content = b"test-content".to_vec();
        let signature =
            acl_signer::cal_signature_with_algorithm(&content, "secret", SignatureAlgorithm::HmacSha256).unwrap();

        let mut context = DefaultAuthenticationContext::new();
        context.set_username("alice".into());
        context.set_content(content);
        context.set_signature(signature.into());

        assert!(provider.authenticate(&context).await.is_ok());
    }

    #[tokio::test]
    async fn test_authenticate_rejects_expired_request_timestamp() {
        use crate::authentication::chain::acl_signer;
        use crate::authentication::enums::user_status::UserStatus;
        use crate::authentication::enums::user_type::UserType;

        let mut provider = DefaultAuthenticationProvider::new();
        provider
            .initialize(
                AuthConfig {
                    request_timestamp_expired_millis: 1,
                    ..AuthConfig::default()
                },
                None,
            )
            .await
            .unwrap();

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
        context.set_request_timestamp("1".into());
        context.set_request_timestamp_millis(1);

        let result = provider.authenticate(&context).await;

        assert!(result.unwrap_err().to_string().contains("Request timestamp expired"));
    }
}
