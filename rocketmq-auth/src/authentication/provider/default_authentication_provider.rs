//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

//! Default Authentication Provider Implementation (Rust 2021 Standard)

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tracing::debug;
use tracing::info;

use crate::authentication::builder::default_authentication_context_builder::DefaultAuthenticationContextBuilder;
use crate::authentication::builder::AuthenticationContextBuilder;
use crate::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use crate::config::AuthConfig;

use super::authentication_provider::AuthenticationProvider;

/// Default authentication provider.
///
/// Maps to Java class:
/// ```java
/// public class DefaultAuthenticationProvider implements AuthenticationProvider<DefaultAuthenticationContext>
/// ```
pub struct DefaultAuthenticationProvider {
    /// Authentication configuration.
    ///
    /// Maps to: `protected AuthConfig authConfig;`
    auth_config: Option<AuthConfig>,

    /// Metadata service supplier.
    ///
    /// Maps to: `protected Supplier<?> metadataService;`
    metadata_service: Option<Arc<dyn Any + Send + Sync>>,

    /// Authentication context builder.
    ///
    /// Maps to: `protected AuthenticationContextBuilder<DefaultAuthenticationContext>
    /// authenticationContextBuilder;`
    authentication_context_builder: DefaultAuthenticationContextBuilder,
}

impl DefaultAuthenticationProvider {
    /// Create a new default authentication provider.
    pub fn new() -> Self {
        Self {
            auth_config: None,
            metadata_service: None,
            authentication_context_builder: DefaultAuthenticationContextBuilder::new(),
        }
    }

    /// Perform audit logging.
    ///
    /// Maps to: `protected void doAuditLog(DefaultAuthenticationContext context, Throwable ex)`
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
    ///
    /// This is where you'd implement the actual authentication handler chain logic.
    /// In Java this is: `this.newHandlerChain().handle(context)`
    async fn authenticate_internal(&self, _context: &DefaultAuthenticationContext) -> RocketMQResult<()> {
        // TODO: Implement handler chain logic here
        // For now, just return Ok for compatibility
        Ok(())
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
    ///
    /// Maps to Java method:
    /// ```java
    /// public void initialize(AuthConfig config, Supplier<?> metadataService) {
    ///     this.authConfig = config;
    ///     this.metadataService = metadataService;
    ///     this.authenticationContextBuilder = new DefaultAuthenticationContextBuilder();
    /// }
    /// ```
    async fn initialize(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<()> {
        self.auth_config = Some(config);
        self.metadata_service = metadata_service;
        self.authentication_context_builder = DefaultAuthenticationContextBuilder::new();
        Ok(())
    }

    /// Authenticate a request.
    ///
    /// Maps to Java method:
    /// ```java
    /// public CompletableFuture<Void> authenticate(DefaultAuthenticationContext context) {
    ///     return this.newHandlerChain().handle(context)
    ///         .whenComplete((nil, ex) -> doAuditLog(context, ex));
    /// }
    /// ```
    async fn authenticate(&self, context: &Self::Context) -> RocketMQResult<()> {
        // Note: In Java this uses HandlerChain. In Rust, we'll implement the authentication
        // logic directly here. For full chain support, you'd need to implement a handler chain.

        let result = self.authenticate_internal(context).await;

        // Audit log
        match &result {
            Ok(_) => self.do_audit_log(context, None),
            Err(e) => self.do_audit_log(context, Some(&e.to_string())),
        }

        result
    }

    /// Create context from gRPC metadata.
    ///
    /// Maps to Java method:
    /// ```java
    /// public DefaultAuthenticationContext newContext(Metadata metadata, GeneratedMessageV3 request) {
    ///     return this.authenticationContextBuilder.build(metadata, request);
    /// }
    /// ```
    fn new_context_from_metadata(
        &self,
        _metadata: &HashMap<String, String>,
        _request: Box<dyn Any + Send>,
    ) -> Self::Context {
        // TODO: Implement gRPC metadata parsing when grpc feature is enabled
        DefaultAuthenticationContext::new()
    }

    /// Create context from remoting command.
    ///
    /// Maps to Java method:
    /// ```java
    /// public DefaultAuthenticationContext newContext(ChannelHandlerContext context, RemotingCommand command) {
    ///     return this.authenticationContextBuilder.build(context, command);
    /// }
    /// ```
    fn new_context_from_command(&self, command: &RemotingCommand) -> Self::Context {
        self.authentication_context_builder
            .build_from_remoting(command, None)
            .unwrap_or_else(|_| DefaultAuthenticationContext::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Should not fail with empty context (for now)
        let result = provider.authenticate(&context).await;
        assert!(result.is_ok());
    }
}
