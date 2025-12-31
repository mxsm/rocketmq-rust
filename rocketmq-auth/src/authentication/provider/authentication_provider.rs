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

//! Authentication Provider Trait (Rust 2021 Standard)

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::authorization::context::authentication_context::AuthenticationContext;
use crate::config::AuthConfig;

/// Authentication provider trait.
///
/// Maps to Java interface:
/// ```java
/// public interface AuthenticationProvider<T extends AuthenticationContext>
/// ```
#[allow(async_fn_in_trait)]
pub trait AuthenticationProvider: Send + Sync {
    /// Associated authentication context type.
    type Context: AuthenticationContext;

    /// Initialize the provider.
    ///
    /// Maps to: `void initialize(AuthConfig config, Supplier<?> metadataService)`
    async fn initialize(
        &mut self,
        config: AuthConfig,
        metadata_service: Option<Arc<dyn Any + Send + Sync>>,
    ) -> RocketMQResult<()>;

    /// Authenticate a request.
    ///
    /// Maps to: `CompletableFuture<Void> authenticate(T context)`
    async fn authenticate(&self, context: &Self::Context) -> RocketMQResult<()>;

    /// Create context from gRPC metadata.
    ///
    /// Maps to: `T newContext(Metadata metadata, GeneratedMessageV3 request)`
    fn new_context_from_metadata(
        &self,
        metadata: &HashMap<String, String>,
        request: Box<dyn Any + Send>,
    ) -> Self::Context;

    /// Create context from remoting command.
    ///
    /// Maps to: `T newContext(ChannelHandlerContext context, RemotingCommand command)`
    fn new_context_from_command(&self, command: &RemotingCommand) -> Self::Context;
}
